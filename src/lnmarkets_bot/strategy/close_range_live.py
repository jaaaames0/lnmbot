"""Live Strategy adapter for the pure close-range state machine."""

from __future__ import annotations

import json
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]

from .base import Bar, Strategy, StrategyState
from .close_range import BreakoutDecision, CloseRangeMachine, DailyCandle
from .intents import OrderIntent, Side, SignalKind

if TYPE_CHECKING:
    from pathlib import Path


class CloseRangeLive(Strategy):
    """Turn completed LN Markets daily bars into independently routed units."""

    tfs = ("1d",)
    position_slots = ("k0", "k1", "k2", "k3")
    VERSION = 1

    def __init__(
        self,
        params: dict[str, Any] | None = None,
        *,
        machine: CloseRangeMachine | None = None,
    ) -> None:
        super().__init__(params)
        self.unit_notional_usd = float(self.params.get("unit_notional_usd", 100.0))
        self.leverage = float(self.params.get("leverage", 5.0))
        activation = self.params.get("activation_ts")
        self.activation_ts = (
            datetime.fromisoformat(str(activation)).astimezone(UTC)
            if activation
            else datetime.now(UTC)
        )
        if self.unit_notional_usd <= 0 or self.leverage <= 0:
            raise ValueError("breakout size and leverage must be positive")
        self.machine = machine or CloseRangeMachine()
        self._recent_decisions: deque[dict[str, Any]] = deque(maxlen=256)
        self._urgent_intents: deque[OrderIntent] = deque()
        self._closing_campaign_id: str | None = None
        self._closing_slots: set[str] = set()

    def on_startup(self, state: StrategyState) -> None:
        owned = [slot for slot, pos in state.positions.items() if pos.qty_sats]
        if owned and self.machine.campaign is None and set(owned) <= self._closing_slots:
            for slot in owned:
                self._urgent_intents.append(
                    self._exit_intent(slot, "resume_campaign_close", self._closing_campaign_id)
                )
            return
        if owned and (self.machine.campaign is None or self.machine.campaign.origin != "live"):
            raise RuntimeError("owned breakout trades have no live campaign state")
        if (
            self.machine.campaign is not None
            and self.machine.campaign.origin == "live"
            and state.position("k0").qty_sats == 0
            and not self._closing_slots
        ):
            raise RuntimeError("live breakout campaign has no owned parent trade")
        if self.machine.campaign is not None:
            known_slots = {f"k{unit.k}" for unit in self.machine.campaign.units}
            if not set(owned) <= known_slots:
                raise RuntimeError("owned breakout slot is absent from campaign state")

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        urgent = list(self._urgent_intents)
        self._urgent_intents.clear()
        if bar.timeframe != "1d":
            return urgent
        candle_ts = bar.ts.astimezone(UTC) - timedelta(days=1)
        if self.machine.last_bar_ts is not None and candle_ts <= self.machine.last_bar_ts:
            return urgent
        candle = DailyCandle(candle_ts, bar.open, bar.high, bar.low, bar.close)
        decisions = self.machine.complete_and_apply_next_open(
            candle,
            next_open_ts=bar.ts,
            # The aggregate is emitted immediately after the final 1m candle.
            # Its close is the current executable reference until an actual
            # market fill replaces the modeled value.
            next_open_price=bar.close,
            activation_ts=self.activation_ts,
        )
        for decision in decisions:
            self._recent_decisions.append(self._decision_dict(decision))
        if bar.warmup:
            return urgent
        return urgent + self._intents(decisions, state)

    def _intents(
        self, decisions: list[BreakoutDecision], state: StrategyState
    ) -> list[OrderIntent]:
        intents: list[OrderIntent] = []
        for decision in decisions:
            metadata = {
                **decision.metadata,
                "campaign_id": decision.campaign_id,
                "k": decision.k,
                "decision_kind": decision.kind,
            }
            if decision.kind in {"paper_parent", "paper_addon"} and decision.k is not None:
                if self._closing_slots:
                    continue
                # Never adopt an add-on without an owned parent.  Historical
                # occupancy still blocks late parent entry in the pure machine.
                if decision.k > 0 and state.position("k0").qty_sats == 0:
                    continue
                slot = f"k{decision.k}"
                if state.position(slot).qty_sats:
                    continue
                side = Side.LONG if decision.side == 1 else Side.SHORT
                intents.append(
                    OrderIntent(
                        kind=SignalKind.ENTRY,
                        trigger_tf="1d",
                        position_key=slot,
                        side=side,
                        size_usd=self.unit_notional_usd,
                        leverage=self.leverage,
                        reason=decision.reason,
                        metadata=metadata,
                    )
                )
            elif decision.kind == "campaign_exit":
                self._closing_campaign_id = decision.campaign_id
                for slot in self.position_slots:
                    if state.position(slot).qty_sats:
                        self._closing_slots.add(slot)
                        intents.append(
                            self._exit_intent(
                                slot,
                                decision.reason,
                                decision.campaign_id,
                                trigger_tf="1d",
                            )
                        )
        return intents

    def on_order_result(self, intent: OrderIntent, decision: Any, state: StrategyState) -> None:
        if intent.kind == SignalKind.EXIT:
            if decision.order_id and decision.order_id > 0:
                self._closing_slots.discard(intent.position_key)
                if not self._closing_slots:
                    self._closing_campaign_id = None
            return
        if intent.kind != SignalKind.ENTRY or not decision.order_id or decision.order_id <= 0:
            return
        price = decision.detail.get("price_usd")
        if price is None:
            raise RuntimeError("confirmed breakout entry has no fill price")
        k = int((intent.position_key or "k0")[1:])
        assert self.machine.campaign is not None
        unit = next(value for value in self.machine.campaign.units if value.k == k)
        self.machine.confirm_owned_fill(
            k=k,
            ts=unit.entry_ts,
            price=float(price),
        )

    def on_external_position_closed(self, event: Any, state: StrategyState) -> None:
        """Fold venue liquidation into campaign state and close surviving children."""
        k = int(str(event.position_key).removeprefix("k"))
        if self.machine.campaign is None and event.position_key in self._closing_slots:
            # The daily campaign decision already made the machine flat and
            # queued this unit for closure. The venue got there first (manual
            # close or liquidation), so only durable close bookkeeping remains.
            self._closing_slots.discard(event.position_key)
            if not self._closing_slots:
                self._closing_campaign_id = None
            return
        if self.machine.campaign is None:
            raise RuntimeError("externally closed breakout unit has no campaign")
        if k == 0:
            outcome = self.machine.parent_liquidated(event.observed_at, event.price_usd)
            self._recent_decisions.append(self._decision_dict(outcome))
            self._closing_campaign_id = outcome.campaign_id
            for slot in self.position_slots[1:]:
                if state.position(slot).qty_sats:
                    self._closing_slots.add(slot)
                    self._urgent_intents.append(
                        self._exit_intent(
                            slot,
                            "parent_liquidation",
                            outcome.campaign_id,
                            {"parent_trade_id": event.trade_id},
                        )
                    )
        else:
            outcome = self.machine.child_liquidated(
                k=k, ts=event.observed_at, price=event.price_usd
            )
            self._recent_decisions.append(self._decision_dict(outcome))

    def reconcile_execution_state(self, state: StrategyState) -> None:
        for slot in tuple(self._closing_slots):
            if state.position(slot).qty_sats == 0:
                self._closing_slots.discard(slot)
        if not self._closing_slots:
            self._closing_campaign_id = None

    def persistent_state(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "activation_ts": self.activation_ts.isoformat(),
            "unit_notional_usd": self.unit_notional_usd,
            "leverage": self.leverage,
            "machine": self.machine.persistent_state(),
            "recent_decisions": list(self._recent_decisions),
            "closing_campaign_id": self._closing_campaign_id,
            "closing_slots": sorted(self._closing_slots),
        }

    def restore_persistent_state(self, snapshot: dict[str, Any]) -> bool:
        if snapshot.get("version") != self.VERSION:
            return False
        if float(snapshot.get("unit_notional_usd", -1)) != self.unit_notional_usd:
            return False
        if float(snapshot.get("leverage", -1)) != self.leverage:
            return False
        self.activation_ts = datetime.fromisoformat(snapshot["activation_ts"]).astimezone(UTC)
        self.machine = CloseRangeMachine.restore(snapshot["machine"])
        self._recent_decisions = deque(snapshot.get("recent_decisions", []), maxlen=256)
        self._closing_campaign_id = snapshot.get("closing_campaign_id")
        self._closing_slots = set(snapshot.get("closing_slots", []))
        return self._closing_slots <= set(self.position_slots)

    @staticmethod
    def _exit_intent(
        slot: str,
        reason: str,
        campaign_id: str | None,
        metadata: dict[str, Any] | None = None,
        *,
        trigger_tf: str = "1m",
    ) -> OrderIntent:
        return OrderIntent(
            kind=SignalKind.EXIT,
            trigger_tf=trigger_tf,
            position_key=slot,
            reason=reason,
            metadata={
                "campaign_id": campaign_id,
                "k": int(slot[1:]),
                **(metadata or {}),
            },
        )

    @staticmethod
    def _decision_dict(value: BreakoutDecision) -> dict[str, Any]:
        return {
            "ts": value.ts.isoformat(),
            "kind": value.kind,
            "reason": value.reason,
            "campaign_id": value.campaign_id,
            "k": value.k,
            "side": value.side,
            "price": value.price,
            "metadata": value.metadata,
        }


__all__ = ["CloseRangeLive"]


def load_seed_machine(daily_path: Path, campaign_path: Path) -> CloseRangeMachine:
    """Build the first live state from immutable LN Markets daily evidence."""
    seed = json.loads(campaign_path.read_text())
    if seed.get("source") != "lnmarkets_btcusd_daily":
        raise ValueError("breakout seed must identify LN Markets daily candles")
    as_of = pd.Timestamp(seed["as_of_close"])
    as_of = as_of.tz_localize("UTC") if as_of.tzinfo is None else as_of.tz_convert("UTC")
    frame = pd.read_parquet(daily_path).sort_values("ts")
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame = frame[frame.ts <= as_of]
    if frame.empty or frame.iloc[-1].ts != as_of:
        raise ValueError("LN Markets breakout seed candle is absent")
    if not frame.ts.diff().dropna().eq(pd.Timedelta(days=1)).all():
        raise ValueError("LN Markets breakout seed history is not contiguous")
    # Six historical venue rows miss their open by 0.5-$8 at a quoted high or
    # low. Preserve open/close and minimally expand the range so the durable
    # production invariant remains true.
    frame["high"] = frame[["open", "high", "close"]].max(axis=1)
    frame["low"] = frame[["open", "low", "close"]].min(axis=1)
    machine = CloseRangeMachine()
    machine.warmup(
        [
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close)
            for row in frame.itertuples(index=False)
        ]
    )
    campaign = seed.get("active_hypothetical_stack")
    if campaign:
        machine.seed_campaign(campaign)
    return machine


__all__.append("load_seed_machine")
