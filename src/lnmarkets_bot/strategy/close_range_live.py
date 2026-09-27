"""Live Strategy adapter for the pure close-range state machine."""

from __future__ import annotations

import json
import math
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]

from .base import Bar, Strategy, StrategyState
from .close_range import (
    BreakoutDecision,
    CampaignState,
    CampaignUnit,
    CloseRangeMachine,
    DailyCandle,
)
from .intents import OrderIntent, Side, SignalKind

if TYPE_CHECKING:
    from pathlib import Path


class CloseRangeLive(Strategy):
    """Turn completed LN Markets daily bars into independently routed units."""

    tfs = ("1d",)
    position_slots = ("k0", "k1", "k2", "k3")
    VERSION = 1
    REVERSAL_ENTRY_WINDOW = timedelta(minutes=5)

    def __init__(
        self,
        params: dict[str, Any] | None = None,
        *,
        machine: CloseRangeMachine | None = None,
    ) -> None:
        super().__init__(params)
        self.unit_notional_usd = float(self.params.get("unit_notional_usd", 100.0))
        self.entries_enabled = bool(self.params.get("entries_enabled", True))
        self.direction_mode = str(self.params.get("direction_mode", "both"))
        if self.direction_mode not in CloseRangeMachine.DIRECTION_MODES:
            raise ValueError(f"unsupported breakout direction mode: {self.direction_mode!r}")
        self.direction_mode_changed_at: str | None = None
        # The seeded, unowned campaign is marked at the size used when it was
        # first modeled. Changing the size of future funded entries must not
        # retroactively revalue that paper history.
        self.historical_unit_notional_usd = self.unit_notional_usd
        self.leverage = float(self.params.get("leverage", 5.0))
        activation = self.params.get("activation_ts")
        self.activation_ts = (
            datetime.fromisoformat(str(activation)).astimezone(UTC)
            if activation
            else datetime.now(UTC)
        )
        if not all(
            math.isfinite(value) and value > 0 for value in (self.unit_notional_usd, self.leverage)
        ):
            raise ValueError("breakout size and leverage must be positive")
        self.machine = machine or CloseRangeMachine()
        if self.machine.campaign is not None and self.machine.campaign.origin == "live":
            self.machine.historical_model_complete = True
        self._recent_decisions: deque[dict[str, Any]] = deque(maxlen=256)
        self._urgent_intents: deque[OrderIntent] = deque()
        self._closing_campaign_id: str | None = None
        self._closing_slots: set[str] = set()
        self._aborting_slots: set[str] = set()
        self._pending_reversal: dict[str, Any] | None = None

    def on_startup(self, state: StrategyState) -> None:
        owned = [slot for slot, pos in state.positions.items() if pos.qty_sats]
        if self._pending_reversal and owned == ["k0"] and not self._closing_slots:
            campaign = self.machine.campaign
            if campaign is None or campaign.campaign_id != self._pending_reversal["campaign_id"]:
                raise RuntimeError("pending reversal does not match owned parent")
            price = state.position("k0").entry_price_usd
            if price is None:
                raise RuntimeError("owned reversal parent has no entry price")
            self.machine.confirm_owned_fill(k=0, ts=campaign.entry_ts, price=price)
            self._pending_reversal = None
            return
        campaign = self.machine.campaign
        if campaign is not None and campaign.origin == "paper":
            if "k0" in owned:
                parent = state.position("k0")
                if parent.entry_price_usd is None:
                    raise RuntimeError("owned breakout parent has no entry price")
                expected_side = "long" if campaign.side == 1 else "short"
                if parent.side != expected_side:
                    raise RuntimeError("owned breakout parent side disagrees with campaign")
                self.machine.confirm_owned_fill(
                    k=0,
                    ts=parent.entry_ts or campaign.entry_ts,
                    price=parent.entry_price_usd,
                )
            elif not owned:
                # A snapshot committed before a rejected/never-submitted order
                # must not turn a missed entry into indefinite paper occupancy.
                self.machine.campaign = None
                self.machine.pending_exit = None
        campaign = self.machine.campaign
        if campaign is not None and campaign.origin == "live":
            for unit in reversed(tuple(campaign.units[1:])):
                if unit.origin != "paper":
                    continue
                slot = f"k{unit.k}"
                position = state.position(slot)
                if position.qty_sats:
                    if position.entry_price_usd is None:
                        raise RuntimeError(f"owned breakout {slot} has no entry price")
                    expected_side = "long" if campaign.side == 1 else "short"
                    if position.side != expected_side:
                        raise RuntimeError(f"owned breakout {slot} side disagrees with campaign")
                    self.machine.confirm_owned_fill(
                        k=unit.k,
                        ts=position.entry_ts or unit.entry_ts,
                        price=position.entry_price_usd,
                    )
                else:
                    self.machine.discard_unfilled_addon(unit.k)
            self._mark_out_of_range_fills()
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
        historical = []
        if bar.timeframe == "1m":
            for decision in self.machine.observe_historical_prices(
                bar.ts, bar.open, bar.low, bar.high
            ):
                self._recent_decisions.append(self._decision_dict(decision))
                if not bar.warmup:
                    historical.append(
                        OrderIntent.noop(
                            "1m",
                            reason=decision.reason,
                            metadata={"historical": True, "campaign_id": decision.campaign_id},
                        )
                    )
        urgent = [] if bar.warmup else [*self._urgent_intents, *historical]
        if not bar.warmup:
            self._urgent_intents.clear()
            if not self.entries_enabled and self._pending_reversal:
                self._abandon_pending_reversal()
            if self._pending_reversal and not self.machine._direction_allowed(
                self.direction_mode, int(self._pending_reversal["side"])
            ):
                self._record_reversal_status(
                    bar.ts.astimezone(UTC), "reject", "reversal_direction_mode"
                )
                self._abandon_pending_reversal()
            if bar.timeframe == "1m":
                urgent.extend(
                    self._exit_intent(
                        slot,
                        "addon_fill_too_far",
                        self.machine.campaign.campaign_id if self.machine.campaign else None,
                        {"abort_addon": True, "observed_at": bar.ts.isoformat()},
                    )
                    for slot in sorted(self._aborting_slots)
                    if state.position(slot).qty_sats and slot not in self._closing_slots
                )
        if self._pending_reversal and not bar.warmup:
            entry_ts = datetime.fromisoformat(self._pending_reversal["ts"]).astimezone(UTC)
            if bar.ts.astimezone(UTC) > entry_ts + self.REVERSAL_ENTRY_WINDOW:
                self._record_reversal_status(
                    bar.ts.astimezone(UTC), "reject", "reversal_entry_expired"
                )
                self._abandon_pending_reversal()
            elif not self._closing_slots and not any(
                state.position(slot).qty_sats for slot in self.position_slots
            ):
                if self.machine.campaign is None:
                    self._restore_pending_reversal_campaign()
                if bar.timeframe == "1m":
                    return [*urgent, self._pending_reversal_intent()]
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
            direction_mode=self.direction_mode,
        )
        for decision in decisions:
            self._recent_decisions.append(self._decision_dict(decision))
        if bar.warmup:
            self._reconcile_warmup_decisions(decisions, state)
            return urgent
        return urgent + self._intents(decisions, state)

    def _reconcile_warmup_decisions(
        self, decisions: list[BreakoutDecision], state: StrategyState
    ) -> None:
        """Keep missed exits actionable without entering on replayed signals."""
        for decision in decisions:
            if decision.kind == "campaign_exit":
                owned = [slot for slot in self.position_slots if state.position(slot).qty_sats]
                if owned:
                    self._closing_campaign_id = decision.campaign_id
                    for slot in owned:
                        self._closing_slots.add(slot)
                        self._urgent_intents.append(
                            self._exit_intent(
                                slot, decision.reason, decision.campaign_id, trigger_tf="1d"
                            )
                        )
            elif decision.kind == "paper_parent":
                self._abandon_unfunded_parent(decision.campaign_id)
            elif decision.kind == "paper_addon":
                campaign = self.machine.campaign
                if campaign is not None and campaign.origin == "live" and decision.k is not None:
                    self.machine.discard_unfilled_addon(decision.k)

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
                "direction_mode": self.direction_mode,
            }
            if decision.kind in {"paper_parent", "paper_addon"} and decision.k is not None:
                if self._aborting_slots:
                    if decision.kind == "paper_parent":
                        self._abandon_unfunded_parent(decision.campaign_id)
                    elif (
                        self.machine.campaign is not None and self.machine.campaign.origin == "live"
                    ):
                        self.machine.discard_unfilled_addon(decision.k)
                    continue
                if (
                    not self.entries_enabled
                    or not self.machine.historical_model_complete
                    or not self.machine.historical_funding_available
                ):
                    if decision.kind == "paper_parent":
                        self._abandon_unfunded_parent(decision.campaign_id)
                    elif (
                        self.machine.campaign is not None and self.machine.campaign.origin == "live"
                    ):
                        self.machine.discard_unfilled_addon(decision.k)
                    continue
                if self._closing_slots:
                    if decision.kind == "paper_parent" and self._closing_campaign_id:
                        self._pending_reversal = self._decision_dict(decision)
                        self._record_reversal_status(
                            decision.ts, "deferred_parent", "awaiting_campaign_close"
                        )
                    # The machine models the new parent before live closes are
                    # confirmed. Keep it flat until the funded old stack is gone.
                    if decision.kind == "paper_parent":
                        self.machine.campaign = None
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
                if intent.metadata.get("abort_addon"):
                    self._finish_aborted_addon(
                        intent.position_key,
                        datetime.fromisoformat(intent.metadata["observed_at"]),
                        float(decision.detail.get("price_usd") or 0),
                    )
            return
        if intent.kind != SignalKind.ENTRY:
            return
        if not decision.order_id or decision.order_id <= 0:
            if intent.position_key == "k0":
                self._abandon_unfunded_parent(intent.metadata.get("campaign_id"))
            else:
                self.machine.discard_unfilled_addon(int(intent.position_key[1:]))
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
        if k == 0:
            self._pending_reversal = None
        else:
            self._mark_out_of_range_fills()

    def post_entry_exits(self, intent: OrderIntent, decision: Any, bar: Bar) -> list[OrderIntent]:
        """Immediately unwind a market add-on filled beyond the distance cap."""
        slot = intent.position_key
        if slot not in self._aborting_slots:
            return []
        return [
            self._exit_intent(
                slot,
                "addon_fill_too_far",
                self.machine.campaign.campaign_id if self.machine.campaign else None,
                {"abort_addon": True, "observed_at": bar.ts.isoformat()},
                trigger_tf=bar.timeframe,
            )
        ]

    def on_external_positions_closed(self, events, state: StrategyState) -> None:
        # Remove closed children before ending the parent campaign. All venue
        # positions were mirrored together, so only true survivors are queued.
        for event in sorted(events, key=lambda e: e.position_key == "k0"):
            self.on_external_position_closed(event, state)

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
            outcome = self.machine.parent_liquidated(
                event.observed_at, event.price_usd, reason=getattr(event, "reason", "liquidation")
            )
            self._recent_decisions.append(self._decision_dict(outcome))
            self._closing_campaign_id = outcome.campaign_id
            for slot in self.position_slots[1:]:
                if state.position(slot).qty_sats:
                    self._closing_slots.add(slot)
                    self._urgent_intents.append(
                        self._exit_intent(
                            slot,
                            "parent_liquidation"
                            if getattr(event, "liquidated", True)
                            else "parent_external_close",
                            outcome.campaign_id,
                            {"parent_trade_id": event.trade_id},
                        )
                    )
        else:
            outcome = self.machine.child_liquidated(
                k=k,
                ts=event.observed_at,
                price=event.price_usd,
                reason=getattr(event, "reason", "liquidation"),
            )
            self._recent_decisions.append(self._decision_dict(outcome))
            self._aborting_slots.discard(event.position_key)

    def reconcile_execution_state(self, state: StrategyState) -> None:
        for slot in tuple(self._aborting_slots):
            if state.position(slot).qty_sats == 0:
                campaign = self.machine.campaign
                k = int(slot[1:])
                if campaign is not None and any(unit.k == k for unit in campaign.units):
                    self.machine.forget_closed_child(k)
                self._aborting_slots.discard(slot)
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
            "historical_unit_notional_usd": self.historical_unit_notional_usd,
            "leverage": self.leverage,
            "direction_mode": self.direction_mode,
            "direction_mode_changed_at": self.direction_mode_changed_at,
            "machine": self.machine.persistent_state(),
            "recent_decisions": list(self._recent_decisions),
            "closing_campaign_id": self._closing_campaign_id,
            "closing_slots": sorted(self._closing_slots),
            "aborting_slots": sorted(self._aborting_slots),
            "pending_reversal": self._pending_reversal,
        }

    def restore_persistent_state(self, snapshot: dict[str, Any]) -> bool:
        if snapshot.get("version") != self.VERSION:
            return False
        try:
            previous_unit = float(snapshot["unit_notional_usd"])
            historical_unit = float(snapshot.get("historical_unit_notional_usd", previous_unit))
            previous_leverage = float(snapshot["leverage"])
        except (KeyError, TypeError, ValueError):
            return False
        if not all(
            math.isfinite(value) and value > 0 for value in (previous_unit, historical_unit)
        ):
            return False
        if previous_leverage != self.leverage:
            return False
        previous_mode = str(snapshot.get("direction_mode", "both"))
        if previous_mode not in CloseRangeMachine.DIRECTION_MODES:
            return False
        self.historical_unit_notional_usd = historical_unit
        self.activation_ts = datetime.fromisoformat(snapshot["activation_ts"]).astimezone(UTC)
        self.machine = CloseRangeMachine.restore(snapshot["machine"])
        self._recent_decisions = deque(snapshot.get("recent_decisions", []), maxlen=256)
        if previous_mode != self.direction_mode:
            changed_at = datetime.now(UTC)
            self.direction_mode_changed_at = changed_at.isoformat()
            self._recent_decisions.append(
                self._decision_dict(
                    BreakoutDecision(
                        ts=changed_at,
                        kind="control",
                        reason="direction_mode_changed",
                        campaign_id=self.machine.campaign.campaign_id
                        if self.machine.campaign
                        else None,
                        k=None,
                        side=None,
                        price=None,
                        metadata={
                            "previous_mode": previous_mode,
                            "direction_mode": self.direction_mode,
                        },
                    )
                )
            )
        else:
            self.direction_mode_changed_at = snapshot.get("direction_mode_changed_at")
        self._closing_campaign_id = snapshot.get("closing_campaign_id")
        self._closing_slots = set(snapshot.get("closing_slots", []))
        self._aborting_slots = set(snapshot.get("aborting_slots", []))
        pending = snapshot.get("pending_reversal")
        self._pending_reversal = dict(pending) if pending else None
        return self._closing_slots <= set(self.position_slots) and self._aborting_slots <= set(
            self.position_slots[1:]
        )

    def _mark_out_of_range_fills(self) -> None:
        campaign = self.machine.campaign
        if campaign is None or campaign.origin != "live":
            return
        parent_price = campaign.units[0].entry_price
        for unit in campaign.units[1:]:
            slot = f"k{unit.k}"
            if unit.origin != "live":
                continue
            # During on_order_result the executor has not yet been mirrored to
            # StrategyState, so use the confirmed unit fill itself.
            displacement = campaign.side * (unit.entry_price / parent_price - 1)
            if displacement > self.machine.MAX_ADDON_DISPLACEMENT + 1e-12:
                self._aborting_slots.add(slot)

    def _finish_aborted_addon(self, slot: str, ts: datetime, price: float) -> None:
        campaign = self.machine.campaign
        k = int(slot[1:])
        if campaign is not None and any(unit.k == k for unit in campaign.units):
            if price > 0:
                outcome = self.machine.child_closed(
                    k=k, ts=ts, price=price, reason="addon_fill_too_far"
                )
                self._recent_decisions.append(self._decision_dict(outcome))
            else:
                self.machine.forget_closed_child(k)
        self._aborting_slots.discard(slot)

    def _pending_reversal_intent(self) -> OrderIntent:
        assert self._pending_reversal is not None
        pending = self._pending_reversal
        side = Side.LONG if pending["side"] == 1 else Side.SHORT
        return OrderIntent(
            kind=SignalKind.ENTRY,
            trigger_tf="1d",
            position_key="k0",
            side=side,
            size_usd=self.unit_notional_usd,
            leverage=self.leverage,
            reason=pending["reason"],
            metadata={
                **pending["metadata"],
                "campaign_id": pending["campaign_id"],
                "k": 0,
                "decision_kind": pending["kind"],
                "deferred_reversal": True,
                "intended_entry_ts": pending["ts"],
                "direction_mode": self.direction_mode,
            },
        )

    def _restore_pending_reversal_campaign(self) -> None:
        assert self._pending_reversal is not None
        pending = self._pending_reversal
        ts = datetime.fromisoformat(pending["ts"]).astimezone(UTC)
        price = float(pending["price"])
        side = int(pending["side"])
        self.machine.campaign = CampaignState(
            campaign_id=str(pending["campaign_id"]),
            side=side,
            boundary=float(pending["metadata"]["boundary"]),
            entry_ts=ts,
            held_days=0,
            peak_favorable=0.0,
            origin="paper",
            lifetime_units=1,
            units=[CampaignUnit(k=0, entry_ts=ts, entry_price=price, origin="paper")],
        )

    def _abandon_unfunded_parent(self, campaign_id: str | None) -> None:
        campaign = self.machine.campaign
        if campaign and campaign.campaign_id == campaign_id and campaign.origin == "paper":
            self.machine.campaign = None
        if self._pending_reversal and self._pending_reversal["campaign_id"] == campaign_id:
            self._pending_reversal = None

    def _abandon_pending_reversal(self) -> None:
        if self._pending_reversal:
            self._abandon_unfunded_parent(self._pending_reversal["campaign_id"])

    def _record_reversal_status(self, ts: datetime, kind: str, reason: str) -> None:
        assert self._pending_reversal is not None
        pending = self._pending_reversal
        self._recent_decisions.append(
            self._decision_dict(
                BreakoutDecision(
                    ts=ts,
                    kind=kind,
                    reason=reason,
                    campaign_id=pending["campaign_id"],
                    k=0,
                    side=pending["side"],
                    price=None,
                    metadata={"signal_ts": pending["metadata"]["signal_ts"]},
                )
            )
        )

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
