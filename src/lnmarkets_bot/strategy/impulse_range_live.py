"""Live Strategy adapter for the pure impulse-range machine.

Bar protocol (MultiTimeframeDataSource): each completed 1m bar (ts = minute
start) is followed, at a boundary, by the higher bars it completes, largest
first (1d then 4h, ts = bucket end). The adapter maps this onto the machine:

- 1d bar  -> ``observe_daily`` for the completed day;
- 4h bar  -> ``close_bar`` for the completed 4h bar, then ``open_bar`` for the
  next one, using the completed bar's close as the executable open;
- 1m bar  -> ``minute_signal``. A signal on the last minute of a 4h bar is
  carried past that bar's close and next open, and dropped if the channel
  stopped trading or the position changed, exactly as in the validated replay.

Modes:

- ``shadow``: order-incapable. Signals fill a paper book at the next minute's
  open with the research cost model (fees and slippage, no funding). Each paper
  fill is recorded as a NOOP intent for the audit trail.
- ``funded``: market entries and exits for one owned slot (``r0``) through the
  shared risk guard. Exits reduce exposure and are retried by the executor;
  the machine is flat as soon as an exit is requested.

Entries are never taken on warmup (replayed) bars. Exits that became due
while the process was down are issued on the first live bar. A gap in daily
or 4h evidence marks the model incomplete: entries stop until the state is
rebuilt, while owned exits continue.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import pandas as pd  # type: ignore[import-untyped]

from .base import Bar, Strategy, StrategyState
from .impulse_range import (
    DAY,
    H4,
    MINUTE,
    Candle,
    Event,
    ImpulseRangeMachine,
    ImpulseRangeParams,
)
from .intents import OrderIntent, Side, SignalKind

if TYPE_CHECKING:
    from pathlib import Path

SLOT = "r0"
FEE = 0.001
SLIP = 0.0005
# Parameters that shape range construction; changing them needs a rebuild.
_CONSTRUCTION = ("pullback", "tolerance", "max_age_days", "zone", "max_width", "taper")


class ImpulseRangeLive(Strategy):
    tfs = ("1d", "4h")
    position_slots = (SLOT,)
    VERSION: ClassVar[int] = 2
    MODES: ClassVar[frozenset[str]] = frozenset({"shadow", "funded"})
    MAX_EVENTS: ClassVar[int] = 200
    MAX_PAPER_TRADES: ClassVar[int] = 500

    def __init__(
        self,
        params: dict[str, Any] | None = None,
        *,
        machine: ImpulseRangeMachine | None = None,
    ) -> None:
        super().__init__(params)
        self.mode = str(self.params.get("mode", "shadow"))
        if self.mode not in self.MODES:
            raise ValueError(f"unsupported impulse-range mode: {self.mode!r}")
        # False keeps managing an owned position without admitting new entries.
        self.entries_enabled = bool(self.params.get("entries_enabled", True))
        self.unit_notional_usd = float(self.params.get("unit_notional_usd", 100.0))
        self.leverage = float(self.params.get("leverage", 2.0))
        if not all(math.isfinite(v) and v > 0 for v in (self.unit_notional_usd, self.leverage)):
            raise ValueError("impulse-range size and leverage must be positive")
        self.machine_params = ImpulseRangeParams(
            direction_mode=str(self.params.get("direction_mode", "both")),
            chop_filter=bool(self.params.get("chop_filter", True)),
            chop_threshold=float(self.params.get("chop_threshold", 0.22)),
        )
        self.range_machine = machine or ImpulseRangeMachine(self.machine_params)
        self._adopt_policy(self.range_machine)
        self.model_complete = True
        self.incomplete_reason: str | None = None
        self.last_minute_ts: datetime | None = None
        self.events: deque[dict[str, Any]] = deque(maxlen=self.MAX_EVENTS)
        # A signal on the last minute of a 4h bar: (kind, side, mult, minute_ts).
        self._carried: tuple[str, int, float, str] | None = None
        self._entry_blocked_bar: datetime | None = None
        self._inflight_entry: dict[str, Any] | None = None
        self._closing = False
        self._urgent_exit: str | None = None
        self._close_trade_id: str | None = None
        self._closing_since: datetime | None = None
        # Shadow paper book.
        self._paper_pending: dict[str, Any] | None = None
        self.paper_position: dict[str, Any] | None = None
        self.paper_trades: deque[dict[str, Any]] = deque(maxlen=self.MAX_PAPER_TRADES)
        self.paper_totals = {"trades": 0, "net_pct_sum": 0.0, "wins": 0}

    # ------------------------------------------------------------------ lifecycle
    def on_startup(self, state: StrategyState) -> None:
        owned = state.position(SLOT).qty_sats != 0
        if self.mode == "shadow":
            if owned:
                # A funded position left from an earlier funded run: unwind it.
                self._closing = True
                self._urgent_exit = "mode_shadow_unwind"
            return
        if self.paper_position is not None or self._paper_pending is not None:
            # Switching from shadow to funded: paper exposure is not owned.
            self.paper_position = None
            self._paper_pending = None
            self.range_machine.position = None
            self._log(None, "paper_position_dropped", {"reason": "mode_funded"})
        if owned and (self._closing or self.range_machine.position is None):
            self._closing = True
            self._urgent_exit = self._urgent_exit or (
                "resume_owned_close" if self._closing_since else "unmodelled_owned_position"
            )
            self._close_trade_id = state.position(SLOT).trade_id
        elif not owned and self.range_machine.position is not None:
            self.range_machine.position = None
            self._log(None, "machine_position_cleared", {"reason": "venue_flat_at_startup"})

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        ts = bar.ts.astimezone(UTC)
        intents: list[OrderIntent] = []
        if (
            not bar.warmup
            and bar.timeframe == "1m"
            and self._urgent_exit is not None
            and state.position(SLOT).qty_sats
        ):
            intents.append(self._exit_intent(self._urgent_exit, bar.timeframe))
        if bar.timeframe == "1d":
            self._on_daily(ts, bar)
        elif bar.timeframe == "4h":
            intents += self._on_4h(ts, bar, state)
        elif bar.timeframe == "1m":
            intents += self._on_minute(ts, bar, state)
        # Replayed bars rebuild state only; nothing they produce is submitted.
        return [] if bar.warmup else intents

    # ------------------------------------------------------------------ bars
    def _on_daily(self, ts: datetime, bar: Bar) -> None:
        day = ts - DAY
        last = self.range_machine.detector.last_ts
        if last is not None and day <= last:
            return
        if not bar.complete:
            self._incomplete(ts, "incomplete daily candle")
            return
        try:
            events = self.range_machine.observe_daily(_candle(day, bar))
        except ValueError as exc:
            self._incomplete(ts, f"daily evidence gap: {exc}")
            return
        self._record(events)

    def _on_4h(self, ts: datetime, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        m = self.range_machine
        start = ts - H4
        if m.bar_ts is not None and ts <= m.bar_ts:
            return []  # already processed before a restart
        if not bar.complete:
            self._incomplete(ts, "incomplete 4h candle")
            return []
        had_position = m.position is not None
        events: list[Event] = []
        if m.bar_ts is not None:
            if start != m.bar_ts:
                self._incomplete(ts, f"4h evidence gap after {m.bar_ts.isoformat()}")
            else:
                events += m.close_bar(_candle(start, bar))
        events += m.open_bar(ts, bar.close)
        self._record(events)
        intents: list[OrderIntent] = []
        exits = [e for e in events if e.kind == "exit"]
        if exits:
            reason = exits[-1].detail["reason"]
            if self.mode == "funded":
                if had_position:
                    intents += self._funded_exit(reason, bar, state)
            elif self.paper_position is not None:
                # The replay fills these at the first minute of the new bar.
                self._paper_pending = {"kind": "exit", "reason": reason}
        carried, self._carried = self._carried, None
        if carried is not None and not bar.warmup:
            intents += self._act(
                carried[0], carried[1], carried[2], datetime.fromisoformat(carried[3]), bar, state
            )
        return intents

    def _on_minute(self, ts: datetime, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        m = self.range_machine
        if self.last_minute_ts is not None and ts <= self.last_minute_ts:
            return []
        self.last_minute_ts = ts
        minute = _candle(ts, bar)
        intents: list[OrderIntent] = []
        if self.mode == "shadow" and self._paper_pending is not None:
            # As in the replay, the minute in which an order was due is not
            # evaluated for a new signal.
            intents += self._paper_fill(minute, replayed=bar.warmup)
            return intents
        # Frozen reconstruction must not suppress an already-owned target or
        # clock-based expiry. These use the last verified channel, never partial
        # candles or reconstructed entry eligibility.
        lv = m.levels()
        if (
            not self.model_complete
            and self.mode == "funded"
            and m.position is not None
            and state.position(SLOT).qty_sats
            and lv is not None
        ):
            ch = m.channel
            reason = None
            if ch and ts >= ch.confirmed_ts + DAY * m.p.max_age_days:
                reason = "expired"
            elif ts != m.position.fill_minute and (
                bar.close >= lv.mid if m.position.side == 1 else bar.close <= lv.mid
            ):
                reason = "target"
            if reason:
                m.record_target_exit(fill_minute=ts + MINUTE)
                return intents + self._funded_exit(reason, bar, state, signal_ts=ts)
        if m.bar_ts is None or not m.bar_ts <= ts < m.bar_ts + H4:
            return intents
        signal = m.minute_signal(minute)
        if signal is None:
            return intents
        lv = m.levels()
        assert lv is not None
        side = 0 if signal == "exit" else (1 if signal == "long" else -1)
        if ts + MINUTE == m.bar_ts + H4:
            self._carried = (signal, side, lv.size_multiplier, ts.isoformat())
            return intents
        return intents + self._act(signal, side, lv.size_multiplier, ts, bar, state)

    # ------------------------------------------------------------------ actions
    def _act(
        self,
        signal: str,
        side: int,
        mult: float,
        signal_ts: datetime,
        bar: Bar,
        state: StrategyState,
    ) -> list[OrderIntent]:
        m = self.range_machine
        lv = m.levels()
        if lv is None:
            return []
        fill_minute = signal_ts + MINUTE
        if signal == "exit":
            if m.position is None:
                return []
            if self.mode == "shadow":
                self._paper_pending = {
                    "kind": "exit",
                    "reason": "target",
                    "fill_minute": fill_minute.isoformat(),
                }
                return []
            m.record_target_exit(fill_minute=fill_minute)
            return self._funded_exit("target", bar, state, signal_ts=signal_ts)
        if m.position is not None or side not in lv.allowed:
            return []
        if self.mode == "shadow":
            self._paper_pending = {
                "kind": "entry",
                "side": side,
                "mult": mult,
                "fill_minute": fill_minute.isoformat(),
            }
            return []
        if bar.warmup or not self.entries_enabled or not self.model_complete or self._closing:
            return []
        if self._entry_blocked_bar == m.bar_ts or state.position(SLOT).qty_sats:
            return []
        size = math.floor(self.unit_notional_usd * mult)
        if size < 1:
            return []
        self._inflight_entry = {
            "side": side,
            "mult": mult,
            "fill_minute": fill_minute,
            "signal_ts": signal_ts,
            "bar_ts": m.bar_ts,
        }
        ch = m.channel
        assert ch is not None
        return [
            OrderIntent(
                kind=SignalKind.ENTRY,
                trigger_tf="1m",
                position_key=SLOT,
                side=Side.LONG if side == 1 else Side.SHORT,
                size_usd=float(size),
                leverage=self.leverage,
                reason="range_edge",
                metadata=self._range_metadata()
                | {
                    "signal_ts": signal_ts.isoformat(),
                    "size_multiplier": mult,
                    "level": lv.buy if side == 1 else lv.sell,
                    "target": lv.mid,
                },
            )
        ]

    def _funded_exit(
        self, reason: str, bar: Bar, state: StrategyState, *, signal_ts: datetime | None = None
    ) -> list[OrderIntent]:
        self._closing = True
        self._urgent_exit = reason
        self._close_trade_id = state.position(SLOT).trade_id
        self._closing_since = self._closing_since or bar.ts
        if bar.warmup:
            return []
        if not state.position(SLOT).qty_sats:
            return []
        return [self._exit_intent(reason, bar.timeframe, signal_ts)]

    def _exit_intent(
        self, reason: str, trigger_tf: str, signal_ts: datetime | None = None
    ) -> OrderIntent:
        metadata = self._range_metadata()
        metadata["close_trade_id"] = self._close_trade_id
        if signal_ts is not None:
            metadata["signal_ts"] = signal_ts.isoformat()
        return OrderIntent(
            kind=SignalKind.EXIT,
            trigger_tf=trigger_tf,
            position_key=SLOT,
            reason=reason,
            metadata=metadata,
        )

    # ------------------------------------------------------------------ shadow book
    def _paper_fill(self, minute: Candle, *, replayed: bool) -> list[OrderIntent]:
        pending, self._paper_pending = self._paper_pending, None
        assert pending is not None
        m = self.range_machine
        fill_minute = pending.get("fill_minute")
        if fill_minute is not None and datetime.fromisoformat(fill_minute) != minute.ts:
            return []  # the minute it was due in is missing; the replay drops it
        if pending["kind"] == "exit":
            pos = self.paper_position
            if pos is None:
                return []
            px = minute.open * (1 - pos["side"] * SLIP)
            if pending["reason"] == "target":
                if m.position is None:
                    return []
                m.record_target_exit(fill_minute=minute.ts)
            return [self._paper_close(pos, px, minute.ts, pending["reason"])]
        side, mult = int(pending["side"]), float(pending["mult"])
        lv = m.levels()
        if lv is None or m.position is not None or self.paper_position is not None:
            return []
        if not m.bar_ts <= minute.ts < m.bar_ts + H4:
            return []
        q = math.floor(self.unit_notional_usd * mult)
        if q < 1:
            return []
        px = minute.open * (1 + side * SLIP)
        m.record_entry(side, px, minute.ts, mult, fill_minute=minute.ts)
        ch = m.channel
        assert ch is not None
        self.paper_position = {
            "side": side,
            "q": q,
            "entry": px,
            "entry_ts": minute.ts.isoformat(),
            "mult": mult,
            "range_id": ch.id,
            "replayed": replayed,
        }
        return [
            OrderIntent.noop(
                "1m",
                reason="shadow_entry",
                metadata={"shadow": True, **self.paper_position, **self._range_metadata()},
            )
        ]

    def _paper_close(
        self, pos: dict[str, Any], px: float, ts: datetime, reason: str
    ) -> OrderIntent:
        q, entry, side = pos["q"], pos["entry"], pos["side"]
        gross = side * q * (1 / entry - 1 / px)
        net = gross - q * FEE / entry - q * FEE / px
        net_pct = net / (q / entry) * 100
        trade = {
            **pos,
            "exit": px,
            "exit_ts": ts.isoformat(),
            "reason": reason,
            "net_btc": net,
            "net_pct": net_pct,
        }
        self.paper_trades.append(trade)
        self.paper_totals["trades"] += 1
        self.paper_totals["net_pct_sum"] += net_pct
        self.paper_totals["wins"] += int(net_pct > 0)
        self.paper_position = None
        return OrderIntent.noop("1m", reason="shadow_exit", metadata={"shadow": True, **trade})

    # ------------------------------------------------------------------ executor feedback
    def on_order_result(self, intent: OrderIntent, decision: Any, state: StrategyState) -> None:
        if intent.kind == SignalKind.EXIT:
            return  # failed closes are retried by the executor; stay "closing"
        if intent.kind != SignalKind.ENTRY:
            return
        inflight, self._inflight_entry = self._inflight_entry, None
        if not decision.order_id or decision.order_id <= 0:
            self._entry_blocked_bar = self.range_machine.bar_ts
            self._log(None, "entry_not_filled", {"reason": decision.detail.get("reason")})
            return
        price = decision.detail.get("price_usd")
        if price is None or inflight is None:
            raise RuntimeError("confirmed impulse-range entry has no fill record")
        try:
            self.range_machine.record_entry(
                inflight["side"],
                float(price),
                inflight["fill_minute"],
                inflight["mult"],
                fill_minute=inflight["fill_minute"],
            )
        except RuntimeError:
            # The range changed under an accepted order: unwind it.
            self._closing, self._urgent_exit = True, "entry_no_longer_admissible"

    def on_intent_rejected(self, intent: OrderIntent) -> None:
        if intent.kind == SignalKind.ENTRY:
            self._entry_blocked_bar = self.range_machine.bar_ts

    def on_external_position_closed(self, event: Any, state: StrategyState) -> None:
        self.range_machine.position_closed_externally()
        self._closing = False
        self._log(
            getattr(event, "observed_at", None),
            "external_close",
            {"reason": getattr(event, "reason", None)},
        )

    def reconcile_execution_state(self, state: StrategyState) -> None:
        owned = state.position(SLOT).qty_sats != 0
        if self._closing and not owned:
            self._closing = False
            self._urgent_exit = None
            self._close_trade_id = None
            self._closing_since = None
        if (
            self.mode == "funded"
            and self.range_machine.position is not None
            and not owned
            and self._inflight_entry is None
        ):
            self.range_machine.position = None
            self._log(None, "machine_position_cleared", {"reason": "venue_flat"})

    # ------------------------------------------------------------------ persistence
    def persistent_state(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "mode": self.mode,
            "entries_enabled": self.entries_enabled,
            "machine": self.range_machine.persistent_state(),
            "model_complete": self.model_complete,
            "incomplete_reason": self.incomplete_reason,
            "last_minute_ts": _iso(self.last_minute_ts),
            "carried": list(self._carried) if self._carried else None,
            "entry_blocked_bar": _iso(self._entry_blocked_bar),
            "closing": self._closing,
            "urgent_exit": self._urgent_exit,
            "close_trade_id": self._close_trade_id,
            "closing_since": _iso(self._closing_since),
            "paper_pending": self._paper_pending,
            "paper_position": self.paper_position,
            "paper_trades": list(self.paper_trades),
            "paper_totals": self.paper_totals,
            "events": list(self.events),
        }

    def restore_persistent_state(self, snapshot: dict[str, Any]) -> bool:
        if snapshot.get("version") not in (1, self.VERSION):
            return False
        machine = ImpulseRangeMachine.restore(snapshot["machine"])
        self.range_machine = machine
        self.model_complete = bool(snapshot["model_complete"])
        self.incomplete_reason = snapshot["incomplete_reason"]
        self.last_minute_ts = _ts(snapshot["last_minute_ts"])
        carried = snapshot["carried"]
        self._carried = tuple(carried) if carried else None  # type: ignore[assignment]
        self._entry_blocked_bar = _ts(snapshot["entry_blocked_bar"])
        self._closing = bool(snapshot["closing"])
        self._urgent_exit = snapshot["urgent_exit"]
        self._close_trade_id = snapshot.get("close_trade_id")
        self._closing_since = _ts(snapshot.get("closing_since"))
        self._paper_pending = snapshot["paper_pending"]
        self.paper_position = snapshot["paper_position"]
        self.paper_trades = deque(snapshot["paper_trades"], maxlen=self.MAX_PAPER_TRADES)
        self.paper_totals = dict(snapshot["paper_totals"])
        self.events = deque(snapshot["events"], maxlen=self.MAX_EVENTS)
        self._adopt_policy(machine)
        if snapshot.get("version") == 1:
            self._log(None, "state_migrated", {"from": 1, "to": self.VERSION, "width_rule": 2})
        if snapshot["mode"] != self.mode:
            self._log(None, "mode_changed", {"from": snapshot["mode"], "to": self.mode})
        return True

    # ------------------------------------------------------------------ helpers
    def _adopt_policy(self, machine: ImpulseRangeMachine) -> None:
        """Apply configured trading policy; range construction must not change."""
        saved, wanted = machine.p, self.machine_params
        if any(getattr(saved, k) != getattr(wanted, k) for k in _CONSTRUCTION):
            raise ValueError("impulse-range construction parameters differ from saved state")
        if saved != wanted:
            channel = machine.channel
            old_eligibility = channel.tradeable if channel else None
            machine.p = replace(
                saved,
                direction_mode=wanted.direction_mode,
                chop_filter=wanted.chop_filter,
                chop_threshold=wanted.chop_threshold,
            )
            if channel is not None and channel.er_checked:
                channel.tradeable = (
                    not wanted.chop_filter
                    or (
                        channel.er_at_confirm is not None
                        and channel.er_at_confirm >= wanted.chop_threshold
                    )
                ) and channel.hi / channel.lo - 1 <= wanted.max_width + 1e-12
            # __init__ adopts policy before its event buffer exists.
            if hasattr(self, "events"):
                self._log(
                    None,
                    "policy_changed",
                    {
                        "old": asdict(saved),
                        "new": asdict(machine.p),
                        "old_tradeable": old_eligibility,
                        "tradeable": channel.tradeable if channel else None,
                    },
                )

    def mark_evidence_incomplete(self, ts: datetime) -> None:
        self._incomplete(ts, "market evidence incomplete; verified reconstruction required")

    def _incomplete(self, ts: datetime, reason: str) -> None:
        if self.model_complete:
            self._log(ts, "model_incomplete", {"reason": reason})
            self.incomplete_reason = reason
        self.model_complete = False

    def _record(self, events: list[Event]) -> None:
        for e in events:
            self._log(e.ts, e.kind, e.detail)

    def _log(self, ts: datetime | None, kind: str, detail: dict[str, Any]) -> None:
        self.events.append({"ts": _iso(ts), "kind": kind, "detail": detail})

    def _range_metadata(self) -> dict[str, Any]:
        ch = self.range_machine.channel
        if ch is None:
            return {"range_id": None}
        return {
            "range_id": ch.id,
            "channel_lo": ch.lo,
            "channel_hi": ch.hi,
            "er_at_confirm": ch.er_at_confirm,
            "redraws": ch.redraws,
        }

    def status(self) -> dict[str, Any]:
        """Read-only summary for dashboards and logs."""
        m = self.range_machine
        lv = m.levels()
        ch = m.channel
        return {
            "mode": self.mode,
            "entries_enabled": self.entries_enabled,
            "state": m.state,
            "model_complete": self.model_complete,
            "incomplete_reason": self.incomplete_reason,
            "channel": None if ch is None else asdict(ch) | {"mid": ch.mid},
            "levels": None if lv is None else asdict(lv) | {"allowed": sorted(lv.allowed)},
            "position": None if m.position is None else asdict(m.position),
            "paper_totals": self.paper_totals,
        }


def load_cold_machine(
    daily_path: Path, params: ImpulseRangeParams, *, through_day: datetime
) -> ImpulseRangeMachine:
    """Warm the impulse detector from a daily seed file up to `through_day` inclusive.

    The live feed's own warmup then continues from the next day, so its 4h
    history rebuilds recent range state. The seed must reach `through_day`.
    """
    frame = pd.read_parquet(daily_path).sort_values("ts")
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame = frame[frame.ts <= pd.Timestamp(through_day)]
    if frame.empty or frame.ts.iloc[-1].to_pydatetime() != through_day:
        raise ValueError(
            f"daily seed {daily_path} does not reach {through_day.date()}; "
            "a newer seed is required for a cold start"
        )
    if not frame.ts.diff().dropna().eq(pd.Timedelta(days=1)).all():
        raise ValueError(f"daily seed {daily_path} is not contiguous")
    machine = ImpulseRangeMachine(params)
    for row in frame.itertuples(index=False):
        machine.observe_daily(_candle(row.ts.to_pydatetime(), row))
    # Seed history only warms indicators; never act on its last signal.
    machine.pending_impulse = None
    return machine


def _candle(ts: datetime, bar: Any) -> Candle:
    """Venue candles occasionally quote an open just outside their high/low
    (six LN Markets daily rows miss by $0.5-8). Keep open and close and widen
    the range minimally, as the breakout seed loader does."""
    o, c = float(bar.open), float(bar.close)
    return Candle(ts, o, max(o, float(bar.high), c), min(o, float(bar.low), c), c)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _ts(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value).astimezone(UTC)


__all__ = ["SLOT", "ImpulseRangeLive", "load_cold_machine"]
