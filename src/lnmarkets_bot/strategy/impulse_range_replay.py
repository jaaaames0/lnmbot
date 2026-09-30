"""Deterministic replays of the impulse-range machine with the research cost model.

Two execution models follow the 2026-09-29 research timing and cost model.
Rule version 2 enforces the initial width cap, changing historical trade
membership; the dated original research parity applies to version 1 only.

- `replay_4h`: limit-style fills at the level on 4h bars, no same-bar round trip
  (the research reference).
- `replay_1m`: market orders on the next minute after a 1m close through the
  level (the live execution model).

Costs: 0.1% fee per fill, 5bp slippage on market fills, inverse P&L on whole
USD contracts, funding at the bar boundary. Results are per trade; there is no
wallet or margin model.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_left
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .impulse_range import DAY, H4, Candle, Event, ImpulseRangeMachine, ImpulseRangeParams

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

FEE = 0.001
SLIP = 0.0005


@dataclass
class ReplayTrade:
    range_id: int
    side: int
    q: int
    entry_ts: datetime
    entry: float
    size_multiplier: float
    fees_btc: float
    funding_btc: float = 0.0
    exit_ts: datetime | None = None
    exit: float | None = None
    reason: str | None = None
    net_btc: float | None = None

    @property
    def net_pct(self) -> float:
        assert self.net_btc is not None
        return self.net_btc / (self.q / self.entry) * 100


@dataclass
class ReplayResult:
    trades: list[ReplayTrade] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)

    @property
    def net_pct_sum(self) -> float:
        return sum(t.net_pct for t in self.trades)


class _Book:
    def __init__(self, unit_usd: float) -> None:
        self.unit_usd = unit_usd
        self.pos: ReplayTrade | None = None
        self.result = ReplayResult()

    def open(
        self,
        m: ImpulseRangeMachine,
        side: int,
        px: float,
        ts: datetime,
        mult: float,
        fill_minute: datetime | None = None,
    ) -> None:
        q = math.floor(self.unit_usd * mult)
        if q < 1:
            return
        assert m.channel is not None
        m.record_entry(side, px, ts, mult, fill_minute)
        self.pos = ReplayTrade(m.channel.id, side, q, ts, px, mult, fees_btc=q * FEE / px)

    def close(self, px: float, ts: datetime, reason: str, *, slip: bool) -> None:
        t = self.pos
        assert t is not None
        if slip:
            px *= 1 - t.side * SLIP
        gross = t.side * t.q * (1 / t.entry - 1 / px)
        t.fees_btc += t.q * FEE / px
        t.exit_ts, t.exit, t.reason = ts, px, reason
        t.net_btc = gross - t.fees_btc + t.funding_btc
        self.result.trades.append(t)
        self.pos = None

    def fund(
        self, bar: Candle, funding: Mapping[datetime, tuple[float, float]] | None, synthetic: float
    ) -> None:
        t = self.pos
        if t is None:
            return
        if funding is not None and bar.ts in funding:
            rate, fixing = funding[bar.ts]
            t.funding_btc -= t.side * t.q * rate / fixing
        elif synthetic and bar.ts.hour % 8 == 0:
            t.funding_btc -= t.q * synthetic / bar.open


def _roundtrip(m: ImpulseRangeMachine) -> ImpulseRangeMachine:
    """Serialize and restore through JSON (tests that persistence is lossless)."""
    return ImpulseRangeMachine.restore(json.loads(json.dumps(m.persistent_state())), m.p)


def _feed_daily(
    m: ImpulseRangeMachine, daily: Sequence[Candle], k: int, ts: datetime, events: list[Event]
) -> int:
    while k < len(daily) and daily[k].ts + DAY <= ts:
        events += m.observe_daily(daily[k])
        k += 1
    return k


def replay_4h(
    bars: Sequence[Candle],
    daily: Sequence[Candle],
    params: ImpulseRangeParams | None = None,
    *,
    start: datetime,
    end: datetime,
    unit_usd: float,
    funding: Mapping[datetime, tuple[float, float]] | None = None,
    synthetic_funding: float = 0.0,
    roundtrip_state: bool = False,
) -> ReplayResult:
    """Research reference: limit fills at the level on 4h bars."""
    m = ImpulseRangeMachine(params)
    book = _Book(unit_usd)
    ev = book.result.events
    k = 0
    last: Candle | None = None
    for bar in bars:
        if bar.ts >= end:
            break
        last = bar
        if roundtrip_state:
            m = _roundtrip(m)
        k = _feed_daily(m, daily, k, bar.ts, ev)
        book.fund(bar, funding, synthetic_funding)
        opened = m.open_bar(bar.ts, bar.open)
        ev += opened
        for e in opened:
            if e.kind == "exit" and book.pos is not None:
                book.close(bar.open, bar.ts, e.detail["reason"], slip=True)
        lv = m.levels()
        if lv is not None and bar.ts >= start:
            if book.pos is not None:
                t = book.pos
                if t.entry_ts < bar.ts and (
                    bar.high >= lv.mid if t.side == 1 else bar.low <= lv.mid
                ):
                    px = max(bar.open, lv.mid) if t.side == 1 else min(bar.open, lv.mid)
                    book.close(px, bar.ts, "target", slip=False)
                    m.record_target_exit()
            else:
                hit_b = 1 in lv.allowed and bar.low <= lv.buy
                hit_s = -1 in lv.allowed and bar.high >= lv.sell
                if hit_b and hit_s:
                    hit_b = abs(bar.open - lv.buy) <= abs(bar.open - lv.sell)
                    hit_s = not hit_b
                if hit_b:
                    book.open(m, 1, min(bar.open, lv.buy), bar.ts, lv.size_multiplier)
                elif hit_s:
                    book.open(m, -1, max(bar.open, lv.sell), bar.ts, lv.size_multiplier)
        ev += m.close_bar(bar)
    if book.pos is not None and last is not None:
        book.close(last.close, end, "terminal", slip=True)
    return book.result


def replay_1m(
    bars: Sequence[Candle],
    daily: Sequence[Candle],
    minutes: Sequence[Candle],
    params: ImpulseRangeParams | None = None,
    *,
    start: datetime,
    end: datetime,
    unit_usd: float,
    funding: Mapping[datetime, tuple[float, float]] | None = None,
    synthetic_funding: float = 0.0,
    roundtrip_state: bool = False,
) -> ReplayResult:
    """Live model: market order filled at the next minute's open after a 1m close signal.

    Range state, the close-based stop and impulses stay on 4h/daily bars. A
    stop fills at the first minute of the next 4h bar. A signal on the last
    minute of a 4h bar is carried to the next bar and dropped if an exit fired
    at that open or the channel stopped trading.
    """
    m = ImpulseRangeMachine(params)
    book = _Book(unit_usd)
    ev = book.result.events
    mts = [c.ts for c in minutes]
    pending: tuple[str, int, int, float] | None = None  # kind, side, minute index, mult
    k = 0
    last: Candle | None = None
    for bar in bars:
        if bar.ts >= end:
            break
        last = bar
        if roundtrip_state:
            m = _roundtrip(m)
        k = _feed_daily(m, daily, k, bar.ts, ev)
        book.fund(bar, funding, synthetic_funding)
        i0, i1 = bisect_left(mts, bar.ts), bisect_left(mts, bar.ts + H4)
        opened = m.open_bar(bar.ts, bar.open)
        ev += opened
        for e in opened:
            if e.kind == "exit" and book.pos is not None:
                if e.detail["reason"] == "stop":
                    pending = None
                    px = minutes[min(i0, len(minutes) - 1)].open
                else:
                    px = bar.open
                book.close(px, bar.ts, e.detail["reason"], slip=True)
        lv = m.levels()
        if lv is not None and bar.ts >= start:
            if pending is not None and pending[2] < i0:
                pending = None
            for j in range(i0, i1):
                minute = minutes[j]
                if pending is not None and pending[2] == j:
                    kind, side, _, mult = pending
                    pending = None
                    if kind == "exit" and book.pos is not None:
                        book.close(minute.open, minute.ts, "target", slip=True)
                        m.record_target_exit(fill_minute=minute.ts)
                    elif kind == "entry" and book.pos is not None:
                        pass
                    elif kind == "entry" and m.levels() is not None:
                        book.open(
                            m,
                            side,
                            minute.open * (1 + side * SLIP),
                            minute.ts,
                            mult,
                            fill_minute=minute.ts,
                        )
                    continue
                if pending is not None:
                    continue
                signal = m.minute_signal(minute)
                if signal == "exit":
                    pending = ("exit", book.pos.side if book.pos else 0, j + 1, lv.size_multiplier)
                elif signal in ("long", "short"):
                    pending = ("entry", 1 if signal == "long" else -1, j + 1, lv.size_multiplier)
        ev += m.close_bar(bar)
    if book.pos is not None and last is not None:
        book.close(last.close, end, "terminal", slip=True)
    return book.result


__all__ = ["FEE", "SLIP", "ReplayResult", "ReplayTrade", "replay_1m", "replay_4h"]
