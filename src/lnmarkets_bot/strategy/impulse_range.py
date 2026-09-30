"""Pure state machine for the impulse -> swing-channel range strategy.

After a structure-passing daily close breakout (the same candidate rule as the
close-range breakout strategy), the machine waits for a pullback, confirms a
swing channel between the impulse extreme and the pullback swing, and trades
from the channel edges back to its midpoint. A 4h close beyond an edge stops
the position out and the channel is redrawn to the new extreme once price
retraces; the range is abandoned as a trend once an expansion exceeds a width
cap.

Rules are frozen from the 2026-09-29 research (v2 central) and verified
trade-for-trade against it. Optionally, a range whose lead-in was choppy
(20-day efficiency ratio below a threshold at confirmation) is tracked but not
traded.

The machine has no exchange, persistence or order dependency. A driver feeds
completed daily candles, 4h bar opens and closes, and (live) completed 1m
bars, executes the exits and entries it asks for, and reports fills back.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any, ClassVar

DAY = timedelta(days=1)
H4 = timedelta(hours=4)
MINUTE = timedelta(minutes=1)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class ImpulseRangeParams:
    pullback: float = 0.08  # minimum retrace from the impulse extreme
    tolerance: float = 0.10  # stop distance beyond an edge, as a fraction of width
    max_age_days: int = 120  # range expiry; size tapers linearly to zero over it
    zone: float = 0.15  # entry distance inside an edge, as a fraction of width
    max_width: float = 0.40  # abandon as a trend beyond this width
    taper: bool = True
    direction_mode: str = "both"  # "both" | "long_only" | "short_only"
    chop_filter: bool = True
    chop_threshold: float = 0.22  # 20-day efficiency ratio at confirmation
    DIRECTION_MODES: ClassVar[frozenset[str]] = frozenset({"both", "long_only", "short_only"})

    def __post_init__(self) -> None:
        if self.direction_mode not in self.DIRECTION_MODES:
            raise ValueError(f"unsupported direction mode: {self.direction_mode!r}")
        for name in ("pullback", "tolerance", "zone", "max_width"):
            value = getattr(self, name)
            if not (math.isfinite(value) and value > 0):
                raise ValueError(f"{name} must be positive")
        if not 0 < self.zone < 0.5:
            raise ValueError("zone must lie inside the half-width")
        if self.max_age_days <= 0:
            raise ValueError("max_age_days must be positive")

    def allowed_sides(self) -> frozenset[int]:
        return {
            "both": frozenset({1, -1}),
            "long_only": frozenset({1}),
            "short_only": frozenset({-1}),
        }[self.direction_mode]


@dataclass(frozen=True)
class Candle:
    """A completed OHLC bar identified by its opening timestamp."""

    ts: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts", _utc(self.ts))
        for name in ("open", "high", "low", "close"):
            object.__setattr__(self, name, float(getattr(self, name)))
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("prices must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC candle")


@dataclass(frozen=True)
class Impulse:
    effective_ts: datetime  # next daily open, when the machine acts on it
    side: int
    extreme: float  # signal-day high (up) or low (down)


class ImpulseDetector:
    """Daily structure-passing close breakout (the close-range candidate rule)."""

    MIN_HISTORY: ClassVar[int] = 120
    DISTANCE_MIN: ClassVar[float] = 1.5
    OVERLAP_MAX: ClassVar[float] = 0.55
    ER_LOOKBACK: ClassVar[int] = 20

    def __init__(self) -> None:
        self.count = 0
        self.last_ts: datetime | None = None
        self.closes: deque[float] = deque(maxlen=self.ER_LOOKBACK + 1)
        self.candles: deque[Candle] = deque(maxlen=10)
        self.true_ranges: deque[float] = deque(maxlen=14)
        self.ema20: float | None = None
        self.er: deque[tuple[datetime, float | None]] = deque(maxlen=8)

    def observe(self, candle: Candle) -> Impulse | None:
        if candle.ts.hour or candle.ts.minute or candle.ts.second or candle.ts.microsecond:
            raise ValueError("daily candle must start at 00:00 UTC")
        if self.last_ts is not None and candle.ts != self.last_ts + DAY:
            raise ValueError(f"daily candles must be contiguous: {self.last_ts} -> {candle.ts}")
        impulse = self._candidate(candle)
        if self.closes:
            prev = self.closes[-1]
            self.true_ranges.append(
                max(candle.high - candle.low, abs(candle.high - prev), abs(candle.low - prev))
            )
        self.ema20 = (
            candle.close if self.ema20 is None else (2 / 21 * candle.close + 19 / 21 * self.ema20)
        )
        self.closes.append(candle.close)
        self.candles.append(candle)
        self.count += 1
        self.last_ts = candle.ts
        self.er.append((candle.ts, self._efficiency()))
        return impulse

    def efficiency_for_day(self, day: datetime) -> float | None:
        """20-day efficiency ratio of a completed day; requires it to be recent."""
        for ts, value in self.er:
            if ts == day:
                return value
        raise LookupError(f"efficiency ratio for {day.date()} is not available")

    def _efficiency(self) -> float | None:
        if len(self.closes) <= self.ER_LOOKBACK:
            return None
        c = list(self.closes)
        path = sum(abs(b - a) for a, b in pairwise(c))
        return abs(c[-1] - c[0]) / path if path > 0 else None

    def _candidate(self, candle: Candle) -> Impulse | None:
        if self.count < self.MIN_HISTORY or len(self.true_ranges) < 14 or self.ema20 is None:
            return None
        previous20 = list(self.closes)[-20:]
        side = 1 if candle.close > max(previous20) else -1 if candle.close < min(previous20) else 0
        if not side:
            return None
        atr = sum(self.true_ranges) / 14
        if atr <= 0:
            return None
        overlaps = []
        for left, right in pairwise(self.candles):
            inter = max(0.0, min(left.high, right.high) - max(left.low, right.low))
            union = max(left.high, right.high) - min(left.low, right.low)
            overlaps.append(inter / union if union else 0.0)
        if side * (candle.close - self.ema20) / atr < self.DISTANCE_MIN:
            return None
        if sum(overlaps) / len(overlaps) > self.OVERLAP_MAX:
            return None
        return Impulse(candle.ts + DAY, side, candle.high if side == 1 else candle.low)

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "last_ts": _iso(self.last_ts),
            "closes": list(self.closes),
            "candles": [[c.ts.isoformat(), c.open, c.high, c.low, c.close] for c in self.candles],
            "true_ranges": list(self.true_ranges),
            "ema20": self.ema20,
            "er": [[ts.isoformat(), v] for ts, v in self.er],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ImpulseDetector:
        d = cls()
        d.count = int(data["count"])
        d.last_ts = _ts(data["last_ts"])
        d.closes.extend(float(x) for x in data["closes"])
        d.candles.extend(Candle(_ts(t), o, h, lo, c) for t, o, h, lo, c in data["candles"])
        d.true_ranges.extend(float(x) for x in data["true_ranges"])
        d.ema20 = data["ema20"]
        d.er.extend((_ts(t), v) for t, v in data["er"])
        return d


@dataclass
class Setup:
    """An impulse waiting for its pullback and confirmation."""

    side: int
    impulse_ts: datetime
    extreme: float
    pulled: bool = False
    swing: float | None = None


@dataclass
class Channel:
    id: int
    side: int
    impulse_ts: datetime
    confirmed_ts: datetime  # first 4h bar on which the channel is tradeable
    lo: float
    hi: float
    first_lo: float
    first_hi: float
    expanding: int = 0  # -1 broke below, +1 broke above, 0 intact
    new_extreme: float | None = None
    redraws: int = 0
    breaks: int = 0
    pending_stop: bool = False
    er_at_confirm: float | None = None
    er_checked: bool = False
    tradeable: bool = True

    @property
    def width(self) -> float:
        return self.hi - self.lo

    @property
    def mid(self) -> float:
        return (self.lo + self.hi) / 2


@dataclass
class Holding:
    side: int
    entry_price: float
    entry_ts: datetime
    size_multiplier: float
    fill_minute: datetime | None = None


@dataclass(frozen=True)
class Levels:
    buy: float
    sell: float
    mid: float
    stop_lo: float
    stop_hi: float
    size_multiplier: float
    allowed: frozenset[int]  # empty when the channel is not tradeable


@dataclass(frozen=True)
class Event:
    ts: datetime
    kind: str  # impulse | impulse_ignored | pullback | setup_cancel | confirm | chop_skip |
    #            break | redraw | exit | range_end
    detail: dict[str, Any] = field(default_factory=dict)


class ImpulseRangeMachine:
    """Causal range state; the caller owns execution and reports fills."""

    VERSION: ClassVar[int] = 3

    def __init__(self, params: ImpulseRangeParams | None = None) -> None:
        self.p = params or ImpulseRangeParams()
        self.detector = ImpulseDetector()
        self.pending_impulse: Impulse | None = None
        self.state = "idle"  # idle | seek | active
        self.setup: Setup | None = None
        self.channel: Channel | None = None
        self.position: Holding | None = None
        self.ended_ranges = 0
        self.bar_ts: datetime | None = None  # start of the current 4h bar
        self.last_exit_minute: datetime | None = None

    # ------------------------------------------------------------------ daily
    def observe_daily(self, candle: Candle) -> list[Event]:
        impulse = self.detector.observe(candle)
        if impulse is None:
            return []
        self.pending_impulse = impulse
        return [
            Event(
                candle.ts + DAY,
                "impulse_signal",
                {"side": impulse.side, "extreme": impulse.extreme},
            )
        ]

    # ------------------------------------------------------------------ 4h
    def open_bar(self, ts: datetime, open_price: float) -> list[Event]:
        """Actions at a 4h bar open. Exits listed here are executed at `open_price`."""
        ts = _utc(ts)
        if ts.hour % 4 or ts.minute or ts.second or ts.microsecond:
            raise ValueError("4h bars must start on a 4h UTC boundary")
        if self.bar_ts is not None and ts <= self.bar_ts:
            raise ValueError("4h bars must advance")
        self.bar_ts = ts
        p, ev = self.p, []
        impulse = self.pending_impulse
        if impulse is not None and impulse.effective_ts <= ts:
            self.pending_impulse = None
            if impulse.effective_ts < ts:
                impulse = None  # never act on a stale signal
        else:
            impulse = None
        if impulse is not None:
            if self.state == "active" and self._inside(open_price):
                ev.append(Event(ts, "impulse_ignored", {"side": impulse.side}))
            else:
                ev += self._exit(ts, open_price, "new_impulse")
                ev += self._end_range(ts, "new_impulse")
                self.state = "seek"
                self.setup = Setup(impulse.side, ts, impulse.extreme)
                ev.append(Event(ts, "impulse", {"side": impulse.side, "extreme": impulse.extreme}))
        ch = self.channel
        if self.state == "active" and ch is not None and ch.pending_stop:
            ev += self._exit(ts, open_price, "stop")
            ch.pending_stop = False
            ch.expanding = -1 if open_price < ch.mid else 1
            ch.new_extreme = open_price
            ch.breaks += 1
            ev.append(Event(ts, "break", {"id": ch.id, "dir": ch.expanding}))
        if (
            self.state == "active"
            and ch is not None
            and ts - ch.confirmed_ts >= timedelta(days=p.max_age_days)
        ):
            ev += self._exit(ts, open_price, "expiry")
            ev += self._end_range(ts, "expiry")
        ch = self.channel
        if (
            self.state == "active"
            and ch is not None
            and ts >= ch.confirmed_ts
            and not ch.er_checked
        ):
            ch.er_checked = True
            day = ch.confirmed_ts.replace(hour=0, minute=0, second=0, microsecond=0) - DAY
            try:
                ch.er_at_confirm = self.detector.efficiency_for_day(day)
            except LookupError:
                # Missing daily evidence: the filter cannot vouch for the range.
                if p.chop_filter:
                    ch.tradeable = False
                    ev.append(
                        Event(
                            ts,
                            "chop_skip",
                            {"id": ch.id, "er": None, "reason": "efficiency_unavailable"},
                        )
                    )
            else:
                if (
                    p.chop_filter
                    and ch.er_at_confirm is not None
                    and ch.er_at_confirm < p.chop_threshold
                ):
                    ch.tradeable = False
                    ev.append(Event(ts, "chop_skip", {"id": ch.id, "er": ch.er_at_confirm}))
        return ev

    def levels(self) -> Levels | None:
        """Trading levels for the current 4h bar, or None while not trading."""
        ch = self.channel
        if self.state != "active" or ch is None or self.bar_ts is None:
            return None
        if self.bar_ts < ch.confirmed_ts or ch.expanding:
            return None
        p, w = self.p, ch.width
        age = (self.bar_ts - ch.confirmed_ts) / timedelta(days=p.max_age_days)
        return Levels(
            buy=ch.lo + p.zone * w,
            sell=ch.hi - p.zone * w,
            mid=ch.mid,
            stop_lo=ch.lo - p.tolerance * w,
            stop_hi=ch.hi + p.tolerance * w,
            size_multiplier=max(0.0, 1 - age) if p.taper else 1.0,
            allowed=p.allowed_sides() if ch.tradeable else frozenset(),
        )

    def close_bar(self, bar: Candle) -> list[Event]:
        """State updates from a completed 4h bar (after any fills within it)."""
        if bar.ts != self.bar_ts:
            raise ValueError("close_bar must follow open_bar for the same 4h bar")
        p, ev = self.p, []
        if self.state == "seek":
            s = self.setup
            assert s is not None
            if not s.pulled:
                s.extreme = max(s.extreme, bar.high) if s.side == 1 else min(s.extreme, bar.low)
                if (
                    bar.low <= s.extreme * (1 - p.pullback)
                    if s.side == 1
                    else bar.high >= s.extreme * (1 + p.pullback)
                ):
                    s.pulled, s.swing = True, bar.low if s.side == 1 else bar.high
                    ev.append(Event(bar.ts, "pullback", {"extreme": s.extreme, "swing": s.swing}))
                return ev
            assert s.swing is not None
            if s.side * (bar.close - s.extreme) > 0:
                self.state, self.setup = "idle", None
                return [Event(bar.ts, "setup_cancel", {})]
            s.swing = min(s.swing, bar.low) if s.side == 1 else max(s.swing, bar.high)
            if s.side * (bar.close - s.swing) >= abs(s.extreme - s.swing) / 3:
                # The width cap applies only while a channel expands (rule 3,
                # as tested): a crash can confirm a wider first channel. Its
                # isolated margin, not the cap, bounds a loss beyond the stop.
                lo, hi = sorted((s.extreme, s.swing))
                self.channel = Channel(
                    id=self.ended_ranges,
                    side=s.side,
                    impulse_ts=s.impulse_ts,
                    confirmed_ts=bar.ts + H4,
                    lo=lo,
                    hi=hi,
                    first_lo=lo,
                    first_hi=hi,
                )
                self.state, self.setup = "active", None
                ev.append(Event(bar.ts, "confirm", {"id": self.channel.id, "lo": lo, "hi": hi}))
            return ev
        ch = self.channel
        if self.state != "active" or ch is None or bar.ts < ch.confirmed_ts:
            return ev
        if ch.expanding:
            d = ch.expanding
            assert ch.new_extreme is not None
            ch.new_extreme = (
                min(ch.new_extreme, bar.low) if d == -1 else max(ch.new_extreme, bar.high)
            )
            lo, hi = (ch.new_extreme, ch.hi) if d == -1 else (ch.lo, ch.new_extreme)
            if hi / lo - 1 > p.max_width + 1e-12:
                return self._end_range(bar.ts, "trend")
            other = ch.hi if d == -1 else ch.lo
            if d * (ch.new_extreme - bar.close) >= abs(other - ch.new_extreme) / 3:
                ch.lo, ch.hi, ch.expanding, ch.new_extreme = lo, hi, 0, None
                ch.redraws += 1
                ev.append(Event(bar.ts, "redraw", {"id": ch.id, "lo": lo, "hi": hi}))
            return ev
        lv = self.levels()
        assert lv is not None
        if bar.close < lv.stop_lo or bar.close > lv.stop_hi:
            ch.pending_stop = True
        return ev

    # ------------------------------------------------------------------ 1m (live)
    def minute_signal(self, bar: Candle) -> str | None:
        """Market-order signal from a completed 1m bar: "long", "short", "exit" or None.

        Mirrors the validated execution: act on a 1m close through the level
        and fill at the next minute. The minute in which a fill occurred is
        not evaluated.
        """
        lv = self.levels()
        if lv is None or self.bar_ts is None or not self.bar_ts <= bar.ts < self.bar_ts + H4:
            return None
        pos = self.position
        if pos is not None:
            if pos.fill_minute == bar.ts:
                return None
            return (
                "exit" if (bar.close >= lv.mid if pos.side == 1 else bar.close <= lv.mid) else None
            )
        if self.last_exit_minute == bar.ts:
            return None
        if 1 in lv.allowed and bar.close <= lv.buy:
            return "long"
        if -1 in lv.allowed and bar.close >= lv.sell:
            return "short"
        return None

    # ------------------------------------------------------------------ fills
    def record_entry(
        self,
        side: int,
        price: float,
        ts: datetime,
        size_multiplier: float,
        fill_minute: datetime | None = None,
    ) -> None:
        if self.position is not None:
            raise RuntimeError("impulse-range machine already holds a position")
        lv = self.levels()
        if lv is None or side not in lv.allowed:
            raise RuntimeError("entry is not admissible in the current range state")
        self.position = Holding(side, float(price), _utc(ts), float(size_multiplier), fill_minute)

    def record_target_exit(self, fill_minute: datetime | None = None) -> None:
        if self.position is None:
            raise RuntimeError("no position to exit")
        self.position = None
        self.last_exit_minute = fill_minute

    def position_closed_externally(self) -> None:
        """The venue closed the owned trade (e.g. liquidation); keep the range state."""
        self.position = None

    # ------------------------------------------------------------------ helpers
    def _inside(self, px: float) -> bool:
        ch = self.channel
        assert ch is not None
        if ch.expanding:
            return True
        return ch.lo - self.p.tolerance * ch.width <= px <= ch.hi + self.p.tolerance * ch.width

    def _exit(self, ts: datetime, price: float, reason: str) -> list[Event]:
        if self.position is None:
            return []
        pos, self.position = self.position, None
        return [Event(ts, "exit", {"reason": reason, "side": pos.side, "price": price})]

    def _end_range(self, ts: datetime, reason: str) -> list[Event]:
        ev = []
        if self.state == "active" and self.channel is not None:
            ch = self.channel
            ev.append(
                Event(
                    ts,
                    "range_end",
                    {
                        "id": ch.id,
                        "reason": reason,
                        "lo": ch.lo,
                        "hi": ch.hi,
                        "redraws": ch.redraws,
                    },
                )
            )
            self.ended_ranges += 1
        self.state, self.setup, self.channel = "idle", None, None
        return ev

    # ------------------------------------------------------------------ persistence
    def persistent_state(self) -> dict[str, Any]:
        def enc(obj: Any) -> Any:
            if obj is None:
                return None
            return {k: _iso(v) if isinstance(v, datetime) else v for k, v in asdict(obj).items()}

        imp = self.pending_impulse
        return {
            "version": self.VERSION,
            "params": {k: v for k, v in asdict(self.p).items()},
            "detector": self.detector.to_dict(),
            "pending_impulse": None
            if imp is None
            else [imp.effective_ts.isoformat(), imp.side, imp.extreme],
            "state": self.state,
            "setup": enc(self.setup),
            "channel": enc(self.channel),
            "position": enc(self.position),
            "ended_ranges": self.ended_ranges,
            "bar_ts": _iso(self.bar_ts),
            "last_exit_minute": _iso(self.last_exit_minute),
        }

    @classmethod
    def restore(
        cls, data: dict[str, Any], params: ImpulseRangeParams | None = None
    ) -> ImpulseRangeMachine:
        # Version 2 cancelled setups wider than the cap at confirmation;
        # its surviving states are valid under the tested rule 3 unchanged.
        if data.get("version") not in (1, 2, cls.VERSION):
            raise ValueError("unsupported impulse-range state version")
        saved = ImpulseRangeParams(**data["params"])
        if params is not None and params != saved:
            raise ValueError("impulse-range parameters differ from the saved state")
        m = cls(saved)
        m.detector = ImpulseDetector.from_dict(data["detector"])
        if data["pending_impulse"] is not None:
            t, side, extreme = data["pending_impulse"]
            m.pending_impulse = Impulse(_ts(t), int(side), float(extreme))
        m.state = data["state"]
        if data["setup"] is not None:
            m.setup = Setup(**_dec(data["setup"], ("impulse_ts",)))
        if data["channel"] is not None:
            m.channel = Channel(**_dec(data["channel"], ("impulse_ts", "confirmed_ts")))
        if data["position"] is not None:
            m.position = Holding(**_dec(data["position"], ("entry_ts", "fill_minute")))
        m.ended_ranges = int(data["ended_ranges"])
        m.bar_ts = _ts(data["bar_ts"])
        m.last_exit_minute = _ts(data["last_exit_minute"])
        if (
            m.state not in {"idle", "seek", "active"}
            or (m.state == "seek") != (m.setup is not None)
            or (m.state == "active") != (m.channel is not None)
        ):
            raise ValueError("inconsistent impulse-range state")
        return m


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _ts(value: str | None) -> datetime | None:
    return None if value is None else _utc(datetime.fromisoformat(value))


def _dec(data: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {k: _ts(v) if k in keys else v for k, v in data.items()}


__all__ = [
    "Candle",
    "Channel",
    "Event",
    "Holding",
    "Impulse",
    "ImpulseDetector",
    "ImpulseRangeMachine",
    "ImpulseRangeParams",
    "Levels",
]
