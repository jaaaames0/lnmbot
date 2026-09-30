"""Impulse-range machine: synthetic-data behaviour and research parity.

The golden fixture was produced by the 2026-09-29 research simulator on the
seeded synthetic path below (scripts/research/r2026_09_29_complement/
make_impulse_range_golden.py); the full-history parity checks live in
tests/research/r2026_09_29_impulse_range.
"""

from __future__ import annotations

import json
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lnmarkets_bot.strategy.close_range import CloseRangeMachine, DailyCandle
from lnmarkets_bot.strategy.impulse_range import (
    H4,
    Candle,
    Channel,
    ImpulseDetector,
    ImpulseRangeMachine,
    ImpulseRangeParams,
)
from lnmarkets_bot.strategy.impulse_range_replay import replay_1m, replay_4h

GOLDEN = Path(__file__).parent / "fixtures" / "impulse_range_golden.json"
T0 = datetime(2021, 1, 1, tzinfo=UTC)
START = T0 + timedelta(days=150)


def synthetic_path(seed: int = 7, days: int = 1100) -> tuple[list[Candle], list[Candle]]:
    """Seeded 4h path with alternating trend and range regimes, and its daily bars."""
    rng = random.Random(seed)
    price, bars = 20_000.0, []
    drift, regime_left = 0.0, 0
    for i in range(days * 6):
        if regime_left <= 0:
            regime_left = rng.randint(60, 360)
            drift = rng.choice([0.0, 0.0, 0.004, -0.004, 0.008, -0.008])
        regime_left -= 1
        vol = 0.012 if drift else 0.009
        close = price * math.exp(drift + rng.gauss(0, vol))
        high = max(price, close) * (1 + abs(rng.gauss(0, vol / 2)))
        low = min(price, close) * (1 - abs(rng.gauss(0, vol / 2)))
        bars.append(Candle(T0 + i * H4, price, high, low, close))
        price = close
    daily = []
    for d in range(days):
        chunk = bars[d * 6 : d * 6 + 6]
        daily.append(
            Candle(
                chunk[0].ts,
                chunk[0].open,
                max(b.high for b in chunk),
                min(b.low for b in chunk),
                chunk[-1].close,
            )
        )
    return bars, daily


def synthetic_minutes(bars: list[Candle], seed: int = 11) -> list[Candle]:
    """Deterministic 1m bars inside each 4h bar that respect its OHLC."""
    rng = random.Random(seed)
    out = []
    for b in bars:
        n = 240
        path = [b.open]
        for _ in range(n - 1):
            path.append(path[-1] * math.exp(rng.gauss(0, 0.0008)))
        path.append(b.close)
        lo_i, hi_i = sorted(rng.sample(range(1, n), 2))
        lo_val, hi_val = (b.low, b.high) if rng.random() < 0.5 else (b.high, b.low)
        path[lo_i], path[hi_i] = lo_val, hi_val
        path = [min(max(x, b.low), b.high) for x in path]
        for j in range(n):
            o, c = path[j], path[j + 1]
            out.append(Candle(b.ts + timedelta(minutes=j), o, max(o, c), min(o, c), c))
    return out


def run(params: ImpulseRangeParams, **kw):
    bars, daily = synthetic_path()
    return replay_4h(bars, daily, params, start=START, end=bars[-1].ts + H4, unit_usd=100_000, **kw)


def test_golden_parity_with_research_simulator():
    golden = json.loads(GOLDEN.read_text())
    got = run(ImpulseRangeParams(chop_filter=False), synthetic_funding=0.0001)
    assert len(got.trades) == len(golden["trades"]) > 10
    for ref, t in zip(golden["trades"], got.trades, strict=True):
        assert ref["entry_ts"] == t.entry_ts.isoformat()
        assert ref["exit_ts"] == t.exit_ts.isoformat()
        assert (ref["side"], ref["why"], ref["q"], ref["range_id"]) == (
            t.side,
            t.reason,
            t.q,
            t.range_id,
        )
        assert math.isclose(ref["entry"], t.entry, rel_tol=1e-12)
        assert math.isclose(ref["exit"], t.exit, rel_tol=1e-12)
        assert math.isclose(ref["net_btc"], t.net_btc, rel_tol=1e-9, abs_tol=1e-15)


@pytest.mark.parametrize("chop_filter", [False, True])
def test_state_roundtrip_is_lossless(chop_filter):
    params = ImpulseRangeParams(chop_filter=chop_filter)
    plain = run(params)
    restored = run(params, roundtrip_state=True)
    assert [vars(t) for t in plain.trades] == [vars(t) for t in restored.trades]
    assert plain.events == restored.events


def test_minute_replay_roundtrip_and_sanity():
    bars, daily = synthetic_path(days=500)
    minutes = synthetic_minutes(bars[150 * 6 :])
    kw = dict(start=START, end=bars[-1].ts + H4, unit_usd=100_000)
    params = ImpulseRangeParams(chop_filter=False)
    a = replay_1m(bars, daily, minutes, params, **kw)
    b = replay_1m(bars, daily, minutes, params, roundtrip_state=True, **kw)
    assert a.trades and [vars(t) for t in a.trades] == [vars(t) for t in b.trades]
    for t in a.trades:
        assert t.exit_ts > t.entry_ts
        assert t.reason in {"target", "stop", "expiry", "new_impulse", "terminal"}


def test_chop_filter_skips_choppy_ranges_only():
    unfiltered = run(ImpulseRangeParams(chop_filter=False))
    filtered = run(ImpulseRangeParams(chop_filter=True, chop_threshold=0.3))
    skipped = {e.detail["id"] for e in filtered.events if e.kind == "chop_skip"}
    assert skipped, "synthetic path should contain choppy-start ranges"
    assert all(e.detail["er"] < 0.3 for e in filtered.events if e.kind == "chop_skip")
    assert not {t.range_id for t in filtered.trades} & skipped
    kept = [t for t in unfiltered.trades if t.range_id not in skipped]
    assert [vars(t) for t in kept] == [vars(t) for t in filtered.trades]
    # Range construction itself is unaffected by the filter.
    confirms = [e for e in unfiltered.events if e.kind == "confirm"]
    assert confirms == [e for e in filtered.events if e.kind == "confirm"]


@pytest.mark.parametrize(("mode", "side"), [("long_only", 1), ("short_only", -1)])
def test_direction_mode_restricts_entries(mode, side):
    got = run(ImpulseRangeParams(chop_filter=False, direction_mode=mode))
    assert got.trades and all(t.side == side for t in got.trades)


def test_detector_matches_close_range_candidates():
    _, daily = synthetic_path()
    detector, machine = ImpulseDetector(), CloseRangeMachine()
    found = 0
    for c in daily:
        impulse = detector.observe(c)
        candidate = machine._candidate(DailyCandle(c.ts, c.open, c.high, c.low, c.close))
        machine._append_indicators(DailyCandle(c.ts, c.open, c.high, c.low, c.close))
        passing = candidate is not None and candidate.structure_pass
        assert (impulse is not None) == passing, c.ts
        if impulse is not None:
            found += 1
            assert impulse.side == candidate.side
            assert impulse.effective_ts == c.ts + timedelta(days=1)
    assert found > 5


def test_detector_rejects_gaps_and_misaligned_candles():
    d = ImpulseDetector()
    d.observe(Candle(T0, 1, 1, 1, 1))
    with pytest.raises(ValueError, match="contiguous"):
        d.observe(Candle(T0 + timedelta(days=2), 1, 1, 1, 1))
    with pytest.raises(ValueError, match="00:00"):
        ImpulseDetector().observe(Candle(T0 + H4, 1, 1, 1, 1))


def test_open_bar_requires_advancing_4h_boundaries():
    m = ImpulseRangeMachine()
    m.open_bar(T0, 100.0)
    with pytest.raises(ValueError, match="advance"):
        m.open_bar(T0, 100.0)
    with pytest.raises(ValueError, match="boundary"):
        m.open_bar(T0 + timedelta(hours=5), 100.0)


def _active_machine(**kw) -> ImpulseRangeMachine:
    m = ImpulseRangeMachine(ImpulseRangeParams(chop_filter=False, **kw))
    m.state = "active"
    m.channel = Channel(
        id=0,
        side=1,
        impulse_ts=T0,
        confirmed_ts=T0 + H4,
        lo=90.0,
        hi=110.0,
        first_lo=90.0,
        first_hi=110.0,
        er_checked=True,
    )
    m.bar_ts = T0 + H4
    return m


def _minute(offset: int, close: float) -> Candle:
    return Candle(T0 + H4 + timedelta(minutes=offset), close, close, close, close)


def test_minute_signals_follow_levels_and_skip_fill_minutes():
    m = _active_machine()
    lv = m.levels()
    assert (lv.buy, lv.sell, lv.mid) == (93.0, 107.0, 100.0)
    assert m.minute_signal(_minute(0, 95.0)) is None
    assert m.minute_signal(_minute(1, 93.0)) == "long"
    assert m.minute_signal(_minute(2, 107.5)) == "short"
    fill = _minute(2, 93.1).ts
    m.record_entry(1, 93.1, fill, lv.size_multiplier, fill_minute=fill)
    assert m.minute_signal(_minute(2, 101.0)) is None  # the fill minute is not evaluated
    assert m.minute_signal(_minute(3, 99.0)) is None
    assert m.minute_signal(_minute(4, 100.0)) == "exit"
    m.record_target_exit(fill_minute=_minute(5, 100.0).ts)
    assert m.minute_signal(_minute(5, 92.0)) is None
    assert m.minute_signal(_minute(6, 92.0)) == "long"
    assert m.minute_signal(Candle(T0 + 2 * H4, 92, 92, 92, 92)) is None  # outside the bar


def test_stop_break_redraw_and_trend_end():
    m = _active_machine()
    m.record_entry(1, 93.0, T0 + H4, 1.0)
    # A 4h close below lo - 0.1 * width = 88 arms the stop.
    assert m.close_bar(Candle(T0 + H4, 95, 96, 87, 87.5)) == []
    ev = m.open_bar(T0 + 2 * H4, 87.0)
    assert [e.kind for e in ev] == ["exit", "break"]
    assert ev[0].detail["reason"] == "stop" and m.position is None
    assert m.levels() is None and m.channel.expanding == -1
    # New low 85; a close back up by a third of (110 - 85) redraws to 85-110.
    ev = m.close_bar(Candle(T0 + 2 * H4, 87, 94, 85, 93.5))
    assert [e.kind for e in ev] == ["redraw"]
    assert (m.channel.lo, m.channel.hi, m.channel.redraws) == (85, 110, 1)
    m.open_bar(T0 + 3 * H4, 93.5)
    assert m.levels().buy == pytest.approx(85 + 0.15 * 25)
    # Break above, then extend beyond the 40% width cap: the range ends as a trend.
    m.close_bar(Candle(T0 + 3 * H4, 93.5, 114, 93, 113.5))
    m.open_bar(T0 + 4 * H4, 113.5)
    ev = m.close_bar(Candle(T0 + 4 * H4, 113.5, 120, 113, 119.5))
    assert [e.kind for e in ev] == ["range_end"] and ev[0].detail["reason"] == "trend"
    assert m.state == "idle" and m.ended_ranges == 1


def test_record_entry_rejects_inadmissible_states():
    m = _active_machine(direction_mode="long_only")
    with pytest.raises(RuntimeError):
        m.record_entry(-1, 107.0, T0 + H4, 1.0)
    m.channel.expanding = 1
    with pytest.raises(RuntimeError):
        m.record_entry(1, 93.0, T0 + H4, 1.0)


def test_restore_rejects_changed_params_and_bad_version():
    m = _active_machine()
    state = json.loads(json.dumps(m.persistent_state()))
    with pytest.raises(ValueError, match="differ"):
        ImpulseRangeMachine.restore(state, ImpulseRangeParams())
    with pytest.raises(ValueError, match="version"):
        ImpulseRangeMachine.restore({**state, "version": 99})
    again = ImpulseRangeMachine.restore(state, ImpulseRangeParams(chop_filter=False))
    assert again.levels() == m.levels()


def test_params_validation():
    with pytest.raises(ValueError):
        ImpulseRangeParams(direction_mode="sideways")
    with pytest.raises(ValueError):
        ImpulseRangeParams(zone=0.6)
