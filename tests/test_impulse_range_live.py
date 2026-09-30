"""Live adapter for the impulse-range machine, driven the way the live feed orders bars."""

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.impulse_range import DAY, H4, MINUTE, ImpulseRangeParams
from lnmarkets_bot.strategy.impulse_range_live import SLOT, ImpulseRangeLive
from lnmarkets_bot.strategy.impulse_range_replay import SLIP, replay_1m
from lnmarkets_bot.strategy.intents import SignalKind
from tests.test_impulse_range import START, T0, synthetic_minutes, synthetic_path

UNIT = 100_000


@pytest.fixture(scope="module")
def market():
    bars, daily = synthetic_path(days=420)
    minutes = synthetic_minutes(bars[150 * 6 :])
    return bars, daily, minutes


def feed(bars, daily, minutes, *, warmup_before=None):
    """Yield Bars in MultiTimeframeDataSource order: 1m, then completed 1d, then 4h."""
    by_end = {d.ts + DAY: d for d in daily}
    k = 0
    for b in bars:
        end = b.ts + H4
        while k < len(minutes) and minutes[k].ts < end:
            m = minutes[k]
            w = warmup_before is not None and m.ts < warmup_before
            yield Bar(m.ts, m.open, m.high, m.low, m.close, 0.0, "1m", warmup=w)
            k += 1
        w = warmup_before is not None and end <= warmup_before
        if end in by_end:
            d = by_end[end]
            yield Bar(end, d.open, d.high, d.low, d.close, 0.0, "1d", warmup=w)
        yield Bar(end, b.open, b.high, b.low, b.close, 0.0, "4h", warmup=w)


def strategy(mode, **kw):
    s = ImpulseRangeLive({"mode": mode, "unit_notional_usd": UNIT, "chop_filter": False, **kw})
    return s


def primed(s, bars):
    s.machine.open_bar(T0, bars[0].open)
    return s


def reference(market, **params):
    bars, daily, minutes = market
    res = replay_1m(
        bars,
        daily,
        minutes,
        ImpulseRangeParams(chop_filter=False, **params),
        start=START,
        end=bars[-1].ts + H4,
        unit_usd=UNIT,
    )
    return [t for t in res.trades if t.reason != "terminal"]


class FakeVenue:
    """Fills market orders at the next minute's open with the research slippage."""

    def __init__(self, minutes):
        self.open_at = {m.ts: m.open for m in minutes}
        self.trades = []
        self.order_id = 0

    def execute(self, s, intent, state, bar):
        pos = state.position(SLOT)
        self.order_id += 1
        if intent.kind == SignalKind.ENTRY:
            side = 1 if intent.side.value == "long" else -1
            fill = bar.ts + MINUTE if bar.timeframe == "1m" else bar.ts
            px = self.open_at[fill] * (1 + side * SLIP)
            pos.side, pos.qty_sats, pos.entry_price_usd = intent.side.value, side * 10, px
            self.trades.append({"side": side, "entry_ts": fill, "entry": px})
            decision = SimpleNamespace(order_id=self.order_id, detail={"price_usd": px})
        elif intent.kind == SignalKind.EXIT:
            side = 1 if pos.qty_sats > 0 else -1
            sig = intent.metadata.get("signal_ts")
            fill = bar.ts if sig is None else (bar.ts + MINUTE if bar.timeframe == "1m" else bar.ts)
            px = self.open_at[fill] * (1 - side * SLIP)
            pos.side, pos.qty_sats, pos.entry_price_usd = None, 0, None
            self.trades[-1].update(exit_ts=fill, exit=px, reason=intent.reason)
            decision = SimpleNamespace(order_id=self.order_id, detail={"price_usd": px})
        else:
            return
        s.on_order_result(intent, decision, state)


def run_funded(s, stream, venue, *, roundtrip=False):
    state = StrategyState()
    state.positions[SLOT] = TfPosition()
    s.on_startup(state)
    for bar in stream:
        for intent in s.on_bar(bar, state):
            venue.execute(s, intent, state, bar)
        s.reconcile_execution_state(state)
        if roundtrip and bar.timeframe == "4h":
            snap = json.loads(json.dumps(s.persistent_state()))
            fresh = ImpulseRangeLive(s.params)
            assert fresh.restore_persistent_state(snap)
            s = fresh
    return s, state


def run_shadow(s, stream, *, roundtrip=False):
    state = StrategyState()
    state.positions[SLOT] = TfPosition()
    s.on_startup(state)
    noops = []
    for bar in stream:
        intents = s.on_bar(bar, state)
        assert all(i.kind == SignalKind.NOOP for i in intents)
        noops += intents
        if roundtrip and bar.timeframe in ("4h", "1m") and bar.ts.minute % 7 == 0:
            snap = json.loads(json.dumps(s.persistent_state()))
            fresh = ImpulseRangeLive(s.params)
            assert fresh.restore_persistent_state(snap)
            s = fresh
    return s, noops


@pytest.mark.parametrize("roundtrip", [False, True])
def test_shadow_book_matches_validated_minute_replay(market, roundtrip):
    bars, _daily, _minutes = market
    ref = reference(market)
    s, noops = run_shadow(primed(strategy("shadow"), bars), feed(*market), roundtrip=roundtrip)
    got = list(s.paper_trades)
    assert len(ref) > 5 and len(got) == len(ref)
    for r, t in zip(ref, got, strict=True):
        assert (t["side"], t["q"], t["reason"]) == (r.side, r.q, r.reason)
        assert (t["entry_ts"], t["exit_ts"]) == (r.entry_ts.isoformat(), r.exit_ts.isoformat())
        assert t["entry"] == pytest.approx(r.entry, rel=1e-12)
        assert t["exit"] == pytest.approx(r.exit, rel=1e-12)
        assert t["net_btc"] == pytest.approx(r.net_btc, rel=1e-9)
    assert s.paper_totals["trades"] == len(ref)
    assert sum(1 for n in noops if n.reason == "shadow_entry") >= len(ref)


@pytest.mark.parametrize("roundtrip", [False, True])
def test_funded_orders_match_validated_minute_replay(market, roundtrip):
    bars, _daily, minutes = market
    ref = reference(market)
    venue = FakeVenue(minutes)
    run_funded(primed(strategy("funded"), bars), feed(*market), venue, roundtrip=roundtrip)
    got = [t for t in venue.trades if "exit_ts" in t]
    assert len(got) == len(ref) > 5
    for r, t in zip(ref, got, strict=True):
        assert (t["side"], t["entry_ts"], t["exit_ts"], t["reason"]) == (
            r.side,
            r.entry_ts,
            r.exit_ts,
            r.reason,
        )
        assert t["entry"] == pytest.approx(r.entry) and t["exit"] == pytest.approx(r.exit)


def test_chop_filter_and_direction_mode_apply_live(market):
    bars, _daily, _minutes = market
    ref = reference(market, direction_mode="long_only", chop_threshold=0.3)
    s, _ = run_shadow(
        primed(
            ImpulseRangeLive(
                {
                    "mode": "shadow",
                    "unit_notional_usd": UNIT,
                    "direction_mode": "long_only",
                    "chop_filter": True,
                    "chop_threshold": 0.3,
                }
            ),
            bars,
        ),
        feed(*market),
    )
    skipped = {e["detail"]["id"] for e in s.events if e["kind"] == "chop_skip"}
    assert all(t["side"] == 1 for t in s.paper_trades)
    assert not {t["range_id"] for t in s.paper_trades} & skipped
    assert len(s.paper_trades) == len([t for t in ref if t.range_id not in skipped])


def test_no_entries_on_warmup_and_missed_exit_is_issued_live(market):
    bars, _daily, minutes = market
    venue = FakeVenue(minutes)
    _s, _state = run_funded(primed(strategy("funded"), bars), feed(*market), venue)
    first_entry = venue.trades[0]["entry_ts"]
    # Replaying up to just past the first entry as warmup must not enter.
    venue2 = FakeVenue(minutes)
    cut = first_entry + timedelta(hours=1)
    stream = [b for b in feed(*market) if b.ts <= cut]
    _s2, _state2 = run_funded(primed(strategy("funded"), bars), iter(stream[:-1]), venue2)
    assert venue2.trades  # the live run up to the cut entered
    venue3 = FakeVenue(minutes)
    _s3, _ = run_funded(primed(strategy("funded"), bars), feed(*market, warmup_before=cut), venue3)
    assert all(t["entry_ts"] >= cut for t in venue3.trades)


def test_exit_due_during_downtime_is_sent_on_first_live_bar(market):
    bars, _daily, minutes = market
    venue = FakeVenue(minutes)
    stream = list(feed(*market))
    s = primed(strategy("funded"), bars)
    state = StrategyState()
    state.positions[SLOT] = TfPosition()
    s.on_startup(state)
    # Run live until the first entry, then snapshot as if the process stopped.
    i = 0
    while not venue.trades:
        for intent in s.on_bar(stream[i], state):
            venue.execute(s, intent, state, stream[i])
        s.reconcile_execution_state(state)
        i += 1
    snap = json.loads(json.dumps(s.persistent_state()))
    first = venue.trades[0]
    restarted = ImpulseRangeLive(s.params)
    assert restarted.restore_persistent_state(snap)
    restarted.on_startup(state)
    # Replay as warmup until well after the reference exit would have happened.
    ref = reference(market)[0]
    resume = ref.exit_ts + timedelta(hours=8)
    exits = []
    for bar in stream[i:]:
        live = bar.ts > resume
        b = Bar(bar.ts, bar.open, bar.high, bar.low, bar.close, 0.0, bar.timeframe, warmup=not live)
        out = restarted.on_bar(b, state)
        if not live:
            assert out == []
        else:
            exits = out
            break
    assert first["entry_ts"] == ref.entry_ts
    assert [x.kind for x in exits] == [SignalKind.EXIT]
    assert exits[0].reason == ref.reason


def test_rejected_entry_blocks_rest_of_bar_only():
    s = strategy("funded")
    s.machine.bar_ts = T0
    s.on_intent_rejected(SimpleNamespace(kind=SignalKind.ENTRY))
    assert s._entry_blocked_bar == T0


def test_4h_gap_marks_model_incomplete_and_blocks_entries(market):
    bars, _daily, minutes = market
    stream = [b for b in feed(*market) if not (b.timeframe == "4h" and b.ts == START + DAY)]
    venue = FakeVenue(minutes)
    s, _ = run_funded(primed(strategy("funded"), bars), iter(stream), venue)
    assert not s.model_complete
    assert venue.trades == []
    assert s.incomplete_reason.startswith("4h evidence gap")


def test_startup_unwinds_unmodelled_or_shadow_owned_position():
    for mode, reason in (("funded", "unmodelled_owned_position"), ("shadow", "mode_shadow_unwind")):
        s = strategy(mode)
        state = StrategyState()
        state.positions[SLOT] = TfPosition(side="long", qty_sats=10)
        s.on_startup(state)
        out = s.on_bar(Bar(T0, 1, 1, 1, 1, 0.0, "1m"), state)
        assert [(i.kind, i.reason) for i in out] == [(SignalKind.EXIT, reason)]


def test_construction_change_is_refused_but_policy_change_is_adopted():
    s = strategy("shadow")
    snap = json.loads(json.dumps(s.persistent_state()))
    snap["machine"]["params"]["zone"] = 0.2
    with pytest.raises(ValueError, match="construction"):
        strategy("shadow").restore_persistent_state(snap)
    snap = json.loads(json.dumps(s.persistent_state()))
    other = ImpulseRangeLive(
        {"mode": "funded", "direction_mode": "short_only", "chop_filter": True}
    )
    assert other.restore_persistent_state(snap)
    assert other.machine.p.direction_mode == "short_only" and other.machine.p.chop_filter
    assert other.events[-1]["kind"] == "mode_changed"


def test_invalid_configuration():
    with pytest.raises(ValueError):
        ImpulseRangeLive({"mode": "live"})
    with pytest.raises(ValueError):
        ImpulseRangeLive({"leverage": 0})
