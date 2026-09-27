from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lnmarkets_bot.strategy.close_range import (
    BreakoutCandidate,
    CloseRangeMachine,
    DailyCandle,
)

ROOT = Path(__file__).resolve().parents[1]


def candle(ts: datetime, price: float, *, close: float | None = None) -> DailyCandle:
    closing = price if close is None else close
    return DailyCandle(
        ts=ts,
        open=price,
        high=max(price, closing) * 1.01,
        low=min(price, closing) * 0.99,
        close=closing,
    )


def _qualifying_candidate(ts: datetime, side: int) -> BreakoutCandidate:
    return BreakoutCandidate(
        signal_ts=ts,
        side=side,
        boundary=105.0 if side == 1 else 95.0,
        signal_close=110.0 if side == 1 else 90.0,
        ema20=100.0,
        atr14=2.0,
        average_overlap10=0.4,
        distance_ema_atr=3.0,
        structure_pass=True,
    )


@pytest.mark.parametrize("side", [1, -1])
def test_recovery_blocks_only_same_side_parent_at_exit_open(side: int) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "old",
            "side": "long" if side == 1 else "short",
            "entry_ts": (start + timedelta(days=30)).isoformat(),
            "entry_price": 100.0,
            "boundary": 90.0 if side == 1 else 110.0,
            "held_days": 90,
            "peak_favorable_pct": 0.2,
            "active_units": 1,
            "pending_exit": "recover",
        }
    )
    machine.pending_candidate = _qualifying_candidate(start + timedelta(days=119), side)
    first = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=120),
    )
    assert [(decision.kind, decision.reason) for decision in first] == [
        ("historical_exit", "recover"),
        ("reject", "recovery_same_open"),
    ]
    assert first[1].metadata["recovery_campaign_id"] == "old"
    assert machine.campaign is None

    machine.pending_candidate = _qualifying_candidate(start + timedelta(days=120), side)
    next_open = machine.advance(
        candle(start + timedelta(days=121), 100),
        activation_ts=start + timedelta(days=120),
    )
    assert next_open[0].kind == "paper_parent"
    assert machine.campaign is not None and machine.campaign.side == side


@pytest.mark.parametrize("exit_reason", ["range_close", "maximum_hold"])
def test_other_exit_reasons_keep_same_open_parent_admission(exit_reason: str) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "old",
            "side": "long",
            "entry_ts": (start + timedelta(days=30)).isoformat(),
            "entry_price": 100.0,
            "boundary": 90.0,
            "held_days": 90,
            "peak_favorable_pct": 0.2,
            "active_units": 1,
            "pending_exit": exit_reason,
        }
    )
    machine.pending_candidate = _qualifying_candidate(start + timedelta(days=119), 1)
    decisions = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=120),
    )
    assert [(decision.kind, decision.reason) for decision in decisions[:2]] == [
        ("historical_exit", exit_reason),
        ("paper_parent", "structure_parent"),
    ]


def test_recovery_exit_does_not_block_opposite_side_parent() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "old",
            "side": "long",
            "entry_ts": (start + timedelta(days=30)).isoformat(),
            "entry_price": 100.0,
            "boundary": 90.0,
            "held_days": 90,
            "peak_favorable_pct": 0.2,
            "active_units": 1,
            "pending_exit": "recover",
        }
    )
    machine.pending_candidate = _qualifying_candidate(start + timedelta(days=119), -1)
    decisions = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="both",
    )
    assert [(decision.kind, decision.reason) for decision in decisions[:2]] == [
        ("historical_exit", "recover"),
        ("paper_parent", "structure_parent"),
    ]


def test_feature_calculation_matches_frozen_research_matrix():
    pd = pytest.importorskip("pandas")
    from scripts.investigate_btc_broad_features import daily_features
    from scripts.replay_btc_close_range_native import load_data

    daily, _, _ = load_data()
    reference = daily_features(daily)
    expected = reference[(reference.index >= 120) & reference.raw_side.ne(0)].copy()
    machine = CloseRangeMachine()
    actual = {}
    activation = datetime(2100, 1, 1, tzinfo=UTC)
    for row in daily.itertuples(index=False):
        decisions = machine.advance(
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close),
            activation_ts=activation,
        )
        for decision in decisions:
            if decision.kind == "signal":
                actual[pd.Timestamp(decision.metadata["signal_ts"])] = decision

    assert set(actual) == set(expected.ts)
    for row in expected.itertuples(index=False):
        decision = actual[row.ts]
        assert decision.side == int(row.raw_side)
        assert decision.metadata["boundary"] == pytest.approx(
            row.upper if row.raw_side == 1 else row.lower
        )
        assert decision.metadata["ema20"] == pytest.approx(row.ema20, rel=1e-12)
        assert decision.metadata["atr14"] == pytest.approx(row.atr14, rel=1e-12)
        assert decision.metadata["average_overlap10"] == pytest.approx(
            row.average_overlap10, rel=1e-12
        )
        assert decision.metadata["structure_pass"] == bool(
            row.raw_side * (row.close - row.ema20) / row.atr14 >= 1.5
            and row.average_overlap10 <= 0.55
        )


def test_current_seed_retains_latest_signal_and_is_not_owned():
    pd = pytest.importorskip("pandas")
    snapshot = json.loads((ROOT / "runs/btc-close-range-shadow-latest.json").read_text())
    daily = pd.read_parquet(ROOT / "data/cache/btcusdt_perp_1d_shadow_2026-09-22.parquet")
    daily = daily[daily.ts <= pd.Timestamp(snapshot["as_of_close"])].sort_values("ts")
    machine = CloseRangeMachine()
    machine.warmup(
        [
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close)
            for row in daily.itertuples(index=False)
        ]
    )
    machine.seed_campaign(snapshot["active_hypothetical_stack"])
    assert machine.pending_candidate is not None
    assert machine.pending_candidate.signal_ts == datetime(2026, 9, 21, tzinfo=UTC)
    next_bar = candle(datetime(2026, 9, 22, tzinfo=UTC), 86_000)
    decisions = machine.advance(next_bar, activation_ts=datetime(2026, 9, 22, tzinfo=UTC))
    rejection = next(value for value in decisions if value.reason == "addon_cap")
    assert rejection.kind == "reject"
    assert machine.campaign is not None
    assert machine.campaign.origin == "historical"
    assert machine.campaign.lifetime_units == 4
    assert len(machine.campaign.units) == 1
    assert all(unit.origin == "historical" for unit in machine.campaign.units)


def test_seeded_exit_clears_at_next_open_without_creating_owned_close():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100 + i / 10) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "seed",
            "side": "long",
            "entry_ts": (start + timedelta(days=80)).isoformat(),
            "entry_price": 105.0,
            "boundary": 99.0,
            "held_days": 90,
            "peak_favorable_pct": 0.2,
            "active_units": 1,
            "pending_exit": "recover",
        }
    )
    decisions = machine.advance(
        candle(start + timedelta(days=120), 110),
        activation_ts=start + timedelta(days=120),
    )
    exit_decision = decisions[0]
    assert exit_decision.kind == "historical_exit"
    assert exit_decision.metadata == {"origin": "historical", "owned": False}


def test_ordinary_rules_apply_to_seeded_addon_without_funded_policy():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "seed",
            "side": "long",
            "entry_ts": (start + timedelta(days=100)).isoformat(),
            "entry_price": 100.0,
            "boundary": 95.0,
            "held_days": 20,
            "peak_favorable_pct": 0.1,
            "active_units": 1,
            "pending_exit": None,
        }
    )
    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=119),
        side=1,
        boundary=105,
        signal_close=106,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    decisions = machine.advance(
        candle(start + timedelta(days=120), 110),
        activation_ts=start + timedelta(days=120),
        direction_mode="short_only",
    )
    addon = decisions[0]
    assert addon.kind == "historical_addon"
    assert addon.metadata["owned"] is False
    assert machine.campaign is not None and machine.campaign.units[1].origin == "historical"


def test_direction_mode_rejects_parent_without_occupying_next_signal():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=119),
        side=-1,
        boundary=105,
        signal_close=94,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    first = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="long_only",
    )
    assert [(decision.kind, decision.reason) for decision in first] == [
        ("reject", "parent_direction_mode")
    ]
    assert first[0].metadata["direction_mode"] == "long_only"
    assert machine.campaign is None

    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=120),
        side=1,
        boundary=95,
        signal_close=106,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    second = machine.advance(
        candle(start + timedelta(days=121), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="long_only",
    )
    assert [decision.kind for decision in second] == ["paper_parent"]
    assert machine.campaign is not None and machine.campaign.side == 1


def test_direction_mode_does_not_rewrite_pre_activation_reference_parent():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=119),
        side=-1,
        boundary=105,
        signal_close=94,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    decisions = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=121),
        direction_mode="long_only",
    )
    assert [decision.kind for decision in decisions] == ["historical_parent"]
    assert machine.campaign is not None and machine.campaign.origin == "historical"


def test_direction_mode_blocks_addon_but_keeps_open_campaign_exit():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100) for i in range(120)])
    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=119),
        side=-1,
        boundary=105,
        signal_close=94,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    parent = machine.advance(
        candle(start + timedelta(days=120), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="both",
    )
    assert [decision.kind for decision in parent] == ["paper_parent"]
    machine.pending_candidate = BreakoutCandidate(
        signal_ts=start + timedelta(days=120),
        side=-1,
        boundary=105,
        signal_close=94,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    blocked = machine.advance(
        candle(start + timedelta(days=121), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="long_only",
    )
    assert [(decision.kind, decision.reason) for decision in blocked] == [
        ("reject", "addon_direction_mode")
    ]
    assert machine.campaign is not None and machine.campaign.lifetime_units == 1
    machine.pending_exit = "range_close"
    exited = machine.advance(
        candle(start + timedelta(days=122), 100),
        activation_ts=start + timedelta(days=120),
        direction_mode="long_only",
    )
    assert [(decision.kind, decision.reason) for decision in exited] == [
        ("campaign_exit", "range_close")
    ]
    assert machine.campaign is None


def test_persistent_state_round_trip_and_contiguous_days():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    machine.warmup([candle(start + timedelta(days=i), 100 + i) for i in range(120)])
    machine.seed_campaign(
        {
            "parent_id": "seed",
            "side": "short",
            "entry_ts": (start + timedelta(days=100)).isoformat(),
            "entry_price": 200.0,
            "boundary": 210.0,
            "held_days": 20,
            "peak_favorable_pct": 0.05,
            "active_units": 2,
            "pending_exit": None,
        }
    )
    restored = CloseRangeMachine.restore(json.loads(json.dumps(machine.persistent_state())))
    assert restored.persistent_state() == machine.persistent_state()
    assert restored.source_count == 120
    assert len(restored.source_digest) == 64
    with pytest.raises(ValueError, match="contiguous"):
        restored.advance(candle(start + timedelta(days=121), 220), activation_ts=start)


def test_live_completed_candle_applies_signal_at_immediate_next_open():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = CloseRangeMachine()
    history = [
        DailyCandle(
            start + timedelta(days=i),
            100 + i * 5,
            101 + i * 5,
            99 + i * 5,
            100 + i * 5,
        )
        for i in range(120)
    ]
    machine.warmup(history)
    # Force a qualifying close outside the preceding range.  The action is
    # available at the following boundary rather than one daily bar later.
    signal = DailyCandle(
        start + timedelta(days=120),
        700,
        1_010,
        699,
        1_000,
    )
    decisions = machine.complete_and_apply_next_open(
        signal,
        next_open_ts=start + timedelta(days=121),
        next_open_price=1_001,
        activation_ts=start + timedelta(days=121),
    )
    parent = next(value for value in decisions if value.kind == "paper_parent")
    assert parent.ts == start + timedelta(days=121)
    assert parent.price == pytest.approx(1_001 * 1.0005)
    assert machine.last_bar_ts == signal.ts


def test_parent_liquidation_requires_campaign_and_preserves_ownership_flag():
    machine = CloseRangeMachine()
    machine.seed_campaign(
        {
            "parent_id": "seed",
            "side": "long",
            "entry_ts": "2026-01-01T00:00:00+00:00",
            "entry_price": 100.0,
            "boundary": 90.0,
            "held_days": 1,
            "peak_favorable_pct": 0.0,
            "active_units": 1,
            "pending_exit": None,
        }
    )
    decision = machine.parent_liquidated(datetime(2026, 1, 2, tzinfo=UTC), 83.33)
    assert decision.reason == "parent_liquidation"
    assert decision.metadata["owned"] is False
    with pytest.raises(ValueError, match="no active"):
        machine.parent_liquidated(datetime(2026, 1, 3, tzinfo=UTC), 80)


def test_forward_source_verification_rejects_revision_and_truncation():
    pd = pytest.importorskip("pandas")
    from scripts.run_breakout_forward_shadow import verify_processed_source

    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles = [candle(start + timedelta(days=i), 100 + i) for i in range(120)]
    frame = pd.DataFrame(
        [
            {
                "ts": value.ts,
                "open": value.open,
                "high": value.high,
                "low": value.low,
                "close": value.close,
            }
            for value in candles
        ]
    )
    machine = CloseRangeMachine()
    machine.warmup(candles)
    verify_processed_source(machine, frame)
    revised = frame.copy()
    revised.loc[50, "close"] += 0.01
    with pytest.raises(ValueError, match="changed"):
        verify_processed_source(machine, revised)
    with pytest.raises(ValueError, match="truncated"):
        verify_processed_source(machine, frame.iloc[:-1])
