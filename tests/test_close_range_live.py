from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.close_range import (
    BreakoutDecision,
    CampaignUnit,
    CloseRangeMachine,
    DailyCandle,
)
from lnmarkets_bot.strategy.close_range_live import CloseRangeLive

ROOT = Path(__file__).resolve().parents[1]


def _machine_with_pending_parent(start: datetime) -> CloseRangeMachine:
    machine = CloseRangeMachine()
    machine.warmup(
        [
            DailyCandle(
                start + timedelta(days=i),
                100 + i * 5,
                101 + i * 5,
                99 + i * 5,
                100 + i * 5,
            )
            for i in range(120)
        ]
    )
    return machine


def _state() -> StrategyState:
    return StrategyState(
        positions={f"k{i}": TfPosition() for i in range(4)},
    )


def _august_reversal(*, unit_count: int = 1) -> tuple[CloseRangeLive, StrategyState, datetime]:
    """The real LN Markets long signal arrived while a funded short was closing."""
    frame = pd.read_parquet(
        ROOT / "data/cache/lnmarkets_btc_1d_2019-09-09_2026-09-13.parquet"
    ).sort_values("ts")
    frame["ts"] = pd.to_datetime(frame.ts, utc=True)
    frame["high"] = frame[["open", "high", "close"]].max(axis=1)
    frame["low"] = frame[["open", "low", "close"]].min(axis=1)
    machine = CloseRangeMachine()
    history = frame[frame.ts <= pd.Timestamp("2026-08-20", tz="UTC")]
    machine.warmup(
        [
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close)
            for row in history.itertuples(index=False)
        ]
    )
    machine.seed_campaign(
        {
            "parent_id": "20260602S",
            "side": "short",
            "entry_ts": "2026-06-02T00:00:00+00:00",
            "entry_price": 71355.8,
            "boundary": 73347.5,
            "held_days": 80,
            "peak_favorable_pct": 0.1,
            "active_units": unit_count,
            "pending_exit": None,
        }
    )
    assert machine.campaign is not None
    machine.campaign.origin = "live"
    machine.campaign.units[0].origin = "live"
    for k in range(1, unit_count):
        machine.campaign.units.append(
            CampaignUnit(k, datetime(2026, 6, 2, tzinfo=UTC), 71_355.8, "live")
        )
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": "2026-01-01T00:00:00+00:00"},
        machine=machine,
    )
    state = _state()
    for k in range(unit_count):
        state.position(f"k{k}").qty_sats = -100
        state.position(f"k{k}").entry_price_usd = 71355.8
    next_open = datetime(2026, 8, 22, tzinfo=UTC)
    signal = frame[frame.ts == pd.Timestamp("2026-08-21", tz="UTC")].iloc[0]
    intents = strategy.on_bar(
        Bar(
            ts=next_open,
            open=signal.open,
            high=signal.high,
            low=signal.low,
            close=signal.close,
            volume=1,
            timeframe="1d",
        ),
        state,
    )
    assert [(intent.kind.value, intent.position_key) for intent in intents] == [
        ("exit", f"k{k}") for k in range(unit_count)
    ]
    assert strategy.machine.campaign is None
    assert strategy.persistent_state()["pending_reversal"]["campaign_id"] == "20260822L"
    assert strategy.persistent_state()["recent_decisions"][-1]["reason"] == "awaiting_campaign_close"
    return strategy, state, next_open


def _minute(ts: datetime) -> Bar:
    return Bar(ts, 78_300, 78_300, 78_300, 78_300, 1, timeframe="1m")


def test_funded_reversal_enters_only_after_old_close_confirms():
    strategy, state, next_open = _august_reversal(unit_count=2)
    assert strategy.on_bar(_minute(next_open + timedelta(minutes=1)), state) == []
    strategy.on_order_result(
        strategy._exit_intent("k0", "range_close", "20260602S"),
        SimpleNamespace(order_id=9, detail={}),
        state,
    )
    state.position("k0").qty_sats = 0
    assert strategy.on_bar(_minute(next_open + timedelta(minutes=2)), state) == []
    strategy.on_order_result(
        strategy._exit_intent("k1", "range_close", "20260602S"),
        SimpleNamespace(order_id=10, detail={}),
        state,
    )
    state.position("k1").qty_sats = 0
    entry = strategy.on_bar(_minute(next_open + timedelta(minutes=3)), state)
    assert [(intent.kind.value, intent.position_key) for intent in entry] == [("entry", "k0")]
    assert entry[0].metadata["campaign_id"] == "20260822L"
    strategy.on_order_result(entry[0], SimpleNamespace(order_id=12, detail={"price_usd": 78_301}), state)
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.origin == "live"
    assert strategy.machine.campaign.units[0].entry_price == 78_301
    assert strategy.persistent_state()["pending_reversal"] is None


def test_failed_reversal_close_survives_restart_and_never_enters_early():
    strategy, state, next_open = _august_reversal()
    snapshot = strategy.persistent_state()
    restored = CloseRangeLive(strategy.params)
    assert restored.restore_persistent_state(snapshot)
    restored.reconcile_execution_state(state)
    restored.on_startup(state)
    warmup = replace(_minute(next_open + timedelta(seconds=30)), warmup=True)
    assert restored.on_bar(warmup, state) == []
    retry = restored.on_bar(_minute(next_open + timedelta(minutes=1)), state)
    assert [(intent.kind.value, intent.position_key) for intent in retry] == [("exit", "k0")]
    restored.on_order_result(retry[0], SimpleNamespace(order_id=-1, detail={}), state)
    assert restored.on_bar(_minute(next_open + timedelta(minutes=2)), state) == []
    # The executor's retry path does not call on_order_result; its next state
    # mirror and reconciliation must still release the deferred parent.
    state.position("k0").qty_sats = 0
    restored.reconcile_execution_state(state)
    assert [intent.kind.value for intent in restored.on_bar(_minute(next_open + timedelta(minutes=3)), state)] == ["entry"]


def test_changed_unit_size_restores_old_trade_and_sizes_next_entry():
    strategy, state, next_open = _august_reversal()
    legacy_snapshot = strategy.persistent_state()
    legacy_snapshot.pop("historical_unit_notional_usd")
    smaller = CloseRangeLive({**strategy.params, "unit_notional_usd": 40})
    assert smaller.restore_persistent_state(legacy_snapshot)
    smaller.reconcile_execution_state(state)
    smaller.on_startup(state)
    assert state.position("k0").qty_sats == -100
    assert smaller.persistent_state()["historical_unit_notional_usd"] == 100
    assert smaller.persistent_state()["unit_notional_usd"] == 40

    retry = smaller.on_bar(_minute(next_open + timedelta(minutes=1)), state)
    assert [(intent.kind.value, intent.position_key) for intent in retry] == [("exit", "k0")]
    smaller.on_order_result(
        retry[0],
        SimpleNamespace(order_id=9, detail={}),
        state,
    )
    state.position("k0").qty_sats = 0
    entry = smaller.on_bar(_minute(next_open + timedelta(minutes=2)), state)
    assert len(entry) == 1
    assert entry[0].size_usd == 40

    larger = CloseRangeLive({**strategy.params, "unit_notional_usd": 80})
    assert larger.restore_persistent_state(smaller.persistent_state())
    assert larger.persistent_state()["historical_unit_notional_usd"] == 100
    assert larger.persistent_state()["unit_notional_usd"] == 80


def test_invalid_saved_size_is_not_accepted_as_restart_state():
    strategy = CloseRangeLive({"unit_notional_usd": 100, "leverage": 5})
    snapshot = strategy.persistent_state()
    snapshot["unit_notional_usd"] = float("nan")
    assert not CloseRangeLive({"unit_notional_usd": 40, "leverage": 5}).restore_persistent_state(
        snapshot
    )


def test_resized_addon_does_not_change_existing_parent_position():
    previous = CloseRangeLive({"unit_notional_usd": 100, "leverage": 5})
    current = CloseRangeLive({"unit_notional_usd": 40, "leverage": 5})
    assert current.restore_persistent_state(previous.persistent_state())
    state = _state()
    state.position("k0").qty_sats = 75
    state.position("k0").entry_price_usd = 80_000
    addon = BreakoutDecision(
        ts=datetime(2026, 9, 24, tzinfo=UTC),
        kind="paper_addon",
        reason="range_breakout",
        campaign_id="20260822L",
        k=1,
        side=1,
        price=85_000,
        metadata={},
    )
    intents = current._intents([addon], state)
    assert len(intents) == 1
    assert intents[0].size_usd == 40
    assert state.position("k0").qty_sats == 75
    assert state.position("k0").entry_price_usd == 80_000


def test_reversal_expires_instead_of_entering_late_or_leaving_paper_occupancy():
    strategy, state, next_open = _august_reversal()
    assert strategy.on_bar(_minute(next_open + timedelta(minutes=6)), state) == []
    assert strategy.persistent_state()["pending_reversal"] is None
    assert strategy.machine.campaign is None
    assert strategy.persistent_state()["closing_slots"] == ["k0"]
    assert strategy.persistent_state()["recent_decisions"][-1]["reason"] == "reversal_entry_expired"


def test_rejected_reversal_entry_clears_unfunded_campaign():
    strategy, state, next_open = _august_reversal()
    exit_intent = strategy._exit_intent("k0", "range_close", "20260602S")
    strategy.on_order_result(exit_intent, SimpleNamespace(order_id=9, detail={}), state)
    state.position("k0").qty_sats = 0
    entry = strategy.on_bar(_minute(next_open + timedelta(minutes=1)), state)[0]
    strategy.on_order_result(entry, SimpleNamespace(order_id=None, detail={}), state)
    assert strategy.machine.campaign is None
    assert strategy.persistent_state()["pending_reversal"] is None


def test_restart_after_reversal_fill_recovers_owner_without_duplicate_entry():
    strategy, state, next_open = _august_reversal()
    exit_intent = strategy._exit_intent("k0", "range_close", "20260602S")
    strategy.on_order_result(exit_intent, SimpleNamespace(order_id=9, detail={}), state)
    state.position("k0").qty_sats = 0
    assert strategy.on_bar(_minute(next_open + timedelta(minutes=1)), state)
    pre_fill_snapshot = strategy.persistent_state()
    state.position("k0").qty_sats = 100
    state.position("k0").entry_price_usd = 78_301
    restored = CloseRangeLive(strategy.params)
    assert restored.restore_persistent_state(pre_fill_snapshot)
    restored.reconcile_execution_state(state)
    restored.on_startup(state)
    assert restored.machine.campaign is not None
    assert restored.machine.campaign.origin == "live"
    assert restored.persistent_state()["pending_reversal"] is None
    assert restored.on_bar(_minute(next_open + timedelta(minutes=2)), state) == []


def test_live_adapter_routes_parent_to_k0_and_confirms_actual_fill():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": start.isoformat()},
        machine=_machine_with_pending_parent(start),
    )
    bar = Bar(
        ts=start + timedelta(days=121),
        open=700,
        high=1_010,
        low=699,
        close=1_000,
        volume=1,
        timeframe="1d",
    )
    intents = strategy.on_bar(bar, _state())
    assert len(intents) == 1
    assert intents[0].position_key == "k0"
    assert intents[0].size_usd == 100
    assert intents[0].leverage == 5

    strategy.on_order_result(
        intents[0], SimpleNamespace(order_id=1, detail={"price_usd": 111.25}), _state()
    )
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.origin == "live"
    assert strategy.machine.campaign.units[0].entry_price == 111.25


def test_historical_campaign_never_funds_an_addon_without_owned_parent():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = _machine_with_pending_parent(start)
    machine.seed_campaign(
        {
            "parent_id": "historical",
            "side": "long",
            "entry_ts": (start + timedelta(days=100)).isoformat(),
            "entry_price": 100,
            "boundary": 95,
            "held_days": 20,
            "peak_favorable_pct": 0.1,
            "active_units": 1,
            "pending_exit": None,
        }
    )
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": start.isoformat()},
        machine=machine,
    )
    bar = Bar(
        ts=start + timedelta(days=121),
        open=700,
        high=1_010,
        low=699,
        close=1_000,
        volume=1,
        timeframe="1d",
    )
    assert strategy.on_bar(bar, _state()) == []


def test_parent_liquidation_queues_immediate_child_closes_and_persists_closing_state():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    machine = _machine_with_pending_parent(start)
    machine.seed_campaign(
        {
            "parent_id": "live-campaign",
            "side": "long",
            "entry_ts": (start + timedelta(days=100)).isoformat(),
            "entry_price": 700,
            "boundary": 650,
            "held_days": 20,
            "peak_favorable_pct": 0.1,
            "active_units": 3,
            "pending_exit": None,
        }
    )
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": start.isoformat()},
        machine=machine,
    )
    state = _state()
    state.position("k1").qty_sats = 100
    state.position("k2").qty_sats = 100
    strategy.on_external_position_closed(
        SimpleNamespace(
            position_key="k0",
            price_usd=583,
            observed_at=start + timedelta(days=121),
            trade_id="parent-trade",
        ),
        state,
    )
    intents = strategy.on_bar(
        Bar(
            ts=start + timedelta(days=121, minutes=1),
            open=583,
            high=583,
            low=583,
            close=583,
            volume=1,
            timeframe="1m",
        ),
        state,
    )
    assert [intent.position_key for intent in intents] == ["k1", "k2"]
    snapshot = strategy.persistent_state()
    assert snapshot["closing_campaign_id"] == "live-campaign"
    assert snapshot["closing_slots"] == ["k1", "k2"]


def test_external_close_during_pending_campaign_close_completes_slot_bookkeeping():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": start.isoformat()},
        machine=_machine_with_pending_parent(start),
    )
    strategy._closing_campaign_id = "closing-campaign"
    strategy._closing_slots = {"k0", "k1"}

    strategy.on_external_position_closed(
        SimpleNamespace(
            position_key="k0",
            price_usd=90,
            observed_at=start + timedelta(days=121),
            trade_id="parent-trade",
        ),
        _state(),
    )

    assert strategy.persistent_state()["closing_slots"] == ["k1"]
    assert strategy.persistent_state()["closing_campaign_id"] == "closing-campaign"
