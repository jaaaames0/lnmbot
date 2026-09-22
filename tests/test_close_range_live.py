from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.close_range import CloseRangeMachine, DailyCandle
from lnmarkets_bot.strategy.close_range_live import CloseRangeLive


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
