"""Operator-approved external-close rules survive warmup and event replay."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from lnmarkets_bot.engine.live_executor import ExternalClose
from lnmarkets_bot.strategy.base import Bar, StrategyState, TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross


def bar(day, close=120.0, tf="1d", warmup=False):
    return Bar(
        ts=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=day),
        open=close, high=close, low=close, close=close, volume=1,
        timeframe=tf, warmup=warmup,
    )


def ready():
    strategy, state = MaCross(), StrategyState()
    strategy.on_startup(state)
    for day in range(21):
        strategy.on_bar(bar(day, close=100, warmup=True), state)
    strategy.on_bar(bar(21), state)
    state.positions["4h"] = TfPosition(side="short", entry_price_usd=130)
    strategy._suppressed_signals["4h"] = 7
    strategy._loss_suppressed_signals["4h"] = 2
    return strategy, state


def event(liquidated=False, tf="1d", side="long", exit_price=119):
    return ExternalClose(
        execution_key=f"ma_cross_primary:{tf}", strategy_instance_id="ma_cross_primary",
        position_key=tf, trigger_tf=tf, trade_id="closed-ma", observed_at=bar(21).ts,
        reason="liquidation" if liquidated else "external_close", liquidated=liquidated,
        price_usd=exit_price, net_pl_sats=-1000, entry_price_usd=120, side=side,
    )


@pytest.mark.parametrize("tf,count", [("1d", 3), ("4h", 4)])
@pytest.mark.parametrize("side,price", [("long", 119), ("short", 121)])
def test_liquidation_starts_loss_cooldown_below_price_threshold(tf, count, side, price):
    strategy, state = ready()
    strategy.on_external_position_closed(event(True, tf, side, price), state)
    assert strategy._loss_suppressed_signals[tf] == count
    assert strategy._suppressed_signals[tf] == 0
    assert state.position(tf).side is None
    assert strategy._pending_position_reconciliation[tf] is None
    assert tf not in strategy._external_reset_pending
    assert strategy._last_trade_pnl_pct[tf] == pytest.approx(-1 / 120)


def test_manual_close_reconsiders_unchanged_direction_only_at_next_live_boundary():
    strategy, state = ready()
    strategy._suppressed_signals["1d"] = 8
    strategy._loss_suppressed_signals["1d"] = 1
    strategy.on_external_position_closed(event(), state)
    assert strategy._active_cooldowns("1d") == {}
    assert strategy.on_bar(bar(22, tf="1m"), state) == []
    assert state.position("1d").side is None
    assert strategy._suppressed_signals["4h"] == 7
    assert strategy._loss_suppressed_signals["4h"] == 2
    assert state.position("4h").side == "short"
    # Simulate a crash after acknowledged delivery and overlapping restart warmup.
    restored = MaCross()
    assert restored.restore_persistent_state(strategy.persistent_state())
    restored.on_startup(state)
    assert restored.on_bar(bar(21, warmup=True), state) == []
    assert restored.on_bar(bar(22, warmup=True), state) == []
    assert restored.on_bar(bar(22, tf="1m"), state) == []
    intents = restored.on_bar(bar(23), state)
    assert [i.kind.value for i in intents] == ["entry"]
    assert intents[0].side.value == "long"
    assert restored.on_bar(bar(23), state) == []


def test_liquidation_replay_does_not_restart_a_consumed_cooldown():
    strategy, state = ready()
    closed = event(True)
    strategy.on_external_position_closed(closed, state)
    assert strategy.on_bar(bar(22, tf="1m"), state) == []
    assert strategy.on_bar(bar(22), state) == []  # unchanged verdict spends no slot
    intents = strategy.on_bar(bar(23, close=70), state)
    assert [i.kind.value for i in intents] == ["noop"]
    assert strategy._loss_suppressed_signals["1d"] == 2
    restored = MaCross()
    assert restored.restore_persistent_state(strategy.persistent_state())
    restored.on_external_position_closed(closed, state)
    assert restored._loss_suppressed_signals["1d"] == 2


def test_manual_close_at_neutral_boundary_stays_flat():
    strategy, state = ready()
    strategy.on_external_position_closed(event(), state)
    strategy.on_bar(bar(22, close=101), state)
    assert state.position("1d").side is None
    assert not strategy._external_reset_pending


def test_external_close_refuses_a_slot_outside_its_owner():
    strategy, state = ready()
    with pytest.raises(ValueError, match="owning timeframe"):
        strategy.on_external_position_closed(event(tf="k0"), state)


def test_unknown_closure_uses_loss_cooldown_without_claiming_liquidation():
    strategy, state = ready()
    closed = replace(event(), liquidated=None, reason="external_close_unclassified")
    strategy.on_external_position_closed(closed, state)
    assert strategy._loss_suppressed_signals["1d"] == 3
    assert strategy._last_external_closures["1d"]["liquidated"] is None
    assert "1d" not in strategy._external_reset_pending
    assert strategy.on_bar(bar(22), state) == []
    strategy.on_bar(bar(23, close=70), state)
    assert strategy._loss_suppressed_signals["1d"] == 2
    restored = MaCross()
    assert restored.restore_persistent_state(strategy.persistent_state())
    restored.classify_external_close(replace(closed, liquidated=True), state)
    assert restored._loss_suppressed_signals["1d"] == 2


def test_operator_confirmed_manual_cause_resets_a_previous_unknown_close():
    strategy, state = ready()
    closed = replace(event(), liquidated=None, reason="external_close_unclassified")
    strategy.on_external_position_closed(closed, state)
    strategy.classify_external_close(replace(closed, liquidated=False), state)
    assert strategy._active_cooldowns("1d") == {}
    assert "1d" in strategy._external_reset_pending
    assert [i.kind.value for i in strategy.on_bar(bar(22), state)] == ["entry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["manual", "liquidation"])
async def test_real_rest_shape_and_operator_classification_are_atomic(tmp_path, cause):
    import importlib.util
    import sqlite3
    from pathlib import Path

    from lnmarkets_bot.api.isolated import IsolatedTradesApi
    from lnmarkets_bot.engine.live_executor import LiveExecutor
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, _deliver_external
    from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
    from lnmarkets_bot.persistence.recorder import Recorder
    from lnmarkets_bot.strategy.intents import OrderIntent
    from tests.test_live_executor import FakeIsolatedTradesApi

    path = tmp_path / "copy.sqlite"
    init_schema(make_engine(path))
    recorder = Recorder(make_session_factory(make_engine(path)))
    run = recorder.start_run(mode="live", strategy_name="portfolio", strategy_params={}, config={}, started_at=bar(21).ts)
    api = FakeIsolatedTradesApi()
    api.entry_price = 120
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=run)
    executor.update_price(120)
    intent = replace(OrderIntent.enter_long("1d", 100, 5), strategy_instance_id="ma_cross_primary", position_key="1d")
    await executor.submit(intent=intent, signal_id=None, run_id=run, ts=bar(21).ts, size_usd=100, leverage=5)
    api.running.clear()
    # Actual v3 field shape: liquidation is a PRICE and closed is a BOOLEAN.
    api.closed["iso-1"] = IsolatedTradesApi._parse_trade({
        "id": "iso-1", "type": "market", "side": "buy", "quantity": 100,
        "leverage": 5, "price": 120, "entryPrice": 120, "exitPrice": 119,
        "liquidation": 100, "closed": True, "closedAt": bar(22).ts.isoformat(),
        "pl": -1000, "closingFee": 5,
    })
    events = await executor.reconcile_external_closures(run_id=run, ts=bar(22).ts)
    assert events[0].liquidated is None
    assert events[0].reason == "external_close_unclassified"
    strategy, state = ready()
    binding = StrategyBinding("ma_cross_primary", strategy)
    _deliver_external(binding, state, events, recorder, run)
    assert strategy._loss_suppressed_signals["1d"] == 3
    assert executor.pending_external_events() == []

    spec = importlib.util.spec_from_file_location("classify_ma", Path(__file__).resolve().parents[1] / "scripts/classify_ma_external_close.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    before = path.read_bytes()
    report = module.classify(path, "1d", "iso-1", cause)
    assert report["applied"] is False and path.read_bytes() == before
    report = module.classify(path, "1d", "iso-1", cause, apply=True, evidence="operator-confirmed fixture")
    assert report["manual_reset_pending"] == (cause == "manual")
    assert report["loss_cooldown_remaining"] == (0 if cause == "manual" else 3)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT sum(amount_sats) FROM strategy_pnl_events").fetchone()[0] == -1005
        assert db.execute("SELECT count(*) FROM orders").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM fills").fetchone()[0] == 2
        kind = db.execute("SELECT kind FROM strategy_pnl_events WHERE event_key='close:iso-1'").fetchone()[0]
        assert kind == ("liquidation" if cause == "liquidation" else "external_close_net_pl")
        import json

        metadata = json.loads(db.execute("SELECT metadata_json FROM strategy_pnl_events WHERE event_key='close:iso-1'").fetchone()[0])
        assert metadata["operator_closure_classification"]["cause"] == cause


@pytest.mark.parametrize("raw", ["2026-09-27T10:00:00Z", "2026-09-27T12:00:00+02:00", datetime(2026, 9, 27, 10)])
def test_venue_trade_timestamps_are_normalized_at_api_boundary(raw):
    from lnmarkets_bot.api.isolated import IsolatedTradesApi

    trade = IsolatedTradesApi._parse_trade({"createdAt": raw, "filledAt": raw, "closedAt": raw})
    assert trade.created_at == trade.filled_at == trade.closed_at == datetime(2026, 9, 27, 10, tzinfo=UTC)


@pytest.mark.parametrize("raw", ["bad-date", 1234, {}])
def test_malformed_venue_trade_timestamps_fail_closed(raw):
    from lnmarkets_bot.api.isolated import IsolatedTradesApi

    with pytest.raises(ValueError):
        IsolatedTradesApi._parse_trade({"closedAt": raw})
