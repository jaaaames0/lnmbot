"""Regression coverage for independent audit findings. All venue actions are simulated."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
import structlog

from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTrade
from lnmarkets_bot.engine.live_executor import LiveExecutor, UnsafeLiveStateError
from lnmarkets_bot.persistence.models import fills, funding_fees, orders, strategy_pnl_events
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
from lnmarkets_bot.strategy.ma_cross import MaCross
from tests.test_breakout_funded_campaign_trace import (
    BOUNDARY,
    INSTANCE,
    _continue,
    _day,
    _funded_stack,
    _rows,
)


def minute():
    return Bar(datetime(2026, 8, 29, 12, tzinfo=UTC), 70000, 70000, 70000, 70000, 1, timeframe="1m")


@pytest.mark.asyncio
@pytest.mark.parametrize("mask", range(1, 16))
@pytest.mark.parametrize("reverse", [False, True])
async def test_simultaneous_campaign_closures(cfg, mask, reverse):
    v, e, r, _f, p, s = await _funded_stack(cfg)
    if reverse:
        e.positions = dict(reversed(list(e.positions.items())))
    for k in range(4):
        if mask & (1 << k):
            v.liquidate(f"trace-{k}", 65000)
    await _continue(cfg, v, e, r, p, s, [minute()])
    if mask & 1:
        assert not v.running
    else:
        assert sorted(v.running) == [f"trace-{k}" for k in range(4) if not mask & (1 << k)]
    assert all(row["notified"] for row in r.commands(["applied"]))


@pytest.mark.asyncio
async def test_full_stack_partial_close_retries_and_restarts(cfg):
    v, e, r, f, p, s = await _funded_stack(cfg)
    real_close = v.close_trade

    async def fail_one(tid):
        if tid == "trace-2":
            raise ConnectionError("synthetic unavailable")
        return await real_close(tid)

    v.close_trade = fail_one
    await _continue(cfg, v, e, r, p, s, [_day(8, close=BOUNDARY, low=72000)])
    assert list(v.running) == ["trace-2"]
    e = LiveExecutor(trades_api=v, recorder=r, run_id=0)
    await e.reconcile()
    s = CloseRangeLive(s.params)
    v.close_trade = real_close
    await _continue(cfg, v, e, r, p, s, [minute()])
    assert v.running == {}
    assert len(_rows(f, orders)) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_write", ["record_order", "record_fill", "upsert_daily_pnl", "record_strategy_pnl_event"]
)
async def test_close_accounting_commit_failure_is_replayed(cfg, failed_write):
    v, e, r, f, p, s = await _funded_stack(cfg)
    real_fill = getattr(r, failed_write)

    def fail(*a, **kw):
        raise OSError("synthetic disk error after close order commit")

    setattr(r, failed_write, fail)
    with pytest.raises(UnsafeLiveStateError):
        await _continue(cfg, v, e, r, p, s, [_day(8, close=BOUNDARY, low=72000)])
    setattr(r, failed_write, real_fill)
    e = LiveExecutor(trades_api=v, recorder=r, run_id=0)
    await e.reconcile()
    s = CloseRangeLive(s.params)
    await _continue(cfg, v, e, r, p, s, [minute()])
    assert v.running == {}
    assert len(_rows(f, orders)) == 8
    assert len(_rows(f, fills)) == 8
    assert len(_rows(f, strategy_pnl_events)) == 8


@pytest.mark.asyncio
async def test_final_funding_retained_after_external_close(cfg):
    v, e, r, f, p, s = await _funded_stack(cfg)
    fee = {
        "tradeId": "trace-0",
        "settlementId": "late",
        "time": minute().ts.isoformat(),
        "fee": 100,
    }

    async def funding(*args):
        yield fee

    v.iter_funding_fees = funding
    v.liquidate("trace-0", 65000)
    await _continue(cfg, v, e, r, p, s, [minute()])
    assert v.running == {}
    assert [row["fee_sats"] for row in _rows(f, funding_fees)] == [100]


@pytest.mark.asyncio
async def test_mutating_post_not_retried_after_500(monkeypatch):
    c = LnmRestClient(base_url="https://audit.invalid/v3")
    created = []

    async def respond(request):
        created.append(len(created) + 1)
        return httpx.Response(500 if len(created) == 1 else 200, json={"id": str(created[-1])})

    await c._client.aclose()
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    monkeypatch.setattr("lnmarkets_bot.api.client.asyncio.sleep", AsyncMock())
    try:
        from lnmarkets_bot.api.client import LnmApiError

        with pytest.raises(LnmApiError):
            await c.post("/futures/isolated/trade", body={"quantity": 100})
        assert created == [1]
    finally:
        await c.aclose()


def test_ma_warmup_consumes_missed_cooldown_transition():
    s = MaCross({"tfs": ("4h",)})
    state = StrategyState()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for i in range(21):
        s.on_bar(
            Bar(start + timedelta(hours=4 * i), 100, 100, 100, 100, 1, timeframe="4h", warmup=True),
            state,
        )
    s._suppressed_signals["4h"] = 3
    live = MaCross(s.params)
    assert live.restore_persistent_state(s.persistent_state())
    missed = Bar(start + timedelta(hours=84), 110, 110, 110, 110, 1, timeframe="4h")
    live.on_bar(missed, StrategyState())
    s.on_bar(replace(missed, warmup=True), state)
    assert live._suppressed_signals["4h"] == 2
    assert s._suppressed_signals["4h"] == 2


def test_ma_size_change_preserves_existing_snapshot():
    s = MaCross({"base_size_usd": 100})
    replacement = MaCross({"base_size_usd": 200})
    assert replacement.restore_persistent_state(s.persistent_state()) is True


@pytest.mark.asyncio
async def test_new_remote_trade_blocks_admission_during_runtime(cfg):
    v, e, _r, _f, _p, _s = await _funded_stack(cfg)
    v.running["unknown"] = IsolatedTrade(
        id="unknown",
        type="market",
        side="buy",
        quantity=500,
        leverage=5,
        price=80000,
        entry_price=80000,
    )
    assert await e.reconcile_external_closures(run_id=e.run_id, ts=minute().ts) == []
    assert e.entries_blocked_reason() == "unowned_remote_exposure"
    assert e.open_notional_usd() == 400
    assert sum(t.quantity for t in v.running.values()) == 900


@pytest.mark.asyncio
async def test_funding_updates_running_daily_loss_guard(cfg):
    v, e, r, _f, _p, _s = await _funded_stack(cfg)
    guard = RiskGuard(limits=from_config(cfg), recorder=r, executor=e)
    guard.current_price_usd = 70000
    assert not guard.is_daily_loss_tripped(minute().ts)

    async def funding(*args):
        yield {
            "tradeId": "trace-0",
            "settlementId": "loss",
            "time": minute().ts.isoformat(),
            "fee": 1000000,
        }

    v.iter_funding_fees = funding
    await e.sync_funding(minute().ts, force=True)
    assert r.net_daily_pnl_sats("2026-08-29") == -1000000
    assert guard.is_daily_loss_tripped(minute().ts)
    restarted = RiskGuard(limits=from_config(cfg), recorder=r, executor=e)
    restarted.current_price_usd = 70000
    assert restarted.is_daily_loss_tripped(minute().ts)


@pytest.mark.asyncio
async def test_exit_order_price_matches_actual_fill(cfg):
    v, e, r, f, p, s = await _funded_stack(cfg)
    real_close = v.close_trade

    async def slipped(tid):
        v.mark = 70000
        return await real_close(tid)

    v.close_trade = slipped
    await _continue(cfg, v, e, r, p, s, [_day(8, close=BOUNDARY, low=72000)])
    assert [o["price_usd"] for o in _rows(f, orders)[4:]] == [70000] * 4
    assert [o["price_usd"] for o in _rows(f, fills)[4:]] == [70000] * 4


@pytest.mark.asyncio
async def test_first_external_loss_of_day_is_counted_once(cfg):
    _v, e, r, _f, _p, _s = await _funded_stack(cfg)
    guard = RiskGuard(limits=from_config(cfg), recorder=r, executor=e)
    guard.current_price_usd = 100000
    r.upsert_daily_pnl(e.run_id, date_str="2026-08-29", realized_delta_sats=-100000)
    guard.record_realized_pnl(-100, minute().ts)
    assert guard._today_realized_pnl_usd == -100
    assert r.net_daily_pnl_sats("2026-08-29") == -100000


@pytest.mark.asyncio
async def test_expired_entry_blocks_while_owned_exits_continue(cfg):
    v, e, r, _f, p, s = await _funded_stack(cfg)
    now = minute().ts + timedelta(hours=6)
    e._clock = lambda: now
    e.max_entry_age_seconds = 300
    intent = __import__("lnmarkets_bot.strategy", fromlist=["OrderIntent"]).OrderIntent.enter_long(
        "4h", 100, 5
    )
    _, detail = await e.submit(
        intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts, size_usd=100, leverage=5
    )
    assert detail["reason"] == "entry_expired"
    assert len(v.running) == 4
    await _continue(cfg, v, e, r, p, s, [_day(8, close=BOUNDARY, low=72000)])
    assert not v.running


@pytest.mark.asyncio
async def test_external_event_snapshot_failure_replays_before_startup(cfg):
    v, e, r, f, p, s = await _funded_stack(cfg)
    v.liquidate("trace-0", 65000)
    original = r.save_strategy_state
    r.save_strategy_state = lambda *a, **kw: (_ for _ in ()).throw(OSError("snapshot disk"))
    with pytest.raises(OSError):
        await _continue(cfg, v, e, r, p, s, [minute()])
    r.save_strategy_state = original
    restored = LiveExecutor(trades_api=v, recorder=r, run_id=e.run_id)
    await restored.reconcile()
    strategy = CloseRangeLive(s.params)
    await _continue(cfg, v, restored, r, p, strategy, [minute()])
    assert not v.running
    assert len(_rows(f, orders)) == 8
    assert len(_rows(f, strategy_pnl_events)) == 8


@pytest.mark.asyncio
async def test_cash_admission_rejects_only_new_risk_and_keeps_exits(cfg):
    from lnmarkets_bot.strategy import OrderIntent

    v, e, r, _f, _p, _s = await _funded_stack(cfg)

    class Cash:
        async def available_cash_usd(self, **kwargs):
            return 1

    guard = RiskGuard(
        limits=from_config(cfg), recorder=r, executor=e, account_balance_provider=Cash()
    )
    guard.current_price_usd = 70000
    decision = await guard.submit(
        intent=OrderIntent.enter_long("4h", 100, 5), signal_id=1, run_id=e.run_id, ts=minute().ts
    )
    assert decision.detail["reason"] == "insufficient_cash"
    assert len(v.running) == 4
    intent = OrderIntent.exit("1d")
    intent = replace(intent, strategy_instance_id=INSTANCE, position_key="k0")
    decision = await guard.submit(intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts)
    assert decision.order_id > 0


@pytest.mark.asyncio
async def test_funding_transaction_failure_retries_without_dedup_loss(cfg):
    v, e, r, f, _p, _s = await _funded_stack(cfg)

    async def funding(*args):
        yield {
            "tradeId": "trace-0",
            "settlementId": "atomic",
            "time": minute().ts.isoformat(),
            "fee": 100,
        }

    v.iter_funding_fees = funding
    original = r.record_strategy_pnl_event
    r.record_strategy_pnl_event = lambda *a, **kw: (_ for _ in ()).throw(OSError("disk"))
    await e.sync_funding(minute().ts, force=True)
    assert not _rows(f, funding_fees)
    r.record_strategy_pnl_event = original
    with structlog.testing.capture_logs() as logs:
        await e.sync_funding(minute().ts, force=True)
        await e.sync_funding(minute().ts, force=True)
    assert len(_rows(f, funding_fees)) == 1
    assert [log["event"] for log in logs].count("live.funding_recorded") == 1
    assert r.net_daily_pnl_sats("2026-08-29") == -100


@pytest.mark.asyncio
async def test_http_timeout_status_is_not_a_definitive_rejection(cfg):
    from lnmarkets_bot.api.client import LnmApiError
    from lnmarkets_bot.strategy import OrderIntent

    v, e, _r, _f, _p, _s = await _funded_stack(cfg)

    async def timeout(*a):
        raise LnmApiError(408, "timeout", "https://audit.invalid")

    v.new_trade = timeout
    order, detail = await e.submit(
        intent=OrderIntent.enter_long("4h", 100, 5),
        signal_id=1,
        run_id=e.run_id,
        ts=minute().ts,
        size_usd=100,
        leverage=5,
    )
    assert order == -1 and detail["reason"] == "entry_outcome_unresolved"
    assert e.entries_blocked_reason() == "entry_outcome_unresolved"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_write", ["record_order", "record_fill", "upsert_daily_pnl", "record_strategy_pnl_event"]
)
async def test_accepted_entry_accounting_replays_once(cfg, failed_write):
    from lnmarkets_bot.strategy import OrderIntent

    v, e, r, f, _p, _s = await _funded_stack(cfg)
    original = getattr(r, failed_write)
    setattr(r, failed_write, lambda *a, **kw: (_ for _ in ()).throw(OSError("disk")))
    intent = replace(
        OrderIntent.enter_short("4h", 100, 5),
        strategy_instance_id="ma_cross_primary",
        position_key="4h",
    )
    with pytest.raises(UnsafeLiveStateError):
        await e.submit(
            intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts, size_usd=100, leverage=5
        )
    setattr(r, failed_write, original)
    restored = LiveExecutor(trades_api=v, recorder=r, run_id=e.run_id)
    await restored.reconcile()
    await restored.reconcile()
    assert len(v.opens) == 5 and len(v.running) == 5
    assert restored.position_qty_sats("ma_cross_primary:4h") == -100
    assert len(_rows(f, orders)) == len(_rows(f, fills)) == len(_rows(f, strategy_pnl_events)) == 5
    restored.update_price(80000)
    decision, detail = await restored.submit(
        intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts, size_usd=100, leverage=5
    )
    assert decision == -1 and detail["reason"] == "position_already_open"
    assert len(v.opens) == 5


@pytest.mark.asyncio
async def test_manual_parent_and_child_close_preserves_reason_and_exits_survivors(cfg):
    v, e, r, f, p, s = await _funded_stack(cfg)
    for k in (0, 2):
        v.liquidate(f"trace-{k}", 65000)
        trade = v.closed[f"trace-{k}"]
        v.closed[trade.id] = replace(
            trade, raw={**trade.raw, "liquidated": False, "closeReason": "manual_close"}
        )
    await _continue(cfg, v, e, r, p, s, [minute()])
    assert not v.running
    external = [
        row
        for row in _rows(f, strategy_pnl_events)
        if row["metadata_json"].get("external_reason") == "manual_close"
    ]
    assert len(external) == 2 and all(row["kind"] == "external_close_net_pl" for row in external)
    assert any(
        row["reason"] == "parent_external_close" for row in s.persistent_state()["recent_decisions"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("minutes,reason", [(4, None), (6, "entry_expired")])
async def test_deferred_entry_age_uses_original_open_and_wall_clock(cfg, minutes, reason):
    from lnmarkets_bot.strategy import OrderIntent

    _v, e, _r, _f, _p, _s = await _funded_stack(cfg)
    now = minute().ts + timedelta(minutes=minutes)
    e._clock = lambda: now
    e.max_entry_age_seconds = 300
    intent = replace(
        OrderIntent.enter_long("4h", 100, 5),
        metadata={"intended_entry_ts": minute().ts.isoformat()},
    )
    assert e.entry_admission_reason(intent, now - timedelta(minutes=1)) == reason
    assert (
        e.entry_admission_reason(replace(intent, metadata={}), now - timedelta(minutes=2))
        == "quote_stale"
    )


@pytest.mark.asyncio
async def test_six_owned_units_share_cash_and_gross_caps_without_future_reservation(cfg):
    from lnmarkets_bot.strategy import OrderIntent

    v, e, r, f, _p, _s = await _funded_stack(cfg)

    class Cash:
        cash = 41.0

        async def available_cash_usd(self, **kwargs):
            return self.cash

    cash = Cash()
    guard = RiskGuard(
        limits=replace(from_config(cfg), max_total_notional_usd=600, max_total_margin_usd=125),
        recorder=r,
        executor=e,
        account_balance_provider=cash,
    )
    guard.current_price_usd = 80000
    for tf in ("1d", "4h"):
        intent = replace(
            OrderIntent.enter_short(tf, 100, 5),
            strategy_instance_id="ma_cross_primary",
            position_key=tf,
        )
        decision = await guard.submit(intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts)
        assert decision.order_id > 0
        cash.cash -= 20.22
    assert len(v.running) == 6 and e.open_notional_usd() == 600
    assert {row["strategy_instance_id"] for row in _rows(f, orders)} == {
        INSTANCE,
        "ma_cross_primary",
    }
    decision = await guard.submit(
        intent=OrderIntent.enter_long("1h", 100, 5), signal_id=1, run_id=e.run_id, ts=minute().ts
    )
    assert decision.detail["reason"] == "total_notional_exceeded"
    # An existing position retains its actual size when entry configuration changes.
    assert e.position_qty_sats(f"{INSTANCE}:k0") == 100
    e.update_price(80000)
    before = e.open_margin_usd()
    e.update_price(160000)
    assert e.open_margin_usd() == pytest.approx(before * 2)


@pytest.mark.asyncio
async def test_delayed_closed_history_blocks_admission_and_keeps_other_exits(cfg):

    v, e, r, f, p, s = await _funded_stack(cfg)
    v.liquidate("trace-0", 65000)
    history = v.get_closed_trades
    v.get_closed_trades = AsyncMock(return_value=[])
    assert await e.reconcile_external_closures(run_id=e.run_id, ts=minute().ts) == []
    assert e.entries_blocked_reason() == "owned_trade_outcome_unresolved"
    assert e.position_qty_sats(f"{INSTANCE}:k0") == 100
    await _continue(cfg, v, e, r, p, s, [_day(8, close=BOUNDARY, low=72000)])
    assert not v.running
    v.get_closed_trades = history
    await _continue(cfg, v, e, r, p, s, [minute()])
    assert not v.running and len(_rows(f, orders)) == 8


@pytest.mark.asyncio
async def test_inventory_read_failure_blocks_entries_without_stopping_exits(cfg):
    from lnmarkets_bot.strategy import OrderIntent

    v, e, _r, _f, _p, _s = await _funded_stack(cfg)
    v.get_running_trades = AsyncMock(side_effect=ConnectionError("inventory unavailable"))
    assert await e.reconcile_external_closures(run_id=e.run_id, ts=minute().ts) == []
    assert e.entries_blocked_reason() == "venue_inventory_unavailable"
    intent = replace(OrderIntent.exit("1d"), strategy_instance_id=INSTANCE, position_key="k1")
    order, _detail = await e.submit(
        intent=intent, signal_id=1, run_id=e.run_id, ts=minute().ts, size_usd=0, leverage=5
    )
    assert order > 0 and "trace-1" not in v.running
