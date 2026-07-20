"""Test LiveExecutor with a fake IsolatedTradesApi (no network)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from lnmarkets_bot.api.isolated import IsolatedCloseResponse, IsolatedTrade
from lnmarkets_bot.engine.live_executor import LiveExecutor, UnsafeLiveStateError
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import daily_pnl, fills, funding_fees
from lnmarkets_bot.persistence.models import orders as orders_t
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy.intents import OrderIntent


class FakeIsolatedTradesApi:
    """Fake IsolatedTradesApi for testing without the network."""

    def __init__(self):
        self.trades: list[dict] = []
        self.closes: list[str] = []
        self.running: dict[str, IsolatedTrade] = {}
        self.next_id = 1
        self.close_pl_sats = 0
        self.opening_fee_sats = 0
        self.closing_fee_sats = 0
        self.funding_rows: list[dict] = []

    async def new_trade(self, params):
        tid = f"iso-{self.next_id}"
        self.next_id += 1
        self.trades.append(
            {
                "id": tid,
                "type": params.type,
                "side": params.side,
                "quantity": params.quantity,
                "leverage": params.leverage,
            }
        )
        trade = IsolatedTrade(
            id=tid,
            type=params.type,
            side=params.side,
            quantity=params.quantity,
            leverage=params.leverage,
            price=0.0,
            opening_fee=self.opening_fee_sats,
        )
        self.running[tid] = trade
        return trade

    async def close_trade(self, trade_id):
        self.closes.append(trade_id)
        self.running.pop(trade_id, None)
        return IsolatedCloseResponse(
            id=trade_id,
            pl=self.close_pl_sats,
            raw={"id": trade_id},
            closing_fee=self.closing_fee_sats,
        )

    async def get_running_trades(self):
        return list(self.running.values())

    async def iter_funding_fees(self, _from_ts, _to_ts):
        for row in self.funding_rows:
            yield row


@pytest.fixture
def recorder(tmp_path):
    eng = make_engine(tmp_path / "live_test.sqlite")
    init_schema(eng)
    fac = make_session_factory(eng)
    return Recorder(fac)


async def test_live_executor_entry_exit(recorder, tmp_path):
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(
        trades_api=api,
        recorder=recorder,
        run_id=1,
        symbol="BTCUSD",
    )
    run_id = recorder.start_run(
        mode="paper",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    executor.run_id = run_id

    executor.update_price(50000.0)
    sig_id = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=1000,
        target_leverage=2.0,
        reason="entry long",
    )
    order_id, meta = await executor.submit(
        intent=OrderIntent.enter_long("1d", 1000.0, 2.0, reason="entry long"),
        signal_id=sig_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1000.0,
        leverage=2.0,
    )
    assert order_id > 0, f"entry order should be created (got {order_id}, meta={meta})"
    assert executor.positions["1d"].side == "long"
    # LNM uses USD 1 inverse-futures contracts, not BTC sats.
    assert executor.positions["1d"].qty_sats == 1000
    assert api.trades[0]["side"] == "buy"
    assert api.trades[0]["quantity"] == 1000
    assert executor.positions["1d"].trade_id == "iso-1"

    # Price moves up; close the position.
    executor.update_price(55000.0)
    sig_id = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="exit",
        side=None,
        reason="exit long",
    )
    order_id_exit, meta_exit = await executor.submit(
        intent=OrderIntent.exit("1d", reason="exit long"),
        signal_id=sig_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=0,
        leverage=2.0,
    )
    assert order_id_exit > 0, (
        f"exit order should be created (got {order_id_exit}, meta={meta_exit})"
    )
    assert executor.positions["1d"].side is None
    assert executor.positions["1d"].qty_sats == 0
    assert executor.positions["1d"].trade_id is None
    assert len(api.closes) == 1
    assert api.closes[0] == "iso-1"

    # Verify DB has 2 orders
    eng = recorder._factory().get_bind()  # type: ignore[attr-defined]
    fac = make_session_factory(eng)
    with fac() as s:
        n = s.execute(
            select(func.count()).select_from(orders_t).where(orders_t.c.run_id == run_id)
        ).scalar()
    assert n == 2, f"expected 2 orders, got {n}"


async def test_live_executor_reports_realized_pnl_delta(recorder):
    api = FakeIsolatedTradesApi()
    api.close_pl_sats = -1_000
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1, symbol="BTCUSD")
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    executor.update_price(50_000.0)
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long", reason="entry"
    )
    await executor.submit(
        intent=OrderIntent.enter_long("1d", 1, 1),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1,
        leverage=1,
    )
    await executor.submit(
        intent=OrderIntent.exit("1d"),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=0,
        leverage=1,
    )

    assert executor.consume_realized_pnl_usd() == pytest.approx(-0.5)
    assert executor.consume_realized_pnl_usd() == 0.0


async def test_live_executor_records_actual_trade_fees_and_net_daily_pnl(recorder):
    api = FakeIsolatedTradesApi()
    api.opening_fee_sats = 2
    api.closing_fee_sats = 3
    api.close_pl_sats = 10
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    executor.update_price(50_000.0)
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long", reason="entry"
    )
    await executor.submit(
        intent=OrderIntent.enter_long("1d", 1, 1),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1,
        leverage=1,
    )
    await executor.submit(
        intent=OrderIntent.exit("1d"),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=0,
        leverage=1,
    )
    eng = recorder._factory().get_bind()  # type: ignore[attr-defined]
    with make_session_factory(eng)() as session:
        fees = session.execute(select(fills.c.fee_sats).order_by(fills.c.id)).scalars().all()
        realized = session.execute(
            select(daily_pnl.c.realized_pnl_sats).where(daily_pnl.c.run_id == run_id)
        ).scalar_one()
    assert fees == [2, 3]
    assert realized == 5
    assert executor.consume_realized_pnl_usd() == pytest.approx(0.0025)


async def test_live_executor_closes_remote_trade_when_order_persistence_fails():
    class FailingRecorder:
        def record_order(self, *args, **kwargs):
            raise RuntimeError("database unavailable")

    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=FailingRecorder(), run_id=1)
    executor.update_price(50_000.0)

    order_id, meta = await executor.submit(
        intent=OrderIntent.enter_long("1d", 1, 1),
        signal_id=1,
        run_id=1,
        ts=datetime.now(UTC),
        size_usd=1,
        leverage=1,
    )

    assert order_id == -1
    assert meta["reason"].startswith("persistence_failed_trade_closed")
    assert api.closes == ["iso-1"]
    assert api.running == {}


async def test_same_direction_entry_is_idempotent(recorder):
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    run_id = recorder.start_run(
        mode="live", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    executor.update_price(50_000.0)
    intent = OrderIntent.enter_long("1d", 1, 1)
    await executor.submit(
        intent=intent,
        signal_id=1,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1,
        leverage=1,
    )

    order_id, meta = await executor.submit(
        intent=intent,
        signal_id=2,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1,
        leverage=1,
    )

    assert order_id == -1
    assert meta["reason"] == "position_already_open"
    assert len(api.trades) == 1


async def test_ambiguous_entry_submission_fails_closed_when_remote_trade_exists(recorder):
    class ResponseLostApi(FakeIsolatedTradesApi):
        async def new_trade(self, params):
            await super().new_trade(params)
            raise TimeoutError("response lost after remote acceptance")

    api = ResponseLostApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    executor.update_price(50_000.0)

    with pytest.raises(UnsafeLiveStateError, match="untracked remote trade"):
        await executor.submit(
            intent=OrderIntent.enter_long("1d", 1, 1),
            signal_id=1,
            run_id=1,
            ts=datetime.now(UTC),
            size_usd=1,
            leverage=1,
        )

    assert list(api.running) == ["iso-1"]


async def test_live_executor_per_tf_isolation(recorder, tmp_path):
    """1d entry should not affect 4h virtual state."""
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(
        trades_api=api,
        recorder=recorder,
        run_id=1,
        symbol="BTCUSD",
    )
    run_id = recorder.start_run(
        mode="paper",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    executor.run_id = run_id

    executor.update_price(50000.0)
    # 1d entry long
    sig_id = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=1000,
        target_leverage=2.0,
        reason="1d long",
    )
    await executor.submit(
        intent=OrderIntent.enter_long("1d", 1000.0, 2.0, reason="1d long"),
        signal_id=sig_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1000.0,
        leverage=2.0,
    )
    # 4h should be flat (no virtual state created since no 4h order was placed)
    assert executor.positions.get("4h") is None
    # 1d should be long
    assert executor.positions["1d"].side == "long"
    assert executor.positions["1d"].qty_sats == 1000


async def test_live_executor_fake_api_records_lnm_order_id(recorder, tmp_path):
    """Orders should have lnm_order_id set from the FakeTradesApi response."""
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(
        trades_api=api,
        recorder=recorder,
        run_id=1,
        symbol="BTCUSD",
    )
    run_id = recorder.start_run(
        mode="paper",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    executor.run_id = run_id
    executor.update_price(50000.0)
    sig_id = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=1000,
        target_leverage=2.0,
        reason="entry long",
    )
    await executor.submit(
        intent=OrderIntent.enter_long("1d", 1000.0, 2.0, reason="entry long"),
        signal_id=sig_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1000.0,
        leverage=2.0,
    )
    eng = recorder._factory().get_bind()  # type: ignore[attr-defined]
    fac = make_session_factory(eng)
    with fac() as s:
        order = s.execute(select(orders_t).where(orders_t.c.run_id == run_id)).first()
    assert order is not None
    assert order.lnm_order_id == "iso-1"


async def test_live_executor_reconciles_recorded_running_trade(recorder, tmp_path):
    api = FakeIsolatedTradesApi()
    original = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    run_id = recorder.start_run(
        mode="paper",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    original.run_id = run_id
    original.update_price(50_000.0)
    signal_id = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        reason="entry",
    )
    await original.submit(
        intent=OrderIntent.enter_long("1d", 1000.0, 2.0),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
        size_usd=1000.0,
        leverage=2.0,
    )

    restored = LiveExecutor(trades_api=api, recorder=recorder, run_id=run_id)
    await restored.reconcile()
    assert restored.position_side("1d") == "long"
    assert restored.positions["1d"].trade_id == "iso-1"
    assert restored.positions["1d"].entry_ts is not None


async def test_live_executor_records_each_funding_settlement_once(recorder):
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    entry_ts = datetime(2026, 7, 14, 0, tzinfo=UTC)
    executor.run_id = run_id
    executor.update_price(100_000.0)
    signal_id = recorder.record_signal(
        run_id, ts=entry_ts, kind="entry", side="long", reason="entry"
    )
    await executor.submit(
        intent=OrderIntent.enter_long("1d", 1, 1),
        signal_id=signal_id,
        run_id=run_id,
        ts=entry_ts,
        size_usd=1,
        leverage=1,
    )
    api.funding_rows = [
        {
            "tradeId": "iso-1",
            "settlementId": "settlement-1",
            "time": "2026-07-14T08:00:00.000Z",
            "fee": -2,
        }
    ]

    await executor.sync_funding(entry_ts + timedelta(hours=9))
    await executor.sync_funding(entry_ts + timedelta(hours=10))

    engine = recorder._factory().get_bind()  # type: ignore[attr-defined]
    factory = make_session_factory(engine)
    with factory() as session:
        fees = session.execute(select(funding_fees.c.fee_sats)).scalars().all()
        funding_total = session.execute(
            select(daily_pnl.c.funding_pnl_sats).where(daily_pnl.c.run_id == run_id)
        ).scalar_one()
    assert fees == [-2]
    assert funding_total == 2
