"""End-to-end MA execution with a local candle stream and fake venue API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import select

from lnmarkets_bot.api.isolated import IsolatedCloseResponse, IsolatedTrade
from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import MockLiveStream, MultiTimeframeDataSource
from lnmarkets_bot.engine.live import run_paper
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders as orders_t
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy.ma_cross import MaCross


class FakeIsolatedTradesApi:
    """Expose the inventory reads required before the executor admits entries."""

    def __init__(self) -> None:
        self.trades: list[IsolatedTrade] = []
        self.running: dict[str, IsolatedTrade] = {}
        self.closed: list[str] = []

    async def new_trade(self, params) -> IsolatedTrade:
        trade = IsolatedTrade(
            id=f"iso-{len(self.trades) + 1}",
            type=params.type,
            side=params.side,
            quantity=params.quantity,
            leverage=params.leverage,
            price=0.0,
        )
        self.trades.append(trade)
        self.running[trade.id] = trade
        return trade

    async def close_trade(self, trade_id: str) -> IsolatedCloseResponse:
        self.closed.append(trade_id)
        self.running.pop(trade_id, None)
        return IsolatedCloseResponse(id=trade_id, pl=0, raw={"id": trade_id})

    async def get_running_trades(self) -> list[IsolatedTrade]:
        return list(self.running.values())

    async def get_closed_trades(self) -> list[IsolatedTrade]:
        return []

    async def iter_funding_fees(self, _from_ts, _to_ts):
        if False:
            yield None


def _synthetic_candles(path) -> None:
    """Sparse minute input yields 4h and daily closes without a large cache fixture."""
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(30 * 6 + 1):
        day = index // 6
        price = 100.0 if day < 24 else 120.0 if day < 27 else 80.0
        rows.append(
            {
                "ts": start + timedelta(hours=4 * index),
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "volume": 1.0,
            }
        )
    pd.DataFrame(rows).to_parquet(path, index=False)


async def _run_with_fake_api(tmp_path):
    candles = tmp_path / "candles.parquet"
    _synthetic_candles(candles)
    cfg = BotConfig(
        storage_db_path=tmp_path / "live.sqlite",
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=10.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=10_000,
    )
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    sessions = make_session_factory(engine)
    recorder = Recorder(sessions)
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=-1, symbol="BTCUSD")
    stream = MultiTimeframeDataSource(
        MockLiveStream(candles, seconds_per_bar=0.0, loop_forever=False),
        higher_timeframes=("1d", "4h"),
    )
    run_id = await run_paper(
        cfg=cfg,
        data_source=stream,
        strategy=MaCross(),
        install_signal_handlers=False,
        executor_factory=lambda: executor,
    )
    with sessions() as session:
        rows = session.execute(select(orders_t).where(orders_t.c.run_id == run_id)).mappings().all()
    return run_id, api, rows


@pytest.mark.asyncio
async def test_live_engine_with_fake_api(tmp_path) -> None:
    run_id, api, orders = await _run_with_fake_api(tmp_path)
    assert run_id > 0
    assert orders
    assert api.trades
    assert all(row["lnm_order_id"] for row in orders)


@pytest.mark.asyncio
async def test_live_engine_per_tf_isolation(tmp_path) -> None:
    _, api, orders = await _run_with_fake_api(tmp_path)
    assert {row["trigger_tf"] for row in orders} == {"1d", "4h"}
    assert len({trade.id for trade in api.trades}) == len(api.trades)
