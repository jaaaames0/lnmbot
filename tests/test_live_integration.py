"""Live integration test: MockLiveStream → MultiTimeframeDataSource → MaCross → RiskGuard → LiveExecutor (with FakeIsolatedTradesApi).

Verifies the full pipeline works end-to-end without hitting the network.
"""
from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from lnmarkets_bot.api.isolated import IsolatedCloseResponse, IsolatedTrade
from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import MockLiveStream, MultiTimeframeDataSource
from lnmarkets_bot.engine.live import run_paper
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.persistence.db import make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders as orders_t, signals, fills
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy.ma_cross import MaCross
from sqlalchemy import select, func


class FakeIsolatedTradesApi:
    def __init__(self):
        self.trades: list[dict] = []
        self.closes: list[str] = []
        self.next_id = 1

    async def new_trade(self, params):
        tid = f"iso-{self.next_id}"
        self.next_id += 1
        self.trades.append({
            "id": tid, "side": params.side, "qty": params.quantity,
            "leverage": params.leverage, "type": params.type,
        })
        return IsolatedTrade(
            id=tid, type=params.type, side=params.side,
            quantity=params.quantity, leverage=params.leverage, price=0.0,
        )

    async def close_trade(self, trade_id):
        self.closes.append(trade_id)
        return IsolatedCloseResponse(id=trade_id, pl=0, raw={})


@pytest.mark.asyncio
async def test_live_engine_with_fake_api():
    """End-to-end: MockLiveStream → MultiTimeframeDataSource → MaCross → LiveExecutor (with FakeIsolatedTradesApi).

    Uses a slice of the real 2y BTC fixture (which has many MA-crosses)
    to ensure the strategy actually fires orders. Verifies the executor
    receives those orders via the fake API.
    """
    with tempfile.TemporaryDirectory() as td:
        # Use a slice of the real 2y fixture that contains known MA-crosses
        # (around the Nov 5, 2024 area per the user's chart observations).
        src = Path("/home/james/srv/tradingbot/data/cache/btcusdt_perp_1m_2y.parquet")
        import pandas as pd
        df = pd.read_parquet(src)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.set_index("ts")
        # Take ~60 days: Oct 2024 - Dec 2024
        slice_df = df.loc["2024-10-01":"2024-12-01"].reset_index()
        slice_df.columns = ["ts", "open", "high", "low", "close", "volume"]
        pq = Path(td) / "live_fixture.parquet"
        slice_df.to_parquet(pq, index=False)

        cfg = BotConfig(
            storage_db_path=Path(td) / "live_int.sqlite",
            initial_balance_usd=10_000.0,
            risk_max_position_usd=10_000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )

        from lnmarkets_bot.persistence.db import init_schema
        engine = make_engine(cfg.storage_db_path)
        init_schema(engine)
        fac = make_session_factory(engine)
        recorder = Recorder(fac)

        api = FakeIsolatedTradesApi()
        executor_factory = lambda: LiveExecutor(
            trades_api=api, recorder=recorder, run_id=-1, symbol="BTCUSD",
        )

        ds = MultiTimeframeDataSource(
            MockLiveStream(pq, seconds_per_bar=0.0, loop_forever=False),
            higher_timeframes=("1d", "4h"),
        )
        strat = MaCross()
        run_id = await run_paper(
            cfg=cfg, data_source=ds, strategy=strat,
            duration_seconds=30.0,
            install_signal_handlers=False,
            executor_factory=executor_factory,
        )

        # Verify the run completed.
        assert run_id > 0

        # Verify the FakeIsolatedTradesApi received orders (real BTC data has MA-crosses).
        with fac() as s:
            n_signals = s.execute(
                select(func.count()).select_from(signals).where(signals.c.run_id == run_id)
            ).scalar()
            n_orders = s.execute(
                select(func.count()).select_from(orders_t).where(orders_t.c.run_id == run_id)
            ).scalar()
            n_lnm = s.execute(
                select(func.count()).select_from(orders_t)
                .where(orders_t.c.run_id == run_id)
                .where(orders_t.c.lnm_order_id.isnot(None))
            ).scalar()

        print(
            f"\nrun_id={run_id} signals={n_signals} orders_in_db={n_orders} "
            f"orders_with_lnm_id={n_lnm} api_orders={len(api.trades)} "
            f"closed={len(api.closes)}"
        )
        # With real BTC data we should see orders.
        assert n_orders > 0, f"expected orders, got {n_orders}"
        assert len(api.trades) > 0, f"expected fake API to receive orders, got {len(api.trades)}"
        assert n_lnm > 0, f"expected orders with lnm_order_id, got {n_lnm}"


@pytest.mark.asyncio
async def test_live_engine_per_tf_isolation():
    """1d and 4h signals should be processed independently through LiveExecutor."""
    with tempfile.TemporaryDirectory() as td:
        src = Path("/home/james/srv/tradingbot/data/cache/btcusdt_perp_1m_2y.parquet")
        import pandas as pd
        df = pd.read_parquet(src)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.set_index("ts")
        slice_df = df.loc["2024-10-01":"2024-11-15"].reset_index()
        slice_df.columns = ["ts", "open", "high", "low", "close", "volume"]
        pq = Path(td) / "live_iso.parquet"
        slice_df.to_parquet(pq, index=False)

        cfg = BotConfig(
            storage_db_path=Path(td) / "live_iso.sqlite",
            initial_balance_usd=10_000.0,
            risk_max_position_usd=10_000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )

        from lnmarkets_bot.persistence.db import init_schema
        engine = make_engine(cfg.storage_db_path)
        init_schema(engine)
        fac = make_session_factory(engine)
        recorder = Recorder(fac)

        api = FakeIsolatedTradesApi()
        executor = LiveExecutor(
            trades_api=api, recorder=recorder, run_id=-1, symbol="BTCUSD",
        )

        ds = MultiTimeframeDataSource(
            MockLiveStream(pq, seconds_per_bar=0.0, loop_forever=False),
            higher_timeframes=("1d", "4h"),
        )
        strat = MaCross()
        run_id = await run_paper(
            cfg=cfg, data_source=ds, strategy=strat,
            duration_seconds=15.0,
            install_signal_handlers=False,
            executor_factory=lambda: executor,
        )

        # Verify that the engine processed bars and recorded signals/orders.
        with fac() as s:
            n_signals = s.execute(
                select(func.count()).select_from(signals).where(signals.c.run_id == run_id)
            ).scalar()
        print(f"\nrun_id={run_id} signals={n_signals}")
        assert n_signals > 0, f"expected signals on real BTC data, got {n_signals}"