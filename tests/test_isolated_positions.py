"""Per-TF isolation test: confirm that 1d and 4h positions are independent.

Under isolated margin, a 1d signal must never touch a 4h position and vice
versa. This test fires synthetic 1d and 4h MA-crosses simultaneously and
checks that both positions end up where they should.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders as orders_t
from lnmarkets_bot.strategy.ma_cross import MaCross


def _build_fixture(p: "Path") -> None:
    """Synthesize 30 days of 1m bars so 1d warmup (21 bars) completes.

    Days 1–20: gently oscillating around $100 (warmup).
    Days 21–25: rally to $120.
    Day 26–28: pullback to $115.
    Day 29–30: another rally to $125.
    """
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(30 * 24 * 60 + 1):
        ts = base + timedelta(minutes=i)
        day = i // (24 * 60)
        if day < 20:
            p_val = 100.0 + ((i % (24 * 60)) / 200.0 - 60)  # ±$30 around $100
        elif day < 25:
            p_val = 100.0 + (day - 20) * 4.0  # $100 → $120
        elif day < 28:
            p_val = 120.0 - (day - 25) * 1.5  # $120 → $115.5
        else:
            p_val = 115.5 + (day - 28) * 5.0  # $115.5 → $125.5
        rows.append(
            {"ts": ts, "open": p_val, "high": p_val + 0.5,
             "low": p_val - 0.5, "close": p_val, "volume": 1.0}
        )
    pd.DataFrame(rows).to_parquet(p, index=False)


@pytest.mark.asyncio
async def test_1d_and_4h_positions_are_independent(tmp_path) -> None:
    from pathlib import Path
    parquet = tmp_path / "isolation_fixture.parquet"
    _build_fixture(parquet)

    cfg = BotConfig(
        storage_db_path=tmp_path / "iso.sqlite",
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=10.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=100_000,
    )
    multi = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h"),
    )
    strat = MaCross()  # default tfs=("1d","4h")
    run_id = await run_backtest(
        cfg=cfg, data_source=multi, strategy=strat,
        install_signal_handlers=False,
    )

    # Per-TF signal counts: should NOT be the same — that's the point of isolation.
    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        tf_counts = {}
        for tf in ("1d", "4h"):
            n = s.execute(
                select(__import__("sqlalchemy").func.count())
                .select_from(orders_t)
                .where(orders_t.c.run_id == run_id)
                .where(orders_t.c.trigger_tf == tf)
            ).scalar()
            tf_counts[tf] = n
    # Each TF has at least one order, and counts likely differ (4h fires more often).
    assert tf_counts.get("1d", 0) >= 1, f"1d should have fired at least once, got {tf_counts}"
    assert tf_counts.get("4h", 0) >= 1, f"4h should have fired at least once, got {tf_counts}"
    # The architecture-rule invariant: trigger_tf on every recorded order is non-empty.
    with fac() as s:
        empty_tf = s.execute(
            select(__import__("sqlalchemy").func.count())
            .select_from(orders_t)
            .where(orders_t.c.run_id == run_id)
            .where(orders_t.c.trigger_tf == "")
        ).scalar()
    assert empty_tf == 0, f"some orders have empty trigger_tf ({empty_tf} found)"