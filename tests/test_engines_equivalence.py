"""The load-bearing proof: same strategy + same data + different engines =
identical behaviour.

This test runs the do_nothing strategy through both engines using the same
parquet fixture, then asserts that the recorded signal/order counts match.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MockLiveStream
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.engine.live import run_paper
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders, runs, signals
from lnmarkets_bot.strategy import import_strategy
from sqlalchemy import func, select


def _counts_for_run(s, run_id: int) -> dict[str, int]:
    return {
        "runs": s.execute(select(func.count()).select_from(runs).where(runs.c.id == run_id)).scalar(),
        "signals": s.execute(select(func.count()).select_from(signals).where(signals.c.run_id == run_id)).scalar(),
        "orders": s.execute(select(func.count()).select_from(orders).where(orders.c.run_id == run_id)).scalar(),
    }


@pytest.mark.asyncio
async def test_backtest_and_paper_use_same_strategy_code(
    cfg: BotConfig, small_fixture_path, tmp_path
) -> None:
    cfg1 = BotConfig(**{**cfg.model_dump(), "storage_db_path": tmp_path / "bt.sqlite"})
    cfg2 = BotConfig(**{**cfg.model_dump(), "storage_db_path": tmp_path / "ppr.sqlite"})

    strat = import_strategy(cfg.strategy)

    bt_run = await run_backtest(
        cfg=cfg1, data_source=BacktestReplay(small_fixture_path, cadence="instant"),
        strategy=type(strat)(),
        install_signal_handlers=False,
    )
    ppr_run = await run_paper(
        cfg=cfg2, data_source=MockLiveStream(small_fixture_path, seconds_per_bar=0.01, loop_forever=False),
        strategy=type(strat)(),
        duration_seconds=2.0,
        install_signal_handlers=False,
    )

    for cfg_path, run_id, label in (
        (cfg1.storage_db_path, bt_run, "backtest"),
        (cfg2.storage_db_path, ppr_run, "paper"),
    ):
        engine = make_engine(cfg_path)
        init_schema(engine)
        fac = make_session_factory(engine)
        with fac() as s:
            counts = _counts_for_run(s, run_id)
            assert counts["signals"] == 0, f"{label} emitted signals unexpectedly"
            assert counts["orders"] == 0, f"{label} placed orders unexpectedly"


@pytest.mark.asyncio
async def test_same_strategy_class_used_in_both_engines(small_fixture_path, cfg: BotConfig, tmp_path) -> None:
    """The same Strategy implementation must be imported & executed in both modes.

    Both DBs independently start at run_id=1, but that's not what we're proving —
    what we're proving is that both runs reference the same strategy class.
    """
    cfg1 = BotConfig(**{**cfg.model_dump(), "storage_db_path": tmp_path / "a.sqlite"})
    cfg2 = BotConfig(**{**cfg.model_dump(), "storage_db_path": tmp_path / "b.sqlite"})

    StratClass = type(import_strategy(cfg.strategy))
    bt = await run_backtest(
        cfg=cfg1, data_source=BacktestReplay(small_fixture_path, cadence="instant"),
        strategy=StratClass(), install_signal_handlers=False,
    )
    ppr = await run_paper(
        cfg=cfg2, data_source=MockLiveStream(small_fixture_path, seconds_per_bar=0.01, loop_forever=False),
        strategy=StratClass(), duration_seconds=2.0, install_signal_handlers=False,
    )
    bt_strategy_name: str | None = None
    ppr_strategy_name: str | None = None
    for cfg_path, run_id, label in (
        (cfg1.storage_db_path, bt, "backtest"),
        (cfg2.storage_db_path, ppr, "paper"),
    ):
        eng = make_engine(cfg_path)
        fac = make_session_factory(eng)
        with fac() as s:
            row = s.execute(select(runs).where(runs.c.id == run_id)).first()
            assert row is not None, f"{label} run row missing"
            assert row.strategy_name.endswith("DoNothing"), row.strategy_name
            if label == "backtest":
                bt_strategy_name = row.strategy_name
            else:
                ppr_strategy_name = row.strategy_name
            assert row.mode in ("backtest", "paper")
    # The architectural rule: both engines wrote the same strategy_name in their runs row.
    assert bt_strategy_name == ppr_strategy_name
