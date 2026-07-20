"""Shared pytest fixtures."""
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.sqlite"


@pytest.fixture
def cfg(tmp_db_path: Path) -> BotConfig:
    return BotConfig(
        storage_db_path=tmp_db_path,
        risk_max_position_usd=1000.0,
        risk_max_leverage=5.0,
        risk_max_daily_loss_usd=200.0,
        risk_max_orders_per_minute=10,
        strategy="lnmarkets_bot.strategy.do_nothing:DoNothing",
    )


@pytest.fixture
def recorder(tmp_db_path: Path) -> Iterator[Recorder]:
    eng = make_engine(tmp_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    yield Recorder(fac)


@pytest.fixture
def small_fixture_path(tmp_path: Path) -> Path:
    """Build a tiny parquet of 10 OHLCV bars."""
    p = tmp_path / "fixture.parquet"
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(10):
        ts = base + timedelta(minutes=i)
        rows.append(
            {
                "ts": ts,
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1.0,
            }
        )
    df = pd.DataFrame(rows)
    df.to_parquet(p, index=False)
    return p
