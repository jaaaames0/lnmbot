"""MultiTimeframeDataSource — boundary ordering, count, and round-trip with v0 strategy."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from sqlalchemy import func, select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.data.source import DataSource
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders as orders_t
from lnmarkets_bot.persistence.models import signals as signals_t
from lnmarkets_bot.strategy import Bar

if TYPE_CHECKING:
    from pathlib import Path


def _make_fixture(p: Path, *, n_minutes: int = 60 * 24 + 1) -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(n_minutes):
        ts = base + timedelta(minutes=i)
        rows.append(
            {
                "ts": ts,
                "open": 100.0 + (i % 60) / 1000.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0 + ((i + 30) % 60) / 1000.0,
                "volume": 1.0,
            }
        )
    pd.DataFrame(rows).to_parquet(p, index=False)


def _collect(multi: MultiTimeframeDataSource):
    counts: dict[str, int] = {"1m": 0, "5m": 0, "1h": 0, "4h": 0, "1d": 0}
    by_ts: dict[object, list[str]] = defaultdict(list)

    async def run():
        async for b in multi.stream():
            counts[b.timeframe] += 1
            by_ts[b.ts].append(b.timeframe)

    asyncio.run(run())
    return counts, by_ts


def test_counts_24h_fixture(tmp_path: Path) -> None:
    p = tmp_path / "tiny.parquet"
    _make_fixture(p)
    replay = BacktestReplay(p, cadence="instant")
    multi = MultiTimeframeDataSource(replay, higher_timeframes=("1d", "4h", "1h"))
    counts, _ = _collect(multi)
    assert counts["1d"] == 1
    assert counts["4h"] == 6
    assert counts["1h"] == 24
    assert counts["1m"] == 60 * 24 + 1


def test_ordering_at_daily_boundary(tmp_path: Path) -> None:
    p = tmp_path / "tiny.parquet"
    _make_fixture(p)
    replay = BacktestReplay(p, cadence="instant")
    multi = MultiTimeframeDataSource(replay, higher_timeframes=("1d", "4h", "1h"))
    _, by_ts = _collect(multi)
    daily_boundaries = [ts for ts, tfs in by_ts.items() if set(tfs) == {"1d", "4h", "1h", "1m"}]
    assert len(daily_boundaries) == 1
    ts = daily_boundaries[0]
    seq = by_ts[ts]
    assert seq.index("1d") < seq.index("4h") < seq.index("1h") < seq.index("1m")


def test_4h_only_boundary_count(tmp_path: Path) -> None:
    """4h that doesn't coincide with 1d: 5 occurrences (at 04, 08, 12, 16, 20 UTC)."""
    p = tmp_path / "tiny.parquet"
    _make_fixture(p)
    replay = BacktestReplay(p, cadence="instant")
    multi = MultiTimeframeDataSource(replay, higher_timeframes=("1d", "4h", "1h"))
    _, by_ts = _collect(multi)
    n = sum(1 for tfs in by_ts.values() if set(tfs) == {"4h", "1h", "1m"})
    assert n == 5


def test_5m_boundary_count_and_ordering(tmp_path: Path) -> None:
    p = tmp_path / "five-minute.parquet"
    _make_fixture(p, n_minutes=16)
    replay = BacktestReplay(p, cadence="instant")
    multi = MultiTimeframeDataSource(replay, higher_timeframes=("5m",))
    counts, by_ts = _collect(multi)

    assert counts["5m"] == 3
    boundary = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    assert by_ts[boundary] == ["5m", "1m"]


def test_do_nothing_strategy_emits_zero_signals_via_multi_tf(
    cfg: BotConfig, small_fixture_path, tmp_path
) -> None:
    """The v0 do_nothing strategy still produces zero signals when the engine is fed
    a multi-TF stream. Architectural rule preserved."""
    cfg2 = BotConfig(**{**cfg.model_dump(), "storage_db_path": tmp_path / "mtf.sqlite"})
    replay = BacktestReplay(small_fixture_path, cadence="instant")
    multi = MultiTimeframeDataSource(replay, higher_timeframes=("1d", "4h", "1h"))
    from lnmarkets_bot.strategy import import_strategy

    strat = import_strategy(cfg.strategy)
    run_id = asyncio.run(
        run_backtest(cfg=cfg2, data_source=multi, strategy=strat, install_signal_handlers=False)
    )
    eng = make_engine(cfg2.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        n_sig = s.execute(
            select(func.count()).select_from(signals_t).where(signals_t.c.run_id == run_id)
        ).scalar()
        n_ord = s.execute(
            select(func.count()).select_from(orders_t).where(orders_t.c.run_id == run_id)
        ).scalar()
        # Bars from multi-TF stream include the higher-TF bars.
        n_bars = s.execute(
            select(func.count())
            .select_from(__import__("lnmarkets_bot.persistence.models", fromlist=["bars"]).bars)
            .where(
                __import__("lnmarkets_bot.persistence.models", fromlist=["bars"]).bars.c.run_id
                == run_id
            )
        ).scalar()
    assert n_sig == 0
    assert n_ord == 0
    assert n_bars > 0  # at minimum the 1m bars from the small fixture


def test_bar_carries_timeframe_field() -> None:
    b = Bar(
        ts=datetime.now(UTC),
        open=1,
        high=2,
        low=0.5,
        close=1.5,
        volume=1.0,
        timeframe="4h",
    )
    assert b.timeframe == "4h"
    b2 = Bar(
        ts=datetime.now(UTC),
        open=1,
        high=2,
        low=0.5,
        close=1.5,
        volume=1.0,
    )
    assert b2.timeframe == "1m"  # default preserves backward compat


class _StreamingBars(DataSource):
    """Deliberately exposes neither .path nor ._load (like a real stream)."""

    def __init__(self, bars: list[Bar]) -> None:
        self._input = bars

    async def stream(self):
        for bar in self._input:
            yield bar


@pytest.mark.asyncio
async def test_incremental_stream_emits_closed_higher_tf_before_base_bar() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    bars = [
        Bar(
            ts=base + timedelta(minutes=minute),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=1,
            timeframe="1m",
            warmup=True,
        )
        for minute in range(4 * 60 + 1)
    ]
    source = MultiTimeframeDataSource(_StreamingBars(bars), higher_timeframes=("4h",))
    emitted = [bar async for bar in source.stream()]
    boundary = [bar for bar in emitted if bar.ts == base + timedelta(hours=4)]
    assert [bar.timeframe for bar in boundary] == ["4h", "1m"]
    assert boundary[0].warmup


@pytest.mark.asyncio
async def test_incremental_stream_emits_closed_5m_before_base_bar() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    bars = [
        Bar(
            ts=base + timedelta(minutes=minute),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=1,
            timeframe="1m",
            warmup=True,
        )
        for minute in range(6)
    ]
    source = MultiTimeframeDataSource(_StreamingBars(bars), higher_timeframes=("5m",))
    emitted = [bar async for bar in source.stream()]
    boundary = [bar for bar in emitted if bar.ts == base + timedelta(minutes=5)]
    assert [bar.timeframe for bar in boundary] == ["5m", "1m"]


@pytest.mark.asyncio
async def test_incremental_stream_closes_5m_without_waiting_for_next_base_bar() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    bars = [
        Bar(
            ts=base + timedelta(minutes=minute),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=1,
            timeframe="1m",
            warmup=True,
        )
        for minute in range(5)
    ]
    source = MultiTimeframeDataSource(_StreamingBars(bars), higher_timeframes=("5m",))
    emitted = [bar async for bar in source.stream()]

    assert emitted[-1].timeframe == "5m"
    assert emitted[-1].ts == base + timedelta(minutes=5)
