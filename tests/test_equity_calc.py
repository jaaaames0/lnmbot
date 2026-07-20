"""Equity calc unit test.

Lock the convention: balance_sats and equity_sats are USD × 1e8 (i.e. micro-USD).
A position of `qty_sats` BTC-sats at price close_usd has a notional USD value
of `qty_sats * close / 1e8`. In micro-USD that's `qty_sats * close`.

Equity = balance + position_notional + unrealized_pnl. So:
    equity_sats = balance_sats + qty_sats_signed * close
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots
from lnmarkets_bot.strategy import Bar, Strategy, StrategyState, OrderIntent
from lnmarkets_bot.strategy.intents import Side, SignalKind


class _StaticStrategy(Strategy):
    """Test strategy: enters a fixed long at bar 0, never exits."""

    def __init__(self, params=None) -> None:
        super().__init__(params)
        self._emitted = False

    def on_startup(self, state: StrategyState) -> None:
        return None

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        if not self._emitted:
            self._emitted = True
            return [OrderIntent.enter_long(
                trigger_tf="1d", size_usd=1_000.0, leverage=1.0,
                reason="test entry",
            )]
        return []


@pytest.mark.asyncio
async def test_equity_tracks_balance_plus_position_notional(
    tmp_path,
) -> None:
    # Build a 5-bar 1m fixture where close prices are easy to assert.
    parquet = tmp_path / "equity_fixture.parquet"
    import pandas as pd

    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    rows = [
        {"ts": base, "open": 100.0, "high": 101.0, "low": 99.0,
         "close": 100.0, "volume": 1.0},
        {"ts": base.replace(minute=1), "open": 100.0, "high": 101.0, "low": 99.0,
         "close": 110.0, "volume": 1.0},
        {"ts": base.replace(minute=2), "open": 110.0, "high": 111.0, "low": 109.0,
         "close": 105.0, "volume": 1.0},
        {"ts": base.replace(minute=3), "open": 105.0, "high": 106.0, "low": 104.0,
         "close": 100.0, "volume": 1.0},
        {"ts": base.replace(minute=4), "open": 100.0, "high": 101.0, "low": 99.0,
         "close": 120.0, "volume": 1.0},
    ]
    pd.DataFrame(rows).to_parquet(parquet, index=False)

    cfg = BotConfig(
        storage_db_path=tmp_path / "eq.sqlite",
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=5.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=10_000,
    )

    multi = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h", "1h"),
    )
    run_id = await run_backtest(
        cfg=cfg, data_source=multi, strategy=_StaticStrategy(),
        install_signal_handlers=False,
    )

    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        snaps = s.execute(
            select(account_snapshots.c.balance_sats, account_snapshots.c.equity_sats, account_snapshots.c.ts)
            .where(account_snapshots.c.run_id == run_id)
            .order_by(account_snapshots.c.ts.asc())
        ).fetchall()

    # balance_sats is fixed at $10,000 = 10_000 * 1e8
    assert all(r.balance_sats == 10_000 * 1e8 for r in snaps), (
        f"balance_sats should be constant at $10k; got: "
        f"{[r.balance_sats for r in snaps]}"
    )

    # The strategy emits an entry on bar 0 at close=100. After that, the position
    # is long. The last snapshot's equity should be balance + position_notional
    # at the last 1m close.
    from lnmarkets_bot.persistence.models import fills, orders as orders_t

    with fac() as s:
        last_fill = s.execute(
            select(fills.c.qty_sats, fills.c.price_usd)
            .join(orders_t, fills.c.order_id == orders_t.c.id)
            .where(orders_t.c.run_id == run_id)
            .order_by(fills.c.ts.desc())
            .limit(1)
        ).first()
    assert last_fill is not None
    last_qty, last_fill_price = last_fill.qty_sats, last_fill.price_usd

    last = snaps[-1]
    # The last 1m close in the fixture is 120 (the 5th row).
    expected_equity_sats_at_last = int(10_000 * 1e8 + last_qty * 120.0)
    assert abs(last.equity_sats - expected_equity_sats_at_last) < 10_000_000, (
        f"final equity_sats {last.equity_sats} != "
        f"balance + qty*last_close = {expected_equity_sats_at_last}; "
        f"qty={last_qty}, fill_price={last_fill_price}"
    )


@pytest.mark.asyncio
async def test_equity_does_not_wildly_exceed_balance_when_flat(
    tmp_path,
) -> None:
    """When the strategy does nothing, equity should equal balance at every bar."""
    parquet = tmp_path / "flat_fixture.parquet"
    import pandas as pd
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {"ts": base, "open": 100.0, "high": 101.0, "low": 99.0,
         "close": 100.0 + i, "volume": 1.0}
        for i in range(5)
    ]
    pd.DataFrame(rows).to_parquet(parquet, index=False)

    cfg = BotConfig(
        storage_db_path=tmp_path / "eq_flat.sqlite",
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0, risk_max_leverage=5.0,
        risk_max_daily_loss_usd=1_000_000.0, risk_max_orders_per_minute=10_000,
    )

    multi = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h", "1h"),
    )
    from lnmarkets_bot.strategy import DoNothing
    run_id = await run_backtest(
        cfg=cfg, data_source=multi, strategy=DoNothing(),
        install_signal_handlers=False,
    )

    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        snaps = s.execute(
            select(account_snapshots.c.balance_sats, account_snapshots.c.equity_sats)
            .where(account_snapshots.c.run_id == run_id)
        ).fetchall()

    # Flat position: equity_sats must equal balance_sats at every bar.
    expected = 10_000 * 1e8
    mismatches = [(r.balance_sats, r.equity_sats) for r in snaps
                  if r.balance_sats != r.equity_sats]
    assert not mismatches, (
        f"with no position, equity_sats must equal balance_sats everywhere; "
        f"mismatches: {mismatches[:5]}"
    )
    assert all(r.balance_sats == expected for r in snaps)