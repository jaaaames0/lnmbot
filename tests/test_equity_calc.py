"""BTC equity is cash plus unrealized P&L; opening exposure is not income.

These fixtures exercise the legacy linear BTC-quantity simulator, not native
LN Markets inverse contracts or isolated liquidation.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots
from lnmarkets_bot.strategy import Bar, OrderIntent, Strategy, StrategyState


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
            return [
                OrderIntent.enter_long(
                    trigger_tf="1d",
                    size_usd=1_000.0,
                    leverage=1.0,
                    reason="test entry",
                )
            ]
        return []


@pytest.mark.asyncio
async def test_equity_tracks_btc_cash_plus_unrealized_pnl(
    tmp_path,
) -> None:
    # Build a 5-bar 1m fixture where close prices are easy to assert.
    parquet = tmp_path / "equity_fixture.parquet"
    import pandas as pd

    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    rows = [
        {"ts": base, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0},
        {
            "ts": base.replace(minute=1),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 110.0,
            "volume": 1.0,
        },
        {
            "ts": base.replace(minute=2),
            "open": 110.0,
            "high": 111.0,
            "low": 109.0,
            "close": 105.0,
            "volume": 1.0,
        },
        {
            "ts": base.replace(minute=3),
            "open": 105.0,
            "high": 106.0,
            "low": 104.0,
            "close": 100.0,
            "volume": 1.0,
        },
        {
            "ts": base.replace(minute=4),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 120.0,
            "volume": 1.0,
        },
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
        cfg=cfg,
        data_source=multi,
        strategy=_StaticStrategy(),
        install_signal_handlers=False,
    )

    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        snaps = s.execute(
            select(
                account_snapshots.c.balance_sats,
                account_snapshots.c.equity_sats,
                account_snapshots.c.ts,
            )
            .where(account_snapshots.c.run_id == run_id)
            .order_by(account_snapshots.c.ts.asc())
        ).fetchall()

    # The strategy emits an entry on bar 0 at close=100. After that, the position
    # is long. The last snapshot's equity should be balance + position_notional
    # at the last 1m close.
    from lnmarkets_bot.persistence.models import fills
    from lnmarkets_bot.persistence.models import orders as orders_t

    with fac() as s:
        last_fill = s.execute(
            select(fills.c.qty_sats, fills.c.price_usd, fills.c.fee_sats)
            .join(orders_t, fills.c.order_id == orders_t.c.id)
            .where(orders_t.c.run_id == run_id)
            .order_by(fills.c.ts.desc())
            .limit(1)
        ).first()
    assert last_fill is not None
    last_qty, last_fill_price = last_fill.qty_sats, last_fill.price_usd

    initial_cash = int(10_000 / 100 * 1e8)
    expected_cash = initial_cash - last_fill.fee_sats
    assert all(r.balance_sats == expected_cash for r in snaps)
    # Entry has only spread loss and fees; it cannot add its notional to equity.
    first_unrealized = int(last_qty * (100 - last_fill_price) / 100)
    assert snaps[0].equity_sats == expected_cash + first_unrealized
    assert snaps[0].equity_sats < initial_cash
    expected_unrealized = int(last_qty * (120 - last_fill_price) / 120)
    assert snaps[-1].equity_sats == expected_cash + expected_unrealized


@pytest.mark.asyncio
async def test_equity_does_not_wildly_exceed_balance_when_flat(
    tmp_path,
) -> None:
    """When the strategy does nothing, equity should equal balance at every bar."""
    parquet = tmp_path / "flat_fixture.parquet"
    import pandas as pd

    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {"ts": base, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0 + i, "volume": 1.0}
        for i in range(5)
    ]
    pd.DataFrame(rows).to_parquet(parquet, index=False)

    cfg = BotConfig(
        storage_db_path=tmp_path / "eq_flat.sqlite",
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
    from lnmarkets_bot.strategy import DoNothing

    run_id = await run_backtest(
        cfg=cfg,
        data_source=multi,
        strategy=DoNothing(),
        install_signal_handlers=False,
    )

    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    with fac() as s:
        snaps = s.execute(
            select(account_snapshots.c.balance_sats, account_snapshots.c.equity_sats).where(
                account_snapshots.c.run_id == run_id
            )
        ).fetchall()

    # Flat position: equity_sats must equal balance_sats at every bar.
    expected = int(10_000 / 100 * 1e8)
    mismatches = [(r.balance_sats, r.equity_sats) for r in snaps if r.balance_sats != r.equity_sats]
    assert not mismatches, (
        f"with no position, equity_sats must equal balance_sats everywhere; "
        f"mismatches: {mismatches[:5]}"
    )
    assert all(r.balance_sats == expected for r in snaps)
