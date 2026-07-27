"""Live account snapshots must remain independent from sizing decisions."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from lnmarkets_bot.engine.live_account import LiveAccountBalanceProvider
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots
from lnmarkets_bot.persistence.recorder import Recorder


class _AccountApi:
    async def get_balance(self):
        return {"balance": 12_345}


class _IsolatedTradesApi:
    async def get_running_trades(self):
        return []


@pytest.mark.asyncio
async def test_live_account_snapshot_records_settled_balance(tmp_path):
    engine = make_engine(tmp_path / "account.sqlite")
    init_schema(engine)
    recorder = Recorder(make_session_factory(engine))
    run_id = recorder.start_run(
        mode="live",
        strategy_name="test",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    provider = LiveAccountBalanceProvider(
        account_api=_AccountApi(), isolated_trades_api=_IsolatedTradesApi(), recorder=recorder
    )

    balance = await provider.snapshot(
        run_id=run_id,
        ts=datetime(2026, 7, 16, tzinfo=UTC),
        price_usd=100_000.0,
        margin_used_usd=50.0,
    )

    with make_session_factory(engine)() as session:
        row = session.execute(select(account_snapshots)).one()
    assert balance == 12_345
    assert row.balance_sats == 12_345
    assert row.equity_sats == 12_345
    assert row.margin_used_sats == 0
    assert row.unrealized_pnl_sats == 0


@pytest.mark.asyncio
async def test_live_account_balance_uses_total_isolated_equity(tmp_path):
    class _Trade:
        margin = 1_000
        maintenance_margin = 10
        pl = -50

    class _TradesApi:
        async def get_running_trades(self):
            return [_Trade()]

    engine = make_engine(tmp_path / "equity.sqlite")
    init_schema(engine)
    recorder = Recorder(make_session_factory(engine))
    run_id = recorder.start_run(
        mode="live",
        strategy_name="test",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    provider = LiveAccountBalanceProvider(
        account_api=_AccountApi(), isolated_trades_api=_TradesApi(), recorder=recorder
    )

    equity_usd = await provider.balance_usd(
        run_id=run_id,
        ts=datetime(2026, 7, 16, tzinfo=UTC),
        price_usd=100_000.0,
        margin_used_usd=0.0,
    )

    assert equity_usd == pytest.approx(13_305 * 100_000.0 / 1e8)
    with make_session_factory(engine)() as session:
        row = session.execute(select(account_snapshots)).one()
    assert row.balance_sats == 12_345
    assert row.equity_sats == 13_305
    assert row.margin_used_sats == 1_010
    assert row.unrealized_pnl_sats == -50
