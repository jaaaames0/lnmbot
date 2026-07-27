"""Authenticated account balance and read-only snapshot source for live runs."""

from __future__ import annotations

import asyncio
from datetime import UTC
from typing import TYPE_CHECKING

from ..logging import get_logger

_log = get_logger("lnmarkets_bot.engine.live_account")

if TYPE_CHECKING:
    from datetime import datetime

    from ..api.account import AccountApi
    from ..persistence.recorder import Recorder


class LiveAccountBalanceProvider:
    """Fetch balance for sizing and periodically persist an account snapshot."""

    def __init__(self, *, account_api: AccountApi, isolated_trades_api, recorder: Recorder) -> None:
        self._account_api = account_api
        self._isolated_trades_api = isolated_trades_api
        self._recorder = recorder

    async def balance_usd(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> float:
        _, equity_sats = await self._snapshot(
            run_id=run_id,
            ts=ts,
            price_usd=price_usd,
            margin_used_usd=margin_used_usd,
        )
        return equity_sats * price_usd / 1e8

    async def snapshot(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> int:
        """Fetch and record the available balance without sizing from it.

        ``balance_sats`` is the available LN Markets balance. ``equity_sats``
        additionally includes isolated margin, maintenance reserves, and
        unrealised P&L; it is used for equity-fraction sizing.
        """
        balance_sats, _ = await self._snapshot(
            run_id=run_id,
            ts=ts,
            price_usd=price_usd,
            margin_used_usd=margin_used_usd,
        )
        return balance_sats

    async def _snapshot(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> tuple[int, int]:
        account, running_trades = await asyncio.gather(
            self._account_api.get_balance(), self._isolated_trades_api.get_running_trades()
        )
        balance_sats = int(account.get("balance", 0))
        margin_sats = sum(int(trade.margin or 0) for trade in running_trades)
        maintenance_sats = sum(int(trade.maintenance_margin or 0) for trade in running_trades)
        unrealized_pnl_sats = sum(int(trade.pl or 0) for trade in running_trades)
        equity_sats = balance_sats + margin_sats + maintenance_sats + unrealized_pnl_sats
        self._recorder.record_account_snapshot(
            run_id,
            ts=ts.astimezone(UTC),
            balance_sats=balance_sats,
            equity_sats=equity_sats,
            margin_used_sats=margin_sats + maintenance_sats,
            unrealized_pnl_sats=unrealized_pnl_sats,
        )
        _log.info(
            "live.account_snapshot",
            run_id=run_id,
            balance_sats=balance_sats,
            equity_sats=equity_sats,
        )
        return balance_sats, equity_sats
