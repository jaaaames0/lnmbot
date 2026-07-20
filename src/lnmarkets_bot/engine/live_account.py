"""Authenticated account balance and read-only snapshot source for live runs."""

from __future__ import annotations

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

    def __init__(self, *, account_api: AccountApi, recorder: Recorder) -> None:
        self._api = account_api
        self._recorder = recorder

    async def balance_usd(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> float:
        balance_sats = await self.snapshot(
            run_id=run_id,
            ts=ts,
            price_usd=price_usd,
            margin_used_usd=margin_used_usd,
        )
        return balance_sats * price_usd / 1e8

    async def snapshot(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> int:
        """Fetch and record balance without participating in a sizing decision.

        LN Markets' account endpoint provides the available balance used for
        sizing. Isolated margin and unrealised P&L are intentionally not
        folded into this bot-side snapshot: the dashboard obtains those from
        running isolated trades with its own read-only credential.
        """
        account = await self._api.get_balance()
        balance_sats = int(account.get("balance", 0))
        self._recorder.record_account_snapshot(
            run_id,
            ts=ts.astimezone(UTC),
            balance_sats=balance_sats,
            equity_sats=balance_sats,
            margin_used_sats=int(margin_used_usd / price_usd * 1e8) if price_usd else 0,
            unrealized_pnl_sats=0,
        )
        _log.info("live.account_snapshot", run_id=run_id, balance_sats=balance_sats)
        return balance_sats
