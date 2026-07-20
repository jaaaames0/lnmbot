"""Market data endpoints — candles, ticker, funding.

These are read-only and public + private alike. Cross-margin-flavoured.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from .client import LnmRestClient


class MarketApi:
    def __init__(self, client: LnmRestClient) -> None:
        self._c = client

    async def get_candles(
        self,
        symbol: str = "BTCUSD",
        *,
        interval: str = "1m",
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if from_ts is not None:
            params["from"] = from_ts.isoformat()
        if to_ts is not None:
            params["to"] = to_ts.isoformat()
        resp = await self._c.get("/futures/candles", params=params)
        return resp.get("data", []) if isinstance(resp, dict) else resp or []

    async def iter_candles(
        self,
        symbol: str = "BTCUSD",
        *,
        interval: str = "1m",
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate the paginated candle endpoint in chronological pages."""
        params: dict[str, Any] = {"symbol": symbol, "interval": interval}
        if from_ts is not None:
            params["from"] = from_ts.isoformat()
        if to_ts is not None:
            params["to"] = to_ts.isoformat()
        async for candle in self._c.iter_list("/futures/candles", params=params):
            if isinstance(candle, dict):
                yield candle

    async def funding_settlements(
        self,
        symbol: str = "BTCUSD",
        *,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol}
        if from_ts is not None:
            params["from"] = from_ts.isoformat()
        if to_ts is not None:
            params["to"] = to_ts.isoformat()
        items: list[dict[str, Any]] = []
        async for item in self._c.iter_list("/futures/data/funding-settlements", params=params):
            items.append(item)
        return items

    async def ticker(self, symbol: str = "BTCUSD") -> dict[str, Any]:
        return await self._c.get(f"/futures/ticker/{symbol}")
