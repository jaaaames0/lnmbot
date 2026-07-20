"""Account endpoints — balance, cross-margin positions.

LNM represents cross-margin as: every position draws from a shared collateral
pool, with the position's notional / leverage determining exposure. For v0 we
treat cross-margin as the only mode.
"""
from __future__ import annotations

from typing import Any

from .client import LnmRestClient


class AccountApi:
    def __init__(self, client: LnmRestClient) -> None:
        self._c = client

    async def get_balance(self) -> dict[str, Any]:
        return await self._c.get("/account")

    async def get_cross_open_positions(self) -> list[dict[str, Any]]:
        resp = await self._c.get("/futures/cross/positions")
        return resp.get("data", []) if isinstance(resp, dict) else resp or []

    async def get_cross_filled_orders(self, **params: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        async for item in self._c.iter_list("/futures/cross/orders/filled", params=params):
            items.append(item)
        return items
