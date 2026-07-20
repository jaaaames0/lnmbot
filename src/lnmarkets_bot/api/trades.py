"""Cross-margin futures order actions.

Endpoints here are *write* actions. The live engine must NEVER import this
module directly — only `risk/guard.py` does. Enforced by import-linter.

The shape of these requests/responses will be cross-checked against the docs
once the testnet credentials and a live response log are available; for v0
the engine's paper-mode takes a different code path that doesn't hit the
network.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .client import LnmRestClient


@dataclass
class NewOrderParams:
    side: str  # "buy" | "sell"
    qty_sats: int
    leverage: float
    type: str = "market"
    symbol: str = "BTCUSD"


@dataclass
class OrderResponse:
    id: str
    raw: dict[str, Any]


class TradesApi:
    def __init__(self, client: LnmRestClient) -> None:
        self._c = client

    async def new_order(self, params: NewOrderParams) -> OrderResponse:
        body = {
            "side": params.side,
            "qty": params.qty_sats,
            "leverage": params.leverage,
            "type": params.type,
            "symbol": params.symbol,
        }
        resp = await self._c.post("/futures/cross/orders/new", body=body)
        return OrderResponse(id=str(resp.get("id", "")), raw=resp)

    async def close_position(
        self,
        position_id: str,
        *,
        qty_sats: int | None = None,
    ) -> OrderResponse:
        body: dict[str, Any] = {}
        if qty_sats is not None:
            body["qty"] = qty_sats
        resp = await self._c.delete(f"/futures/cross/positions/{position_id}", body=body or None)
        return OrderResponse(id=str(resp.get("id", "")), raw=resp)

    async def cancel_order(self, order_id: str) -> OrderResponse:
        resp = await self._c.delete(f"/futures/cross/orders/{order_id}")
        return OrderResponse(id=str(resp.get("id", "")), raw=resp)

    async def get_filled_orders(self, **params: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        async for item in self._c.iter_list("/futures/cross/orders/filled", params=params):
            items.append(item)
        return items

    async def get_canceled_orders(self, **params: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        async for item in self._c.iter_list("/futures/cross/orders/canceled", params=params):
            items.append(item)
        return items


__all__ = ["NewOrderParams", "OrderResponse", "TradesApi"]
