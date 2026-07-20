"""Isolated-margin futures order actions.

Endpoints here are *write* actions for the LNM isolated-margin futures
product (`/futures/isolated/trade`). Each trade has its own ID, margin, and
isolated P&L — multiple trades per symbol can coexist with their own state.

The v3 contract quantity is an integer number of USD contracts (one contract
has a USD 1 notional), not a BTC amount in sats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from .client import LnmRestClient


@dataclass
class NewIsolatedTradeParams:
    type: str = "market"  # "market" | "limit"
    side: str = "buy"  # "buy" | "sell"
    quantity: int = 0  # whole USD contracts (USD 1 notional each)
    leverage: float = 1.0
    price: float | None = None  # required for limit
    stoploss: float | None = None
    takeprofit: float | None = None


@dataclass
class IsolatedTrade:
    id: str
    type: str
    side: str
    quantity: int
    leverage: float
    price: float
    entry_price: float | None = None
    margin: int | None = None  # in sats
    maintenance_margin: int | None = None  # reserved closing-fee margin, in sats
    pl: int | None = None  # realized P&L in sats
    opening_fee: int | None = None  # in sats
    closing_fee: int | None = None  # in sats
    status: str = "open"  # "open" | "closed" | "canceled"
    created_at: datetime | None = None
    closed_at: datetime | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class IsolatedCloseResponse:
    id: str
    pl: int  # realized P&L in sats
    raw: dict[str, Any]
    closing_fee: int = 0  # in sats


class IsolatedTradesApi:
    """LNM isolated-margin futures API client.

    Each trade has its own ID. Multiple trades can be open at the same time
    per symbol. With isolated margin, each trade's P&L is independent — the
    same as our per-TF isolated-margin model in the strategy.
    """

    def __init__(self, client: LnmRestClient) -> None:
        self._c = client

    async def new_trade(self, params: NewIsolatedTradeParams) -> IsolatedTrade:
        body: dict[str, Any] = {
            "type": params.type,
            "side": params.side,
            "quantity": params.quantity,
            "leverage": params.leverage,
        }
        if params.type == "limit":
            if params.price is None:
                raise ValueError("price required for limit orders")
            body["price"] = params.price
        if params.stoploss is not None:
            body["stoploss"] = params.stoploss
        if params.takeprofit is not None:
            body["takeprofit"] = params.takeprofit
        resp = await self._c.post("/futures/isolated/trade", body=body)
        return self._parse_trade(resp)

    async def close_trade(self, trade_id: str) -> IsolatedCloseResponse:
        resp = await self._c.post("/futures/isolated/trade/close", body={"id": trade_id})
        return IsolatedCloseResponse(
            id=str(resp.get("id", trade_id)),
            pl=int(resp.get("pl", 0)),
            closing_fee=int(resp.get("closingFee", 0)),
            raw=resp if isinstance(resp, dict) else {},
        )

    async def cancel_trade(self, trade_id: str) -> dict[str, Any]:
        resp = await self._c.post("/futures/isolated/trade/cancel", body={"id": trade_id})
        return resp if isinstance(resp, dict) else {}

    async def cancel_all(self) -> list[dict[str, Any]]:
        resp = await self._c.post("/futures/isolated/trades/cancel-all")
        return resp if isinstance(resp, list) else []

    async def get_running_trades(self) -> list[IsolatedTrade]:
        resp = await self._c.get("/futures/isolated/trades/running")
        items = resp.get("data", []) if isinstance(resp, dict) else resp or []
        return [self._parse_trade(t) for t in items]

    async def get_closed_trades(self) -> list[IsolatedTrade]:
        resp = await self._c.get("/futures/isolated/trades/closed")
        items = resp.get("data", []) if isinstance(resp, dict) else resp or []
        return [self._parse_trade(t) for t in items]

    async def get_open_trades(self) -> list[IsolatedTrade]:
        resp = await self._c.get("/futures/isolated/trades/open")
        items = resp.get("data", []) if isinstance(resp, dict) else resp or []
        return [self._parse_trade(t) for t in items]

    async def add_margin(self, trade_id: str, amount: int) -> dict[str, Any]:
        resp = await self._c.post(
            "/futures/isolated/trade/add-margin",
            body={"id": trade_id, "amount": amount},
        )
        return resp if isinstance(resp, dict) else {}

    async def iter_funding_fees(
        self, from_ts: datetime, to_ts: datetime
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate paginated isolated funding settlements."""
        params = {"from": from_ts.isoformat(), "to": to_ts.isoformat()}
        async for fee in self._c.iter_list("/futures/isolated/funding-fees", params=params):
            if isinstance(fee, dict):
                yield fee

    @staticmethod
    def _parse_trade(raw: dict[str, Any]) -> IsolatedTrade:
        if not isinstance(raw, dict):
            return IsolatedTrade(
                id="",
                type="",
                side="",
                quantity=0,
                leverage=1.0,
                price=0.0,
                raw={},
            )
        return IsolatedTrade(
            id=str(raw.get("id", "")),
            type=str(raw.get("type", "market")),
            side=str(raw.get("side", "buy")),
            quantity=int(raw.get("quantity", 0)),
            leverage=float(raw.get("leverage", 1.0)),
            price=float(raw.get("price", 0.0)),
            entry_price=raw.get("entryPrice", raw.get("entry_price")),
            margin=raw.get("margin"),
            maintenance_margin=raw.get("maintenanceMargin", raw.get("maintenance_margin")),
            pl=raw.get("pl"),
            opening_fee=raw.get("openingFee"),
            closing_fee=raw.get("closingFee"),
            status=str(raw.get("status", "open")),
            created_at=raw.get("createdAt", raw.get("created_at")),
            closed_at=raw.get("closedAt", raw.get("closed_at")),
            raw=raw,
        )


__all__ = [
    "IsolatedCloseResponse",
    "IsolatedTrade",
    "IsolatedTradesApi",
    "NewIsolatedTradeParams",
]
