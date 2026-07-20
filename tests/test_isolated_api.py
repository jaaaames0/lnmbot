"""Request-contract tests for LN Markets v3 isolated futures."""

from __future__ import annotations

from datetime import UTC, datetime

from lnmarkets_bot.api.isolated import IsolatedTradesApi, NewIsolatedTradeParams


class _RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []

    async def post(self, path: str, *, body: dict | None = None):
        self.calls.append(("post", path, body))
        return {
            "id": "trade-1",
            "type": "market",
            "side": "buy",
            "quantity": 1,
            "leverage": 1,
            "price": 100_000,
        }

    async def iter_list(self, path: str, *, params: dict):
        self.calls.append(("iter_list", path, params))
        yield {"settlementId": "funding-1"}


def test_isolated_trade_parses_v3_maintenance_margin():
    trade = IsolatedTradesApi._parse_trade(
        {
            "id": "trade-1",
            "type": "market",
            "side": "buy",
            "quantity": 1,
            "leverage": 1,
            "price": 100_000,
            "margin": 1_000,
            "maintenanceMargin": 23,
        }
    )

    assert trade.maintenance_margin == 23


async def test_isolated_create_and_close_use_v3_singular_trade_routes():
    client = _RecordingClient()
    api = IsolatedTradesApi(client)

    trade = await api.new_trade(
        NewIsolatedTradeParams(
            type="market",
            side="buy",
            quantity=1,
            leverage=1,
        )
    )
    await api.close_trade(trade.id)

    assert client.calls == [
        (
            "post",
            "/futures/isolated/trade",
            {
                "type": "market",
                "side": "buy",
                "quantity": 1,
                "leverage": 1,
            },
        ),
        ("post", "/futures/isolated/trade/close", {"id": "trade-1"}),
    ]


async def test_isolated_funding_history_uses_paginated_v3_route():
    client = _RecordingClient()
    api = IsolatedTradesApi(client)
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = datetime(2026, 7, 2, tzinfo=UTC)

    rows = [row async for row in api.iter_funding_fees(start, end)]

    assert rows == [{"settlementId": "funding-1"}]
    assert client.calls == [
        (
            "iter_list",
            "/futures/isolated/funding-fees",
            {"from": start.isoformat(), "to": end.isoformat()},
        )
    ]
