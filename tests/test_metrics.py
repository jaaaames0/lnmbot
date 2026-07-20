from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lnmarkets_bot.metrics import FillRow, pair_fills_into_trades


def test_pairing_converts_sat_fees_to_usd_at_fill_prices() -> None:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    trades = pair_fills_into_trades(
        [
            FillRow(ts, 1, 100_000, 100_000.0, 100, "buy"),
            FillRow(ts, 2, 100_000, 110_000.0, 100, "sell"),
        ]
    )

    # $10 gross price gain, less $0.10 opening fee and $0.11 closing fee.
    assert trades[0].pnl_usd == pytest.approx(9.79)
