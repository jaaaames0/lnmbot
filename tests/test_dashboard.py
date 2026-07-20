"""Dashboard operational-history queries."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest


def _dashboard_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_dashboard.py"
    spec = importlib.util.spec_from_file_location("run_dashboard_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_signals_span_restart_runs_by_default(tmp_path):
    db_path = tmp_path / "dashboard.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE signals ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, ts TEXT, kind TEXT, side TEXT, "
            "target_size_usd REAL, target_leverage REAL, reason TEXT, metadata_json TEXT)"
        )
        connection.execute(
            "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                2,
                "2026-07-16 08:00:00",
                "noop",
                None,
                0.0,
                1.0,
                "verdict_flat",
                json.dumps({"trigger_tf": "4h"}),
            ),
        )

    dashboard = _dashboard_module()

    all_signals = dashboard._signals(db_path)
    assert [signal["reason"] for signal in all_signals] == ["verdict_flat"]
    assert dashboard._signals(db_path, run_id=3) == []
    assert dashboard._signals(db_path, tf="4h")[0]["timeframe"] == "4h"


def test_market_context_spans_restart_runs(tmp_path):
    db_path = tmp_path / "market.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE bars (id INTEGER PRIMARY KEY, run_id INTEGER, ts TEXT, close REAL)"
        )
        connection.executemany(
            "INSERT INTO bars VALUES (?, ?, ?, ?)",
            (
                (1, 2, "2026-07-16 10:00:00", 100_000.0),
                (2, 3, "2026-07-16 11:00:00", 101_000.0),
            ),
        )

    dashboard = _dashboard_module()

    price, changes, last_bar = dashboard._market_context(db_path)
    assert price == 101_000.0
    assert last_bar is not None and last_bar.hour == 11
    assert changes[0] == {"period": "1h", "change": "+1.00%"}


def test_position_surfaces_entry_chop_reduction_and_accumulated_funding(tmp_path):
    db_path = tmp_path / "positions.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, metadata_json TEXT)")
        connection.execute(
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, signal_id INTEGER, ts TEXT, trigger_tf TEXT, "
            "side TEXT, qty_sats INTEGER, leverage REAL, price_usd REAL, status TEXT, "
            "lnm_order_id TEXT, rejection_reason TEXT, metadata_json TEXT)"
        )
        connection.execute("CREATE TABLE funding_fees (trade_id TEXT, fee_sats INTEGER)")
        connection.execute(
            "INSERT INTO signals VALUES (?, ?)",
            (1, json.dumps({"chop_regime": "high_chop", "entry_size_multiplier": 0.5})),
        )
        connection.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                1,
                1,
                "2026-07-17 00:00:00",
                "4h",
                "buy",
                62,
                5.0,
                60_000.0,
                "filled",
                "trade-1",
                None,
                json.dumps({"isolated_action": "open"}),
            ),
        )
        connection.executemany(
            "INSERT INTO funding_fees VALUES (?, ?)", (("trade-1", -2), ("trade-1", -3))
        )

    dashboard = _dashboard_module()

    exchange = dashboard.ExchangeSnapshot(
        available_sats=1_000,
        total_sats=1_067,
        margin_used_sats=60,
        maintenance_margin_sats=5,
        running_pl_sats=2,
        trades={
            "trade-1": dashboard.ExchangeTrade(margin_sats=60, maintenance_margin_sats=5, pl_sats=2)
        },
        fetched_at=dashboard.datetime.now(dashboard.UTC),
    )
    positions = dashboard._open_positions(db_path, dashboard._orders(db_path), 61_000.0, exchange)
    assert positions[0]["accumulated_funding_sats"] == -5
    assert positions[0]["entry_adjustment"] == "CHOP *0.50"
    assert positions[0]["estimated_unrealized_sats"] == 2
    assert positions[0]["margin_sats"] == 60
    assert positions[0]["pnl_source"] == "LN Markets"
    assert positions[0]["position_change_pct"] == pytest.approx(8.3333333333)
    assert "positive" in dashboard._signed_amount_html(-5, "sats", None, invert=True)
    assert "negative" in dashboard._signed_amount_html(-5, "sats", None)


def test_dashboard_price_stream_records_public_last_price():
    dashboard = _dashboard_module()
    stream = dashboard.DashboardPriceStream()

    stream._record_message(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "topic": "futures/inverse/btc_usd/lastPrice",
                    "data": {"time": 1_784_514_593_978, "lastPrice": 64_609},
                },
            }
        )
    )

    tick = stream.latest()
    assert tick is not None
    assert tick.price == 64_609
    assert tick.ts.tzinfo == dashboard.UTC
