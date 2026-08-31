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


def test_market_context_spans_restart_runs_without_optional_binance_cache(tmp_path, monkeypatch):
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
    monkeypatch.setattr(dashboard, "BINANCE_HOURLY_CACHE", tmp_path / "missing-hourly.parquet")
    monkeypatch.setattr(dashboard, "BINANCE_DAILY_CACHE", tmp_path / "missing-daily.parquet")
    dashboard._binance_hourly_close_history.cache_clear()
    dashboard._binance_daily_close_history.cache_clear()

    price, changes, last_bar = dashboard._market_context(db_path)
    assert price == 101_000.0
    assert last_bar is not None and last_bar.hour == 11
    assert changes[0] == {"period": "1h", "change": "+1.00%"}


def test_ma_levels_prefer_the_persisted_live_strategy_state(tmp_path):
    db_path = tmp_path / "state.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE strategy_state_snapshots ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, mode TEXT, strategy_name TEXT, "
            "ts TEXT, state_json TEXT)"
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            (
                1,
                1,
                "live",
                "lnmarkets_bot.strategy.ma_cross.MaCross",
                "2026-07-28 04:00:00",
                json.dumps(
                    {
                        "version": 1,
                        "timeframes": {
                            "1d": {
                                "sma": 64_300.0,
                                "ema": 64_257.0,
                                "last_bar_ts": "2026-07-28T00:00:00+00:00",
                            }
                        },
                    }
                ),
            ),
        )

    dashboard = _dashboard_module()
    levels = dashboard._ma_levels(db_path, tolerance_pct=0.005)

    assert levels["1d"]["ema21"] == 64_257.0
    assert levels["1d"]["short_trigger"] == pytest.approx(63_935.715)
    assert levels["1d"]["bootstrap_source"] == "persisted_live_state"


def test_persisted_cooldowns_and_position_card_explain_remaining_verdict_changes(tmp_path):
    db_path = tmp_path / "cooldowns.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE strategy_state_snapshots ("
            "id INTEGER PRIMARY KEY, mode TEXT, ts TEXT, state_json TEXT)"
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES (?, ?, ?, ?)",
            (
                1,
                "live",
                "2026-07-30 12:00:00",
                json.dumps(
                    {
                        "winner_suppressed_signals": {"1d": 0, "4h": 6},
                        "loss_suppressed_signals": {"1d": 2, "4h": 0},
                    }
                ),
            ),
        )

    dashboard = _dashboard_module()

    cooldowns = dashboard._persisted_cooldowns(db_path)
    assert cooldowns == {"1d": {"winner": 0, "loss": 2}, "4h": {"winner": 6, "loss": 0}}
    card = dashboard._position_card("4h", None, "sats", None, None, cooldowns["4h"])
    assert "Cool-off active" in card
    assert "6 verdict changes left" in card
    assert "winner 6" in card


def test_strategy_explainer_says_triggering_exit_does_not_spend_a_slot():
    dashboard = _dashboard_module()

    explainer = dashboard._strategy_explainer(
        {
            "strategy_params_json": json.dumps(
                {
                    "tolerance_pct": 0.005,
                    "cooldown_mode": "verdict_transition",
                    "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
                    "loss_cooldown_threshold_pct": {"1d": 0.05, "4h": 0.02},
                    "cooldown_signal_count": {"1d": 12, "4h": 11},
                    "loss_cooldown_signal_count": {"1d": 3, "4h": 4},
                }
            ),
            "config_json": json.dumps({"strategy_4h_chop_reduce_enabled": True}),
        }
    )

    assert "does <b>not</b> spend a slot" in explainer
    assert "including a move to or from Flat" in explainer
    assert "high, new 4h entries use the configured reduced size" in explainer


def test_active_run_ignores_newer_manual_recovery_rows(tmp_path):
    db_path = tmp_path / "runs.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE runs ("
            "id INTEGER PRIMARY KEY, mode TEXT, status TEXT, started_at TEXT, ended_at TEXT, "
            "strategy_params_json TEXT, config_json TEXT)"
        )
        connection.executemany(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    11,
                    "live",
                    "running",
                    "2026-07-27 00:22:02",
                    None,
                    json.dumps({"tfs": ["1d", "4h"]}),
                    json.dumps({"sizing_mode": "equity_fraction"}),
                ),
                (
                    12,
                    "manual_recovery",
                    "complete",
                    "2026-07-27 00:15:32",
                    "2026-07-27 00:15:32",
                    json.dumps({}),
                    json.dumps({}),
                ),
            ),
        )

    dashboard = _dashboard_module()

    active = dashboard._active_run(db_path)
    assert active is not None
    assert active["id"] == 11
    assert active["status"] == "running"
    assert json.loads(active["config_json"])["sizing_mode"] == "equity_fraction"


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


def _create_normalized_pnl_db(db_path):
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, metadata_json TEXT)")
        connection.execute(
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, signal_id INTEGER, ts TEXT, trigger_tf TEXT, "
            "side TEXT, qty_sats INTEGER, leverage REAL, price_usd REAL, status TEXT, "
            "lnm_order_id TEXT, rejection_reason TEXT, metadata_json TEXT)"
        )
        connection.execute("CREATE TABLE funding_fees (trade_id TEXT, fee_sats INTEGER)")
        connection.execute("INSERT INTO signals VALUES (?, ?)", (1, "{}"))
        connection.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    1,
                    1,
                    1,
                    "2026-08-01 00:00:00",
                    "4h",
                    "buy",
                    100,
                    5.0,
                    50_000.0,
                    "filled",
                    "trade-1",
                    None,
                    json.dumps(
                        {
                            "isolated_action": "open",
                            "lnm_trade_id": "trade-1",
                            "opening_fee_sats": 100,
                        }
                    ),
                ),
                (
                    2,
                    1,
                    1,
                    "2026-08-02 00:00:00",
                    "4h",
                    "sell",
                    100,
                    1.0,
                    55_000.0,
                    "filled",
                    "trade-1",
                    None,
                    json.dumps(
                        {
                            "isolated_action": "close",
                            "lnm_trade_id": "trade-1",
                            "closing_fee_sats": 100,
                            "gross_pl_sats": 18_182,
                        }
                    ),
                ),
                (
                    3,
                    1,
                    1,
                    "2026-08-03 00:00:00",
                    "4h",
                    "buy",
                    1_000,
                    5.0,
                    50_000.0,
                    "filled",
                    "trade-2",
                    None,
                    json.dumps(
                        {
                            "isolated_action": "open",
                            "lnm_trade_id": "trade-2",
                            "opening_fee_sats": 1_000,
                        }
                    ),
                ),
                (
                    4,
                    1,
                    1,
                    "2026-08-04 00:00:00",
                    "4h",
                    "sell",
                    1_000,
                    1.0,
                    55_000.0,
                    "filled",
                    "trade-2",
                    None,
                    json.dumps(
                        {
                            "isolated_action": "close",
                            "lnm_trade_id": "trade-2",
                            "closing_fee_sats": 1_000,
                            "gross_pl_sats": 181_820,
                        }
                    ),
                ),
            ),
        )
        connection.executemany(
            "INSERT INTO funding_fees VALUES (?, ?)", (("trade-1", 50), ("trade-2", 500))
        )


def test_constant_notional_replay_normalizes_by_entry_size_and_includes_costs(tmp_path):
    db_path = tmp_path / "normalized.sqlite"
    _create_normalized_pnl_db(db_path)
    dashboard = _dashboard_module()

    events = dashboard._closed_trade_components(db_path)
    assert len(events) == 2
    expected_return_pct = (18_182 - 100 - 100 - 50) * 55_000 / 1e8 / 100 * 100
    assert [event["net_return_pct"] for event in events] == pytest.approx(
        [expected_return_pct, expected_return_pct]
    )

    summary = dashboard._constant_notional_pnl_summary(
        db_path,
        [],
        nominal_usd=dashboard.CONSTANT_NOTIONAL_USD,
        btc_price=55_000.0,
        now=dashboard.datetime(2026, 8, 4, 1, tzinfo=dashboard.UTC),
    )
    all_time = next(row for row in summary if row["key"] == "alltime")
    assert all_time["net"] == pytest.approx(expected_return_pct / 100 * 100 * 2)
    assert all_time["portfolio_return_pct"] == pytest.approx(expected_return_pct)


def test_trade_ledger_and_fixed_pnl_controls(tmp_path):
    db_path = tmp_path / "normalized.sqlite"
    _create_normalized_pnl_db(db_path)
    dashboard = _dashboard_module()

    ledger = dashboard._trade_history_rows(db_path, tf=None, denomination="usd", btc_price=55_000.0)
    assert len(ledger) == 2
    assert all("+9.86%" in row["net_return"] for row in ledger)

    controls = dashboard._pnl_basis_controls("usd", "weekly", "constant")
    assert ">Actual</a>" in controls
    assert ">Constant notional</a>" in controls
    assert "pnl_basis=constant" in controls
    assert "nominal_usd" not in controls
    assert "USD per trade" not in controls
