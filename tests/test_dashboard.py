"""Dashboard operational-history queries."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import threading
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest


def _dashboard_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_dashboard.py"
    spec = importlib.util.spec_from_file_location("run_dashboard_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module._EXCHANGE_CACHE.disabled = True  # Tests never consult venue credentials.
    return module


def test_optional_portfolio_panel_is_read_only_and_separates_seed_profit(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from lnmarkets_bot.portfolio.store import PortfolioStore

    dashboard = _dashboard_module()
    monkeypatch.delenv("LNMBOT_PORTFOLIO_SHADOW_DB", raising=False)
    assert dashboard._portfolio_panel() == ""
    path = tmp_path / "shadow.sqlite"
    monkeypatch.setenv("LNMBOT_PORTFOLIO_SHADOW_DB", str(path))
    assert "unavailable" in dashboard._portfolio_panel()
    assert not path.exists()
    store = PortfolioStore(path)
    store.register("breakout", "paper", "rules")
    store.import_shadow_observation(
        "breakout",
        {
            "mode": "shadow_no_orders",
            "order_capability": False,
            "strategy": "structure_parent_addons_raw",
            "rules_sha256": "rules",
            "as_of_close": "2026-09-21T00:00:00+00:00",
            "next_open": "2026-09-22T00:00:00+00:00",
            "generated_at": "2026-09-22T01:00:00+00:00",
            "active_hypothetical_stack": {
                "parent_id": "20260822L<script>",
                "entry_ts": "2026-08-22T00:00:00+00:00",
                "side": "long",
                "boundary": 72998.7,
                "active_units": 4,
            },
        },
        now=datetime(2026, 9, 22, 2, tzinfo=UTC),
    )
    before = path.read_bytes()
    panel = dashboard._portfolio_panel()
    assert "Blocked by historical campaign" in panel
    assert "no owned trades" in panel
    assert "20260822L&lt;script&gt;" in panel
    assert "20260822L<script>" not in panel
    assert path.read_bytes() == before


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
        connection.execute(
            "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                2, 3, "2026-07-16 08:01:00", "noop", None, 0.0, 1.0,
                "restart_state_aligned", json.dumps({"trigger_tf": "4h"}),
            ),
        )

    dashboard = _dashboard_module()

    all_signals = dashboard._signals(db_path)
    assert [signal["reason"] for signal in all_signals] == ["verdict_flat"]
    assert dashboard._signals(db_path, run_id=3) == []
    assert dashboard._signals(db_path, tf="4h")[0]["timeframe"] == "4h"


def test_cooloff_signal_shows_slot_number_and_verdict_transition(tmp_path):
    db_path = tmp_path / "cooloff-signals.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE signals ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, ts TEXT, kind TEXT, side TEXT, "
            "target_size_usd REAL, target_leverage REAL, reason TEXT, metadata_json TEXT)"
        )
        connection.execute(
            "INSERT INTO signals VALUES (1,1,'2026-09-24 00:00:00','noop',NULL,0,1,'cool_off',?)",
            (json.dumps({
                "trigger_tf": "1d", "previous_verdict": "UP_TRUE", "verdict": "FLAT",
                "cooldown_types": ["winner"], "winner_remaining_before": 11,
                "winner_remaining_after": 10, "winner_total": 12,
                "loss_remaining_before": 0, "loss_remaining_after": 0, "loss_total": 3,
            }),),
        )

    dashboard = _dashboard_module()
    rows = dashboard._signal_timeline_rows(db_path, "1d", None, None)

    assert len(rows) == 1
    assert rows[0]["strategy"] == "MA cross"
    assert rows[0]["signal_ts"] == "2026-09-23T00:00:00+00:00"
    assert rows[0]["event"] == "Suppressed"
    assert rows[0]["detail"] == "winner 2/12 · Up → Flat"


def test_breakout_campaign_exit_displays_once_with_all_unit_slots(tmp_path):
    db_path = tmp_path / "exit-signals.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE signals ("
            "id INTEGER PRIMARY KEY, run_id INTEGER, ts TEXT, kind TEXT, side TEXT, "
            "target_size_usd REAL, target_leverage REAL, reason TEXT, metadata_json TEXT, "
            "strategy_instance_id TEXT, position_key TEXT)"
        )
        connection.executemany(
            "INSERT INTO signals VALUES (?,1,'2026-09-24 00:00:00','exit',NULL,0,1,"
            "'range_close',?,'btc_close_range_v1',?)",
            [
                (k + 1, json.dumps({"trigger_tf": "1d", "campaign_id": "20260822L"}), f"k{k}")
                for k in range(4)
            ],
        )
    dashboard = _dashboard_module()
    state = {"recent_decisions": [{
        "ts": "2026-09-24T00:00:00+00:00", "kind": "campaign_exit",
        "reason": "range_close", "k": None,
    }]}

    rows = dashboard._signal_timeline_rows(db_path, "breakout", state, None)

    assert len(dashboard._signals(db_path, tf="breakout")) == 4
    assert len(rows) == 1
    assert rows[0]["slot"] == "K0-K3"
    assert rows[0]["event"] == "Exit"
    assert rows[0]["detail"] == "Back inside parent range"
    assert rows[0]["qualifiers"] == "-"


def test_signal_timeline_shows_causal_order_and_only_signal_qualifiers(tmp_path):
    db_path = tmp_path / "timeline.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE signals (id INTEGER PRIMARY KEY, run_id INTEGER, ts TEXT, kind TEXT, "
            "side TEXT, target_size_usd REAL, target_leverage REAL, reason TEXT, "
            "metadata_json TEXT, strategy_instance_id TEXT, position_key TEXT)"
        )
        connection.execute(
            "INSERT INTO signals VALUES (1,1,'2026-09-24 00:00:00','entry','long',250,5,"
            "'ma daily',?,'ma_cross_primary','4h')",
            (json.dumps({"trigger_tf": "4h", "chop_regime": "high", "chop_value": 65,
                         "entry_size_multiplier": 0.5}),),
        )
    dashboard = _dashboard_module()
    metadata = {"signal_ts": "2026-09-23T00:00:00+00:00", "signal_close": 85_000,
                "boundary": 82_000, "distance_ema_atr": 1.2, "average_overlap10": 0.31}
    state = {"recent_decisions": [
        {"ts": "2026-09-24T00:00:00+00:00", "kind": "signal", "reason": "structure_pass",
         "k": None, "metadata": metadata},
        {"ts": "2026-09-24T00:00:00+00:00", "kind": "reject", "reason": "addon_cap",
         "k": None, "metadata": metadata},
    ]}
    rows = dashboard._signal_timeline_rows(db_path, None, state, {"signal_trail": []})
    assert [row["event"] for row in rows[:2]] == ["Breakout", "Blocked"]
    assert [row["strategy"] for row in rows[:2]] == ["Breakout", "Breakout*"]
    assert rows[0]["signal_ts"] == "2026-09-23T00:00:00+00:00"
    assert rows[0]["qualifiers"] == "close $85,000.00 · range $82,000.00 · EMA ATR 1.20 · overlap 0.310"
    assert rows[2]["signal_ts"] == "2026-09-23T20:00:00+00:00"
    assert rows[2]["qualifiers"] == "chop high (65.00)"


def test_execution_alignment_requires_current_venue_evidence(tmp_path):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    db_path = tmp_path / "alignment.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE strategy_state_snapshots ("
            "mode TEXT, strategy_name TEXT, state_json TEXT, ts TEXT)"
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES ('live', ?, ?, '2026-09-24 00:00:00')",
            (
                "lnmarkets_bot.strategy.ma_cross.MaCross",
                json.dumps({
                    "timeframes": {"1d": {"verdict": "UP_TRUE"}, "4h": {"verdict": "FLAT"}},
                    "pending_position_reconciliation": {"1d": None, "4h": None},
                }),
            ),
        )
    dashboard = _dashboard_module()
    position = {"strategy": "ma_cross_primary", "slot": "1d", "side": "long", "trade_id": "ma-1"}
    venue = SimpleNamespace(trades={"ma-1": object()}, fetched_at=datetime.now(UTC))

    assert dashboard._execution_alignment(db_path, [position], venue)[0] == "Aligned"
    assert dashboard._execution_alignment(
        db_path, [position], venue, breakout_enabled=True
    )[0] == "Unknown"
    assert dashboard._execution_alignment(db_path, [position], None)[0] == "Venue unchecked"
    assert dashboard._execution_alignment(
        db_path, [position], SimpleNamespace(trades=venue.trades, fetched_at=datetime.now(UTC) - timedelta(minutes=2))
    )[0] == "Venue unchecked"
    assert dashboard._execution_alignment(
        db_path, [position], SimpleNamespace(trades={}, fetched_at=datetime.now(UTC))
    )[0] == "Action needed"
    assert dashboard._execution_alignment(
        db_path, [position],
        SimpleNamespace(trades={"ma-1": object(), "unknown": object()}, fetched_at=datetime.now(UTC)),
    )[0] == "Action needed"
    assert dashboard._execution_alignment(db_path, [{**position, "side": "short"}], venue)[0] == "Action needed"


def test_execution_alignment_checks_breakout_campaign_slots(tmp_path):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    db_path = tmp_path / "breakout-alignment.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE strategy_state_snapshots ("
            "mode TEXT, strategy_name TEXT, state_json TEXT, ts TEXT)"
        )
        connection.executemany(
            "INSERT INTO strategy_state_snapshots VALUES ('live', ?, ?, '2026-09-24 00:00:00')",
            (
                ("lnmarkets_bot.strategy.ma_cross.MaCross", json.dumps({
                    "timeframes": {"1d": {"verdict": "FLAT"}, "4h": {"verdict": "FLAT"}},
                    "pending_position_reconciliation": {},
                })),
                ("lnmarkets_bot.strategy.close_range_live.CloseRangeLive", json.dumps({
                    "machine": {"campaign": {
                        "origin": "live", "units": [
                            {"k": 0, "origin": "live"}, {"k": 1, "origin": "live"},
                        ],
                    }},
                    "closing_slots": [],
                })),
            ),
        )
    dashboard = _dashboard_module()
    parent = {"strategy": "btc_close_range_v1", "slot": "k0", "side": "long", "trade_id": "bo-0"}
    child = {**parent, "slot": "k1", "trade_id": "bo-1"}
    venue = SimpleNamespace(
        trades={"bo-0": object(), "bo-1": object()}, fetched_at=datetime.now(UTC)
    )

    assert dashboard._execution_alignment(db_path, [parent], venue)[0] == "Action needed"
    assert dashboard._execution_alignment(db_path, [parent, child], venue)[0] == "Aligned"


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
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            (
                2,
                1,
                "live",
                "lnmarkets_bot.strategy.close_range_live.CloseRangeLive",
                "2026-07-28 08:00:00",
                json.dumps({"campaign": {"id": "later-breakout-state"}}),
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
            "id INTEGER PRIMARY KEY, mode TEXT, strategy_name TEXT, ts TEXT, state_json TEXT)"
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES (?, ?, ?, ?, ?)",
            (
                1,
                "live",
                "lnmarkets_bot.strategy.ma_cross.MaCross",
                "2026-07-30 12:00:00",
                json.dumps(
                    {
                        "winner_suppressed_signals": {"1d": 0, "4h": 6},
                        "loss_suppressed_signals": {"1d": 2, "4h": 0},
                    }
                ),
            ),
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots VALUES (?, ?, ?, ?, ?)",
            (
                2,
                "live",
                "lnmarkets_bot.strategy.close_range_live.CloseRangeLive",
                "2026-07-30 16:00:00",
                json.dumps({"campaign": {"id": "later-breakout-state"}}),
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


def test_strategy_explainer_handles_portfolio_params_and_describes_breakout():
    dashboard = _dashboard_module()
    explainer = dashboard._strategy_explainer(
        {
            "strategy_params_json": json.dumps(
                {
                    "ma_cross_primary": {
                        "strategy": "lnmarkets_bot.strategy.ma_cross.MaCross",
                        "params": {
                            "tolerance_pct": 0.005,
                            "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
                            "loss_cooldown_threshold_pct": {"1d": 0.05, "4h": 0.02},
                            "cooldown_signal_count": {"1d": 12, "4h": 11},
                            "loss_cooldown_signal_count": {"1d": 3, "4h": 4},
                        },
                    },
                    "btc_close_range_v1": {
                        "strategy": "lnmarkets_bot.strategy.close_range_live.CloseRangeLive",
                        "params": {"unit_notional_usd": 100, "leverage": 5},
                    },
                }
            ),
            "config_json": "{}",
        }
    )
    assert "0.50% above" in explainer
    assert "Close-range breakout" in explainer
    assert "$100 at 5x" in explainer


def test_breakout_direction_mode_is_visible_and_block_reasons_are_explained():
    dashboard = _dashboard_module()
    context = dashboard._breakout_context(
        {"direction_mode": "long_only", "machine": {"campaign": None}}, []
    )
    assert context["direction_mode"] == "long_only"
    assert "Daily campaign · Long only" in dashboard._breakout_card(context, "sats", None)
    assert dashboard._signal_detail(
        "parent_direction_mode", {"direction_mode": "long_only"}
    ) == "Long only blocks this entry"
    assert dashboard._signal_detail(
        "direction_mode_changed", {"previous_mode": "both", "direction_mode": "short_only"}
    ) == "Both directions → Short only"
    assert dashboard._signal_detail("recovery_same_open") == (
        "Recovery exit · same-open parent blocked"
    )


def test_strategy_accounting_panel_separates_shared_wallet_results(tmp_path):
    db_path = tmp_path / "accounting.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE strategy_pnl_events ("
            "strategy_instance_id TEXT, trade_id TEXT, kind TEXT, amount_sats INTEGER)"
        )
        connection.executemany(
            "INSERT INTO strategy_pnl_events VALUES (?, ?, ?, ?)",
            (
                ("ma_cross_primary", "ma-1", "close_net_pl", 100),
                ("btc_close_range_v1", "bo-1", "opening_fee", -5),
                ("btc_close_range_v1", "bo-1", "funding", 2),
            ),
        )

    dashboard = _dashboard_module()
    panel = dashboard._strategy_accounting_panel(db_path, "sats", None)

    assert "MA cross" in panel
    assert "Breakout" in panel
    assert "class=positive>+100 " in panel
    assert "class=negative>-3 " in panel


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
    assert "portfolio_return_pct" not in all_time


def test_return_on_margin_ignores_trade_size_and_needs_history_to_annualise(tmp_path):
    db_path = tmp_path / "normalized.sqlite"
    _create_normalized_pnl_db(db_path)
    dashboard = _dashboard_module()
    # Both trades earn the same multiple of their posted margin; the second is ten
    # times larger (as after a deposit) but the return on margin is unchanged.
    per_trade = (18_182 - 100 - 100 - 50) / (100 / 5 / 50_000 * 1e8)
    larger = (181_820 - 1_000 - 1_000 - 500) / (1_000 / 5 / 50_000 * 1e8)
    assert larger == pytest.approx(per_trade)

    def rows(now):
        return {
            row["strategy"]: row
            for row in dashboard._margin_return_rows(db_path, [], "sats", None, now=now)
        }

    early = rows(dashboard.datetime(2026, 8, 5, tzinfo=dashboard.UTC))
    total = (1 + per_trade) ** 2 - 1
    assert early["MA 4h"]["trades"] == 2
    assert early["MA 4h"]["return_on_margin"] == dashboard._signed_percent_html(total * 100)
    assert early["Account"]["return_on_margin"] == early["MA 4h"]["return_on_margin"]
    assert early["MA 4h"]["extrapolated_cagr"] == "< 30d of data"
    assert early["Range"]["trades"] == 0 and early["Breakout k3"]["return_on_margin"] == "-"
    later = rows(dashboard.datetime(2026, 9, 15, tzinfo=dashboard.UTC))
    cagr = (1 + total) ** (365 / 45) - 1
    assert later["MA 4h"]["extrapolated_cagr"] == dashboard._signed_percent_html(cagr * 100)


def test_capital_page_sizes_next_entries_and_plans_a_target_mix(tmp_path):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO bars (run_id, ts, open, high, low, close, volume) "
            "VALUES (1, '2026-09-24 00:00:00', 80000, 80000, 80000, 80000, 0)"
        )
        connection.execute(
            "INSERT INTO account_snapshots (run_id, ts, balance_sats, equity_sats, "
            "margin_used_sats, unrealized_pnl_sats) "
            "VALUES (1, '2026-09-24 00:00:00', 5000000, 5000000, 0, 0)"
        )
    dashboard = _dashboard_module()
    config = {
        "sizing_mode": "equity_fraction",
        "sizing_leverage": 5,
        "sizing_total_margin_fraction": 0.2,
        "sizing_timeframe_weights": {"1d": 0.6, "4h": 0.4},
        "sizing_equity_haircut": 0.95,
        "risk_max_position_usd": 2000,
        "risk_max_leverage": 5,
        "strategy_breakout_enabled": True,
        "strategy_breakout_unit_notional_usd": 100,
        "strategy_breakout_leverage": 5,
        "strategy_range_mode": "funded",
        "strategy_range_unit_notional_usd": 100,
        "strategy_range_leverage": 5,
    }
    run = {"id": 1, "config_json": json.dumps(config)}
    # $4,000 equity: MA 1d would open $2,280 but the position cap clips it to $2,000.
    page = dashboard._capital_page(
        db_path,
        run,
        "sats",
        None,
        {"budget": "40", "ma": "50", "breakout": "25", "range": "25", "w1d": "60",
         "n4h": "1600", "n1d": "2400", "nk": "500", "nr": "2000"},
    )
    assert "clipped by position cap $2,000" in page
    assert "<td>$1,520</td>" in page and "<td>$304</td>" in page
    # 40% of $4,000 = $1,600 margin: MA $800 (1d $480, 4h $320), $100 per breakout
    # unit and $400 for range, at 5x.
    for notional in ("$2,400", "$1,600", "$500", "$2,000"):
        assert f"<td>{notional}</td>" in page
    assert "<td>SIZING_TOTAL_MARGIN_FRACTION</td><td>0.2</td><td>0.2105</td>" in page
    assert "<td>STRATEGY_BREAKOUT_UNIT_NOTIONAL_USD</td><td>100</td><td>500</td>" in page
    assert "<td>STRATEGY_RANGE_UNIT_NOTIONAL_USD</td><td>100</td><td>2000</td>" in page
    assert "<td>RISK_MAX_POSITION_USD</td><td>2000</td><td>2400</td>" in page
    # Those sizes need exactly the current equity at a 40% budget.
    assert "<td>Equity needed at 40% budget</td><td>$4,000</td>" in page
    assert "no history · counted as 100%" in page and "data-preserve" in page
    rendered = dashboard._render(db_path, "capital", None)
    assert "<h1>Capital</h1>" in rendered and 'href="/capital"' in rendered


def _create_multistrategy_dashboard_db(db_path, *, funded: bool) -> None:
    from lnmarkets_bot.persistence.db import init_schema, make_engine

    init_schema(make_engine(db_path))
    campaign = {
        "campaign_id": "20260822L" if not funded else "20260923L",
        "side": 1,
        "origin": "historical" if not funded else "live",
        "boundary": 72_968.0,
        "entry_ts": "2026-08-22T00:00:00+00:00",
        "lifetime_units": 4 if not funded else 2,
        "units": (
            [{"k": 0, "entry_price": 78_330.0}, {"k": 1, "entry_price": 79_000.0}]
            if funded
            else [{"k": 0, "entry_price": 78_330.64575}]
        ),
    }
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO runs (id,mode,strategy_name,strategy_params_json,config_json,started_at,status) "
            "VALUES (1,'live','portfolio',?,?,?, 'running')",
            (
                json.dumps({"ma_cross_primary": {"params": {"tolerance_pct": 0.005}}}),
                json.dumps({"strategy_breakout_enabled": True}),
                "2026-09-22 00:00:00",
            ),
        )
        connection.executemany(
            "INSERT INTO strategy_state_snapshots "
            "(run_id,mode,strategy_name,ts,state_json) VALUES (1,'live',?,?,?)",
            (
                (
                    "lnmarkets_bot.strategy.ma_cross.MaCross",
                    "2026-09-22 12:00:00",
                    json.dumps(
                        {
                            "timeframes": {
                                "1d": {
                                    "sma": 79_000,
                                    "ema": 78_900,
                                    "last_bar_ts": "2026-09-22T00:00:00+00:00",
                                },
                                "4h": {
                                    "sma": 79_100,
                                    "ema": 79_050,
                                    "last_bar_ts": "2026-09-22T12:00:00+00:00",
                                },
                            },
                            "winner_suppressed_signals": {"1d": 11, "4h": 0},
                            "loss_suppressed_signals": {"1d": 0, "4h": 0},
                        }
                    ),
                ),
                (
                    "lnmarkets_bot.strategy.close_range_live.CloseRangeLive",
                    "2026-09-22 12:00:00",
                    json.dumps(
                        {
                            "machine": {
                                "last_bar_ts": "2026-09-21T00:00:00+00:00",
                                "campaign": campaign,
                            },
                            "recent_decisions": [
                                {
                                    "ts": "2026-09-22T00:00:00+00:00",
                                    "kind": "reject",
                                    "k": None,
                                    "reason": "addon_cap",
                                }
                            ],
                        }
                    ),
                ),
            ),
        )
        connection.executemany(
            "INSERT INTO signals "
            "(id,run_id,ts,kind,side,target_size_usd,target_leverage,reason,metadata_json,"
            "strategy_instance_id,position_key) VALUES (?,?,?,'entry','buy',100,5,?,?,?,?)",
            (
                (
                    1,
                    1,
                    "2026-09-22 00:00:00",
                    "ma daily",
                    json.dumps({"trigger_tf": "1d"}),
                    "ma_cross_primary",
                    "1d",
                ),
                (
                    2,
                    1,
                    "2026-09-22 00:01:00",
                    "breakout parent",
                    json.dumps({"trigger_tf": "1d"}),
                    "btc_close_range_v1",
                    "k0",
                ),
            ),
        )
        if funded:
            connection.executemany(
                "INSERT INTO orders "
                "(run_id,ts,trigger_tf,side,qty_sats,leverage,price_usd,status,"
                "lnm_order_id,metadata_json,strategy_instance_id,position_key) "
                "VALUES (1,?,'1d','buy',100,5,80000,'filled',?,?,'btc_close_range_v1',?)",
                (
                    (
                        "2026-09-23 00:00:00",
                        "breakout-k0",
                        json.dumps({"isolated_action": "open"}),
                        "k0",
                    ),
                    (
                        "2026-09-24 00:00:00",
                        "breakout-k1",
                        json.dumps({"isolated_action": "open"}),
                        "k1",
                    ),
                ),
            )


def test_overview_shows_historical_breakout_without_counting_it_as_funded(tmp_path):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    dashboard = _dashboard_module()
    run = dashboard._active_run(db_path)

    overview = dashboard._overview(db_path, run, "sats", "7days", None)

    assert overview.count('class="strategy-summary"') == 2
    assert "winner cooldown 11" in overview
    # Model incomplete outranks the historical-campaign label in the card status.
    assert "Model incomplete · entries blocked" in overview and "20260822L" in overview
    assert "execution-strip" not in overview and "pnl-card" not in overview
    assert "no funded units" in overview
    # Every fundable slot is listed even when flat, so the table keeps its shape.
    assert "<h2>Funded positions</h2>" in overview
    for slot in ("1d", "4h", "k0", "k1", "k2", "k3"):
        assert f"<td>{slot}</td>" in overview
    assert "Recent signals" in overview and "Recent activity" not in overview
    assert "Latest funding" not in overview and "stack-toggle" not in overview
    assert "breakout parent" in overview and "ma daily" in overview
    assert "/strategies/ma" in overview and "/charts?strategy=breakout" in overview
    page = dashboard._render(db_path, "overview", None)
    assert "<span>Execution</span>" in page
    assert page.count('class="topbar-pnl"') == 1 and "<span>Net P&amp;L</span>" in page
    # Execution, price, net P&L, equity; no account source or fetch timestamp.
    bar = [page.index(f'class="topbar-{part}') for part in ("alignment", "market", "pnl", "metric")]
    assert bar == sorted(bar) and "Local account snapshot" not in page
    trades = dashboard._render(db_path, "trades", "1d", pnl_window="30days")
    # The top-bar window toggles keep the current page and its filters.
    assert 'href="/trades?pnl_window=1day&amp;tf=1d"' in trades
    assert 'href="/trades?tf=1d"' in trades and "/signals?pnl_window=30days" in trades
    assert "Action needed" in page and "new breakout entries blocked" in page
    detail = dashboard._strategy_page(db_path, run, "breakout", "sats", None)
    assert "range close $72,968.00" in detail
    assert detail.count('class="stack-unit-row"') == 4
    assert "recovery close" not in detail

    rows = dashboard._position_status_rows(
        [],
        {},
        "sats",
        85_000.0,
        dashboard._breakout_context(dashboard._persisted_breakout_state(db_path), []),
    )
    assert len(rows) == 3
    assert [row["slot"] for row in rows] == ["1d", "4h", "campaign"]
    activity = dashboard._breakout_activity_rows(
        db_path,
        dashboard._persisted_breakout_state(db_path),
        dashboard._historical_paper_position(
            dashboard._breakout_context(dashboard._persisted_breakout_state(db_path), []),
            85_000.0,
        ),
    )
    assert len([row for row in activity if row["source"] == "Historical replay"]) == 7
    assert any(row["reason"] == "addon_cap" for row in activity)
    assert any(row["reason"] == "breakout parent" for row in activity)
    assert [dashboard._parse_ts(row["action_ts"]) for row in activity] == sorted(
        [dashboard._parse_ts(row["action_ts"]) for row in activity], reverse=True
    )

    assert [row["reason"] for row in dashboard._signals(db_path, tf="1d")] == ["ma daily"]
    assert [row["reason"] for row in dashboard._signals(db_path, tf="breakout")] == [
        "breakout parent"
    ]
    nav = dashboard._render(db_path, "signals", "breakout")
    assert "tf=breakout" in nav
    assert ">MA 1d</a>" in nav
    assert ">MA 4h</a>" in nav
    assert ">Breakout</a>" in nav
    assert "Blocked by prior short" in nav
    assert "2026-08-19" in nav
    assert "K0 entry" in nav
    assert "EMA ATR" in nav
    assert "overlap" in nav
    assert "breakout parent" in nav
    assert "ma daily" in dashboard._render(db_path, "signals", None)


def test_execution_shows_retrying_historical_funding_as_pending(tmp_path):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    with sqlite3.connect(db_path) as connection:
        row_id, raw = connection.execute(
            "SELECT id, state_json FROM strategy_state_snapshots "
            "WHERE strategy_name = 'lnmarkets_bot.strategy.close_range_live.CloseRangeLive'"
        ).fetchone()
        state = json.loads(raw)
        state["machine"]["historical_model_complete"] = True
        state["machine"]["historical_funding_available"] = False
        connection.execute(
            "UPDATE strategy_state_snapshots SET state_json = ? WHERE id = ?",
            (json.dumps(state), row_id),
        )

    page = _dashboard_module()._render(db_path, "overview", None)
    assert "Pending" in page
    assert "Historical funding pending; new breakout entries paused" in page
    assert "Historical occupancy needs verified reconstruction" not in page


def test_historical_breakout_paper_mark_is_segregated_from_funded_totals(tmp_path):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    dashboard = _dashboard_module()
    context = dashboard._breakout_context(dashboard._persisted_breakout_state(db_path), [])
    paper = dashboard._historical_paper_position(context, 85_000.0)
    assert paper is not None
    assert [unit["k"] for unit in paper["units"]] == [0, 1, 2, 3]
    assert [unit["signal_ts"][:10] for unit in paper["units"]] == [
        "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-27"
    ]
    expected_sats = round(
        sum(100 * (1 / unit["entry_price"] - 1 / 85_000) for unit in paper["units"])
        * 1e8
    )
    assert paper["gross_sats"] == expected_sats
    rows = dashboard._position_status_rows([], {}, "sats", 85_000.0, context)
    campaign = next(row for row in rows if row["slot"] == "campaign")
    assert campaign["mark_pnl"] == dashboard._format_signed_amount(
        expected_sats, "sats", 85_000.0
    )
    assert [row["slot"] for row in campaign["_children"]] == ["k0", "k1", "k2", "k3"]
    assert all("paper" not in str(row).lower() for row in campaign["_children"])
    assert not any(row["strategy"] == "portfolio" for row in rows)
    assert dashboard._orders(db_path) == []

    # Resizing future funded orders must leave the seeded paper stack at its
    # original $100 per unit.
    context["unit_notional_usd"] = 40
    context["historical_unit_notional_usd"] = 100
    resized_paper = dashboard._historical_paper_position(context, 85_000.0)
    assert resized_paper is not None
    assert resized_paper["total_notional_usd"] == 400
    assert resized_paper["gross_sats"] == expected_sats

    # A changed live parent must suppress the reconstruction instead of showing stale prices.
    context["campaign"]["units"][0]["entry_price"] = 78_000.0
    assert dashboard._historical_paper_position(context, 85_000.0) is None


def test_breakout_exit_display_follows_close_based_campaign_lifecycle():
    dashboard = _dashboard_module()
    campaign = {
        "side": 1, "boundary": 90, "held_days": 84,
        "peak_favorable": 0.2, "units": [{"k": 0, "entry_price": 100}],
    }
    assert dashboard._breakout_exit_trigger(campaign) == "range close $90.00"

    campaign["held_days"] = 85
    trigger = dashboard._breakout_exit_trigger(campaign)
    assert "range close $90.00" in trigger
    assert "recovery close $119.40" in trigger
    assert "cap 35d" in trigger

    campaign.update(side=-1, boundary=110, held_days=120)
    trigger = dashboard._breakout_exit_trigger(campaign)
    assert "range close $110.00" in trigger
    assert "recovery close $80.60" in trigger
    assert "cap 0d" in trigger


def test_historical_paper_reference_matches_independent_seed_replay():
    import pandas as pd

    from lnmarkets_bot.strategy.close_range import CloseRangeMachine, DailyCandle

    dashboard = _dashboard_module()
    reference = dashboard._historical_breakout_reference()
    assert reference is not None
    frame = pd.read_parquet(dashboard.LNM_DAILY_SEED_CACHE)
    frame = frame.loc[frame.ts <= pd.Timestamp(reference["source_as_of"])].copy()
    frame["high"] = frame[["open", "high", "close"]].max(axis=1)
    frame["low"] = frame[["open", "low", "close"]].min(axis=1)
    machine = CloseRangeMachine()
    entry_signals = {}
    for row in frame.itertuples(index=False):
        decisions = machine.advance(
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close),
            activation_ts=dashboard.datetime(2100, 1, 1, tzinfo=dashboard.UTC),
        )
        for decision in decisions:
            if (
                decision.campaign_id == reference["campaign_id"]
                and decision.kind in {"historical_parent", "historical_addon"}
            ):
                entry_signals[decision.k] = decision.metadata["signal_ts"]
    assert machine.campaign is not None
    assert machine.campaign.campaign_id == reference["campaign_id"]
    assert machine.campaign.boundary == pytest.approx(reference["boundary"])
    for unit, recorded in zip(machine.campaign.units, reference["units"], strict=True):
        assert unit.k == recorded["k"]
        assert unit.entry_ts.isoformat() == recorded["entry_ts"]
        assert unit.entry_price == pytest.approx(recorded["entry_price"])
        assert entry_signals[unit.k] == recorded["signal_ts"]


def test_overview_shows_funded_breakout_campaign_and_both_owned_units(tmp_path):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=True)
    dashboard = _dashboard_module()
    run = dashboard._active_run(db_path)

    overview = dashboard._overview(db_path, run, "sats", "7days", None)
    positions = dashboard._open_positions(db_path, dashboard._orders(db_path), None)
    status_rows = dashboard._position_status_rows(
        positions,
        dashboard._persisted_strategy_levels(db_path, 0.005),
        "sats",
        None,
        dashboard._breakout_context(dashboard._persisted_breakout_state(db_path), positions),
    )

    assert "Long · 2/4 units" in overview
    assert "20260923L" in overview
    assert "<h2>Funded positions</h2>" in overview
    assert ">k0<" in overview and ">k1<" in overview
    assert "campaign · 2/4" not in overview
    assert len(status_rows) == 3
    campaign = next(row for row in status_rows if row["slot"] == "campaign")
    assert campaign["contracts"] == "$200"
    assert [row["slot"] for row in campaign["_children"]] == ["k0", "k1"]
    assert all(row["exit_trigger"] == "venue liq." for row in campaign["_children"])
    assert not any(row["strategy"] == "portfolio" for row in status_rows)
    marked_positions = dashboard._open_positions(db_path, dashboard._orders(db_path), 85_000.0)
    marked_context = dashboard._breakout_context(
        dashboard._persisted_breakout_state(db_path), marked_positions
    )
    marked_campaign = dashboard._position_status_rows(
        marked_positions, {}, "sats", 85_000.0, marked_context
    )[-1]
    assert marked_campaign["entry_price"] == "$80,000.00"
    assert marked_campaign["mark_pnl"] == dashboard._format_signed_amount(
        sum(position["estimated_unrealized_sats"] for position in marked_positions),
        "sats",
        85_000.0,
    )
    assert len(dashboard._orders(db_path, tf="breakout")) == 2
    assert dashboard._orders(db_path, tf="1d") == []
    assert dashboard._trade_owners(dashboard._orders(db_path)) == {
        "breakout-k0": ("btc_close_range_v1", "k0"),
        "breakout-k1": ("btc_close_range_v1", "k1"),
    }


def test_trade_quality_groups_breakout_units_by_strategy_not_daily_timeframe(tmp_path):
    db_path = tmp_path / "normalized.sqlite"
    _create_normalized_pnl_db(db_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute("ALTER TABLE orders ADD COLUMN strategy_instance_id TEXT DEFAULT ''")
        connection.execute("ALTER TABLE orders ADD COLUMN position_key TEXT DEFAULT ''")
        connection.execute(
            "UPDATE orders SET strategy_instance_id='ma_cross_primary',position_key='4h' "
            "WHERE lnm_order_id='trade-1'"
        )
        connection.execute(
            "UPDATE orders SET strategy_instance_id='btc_close_range_v1',"
            "position_key='k0',trigger_tf='1d' WHERE lnm_order_id='trade-2'"
        )
    dashboard = _dashboard_module()

    quality, risk = dashboard._strategy_performance_rows(
        db_path,
        "sats",
        None,
        nominal_usd=dashboard.CONSTANT_NOTIONAL_USD,
    )

    # Every strategy and breakout unit keeps a row, traded or not.
    assert [(row["timeframe"], row["closed_trades"]) for row in quality] == [
        ("MA 4h", 1),
        ("MA 1d", 0),
        ("Breakout", 1),
        ("Breakout k0", 1),
        ("Breakout k1", 0),
        ("Breakout k2", 0),
        ("Breakout k3", 0),
        ("Range", 0),
        ("Combined", 2),
    ]
    assert [row["timeframe"] for row in risk] == [row["timeframe"] for row in quality]
    assert quality[1]["win_rate"] == "-" and risk[1]["avg_trade"] == "-"
    assert all("cumulative_return" not in row for row in quality)
    assert (
        len(
            dashboard._trade_history_rows(
                db_path, tf="breakout", denomination="sats", btc_price=None
            )
        )
        == 1
    )
    assert (
        len(dashboard._trade_history_rows(db_path, tf="1d", denomination="sats", btc_price=None))
        == 0
    )


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


def test_signals_are_exposure_decisions_and_the_rest_are_events():
    dashboard = _dashboard_module()
    assert dashboard._is_signal({"kind": "entry", "reason": "4h MA-cross ↑ flip to long"})
    assert dashboard._is_signal({"kind": "reject", "reason": "addon_cap"})
    assert dashboard._is_signal({"kind": "decision", "source": "Replay"})
    # A cooldown only suppresses a signal when the new verdict would open or flip.
    assert dashboard._is_signal({"kind": "noop", "reason": "cool_off", "verdict": "UP_TRUE"})
    assert not dashboard._is_signal({"kind": "noop", "reason": "cool_off", "verdict": "FLAT"})
    for row in (
        {"kind": "noop", "reason": "verdict_flat"},
        {"kind": "noop", "reason": "restart_state_aligned"},
        {"kind": "model", "source": "Model"},
        {"kind": "control", "source": "Decision", "reason": "direction_mode_changed"},
        {"kind": "entry", "reason": "shadow_entry"},
    ):
        assert not dashboard._is_signal(row)


def test_strategy_pages_hold_their_config_and_retired_pages_redirect(tmp_path, monkeypatch):
    db_path = tmp_path / "portfolio.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    dashboard = _dashboard_module()
    run = dashboard._active_run(db_path)
    ma = dashboard._strategy_page(db_path, run, "ma", "sats", None)
    assert "<h2>Configuration</h2>" in ma and "4h CHOP overlay" in ma
    assert "Hard risk limits" not in ma and "<h2>Recent events</h2>" in ma
    # At a glance: ascending slots, short activity tables, then config, then rules.
    assert ma.index("<p>4h</p>") < ma.index("<p>1d</p>")
    order = ("Recent signals", "Recent events", "<h2>Configuration</h2>", "<h2>Sizing</h2>")
    order += ("<h2>Strategy rules</h2>", "<h2>4h CHOP overlay</h2>", "strategy-explainer")
    assert [ma.index(marker) for marker in order] == sorted(ma.index(marker) for marker in order)
    assert 'href="/signals?tf=ma"' in ma and 'href="/signals?tf=ma&amp;events=1"' in ma
    signals = dashboard._render(db_path, "signals", "ma")
    with_events = dashboard._render(db_path, "signals", "ma", show_events=True)
    assert "Signals · MA cross" in signals and "Show non-op events" in signals
    assert 'value="ma"' in signals and " checked " not in signals
    assert "Signals and events · MA cross" in with_events and " checked " in with_events
    breakout = dashboard._strategy_page(db_path, run, "breakout", "sats", None)
    assert "Close-range breakout</h2>" in breakout and "4h CHOP overlay" not in breakout
    health = dashboard._render(db_path, "health", None)
    for heading in ("Active run", "Readiness", "Account-wide", "Hard risk limits"):
        assert heading in health
    assert 'href="/runs' not in health and 'href="/strategies"' not in health
    for target in ("/strategies/ma", "/strategies/breakout", "/strategies/range"):
        assert f'href="{target}"' in health

    monkeypatch.setattr(dashboard._EXCHANGE_CACHE, "disabled", True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard._handler(db_path))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        for path, location in (("/strategies", "/"), ("/runs?denom=usd", "/health?denom=usd")):
            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            connection.request("GET", path)
            response = connection.getresponse()
            assert response.status == 302 and response.getheader("Location") == location
            connection.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)
        server.server_close()
