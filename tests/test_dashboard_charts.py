"""Read-only chart geometry, timing, ownership and HTTP contracts."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from http.server import ThreadingHTTPServer
from itertools import pairwise
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from lnmarkets_bot.dashboard import charts
from tests.test_dashboard import _create_multistrategy_dashboard_db, _dashboard_module


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "chart.sqlite"
    _create_multistrategy_dashboard_db(path, funded=False)
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM bars")
        start = datetime(2026, 9, 20, tzinfo=UTC)
        db.executemany(
            "INSERT INTO bars (run_id,ts,open,high,low,close,volume) VALUES (1,?,100,102,98,101,1)",
            [
                ((start + timedelta(minutes=i)).replace(tzinfo=None).isoformat(" "),)
                for i in range(3 * 1440)
                if i != 100
            ],
        )
    return path


def save_state(path, strategy, state, ts="2026-09-22 16:00:00", run_id=1):
    with sqlite3.connect(path) as db:
        db.execute(
            "DELETE FROM strategy_state_snapshots WHERE strategy_name=?", (charts.OWNERS[strategy],)
        )
        db.execute(
            "INSERT INTO strategy_state_snapshots (run_id,mode,strategy_name,ts,state_json) VALUES (?,'live',?,?,?)",
            (run_id, charts.OWNERS[strategy], ts, json.dumps(state)),
        )


def range_state():
    return {
        "version": 2,
        "mode": "funded",
        "entries_enabled": True,
        "model_complete": True,
        "machine": {
            "version": 3,
            "state": "active",
            "params": {"zone": 0.15, "tolerance": 0.1, "chop_threshold": 0.22},
            "channel": {
                "id": 0,
                "lo": 80,
                "hi": 120,
                "confirmed_ts": "2026-09-20T08:00:00Z",
                "tradeable": False,
                "er_at_confirm": 0.077,
                "redraws": 1,
            },
        },
        "events": [
            {
                "ts": "2026-09-20T04:00:00Z",
                "kind": "confirm",
                "detail": {"id": 0, "lo": 80, "hi": 110},
            },
            {"ts": "2026-09-20T08:00:00Z", "kind": "chop_skip", "detail": {"id": 0, "er": 0.077}},
            {"ts": "2026-09-21T04:00:00Z", "kind": "break", "detail": {"id": 0, "dir": 1}},
            {
                "ts": "2026-09-22T04:00:00Z",
                "kind": "redraw",
                "detail": {"id": 0, "lo": 80, "hi": 120},
            },
        ],
    }


def test_price_aggregation_deduplicates_minutes_and_reports_partial_candles(db_path):
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO bars (run_id,ts,open,high,low,close,volume) VALUES (2,'2026-09-22 23:59:00',105,107,103,106,2)"
        )
    view = charts.chart_data(db_path, tf="1d", days=7)
    assert view["schema_version"] == 1
    assert len(view["candles"]) == 3
    first, _, last = view["candles"]
    assert first["minutes"] == 1439 and not first["complete"]
    assert last["minutes"] == 1440 and last["complete"]
    assert last["close"] == 106 and last["high"] == 107
    assert last["volume"] == 1441
    assert view["coverage"]["missing_minutes"] == 4 * 1440 + 1
    before = db_path.read_bytes()
    charts.chart_data(db_path, tf="1m", days=1)
    assert db_path.read_bytes() == before


def test_range_geometry_uses_knowledge_time_and_keeps_skipped_channel_muted(db_path):
    save_state(db_path, "range", range_state())
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    confirm = next(m for m in view["markers"] if m["kind"] == "confirm")
    redraw = next(m for m in view["markers"] if m["kind"] == "redraw")
    assert confirm["time"] == confirm["observed_at"] + 4 * 3600
    assert redraw["time"] == redraw["observed_at"] + 4 * 3600
    assert confirm["anchor"] == confirm["time"] - 1  # inside the candle that closed
    old = [s for s in view["segments"] if s["label"] == "Channel high" and s["value"] == 110]
    assert old[0]["start"] == confirm["time"]
    # Edges stay drawn while the broken channel expands; trading levels stop at the break.
    assert old[-1]["end"] == redraw["time"]
    buy = [s for s in view["segments"] if s["label"] == "Buy ≤" and s["value"] == 84.5]
    assert buy[-1]["end"] == charts.stamp("2026-09-21T04:00:00Z")
    extreme = sorted(
        (s for s in view["segments"] if s["label"] == "Expansion extreme"), key=lambda s: s["start"]
    )
    assert extreme[0]["start"] == charts.stamp("2026-09-21T04:00:00Z")
    assert extreme[-1]["end"] == redraw["time"] and extreme[-1]["value"] == 102
    new = [s for s in view["segments"] if s["label"] == "Channel high" and s["value"] == 120]
    assert min(s["start"] for s in new) == redraw["time"]
    expansion = ("Expansion", "4h redraw")
    assert all(not s["eligible"] for s in view["segments"] if not s["label"].startswith(expansion))
    assert not any(s["start"] < redraw["time"] < s["end"] for s in new)
    levels = {s["label"]: s["value"] for s in view["segments"] if s["start"] == view["state_as_of"]}
    assert levels["Buy ≤"] == 86 and levels["Sell ≥"] == 114
    assert levels["Midpoint target"] == 100 and levels["4h close stop below"] == 76
    assert view["status"]["admission"] == "Chop skipped"
    assert any(b["role"] == "stop" and b["upper"] == 80 and b["lower"] == 76 for b in view["bands"])
    assert not any(m["origin"] == "Recorded execution" for m in view["markers"])


def test_range_formation_is_replayed_from_events_and_recorded_candles(db_path):
    state = range_state()
    state["machine"].update(channel=None, state="seek", setup={"side": 1, "extreme": 102})
    state["events"] = [
        {"ts": "2026-09-20T08:00:00Z", "kind": "impulse", "detail": {"side": 1, "extreme": 101}}
    ]
    save_state(db_path, "range", state)
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    extreme = sorted(
        (s for s in view["segments"] if s["label"] == "Impulse extreme"), key=lambda s: s["start"]
    )
    # The extreme follows recorded 4h highs from the impulse bar onward, without gaps.
    assert extreme[0]["start"] == charts.stamp("2026-09-20T08:00:00Z")
    assert extreme[0]["value"] == 102 and extreme[-1]["end"] == view["end"]
    assert all(a["end"] == b["start"] for a, b in pairwise(extreme))
    threshold = next(s for s in view["segments"] if s["label"] == "Pullback threshold")
    assert threshold["value"] == pytest.approx(102 * 0.92)
    impulse = next(m for m in view["markers"] if m["kind"] == "impulse")
    assert impulse["layer"] == "model" and impulse["direction"] == 1 and impulse["price"] == 101


def test_current_range_formation_is_not_projected_backwards(db_path):
    state = range_state()
    state["machine"]["channel"] = None
    state["machine"]["state"] = "seek"
    state["machine"]["setup"] = {"side": 1, "extreme": 120, "swing": 90, "pulled": True}
    state["events"] = []
    save_state(db_path, "range", state)
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert all(s["start"] == view["state_as_of"] for s in view["segments"])
    assert (
        next(s["value"] for s in view["segments"] if s["label"].startswith("4h confirmation"))
        == 100
    )
    state["machine"]["channel"] = {"lo": 80, "hi": 120, "expanding": 1, "new_extreme": 140}
    state["machine"]["setup"] = None
    save_state(db_path, "range", state)
    expanded = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert not expanded["bands"]
    assert (
        next(s["value"] for s in expanded["segments"] if s["label"].startswith("4h redraw")) == 120
    )


def test_breakout_boundaries_lag_daily_close_and_units_are_model_references(db_path):
    daily = [{"ts": f"2026-09-{i:02d}T00:00:00Z", "close": 80 + i} for i in range(1, 23)]
    state = {
        "version": 1,
        "direction_mode": "long_only",
        "machine": {
            "candles": daily,
            "historical_model_complete": True,
            "historical_funding_available": True,
            "campaign": {
                "campaign_id": "test",
                "side": 1,
                "origin": "historical",
                "entry_ts": "2026-09-20T00:00:00Z",
                "boundary": 95,
                "held_days": 85,
                "peak_favorable": 0.2,
                "units": [
                    {
                        "k": 0,
                        "entry_ts": "2026-09-20T00:00:00Z",
                        "entry_price": 100,
                        "origin": "historical",
                    }
                ],
            },
        },
    }
    save_state(db_path, "breakout", state)
    view = charts.chart_data(db_path, strategy="breakout", tf="1d", days=7)
    prior = next(s for s in view["segments"] if s["label"].startswith("Prior 20-close upper"))
    assert prior["start"] == charts.stamp("2026-09-21T00:00:00Z") and prior["value"] == 100
    assert next(
        s["value"] for s in view["segments"] if s["label"].startswith("Add-on")
    ) == pytest.approx(115)
    recovery = next(s for s in view["segments"] if s["label"].startswith("97%"))
    assert recovery["value"] == pytest.approx(119.4) and recovery["start"] == view["state_as_of"]
    model = next(m for m in view["markers"] if m["kind"] == "model_entry")
    assert model["origin"] == "Historical model · not funded"
    assert not any(m["kind"] == "entry" for m in view["markers"])


def test_ma_history_matches_complete_local_candles_and_preserves_cooldown(db_path):
    start = datetime(2026, 8, 30, tzinfo=UTC)
    with sqlite3.connect(db_path) as db:
        db.execute("DELETE FROM bars")
        db.executemany(
            "INSERT INTO bars (run_id,ts,open,high,low,close,volume) VALUES (1,?,100,102,98,100,1)",
            [
                ((start + timedelta(minutes=i)).replace(tzinfo=None).isoformat(" "),)
                for i in range(24 * 1440)
            ],
        )
    completed = (start + timedelta(days=24)).isoformat()
    state = {
        "version": 1,
        "strategy_params": {"tolerance_pct": 0.005},
        "timeframes": {
            "1d": {
                "sma": 100,
                "ema": 100,
                "last_bar_ts": completed,
                "verdict": "FLAT",
                "closes": [100] * 24,
            }
        },
        "winner_suppressed_signals": {"1d": 11},
        "loss_suppressed_signals": {"1d": 0},
    }
    save_state(db_path, "ma", state, ts=completed)
    view = charts.chart_data(db_path, strategy="ma", tf="1d", days=30)
    assert view["status"]["winner_remaining"] == 11
    ema = next(s for s in view["series"] if s["label"] == "EMA21")
    sma = next(s for s in view["series"] if s["label"] == "SMA20")
    # SMA20 and EMA21 (seeded with SMA21) from recorded candles: 24 closes give 4 points.
    assert len(ema["points"]) == 4 and len(sma["points"]) == 4
    assert all(p["value"] == pytest.approx(100) for p in ema["points"])
    assert all(p["time"] <= charts.stamp(completed) for s in view["series"] for p in s["points"])
    # Last completed instant equals price history end; no future/current band is fabricated.
    assert not view["segments"]
    assert all(b["end"] <= charts.stamp(completed) for b in view["bands"])
    assert view["bands"][-1]["lower"] == pytest.approx(99.5)
    assert view["bands"][-1]["upper"] == pytest.approx(100.5)
    assert any("not an authoritative" in w for w in view["coverage"]["warnings"])
    with sqlite3.connect(db_path) as db:
        db.execute("DELETE FROM bars WHERE ts='2026-09-20 12:00:00'")
    partial = charts.chart_data(db_path, strategy="ma", tf="1d", days=30)
    # A candle missing a recorded minute still contributes its close.
    assert len(next(s for s in partial["series"] if s["label"] == "EMA21")["points"]) == 4


def test_execution_markers_distinguish_rejection_intent_shadow_and_fills(db_path):
    state = range_state()
    state["paper_trades"] = [
        {
            "entry_ts": "2026-09-22T10:00:00Z",
            "exit_ts": "2026-09-22T11:00:00Z",
            "entry": 80,
            "exit": 100,
        }
    ]
    save_state(db_path, "range", state)
    with sqlite3.connect(db_path) as db:
        for i, status, owner in [
            (101, "filled", charts.OWNERS["range"]),
            (102, "rejected", charts.OWNERS["range"]),
            (103, "filled", charts.OWNERS["ma"]),
        ]:
            db.execute(
                "INSERT INTO orders (id,run_id,trigger_tf,qty_sats,leverage,ts,side,price_usd,status,strategy_instance_id,position_key,metadata_json) VALUES (?,1,'1m',100,5,'2026-09-22 12:00:00','buy',90,?,?,'r0',?)",
                (i, status, owner, json.dumps({"isolated_action": "open"})),
            )
        db.execute("INSERT INTO fills VALUES (1,101,'2026-09-22 12:00:00',100,91,1)")
        db.execute(
            "INSERT INTO signals (run_id,ts,kind,reason,strategy_instance_id,position_key,metadata_json) VALUES (1,'2026-09-22 12:00:00','entry','target',?,'r0','{}')",
            (charts.OWNERS["range"],),
        )
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert len([m for m in view["markers"] if m["kind"] == "entry"]) == 1
    entry = next(m for m in view["markers"] if m["kind"] == "entry")
    assert entry["layer"] == "execution" and entry["direction"] == 1
    assert entry["anchor"] == entry["time"] and "long" in entry["label"]
    assert next(m for m in view["markers"] if m["kind"] == "order_status")["layer"] == "diagnostic"
    assert next(m for m in view["markers"] if m["kind"] == "signal")["layer"] == "intent"
    assert next(m["price"] for m in view["markers"] if m["kind"] == "entry") == 91
    assert any(m["kind"] == "order_status" and "rejected" in m["label"] for m in view["markers"])
    assert any(m["kind"] == "signal" and "not a fill" in m["origin"] for m in view["markers"])
    assert len([m for m in view["markers"] if m["kind"].startswith("shadow_")]) == 2
    view["markers"].clear()
    assert charts.chart_data(db_path, strategy="range", tf="4h", days=7)[
        "markers"
    ]  # cache isolation
    with sqlite3.connect(db_path) as db:
        db.execute("INSERT INTO fills VALUES (2,101,'2026-09-22 12:00:00',100,92,1)")
    assert (
        next(
            m["price"]
            for m in charts.chart_data(db_path, strategy="range", tf="4h", days=7)["markers"]
            if m["kind"] == "entry"
        )
        == 92
    )


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"version": 99},
        {"version": 2, "machine": {"version": 3, "channel": {"lo": "bad", "hi": 100}}},
    ],
)
def test_absent_unknown_or_malformed_state_keeps_prices_available(db_path, state):
    save_state(db_path, "range", state)
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert view["candles"] and not view["segments"]
    assert view["coverage"]["warnings"]
    json.dumps(view, allow_nan=False)


def test_old_run_and_entry_blocks_are_explicit(db_path):
    save_state(db_path, "range", range_state(), run_id=0)
    view = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert "Earlier-run" in view["status"]["admission"]
    with sqlite3.connect(db_path) as db:
        db.execute("UPDATE runs SET config_json=?", (json.dumps({"live_entries_enabled": False}),))
    blocked = charts.chart_data(db_path, strategy="range", tf="4h", days=7)
    assert "owned exits continue" in blocked["status"]["admission"]


def test_parameters_are_bounded_and_missing_database_is_not_created(tmp_path):
    for query in [
        {"days": ["10000"]},
        {"tf": ["1m"], "days": ["90"]},
        {"strategy": ["../../trader.env"]},
        {"end": ["invalid"]},
        {"ma_tf": ["1m"]},
    ]:
        with pytest.raises(ValueError):
            charts.options(query)
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        charts.chart_data(missing)
    assert not missing.exists()


def test_http_chart_assets_and_routes_are_read_only(db_path, monkeypatch):
    dashboard = _dashboard_module()
    monkeypatch.setattr(dashboard._EXCHANGE_CACHE, "disabled", True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard._handler(db_path))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    before = db_path.read_bytes()
    worker.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        for path in [
            "/charts?strategy=range&denom=usd",
            "/strategies",
            "/strategies/ma",
            "/strategies/breakout",
            "/strategies/range",
            "/assets/dashboard_chart.js",
            "/assets/dashboard_chart.css",
            "/healthz",
            "/api/chart?strategy=ma",
        ]:
            with urlopen(base + path, timeout=5) as response:
                assert response.status == 200
                assert response.headers["Cache-Control"] == "no-store"
                body = response.read().decode()
                if path.startswith("/charts"):
                    assert "data-preserve-chart" in body and "strategy=range" in body
        for path, status in [
            ("/api/chart?days=9999", 400),
            ("/api/chart?tf=1m&days=90", 400),
            ("/assets/../../AGENTS.md", 404),
            ("/assets/trader.env", 404),
            ("/strategies/unknown", 404),
        ]:
            with pytest.raises(HTTPError) as error:
                urlopen(base + path, timeout=5)
            assert error.value.code == status
        assert db_path.read_bytes() == before
    finally:
        server.shutdown()
        worker.join(timeout=5)
        server.server_close()


def test_overview_includes_range_model_activity_at_knowledge_time(db_path):
    save_state(db_path, "range", range_state())
    dashboard = _dashboard_module()
    rows = dashboard._signal_timeline_rows(db_path, "range", None, None, include_model_events=True)
    redraw = next(row for row in rows if row["event"] == "Channel redrawn")
    assert charts.stamp(redraw["action_ts"]) == charts.stamp("2026-09-22T08:00:00Z")
    assert redraw["qualifiers"] == "Model observation · not a fill"
