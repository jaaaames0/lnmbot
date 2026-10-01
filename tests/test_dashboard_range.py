"""Dashboard panel for the impulse-range strategy, rendered from a real adapter snapshot."""

from __future__ import annotations

import json
import sqlite3

import pytest

from lnmarkets_bot.strategy import StrategyState
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.impulse_range_live import SLOT, ImpulseRangeLive
from tests.test_dashboard import _create_multistrategy_dashboard_db, _dashboard_module
from tests.test_impulse_range import synthetic_minutes, synthetic_path
from tests.test_impulse_range_live import feed, primed

RANGE_ID = "btc_impulse_range_v1"


@pytest.fixture(scope="module")
def shadow_state():
    """Run the shadow adapter until it holds an active channel and has paper trades."""
    bars, daily = synthetic_path(days=420)
    minutes = synthetic_minutes(bars[150 * 6 :])
    s = primed(
        ImpulseRangeLive({"mode": "shadow", "unit_notional_usd": 100, "chop_filter": False}),
        bars,
    )
    state = StrategyState()
    state.positions[SLOT] = TfPosition()
    s.on_startup(state)
    best = None
    for bar in feed(bars, daily, minutes):
        s.on_bar(bar, state)
        m = s.range_machine
        if (
            bar.timeframe == "4h"
            and m.state == "active"
            and not m.channel.expanding
            and s.paper_totals["trades"] >= 3
        ):
            best = json.loads(json.dumps(s.persistent_state()))
            if s.paper_position is not None:
                break
    assert best is not None
    return best


def _db_with_range(tmp_path, state, *, config=None, funded_position=False):
    db_path = tmp_path / "range.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE runs SET config_json=?, strategy_params_json=? WHERE id=1",
            (
                json.dumps(config or {"strategy_range_mode": "shadow"}),
                json.dumps(
                    {
                        "ma_cross_primary": {"params": {"tolerance_pct": 0.005}},
                        RANGE_ID: {
                            "params": {
                                "mode": state["mode"],
                                "chop_filter": True,
                                "chop_threshold": 0.22,
                                "unit_notional_usd": 100,
                                "leverage": 2,
                            }
                        },
                    }
                ),
            ),
        )
        connection.execute(
            "INSERT INTO strategy_state_snapshots (run_id,mode,strategy_name,ts,state_json) "
            "VALUES (1,'live',?,'2026-09-22 16:00:00',?)",
            (RANGE_ID, json.dumps(state)),
        )
        if funded_position:
            connection.execute(
                "INSERT INTO orders (run_id,ts,trigger_tf,side,qty_sats,leverage,price_usd,status,"
                "lnm_order_id,metadata_json,strategy_instance_id,position_key) "
                "VALUES (1,'2026-09-22 12:01:00','1m','buy',100,2,60000,'filled','range-1',?,?,'r0')",
                (json.dumps({"isolated_action": "open", "lnm_trade_id": "range-1"}), RANGE_ID),
            )
    return db_path


def test_overview_compact_range_with_details_on_strategy_page(tmp_path, shadow_state):
    dashboard = _dashboard_module()
    db_path = _db_with_range(tmp_path, shadow_state)
    run = dashboard._active_run(db_path)
    overview = dashboard._overview(db_path, run, "sats", "7days", None)
    channel = shadow_state["machine"]["channel"]

    assert 'data-strategy="range"' in overview
    assert "Shadow · Flat" in overview
    assert f"Range #{channel['id']} active" in overview
    assert "Buy ≤" not in overview and "Stop on 4h close beyond" not in overview
    assert "<h2>Range shadow book</h2>" not in overview
    assert "<h2>Funded positions</h2>" in overview and "<td>r0</td>" in overview
    detail = dashboard._strategy_page(db_path, run, "range", "sats", None)
    assert "Buy ≤" in detail and "Stop on 4h close beyond" in detail
    assert "ER at confirmation" in detail
    assert "<h2>Range shadow book</h2>" in detail
    assert "excluded from account totals" in detail
    assert "Impulse signal" not in overview

    context = dashboard._range_context(dashboard._persisted_range_state(db_path), [], 60_000.0)
    levels = context["levels"]
    lo, hi = channel["lo"], channel["hi"]
    assert levels["mid"] == pytest.approx((lo + hi) / 2)
    assert levels["buy"] == pytest.approx(lo + 0.15 * (hi - lo))
    assert levels["stop_hi"] == pytest.approx(hi + 0.10 * (hi - lo))
    rows = dashboard._range_trade_rows(context)
    assert len(rows) == min(10, len(shadow_state["paper_trades"]))
    assert rows[0]["exit_ts"] == shadow_state["paper_trades"][-1]["exit_ts"]


def test_range_hidden_when_off_and_shown_in_config_and_explainer(tmp_path, shadow_state):
    dashboard = _dashboard_module()
    db_path = tmp_path / "plain.sqlite"
    _create_multistrategy_dashboard_db(db_path, funded=False)
    run = dashboard._active_run(db_path)
    assert "post-impulse channel" not in dashboard._overview(db_path, run, "sats", "7days", None)

    db_path = _db_with_range(tmp_path, shadow_state)
    run = dashboard._active_run(db_path)
    assert "Impulse range" in dashboard._active_config(run)
    explainer = dashboard._strategy_explainer(run)
    assert "Impulse range:" in explainer
    assert "below 0.22 are tracked but not traded" in explainer
    assert "Shadow mode: paper fills only" in explainer


def test_range_signals_filter_and_labels(tmp_path, shadow_state):
    dashboard = _dashboard_module()
    db_path = _db_with_range(tmp_path, shadow_state)
    with sqlite3.connect(db_path) as connection:
        connection.executemany(
            "INSERT INTO signals (run_id,ts,kind,side,target_size_usd,target_leverage,reason,"
            "metadata_json,strategy_instance_id,position_key) VALUES (1,?,'noop',NULL,0,1,?,?,?,'1m')",
            (
                (
                    "2026-09-22 10:00:00",
                    "shadow_entry",
                    json.dumps(
                        {
                            "shadow": True,
                            "side": 1,
                            "entry": 60_000,
                            "range_id": 7,
                            "trigger_tf": "1m",
                        }
                    ),
                    RANGE_ID,
                ),
                (
                    "2026-09-22 11:00:00",
                    "shadow_exit",
                    json.dumps(
                        {
                            "shadow": True,
                            "side": 1,
                            "reason": "target",
                            "net_pct": 1.25,
                            "range_id": 7,
                            "trigger_tf": "1m",
                        }
                    ),
                    RANGE_ID,
                ),
            ),
        )
    assert len(dashboard._signals(db_path, tf="range")) == 2
    assert all(s["strategy"] == RANGE_ID for s in dashboard._signals(db_path, tf="range"))
    rows = dashboard._signal_timeline_rows(db_path, "range", None, None)
    assert [r["event"] for r in rows] == ["Paper exit", "Paper enter long"]
    assert rows[0]["detail"] == "range #7 · Midpoint target · +1.25%"
    assert rows[1]["detail"] == "range #7 · at $60,000.00"
    assert all(r["strategy"] == "Range" for r in rows)
    # Shadow fills are not funded exposure changes: events, not signals.
    page = dashboard._render(db_path, "signals", "range")
    assert ">Range</a>" in page and "Paper exit" not in page
    run = dashboard._active_run(db_path)
    detail = dashboard._strategy_page(db_path, run, "range", "sats", None)
    events = detail[detail.index("<h2>Recent events</h2>") :]
    assert "Paper exit" in events and "Paper enter long" in events


def test_alignment_flags_incomplete_model_and_unmodelled_funded_position(tmp_path, shadow_state):
    dashboard = _dashboard_module()
    state = {
        **shadow_state,
        "mode": "funded",
        "model_complete": False,
        "incomplete_reason": "4h evidence gap after 2026-09-22T08:00:00+00:00",
    }
    db_path = _db_with_range(
        tmp_path, state, config={"strategy_range_mode": "funded"}, funded_position=True
    )
    positions = dashboard._open_positions(db_path, dashboard._orders(db_path), None)
    assert [p["strategy"] for p in positions if p["strategy"] == RANGE_ID]
    status, detail, _ = dashboard._execution_alignment(db_path, positions, None)
    assert status == "Action needed"
    assert "Range model incomplete (4h evidence gap" in detail
    context = dashboard._range_context(dashboard._persisted_range_state(db_path), positions, None)
    card = dashboard._range_card(context, "sats", None)
    assert "Impulse range · Funded" in card and "Funded long $100" in card
    assert "Model incomplete" in card
    row = dashboard._range_position_row(context, "sats", None)
    assert row["exposure"] == "$100 · 2.0x" and row["side"] == "long"


def test_card_shows_configured_mode_before_first_snapshot():
    dashboard = _dashboard_module()
    context = dashboard._range_context(None, [], None, "funded")
    card = dashboard._range_card(context, "sats", None)
    assert "Impulse range · Funded" in card and "Awaiting state" in card
    assert "Shadow" not in card
