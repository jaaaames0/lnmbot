import sqlite3

from tests.test_dashboard import _dashboard_module


def test_dashboard_unresolved_entry_is_visible_even_without_remote_exposure(tmp_path):
    path = tmp_path / "commands.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE execution_commands (action TEXT,status TEXT)")
        db.execute("INSERT INTO execution_commands VALUES ('entry','submitted')")
    dashboard = _dashboard_module()
    status, detail, style = dashboard._execution_alignment(path, [], None)
    assert status == "Action needed" and style == "alert"
    assert "new entries blocked" in detail


def test_long_held_position_is_not_lost_behind_500_newer_orders(tmp_path):
    from tests.test_dashboard import _create_multistrategy_dashboard_db

    path = tmp_path / "old-owned.sqlite"
    _create_multistrategy_dashboard_db(path, funded=True)
    with sqlite3.connect(path) as db:
        db.executemany(
            "INSERT INTO orders (run_id,ts,trigger_tf,side,qty_sats,leverage,price_usd,status,lnm_order_id,metadata_json,strategy_instance_id,position_key) VALUES (1,'2026-09-25 00:00:00','4h','sell',100,5,80000,'filled',?,'{\"isolated_action\":\"close\"}','ma_cross_primary','4h')",
            [(f"decoy-{i}",) for i in range(510)],
        )
    dashboard = _dashboard_module()
    overview = dashboard._overview(path, dashboard._active_run(path), "sats", "7days", None)
    assert "Long · 2/4 units" in overview
    assert "Closing campaign" not in overview


def test_unknown_external_cause_is_not_reported_as_liquidation(tmp_path):
    import json

    path = tmp_path / "external.sqlite"
    state = {
        "winner_suppressed_signals": {"1d": 0, "4h": 0},
        "loss_suppressed_signals": {"1d": 3, "4h": 0},
        "last_external_closures": {"1d": {"trade_id": "closed", "liquidated": None}},
    }
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE strategy_state_snapshots (mode TEXT,strategy_name TEXT,state_json TEXT)")
        db.execute("INSERT INTO strategy_state_snapshots VALUES ('live','ma_cross_primary',?)", (json.dumps(state),))
    dashboard = _dashboard_module()
    cooldown = dashboard._persisted_cooldowns(path)["1d"]
    card = dashboard._position_card("1d", None, "sats", 80000, None, cooldown)
    assert "Cool-off active" in card and "cause unavailable" in card
    assert "liquidation" not in card.lower()
