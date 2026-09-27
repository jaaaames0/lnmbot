import sqlite3
from datetime import UTC, datetime

import pytest
from scripts.check_remediation_readiness import inspect_database

from tests.test_breakout_funded_campaign_trace import _funded_stack


def test_empty_database_is_not_a_readiness_pass(tmp_path):
    path = tmp_path / "empty.sqlite"
    sqlite3.connect(path).close()
    result = inspect_database(path)
    assert not result["local_consistency_pass"]
    assert "orders" in result["missing_tables"]


@pytest.mark.asyncio
async def test_preflight_detects_partial_records_and_does_not_mutate(cfg):
    _v, _e, r, _f, _p, _s = await _funded_stack(cfg)
    result = inspect_database(cfg.storage_db_path)
    assert result["local_consistency_pass"]
    assert not result["deployment_approved"]
    with sqlite3.connect(cfg.storage_db_path) as db:
        db.execute("DELETE FROM fills WHERE order_id=1")
        db.execute("UPDATE daily_pnl SET realized_pnl_sats=99")
    r.begin_command("entry:uncertain", "entry", {"ts": datetime.now(UTC).isoformat()})
    before = cfg.storage_db_path.read_bytes()
    result = inspect_database(cfg.storage_db_path)
    assert result["missing_fills"] == [1]
    assert result["daily_total_mismatches"]
    assert result["unresolved_commands"][0]["command_key"] == "entry:uncertain"
    assert not result["local_consistency_pass"]
    assert cfg.storage_db_path.read_bytes() == before


@pytest.mark.asyncio
async def test_preflight_prefers_effective_instance_state_over_retained_legacy_snapshot(cfg):
    import json

    await _funded_stack(cfg)
    with sqlite3.connect(cfg.storage_db_path) as db:
        db.execute(
            "INSERT INTO strategy_state_snapshots (run_id,mode,strategy_name,ts,state_json) VALUES (1,'live','lnmarkets_bot.strategy.close_range_live.CloseRangeLive','2026-08-28 00:00:00',?)",
            (json.dumps({"machine": {"campaign": {"origin": "historical"}}}),),
        )
    report = inspect_database(cfg.storage_db_path)
    assert report["local_consistency_pass"]
    assert report["historical_reconstruction_required"] == []
    assert report["superseded_legacy_snapshots"] == [
        "lnmarkets_bot.strategy.close_range_live.CloseRangeLive"
    ]
