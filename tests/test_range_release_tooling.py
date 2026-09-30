"""Readiness, seed normalization and deployment failure gates, using local fixtures."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from scripts.deploy_range_remediation import SEEDS, accept, normalize_env
from scripts.rebuild_impulse_range import reconstruct

from lnmarkets_bot.operations.readiness import BREAKOUT, MA, RANGE, inspect_readiness
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder


@pytest.fixture
def ready_db(tmp_path):
    path = tmp_path / "state.sqlite"
    engine = make_engine(path)
    init_schema(engine)
    recorder = Recorder(make_session_factory(engine))
    now = datetime.now(UTC)
    owners = {owner: {"params": {"entries_enabled": True}} for owner in (MA, BREAKOUT, RANGE)}
    run = recorder.start_run(
        mode="live",
        strategy_name="portfolio",
        strategy_params=owners,
        config={"strategy_range_mode": "funded", "strategy_breakout_enabled": True},
        started_at=now,
    )
    recorder.record_bar(run, ts=now, open=100, high=100, low=100, close=100, volume=0)
    for owner in owners:
        recorder.save_strategy_state(
            run,
            mode="live",
            strategy_name=owner,
            ts=now,
            state={
                "model_complete": True,
                "entries_enabled": True,
                "machine": {
                    "historical_model_complete": True,
                    "historical_funding_available": True,
                },
            },
        )
    return path, now, run


def test_ready_db_requires_fresh_venue_and_current_run(ready_db):
    path, now, _ = ready_db
    assert inspect_readiness(path, venue_ids=set(), venue_ts=now)["ready"]
    assert not inspect_readiness(path)["ready"]
    assert not inspect_readiness(path, venue_ids=set(), venue_ts=now - timedelta(seconds=61))[
        "ready"
    ]
    with sqlite3.connect(path) as db:
        db.execute("UPDATE strategy_state_snapshots SET run_id=0 WHERE strategy_name=?", (RANGE,))
    assert not inspect_readiness(path, venue_ids=set(), venue_ts=now)["ready"]


@pytest.mark.parametrize(
    "failure", ["owner", "incomplete", "gap", "inactive", "stale", "command", "venue"]
)
def test_readiness_rejects_unhealthy_states(ready_db, failure):
    path, now, _ = ready_db
    with sqlite3.connect(path) as db:
        if failure == "owner":
            db.execute(
                "UPDATE runs SET strategy_params_json=?", (json.dumps({MA: {}, BREAKOUT: {}}),)
            )
        elif failure in ("incomplete", "gap"):
            state = {
                "model_complete": failure != "incomplete",
                "engine_data_health": {"4h": now.isoformat()} if failure == "gap" else {},
            }
            db.execute(
                "UPDATE strategy_state_snapshots SET state_json=? WHERE strategy_name=?",
                (json.dumps(state), RANGE),
            )
        elif failure == "inactive":
            db.execute("UPDATE runs SET status='finished'")
        elif failure == "stale":
            db.execute("UPDATE bars SET ts=?", ((now - timedelta(minutes=4)).isoformat(),))
        elif failure == "command":
            db.execute(
                "INSERT INTO execution_commands(command_key,action,status,request_json,notified) VALUES ('test','entry','submitted','{}',0)"
            )
    ids = {"unknown"} if failure == "venue" else set()
    assert not inspect_readiness(path, venue_ids=ids, venue_ts=now)["ready"]


@pytest.mark.parametrize("stage", ["readiness", "backup", "second_readiness"])
def test_failed_acceptance_never_disarms(tmp_path, stage):
    (tmp_path / "candidate.json").write_text("{}")
    disarmed = []
    calls = []

    def readiness(_):
        calls.append(1)
        if stage == "readiness" or (stage == "second_readiness" and len(calls) == 2):
            raise RuntimeError("unhealthy")
        return {"ready": True}

    def backup():
        if stage == "backup":
            raise RuntimeError("backup failed")

    with pytest.raises(RuntimeError):
        accept(
            tmp_path, readiness=readiness, backup_fn=backup, disarm=lambda: disarmed.append(True)
        )
    assert not disarmed


def test_acceptance_disarms_last(tmp_path):
    (tmp_path / "candidate.json").write_text("{}")
    calls = []
    accept(
        tmp_path,
        readiness=lambda _: calls.append("ready") or {"ready": True},
        backup_fn=lambda: calls.append("backup"),
        disarm=lambda: calls.append("disarm"),
    )
    assert calls == ["ready", "backup", "ready", "disarm"]


def test_normalize_seed_roles_and_recovery_do_not_change_risk(tmp_path):
    release = Path(__file__).resolve().parents[1]
    text = "\n".join(
        f"{key}={release / 'config/seeds' / filename}" for key, filename in SEEDS.items()
    )
    text += "\nRISK_MAX_DAILY_LOSS_USD=2000\nSTRATEGY_RANGE_MODE=funded\n"
    candidate = normalize_env(text, release)
    assert "LIVE_ENTRIES_ENABLED=true" in candidate
    assert "LIVE_ENTRIES_ENABLED=false" in normalize_env(text, release, recovery=True)
    assert "RISK_MAX_DAILY_LOSS_USD=2000" in candidate
    assert "STRATEGY_RANGE_MODE=funded" in candidate
    with pytest.raises(ValueError, match="missing seed"):
        normalize_env("", release)


@pytest.mark.asyncio
async def test_reconstruction_rejects_missing_minute_before_seed_loading(tmp_path):
    path = tmp_path / "minutes.parquet"
    pd.DataFrame({"ts": pd.to_datetime(["2026-09-01T00:00Z", "2026-09-01T00:02Z"])}).to_parquet(
        path
    )
    with pytest.raises(ValueError, match="gaps"):
        await reconstruct(tmp_path / "unused.parquet", path, {})


def test_pending_venue_order_is_not_ready(ready_db):
    path, now, _ = ready_db
    report = inspect_readiness(path, venue_ids=set(), venue_ts=now, pending_ids={"pending"})
    assert not report["ready"]
    assert "venue has pending isolated orders" in report["errors"]


@pytest.mark.parametrize("failure", ["arm", "verify_arm"])
def test_timer_failure_precedes_every_live_install(tmp_path, monkeypatch, failure):
    from scripts import deploy_range_remediation as deploy

    unit = tmp_path / "lnmbot.service"
    dashboard_unit = tmp_path / "dashboard.service"
    env = tmp_path / "protected.env"
    database = tmp_path / "db.sqlite"
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    unit.write_text("WorkingDirectory=/previous-trader\n")
    dashboard_unit.write_text("WorkingDirectory=/previous-dashboard\n")
    env.write_text("CONFIG=unchanged\n")
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE evidence(id INTEGER)")
    candidate = {role: str(tmp_path / role) for role in ("trader", "dashboard", "recovery")}
    (checkpoint / "candidate.json").write_text(json.dumps(candidate))
    for name, value in [
        ("TRADER_UNIT", unit),
        ("DASHBOARD_UNIT", dashboard_unit),
        ("ENV", env),
        ("DB", database),
    ]:
        monkeypatch.setattr(deploy, name, value)
    monkeypatch.setattr(deploy, "verify_runtime", lambda *_: None)
    monkeypatch.setattr(deploy, "normalize_env", lambda text, *_a, **_kw: text)
    monkeypatch.setattr(deploy, "backup", lambda: None)
    installed = []
    monkeypatch.setattr(deploy, "install", lambda *_a, **_kw: installed.append(True))

    def run(*args, **_kwargs):
        if (failure == "arm" and args[0] == "systemd-run") or (
            failure == "verify_arm" and args[0:3] == ("systemctl", "is-active", "--quiet")
        ):
            raise RuntimeError("cannot arm recovery")

    monkeypatch.setattr(deploy, "run", run)
    with pytest.raises(RuntimeError, match="cannot arm"):
        deploy.cutover(checkpoint)
    assert not installed
    assert env.read_text() == "CONFIG=unchanged\n"


def test_deployment_acceptance_requires_selected_range_scope(monkeypatch):
    from scripts import deploy_range_remediation as deploy

    calls = []
    candidate = {"trader": "/candidate/trader", "dashboard": "/candidate/dashboard"}
    monkeypatch.setattr(
        deploy,
        "current",
        lambda unit: candidate["trader" if unit == deploy.TRADER_UNIT else "dashboard"],
    )
    monkeypatch.setattr(deploy, "run", lambda *_a, **_kw: "0")

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def read(self, *_args):
            return self.payload

    def request(url, **_kwargs):
        calls.append(url)
        if url.endswith("readyz"):
            return Response(
                json.dumps(
                    {"ready": True, "entries_enabled": True, "owners": [MA, BREAKOUT, RANGE]}
                ).encode()
            )
        return Response(b'<a class="scope-link" href="/signals?tf=range">Range</a>')

    monkeypatch.setattr(deploy.urllib.request, "urlopen", request)
    with pytest.raises(RuntimeError, match="range route"):
        deploy.require_readiness(candidate)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_verified_reconstruction_preserves_daily_seed_seam(tmp_path):
    start = pd.Timestamp("2026-09-01", tz="UTC")
    daily = tmp_path / "daily.parquet"
    minutes = tmp_path / "minutes.parquet"

    def frame(stamps):
        return pd.DataFrame(
            {"ts": stamps, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0}
        )

    frame(pd.date_range(start - pd.Timedelta(days=150), periods=150, freq="D")).to_parquet(daily)
    frame(pd.date_range(start, periods=1440, freq="min")).to_parquet(minutes)
    report = await reconstruct(daily, minutes, {})
    assert report["dry_run"] and report["state"]["model_complete"]
    assert report["state"]["machine"]["detector"]["count"] == 151
    assert report["state"]["machine"]["detector"]["last_ts"] == start.isoformat()
    assert report["state"]["machine"]["position"] is None
    assert set(report["inputs"]) == {str(daily), str(minutes)}
