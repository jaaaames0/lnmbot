from __future__ import annotations

import fcntl
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def module():
    path = ROOT / "scripts/run_breakout_shadow_job.py"
    spec = importlib.util.spec_from_file_location("breakout_shadow_job_test", path)
    assert spec and spec.loader
    result = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = result
    spec.loader.exec_module(result)
    return result


def daily_history() -> pd.DataFrame:
    end = datetime(2026, 9, 21, tzinfo=UTC)
    start = end - timedelta(days=130)
    rows = []
    for index in range(131):
        price = 70_000.0 + index
        rows.append(
            {
                "ts": start + timedelta(days=index),
                "open": price,
                "high": price + 100,
                "low": price - 100,
                "close": price + 25,
                "volume": 1.0,
            }
        )
    return pd.DataFrame(rows)


def test_job_refreshes_then_initializes_order_incapable_state(tmp_path, monkeypatch):
    job = module()
    cache = tmp_path / "daily.parquet"
    database = tmp_path / "portfolio.sqlite"
    lock = tmp_path / "job.lock"
    source = daily_history()
    calls = []

    def refresh(**kwargs):
        calls.append(kwargs)
        source.to_parquet(kwargs["cache_path"], index=False)
        return source

    monkeypatch.setattr(job, "refresh_completed_cache", refresh)
    now = datetime(2026, 9, 22, 5, tzinfo=UTC)
    summary = job.run_job(
        snapshot_path=ROOT / "config/seeds/btc-close-range-shadow-seed-2026-09-22.json",
        cache_path=cache,
        database_path=database,
        lock_path=lock,
        bootstrap_start=source.iloc[0].ts.to_pydatetime(),
        now=now,
    )
    assert calls and calls[0]["interval"] == "1d"
    assert summary["order_capability"] is False
    assert summary["last_candle_ts"] == "2026-09-21T00:00:00+00:00"
    assert summary["campaign"]["campaign_id"] == "20260822L"
    assert summary["campaign"]["origin"] == "historical"
    assert database.exists()
    # Exact retry leaves the state and source usable.
    again = job.run_job(
        snapshot_path=ROOT / "config/seeds/btc-close-range-shadow-seed-2026-09-22.json",
        cache_path=cache,
        database_path=database,
        lock_path=lock,
        bootstrap_start=source.iloc[0].ts.to_pydatetime(),
        now=now,
    )
    assert again["days_advanced"] == 0


def test_job_skips_a_concurrent_run_before_market_fetch(tmp_path, monkeypatch):
    job = module()
    lock_path = tmp_path / "job.lock"
    lock_path.touch()
    called = False

    def refresh(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(job, "refresh_completed_cache", refresh)
    with lock_path.open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = job.run_job(
            snapshot_path=tmp_path / "unused.json",
            cache_path=tmp_path / "daily.parquet",
            database_path=tmp_path / "portfolio.sqlite",
            lock_path=lock_path,
            bootstrap_start=datetime(2026, 1, 1, tzinfo=UTC),
            now=datetime(2026, 1, 2, tzinfo=UTC),
        )
    assert result == {
        "mode": "forward_shadow_no_orders",
        "order_capability": False,
        "skipped": "locked",
    }
    assert called is False


def test_job_refuses_var_lib_without_installed_service_flag(tmp_path):
    job = module()
    try:
        job.run_job(
            snapshot_path=tmp_path / "unused.json",
            cache_path=Path("/var/lib/lnmbot-shadow/cache.parquet"),
            database_path=tmp_path / "portfolio.sqlite",
            lock_path=tmp_path / "lock",
            bootstrap_start=datetime(2026, 1, 1, tzinfo=UTC),
            now=datetime(2026, 1, 2, tzinfo=UTC),
        )
    except ValueError as exc:
        assert "production state" in str(exc)
    else:
        raise AssertionError("production path was accepted")


def test_installed_flag_is_limited_to_dedicated_shadow_paths(tmp_path):
    job = module()
    try:
        job.run_job(
            snapshot_path=tmp_path / "unused.json",
            cache_path=Path("/var/lib/lnmbot-shadow/cache.parquet"),
            database_path=Path("/var/lib/lnmbot/lnmarkets.sqlite"),
            lock_path=Path("/run/lnmbot-shadow/job.lock"),
            bootstrap_start=datetime(2026, 1, 1, tzinfo=UTC),
            now=datetime(2026, 1, 2, tzinfo=UTC),
            allow_production_path=True,
        )
    except ValueError as exc:
        assert "dedicated state" in str(exc)
    else:
        raise AssertionError("funded trader database path was accepted")


def test_seed_is_minimal_and_order_incapable():
    seed = json.loads((ROOT / "config/seeds/btc-close-range-shadow-seed-2026-09-22.json").read_text())
    assert seed["order_capability"] is False
    assert "historical_replay_metrics" not in seed
    assert seed["active_hypothetical_stack"]["parent_id"] == "20260822L"


def test_systemd_candidate_is_isolated_order_incapable_and_retries():
    service = (ROOT / "scripts/lnmbot-breakout-shadow.service").read_text()
    timer = (ROOT / "scripts/lnmbot-breakout-shadow.timer").read_text()
    assert "User=lnmbot-shadow" in service
    assert "Group=lnmbot-shadow-db" in service
    assert "InaccessiblePaths=-/var/lib/lnmbot" in service
    assert "InaccessiblePaths=-/etc/lnmbot" in service
    assert "--installed-order-incapable-service" in service
    assert "--allow-orders" not in service
    assert "LNM_" not in service
    assert timer.count("OnCalendar=") == 3
    assert "Persistent=true" in timer
