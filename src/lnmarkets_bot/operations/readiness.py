"""Read-only readiness from the active run, durable owners and fresh venue evidence."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

MA = "ma_cross_primary"
BREAKOUT = "btc_close_range_v1"
RANGE = "btc_impulse_range_v1"
# Same grace as the engine: settlements publish minutes after each 8h boundary.
FUNDING_GRACE = timedelta(minutes=15)


def _stamp(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _settlement_pending(machine: dict, now: datetime) -> bool:
    """Only the newest 8h settlement is missing, and it is still within grace."""
    last = machine.get("last_historical_funding_ts")
    boundary = now.replace(hour=now.hour // 8 * 8, minute=0, second=0, microsecond=0)
    return (
        last is not None
        and _stamp(last) >= boundary - timedelta(hours=8)
        and now - boundary < FUNDING_GRACE
    )


def inspect_readiness(
    path: Path, *, venue_ids=None, venue_ts=None, pending_ids=(), now=None
) -> dict:
    now = now or datetime.now(UTC)
    errors = []
    report = {
        "ready": False,
        "errors": errors,
        "pending": [],
        "run_id": None,
        "owners": [],
        "open_trade_ids": [],
    }
    try:
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                errors.append("database integrity failed")
            run = db.execute(
                "SELECT * FROM runs WHERE mode='live' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if run is None:
                errors.append("no live run")
                return report
            report["run_id"] = run["id"]
            if run["status"] != "running" or run["ended_at"] is not None:
                errors.append("trader run is not running")
            cfg = json.loads(run["config_json"] or "{}")
            report["entries_enabled"] = cfg.get("live_entries_enabled", True)
            bindings = json.loads(run["strategy_params_json"] or "{}")
            required = {MA}
            if cfg.get("strategy_breakout_enabled"):
                required.add(BREAKOUT)
            if cfg.get("strategy_range_mode", "off") != "off":
                required.add(RANGE)
            report["owners"] = sorted(bindings)
            for owner in required - set(bindings):
                errors.append(f"configured owner absent: {owner}")
            bars = db.execute("SELECT MAX(ts) FROM bars WHERE run_id=?", (run["id"],)).fetchone()[0]
            if bars is None or not -60 <= (now - _stamp(bars)).total_seconds() <= 180:
                errors.append("minute feed unavailable or stale")
            unresolved = db.execute(
                "SELECT COUNT(*) FROM execution_commands WHERE status IN ('submitted','received')"
            ).fetchone()[0]
            if unresolved:
                errors.append(f"unresolved execution commands: {unresolved}")
            opened = db.execute(
                "SELECT * FROM orders WHERE id IN (SELECT MAX(id) FROM orders WHERE lnm_order_id IS NOT NULL GROUP BY lnm_order_id)"
            ).fetchall()
            opened = [
                r
                for r in opened
                if json.loads(r["metadata_json"] or "{}").get("isolated_action") == "open"
            ]
            report["open_trade_ids"] = sorted(str(r["lnm_order_id"]) for r in opened)
            for row in opened:
                owner = row["strategy_instance_id"] or MA
                if owner not in bindings:
                    errors.append(f"owned position without binding: {owner}")
            required |= {r["strategy_instance_id"] or MA for r in opened}
            for owner in required | set(bindings):
                row = db.execute(
                    "SELECT * FROM strategy_state_snapshots WHERE mode='live' AND strategy_name=?",
                    (owner,),
                ).fetchone()
                if row is None or row["run_id"] != run["id"]:
                    errors.append(f"current-run snapshot absent: {owner}")
                    continue
                state = json.loads(row["state_json"])
                if state.get("engine_data_health"):
                    errors.append(f"market evidence incomplete: {owner}")
                machine = state.get("machine", {})
                if owner == RANGE:
                    if not state.get("model_complete", False):
                        errors.append("range reconstruction incomplete")
                    if state.get("closing"):
                        errors.append("range close pending")
                    effective = (
                        bindings.get(owner, {}).get("params", {}).get("entries_enabled", True)
                    )
                    if state.get("entries_enabled") != effective:
                        errors.append("range admission policy disagrees with binding")
                if owner == BREAKOUT and not machine.get("historical_model_complete", False):
                    errors.append("breakout reconstruction incomplete")
                if owner == BREAKOUT and not machine.get("historical_funding_available", True):
                    if _settlement_pending(machine, now.astimezone(UTC)):
                        report["pending"].append("breakout funding settlement not yet published")
                    else:
                        errors.append("breakout funding evidence incomplete")
            if (
                venue_ids is None
                or venue_ts is None
                or not -60 <= (now - venue_ts).total_seconds() <= 60
            ):
                errors.append("venue inventory unavailable or stale")
            elif pending_ids:
                errors.append("venue has pending isolated orders")
            elif set(report["open_trade_ids"]) != set(venue_ids):
                errors.append("venue inventory disagrees with durable ledger")
    except (sqlite3.Error, ValueError, TypeError, KeyError) as exc:
        errors.append(f"readiness evidence invalid: {type(exc).__name__}")
    report["ready"] = not errors
    return report
