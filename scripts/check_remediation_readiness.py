"""Read-only preflight for the execution-ledger remediation.

No exchange access, migrations or repair writes. Use an operator-created DB
copy and compare this report with a separately obtained venue inventory.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def inspect_database(path: Path) -> dict:
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        report = {
            "integrity": db.execute("PRAGMA quick_check").fetchone()[0],
            "missing_tables": sorted(
                {
                    "orders",
                    "fills",
                    "strategy_pnl_events",
                    "daily_pnl",
                    "funding_fees",
                    "strategy_state_snapshots",
                }
                - tables
            ),
            "missing_fills": [],
            "duplicate_trade_actions": [],
            "funding_attribution_mismatches": [],
            "daily_total_mismatches": [],
            "undelivered_external_events": [],
            "missing_pnl_events": [],
            "unresolved_commands": [],
            "historical_reconstruction_required": [],
            "superseded_legacy_snapshots": [],
        }
        if "orders" in tables:
            rows = db.execute("SELECT * FROM orders ORDER BY id").fetchall()
            seen = set()
            for row in rows:
                meta = json.loads(row["metadata_json"] or "{}")
                action = meta.get("isolated_action")
                trade_id = row["lnm_order_id"]
                if not trade_id or action not in {"open", "close", "external_close"}:
                    continue
                if (
                    "fills" not in tables
                    or not db.execute(
                        "SELECT 1 FROM fills WHERE order_id=?", (row["id"],)
                    ).fetchone()
                ):
                    report["missing_fills"].append(row["id"])
                identity = (trade_id, "open" if action == "open" else "close")
                if identity in seen:
                    report["duplicate_trade_actions"].append(
                        {"trade_id": trade_id, "action": identity[1], "order_id": row["id"]}
                    )
                seen.add(identity)
                keys = (
                    [f"open:{trade_id}"]
                    if action == "open"
                    else [f"close:{trade_id}", f"external_close:{trade_id}"]
                )
                if (
                    "strategy_pnl_events" not in tables
                    or not db.execute(
                        "SELECT 1 FROM strategy_pnl_events WHERE event_key IN ("
                        + ",".join("?" for _ in keys)
                        + ")",
                        keys,
                    ).fetchone()
                ):
                    report["missing_pnl_events"].append(row["id"])
        if {"funding_fees", "strategy_pnl_events"} <= tables:
            for fee in db.execute("SELECT * FROM funding_fees"):
                event = db.execute(
                    "SELECT amount_sats FROM strategy_pnl_events WHERE event_key=?",
                    (f"funding:{fee['trade_id']}:{fee['settlement_id']}",),
                ).fetchone()
                if event is None or event[0] != -fee["fee_sats"]:
                    report["funding_attribution_mismatches"].append(fee["id"])
        if {"daily_pnl", "strategy_pnl_events"} <= tables:
            events = dict(
                db.execute(
                    "SELECT date(ts),sum(amount_sats) FROM strategy_pnl_events GROUP BY date(ts)"
                ).fetchall()
            )
            daily = dict(
                db.execute(
                    "SELECT date,sum(realized_pnl_sats+funding_pnl_sats) FROM daily_pnl GROUP BY date"
                ).fetchall()
            )
            for date in sorted(events.keys() | daily.keys()):
                if events.get(date, 0) != daily.get(date, 0):
                    report["daily_total_mismatches"].append(
                        {
                            "date": date,
                            "strategy_events_sats": events.get(date, 0),
                            "daily_total_sats": daily.get(date, 0),
                        }
                    )
        if "execution_commands" in tables:
            report["undelivered_external_events"] = [
                row[0]
                for row in db.execute(
                    "SELECT command_key FROM execution_commands WHERE status='applied' AND notified=0"
                )
            ]
            report["unresolved_commands"] = [
                dict(row)
                for row in db.execute(
                    "SELECT command_key, action, status FROM execution_commands WHERE status IN ('submitted','received')"
                )
            ]
        if "strategy_state_snapshots" in tables:
            snapshots = db.execute(
                "SELECT strategy_name,state_json FROM strategy_state_snapshots WHERE mode='live'"
            ).fetchall()
            names = {row["strategy_name"] for row in snapshots}
            aliases = {
                "lnmarkets_bot.strategy.ma_cross.MaCross": "ma_cross_primary",
                "lnmarkets_bot.strategy.close_range_live.CloseRangeLive": "btc_close_range_v1",
            }
            for row in snapshots:
                if aliases.get(row["strategy_name"]) in names:
                    report["superseded_legacy_snapshots"].append(row["strategy_name"])
                    continue
                state = json.loads(row["state_json"])
                machine = state.get("machine", {})
                campaign = machine.get("campaign") or {}
                complete = machine.get(
                    "historical_model_complete", campaign.get("origin") != "historical"
                )
                if not complete or not machine.get("historical_funding_available", complete):
                    report["historical_reconstruction_required"].append(row["strategy_name"])
        report["local_consistency_pass"] = report["integrity"] == "ok" and not any(
            report[k]
            for k in [
                "missing_tables",
                "missing_fills",
                "duplicate_trade_actions",
                "funding_attribution_mismatches",
                "daily_total_mismatches",
                "undelivered_external_events",
                "missing_pnl_events",
                "unresolved_commands",
                "historical_reconstruction_required",
            ]
        )
        report["deployment_approved"] = False
        report["limitations"] = [
            "No independent venue ownership or funding reconciliation.",
            "Legacy totals may need evidence-based repair; no repair performed.",
        ]
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    args = parser.parse_args()
    report = inspect_database(args.db)
    print(json.dumps(report, indent=2))
    return 0 if report["local_consistency_pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
