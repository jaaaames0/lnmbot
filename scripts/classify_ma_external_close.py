"""Review/apply an operator-confirmed MA external closure, with the trader stopped.

No venue request is made. REST closure history alone does not establish cause.
Default is a dry run. Use a copied database first; never classify by price alone.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lnmarkets_bot.strategy.base import StrategyState
from lnmarkets_bot.strategy.ma_cross import MaCross


def classify(db_path: Path, timeframe: str, trade_id: str, cause: str, *, apply=False, evidence=""):
    if cause not in {"manual", "liquidation"}:
        raise ValueError("classification must be manual or liquidation")
    if apply and not evidence.strip():
        raise ValueError("an operator evidence note is required for application")
    path = db_path.resolve()
    if apply and path.is_relative_to("/var/lib"):
        if path != Path("/var/lib/lnmbot/lnmarkets.sqlite"):
            raise ValueError("unexpected production database")
        active = subprocess.check_output(
            ["systemctl", "show", "lnmbot.service", "-p", "ActiveState", "-p", "MainPID"],
            text=True,
        )
        if "ActiveState=inactive" not in active or "MainPID=0" not in active:
            raise ValueError("stop the trader before applying a classification")
    db = sqlite3.connect(path.as_uri() + ("?mode=rw" if apply else "?mode=ro"), uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        snapshot = db.execute(
            "SELECT * FROM strategy_state_snapshots WHERE mode='live' AND strategy_name='ma_cross_primary'"
        ).fetchone()
        if snapshot is None:
            raise ValueError("no stable MA instance snapshot")
        state = json.loads(snapshot["state_json"])
        closure = state.get("last_external_closures", {}).get(timeframe)
        if closure is None or closure["trade_id"] != trade_id or closure["liquidated"] is not None:
            raise ValueError("trade/timeframe does not match the last unknown closure")
        command = db.execute(
            "SELECT * FROM execution_commands WHERE command_key=?", (f"close:{trade_id}",)
        ).fetchone()
        if command is None or command["status"] != "applied" or not command["notified"]:
            raise ValueError("external close accounting/delivery is incomplete")
        result = json.loads(command["result_json"])
        event = result.get("external_event") or {}
        if event.get("strategy_instance_id") != "ma_cross_primary" or event.get("position_key") != timeframe:
            raise ValueError("execution result does not establish this trade's MA ownership")
        latest = db.execute(
            "SELECT lnm_order_id,metadata_json FROM orders WHERE strategy_instance_id='ma_cross_primary' AND position_key=? ORDER BY id DESC LIMIT 1",
            (timeframe,),
        ).fetchone()
        if latest is None or latest["lnm_order_id"] != trade_id or json.loads(latest["metadata_json"]).get("isolated_action") != "external_close":
            raise ValueError("a later trade exists on this timeframe; classification cannot reset it")
        if db.execute("SELECT 1 FROM execution_commands WHERE status IN ('submitted','received') LIMIT 1").fetchone():
            raise ValueError("resolve outstanding execution commands before classification")
        strategy = MaCross(state["strategy_params"])
        if not strategy.restore_persistent_state(state):
            raise ValueError("MA snapshot is incompatible")
        strategy.classify_external_close(
            SimpleNamespace(
                position_key=timeframe, trade_id=trade_id, liquidated=cause == "liquidation",
                price_usd=closure["price_usd"], entry_price_usd=closure.get("entry_price_usd"),
                side=closure.get("side"),
                observed_at=datetime.fromisoformat(closure["observed_at"]),
                net_pl_sats=closure.get("net_pl_sats"),
            ),
            StrategyState(),
        )
        after = strategy.persistent_state()
        summary = {
            "timeframe": timeframe, "trade_id": trade_id, "cause": cause,
            "loss_cooldown_remaining": after["loss_suppressed_signals"][timeframe],
            "manual_reset_pending": timeframe in after["external_reset_pending"],
            "applied": apply,
        }
        if apply:
            event["liquidated"] = cause == "liquidation"
            event["reason"] = "liquidation" if cause == "liquidation" else "external_close"
            result["external_event"] = event
            classification = {
                "cause": cause, "evidence": evidence, "at": datetime.now(UTC).isoformat(),
            }
            kind = "liquidation" if cause == "liquidation" else "external_close_net_pl"
            result["pnl"]["kind"] = kind
            for fact in (result["order"], result["pnl"]):
                fact.setdefault("metadata", {})["operator_closure_classification"] = classification
                fact["metadata"]["liquidated"] = cause == "liquidation"
            db.execute(
                "UPDATE strategy_state_snapshots SET state_json=?,ts=? WHERE mode='live' AND strategy_name='ma_cross_primary'",
                (json.dumps(after), datetime.now(UTC).replace(tzinfo=None).isoformat(" ")),
            )
            db.execute(
                "UPDATE execution_commands SET result_json=? WHERE command_key=?",
                (json.dumps(result), f"close:{trade_id}"),
            )
            # Classification changes lifecycle attribution, never money.
            db.execute(
                "UPDATE strategy_pnl_events SET kind=?,metadata_json=? WHERE event_key=?",
                (kind, json.dumps(result["pnl"]["metadata"]), result["pnl"]["event_key"]),
            )
            db.execute(
                "UPDATE orders SET metadata_json=? WHERE lnm_order_id=? AND json_extract(metadata_json,'$.isolated_action')='external_close'",
                (json.dumps(result["order"]["metadata"]), trade_id),
            )
            db.commit()
        else:
            db.rollback()
        return summary
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--timeframe", required=True, choices=("1d", "4h"))
    parser.add_argument("--trade-id", required=True)
    parser.add_argument("--cause", required=True, choices=("manual", "liquidation"))
    parser.add_argument("--evidence-note", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(classify(args.db, args.timeframe, args.trade_id, args.cause,
        apply=args.apply, evidence=args.evidence_note), indent=2))


if __name__ == "__main__":
    main()
