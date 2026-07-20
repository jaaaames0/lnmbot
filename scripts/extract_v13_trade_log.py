"""Render the v1.3 backtest's strategy-signal audit as Markdown.

Unlike the old trade log, this includes every emitted intent: entries, exits,
neutral verdict changes, and cool-off suppressions. Each row is linked to the
order produced by that intent (if any), so it can be reconciled directly with
a chart.
"""
# ruff: noqa: I001
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders, runs, signals


DEFAULT_DB = Path("runs/v13_verify.sqlite")
DEFAULT_OUTPUT = Path("runs/v13_trades_2y.md")
TIMEFRAMES = ("1d", "4h")


def _metadata(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _action(row) -> str:
    metadata = _metadata(row.metadata_json)
    if row.kind == "noop":
        if row.reason == "cool_off":
            if "suppressed_remaining_before" in metadata:
                before = metadata["suppressed_remaining_before"]
                after = metadata.get("suppressed_remaining_after", "?")
                return f"SUPPRESSED (cool-off slot {before} → {after})"
            slots = []
            for cooldown_type in metadata.get("cooldown_types", []):
                before = metadata.get(f"{cooldown_type}_remaining_before", "?")
                after = metadata.get(f"{cooldown_type}_remaining_after", "?")
                slots.append(f"{cooldown_type} {before} → {after}")
            return f"SUPPRESSED ({', '.join(slots) or 'cool-off'})"
        return "NO ACTION (neutral verdict)"

    if row.order_id is None:
        return "NO ORDER (risk/executor no-op)"

    price = f" @ ${row.price_usd:,.0f}" if row.price_usd is not None else ""
    return f"ORDER #{row.order_id}: {row.order_side.upper()} {row.qty_sats:,} sats{price}"


def _cooldown_note(metadata: dict[str, Any]) -> str:
    if not metadata.get("cool_off_started"):
        return ""
    pnl = float(metadata.get("trade_pnl_pct", 0.0)) * 100
    types = metadata.get("cooldown_types", [])
    label = ", ".join(types) if types else "winner"
    return f"; cool-off STARTED ({label}, {pnl:+.2f}%)"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    engine = make_engine(args.db)
    init_schema(engine)
    factory = make_session_factory(engine)
    with factory() as session:
        run_id = session.execute(
            select(runs.c.id).order_by(runs.c.id.desc()).limit(1)
        ).scalar_one()
        rows = session.execute(
            select(
                signals.c.id.label("signal_id"), signals.c.ts, signals.c.kind,
                signals.c.side, signals.c.reason, signals.c.metadata_json,
                orders.c.id.label("order_id"), orders.c.side.label("order_side"),
                orders.c.qty_sats, orders.c.price_usd,
            )
            .outerjoin(orders, orders.c.signal_id == signals.c.id)
            .where(signals.c.run_id == run_id)
            .order_by(signals.c.ts, signals.c.id)
        ).all()

    per_tf: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        tf = str(_metadata(row.metadata_json).get("trigger_tf", "unknown"))
        per_tf[tf].append(row)

    out = [
        "# v1.3 2y backtest — strategy signal audit",
        "",
        "Every emitted strategy intent is listed below in chronological order,",
        "separately for each timeframe. `SUPPRESSED` rows are cool-off transitions",
        "that deliberately did not place an order. A `FLAT` verdict also appears so",
        "you can see when it consumed a cool-off slot.",
        "",
        f"Source: `{args.db}`; run ID: `{run_id}`.",
        "",
        "## Summary",
        "",
        "| Timeframe | Signals | Orders | Cool-off suppressions | Neutral verdicts |",
        "|---|---:|---:|---:|---:|",
    ]
    for tf in TIMEFRAMES:
        tf_rows = per_tf[tf]
        out.append(
            f"| {tf} | {len(tf_rows)} | "
            f"{sum(row.order_id is not None for row in tf_rows)} | "
            f"{sum(row.reason == 'cool_off' for row in tf_rows)} | "
            f"{sum(row.reason == 'verdict_flat' for row in tf_rows)} |"
        )
    if per_tf.get("unknown"):
        raise RuntimeError(
            "signals without metadata_json.trigger_tf found; regenerate the backtest "
            "with the current engine before rendering this audit"
        )

    for tf in TIMEFRAMES:
        out.extend([
            "",
            f"## {tf} signals (chronological)",
            "",
            "| # | UTC | Transition | Intent | Action | Reason |",
            "|---:|:---|:---|:---|:---|:---|",
        ])
        for index, row in enumerate(per_tf[tf], 1):
            metadata = _metadata(row.metadata_json)
            transition = (
                f"{metadata.get('previous_verdict', '?')} → "
                f"{metadata.get('verdict', row.side or '?')}"
            )
            reason = f"{row.reason}{_cooldown_note(metadata)}"
            out.append(
                f"| {index} | {row.ts.strftime('%Y-%m-%d %H:%M')} | {transition} | "
                f"{row.kind.upper()} | {_action(row)} | {reason} |"
            )

    args.output.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
