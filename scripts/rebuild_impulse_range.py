"""Dry-run range reconstruction / daily seed refresh from verified local candles.

Never modifies a live DB. Output is a reviewable snapshot with input hashes.
Only flat, command-resolved copied state is eligible for a replacement model.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from lnmarkets_bot.data.multitimeframe import MultiTimeframeDataSource
from lnmarkets_bot.data.source import DataSource
from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.impulse_range import ImpulseRangeParams
from lnmarkets_bot.strategy.impulse_range_live import ImpulseRangeLive, load_cold_machine


class _Minutes(DataSource):
    def __init__(self, rows):
        self.rows = rows

    async def stream(self):
        for row in self.rows.itertuples(index=False):
            yield Bar(
                row.ts.to_pydatetime(),
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                warmup=True,
            )


async def reconstruct(daily: Path, minutes: Path, params: dict, copied_db: Path | None = None):
    if copied_db is not None:
        if copied_db.resolve() == Path("/var/lib/lnmbot/lnmarkets.sqlite"):
            raise ValueError("use an operator-created DB copy")
        with sqlite3.connect(copied_db.resolve().as_uri() + "?mode=ro", uri=True) as db:
            if db.execute(
                "SELECT COUNT(*) FROM execution_commands WHERE status IN ('submitted','received')"
            ).fetchone()[0]:
                raise ValueError("resolve commands before rebuilding")
            opened = db.execute(
                "SELECT strategy_instance_id, metadata_json FROM orders WHERE id IN (SELECT MAX(id) FROM orders WHERE lnm_order_id IS NOT NULL GROUP BY lnm_order_id)"
            ).fetchall()
            if any(
                owner == "btc_impulse_range_v1"
                and json.loads(meta or "{}").get("isolated_action") == "open"
                for owner, meta in opened
            ):
                raise ValueError("range must be authoritatively flat before reconstruction")
    rows = pd.read_parquet(minutes).sort_values("ts")
    rows["ts"] = pd.to_datetime(rows["ts"], utc=True)
    if rows.empty or rows.ts.duplicated().any():
        raise ValueError("empty or duplicate minute evidence")
    if not rows.ts.diff().iloc[1:].eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("minute evidence has gaps")
    start = rows.ts.iloc[0].to_pydatetime()
    end = rows.ts.iloc[-1].to_pydatetime() + timedelta(minutes=1)
    if start.hour or start.minute or end.hour or end.minute:
        raise ValueError("reconstruction must span whole UTC days")
    if end > datetime.now(UTC):
        raise ValueError("evidence includes an unclosed minute")
    machine = load_cold_machine(
        daily,
        ImpulseRangeParams(
            chop_filter=params.get("chop_filter", True),
            chop_threshold=params.get("chop_threshold", 0.22),
            direction_mode=params.get("direction_mode", "both"),
        ),
        through_day=start - timedelta(days=1),
    )
    strategy = ImpulseRangeLive(params | {"mode": "funded"}, machine=machine)
    state = StrategyState()
    async for bar in MultiTimeframeDataSource(
        _Minutes(rows), higher_timeframes=("1d", "4h")
    ).stream():
        strategy.on_bar(bar, state)
    if not strategy.model_complete:
        raise ValueError(strategy.incomplete_reason)
    return {
        "dry_run": True,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "inputs": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (daily, minutes)},
        "state": strategy.persistent_state(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--minutes", type=Path)
    parser.add_argument("--copied-db", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--refresh-seed", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    if args.refresh_seed:
        rows = pd.read_parquet(args.daily).sort_values("ts")
        rows["ts"] = pd.to_datetime(rows["ts"], utc=True)
        if (
            rows.empty
            or rows.ts.duplicated().any()
            or not rows.ts.diff().iloc[1:].eq(pd.Timedelta(days=1)).all()
        ):
            parser.error("daily seed must be nonempty, unique and contiguous")
        if rows.ts.iloc[-1] + pd.Timedelta(days=1) > pd.Timestamp.now(tz="UTC"):
            parser.error("daily seed includes an unclosed day")
        rows.to_parquet(args.output, index=False)
        print(
            json.dumps(
                {
                    "rows": len(rows),
                    "through": str(rows.ts.iloc[-1]),
                    "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                }
            )
        )
    else:
        if args.minutes is None:
            parser.error("--minutes required for reconstruction")
        report = asyncio.run(reconstruct(args.daily, args.minutes, {}, args.copied_db))
        args.output.write_text(json.dumps(report, indent=2))
        print("Dry-run snapshot written; production state was not modified.")


if __name__ == "__main__":
    main()
