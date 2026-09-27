#!/usr/bin/env python3
"""Refresh completed Binance daily candles and atomically advance shadow state."""

from __future__ import annotations

import argparse
import fcntl
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_breakout_forward_shadow import advance_forward_shadow  # noqa: E402

from lnmarkets_bot.data.binance_cache import refresh_completed_cache  # noqa: E402
from lnmarkets_bot.portfolio.store import timestamp  # noqa: E402


def run_job(
    *,
    snapshot_path: Path,
    cache_path: Path,
    database_path: Path,
    lock_path: Path,
    bootstrap_start: datetime,
    now: datetime,
    allow_production_path: bool = False,
) -> dict[str, object]:
    """Run one serialized refresh/advance transaction.

    Market-cache replacement completes before the database advances. A failure
    in either stage leaves the preceding strategy state usable on the next run.
    """
    if allow_production_path:
        if not (
            cache_path.resolve().is_relative_to(Path("/var/lib/lnmbot-shadow"))
            and database_path.resolve().is_relative_to(Path("/var/lib/lnmbot-shadow"))
            and lock_path.resolve().is_relative_to(Path("/run/lnmbot-shadow"))
        ):
            raise ValueError("installed shadow paths must stay inside dedicated state directories")
    elif any(
        path.resolve().is_relative_to(Path("/var/lib"))
        for path in (cache_path, database_path, lock_path)
    ):
        raise ValueError("development job cannot write production state")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {
                "mode": "forward_shadow_no_orders",
                "order_capability": False,
                "skipped": "locked",
            }
        daily = refresh_completed_cache(
            cache_path=cache_path,
            symbol="BTCUSDT",
            interval="1d",
            bootstrap_start=timestamp(bootstrap_start),
            now=timestamp(now),
        )
        summary = advance_forward_shadow(
            snapshot_path=snapshot_path,
            daily_path=cache_path,
            database_path=database_path,
            now=timestamp(now),
            forbid_production_path=not allow_production_path,
        )
        summary["completed_daily_rows"] = len(daily)
        summary["cache_path"] = str(cache_path)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--bootstrap-start", default="2019-09-09T00:00:00+00:00")
    parser.add_argument("--now", help="UTC test clock; defaults to current time")
    parser.add_argument(
        "--installed-order-incapable-service",
        action="store_true",
        help="Permit /var/lib paths; this never enables exchange/order code",
    )
    args = parser.parse_args()
    try:
        summary = run_job(
            snapshot_path=args.snapshot,
            cache_path=args.cache,
            database_path=args.database,
            lock_path=args.lock,
            bootstrap_start=timestamp(args.bootstrap_start),
            now=timestamp(args.now) if args.now else datetime.now(UTC),
            allow_production_path=args.installed_order_incapable_service,
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
