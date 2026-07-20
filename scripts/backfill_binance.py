#!/usr/bin/env python3
"""Fetch a window of BTCUSDT 1m klines from Binance and cache to parquet.

Usage:
    python scripts/backfill_binance.py --start 2026-01-01 --end 2026-02-01 \
        --output data/cache/btcusdt_perp_1m.parquet
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from lnmarkets_bot.config import load_config
from lnmarkets_bot.data.binance import fetch_klines
from lnmarkets_bot.logging import configure_logging, get_logger


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--start", required=True, help="ISO 8601 UTC, e.g. 2026-01-01T00:00:00Z")
    parser.add_argument("--end", required=True, help="ISO 8601 UTC")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    cfg = load_config()
    configure_logging(args.log_level, cfg.storage_log_path)
    log = get_logger("backfill_binance")

    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(UTC)
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00")).astimezone(UTC)

    log.info(
        "backfill.start",
        symbol=args.symbol,
        interval=args.interval,
        start=start.isoformat(),
        end=end.isoformat(),
        output=str(args.output),
    )
    df = fetch_klines(
        symbol=args.symbol,
        interval=args.interval,
        start=start,
        end=end,
        cache_path=args.output,
        on_progress=lambda rows, through: print(
            f"backfill progress: {rows:,} rows through {through.isoformat()}", flush=True
        ),
    )
    log.info("backfill.done", rows=len(df), output=str(args.output))


if __name__ == "__main__":
    main()
