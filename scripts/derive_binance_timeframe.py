#!/usr/bin/env python3
"""Derive UTC right-labelled Binance candles from a finer parquet cache."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--frequency", required=True, help="Pandas frequency, e.g. 1h")
    args = parser.parse_args()

    df = pd.read_parquet(args.input).sort_values("ts")
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize("UTC")
    else:
        df["ts"] = df["ts"].dt.tz_convert("UTC")
    last_source_ts = df["ts"].iloc[-1]
    derived = (
        df.set_index("ts")
        .resample(args.frequency, label="right", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
        .reset_index()
    )
    # Match the live aggregation rule: do not emit a final incomplete bucket.
    derived = derived[derived["ts"] <= last_source_ts]
    # ``resample`` labels at the close so incomplete buckets are easy to
    # exclude above. Binance parquet caches use candle *open* timestamps;
    # convert back to that convention before writing so direct replay shifts
    # the completed candle exactly once.
    derived["ts"] = derived["ts"] - pd.tseries.frequencies.to_offset(args.frequency)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    derived.to_parquet(args.output, index=False)
    print(f"wrote {args.output}: {len(derived):,} rows")


if __name__ == "__main__":
    main()
