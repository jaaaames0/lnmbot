"""Build a reviewed historical machine artifact from local immutable evidence.

Does not access the exchange or update any database. Output is a new JSON
artifact; a live snapshot migration must be reviewed separately.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lnmarkets_bot.strategy.close_range_live import load_seed_machine
from lnmarkets_bot.strategy.historical import hydrate_historical


def rebuild(daily: Path, seed: Path, reference: Path, funding: Path) -> dict:
    ref = json.loads(reference.read_text())
    if (
        hashlib.sha256(daily.read_bytes()).hexdigest() != ref["candles_sha256"]
        or hashlib.sha256(seed.read_bytes()).hexdigest() != ref["seed_sha256"]
    ):
        raise ValueError("reference source hashes differ")
    machine = load_seed_machine(daily, seed)
    frame = pd.read_parquet(funding)
    rows = [
        (pd.Timestamp(row.ts).to_pydatetime(), float(row.fundingRate), float(row.fixingPrice))
        for row in frame.itertuples(index=False)
    ]
    hydrate_historical(machine, ref, rows)
    return {
        "mode": "historical_only_no_orders",
        "source_hashes": {
            "daily": ref["candles_sha256"],
            "seed": ref["seed_sha256"],
            "reference": hashlib.sha256(reference.read_bytes()).hexdigest(),
            "funding": hashlib.sha256(funding.read_bytes()).hexdigest(),
        },
        "machine": machine.persistent_state(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--funding", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(Path("/var/lib")) or args.output.exists():
        parser.error("output must be a new non-production artifact")
    result = rebuild(args.daily, args.seed, args.reference, args.funding)
    args.output.write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
