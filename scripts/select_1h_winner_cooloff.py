"""Training-only joint selection of 1h tolerance and winner cool-off.

The candidate grid is fixed in this file. Candidates are ranked exclusively
on the first year; only the winner's second-year result is written to the
report. Loss cool-off remains disabled in this stage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from evaluate_timeframe import evaluate

TOLERANCES = (0.004, 0.005, 0.007, 0.009)
WINNER_COOLOFFS = (
    (0.0, 0),
    (0.02, 2),
    (0.02, 4),
    (0.03, 4),
    (0.03, 8),
    (0.05, 4),
    (0.05, 8),
    (0.05, 11),
)


async def select(data: Path) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    results: dict[tuple[float, float, int], dict[str, Any]] = {}
    total = len(TOLERANCES) * len(WINNER_COOLOFFS)
    number = 0
    for tolerance in TOLERANCES:
        for threshold, count in WINNER_COOLOFFS:
            number += 1
            print(
                f"[train {number}/{total}] tolerance={tolerance:.3%} winner={threshold:.1%}/{count}",
                flush=True,
            )
            result = await evaluate(
                data=data,
                timeframe="1h",
                tolerance=tolerance,
                winner_threshold=threshold,
                winner_count=count,
                loss_threshold=0.0,
                loss_count=0,
                notional_usd=1000.0,
                leverage=2.0,
            )
            key = (tolerance, threshold, count)
            results[key] = result
            train = result["train_summary"]
            candidates.append(
                {
                    "tolerance_pct": tolerance,
                    "winner_threshold_pct": threshold,
                    "winner_signal_count": count,
                    "train_pnl_usd": train.get("total_pnl_usd", 0.0),
                    "train_n_trades": train.get("n_trades", 0),
                }
            )
    candidates.sort(key=lambda row: float(row["train_pnl_usd"]), reverse=True)
    selected = candidates[0]
    result = results[
        (
            float(selected["tolerance_pct"]),
            float(selected["winner_threshold_pct"]),
            int(selected["winner_signal_count"]),
        )
    ]
    return {
        "protocol": {
            "train": "2024-07-11 through 2025-07-10",
            "holdout": "2025-07-11 through 2026-07-11",
            "selection": "highest training net P&L after 10 bps per-fill fee and 5 bps per-fill slippage",
            "loss_cooldown": "disabled",
            "funding": "not included; fixture contains candles only",
            "tolerances": TOLERANCES,
            "winner_cooloffs": WINNER_COOLOFFS,
        },
        "candidates_training_only": candidates,
        "selected": {
            **selected,
            "whole_period_summary": result["summary"],
            "holdout_summary": result["holdout_summary"],
            "costs": result["costs"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument(
        "--json-out", type=Path, default=Path("runs/1h_winner_cooloff_selection_2y.json")
    )
    args = parser.parse_args()
    report = asyncio.run(select(args.data))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    selected = report["selected"]
    print(
        f"selected tolerance={selected['tolerance_pct']:.3%} "
        f"winner={selected['winner_threshold_pct']:.1%}/{selected['winner_signal_count']}: "
        f"train=${selected['train_pnl_usd']:+.2f}; "
        f"holdout=${selected['holdout_summary'].get('total_pnl_usd', 0.0):+.2f}"
    )
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
