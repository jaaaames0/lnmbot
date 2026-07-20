"""Select a 1h tolerance on training data, then reveal its holdout result.

This deliberately ranks candidates using only trades closed before 2025-07-11.
The selected candidate's second-year metrics are reported once, after the
selection. It is the first gate before testing winner/loss cool-offs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from evaluate_timeframe import evaluate

TOLERANCES = (0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010)


async def select(data: Path, tolerances: tuple[float, ...]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    results: dict[float, dict[str, Any]] = {}
    for tolerance in tolerances:
        print(f"[train] tolerance={tolerance:.3%}", flush=True)
        result = await evaluate(
            data=data,
            timeframe="1h",
            tolerance=tolerance,
            winner_threshold=0.0,
            winner_count=0,
            loss_threshold=0.0,
            loss_count=0,
            notional_usd=1000.0,
            leverage=2.0,
        )
        results[tolerance] = result
        train = result["train_summary"]
        candidates.append(
            {
                "tolerance_pct": tolerance,
                "train_n_trades": train.get("n_trades", 0),
                "train_pnl_usd": train.get("total_pnl_usd", 0.0),
                "train_win_rate": train.get("win_rate", 0.0),
            }
        )

    candidates.sort(key=lambda row: float(row["train_pnl_usd"]), reverse=True)
    selected = candidates[0]
    result = results[float(selected["tolerance_pct"])]
    return {
        "protocol": {
            "train": "2024-07-11 through 2025-07-10",
            "holdout": "2025-07-11 through 2026-07-11",
            "selection": "highest training net P&L after 10 bps per-fill fee and 5 bps per-fill slippage",
            "cooldowns": "disabled for this tolerance-selection stage",
            "funding": "not included; candle fixture has no historical funding series",
            "candidates": tolerances,
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
        "--json-out", type=Path, default=Path("runs/1h_tolerance_selection_2y.json")
    )
    args = parser.parse_args()
    report = asyncio.run(select(args.data, TOLERANCES))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    selected = report["selected"]
    print(
        f"selected tolerance={selected['tolerance_pct']:.3%}: "
        f"train=${selected['train_pnl_usd']:+.2f}; "
        f"holdout=${selected['holdout_summary'].get('total_pnl_usd', 0.0):+.2f}"
    )
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
