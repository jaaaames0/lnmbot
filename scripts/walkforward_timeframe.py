#!/usr/bin/env python3
"""Four-year, fixed-rule walk-forward validation for one MA-cross timeframe.

The candidate grid below is deliberately small and fixed before running.  It
selects one rule on 2022-07-11..2024-07-10, then evaluates that *unchanged*
rule in the next two annual windows.  This is research only: it does not alter
the live configuration.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

from evaluate_timeframe import evaluate

from lnmarkets_bot.metrics import Trade, per_tf_summary

WINDOWS = (
    ("2022-23", datetime(2022, 7, 11, tzinfo=UTC), datetime(2023, 7, 11, tzinfo=UTC)),
    ("2023-24", datetime(2023, 7, 11, tzinfo=UTC), datetime(2024, 7, 11, tzinfo=UTC)),
    ("2024-25", datetime(2024, 7, 11, tzinfo=UTC), datetime(2025, 7, 11, tzinfo=UTC)),
    ("2025-26", datetime(2025, 7, 11, tzinfo=UTC), datetime(2026, 7, 11, tzinfo=UTC)),
)

TOLERANCES = (0.003, 0.004, 0.005, 0.006, 0.007)
WINNER_OPTIONS = {
    "1d": ((0.0, 0), (0.02, 8), (0.03, 8), (0.03, 12), (0.03, 16), (0.05, 12)),
    "4h": ((0.0, 0), (0.03, 8), (0.05, 8), (0.05, 11), (0.05, 14), (0.08, 11)),
}
LOSS_OPTIONS = {
    "1d": ((0.0, 0), (0.03, 2), (0.05, 3), (0.08, 3)),
    "4h": ((0.0, 0), (0.01, 3), (0.02, 4), (0.03, 4)),
}
LOCKED = {
    "1d": (0.005, 0.03, 12, 0.05, 3),
    "4h": (0.005, 0.05, 11, 0.02, 4),
}


def _window_summary(
    trades: list[Trade], timeframe: str, start: datetime, end: datetime
) -> dict[str, Any]:
    selected = [trade for trade in trades if start <= trade.exit_ts < end]
    return per_tf_summary({timeframe: selected}, notional_per_trade_usd=1000.0)["by_tf"].get(
        timeframe, {}
    )


def _pnl(summary: dict[str, Any]) -> float:
    return float(summary.get("total_pnl_usd", 0.0))


async def _candidate(
    *,
    data: Path,
    timeframe: str,
    tolerance: float,
    winner: tuple[float, int],
    loss: tuple[float, int],
    include_holdout: bool,
) -> dict[str, Any]:
    result = await evaluate(
        data=data,
        timeframe=timeframe,
        input_timeframe=timeframe,
        tolerance=tolerance,
        winner_threshold=winner[0],
        winner_count=winner[1],
        loss_threshold=loss[0],
        loss_count=loss[1],
        notional_usd=1000.0,
        leverage=2.0,
    )
    trades: list[Trade] = result.pop("_trades")
    visible_windows = WINDOWS if include_holdout else WINDOWS[:2]
    annual = {
        name: _window_summary(trades, timeframe, start, end) for name, start, end in visible_windows
    }
    train_pnls = [_pnl(annual[name]) for name, _, _ in WINDOWS[:2]]
    return {
        "tolerance_pct": tolerance,
        "winner_cooldown": {"threshold_pct": winner[0], "count": winner[1]},
        "loss_cooldown": {"threshold_pct": loss[0], "count": loss[1]},
        "annual": annual,
        "train_pnl_usd": sum(train_pnls),
        "train_median_annual_pnl_usd": median(train_pnls),
        **(
            {
                "holdout_pnl_usd": sum(_pnl(annual[name]) for name, _, _ in WINDOWS[2:]),
                "whole_period_pnl_usd": _pnl(result["summary"]),
                "costs": result["costs"],
            }
            if include_holdout
            else {}
        ),
    }


def _rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    # Selection is intentionally based only on the first two windows.  The
    # median makes a one-year spike less decisive than a repeatable result.
    return (
        float(row["train_median_annual_pnl_usd"]),
        float(row["train_pnl_usd"]),
        -float(row["tolerance_pct"]),
    )


async def validate(data: Path, timeframe: str) -> dict[str, Any]:
    candidate_specs = [
        (tolerance, winner, loss)
        for tolerance in TOLERANCES
        for winner in WINNER_OPTIONS[timeframe]
        for loss in LOSS_OPTIONS[timeframe]
    ]
    candidates = []
    for number, (tolerance, winner, loss) in enumerate(candidate_specs, start=1):
        print(
            f"[{number}/{len(candidate_specs)}] {timeframe} tol={tolerance:.3%} "
            f"winner={winner[0]:.1%}/{winner[1]} loss={loss[0]:.1%}/{loss[1]}",
            flush=True,
        )
        candidates.append(
            await _candidate(
                data=data,
                timeframe=timeframe,
                tolerance=tolerance,
                winner=winner,
                loss=loss,
                include_holdout=False,
            )
        )
    candidates.sort(key=_rank_key, reverse=True)
    (
        locked_tolerance,
        locked_winner_threshold,
        locked_winner_count,
        locked_loss_threshold,
        locked_loss_count,
    ) = LOCKED[timeframe]
    locked = await _candidate(
        data=data,
        timeframe=timeframe,
        tolerance=locked_tolerance,
        winner=(locked_winner_threshold, locked_winner_count),
        loss=(locked_loss_threshold, locked_loss_count),
        include_holdout=True,
    )
    selected_train = candidates[0]
    selected = await _candidate(
        data=data,
        timeframe=timeframe,
        tolerance=float(selected_train["tolerance_pct"]),
        winner=(
            float(selected_train["winner_cooldown"]["threshold_pct"]),
            int(selected_train["winner_cooldown"]["count"]),
        ),
        loss=(
            float(selected_train["loss_cooldown"]["threshold_pct"]),
            int(selected_train["loss_cooldown"]["count"]),
        ),
        include_holdout=True,
    )
    return {
        "protocol": {
            "selection_windows": [name for name, _, _ in WINDOWS[:2]],
            "untouched_holdout_windows": [name for name, _, _ in WINDOWS[2:]],
            "selection": "highest median annual net P&L in 2022-24; ties by total training P&L; the 2024-26 windows are not read until selection",
            "fees_slippage": "10 bps per fill fee and 5 bps per fill slippage",
            "funding": "not included; reported as holding-time sensitivity only",
            "candidate_grid": {
                "tolerances": TOLERANCES,
                "winner_cooldowns": WINNER_OPTIONS[timeframe],
                "loss_cooldowns": LOSS_OPTIONS[timeframe],
            },
        },
        "locked_live_rule": locked,
        "candidates_ranked_on_2022_24_only": candidates,
        "selected": selected,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeframe", choices=("1d", "4h"), required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(validate(args.data, args.timeframe))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    selected = report["selected"]
    print(
        f"selected {args.timeframe}: train=${selected['train_pnl_usd']:+.2f}; "
        f"holdout=${selected['holdout_pnl_usd']:+.2f}; wrote {args.json_out}"
    )


if __name__ == "__main__":
    main()
