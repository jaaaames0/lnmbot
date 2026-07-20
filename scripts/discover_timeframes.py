#!/usr/bin/env python3
"""Frozen non-standard timeframe discovery batch; research only.

Protocol: docs/timeframe-discovery-preregistration.md
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import TYPE_CHECKING, Any

from evaluate_timeframe import _resampled_bars, evaluate

if TYPE_CHECKING:
    from lnmarkets_bot.metrics import Trade

NOTIONAL = 1000.0
FUNDING_RATE_8H = 0.0001
SELECTION_END = datetime(2024, 7, 11, tzinfo=UTC)
WINDOWS = (
    ("2022-23", datetime(2022, 7, 11, tzinfo=UTC), datetime(2023, 7, 11, tzinfo=UTC)),
    ("2023-24", datetime(2023, 7, 11, tzinfo=UTC), SELECTION_END),
    ("2024-25", SELECTION_END, datetime(2025, 7, 11, tzinfo=UTC)),
    ("2025-26", datetime(2025, 7, 11, tzinfo=UTC), datetime(2026, 7, 11, tzinfo=UTC)),
)
TIMEFRAMES = ("1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w")
TOLERANCES = (0.002, 0.003, 0.004, 0.005, 0.006, 0.008)
STANDARD_WINNERS = (
    (0.0, 0),
    (0.02, 4),
    (0.03, 8),
    (0.03, 12),
    (0.05, 8),
    (0.05, 12),
    (0.08, 12),
)
STANDARD_LOSSES = ((0.0, 0), (0.02, 3), (0.03, 3), (0.05, 3))
WEEKLY_WINNERS = ((0.0, 0), (0.05, 2), (0.10, 2), (0.10, 4))
WEEKLY_LOSSES = ((0.0, 0), (0.05, 1), (0.10, 1), (0.10, 2))


def _window(trades: list[Trade], start: datetime, end: datetime) -> dict[str, float | int]:
    selected = [trade for trade in trades if start <= trade.exit_ts < end]
    pnl = sum(trade.pnl_usd for trade in selected)
    funding = sum(
        NOTIONAL * (trade.exit_ts - trade.entry_ts).total_seconds() / 3600 / 8 * FUNDING_RATE_8H
        for trade in selected
    )
    return {
        "trades": len(selected),
        "trading_cost_net_usd": pnl,
        "funding_stress_usd": funding,
        "stressed_pnl_usd": pnl - funding,
    }


async def _run(
    *,
    data: Path,
    input_timeframe: str,
    timeframe: str,
    tolerance: float,
    winner: tuple[float, int],
    loss: tuple[float, int],
    end: datetime | None,
    bars: list[Any],
) -> list[Trade]:
    result = await evaluate(
        data=data,
        timeframe=timeframe,
        input_timeframe=input_timeframe,
        tolerance=tolerance,
        winner_threshold=winner[0],
        winner_count=winner[1],
        loss_threshold=loss[0],
        loss_count=loss[1],
        notional_usd=NOTIONAL,
        leverage=5.0,
        end=end,
        bars=bars,
    )
    return result["_trades"]


def _rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        float(row["train_median_annual_stressed_pnl_usd"]),
        float(row["train_stressed_pnl_usd"]),
        -float(row["tolerance_pct"]),
    )


async def _discover_one(*, data: Path, input_timeframe: str, timeframe: str) -> dict[str, Any]:
    winners = WEEKLY_WINNERS if timeframe == "1w" else STANDARD_WINNERS
    losses = WEEKLY_LOSSES if timeframe == "1w" else STANDARD_LOSSES
    specs = [(tol, winner, loss) for tol in TOLERANCES for winner in winners for loss in losses]
    bars = _resampled_bars(data, timeframe, input_timeframe=input_timeframe)
    candidates: list[dict[str, Any]] = []
    for number, (tolerance, winner, loss) in enumerate(specs, start=1):
        if number == 1 or number % 24 == 0 or number == len(specs):
            print(f"[{timeframe} {number}/{len(specs)}]", flush=True)
        trades = await _run(
            data=data,
            input_timeframe=input_timeframe,
            timeframe=timeframe,
            tolerance=tolerance,
            winner=winner,
            loss=loss,
            end=SELECTION_END,
            bars=bars,
        )
        train = [_window(trades, start, end) for _, start, end in WINDOWS[:2]]
        pnls = [float(row["stressed_pnl_usd"]) for row in train]
        candidates.append(
            {
                "tolerance_pct": tolerance,
                "winner_cooldown": {"threshold_pct": winner[0], "count": winner[1]},
                "loss_cooldown": {"threshold_pct": loss[0], "count": loss[1]},
                "train_annual": dict(zip((name for name, *_ in WINDOWS[:2]), train, strict=True)),
                "train_stressed_pnl_usd": sum(pnls),
                "train_median_annual_stressed_pnl_usd": median(pnls),
            }
        )
    candidates.sort(key=_rank_key, reverse=True)
    selected = candidates[0]
    winner = selected["winner_cooldown"]
    loss = selected["loss_cooldown"]
    selected_trades = await _run(
        data=data,
        input_timeframe=input_timeframe,
        timeframe=timeframe,
        tolerance=float(selected["tolerance_pct"]),
        winner=(float(winner["threshold_pct"]), int(winner["count"])),
        loss=(float(loss["threshold_pct"]), int(loss["count"])),
        end=None,
        bars=bars,
    )
    annual = {name: _window(selected_trades, start, end) for name, start, end in WINDOWS}
    stressed = [float(row["stressed_pnl_usd"]) for row in annual.values()]
    validation = stressed[2:]
    gate = {
        "both_validation_years_positive": all(value > 0 for value in validation),
        "at_least_three_positive_years": sum(value > 0 for value in stressed) >= 3,
        "aggregate_positive": sum(stressed) > 0,
        "at_least_12_closed_trades": len(selected_trades) >= 12,
    }
    return {
        "selected": {
            **selected,
            "annual": annual,
            "aggregate_stressed_pnl_usd": sum(stressed),
            "closed_trades": len(selected_trades),
        },
        "viability_gate": gate,
        "verdict": "CANDIDATE_FOR_FURTHER_RESEARCH" if all(gate.values()) else "FAIL",
        "top_training_candidates": candidates[:10],
    }


async def discover(data_1h: Path, data_1d: Path) -> dict[str, Any]:
    report: dict[str, Any] = {
        "protocol": "docs/timeframe-discovery-preregistration.md",
        "selection_windows": [name for name, *_ in WINDOWS[:2]],
        "unchanged_validation_windows": [name for name, *_ in WINDOWS[2:]],
        "results": {},
    }
    for timeframe in TIMEFRAMES:
        data, input_timeframe = (data_1d, "1d") if timeframe in {"1d", "1w"} else (data_1h, "1h")
        report["results"][timeframe] = await _discover_one(
            data=data, input_timeframe=input_timeframe, timeframe=timeframe
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-1h", type=Path, required=True)
    parser.add_argument("--data-1d", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(discover(args.data_1h, args.data_1d))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    for timeframe, result in report["results"].items():
        selected = result["selected"]
        print(
            f"{timeframe}: {result['verdict']} "
            f"train=${selected['train_stressed_pnl_usd']:+.2f} "
            f"validation=${sum(row['stressed_pnl_usd'] for name, row in selected['annual'].items() if name in {'2024-25', '2025-26'}):+.2f}"
        )
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
