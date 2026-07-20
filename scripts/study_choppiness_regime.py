#!/usr/bin/env python3
"""Read-only, walk-forward CHOP study for the locked MA/cool-off strategy."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import TYPE_CHECKING, Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from evaluate_timeframe import _resampled_bars, evaluate

if TYPE_CHECKING:
    from lnmarkets_bot.metrics import Trade

LOCKED = {
    "1d": {"tolerance": 0.005, "winner": (0.03, 12), "loss": (0.05, 3)},
    "4h": {"tolerance": 0.005, "winner": (0.05, 11), "loss": (0.02, 4)},
}
LOOKBACKS = (14, 30, 90)
BUCKETS = ("trend", "neutral", "chop")
OVERLAYS = {
    "baseline": {"trend": 1.0, "neutral": 1.0, "chop": 1.0},
    "reduce_chop": {"trend": 1.0, "neutral": 1.0, "chop": 0.5},
    "boost_trend": {"trend": 1.25, "neutral": 1.0, "chop": 1.0},
    "trend_and_reduce_chop": {"trend": 1.25, "neutral": 1.0, "chop": 0.5},
}
WINDOWS = (
    ("2022-23", datetime(2022, 7, 11, tzinfo=UTC), datetime(2023, 7, 11, tzinfo=UTC)),
    ("2023-24", datetime(2023, 7, 11, tzinfo=UTC), datetime(2024, 7, 11, tzinfo=UTC)),
    ("2024-25", datetime(2024, 7, 11, tzinfo=UTC), datetime(2025, 7, 11, tzinfo=UTC)),
    ("2025-26", datetime(2025, 7, 11, tzinfo=UTC), datetime(2026, 7, 11, tzinfo=UTC)),
)
FUNDING_RATE_8H = 0.0001
NOTIONAL_USD = 1000.0


@dataclass(frozen=True)
class AnnotatedTrade:
    trade: Trade
    bucket: str
    chop: float


def _chop_by_timestamp(bars, lookback: int) -> dict[datetime, float]:
    """CHOP using only the bar available at the strategy's decision time."""
    out: dict[datetime, float] = {}
    true_ranges: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    previous_close: float | None = None
    for bar in bars:
        tr = bar.high - bar.low
        if previous_close is not None:
            tr = max(tr, abs(bar.high - previous_close), abs(bar.low - previous_close))
        true_ranges.append(tr)
        highs.append(bar.high)
        lows.append(bar.low)
        previous_close = bar.close
        if len(true_ranges) < lookback:
            continue
        total_range = max(highs[-lookback:]) - min(lows[-lookback:])
        travelled = sum(true_ranges[-lookback:])
        if total_range > 0 and travelled > 0:
            out[bar.ts] = 100 * math.log10(travelled / total_range) / math.log10(lookback)
    return out


def _bucket(value: float) -> str:
    if value < 38.2:
        return "trend"
    if value > 61.8:
        return "chop"
    return "neutral"


def _annotate(trades: list[Trade], values: dict[datetime, float]) -> list[AnnotatedTrade]:
    return [
        AnnotatedTrade(
            trade=trade, bucket=_bucket(values[trade.entry_ts]), chop=values[trade.entry_ts]
        )
        for trade in trades
        if trade.entry_ts in values
    ]


def _funding_stress(trade: Trade) -> float:
    hours = (trade.exit_ts - trade.entry_ts).total_seconds() / 3600
    return NOTIONAL_USD * hours / 8 * FUNDING_RATE_8H


def _stats(trades: list[AnnotatedTrade], multipliers: dict[str, float]) -> dict[str, float | int]:
    pnls = [item.trade.pnl_usd * multipliers[item.bucket] for item in trades]
    stressed = [
        pnl - _funding_stress(item.trade) * multipliers[item.bucket]
        for pnl, item in zip(pnls, trades, strict=True)
    ]
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "n_trades": len(trades),
        "net_pnl_usd": sum(pnls),
        "stressed_net_pnl_usd": sum(stressed),
        "mean_pnl_usd": sum(pnls) / len(pnls) if pnls else 0.0,
        "median_pnl_usd": median(pnls) if pnls else 0.0,
        "win_rate": sum(pnl > 0 for pnl in pnls) / len(pnls) if pnls else 0.0,
        "closed_trade_max_drawdown_usd": max_drawdown,
    }


def _by_bucket(trades: list[AnnotatedTrade]) -> dict[str, dict[str, float | int]]:
    return {
        bucket: _stats([item for item in trades if item.bucket == bucket], OVERLAYS["baseline"])
        for bucket in BUCKETS
    }


def _by_window(
    trades: list[AnnotatedTrade], multipliers: dict[str, float]
) -> dict[str, dict[str, float | int]]:
    return {
        name: _stats([item for item in trades if start <= item.trade.exit_ts < end], multipliers)
        for name, start, end in WINDOWS
    }


def _candidate(trades: list[AnnotatedTrade], lookback: int, name: str) -> dict[str, Any]:
    annual = _by_window(trades, OVERLAYS[name])
    train = [annual[window[0]]["stressed_net_pnl_usd"] for window in WINDOWS[:2]]
    holdout = [annual[window[0]]["stressed_net_pnl_usd"] for window in WINDOWS[2:]]
    return {
        "lookback": lookback,
        "overlay": name,
        "multipliers": OVERLAYS[name],
        "annual": annual,
        "train_median_stressed_pnl_usd": median(train),
        "train_total_stressed_pnl_usd": sum(train),
        "holdout_stressed_pnl_usd": sum(holdout),
        "holdout_windows_beat_baseline": None,
    }


def _rank_key(candidate: dict[str, Any]) -> tuple[float, float, int, int]:
    return (
        float(candidate["train_median_stressed_pnl_usd"]),
        float(candidate["train_total_stressed_pnl_usd"]),
        -int(candidate["lookback"]),
        -list(OVERLAYS).index(str(candidate["overlay"])),
    )


async def study(data: Path, timeframe: str) -> dict[str, Any]:
    rule = LOCKED[timeframe]
    result = await evaluate(
        data=data,
        timeframe=timeframe,
        input_timeframe=timeframe,
        tolerance=rule["tolerance"],
        winner_threshold=rule["winner"][0],
        winner_count=rule["winner"][1],
        loss_threshold=rule["loss"][0],
        loss_count=rule["loss"][1],
        notional_usd=NOTIONAL_USD,
        leverage=5.0,
    )
    trades: list[Trade] = result.pop("_trades")
    bars = _resampled_bars(data, timeframe, input_timeframe=timeframe)
    descriptive: dict[str, Any] = {}
    candidates: list[dict[str, Any]] = []
    annotations: dict[int, list[AnnotatedTrade]] = {}
    for lookback in LOOKBACKS:
        annotated = _annotate(trades, _chop_by_timestamp(bars, lookback))
        annotations[lookback] = annotated
        descriptive[str(lookback)] = {
            "annotated_trade_count": len(annotated),
            "whole_period": _by_bucket(annotated),
            "annual": {
                name: _by_bucket([item for item in annotated if start <= item.trade.exit_ts < end])
                for name, start, end in WINDOWS
            },
        }
        candidates.extend(_candidate(annotated, lookback, name) for name in OVERLAYS)
    candidates.sort(key=_rank_key, reverse=True)
    selected = candidates[0]
    baseline = next(
        candidate
        for candidate in candidates
        if candidate["lookback"] == selected["lookback"] and candidate["overlay"] == "baseline"
    )
    selected["holdout_windows_beat_baseline"] = {
        name: selected["annual"][name]["stressed_net_pnl_usd"]
        > baseline["annual"][name]["stressed_net_pnl_usd"]
        for name, _, _ in WINDOWS[2:]
    }
    return {
        "protocol": {
            "preregistration": "docs/choppiness-regime-study-preregistration.md",
            "timeframe": timeframe,
            "locked_rule": rule,
            "windows": [name for name, _, _ in WINDOWS],
            "selection_windows": [name for name, _, _ in WINDOWS[:2]],
            "untouched_holdout_windows": [name for name, _, _ in WINDOWS[2:]],
            "funding_stress": "one-sided 1 bp per 8h applied to every open trade",
            "trade_costs": "10 bps per fill plus 5 bps slippage, embedded in trade P&L",
            "note": "sizing overlays are linear attribution only; strategy decisions and cooldown state are unchanged",
        },
        "descriptive_by_lookback": descriptive,
        "candidates_ranked_on_2022_24_only": candidates,
        "selected": selected,
        "baseline_same_lookback": baseline,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeframe", choices=("1d", "4h"), required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(study(args.data, args.timeframe))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    selected = report["selected"]
    print(
        f"{args.timeframe}: selected CHOP {selected['lookback']} / {selected['overlay']}; "
        f"train stress=${selected['train_total_stressed_pnl_usd']:+.2f}; "
        f"holdout stress=${selected['holdout_stressed_pnl_usd']:+.2f}"
    )


if __name__ == "__main__":
    main()
