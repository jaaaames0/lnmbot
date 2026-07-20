#!/usr/bin/env python3
"""Compare locked MA/cool-off state after a cold start with continuous replay."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluate_timeframe import MetricsRecorder, _resampled_bars

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import Bar, TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross

LOCKED = {
    "1d": {"tolerance": 0.005, "winner": (0.03, 12), "loss": (0.05, 3)},
    "4h": {"tolerance": 0.005, "winner": (0.05, 11), "loss": (0.02, 4)},
}
STARTS = (
    datetime(2023, 7, 11, tzinfo=UTC),
    datetime(2024, 7, 11, tzinfo=UTC),
    datetime(2025, 7, 11, tzinfo=UTC),
)
WARMUP_DAYS = 31
FUNDING_RATE_8H = 0.0001


def _strategy(timeframe: str) -> MaCross:
    rule = LOCKED[timeframe]
    return MaCross(
        params={
            "tfs": (timeframe,),
            "tolerance_pct": rule["tolerance"],
            "base_size_usd": 1000.0,
            "base_leverage": 5.0,
            "size_multipliers": {timeframe: 1.0},
            "same_bar_flip": True,
            "warmup_bars_per_tf": 21,
            "cooldown_threshold_pct": {timeframe: rule["winner"][0]},
            "cooldown_signal_count": {timeframe: rule["winner"][1]},
            "loss_cooldown_threshold_pct": {timeframe: rule["loss"][0]},
            "loss_cooldown_signal_count": {timeframe: rule["loss"][1]},
            "cooldown_mode": "verdict_transition",
        }
    )


async def _replay(
    *, bars: list[Bar], timeframe: str, cold_start: datetime | None = None
) -> dict[datetime, dict[str, Any]]:
    cfg = BotConfig(
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=10.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=10_000,
    )
    recorder = MetricsRecorder()
    executor = PaperFillExecutor(recorder=recorder, run_id=1)
    guard = RiskGuard(limits=from_config(cfg), recorder=recorder, executor=executor)
    strategy = _strategy(timeframe)
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    state.positions[timeframe] = TfPosition()
    strategy.on_startup(state)
    snapshots: dict[datetime, dict[str, Any]] = {}
    warmup_start = cold_start - timedelta(days=WARMUP_DAYS) if cold_start else None
    fees_usd = 0.0
    funding_stress_usd = 0.0
    previous_ts: datetime | None = None
    previously_active = False

    for source_bar in bars:
        if cold_start and source_bar.ts < warmup_start:
            continue
        bar = replace(source_bar, warmup=bool(cold_start and source_bar.ts < cold_start))
        if previous_ts is not None and previously_active:
            funding_stress_usd += (
                1000.0 * (bar.ts - previous_ts).total_seconds() / 3600 / 8 * FUNDING_RATE_8H
            )
        executor.update_price(bar.close)
        guard.current_price_usd = bar.close
        before_fills = len(recorder.fills)
        intents = intents_to_list(strategy.on_bar(bar, state))
        for intent in intents:
            signal_id = recorder.record_signal(1)
            decision = await guard.submit(intent=intent, signal_id=signal_id, run_id=1, ts=bar.ts)
            if decision.order_id is not None and decision.order_id > 0:
                guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
        for fill in recorder.fills[before_fills:]:
            fees_usd += fill.fee_sats * fill.price_usd / 1e8
        pos = state.position(timeframe)
        pos.side = executor.position_side(timeframe)
        pos.qty_sats = executor.position_qty_sats(timeframe)
        pos.entry_price_usd = executor.position_entry_price(timeframe)
        execution_pos = executor.positions.get(timeframe)
        unrealized = 0.0
        realized = 0.0
        if execution_pos is not None:
            realized = execution_pos.realized_pnl_usd
            if execution_pos.qty_sats and execution_pos.entry_price_usd is not None:
                unrealized = (
                    execution_pos.qty_sats * (bar.close - execution_pos.entry_price_usd) / 1e8
                )
        previous_ts = bar.ts
        previously_active = bool(pos.qty_sats)
        if cold_start and bar.ts < cold_start:
            continue
        tf_state = strategy.tf_state[timeframe]
        snapshots[bar.ts] = {
            "position": pos.side,
            "winner_cooldown": strategy._suppressed_signals[timeframe],
            "loss_cooldown": strategy._loss_suppressed_signals[timeframe],
            "verdict": tf_state.verdict,
            "marked_value_usd": realized + unrealized - fees_usd,
            "stressed_value_usd": realized + unrealized - fees_usd - funding_stress_usd,
            "intents": tuple(
                (intent.kind.value, intent.side.value if intent.side else None, intent.reason)
                for intent in intents
            ),
        }
    return snapshots


def _state_key(snapshot: dict[str, Any]) -> tuple[object, ...]:
    return (
        snapshot["position"],
        snapshot["winner_cooldown"],
        snapshot["loss_cooldown"],
        snapshot["verdict"],
        snapshot["intents"],
    )


def _divergence_pnl(
    *,
    timestamps: list[datetime],
    continuous: dict[datetime, dict[str, Any]],
    cold: dict[datetime, dict[str, Any]],
    last_mismatch: datetime | None,
) -> dict[str, object]:
    if not timestamps:
        return {}
    start = timestamps[0]
    if last_mismatch is None:
        convergence = start
    else:
        convergence = next((ts for ts in timestamps if ts > last_mismatch), timestamps[-1])

    def delta(path: dict[datetime, dict[str, Any]], field: str, ts: datetime) -> float:
        return float(path[ts][field]) - float(path[start][field])

    relative_marked = [
        (ts, delta(cold, "marked_value_usd", ts) - delta(continuous, "marked_value_usd", ts))
        for ts in timestamps
        if ts <= convergence
    ]
    relative_stressed = [
        (ts, delta(cold, "stressed_value_usd", ts) - delta(continuous, "stressed_value_usd", ts))
        for ts in timestamps
        if ts <= convergence
    ]
    worst_marked = min(relative_marked, key=lambda row: row[1])
    best_marked = max(relative_marked, key=lambda row: row[1])
    return {
        "comparison_start": start.isoformat(),
        "convergence_ts": convergence.isoformat(),
        "continuous_marked_pnl_usd": delta(continuous, "marked_value_usd", convergence),
        "cold_marked_pnl_usd": delta(cold, "marked_value_usd", convergence),
        "continuous_stressed_pnl_usd": delta(continuous, "stressed_value_usd", convergence),
        "cold_stressed_pnl_usd": delta(cold, "stressed_value_usd", convergence),
        "cold_minus_continuous_marked_usd": relative_marked[-1][1],
        "cold_minus_continuous_stressed_usd": relative_stressed[-1][1],
        "best_cold_relative_marked_usd": {
            "ts": best_marked[0].isoformat(),
            "value": best_marked[1],
        },
        "worst_cold_relative_marked_usd": {
            "ts": worst_marked[0].isoformat(),
            "value": worst_marked[1],
        },
    }


async def analyze(data: Path, timeframe: str) -> dict[str, Any]:
    bars = _resampled_bars(data, timeframe, input_timeframe=timeframe)
    continuous = await _replay(bars=bars, timeframe=timeframe)
    comparisons: list[dict[str, Any]] = []
    for start in STARTS:
        cold = await _replay(bars=bars, timeframe=timeframe, cold_start=start)
        timestamps = [ts for ts in cold if ts in continuous]
        mismatches = [ts for ts in timestamps if _state_key(cold[ts]) != _state_key(continuous[ts])]
        last_mismatch = mismatches[-1] if mismatches else None
        post_last_matches = (
            len([ts for ts in timestamps if last_mismatch is not None and ts > last_mismatch])
            if last_mismatch
            else len(timestamps)
        )
        pnl = _divergence_pnl(
            timestamps=timestamps,
            continuous=continuous,
            cold=cold,
            last_mismatch=last_mismatch,
        )
        comparisons.append(
            {
                "cold_start": start.isoformat(),
                "bars_compared": len(timestamps),
                "mismatched_bars": len(mismatches),
                "final_state_equal": bool(timestamps)
                and _state_key(cold[timestamps[-1]]) == _state_key(continuous[timestamps[-1]]),
                "last_mismatch": last_mismatch.isoformat() if last_mismatch else None,
                "matching_bars_after_last_mismatch": post_last_matches,
                "continuous_final_state": continuous[timestamps[-1]] if timestamps else None,
                "cold_final_state": cold[timestamps[-1]] if timestamps else None,
                "divergence_pnl": pnl,
            }
        )
    return {
        "timeframe": timeframe,
        "rule": LOCKED[timeframe],
        "cold_start_policy": "31 calendar days of indicator-only warmup; flat position and zero cooldown counters at live start",
        "comparison": "position, both cooldown counters, verdict, and emitted intents must all match",
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--timeframe", choices=("1d", "4h"), required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(analyze(args.data, args.timeframe))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    for row in report["comparisons"]:
        print(
            f"{args.timeframe} cold start {row['cold_start'][:10]}: "
            f"mismatch={row['mismatched_bars']}/{row['bars_compared']} "
            f"final_equal={row['final_state_equal']} last={row['last_mismatch']}"
        )


if __name__ == "__main__":
    main()
