#!/usr/bin/env python3
"""Build marked 1d/4h portfolio history and isolated-liquidation wick audit.

This research utility replays the locked production strategy on native Binance
candles.  It records daily P&L for $1,000 notional per timeframe after the
same simulated fee/slippage model as the strategy research.  It also checks
each open position against subsequent 4h high/low bars at theoretical LN
Markets isolated liquidation levels.  Funding is deliberately excluded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluate_timeframe import MetricsRecorder, _resampled_bars

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross

LOCKED_PARAMS = {
    "tfs": ("1d", "4h"),
    "tolerance_pct": 0.005,
    "base_size_usd": 1000.0,
    "base_leverage": 5.0,
    "size_multipliers": {"1d": 1.0, "4h": 1.0},
    "same_bar_flip": True,
    "warmup_bars_per_tf": 21,
    "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
    "cooldown_signal_count": {"1d": 12, "4h": 11},
    "loss_cooldown_threshold_pct": {"1d": 0.05, "4h": 0.02},
    "loss_cooldown_signal_count": {"1d": 3, "4h": 4},
    "cooldown_mode": "verdict_transition",
}


@dataclass
class LiquidationAudit:
    seen: set[tuple[str, object]] = field(default_factory=set)
    breached: dict[int, set[tuple[str, object]]] = field(default_factory=lambda: defaultdict(set))

    def inspect(self, executor: PaperFillExecutor, *, high: float, low: float) -> None:
        for tf, pos in executor.positions.items():
            if pos.qty_sats == 0 or pos.entry_price_usd is None:
                continue
            key = (tf, pos.entry_price_usd)
            self.seen.add(key)
            for leverage in (3, 5, 10, 25):
                if pos.side == "long":
                    liquidation = pos.entry_price_usd * leverage / (leverage + 1)
                    hit = low <= liquidation
                else:
                    # Inverse-contract short: equity reaches zero when price
                    # rises to E * L / (L - 1).  This is undefined at 1x.
                    liquidation = pos.entry_price_usd * leverage / (leverage - 1)
                    hit = high >= liquidation
                if hit:
                    self.breached[leverage].add(key)


def _marked_value(
    executor: PaperFillExecutor, fees_usd: dict[str, float], mark: float
) -> dict[str, float]:
    values: dict[str, float] = {}
    for tf in ("1d", "4h"):
        pos = executor.positions.get(tf)
        if pos is None:
            values[tf] = 0.0
            continue
        unrealized = 0.0
        if pos.qty_sats and pos.entry_price_usd is not None:
            unrealized = pos.qty_sats * (mark - pos.entry_price_usd) / 1e8
        values[tf] = pos.realized_pnl_usd + unrealized - fees_usd[tf]
    return values


async def build(*, data_1d: Path, data_4h: Path) -> dict[str, Any]:
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
    strategy = MaCross(params=LOCKED_PARAMS)
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    for tf in strategy.tfs:
        state.positions[tf] = TfPosition()
    strategy.on_startup(state)

    bars = [
        *_resampled_bars(data_1d, "1d", input_timeframe="1d"),
        *_resampled_bars(data_4h, "4h", input_timeframe="4h"),
    ]
    # The normal source emits 1d before 4h at shared midnight boundaries.
    rank = {"1d": 0, "4h": 1}
    bars.sort(key=lambda bar: (bar.ts, rank[bar.timeframe]))
    daily_marks = {bar.ts: bar.close for bar in bars if bar.timeframe == "1d"}
    fees_usd: dict[str, float] = defaultdict(float)
    audit = LiquidationAudit()
    daily: list[dict[str, Any]] = []
    previous = {"1d": 0.0, "4h": 0.0}

    index = 0
    while index < len(bars):
        ts = bars[index].ts
        group: list[Any] = []
        while index < len(bars) and bars[index].ts == ts:
            group.append(bars[index])
            index += 1
        for bar in group:
            if bar.timeframe == "4h":
                # A position existing at the prior close is exposed to this
                # bar's complete range before any close-based strategy action.
                audit.inspect(executor, high=bar.high, low=bar.low)
            executor.update_price(bar.close)
            guard.current_price_usd = bar.close
            before_fills = len(recorder.fills)
            for intent in intents_to_list(strategy.on_bar(bar, state)):
                signal_id = recorder.record_signal(1)
                decision = await guard.submit(
                    intent=intent, signal_id=signal_id, run_id=1, ts=bar.ts
                )
                if decision.order_id is not None and decision.order_id > 0:
                    guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
            for fill in recorder.fills[before_fills:]:
                fees_usd[fill.trigger_tf] += fill.fee_sats * fill.price_usd / 1e8
            for tf in strategy.tfs:
                pos = state.positions[tf]
                pos.side = executor.position_side(tf)
                pos.qty_sats = executor.position_qty_sats(tf)
                pos.entry_price_usd = executor.position_entry_price(tf)

        if ts in daily_marks:
            values = _marked_value(executor, fees_usd, daily_marks[ts])
            daily.append(
                {
                    "ts": ts.isoformat(),
                    "pnl_1d_usd": values["1d"] - previous["1d"],
                    "pnl_4h_usd": values["4h"] - previous["4h"],
                    "pnl_total_usd": sum(values.values()) - sum(previous.values()),
                    "value_1d_usd": values["1d"],
                    "value_4h_usd": values["4h"],
                    "value_total_usd": sum(values.values()),
                    "active_1d": bool(executor.position_qty_sats("1d")),
                    "active_4h": bool(executor.position_qty_sats("4h")),
                }
            )
            previous = values

    return {
        "parameters": {
            "locked_strategy": LOCKED_PARAMS,
            "notional_usd_per_timeframe": 1000.0,
            "simulated_fee_bps_per_fill": 10.0,
            "simulated_slippage_bps_per_fill": 5.0,
            "funding": "not included",
        },
        "liquidation_wick_audit": {
            "open_position_instances": len(audit.seen),
            "theoretical_wick_breaches": {
                f"{leverage}x": len(audit.breached[leverage]) for leverage in (3, 5, 10, 25)
            },
            "caveat": "Uses native 4h high/low and theoretical isolated liquidation. Fees, funding, and order-book execution can make actual liquidation earlier.",
        },
        "daily": daily,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-1d", type=Path, required=True)
    parser.add_argument("--data-4h", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(build(data_1d=args.data_1d, data_4h=args.data_4h))
    args.json_out.write_text(json.dumps(report, indent=2) + "\n")
    audit = report["liquidation_wick_audit"]
    print(
        f"wrote {args.json_out}; {len(report['daily'])} daily marks; "
        f"theoretical wick breaches={audit['theoretical_wick_breaches']}"
    )


if __name__ == "__main__":
    main()
