#!/usr/bin/env python3
"""Exploratory sizing overlay for confirmed locked 1d MA-cross entries.

This does not introduce another trading strategy.  It replays the locked 1d
strategy unchanged and increases only an entry's notional when the entry bar
also passes the frozen daily momentum-burst filter.  Results are descriptive:
the relevant historical data has already informed related research and must
not be treated as a fresh out-of-sample validation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import deque
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from build_sizing_history import LOCKED_PARAMS
from evaluate_timeframe import MetricsRecorder, _resampled_bars

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.metrics import Trade, pair_fills_by_tf
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import Bar, SignalKind, StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross

NOTIONAL_USD = 1000.0
LEVERAGE = 5.0
FUNDING_RATE_8H = 0.0001
WINDOWS = (
    ("2019-20", datetime(2019, 9, 9, tzinfo=UTC), datetime(2020, 7, 11, tzinfo=UTC)),
    ("2020-21", datetime(2020, 7, 11, tzinfo=UTC), datetime(2021, 7, 11, tzinfo=UTC)),
    ("2021-22", datetime(2021, 7, 11, tzinfo=UTC), datetime(2022, 7, 11, tzinfo=UTC)),
    ("2022-23", datetime(2022, 7, 11, tzinfo=UTC), datetime(2023, 7, 11, tzinfo=UTC)),
    ("2023-24", datetime(2023, 7, 11, tzinfo=UTC), datetime(2024, 7, 11, tzinfo=UTC)),
    ("2024-25", datetime(2024, 7, 11, tzinfo=UTC), datetime(2025, 7, 11, tzinfo=UTC)),
    ("2025-26", datetime(2025, 7, 11, tzinfo=UTC), datetime(2026, 7, 11, tzinfo=UTC)),
)


@dataclass
class _ConfirmationState:
    closes: deque[float]
    true_ranges: deque[float]
    sma_history: deque[float]
    ema_history: deque[float]
    ema: float | None = None
    verdict: int = 0


class MomentumConfirmation:
    """Exact entry filter from the frozen momentum-burst experiment."""

    def __init__(self, timeframe: str) -> None:
        self.timeframe = timeframe
        self.state = _ConfirmationState(
            closes=deque(maxlen=128),
            true_ranges=deque(maxlen=128),
            sma_history=deque(maxlen=32),
            ema_history=deque(maxlen=32),
        )

    def update(self, bar: Bar) -> str | None:
        state = self.state
        previous_close = state.closes[-1] if state.closes else None
        true_range = bar.high - bar.low
        if previous_close is not None:
            true_range = max(
                true_range, abs(bar.high - previous_close), abs(bar.low - previous_close)
            )
        state.closes.append(bar.close)
        state.true_ranges.append(true_range)
        if len(state.closes) < 21:
            return None

        closes = list(state.closes)
        sma = sum(closes[-20:]) / 20
        if state.ema is None:
            state.ema = sum(closes[-21:]) / 21
        else:
            state.ema = bar.close * (2 / 22) + state.ema * (20 / 22)
        state.sma_history.append(sma)
        state.ema_history.append(state.ema)

        if bar.close > sma * 1.005 and bar.close > state.ema * 1.005:
            verdict = 1
        elif bar.close < sma * 0.995 and bar.close < state.ema * 0.995:
            verdict = -1
        else:
            verdict = 0
        previous_verdict = state.verdict
        state.verdict = verdict
        if verdict == 0 or verdict == previous_verdict or not self._qualifies(verdict):
            return None
        return "long" if verdict > 0 else "short"

    def _qualifies(self, direction: int) -> bool:
        state = self.state
        offset = 6  # five completed bar-to-bar slope intervals
        if (
            len(state.sma_history) < offset
            or len(state.ema_history) < offset
            or len(state.closes) < 6
            or len(state.true_ranges) < 20
        ):
            return False
        sma_slope = state.sma_history[-1] / state.sma_history[-offset] - 1
        ema_slope = state.ema_history[-1] / state.ema_history[-offset] - 1
        if direction * sma_slope <= 0 or direction * ema_slope <= 0:
            return False
        closes = list(state.closes)
        if self.timeframe == "1d":
            bar_return = closes[-1] / closes[-2] - 1
            atr_pct = sum(list(state.true_ranges)[-20:]) / 20 / closes[-1]
            return direction * bar_return / atr_pct > 0.5
        prior_return = closes[-1] / closes[-6] - 1
        return direction * prior_return > 0


def _window(trades: list[Trade], start: datetime, end: datetime) -> dict[str, float | int]:
    selected = [trade for trade in trades if start <= trade.exit_ts < end]
    trading_cost_net = sum(trade.pnl_usd for trade in selected)
    funding = sum(
        (trade.qty_sats * trade.entry_price_usd / 1e8)
        * (trade.exit_ts - trade.entry_ts).total_seconds()
        / 3600
        / 8
        * FUNDING_RATE_8H
        for trade in selected
    )
    return {
        "trades": len(selected),
        "trading_cost_net_usd": trading_cost_net,
        "funding_stress_usd": funding,
        "stressed_pnl_usd": trading_cost_net - funding,
    }


def _max_drawdown(values: list[float]) -> float:
    peak = 0.0
    drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    return drawdown


async def evaluate(data: Path, timeframe: str, multiplier: float) -> dict[str, Any]:
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
    strategy = MaCross(params={**LOCKED_PARAMS, "tfs": (timeframe,)})
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    state.positions[timeframe] = TfPosition()
    strategy.on_startup(state)
    confirmation = MomentumConfirmation(timeframe)
    entry_confirmation: dict[datetime, bool] = {}
    fees_usd = 0.0
    marks: list[float] = []

    for bar in _resampled_bars(data, timeframe, input_timeframe=timeframe):
        confirmed_side = confirmation.update(bar)
        executor.update_price(bar.close)
        guard.current_price_usd = bar.close
        before = len(recorder.fills)
        for intent in intents_to_list(strategy.on_bar(bar, state)):
            if intent.kind == SignalKind.ENTRY:
                confirmed = confirmed_side == intent.side.value
                entry_confirmation[bar.ts] = confirmed
                if confirmed:
                    intent = replace(
                        intent,
                        size_usd=intent.size_usd * multiplier,
                        metadata={**intent.metadata, "momentum_confirmation": True},
                    )
            signal_id = recorder.record_signal(1)
            decision = await guard.submit(intent=intent, signal_id=signal_id, run_id=1, ts=bar.ts)
            if decision.order_id is not None and decision.order_id > 0:
                guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
        for fill in recorder.fills[before:]:
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
        marks.append(realized + unrealized - fees_usd)

    trades = pair_fills_by_tf(recorder.fills).get(timeframe, [])
    confirmed = [trade for trade in trades if entry_confirmation.get(trade.entry_ts, False)]
    unconfirmed = [trade for trade in trades if not entry_confirmation.get(trade.entry_ts, False)]
    windows = {name: _window(trades, start, end) for name, start, end in WINDOWS}
    return {
        "multiplier": multiplier,
        "timeframe": timeframe,
        "all_entries": _window(trades, WINDOWS[0][1], WINDOWS[-1][2]),
        "confirmed_entries": _window(confirmed, WINDOWS[0][1], WINDOWS[-1][2]),
        "unconfirmed_entries": _window(unconfirmed, WINDOWS[0][1], WINDOWS[-1][2]),
        "annual": windows,
        "max_marked_drawdown_usd": _max_drawdown(marks),
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--timeframe", choices=("1d", "4h"), required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    results = {
        str(multiplier): await evaluate(args.data, args.timeframe, multiplier)
        for multiplier in (1.0, 1.25, 1.5)
    }
    report = {
        "purpose": f"Exploratory only: size locked {args.timeframe} entries more when the frozen momentum filter agrees.",
        "caveat": "Historical periods are descriptive, not a fresh validation, because related challenger results were already observed.",
        "rules": {
            "base_strategy": f"Locked {args.timeframe} MA/cool-off strategy unchanged.",
            "confirmation": (
                "Exact frozen momentum-burst entry filter: aligned five-bar SMA20/EMA21 slopes; "
                + (
                    "same-direction impulse > 0.5 ATR20."
                    if args.timeframe == "1d"
                    else "same-direction preceding five-bar return."
                )
            ),
            "sizing": "Only confirmed locked entries use the stated multiplier; all exits and unconfirmed entries stay unchanged.",
            "costs": "10 bps per fill and 5 bps slippage; always-paid 1 bp/8h funding stress.",
        },
        "results": results,
    }
    args.json_out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    asyncio.run(main())
