#!/usr/bin/env python3
"""Pre-registered Donchian challenger selection and sealed holdout runner."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluate_timeframe import MetricsRecorder, _resampled_bars

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.metrics import Trade, pair_fills_by_tf
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import Strategy, TfPosition
from lnmarkets_bot.strategy.donchian_breakout import DonchianBreakout
from lnmarkets_bot.strategy.ma_cross import MaCross

NOTIONAL = 1000.0
LEVERAGE = 5.0
FUNDING_RATE_8H = 0.0001
CUTOFF = datetime(2022, 7, 11, tzinfo=UTC)
CANDIDATES = ((20, 10), (55, 20), (100, 50))
DEVELOPMENT_WINDOWS = (
    ("2019-20", datetime(2019, 9, 9, tzinfo=UTC), datetime(2020, 7, 11, tzinfo=UTC)),
    ("2020-21", datetime(2020, 7, 11, tzinfo=UTC), datetime(2021, 7, 11, tzinfo=UTC)),
    ("2021-22", datetime(2021, 7, 11, tzinfo=UTC), CUTOFF),
)
HOLDOUT_WINDOWS = (
    ("2022-23", CUTOFF, datetime(2023, 7, 11, tzinfo=UTC)),
    ("2023-24", datetime(2023, 7, 11, tzinfo=UTC), datetime(2024, 7, 11, tzinfo=UTC)),
    ("2024-25", datetime(2024, 7, 11, tzinfo=UTC), datetime(2025, 7, 11, tzinfo=UTC)),
    ("2025-26", datetime(2025, 7, 11, tzinfo=UTC), datetime(2026, 7, 11, tzinfo=UTC)),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _window(trades: list[Trade], start: datetime, end: datetime) -> dict[str, Any]:
    selected = [trade for trade in trades if start <= trade.exit_ts < end]
    pnl = sum(trade.pnl_usd for trade in selected)
    hold_hours = sum((trade.exit_ts - trade.entry_ts).total_seconds() / 3600 for trade in selected)
    funding = NOTIONAL * hold_hours / 8 * FUNDING_RATE_8H
    return {
        "trades": len(selected),
        "pnl_after_trading_costs_usd": pnl,
        "funding_stress_usd": funding,
        "stressed_pnl_usd": pnl - funding,
        "hold_hours": hold_hours,
    }


async def _evaluate(
    *, strategy: Strategy, data: Path, timeframe: str, start: datetime, end: datetime
) -> dict[str, Any]:
    cfg = BotConfig(
        initial_balance_usd=10_000,
        risk_max_position_usd=10_000,
        risk_max_leverage=10,
        risk_max_daily_loss_usd=1_000_000,
        risk_max_orders_per_minute=10_000,
    )
    recorder = MetricsRecorder()
    executor = PaperFillExecutor(recorder=recorder, run_id=1)
    guard = RiskGuard(limits=from_config(cfg), recorder=recorder, executor=executor)
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    state.positions[timeframe] = TfPosition()
    strategy.on_startup(state)
    fees_usd = 0.0
    funding_usd = 0.0
    previous_ts = None
    previously_active = False
    daily_values: list[tuple[datetime, float]] = []

    for bar in _resampled_bars(data, timeframe, input_timeframe=timeframe):
        if bar.ts <= start:
            # Retain earlier bars only as causal warmup when the source begins
            # before the requested evaluation period.
            pass
        if bar.ts > end:
            break
        if previous_ts is not None and previously_active:
            hours = (bar.ts - previous_ts).total_seconds() / 3600
            funding_usd += NOTIONAL * hours / 8 * FUNDING_RATE_8H
        executor.update_price(bar.close)
        guard.current_price_usd = bar.close
        before = len(recorder.fills)
        for intent in intents_to_list(strategy.on_bar(bar, state)):
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
        exec_pos = executor.positions.get(timeframe)
        unrealized = 0.0
        realized = 0.0
        if exec_pos is not None:
            realized = exec_pos.realized_pnl_usd
            if exec_pos.qty_sats and exec_pos.entry_price_usd is not None:
                unrealized = exec_pos.qty_sats * (bar.close - exec_pos.entry_price_usd) / 1e8
        if start < bar.ts <= end and (timeframe == "1d" or bar.ts.hour == 0):
            daily_values.append((bar.ts, realized + unrealized - fees_usd - funding_usd))
        previous_ts = bar.ts
        previously_active = bool(pos.qty_sats)

    trades = pair_fills_by_tf(recorder.fills).get(timeframe, [])
    return {"trades": trades, "daily_values": daily_values, "fills": recorder.fills}


def _strategy(kind: str, timeframe: str, pair: tuple[int, int] | None = None) -> Strategy:
    common = {
        "tfs": (timeframe,),
        "base_size_usd": NOTIONAL,
        "base_leverage": LEVERAGE,
        "size_multipliers": {timeframe: 1.0},
    }
    if kind == "donchian":
        assert pair is not None
        return DonchianBreakout(params={**common, "entry_window": pair[0], "exit_window": pair[1]})
    return MaCross(params=common)


async def select(data: dict[str, Path], output: Path) -> None:
    report: dict[str, Any] = {
        "protocol": "docs/challenger-preregistration.md",
        "cutoff": CUTOFF.isoformat(),
        "data_sha256": {tf: _sha256(path) for tf, path in data.items()},
        "timeframes": {},
    }
    for tf in ("1d", "4h"):
        rows = []
        for pair in CANDIDATES:
            result = await _evaluate(
                strategy=_strategy("donchian", tf, pair),
                data=data[tf],
                timeframe=tf,
                start=DEVELOPMENT_WINDOWS[0][1],
                end=CUTOFF,
            )
            windows = {
                name: _window(result["trades"], start, end)
                for name, start, end in DEVELOPMENT_WINDOWS
            }
            pnls = [window["stressed_pnl_usd"] for window in windows.values()]
            rows.append(
                {
                    "entry_window": pair[0],
                    "exit_window": pair[1],
                    "windows": windows,
                    "positive_windows": sum(value > 0 for value in pnls),
                    "median_window_pnl_usd": median(pnls),
                    "total_stressed_pnl_usd": sum(pnls),
                }
            )
        rows.sort(
            key=lambda row: (
                row["positive_windows"],
                row["median_window_pnl_usd"],
                row["total_stressed_pnl_usd"],
            ),
            reverse=True,
        )
        report["timeframes"][tf] = {"candidates": rows, "selected": rows[0]}
    report["frozen_hashes"] = {
        "strategy": _sha256(
            Path(__file__).resolve().parents[1] / "src/lnmarkets_bot/strategy/donchian_breakout.py"
        ),
        "runner": _sha256(Path(__file__)),
        "preregistration": _sha256(
            Path(__file__).resolve().parents[1] / "docs/challenger-preregistration.md"
        ),
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        "frozen selection: "
        + ", ".join(
            f"{tf}={row['selected']['entry_window']}/{row['selected']['exit_window']}"
            for tf, row in report["timeframes"].items()
        )
    )
    print(f"wrote {output}")


def _combine_daily(results: dict[str, dict[str, Any]]) -> tuple[float, dict[str, float]]:
    by_ts: dict[datetime, float] = defaultdict(float)
    for result in results.values():
        for ts, value in result["daily_values"]:
            by_ts[ts] += value
    values = [value for _, value in sorted(by_ts.items())]
    peak = 0.0
    max_dd = 0.0
    for value in values:
        peak = max(peak, value)
        max_dd = max(max_dd, peak - value)
    annual: dict[str, float] = {}
    for name, start, end in HOLDOUT_WINDOWS:
        annual[name] = sum(
            _window(result["trades"], start, end)["stressed_pnl_usd"] for result in results.values()
        )
    return max_dd, annual


def _liquidation_breaches(result: dict[str, Any], bars_4h: list[Any]) -> int:
    fills = result["fills"]
    breaches = 0
    for index in range(0, len(fills) - 1, 2):
        entry = fills[index]
        exit_fill = fills[index + 1]
        side = "long" if entry.side == "buy" else "short"
        if side == "long":
            level = entry.price_usd * LEVERAGE / (LEVERAGE + 1)
            hit = any(entry.ts < bar.ts <= exit_fill.ts and bar.low <= level for bar in bars_4h)
        else:
            level = entry.price_usd * LEVERAGE / (LEVERAGE - 1)
            hit = any(entry.ts < bar.ts <= exit_fill.ts and bar.high >= level for bar in bars_4h)
        breaches += int(hit)
    return breaches


async def holdout(data: dict[str, Path], selection_path: Path, output: Path) -> None:
    selection = json.loads(selection_path.read_text())
    challenger: dict[str, dict[str, Any]] = {}
    champion: dict[str, dict[str, Any]] = {}
    for tf in ("1d", "4h"):
        chosen = selection["timeframes"][tf]["selected"]
        pair = (int(chosen["entry_window"]), int(chosen["exit_window"]))
        challenger[tf] = await _evaluate(
            strategy=_strategy("donchian", tf, pair),
            data=data[tf],
            timeframe=tf,
            start=CUTOFF,
            end=HOLDOUT_WINDOWS[-1][2],
        )
        champion[tf] = await _evaluate(
            strategy=_strategy("ma", tf),
            data=data[tf],
            timeframe=tf,
            start=CUTOFF,
            end=HOLDOUT_WINDOWS[-1][2],
        )
    bars_4h = _resampled_bars(data["4h"], "4h", input_timeframe="4h")
    challenger_dd, challenger_annual = _combine_daily(challenger)
    champion_dd, champion_annual = _combine_daily(champion)
    challenger_total = sum(challenger_annual.values())
    champion_total = sum(champion_annual.values())
    breaches = sum(_liquidation_breaches(result, bars_4h) for result in challenger.values())
    criteria = {
        "higher_stressed_pnl": challenger_total > champion_total,
        "no_worse_marked_drawdown": challenger_dd <= champion_dd,
        "at_least_three_positive_years": sum(v > 0 for v in challenger_annual.values()) >= 3,
        "no_5x_liquidation_wicks": breaches == 0,
    }
    report = {
        "selection_sha256": _sha256(selection_path),
        "challenger": {
            "annual_stressed_pnl_usd": challenger_annual,
            "total_stressed_pnl_usd": challenger_total,
            "max_marked_drawdown_usd": challenger_dd,
            "theoretical_5x_liquidation_wicks": breaches,
        },
        "locked_champion": {
            "annual_stressed_pnl_usd": champion_annual,
            "total_stressed_pnl_usd": champion_total,
            "max_marked_drawdown_usd": champion_dd,
        },
        "criteria": criteria,
        "verdict": "PASS" if all(criteria.values()) else "FAIL",
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    print(f"wrote {output}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("select", "holdout"):
        command = sub.add_parser(name)
        command.add_argument("--data-1d", type=Path, required=True)
        command.add_argument("--data-4h", type=Path, required=True)
        command.add_argument("--json-out", type=Path, required=True)
        if name == "holdout":
            command.add_argument("--selection", type=Path, required=True)
    args = parser.parse_args()
    data = {"1d": args.data_1d, "4h": args.data_4h}
    if args.command == "select":
        asyncio.run(select(data, args.json_out))
    else:
        asyncio.run(holdout(data, args.selection, args.json_out))


if __name__ == "__main__":
    main()
