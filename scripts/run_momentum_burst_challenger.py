#!/usr/bin/env python3
"""Development gate and frozen holdout runner for MomentumBurst."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import run_donchian_challenger as shared

from lnmarkets_bot.strategy.momentum_burst import MomentumBurst

ROOT = Path(__file__).resolve().parents[1]


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _strategy(timeframe: str) -> MomentumBurst:
    return MomentumBurst(
        params={
            "tfs": (timeframe,),
            "hold_bars": {timeframe: 7 if timeframe == "1d" else 42},
            "base_size_usd": shared.NOTIONAL,
            "base_leverage": shared.LEVERAGE,
            "size_multipliers": {timeframe: 1.0},
        }
    )


async def development(data: dict[str, Path], output: Path) -> None:
    results: dict[str, dict[str, Any]] = {}
    annual = {name: 0.0 for name, _, _ in shared.DEVELOPMENT_WINDOWS}
    attribution: dict[str, Any] = {}
    for tf in ("1d", "4h"):
        result = await shared._evaluate(
            strategy=_strategy(tf),
            data=data[tf],
            timeframe=tf,
            start=shared.DEVELOPMENT_WINDOWS[0][1],
            end=shared.CUTOFF,
        )
        results[tf] = result
        attribution[tf] = {}
        for name, start, end in shared.DEVELOPMENT_WINDOWS:
            row = shared._window(result["trades"], start, end)
            attribution[tf][name] = row
            annual[name] += row["stressed_pnl_usd"]
    gate = {
        "positive_in_at_least_two_windows": sum(value > 0 for value in annual.values()) >= 2,
        "positive_in_aggregate": sum(annual.values()) > 0,
    }
    report = {
        "annual_combined_stressed_pnl_usd": annual,
        "combined_stressed_pnl_usd": sum(annual.values()),
        "attribution": attribution,
        "gate": gate,
        "gate_passed": all(gate.values()),
        "data_sha256": {tf: _hash(path) for tf, path in data.items()},
        "frozen_hashes": {
            "strategy": _hash(ROOT / "src/lnmarkets_bot/strategy/momentum_burst.py"),
            "runner": _hash(Path(__file__)),
            "shared_evaluator": _hash(ROOT / "scripts/run_donchian_challenger.py"),
            "preregistration": _hash(ROOT / "docs/data-informed-challenger-preregistration.md"),
            "exploration": _hash(ROOT / "runs/data_informed_exploration_pre2022.json"),
        },
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    print(f"wrote {output}")


def _assert_frozen(development_report: dict[str, Any]) -> None:
    if not development_report["gate_passed"]:
        raise RuntimeError("development gate failed; holdout is forbidden")
    expected = development_report["frozen_hashes"]
    actual = {
        "strategy": _hash(ROOT / "src/lnmarkets_bot/strategy/momentum_burst.py"),
        "runner": _hash(Path(__file__)),
        "shared_evaluator": _hash(ROOT / "scripts/run_donchian_challenger.py"),
        "preregistration": _hash(ROOT / "docs/data-informed-challenger-preregistration.md"),
        "exploration": _hash(ROOT / "runs/data_informed_exploration_pre2022.json"),
    }
    if actual != expected:
        raise RuntimeError(f"frozen research files changed: expected={expected}, actual={actual}")


async def holdout(data: dict[str, Path], development_path: Path, output: Path) -> None:
    development_report = json.loads(development_path.read_text())
    _assert_frozen(development_report)
    challenger: dict[str, dict[str, Any]] = {}
    champion: dict[str, dict[str, Any]] = {}
    for tf in ("1d", "4h"):
        challenger[tf] = await shared._evaluate(
            strategy=_strategy(tf),
            data=data[tf],
            timeframe=tf,
            start=shared.CUTOFF,
            end=shared.HOLDOUT_WINDOWS[-1][2],
        )
        champion[tf] = await shared._evaluate(
            strategy=shared._strategy("ma", tf),
            data=data[tf],
            timeframe=tf,
            start=shared.CUTOFF,
            end=shared.HOLDOUT_WINDOWS[-1][2],
        )
    bars_4h = shared._resampled_bars(data["4h"], "4h", input_timeframe="4h")
    challenger_dd, challenger_annual = shared._combine_daily(challenger)
    champion_dd, champion_annual = shared._combine_daily(champion)
    challenger_total = sum(challenger_annual.values())
    champion_total = sum(champion_annual.values())
    breaches = sum(shared._liquidation_breaches(result, bars_4h) for result in challenger.values())
    criteria = {
        "higher_stressed_pnl": challenger_total > champion_total,
        "no_worse_marked_drawdown": challenger_dd <= champion_dd,
        "at_least_three_positive_years": sum(v > 0 for v in challenger_annual.values()) >= 3,
        "no_5x_liquidation_wicks": breaches == 0,
    }
    report = {
        "development_report_sha256": _hash(development_path),
        "challenger": {
            "annual_stressed_pnl_usd": challenger_annual,
            "total_stressed_pnl_usd": challenger_total,
            "max_marked_drawdown_usd": challenger_dd,
            "theoretical_5x_liquidation_wicks": breaches,
            "attribution": {
                tf: {
                    name: shared._window(result["trades"], start, end)
                    for name, start, end in shared.HOLDOUT_WINDOWS
                }
                for tf, result in challenger.items()
            },
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
    for name in ("development", "holdout"):
        command = sub.add_parser(name)
        command.add_argument("--data-1d", type=Path, required=True)
        command.add_argument("--data-4h", type=Path, required=True)
        command.add_argument("--json-out", type=Path, required=True)
        if name == "holdout":
            command.add_argument("--development", type=Path, required=True)
    args = parser.parse_args()
    data = {"1d": args.data_1d, "4h": args.data_4h}
    if args.command == "development":
        asyncio.run(development(data, args.json_out))
    else:
        asyncio.run(holdout(data, args.development, args.json_out))


if __name__ == "__main__":
    main()
