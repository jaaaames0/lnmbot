"""Temporally held-out validation for independent loss cool-offs.

The locked winner rule stays fixed. Loss candidates are ranked only by the
first year of realized trades; the second year is reported only after a single
candidate has been selected. This prevents a chart episode in 2025 from being
directly optimized against the whole fixture.
"""
# ruff: noqa: I001
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.metrics import Trade, max_drawdown_pct, per_tf_summary

from sweep_cooldown_modes_2y import run_one


SPLIT = datetime(2025, 7, 11, tzinfo=UTC)
WINNER_COUNTS = (12, 11)
OPTIONS_1D = [(0.0, 0), *[(threshold, count) for threshold in (0.03, 0.05, 0.08, 0.12) for count in (1, 2, 3, 4, 6)]]
OPTIONS_4H = [(0.0, 0), *[(threshold, count) for threshold in (0.02, 0.03, 0.05, 0.08) for count in (1, 2, 3, 4, 6)]]


def _period_summary(result: dict[str, Any], *, after_split: bool) -> dict[str, Any]:
    trades_by_tf: dict[str, list[Trade]] = result["trades_by_tf"]
    selected = {
        tf: [
            trade for trade in trades
            if (trade.exit_ts >= SPLIT) == after_split
        ]
        for tf, trades in trades_by_tf.items()
    }
    equity = [
        value for ts, value in result["equity_points"]
        if (ts >= SPLIT) == after_split
    ]
    return {
        "summary": per_tf_summary(selected, notional_per_trade_usd=1000.0),
        "max_dd_pct": max_drawdown_pct(equity),
    }


def _pnl(period: dict[str, Any], tf: str) -> float:
    return float(period["summary"]["by_tf"].get(tf, {}).get("total_pnl_usd", 0.0))


def _trades(period: dict[str, Any]) -> int:
    return int(period["summary"].get("aggregate", {}).get("n_trades", 0))


async def _run(
    data: Path, *, option_1d: tuple[float, int], option_4h: tuple[float, int],
) -> dict[str, Any]:
    return await run_one(
        data=data,
        mode="verdict_transition",
        count_1d=WINNER_COUNTS[0],
        count_4h=WINNER_COUNTS[1],
        loss_threshold_1d=option_1d[0],
        loss_count_1d=option_1d[1],
        loss_threshold_4h=option_4h[0],
        loss_count_4h=option_4h[1],
        include_trades=True,
    )


async def validate(data: Path) -> dict[str, Any]:
    baseline_result = await _run(data, option_1d=(0.0, 0), option_4h=(0.0, 0))
    baseline_train = _period_summary(baseline_result, after_split=False)

    one_d: dict[tuple[float, int], dict[str, Any]] = {(0.0, 0): baseline_train}
    four_h: dict[tuple[float, int], dict[str, Any]] = {(0.0, 0): baseline_train}
    for option in OPTIONS_1D[1:]:
        print(f"[train] 1d loss threshold/count={option}", flush=True)
        one_d[option] = _period_summary(
            await _run(data, option_1d=option, option_4h=(0.0, 0)), after_split=False,
        )
    for option in OPTIONS_4H[1:]:
        print(f"[train] 4h loss threshold/count={option}", flush=True)
        four_h[option] = _period_summary(
            await _run(data, option_1d=(0.0, 0), option_4h=option), after_split=False,
        )

    # Per-TF trading is independent, so this train P&L sum is exact for every
    # 1d/4h combination. No holdout data is used to make this ranking.
    candidates = [
        {
            "loss_1d": option_1d,
            "loss_4h": option_4h,
            "train_pnl_usd": _pnl(one_d[option_1d], "1d") + _pnl(four_h[option_4h], "4h"),
        }
        for option_1d in OPTIONS_1D
        for option_4h in OPTIONS_4H
    ]
    candidates.sort(key=lambda row: row["train_pnl_usd"], reverse=True)

    # Candidates are ordered by first-year P&L. The first one satisfying the
    # predeclared train-only risk constraints is therefore the selected rule.
    # Holdout metrics are deliberately not read until after this selection.
    selected = None
    validated = []
    baseline_train_trades = _trades(baseline_train)
    for candidate in candidates:
        print(f"[validate-train] {candidate['loss_1d']} / {candidate['loss_4h']}", flush=True)
        result = await _run(
            data, option_1d=candidate["loss_1d"], option_4h=candidate["loss_4h"],
        )
        train = _period_summary(result, after_split=False)
        candidate.update({"train": train})
        validated.append(candidate)
        if (
            train["max_dd_pct"] <= baseline_train["max_dd_pct"] + 0.02
            and _trades(train) >= baseline_train_trades * 0.75
        ):
            selected = candidate
            selected["holdout"] = _period_summary(result, after_split=True)
            break

    # The baseline holdout is read only after a loss candidate has either been
    # selected or the predeclared training grid has been exhausted.
    baseline_holdout = _period_summary(baseline_result, after_split=True)
    return {
        "protocol": {
            "split": SPLIT.isoformat(),
            "winner_cooldown": {"1d": WINNER_COUNTS[0], "4h": WINNER_COUNTS[1]},
            "selection": "highest first-year P&L among candidates with train max DD no more than baseline +2pp and at least 75% of baseline trades",
            "loss_options_1d": OPTIONS_1D,
            "loss_options_4h": OPTIONS_4H,
        },
        "baseline": {"train": baseline_train, "holdout": baseline_holdout},
        "validated_candidates": validated,
        "selected": selected,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument("--json-out", type=Path, default=Path("runs/loss_cooldown_validation_2y.json"))
    args = parser.parse_args()
    report = asyncio.run(validate(args.data))
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")

    selected = report["selected"]
    baseline = report["baseline"]
    print(f"\nbaseline train=${_pnl(baseline['train'], '1d') + _pnl(baseline['train'], '4h'):+.0f} "
          f"holdout=${_pnl(baseline['holdout'], '1d') + _pnl(baseline['holdout'], '4h'):+.0f}")
    if selected is None:
        print("no candidate met the predeclared training risk constraints")
    else:
        print(f"selected on training: 1d={selected['loss_1d']} 4h={selected['loss_4h']} "
              f"train=${selected['train_pnl_usd']:+.0f}")
        print(f"holdout=${_pnl(selected['holdout'], '1d') + _pnl(selected['holdout'], '4h'):+.0f} "
              f"DD={selected['holdout']['max_dd_pct']:.1%}")
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
