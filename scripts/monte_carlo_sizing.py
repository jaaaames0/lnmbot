#!/usr/bin/env python3
"""Block-bootstrap sizing scenarios from the locked portfolio's daily P&L.

This is a decision aid, not a forecast.  It resamples contiguous 30-day blocks
of the observed strategy P&L to retain some trend/chop clustering.  It does
not create price paths or model a liquidation; the companion wick audit must
be used to choose leverage before interpreting these results.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np


def _summarise(equity: np.ndarray, initial_equity: float) -> dict[str, float]:
    peaks = np.maximum.accumulate(equity, axis=1)
    drawdowns = 1.0 - equity / np.maximum(peaks, 1e-9)
    max_dd = drawdowns.max(axis=1)
    ending = equity[:, -1]
    return {
        "median_ending_equity_usd": float(np.median(ending)),
        "p05_ending_equity_usd": float(np.quantile(ending, 0.05)),
        "p95_ending_equity_usd": float(np.quantile(ending, 0.95)),
        "median_max_drawdown_pct": float(np.median(max_dd)),
        "p95_max_drawdown_pct": float(np.quantile(max_dd, 0.95)),
        "probability_max_drawdown_over_25pct": float(np.mean(max_dd > 0.25)),
        "probability_max_drawdown_over_50pct": float(np.mean(max_dd > 0.50)),
        "probability_ending_below_start": float(np.mean(ending < initial_equity)),
        "probability_equity_non_positive": float(np.mean(np.any(equity <= 0, axis=1))),
    }


def _bootstrap_paths(
    pnl_1d: np.ndarray,
    pnl_4h: np.ndarray,
    *,
    paths: int,
    block_days: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Sample contiguous daily blocks with replacement, preserving TF coupling."""
    if block_days > len(pnl_1d):
        raise ValueError("block_days exceeds available history")
    rng = np.random.default_rng(seed)
    horizon = len(pnl_1d)
    out = [np.empty((paths, horizon), dtype=float) for _ in range(2)]
    write = 0
    while write < horizon:
        length = min(block_days, horizon - write)
        starts = rng.integers(0, len(pnl_1d) - length + 1, size=paths)
        indices = starts[:, None] + np.arange(length)
        for destination, source in zip(out, (pnl_1d, pnl_4h), strict=True):
            destination[:, write : write + length] = source[indices]
        write += length
    return tuple(out)  # type: ignore[return-value]


def simulate(
    history: dict[str, Any],
    *,
    initial_equity: float,
    paths: int,
    block_days: int,
    seed: int,
    funding_bps_per_8h: float,
) -> dict[str, Any]:
    rows = history["daily"]
    # Per-$1,000-notional daily P&L, after simulated trading fees/slippage.
    unit_1d = np.array([float(row["pnl_1d_usd"]) / 1000.0 for row in rows])
    unit_4h = np.array([float(row["pnl_4h_usd"]) / 1000.0 for row in rows])
    active_1d = np.array([float(bool(row["active_1d"])) for row in rows])
    active_4h = np.array([float(bool(row["active_4h"])) for row in rows])
    # A paid funding rate applies three times daily only when the respective
    # position is open.  This is intentionally one-sided stress, not expected
    # funding; received funding would improve the paths.
    funding_daily = funding_bps_per_8h / 10_000.0 * 3.0
    unit_1d -= active_1d * funding_daily
    unit_4h -= active_4h * funding_daily
    p1, p4 = _bootstrap_paths(
        unit_1d,
        unit_4h,
        paths=paths,
        block_days=block_days,
        seed=seed,
    )

    scenarios: list[dict[str, Any]] = []
    for per_tf_notional in (125.0, 200.0, 300.0, 400.0):
        daily_pnl = (p1 + p4) * per_tf_notional
        equity = initial_equity + np.cumsum(daily_pnl, axis=1)
        scenarios.append(
            {
                "mode": "fixed_notional",
                "per_timeframe_notional_usd": per_tf_notional,
                "combined_margin_at_5x_usd": 2 * per_tf_notional / 5.0,
                **_summarise(equity, initial_equity),
            }
        )

    for margin_fraction in (0.10, 0.15, 0.20, 0.25, 0.30, 0.50):
        # This mirrors the current policy: N_tf = equity x total-margin
        # fraction x TF weight (50%) x haircut (95%) x leverage (5x).
        equity = np.full(paths, initial_equity, dtype=float)
        path = np.empty((paths, p1.shape[1]), dtype=float)
        notional_multiplier = margin_fraction * 0.5 * 0.95 * 5.0
        for day in range(p1.shape[1]):
            equity += equity * notional_multiplier * (p1[:, day] + p4[:, day])
            path[:, day] = equity
        scenarios.append(
            {
                "mode": "equity_fraction_approximation",
                "total_margin_fraction": margin_fraction,
                "equity_haircut": 0.95,
                "leverage": 5.0,
                "notional_per_timeframe_when_open_pct_of_equity": notional_multiplier,
                "combined_margin_when_both_open_pct_of_equity": margin_fraction * 0.95,
                **_summarise(path, initial_equity),
            }
        )

    return {
        "method": {
            "paths": paths,
            "block_days": block_days,
            "seed": seed,
            "initial_equity_usd": initial_equity,
            "funding_stress_bps_per_8h_when_position_pays": funding_bps_per_8h,
            "limitations": [
                "Resampling cannot create unobserved tail events or exchange outages.",
                "Equity-fraction paths rebalance daily as an approximation; the live bot sizes only at entry from settled balance.",
                "No liquidation is simulated here; consult the historical wick audit separately.",
            ],
        },
        "historical_liquidation_wick_audit": history["liquidation_wick_audit"],
        "scenarios": scenarios,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--initial-equity", type=float, default=329.0)
    parser.add_argument("--paths", type=int, default=10_000)
    parser.add_argument("--block-days", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260716)
    parser.add_argument("--funding-bps-per-8h", type=float, default=1.0)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = simulate(
        json.loads(args.history.read_text()),
        initial_equity=args.initial_equity,
        paths=args.paths,
        block_days=args.block_days,
        seed=args.seed,
        funding_bps_per_8h=args.funding_bps_per_8h,
    )
    args.json_out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"wrote {args.json_out}; {len(report['scenarios'])} scenarios")


if __name__ == "__main__":
    main()
