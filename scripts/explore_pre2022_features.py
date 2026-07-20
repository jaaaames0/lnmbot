#!/usr/bin/env python3
"""Data-informed event study restricted to the pre-July-2022 dataset.

This script does not run a trading strategy or optimise P&L.  It records the
complete feature universe examined and measures signed forward returns after
raw MA verdict transitions across three chronological development windows.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

CUTOFF = pd.Timestamp("2022-07-11T00:00:00Z")
WINDOWS = (
    ("2019-20", pd.Timestamp("2019-09-09T00:00:00Z"), pd.Timestamp("2020-07-11T00:00:00Z")),
    ("2020-21", pd.Timestamp("2020-07-11T00:00:00Z"), pd.Timestamp("2021-07-11T00:00:00Z")),
    ("2021-22", pd.Timestamp("2021-07-11T00:00:00Z"), CUTOFF),
)
HORIZONS = {"1d": (1, 3, 7, 14), "4h": (1, 6, 18, 42)}
FEATURES = (
    "slope_aligned",
    "daily_aligned",
    "breakout_20",
    "volatility_regime",
    "impulse_atr",
    "distance_band",
    "prior_return_aligned",
)


def _load(path: Path, timeframe: str) -> pd.DataFrame:
    df = pd.read_parquet(path).sort_values("ts").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["close_ts"] = df["ts"] + pd.Timedelta(
        days=1 if timeframe == "1d" else 0, hours=4 if timeframe == "4h" else 0
    )
    df = df[df["close_ts"] <= CUTOFF].reset_index(drop=True)
    close = df["close"]
    df["sma20"] = close.rolling(20).mean()
    # Match MaCross exactly: seed EMA21 with the first SMA(21), then recurse
    # with alpha=2/22. Pandas' default EWM starts from the first observation
    # and produces slightly different early verdict transitions.
    ema = pd.Series(np.nan, index=df.index, dtype=float)
    if len(df) >= 21:
        ema.iloc[20] = close.iloc[:21].mean()
        alpha = 2 / 22
        for index in range(21, len(df)):
            ema.iloc[index] = close.iloc[index] * alpha + ema.iloc[index - 1] * (1 - alpha)
    df["ema21"] = ema
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - close.shift()).abs(),
            (df["low"] - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr20_pct"] = true_range.rolling(20).mean() / close
    df["atr_ratio"] = df["atr20_pct"] / df["atr20_pct"].rolling(100).median()
    df["bar_return"] = close.pct_change()
    df["prior_return_5"] = close.pct_change(5)
    df["prior_high_20"] = df["high"].shift().rolling(20).max()
    df["prior_low_20"] = df["low"].shift().rolling(20).min()
    up = (close > df["sma20"] * 1.005) & (close > df["ema21"] * 1.005)
    down = (close < df["sma20"] * 0.995) & (close < df["ema21"] * 0.995)
    df["direction"] = np.select([up, down], [1, -1], default=0)
    df["transition"] = (df["direction"] != 0) & (df["direction"] != df["direction"].shift())
    return df


def _add_features(df: pd.DataFrame, timeframe: str, daily: pd.DataFrame) -> pd.DataFrame:
    events = df[df["transition"]].copy()
    direction = events["direction"]
    slope_sma = df["sma20"].pct_change(5).reindex(events.index)
    slope_ema = df["ema21"].pct_change(5).reindex(events.index)
    events["slope_aligned"] = ((direction * slope_sma > 0) & (direction * slope_ema > 0)).map(
        {True: "yes", False: "no"}
    )
    events["breakout_20"] = np.where(
        ((direction > 0) & (events["close"] > events["prior_high_20"]))
        | ((direction < 0) & (events["close"] < events["prior_low_20"])),
        "yes",
        "no",
    )
    events["volatility_regime"] = pd.cut(
        events["atr_ratio"],
        [-np.inf, 0.8, 1.2, np.inf],
        labels=["compressed", "normal", "expanded"],
    ).astype(str)
    impulse = direction * events["bar_return"] / events["atr20_pct"]
    events["impulse_atr"] = pd.cut(
        impulse, [-np.inf, 0.5, 1.0, np.inf], labels=["small", "medium", "large"]
    ).astype(str)
    midpoint = (events["sma20"] + events["ema21"]) / 2
    distance = direction * (events["close"] / midpoint - 1)
    events["distance_band"] = pd.cut(
        distance, [-np.inf, 0.01, 0.02, np.inf], labels=["0.5-1pct", "1-2pct", "over-2pct"]
    ).astype(str)
    events["prior_return_aligned"] = np.where(direction * events["prior_return_5"] > 0, "yes", "no")
    if timeframe == "4h":
        daily_state = daily[["close_ts", "direction"]].rename(
            columns={"direction": "daily_direction"}
        )
        events = pd.merge_asof(
            events.sort_values("close_ts"),
            daily_state.sort_values("close_ts"),
            on="close_ts",
            direction="backward",
        )
        events["daily_aligned"] = np.where(
            events["direction"] == events["daily_direction"], "yes", "no"
        )
    else:
        events["daily_aligned"] = "self"
    for horizon in HORIZONS[timeframe]:
        future = df["close"].shift(-horizon).reindex(events.index)
        # merge_asof resets the 4h event index; recover by timestamp there.
        if timeframe == "4h":
            future_by_ts = df.set_index("close_ts")["close"].shift(-horizon)
            future = events["close_ts"].map(future_by_ts)
        events[f"forward_{horizon}"] = events["direction"] * (future / events["close"] - 1)
    return events


def _stats(values: pd.Series) -> dict[str, Any]:
    clean = values.dropna()
    return {
        "n": len(clean),
        "mean": float(clean.mean()) if len(clean) else None,
        "median": float(clean.median()) if len(clean) else None,
        "positive_rate": float((clean > 0).mean()) if len(clean) else None,
    }


def explore(data_1d: Path, data_4h: Path) -> dict[str, Any]:
    daily = _load(data_1d, "1d")
    frames = {"1d": daily, "4h": _load(data_4h, "4h")}
    report: dict[str, Any] = {
        "restriction": f"No bar closing after {CUTOFF.isoformat()} was loaded.",
        "search_universe": {"features": FEATURES, "horizons": HORIZONS},
        "timeframes": {},
    }
    for timeframe, frame in frames.items():
        events = _add_features(frame, timeframe, daily)
        tf_report: dict[str, Any] = {"events": len(events), "features": {}}
        for feature in FEATURES:
            feature_rows: dict[str, Any] = {}
            for value, subset in events.groupby(feature, dropna=False):
                window_rows = {}
                for name, start, end in WINDOWS:
                    period = subset[(subset["close_ts"] >= start) & (subset["close_ts"] < end)]
                    window_rows[name] = {
                        str(horizon): _stats(period[f"forward_{horizon}"])
                        for horizon in HORIZONS[timeframe]
                    }
                feature_rows[str(value)] = window_rows
            tf_report["features"][feature] = feature_rows
        report["timeframes"][timeframe] = tf_report
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-1d", type=Path, required=True)
    parser.add_argument("--data-4h", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()
    report = explore(args.data_1d, args.data_4h)
    args.json_out.write_text(json.dumps(report, indent=2) + "\n")
    print(
        f"wrote {args.json_out}; "
        + ", ".join(f"{tf}={row['events']} events" for tf, row in report["timeframes"].items())
    )


if __name__ == "__main__":
    main()
