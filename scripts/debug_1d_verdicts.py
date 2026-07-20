"""Standalone repro of the strategy's 1d verdict computation.

Doesn't touch the engine — just replays 1m bars from the parquet, resamples to 1d,
and runs the same SMA/EMA/verdict logic the strategy uses. Compares with what the
recorded signals actually showed.
"""
from __future__ import annotations

import sys
from collections import deque
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

TOL = 0.002


def ema_seed_then_iterate(closes: deque, alpha: float = 2.0 / 22.0):
    """Returns a list of EMA values, one per close, seeded with SMA(21) on first."""
    closes_list = list(closes)
    if len(closes_list) < 21:
        return [None] * len(closes_list)
    ema = sum(closes_list[:21]) / 21.0
    out = [None] * 20 + [ema]  # first 20: no ema, 21st: seeded
    for c in closes_list[21:]:
        ema = c * alpha + ema * (1 - alpha)
        out.append(ema)
    return out


def main() -> None:
    parquet = Path("/home/james/srv/tradingbot/data/cache/btcusdt_perp_1m_6m.parquet")
    df = pd.read_parquet(parquet)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts")

    # Resample to 1d — use the SAME convention as MultiTimeframeDataSource.
    daily = df.resample("1D", label="right", closed="left").agg(
        close=("close", "last")
    ).dropna(subset=["close"])
    # `daily.index` is the right edge of each bucket.
    print(f"1d bucket count: {len(daily)}, range: {daily.index.min()} → {daily.index.max()}")
    print()

    closes = deque(maxlen=512)
    closes.extend(daily["close"].tolist())

    ema_values = ema_seed_then_iterate(closes)

    sma20 = daily["close"].rolling(20).mean()

    print(f"{'bucket_end_ts':22} {'close':>9} {'sma20':>9} {'ema21':>9} {'verdict':>10} {'prev_verdict':>11} {'transition?'}")
    prev_verdict = "FLAT"
    for i, (ts, close_val) in enumerate(zip(daily.index, daily["close"])):
        if i < 20:
            continue  # warmup
        sma = sma20.iloc[i]
        ema = ema_values[i]
        if sma is None or pd.isna(sma) or ema is None:
            continue
        if close_val > sma * (1 + TOL) and close_val > ema * (1 + TOL):
            verdict = "UP_TRUE"
        elif close_val < sma * (1 - TOL) and close_val < ema * (1 - TOL):
            verdict = "DOWN_TRUE"
        else:
            verdict = "FLAT"
        trans = "*" if verdict != prev_verdict and verdict != "FLAT" else ""
        if trans or verdict != prev_verdict:
            print(f"{ts.strftime('%Y-%m-%d %H:%M'):22} {close_val:9.1f} {sma:9.1f} {ema:9.1f} {verdict:>10} {prev_verdict:>11} {trans}")
        prev_verdict = verdict

    # Count transitions in 1d verdict
    prev_verdict = "FLAT"
    transitions = {"DOWN": 0, "UP": 0, "FLAT": 0}
    for i, close_val in enumerate(daily["close"]):
        if i < 20:
            continue
        sma = sma20.iloc[i]
        ema = ema_values[i]
        if sma is None or pd.isna(sma) or ema is None:
            continue
        if close_val > sma * (1 + TOL) and close_val > ema * (1 + TOL):
            verdict = "UP_TRUE"
        elif close_val < sma * (1 - TOL) and close_val < ema * (1 - TOL):
            verdict = "DOWN_TRUE"
        else:
            verdict = "FLAT"
        if verdict != prev_verdict:
            transitions[verdict.replace("_TRUE", "")] = transitions.get(verdict.replace("_TRUE", ""), 0) + 1
        prev_verdict = verdict
    print()
    print(f"Total non-FLAT transitions: {transitions}")


if __name__ == "__main__":
    main()