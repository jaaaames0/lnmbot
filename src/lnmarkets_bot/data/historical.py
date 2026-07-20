"""Backtest data source — replays a parquet file of historical bars.

Parquet schema (see scripts/backfill_binance.py):
    columns: ts (datetime64[ns, UTC]), open, high, low, close, volume

Replays asynchronously so the engine code path is the same shape as the live
stream. Cadence is configurable: faster-than-realtime (or instant) for a quick
run, `realtime` for a wall-clock-paced run (useful for the paper-mode mock).
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pandas as pd

from ..strategy import Bar
from .source import DataSource

_REPLAY_MODES = ("realtime", "fast", "instant")


class BacktestReplay(DataSource):
    """Reads a parquet of OHLCV bars and yields them in order."""

    def __init__(
        self,
        path: str | Path,
        *,
        cadence: str = "fast",
        symbol: str = "BTCUSDT",
    ) -> None:
        if cadence not in _REPLAY_MODES:
            raise ValueError(f"cadence must be one of {_REPLAY_MODES}, got {cadence!r}")
        self.path = Path(path)
        self.cadence = cadence
        self.symbol = symbol
        self._bars: list[Bar] = []
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        df = pd.read_parquet(self.path)
        if "ts" not in df.columns:
            raise ValueError(f"parquet {self.path} must have a 'ts' column")
        df = df.sort_values("ts").reset_index(drop=True)
        # Coerce timestamp to UTC; parquet may store as ns-epoch
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")
        bars: list[Bar] = []
        for row in df.itertuples(index=False):
            bars.append(
                Bar(
                    ts=row.ts.to_pydatetime(),
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                )
            )
        self._bars = bars
        self._loaded = True

    async def stream(self) -> AsyncIterator[Bar]:
        self._load()
        if self.cadence == "instant":
            for bar in self._bars:
                yield bar
            return
        if self.cadence == "fast":
            # 1000x faster than realtime — still keeps timing-based logic separable but runs in seconds
            interval = 0.001
        else:  # realtime
            interval = 60.0  # 1m bars
        prev_ts = None
        for bar in self._bars:
            if prev_ts is not None and self.cadence == "realtime":
                delta = (bar.ts - prev_ts).total_seconds()
                if delta > 0:
                    await asyncio.sleep(delta)
            elif prev_ts is not None and self.cadence == "fast":
                delta = (bar.ts - prev_ts).total_seconds()
                if delta > 0:
                    await asyncio.sleep(min(delta, interval) / 1000.0)
            yield bar
            prev_ts = bar.ts

    def __len__(self) -> int:  # pragma: no cover (testing convenience)
        self._load()
        return len(self._bars)
