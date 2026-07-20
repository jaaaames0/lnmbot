"""Mock live data source for paper mode and for verification without
LN Markets API credentials.

Replays a parquet file of historical bars in real time (1s between bars)
so the live-paper engine code path can be exercised end-to-end against
the same dataset the backtest uses.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pandas as pd

from ..strategy import Bar
from .source import DataSource


class MockLiveStream(DataSource):
    """Replay a parquet file in wall-clock time. Default 1s per bar so a
    100-bar fixture takes ~100s to stream — adjustable."""

    def __init__(
        self,
        path: str | Path,
        *,
        seconds_per_bar: float = 1.0,
        loop_forever: bool = True,
    ) -> None:
        self.path = Path(path)
        self.seconds_per_bar = seconds_per_bar
        self.loop_forever = loop_forever
        self._bars: list[Bar] = []
        self._loaded = False
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def _load(self) -> None:
        if self._loaded:
            return
        df = pd.read_parquet(self.path).sort_values("ts").reset_index(drop=True)
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")
        self._bars = [
            Bar(
                ts=row.ts.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
            )
            for row in df.itertuples(index=False)
        ]
        self._loaded = True

    async def stream(self) -> AsyncIterator[Bar]:
        self._load()
        while not self._stopped:
            for bar in self._bars:
                if self._stopped:
                    return
                yield bar
                await asyncio.sleep(self.seconds_per_bar)
            if not self.loop_forever:
                return
