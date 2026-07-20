"""Multi-timeframe data source.

Consumes a small-TF (e.g. 1m) `BacktestReplay` and synthesizes higher-TF bars
from the underlying 1m bars via pandas resample. Emits a stream where:

  - at each daily boundary  → 1d bar (tagged "1d"), then 4h bar (tagged "4h"),
                                then the 1m bar
  - at each 5m boundary       → 5m bar, then the 1m bar (when 5m is enabled)
  - at each 4h boundary       → 4h bar, then the 1m bar
  - at each 1h boundary       → 1h bar, then the 1m bar (when 1h is enabled)
  - otherwise                 → 1m bar only

Tags are carried on `Bar.timeframe`. Architecture rule preserved: this lives
in `data/` (a producer) and the engine/strategy treat its stream uniformly.

The 1m bar's ts is the bucket boundary; the 1h/4h/1d bar's ts is *also* that
boundary timestamp (UTC-aligned). The aggregation uses `pd.DataFrame.resample`
with the convention "label='right', closed='right'" so a bar at 12:00 belongs
to the bucket 08:00-12:00 (4h) -- that bar's close IS the close we'd see at
12:00:00:00.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import pandas as pd

from ..strategy import Bar
from .source import DataSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@dataclass(frozen=True)
class _TfSpec:
    timeframe: str
    freq: str  # pandas resample frequency string


_TF_SPECS: dict[str, _TfSpec] = {
    "5m": _TfSpec("5m", "5min"),
    "1h": _TfSpec("1h", "1h"),
    "4h": _TfSpec("4h", "4h"),
    "1d": _TfSpec("1d", "1D"),
}


class MultiTimeframeDataSource(DataSource):
    """Wraps a small-TF BacktestReplay and emits higher-TF bars at boundaries."""

    def __init__(
        self,
        base: DataSource,
        *,
        higher_timeframes: tuple[str, ...] = ("1d", "4h", "1h"),
    ) -> None:
        if not higher_timeframes:
            raise ValueError("higher_timeframes must be non-empty")
        for tf in higher_timeframes:
            if tf not in _TF_SPECS:
                raise ValueError(f"unsupported timeframe {tf!r}; supported: {sorted(_TF_SPECS)}")
        # Sort descending so largest TF is emitted first at each boundary.
        self._tfs_desc: tuple[str, ...] = tuple(
            sorted(
                higher_timeframes, key=lambda t: -pd.Timedelta(_TF_SPECS[t].freq).total_seconds()
            )
        )
        self.base = base
        self._cache: dict[str, list[Bar]] = {}

    def _load_base(self) -> list[Bar]:
        """Force-load the underlying 1m bars into memory if not already done."""
        if hasattr(self.base, "_load"):
            base_obj = self.base  # type: ignore[attr-defined]
            base_obj._load()  # type: ignore[attr-defined]
            return base_obj._bars  # type: ignore[attr-defined]
        # Fall back: read the parquet directly via the public path.
        # (BacktestReplay's _load is a private, but stable, internal API.)
        if hasattr(self.base, "path"):
            df = pd.read_parquet(self.base.path)  # type: ignore[attr-defined]
            df = df.sort_values("ts")
            if df["ts"].dt.tz is None:
                df["ts"] = df["ts"].dt.tz_localize("UTC")
            else:
                df["ts"] = df["ts"].dt.tz_convert("UTC")
            return [
                Bar(
                    ts=row.ts.to_pydatetime(),
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                    timeframe="1m",
                )
                for row in df.itertuples(index=False)
            ]
        raise RuntimeError("base data source must expose .path or _load() for v0")

    def _derive(self, timeframe: str, bars: list[Bar]) -> list[Bar]:
        if timeframe in self._cache:
            return self._cache[timeframe]
        if not bars:
            self._cache[timeframe] = []
            return []
        spec = _TF_SPECS[timeframe]
        # Build a DataFrame directly from pandas-aware objects so resample works.
        idx = pd.DatetimeIndex([b.ts for b in bars], tz="UTC")
        df = pd.DataFrame(
            {
                "open": [b.open for b in bars],
                "high": [b.high for b in bars],
                "low": [b.low for b in bars],
                "close": [b.close for b in bars],
                "volume": [b.volume for b in bars],
            },
            index=idx,
        )
        # Convention: a bar at timestamp T (openTime) represents the interval
        # [T, T+freq). For 4h aggregation, the bucket [08:00, 12:00) contains
        # bars at 08:00, 08:01, ..., 11:59. The aggregate label is the bucket's
        # right edge (12:00) — emitted when the first 1m bar of the next bucket
        # arrives (i.e., at 12:00).
        agg = (
            df.resample(spec.freq, label="right", closed="left")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna(subset=["open"])
        )
        derived = [
            Bar(
                ts=ts.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                timeframe=timeframe,
            )
            for ts, row in agg.iterrows()
        ]
        self._cache[timeframe] = derived
        return derived

    async def stream(self) -> AsyncIterator[Bar]:
        # Historical replays can be preloaded and resampled in bulk. A real
        # live source cannot: it has no finite path or `_load` method, so it
        # must be aggregated incrementally as new 1m bars arrive.
        if not (hasattr(self.base, "_load") or hasattr(self.base, "path")):
            async for bar in self._stream_incremental():
                yield bar
            return

        # Force-load base once (sync, blocking) so we can index higher TFs.
        bars = await asyncio.to_thread(self._load_base)
        derived: dict[str, list[Bar]] = {}
        for tf in self._tfs_desc:
            derived[tf] = await asyncio.to_thread(self._derive, tf, bars)
        # Index of next higher-TF bar to emit. We use a per-tf index.
        idx: dict[str, int] = {tf: 0 for tf in self._tfs_desc}
        # Cadence: fast (used during backtest)
        interval = 0.001
        prev_ts = None
        for bar in bars:
            # Determine which higher-TF bars close at this small-bar ts.
            for tf in self._tfs_desc:
                cur_idx = idx[tf]
                cur_list = derived[tf]
                # Seek past any already-emitted higher-TF bars whose ts < bar.ts
                while cur_idx < len(cur_list) and cur_list[cur_idx].ts < bar.ts:
                    cur_idx += 1
                # Any higher-TF bar whose ts == bar.ts is closed at this moment.
                while cur_idx < len(cur_list) and cur_list[cur_idx].ts == bar.ts:
                    yield cur_list[cur_idx]
                    cur_idx += 1
                idx[tf] = cur_idx
            yield bar
            if prev_ts is not None:
                delta = (bar.ts - prev_ts).total_seconds()
                if delta > 0 and interval > 0:
                    await asyncio.sleep(min(delta, interval) / 1000.0)
            prev_ts = bar.ts

    async def _stream_incremental(self) -> AsyncIterator[Bar]:
        """Aggregate a never-ending 1m stream without preloading it."""
        bucket_start: dict[str, object] = {}
        bucket_bars: dict[str, list[Bar]] = {tf: [] for tf in self._tfs_desc}

        async for bar in self.base.stream():
            if bar.timeframe != "1m":
                yield bar
                continue
            completed: list[Bar] = []
            for tf in self._tfs_desc:
                start = self._bucket_start(bar.ts, tf)
                previous = bucket_start.get(tf)
                if previous is not None and start != previous and bucket_bars[tf]:
                    completed.append(self._aggregate_bucket(tf, bucket_bars[tf], previous))
                    bucket_bars[tf] = []
                bucket_start[tf] = start
                bucket_bars[tf].append(bar)
                # `bar.ts` is the start of a closed 1m candle. Once its end
                # reaches a higher-TF boundary, the bucket is complete; do
                # not wait for the following 1m candle merely to discover
                # that fact. Waiting caused every higher-TF signal to lag by
                # roughly one minute.
                if self._bar_closes_bucket(bar, start, tf):
                    completed.append(self._aggregate_bucket(tf, bucket_bars[tf], start))
                    bucket_bars[tf] = []
            yield bar
            for higher_bar in completed:
                yield higher_bar

    @staticmethod
    def _bucket_start(ts, timeframe: str):
        if timeframe == "1d":
            return ts.replace(hour=0, minute=0, second=0, microsecond=0)
        if timeframe.endswith("m"):
            minutes = int(timeframe.removesuffix("m"))
            return ts.replace(minute=ts.minute - (ts.minute % minutes), second=0, microsecond=0)
        hours = int(timeframe.removesuffix("h"))
        return ts.replace(
            hour=ts.hour - (ts.hour % hours),
            minute=0,
            second=0,
            microsecond=0,
        )

    @staticmethod
    def _bar_closes_bucket(bar: Bar, start, timeframe: str) -> bool:
        return bar.ts + timedelta(minutes=1) == MultiTimeframeDataSource._bucket_end(
            start, timeframe
        )

    @staticmethod
    def _bucket_end(start, timeframe: str):
        if timeframe == "1d":
            return start + timedelta(days=1)
        if timeframe.endswith("m"):
            return start + timedelta(minutes=int(timeframe.removesuffix("m")))
        return start + timedelta(hours=int(timeframe.removesuffix("h")))

    @staticmethod
    def _aggregate_bucket(timeframe: str, bars: list[Bar], start) -> Bar:
        return Bar(
            ts=MultiTimeframeDataSource._bucket_end(start, timeframe),
            open=bars[0].open,
            high=max(bar.high for bar in bars),
            low=min(bar.low for bar in bars),
            close=bars[-1].close,
            volume=sum(bar.volume for bar in bars),
            timeframe=timeframe,
            warmup=all(bar.warmup for bar in bars),
        )


__all__ = ["MultiTimeframeDataSource"]
