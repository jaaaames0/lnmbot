"""Atomic, revision-aware cache for completed Binance perpetual candles."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd  # type: ignore[import-untyped]

from .binance import fetch_klines

if TYPE_CHECKING:
    from pathlib import Path

KLINE_COLUMNS = ("ts", "open", "high", "low", "close", "volume")
INTERVALS = {"4h": timedelta(hours=4), "1d": timedelta(days=1)}
FetchKlines = Callable[..., pd.DataFrame]


def refresh_completed_cache(
    *,
    cache_path: Path,
    symbol: str,
    interval: str,
    bootstrap_start: datetime,
    now: datetime,
    overlap_bars: int = 3,
    fetcher: FetchKlines = fetch_klines,
) -> pd.DataFrame:
    """Refresh a completed-only kline cache and replace it atomically.

    The tail overlap is fetched without passing ``cache_path`` to the generic
    fetcher. This deliberately replaces cached tail rows, including a candle
    that was previously fetched while still forming.
    """
    if interval not in INTERVALS:
        raise ValueError(f"unsupported completed-cache interval: {interval!r}")
    if overlap_bars < 1:
        raise ValueError("overlap_bars must be positive")
    start = _utc(bootstrap_start)
    clock = _utc(now)
    step = INTERVALS[interval]
    expected_last = _floor(clock, step) - step
    if expected_last < start:
        raise ValueError("no completed candle exists after bootstrap start")

    cached = _read_cache(cache_path) if cache_path.exists() else _empty()
    if not cached.empty:
        _validate(cached, step, require_end=None)
        cached = cached[(cached.ts >= start) & (cached.ts <= expected_last)].copy()
    request_start = (
        start if cached.empty else max(start, cached.ts.max().to_pydatetime() - step * overlap_bars)
    )
    fetched = fetcher(
        symbol=symbol,
        interval=interval,
        start=request_start,
        end=clock,
        cache_path=None,
    )
    fetched = _normalize(fetched)
    fetched = fetched[(fetched.ts >= request_start) & (fetched.ts <= expected_last)].copy()
    if fetched.empty:
        raise ValueError("market source returned no completed candles")

    prefix = cached[cached.ts < request_start]
    result = _normalize(pd.concat([prefix, fetched], ignore_index=True))
    result = result[(result.ts >= start) & (result.ts <= expected_last)].reset_index(drop=True)
    _validate(result, step, require_end=expected_last)
    if result.iloc[0].ts.to_pydatetime() != start:
        raise ValueError("completed cache does not reach bootstrap start")
    _atomic_parquet(result, cache_path)
    return result


def _read_cache(path: Path) -> pd.DataFrame:
    try:
        return _normalize(pd.read_parquet(path))
    except Exception as exc:
        raise ValueError(f"cannot read completed candle cache: {path}") from exc


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=KLINE_COLUMNS)


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(KLINE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"market data is missing columns: {sorted(missing)}")
    result = frame.loc[:, KLINE_COLUMNS].copy()
    result["ts"] = pd.to_datetime(result.ts, utc=True)
    for column in KLINE_COLUMNS[1:]:
        result[column] = pd.to_numeric(result[column], errors="raise").astype(float)
    if result.isna().any().any():
        raise ValueError("market data contains null values")
    if result.ts.duplicated().any():
        raise ValueError("market data contains duplicate candles")
    return result.sort_values("ts").reset_index(drop=True)


def _validate(frame: pd.DataFrame, step: timedelta, require_end: datetime | None) -> None:
    if frame.empty:
        raise ValueError("completed candle cache is empty")
    if not frame.ts.diff().dropna().eq(pd.Timedelta(step)).all():
        raise ValueError("completed candle cache has gaps")
    seconds = int(step.total_seconds())
    if any(int(value.timestamp()) % seconds for value in frame.ts.dt.to_pydatetime()):
        raise ValueError("market candle is not aligned to its UTC interval")
    if (frame.loc[:, ["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("market data contains nonpositive prices")
    if (frame.high < frame.loc[:, ["open", "close"]].max(axis=1)).any() or (
        frame.low > frame.loc[:, ["open", "close"]].min(axis=1)
    ).any():
        raise ValueError("market data contains invalid OHLC values")
    if require_end is not None and frame.iloc[-1].ts.to_pydatetime() != require_end:
        raise ValueError("market source is missing the latest completed candle")


def _floor(value: datetime, step: timedelta) -> datetime:
    seconds = int(step.total_seconds())
    return datetime.fromtimestamp(int(value.timestamp()) // seconds * seconds, tz=UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("market-data timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary)
