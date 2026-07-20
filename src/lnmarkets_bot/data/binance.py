"""Binance USDT-margined perpetual public kline fetcher.

Used only for backtest data acquisition. LNM has its own history endpoint; we
chose Binance as the canonical deep-history source per the v0 decision.

Public endpoint:
    GET https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&startTime=...&endTime=...
Returns up to 1000 candles per call.

Caches to parquet for fast replay.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


_BINANCE_FUTURES_BASE = "https://fapi.binance.com"
_INTERVAL_TO_MS = {
    "1m": 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


class BinanceKlineFetchError(RuntimeError):
    pass


def fetch_klines(
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    *,
    cache_path: Path | None = None,
    on_progress: Callable[[int, datetime], None] | None = None,
) -> pd.DataFrame:
    """Fetch and cache Binance perpetual klines. Returns a DataFrame with columns:
    ts, open, high, low, close, volume.

    `start`/`end` are naive UTC datetimes. Caller's responsibility to UTC-tz them.
    """
    if interval not in _INTERVAL_TO_MS:
        raise ValueError(f"unsupported interval: {interval!r}")
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be tz-aware UTC")

    if cache_path and cache_path.exists():
        # Respect any cached range; fetch only the uncovered tail.
        cached = pd.read_parquet(cache_path)
        cached_start = cached["ts"].min().to_pydatetime()
        cached_end = cached["ts"].max().to_pydatetime()
        # We may need to extend at the front and/or the back.
        frames = [cached]
        if start < cached_start:
            frames.insert(
                0,
                _fetch_range(
                    symbol,
                    interval,
                    start,
                    cached_start - timedelta(milliseconds=_INTERVAL_TO_MS[interval]),
                    on_progress,
                ),
            )
        if end > cached_end:
            frames.append(
                _fetch_range(
                    symbol,
                    interval,
                    cached_end + timedelta(milliseconds=_INTERVAL_TO_MS[interval]),
                    end,
                    on_progress,
                )
            )
        if len(frames) > 1:
            df = (
                pd.concat(frames, ignore_index=True)
                .drop_duplicates(subset=["ts"])
                .sort_values("ts")
            )
            df = _normalize(df)
            df.to_parquet(cache_path, index=False)
        else:
            df = _normalize(cached)
        return df

    df = _fetch_range(symbol, interval, start, end, on_progress)
    df = _normalize(df)
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.sort_values("ts").reset_index(drop=True)


def _fetch_range(
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    on_progress: Callable[[int, datetime], None] | None = None,
) -> pd.DataFrame:
    interval_ms = _INTERVAL_TO_MS[interval]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }
    rows: list[dict[str, object]] = []
    url = f"{_BINANCE_FUTURES_BASE}/fapi/v1/klines"
    with httpx.Client(timeout=30.0) as client:
        requests = 0
        while True:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            requests += 1
            batch = resp.json()
            if not batch:
                break
            for k in batch:
                rows.append(
                    {
                        "ts": datetime.fromtimestamp(k[0] / 1000.0, tz=UTC),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                    }
                )
            last_open_ms = int(batch[-1][0])
            if on_progress is not None and requests % 100 == 0:
                on_progress(len(rows), datetime.fromtimestamp(last_open_ms / 1000.0, tz=UTC))
            next_start = last_open_ms + interval_ms
            if next_start > end_ms or len(batch) < params["limit"]:
                break
            params["startTime"] = next_start
            time.sleep(0.05)  # rate-limit politeness — Binance is generous but not infinite
    if not rows:
        raise BinanceKlineFetchError(f"no klines returned for {symbol} {interval} {start}..{end}")
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
