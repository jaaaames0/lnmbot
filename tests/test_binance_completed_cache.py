from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from lnmarkets_bot.data.binance_cache import refresh_completed_cache

START = datetime(2026, 1, 1, tzinfo=UTC)


def rows(start: datetime, count: int, *, revised: dict[int, float] | None = None) -> pd.DataFrame:
    result = []
    revised = revised or {}
    for index in range(count):
        price = revised.get(index, 100.0 + index)
        result.append(
            {
                "ts": start + timedelta(days=index),
                "open": price,
                "high": price + 2,
                "low": price - 2,
                "close": price + 1,
                "volume": 10.0,
            }
        )
    return pd.DataFrame(result)


def test_refresh_replaces_overlap_drops_forming_and_writes_atomically(tmp_path):
    cache = tmp_path / "daily.parquet"
    # The old tail is deliberately wrong, as if cached while forming.
    old = rows(START, 4, revised={3: 50.0})
    old.to_parquet(cache, index=False)
    calls = []

    def fetcher(**kwargs):
        calls.append(kwargs)
        # Requested overlap starts at Jan 1 here (max(start, Jan 4 - 3d)).
        # Jan 6 is forming at the supplied clock and must not be persisted.
        return rows(START, 6, revised={3: 103.0})

    result = refresh_completed_cache(
        cache_path=cache,
        symbol="BTCUSDT",
        interval="1d",
        bootstrap_start=START,
        now=datetime(2026, 1, 6, 0, 10, tzinfo=UTC),
        fetcher=fetcher,
    )
    assert calls[0]["cache_path"] is None
    assert result.ts.tolist() == [START + timedelta(days=i) for i in range(5)]
    assert result.loc[result.ts == START + timedelta(days=3), "open"].item() == 103.0
    pd.testing.assert_frame_equal(pd.read_parquet(cache), result)
    assert list(tmp_path.glob(".daily.parquet.*.tmp")) == []


def test_failed_refresh_preserves_previous_cache(tmp_path):
    cache = tmp_path / "daily.parquet"
    old = rows(START, 4)
    old.to_parquet(cache, index=False)
    before = cache.read_bytes()

    def incomplete(**kwargs):
        return rows(START, 3)

    with pytest.raises(ValueError, match="latest completed"):
        refresh_completed_cache(
            cache_path=cache,
            symbol="BTCUSDT",
            interval="1d",
            bootstrap_start=START,
            now=datetime(2026, 1, 6, 0, 10, tzinfo=UTC),
            fetcher=incomplete,
        )
    assert cache.read_bytes() == before


@pytest.mark.parametrize("problem", ["gap", "duplicate", "bad_ohlc"])
def test_invalid_source_never_replaces_cache(tmp_path, problem):
    cache = tmp_path / "daily.parquet"
    old = rows(START, 2)
    old.to_parquet(cache, index=False)
    before = cache.read_bytes()

    def invalid(**kwargs):
        frame = rows(START, 5)
        if problem == "gap":
            return frame.drop(index=2)
        if problem == "duplicate":
            return pd.concat([frame, frame.iloc[[2]]], ignore_index=True)
        frame.loc[2, "high"] = frame.loc[2, "close"] - 1
        return frame

    with pytest.raises(ValueError):
        refresh_completed_cache(
            cache_path=cache,
            symbol="BTCUSDT",
            interval="1d",
            bootstrap_start=START,
            now=datetime(2026, 1, 6, 0, 10, tzinfo=UTC),
            fetcher=invalid,
        )
    assert cache.read_bytes() == before


def test_requires_timezone_and_completed_range(tmp_path):
    with pytest.raises(ValueError, match="timezone"):
        refresh_completed_cache(
            cache_path=tmp_path / "x.parquet",
            symbol="BTCUSDT",
            interval="1d",
            bootstrap_start=datetime(2026, 1, 1),
            now=datetime(2026, 1, 2, tzinfo=UTC),
            fetcher=lambda **kwargs: rows(START, 1),
        )
    with pytest.raises(ValueError, match="no completed"):
        refresh_completed_cache(
            cache_path=tmp_path / "x.parquet",
            symbol="BTCUSDT",
            interval="1d",
            bootstrap_start=START,
            now=START + timedelta(hours=12),
            fetcher=lambda **kwargs: rows(START, 1),
        )
