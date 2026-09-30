"""Live-candle bootstrap tests without an LN Markets network connection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from lnmarkets_bot.data.live import LnmLiveStream


class _FailingClient:
    async def iter_list(self, path, *, params):
        raise httpx.ConnectError("DNS resolution failed")
        yield  # pragma: no cover


@pytest.mark.asyncio
async def test_warmup_network_failure_is_not_masked_by_logging():
    stream = LnmLiveStream(_FailingClient())

    with pytest.raises(
        RuntimeError, match="unable to load live strategy warmup candles"
    ) as exc_info:
        await anext(stream.stream())

    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)


class _DescendingCatchupClient:
    def __init__(self, base: datetime) -> None:
        self.base = base
        self.calls = 0

    async def iter_list(self, path, *, params):
        self.calls += 1
        minutes = [-5] if self.calls == 1 else [-2, -3]
        for minute in minutes:
            ts = self.base + timedelta(minutes=minute)
            yield {
                "time": ts.isoformat(),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100 + minute,
                "volume": 1,
            }


@pytest.mark.asyncio
async def test_live_catchup_sorts_descending_api_rows_before_advancing_cursor():
    base = datetime.now(UTC).replace(second=0, microsecond=0)
    stream = LnmLiveStream(_DescendingCatchupClient(base), poll_seconds=0)
    iterator = stream.stream()

    warmup = await anext(iterator)
    first_catchup = await anext(iterator)
    second_catchup = await anext(iterator)
    await iterator.aclose()

    assert warmup.ts == base - timedelta(minutes=5)
    assert first_catchup.ts == base - timedelta(minutes=3)
    assert second_catchup.ts == base - timedelta(minutes=2)


@pytest.mark.asyncio
async def test_live_gap_backfill_recovers_evidence_before_aggregation():
    start = datetime(2026, 9, 1, tzinfo=UTC)

    def candle(i):
        return {
            "time": (start + timedelta(minutes=i)).isoformat(),
            "open": 100,
            "high": 100,
            "low": 100,
            "close": 100,
        }

    class Market:
        async def iter_candles(self, *_args, **_kwargs):
            yield candle(1)

    stream = LnmLiveStream(None)
    repaired = await stream._backfill(
        Market(), [candle(0), candle(2)], start + timedelta(minutes=3)
    )
    assert len(repaired) == 3


@pytest.mark.asyncio
async def test_failed_backfill_retains_observations_without_raising():
    start = datetime(2026, 9, 1, tzinfo=UTC)

    class Market:
        async def iter_candles(self, *_args, **_kwargs):
            raise OSError("history unavailable")
            yield

    rows = [
        {
            "time": (start + timedelta(minutes=i)).isoformat(),
            "open": 100,
            "high": 100,
            "low": 100,
            "close": 100,
        }
        for i in (0, 2)
    ]
    assert await LnmLiveStream(None)._backfill(Market(), rows, start + timedelta(minutes=3)) == rows
