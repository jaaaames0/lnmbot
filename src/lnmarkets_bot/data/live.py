"""Live data source — polls LNM REST for the latest 1m candle.

v1.1 implementation. Polls the LNM futures data API every `poll_seconds`
(default 5s) for the latest 1m candle on `symbol`. Yields one Bar per poll
with the new candle's close. When wrapped in `MultiTimeframeDataSource`,
the higher-TF bars (1d, 4h) are aggregated on the fly from the 1m stream.

The LNM WebSocket stream is a future optimization (lower latency). Polling
is sufficient for our 1m-cadence strategy and is far simpler.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from ..logging import get_logger
from ..strategy import Bar
from .source import DataSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_log = get_logger("lnmarkets_bot.data.live")


class LnmLiveStream(DataSource):
    """Polls LNM REST for the latest 1m candle. Yields one Bar per poll.

    The yield interval is approximately `poll_seconds` (5s default). If
    the latest candle hasn't changed since the last yield, we don't yield
    a duplicate. If two new candles have arrived between polls (e.g. we
    were offline for a while), we yield each one in order.

    The "latest candle" for LNM is the most recent 1m candle CLOSED (not
    the currently-forming one). LNM returns closed candles in
    `/futures/candles` with a time range; we request enough history to
    initialize the longest strategy timeframe safely.

    Args:
        client: an `LnmRestClient` (with auth) or `None` for unauthenticated
            (public candles are public).
        symbol: LNM symbol. Default `BTCUSD` (LNM uses underscores-less).
        poll_seconds: seconds between polls. Default 5.
        warmup_days: historical closed candles to load before live polling.
            Defaults to 100 days so a new daily EMA has roughly 79 recursive
            updates after its 21-close seed. Subsequent restarts restore the
            durable indicator state and do not depend on this bootstrap.
    """

    def __init__(
        self,
        client,
        *,
        symbol: str = "BTCUSD",
        poll_seconds: float = 5.0,
        warmup_days: int = 100,
        poll_failure_alert_after: int = 3,
    ) -> None:
        self._client = client
        self.symbol = symbol
        self.poll_seconds = poll_seconds
        self.warmup_days = warmup_days
        self.poll_failure_alert_after = max(1, poll_failure_alert_after)
        self._last_yielded_ts: datetime | None = None
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    async def stream(self) -> AsyncIterator[Bar]:
        from ..api.market import MarketApi  # avoid circular import

        market = MarketApi(self._client)
        end_dt = datetime.now(tz=UTC)
        warmup_start = end_dt - timedelta(days=self.warmup_days)

        # Seed the indicators from closed historical bars. The `warmup` flag
        # prevents the strategy from emitting or executing historical signals.
        try:
            history = [
                candle
                async for candle in market.iter_candles(
                    self.symbol,
                    interval="1m",
                    from_ts=warmup_start,
                    to_ts=end_dt,
                )
            ]
        except Exception as exc:
            _log.error("live.warmup_failed", error=str(exc))
            raise RuntimeError("unable to load live strategy warmup candles") from exc

        history = await self._backfill(market, history, end_dt)
        for candle in sorted(history, key=lambda row: _parse_ts(_candle_time(row)) or end_dt):
            bar = _to_bar(candle, warmup=True)
            if bar is None or bar.ts + timedelta(minutes=1) > end_dt:
                continue
            if self._last_yielded_ts is not None and bar.ts <= self._last_yielded_ts:
                continue
            self._last_yielded_ts = bar.ts
            yield bar

        consecutive_failures = 0
        while not self._stopped:
            try:
                from_dt = (
                    self._last_yielded_ts - timedelta(minutes=1)
                    if self._last_yielded_ts is not None
                    else end_dt - timedelta(minutes=5)
                )
                candles = [
                    candle
                    async for candle in market.iter_candles(
                        self.symbol,
                        interval="1m",
                        from_ts=from_dt,
                        to_ts=end_dt,
                    )
                ]
            except Exception as exc:
                consecutive_failures += 1
                event = (
                    "live.poll_failure_alert"
                    if consecutive_failures == self.poll_failure_alert_after
                    else "live.poll_failed"
                )
                log = _log.error if event == "live.poll_failure_alert" else _log.warning
                log(
                    event,
                    error=str(exc),
                    consecutive_failures=consecutive_failures,
                    last_yielded_ts=self._last_yielded_ts.isoformat()
                    if self._last_yielded_ts
                    else None,
                )
                await asyncio.sleep(self.poll_seconds)
                end_dt = datetime.now(tz=UTC)
                continue

            if consecutive_failures:
                _log.info("live.poll_recovered", consecutive_failures=consecutive_failures)
                consecutive_failures = 0

            candles = await self._backfill(market, candles, end_dt)
            # Do not rely on endpoint page order. If a catch-up response is
            # newest-first, advancing ``_last_yielded_ts`` on its first row
            # would otherwise make every older missing candle look stale and
            # silently discard it.
            for c in sorted(candles, key=lambda row: _parse_ts(_candle_time(row)) or end_dt):
                bar = _to_bar(c, warmup=False)
                if bar is None:
                    continue
                bar_ts = bar.ts
                # Skip already-yielded bars.
                if self._last_yielded_ts is not None and bar_ts <= self._last_yielded_ts:
                    continue
                # Skip the currently-forming candle (close is changing).
                now = datetime.now(tz=UTC)
                if bar_ts + timedelta(minutes=1) > now:
                    continue
                self._last_yielded_ts = bar_ts
                yield bar

            # Sleep until next poll. Random jitter would be nice in production.
            await asyncio.sleep(self.poll_seconds)
            end_dt = datetime.now(tz=UTC)

    async def _backfill(self, market, candles, end_dt):
        """One bounded attempt per observed gap; never fabricate missing prices.

        Ten requests and a 2s deadline each bound outage impact. An enduring
        gap is carried as coverage evidence by the aggregator and engine.
        """
        bars = sorted(
            (bar for c in candles if (bar := _to_bar(c, warmup=True)) is not None),
            key=lambda bar: bar.ts,
        )
        last = self._last_yielded_ts
        gaps = []
        for bar in bars:
            if last is not None and bar.ts > last + timedelta(minutes=1):
                gaps.append((last + timedelta(minutes=1), bar.ts))
            last = max(last, bar.ts) if last else bar.ts
        extra = []
        for start, stop in gaps[:10]:
            try:

                async def fetch(start=start, stop=stop):
                    return [
                        c
                        async for c in market.iter_candles(
                            self.symbol, interval="1m", from_ts=start, to_ts=min(stop, end_dt)
                        )
                    ]

                extra.extend(await asyncio.wait_for(fetch(), timeout=2.0))
            except Exception as exc:
                _log.warning(
                    "live.candle_backfill_failed",
                    start=start.isoformat(),
                    error_type=type(exc).__name__,
                )
        return [*candles, *extra]


def _parse_ts(s) -> datetime | None:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s.astimezone(UTC) if s.tzinfo else s.replace(tzinfo=UTC)
    s = str(s)
    # LNM uses ISO 8601 with Z suffix or +00:00.
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(UTC)
    except ValueError:
        return None


def _candle_time(candle: dict) -> object:
    return candle.get("time", candle.get("timestamp", candle.get("ts")))


def _to_bar(candle: dict, *, warmup: bool) -> Bar | None:
    ts = _parse_ts(_candle_time(candle))
    if ts is None:
        return None
    try:
        return Bar(
            ts=ts,
            open=float(candle["open"]),
            high=float(candle["high"]),
            low=float(candle["low"]),
            close=float(candle["close"]),
            volume=float(candle.get("volume", 0.0)),
            timeframe="1m",
            warmup=warmup,
        )
    except (KeyError, TypeError, ValueError):
        return None
