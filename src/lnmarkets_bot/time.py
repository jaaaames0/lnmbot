"""Time discipline for the bot.

Every timestamp that crosses a module boundary is UTC.
"""
from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum


class BarResolution(str, Enum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"

    @property
    def seconds(self) -> int:
        return _RESOLUTION_SECONDS[self.value]


_RESOLUTION_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}


def now_utc() -> datetime:
    """Current UTC time as a tz-aware datetime."""
    return datetime.now(UTC)


def ensure_utc(dt: datetime) -> datetime:
    """Coerce a datetime to UTC. Naive inputs are assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def floor_to_bar(ts: datetime, resolution: BarResolution) -> datetime:
    """Round a UTC timestamp down to its bar boundary."""
    ts = ensure_utc(ts)
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % resolution.seconds)
    return datetime.fromtimestamp(floored, tz=UTC)


def to_iso(ts: datetime) -> str:
    """ISO 8601 with explicit UTC offset."""
    return ensure_utc(ts).isoformat()


def parse_iso(s: str) -> datetime:
    """Parse an ISO 8601 string to a UTC datetime."""
    dt = datetime.fromisoformat(s)
    return ensure_utc(dt)
