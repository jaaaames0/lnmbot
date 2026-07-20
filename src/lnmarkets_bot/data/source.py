"""DataSource abstract base.

A `DataSource.stream()` is an async generator yielding Bar events. The same
ABC is implemented by:
  - historical replay (parquet)
  - LNM WebSocket live feed
  - the in-process mock used during paper mode without credentials

This keeps the engine plumbing consistent across modes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..strategy import Bar


class DataSource(ABC):
    """Async-iterable stream of Bars."""

    @abstractmethod
    async def stream(self) -> AsyncIterator[Bar]:
        """Yield Bar events in order. Implementations may close on cancellation."""
        ...
        yield  # pragma: no cover  (ABC; this line is never reached)

    async def close(self) -> None:
        """Optional cleanup hook."""
        return None
