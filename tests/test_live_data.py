"""Live-candle bootstrap tests without an LN Markets network connection."""
from __future__ import annotations

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

    with pytest.raises(RuntimeError, match="unable to load live strategy warmup candles") as exc_info:
        await anext(stream.stream())

    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)
