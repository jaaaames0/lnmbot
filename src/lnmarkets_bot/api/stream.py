"""LNM WebSocket stream — JSON-RPC 2.0 wrapper.

Status: skeleton in v0. The architecture is in place; production use requires
filling in:
  - WS connect (`wss://stream.<network>/v1`)
  - authenticate handshake (HMAC signing identical to REST)
  - subscribe payload for candle topic
  - JSON-RPC notification -> Bar mapping

For v0 the paper-mode uses `MockLiveStream` so this remains untested in CI.
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from ..strategy import Bar
from ..data.source import DataSource


class LnmStreamClient(DataSource):
    """Production WS client. Not implemented in v0."""

    def __init__(self, ws_url: str, *, key: str = "", secret: str = "", passphrase: str = "") -> None:
        self.ws_url = ws_url
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self._log = logging.getLogger("lnmarkets_bot.api.stream")

    async def stream(self) -> AsyncIterator[Bar]:
        raise NotImplementedError(
            "LNM WS stream is not implemented in v0. Use MockLiveStream with a parquet file, "
            "or supply testnet credentials and complete this client."
        )
        yield  # pragma: no cover
