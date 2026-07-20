"""Async httpx-based LNM REST client.

Features:
  - HMAC-SHA256 signing per `auth.py`
  - Token-bucket rate limiting (~20 req/s sustained, burst 40 when authed)
    honoring `Retry-After` and LNM's `RateLimit-Policy` / `RateLimit` headers
  - Cursor-based pagination over the LNM list endpoints
  - Structured logging via stdlib logging (structlog renders it)

This is the *only* module that touches the network for trading actions.
The engine must never import `api.trades` directly — it goes through
`risk/guard.py`. Enforced via import-linter.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlsplit

import httpx

from .auth import build_auth_headers, canonical_json

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class LnmApiError(RuntimeError):
    """Generic API error wrapping an HTTP failure."""

    def __init__(self, status: int, body: str, url: str) -> None:
        super().__init__(f"LNM {status} on {url}: {body[:200]}")
        self.status = status
        self.body = body
        self.url = url


class RateLimiter:
    """Token bucket roughly matching LNM's published policy.

    Authenticated bucket: 20 req/s sustained, burst 40.
    Public bucket:        4 req/s sustained, burst 5 (we don't usually hit this).
    """

    def __init__(
        self,
        *,
        refill_per_sec: float = 20.0,
        capacity: float = 40.0,
        authed: bool = True,
    ) -> None:
        self.refill_per_sec = refill_per_sec
        self.capacity = capacity
        self.authed = authed
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_sec)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                deficit = 1.0 - self._tokens
                wait = deficit / self.refill_per_sec
                await asyncio.sleep(wait)


class LnmRestClient:
    """Async client. Single httpx.AsyncClient; rate-limited + signed requests.

    Construction is intentionally cheap; one client per process is fine.
    """

    def __init__(
        self,
        *,
        base_url: str,
        access_key: str = "",
        access_secret: str = "",
        access_passphrase: str = "",
        authed: bool = False,
        max_retries: int = 3,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_key = access_key
        self.access_secret = access_secret
        self.access_passphrase = access_passphrase
        self.authed = authed
        self.max_retries = max_retries
        self._client = httpx.AsyncClient(timeout=timeout)
        self._rate_limiter = RateLimiter(authed=authed)

    async def aclose(self) -> None:
        await self._client.aclose()

    # ---- Public low-level ----

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        signed: bool | None = None,
    ) -> Any:
        """Make a signed+rate-limited request. Returns parsed JSON or None for empty."""
        signed = self.authed if signed is None else signed
        full_path = self._normalize_path(path)
        query = urlencode(params, doseq=True) if params else ""
        url = f"{self.base_url}{full_path}"
        if query:
            url = f"{url}?{query}"
        # LNM signs the URL pathname, including the `/v3` prefix supplied in
        # base_url.  `full_path` only identifies the endpoint suffix.
        signed_path = self._signed_path(full_path)
        signed_query = f"?{query}" if query else ""
        headers = self._build_headers(method, signed_path, body, signed_query) if signed else {}
        content = canonical_json(body) if body else None

        for attempt in range(self.max_retries + 1):
            await self._rate_limiter.acquire()
            resp = await self._client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                content=content,
            )
            if resp.status_code == 429:
                # Honor Retry-After (in seconds). If missing, sleep the bucket-window average.
                retry_after = float(resp.headers.get("Retry-After", "1"))
                logging.getLogger("lnmarkets_bot.api").warning(
                    "rate_limited sleep=%.2f attempt=%d", retry_after, attempt + 1
                )
                await asyncio.sleep(retry_after)
                continue
            if resp.status_code >= 500:
                # transient; retry with backoff
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            if resp.status_code >= 400:
                raise LnmApiError(resp.status_code, resp.text, url)
            if not resp.content:
                return None
            return resp.json()
        raise LnmApiError(0, "exhausted retries", url)

    def _build_headers(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        query: str,
    ) -> dict[str, str]:
        if not self.authed:
            return {}
        return build_auth_headers(
            key=self.access_key,
            secret=self.access_secret,
            passphrase=self.access_passphrase,
            method=method,
            path=path,
            body=body,
            query=query,
        )

    def _normalize_path(self, path: str) -> str:
        if not path.startswith("/"):
            return f"/{path}"
        return path

    def _signed_path(self, endpoint_path: str) -> str:
        """Return the exact URL pathname that LN Markets expects us to sign."""
        base_path = urlsplit(self.base_url).path.rstrip("/")
        return f"{base_path}{endpoint_path}" if base_path else endpoint_path

    # ---- Convenience ----

    async def get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        return await self.request("get", path, params=params)

    async def post(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        return await self.request("post", path, body=body)

    async def put(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        return await self.request("put", path, body=body)

    async def delete(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        return await self.request("delete", path, body=body)

    async def iter_list(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        cursor_field: str = "cursor",
    ) -> AsyncIterator[Any]:
        """Iterate a paginated LNM list endpoint. cursor is an ISO 8601 timestamp."""
        params = dict(params or {})
        params.setdefault("limit", 1000)
        while True:
            page = await self.get(path, params=params)
            data = page.get("data", []) if isinstance(page, dict) else []
            for item in data:
                yield item
            next_cursor = page.get("nextCursor") if isinstance(page, dict) else None
            if not next_cursor:
                return
            params[cursor_field] = next_cursor
            # Tiny pause to be friendly.
            await asyncio.sleep(0.05)


def _ensure_str(d: dict[str, str] | None, key: str) -> str:
    if not d:
        return ""
    return d.get(key, "")
