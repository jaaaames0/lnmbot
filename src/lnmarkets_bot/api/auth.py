"""HMAC-SHA256 request signing per the LN Markets v3 docs.

Signed payload (concatenated, no separators):
    timestamp_ms + method_lowercase + path + data

Where:
    data = request.body as JSON (no whitespace) for POST/PUT
         = URL query string for GET
         = "" for empty body / no query

Signature is Base64(HMAC_SHA256(secret, payload)).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Literal

MethodT = Literal["get", "post", "put", "delete"]


def now_ms() -> int:
    return int(time.time() * 1000)


def canonical_json(body: dict[str, object]) -> str:
    """Serialize JSON like JavaScript's ``JSON.stringify`` for LNM signing.

    In particular, JavaScript emits integral floats as ``1`` rather than
    Python's default ``1.0``. LNM verifies this canonical JSON representation.
    """
    normalized = {
        key: int(value) if isinstance(value, float) and value.is_integer() else value
        for key, value in body.items()
    }
    return json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)


def sign(
    *,
    secret: str,
    timestamp_ms: int,
    method: str,
    path: str,
    body: dict[str, object] | None,
    query: str,
) -> str:
    """Compute the LNM-ACCESS-SIGNATURE header value."""
    if method.lower() not in ("get", "post", "put", "delete"):
        raise ValueError(f"unsupported HTTP method {method!r}")
    data = query if method.lower() == "get" else canonical_json(body) if body else ""
    msg = f"{timestamp_ms}{method.lower()}{path}{data}".encode()
    digest = hmac.new(secret.encode(), msg, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def build_auth_headers(
    *,
    key: str,
    secret: str,
    passphrase: str,
    method: str,
    path: str,
    body: dict[str, object] | None,
    query: str,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    """Return the four LNM-ACCESS-* headers plus Content-Type when needed."""
    ts = timestamp_ms if timestamp_ms is not None else now_ms()
    sig = sign(
        secret=secret,
        timestamp_ms=ts,
        method=method,
        path=path,
        body=body,
        query=query,
    )
    headers = {
        "LNM-ACCESS-KEY": key,
        "LNM-ACCESS-PASSPHRASE": passphrase,
        "LNM-ACCESS-TIMESTAMP": str(ts),
        "LNM-ACCESS-SIGNATURE": sig,
    }
    if method.lower() in ("post", "put", "delete") and body is not None:
        headers["Content-Type"] = "application/json"
    return headers
