"""HMAC signing tests — known-vector + header shape."""

from __future__ import annotations

import base64
import hashlib
import hmac

from lnmarkets_bot.api.auth import build_auth_headers, canonical_json, sign
from lnmarkets_bot.api.client import LnmRestClient


def test_sign_matches_manual_hmac():
    ts = 1700000000000
    method = "get"
    path = "/v3/futures/data/candles"
    query = "?symbol=BTCUSD&interval=1m&limit=100"
    secret = "test-secret"
    sig = sign(
        secret=secret,
        timestamp_ms=ts,
        method=method,
        path=path,
        body=None,
        query=query,
    )
    expected = base64.b64encode(
        hmac.new(
            secret.encode(),
            f"{ts}{method}{path}{query}".encode(),
            hashlib.sha256,
        ).digest()
    ).decode()
    assert sig == expected


def test_sign_for_post_uses_compact_json():
    ts = 1700000000000
    body = {"side": "buy", "qty": 1000, "leverage": 2}
    sig = sign(
        secret="s",
        timestamp_ms=ts,
        method="post",
        path="/v3/futures/cross/orders/new",
        body=body,
        query="",
    )
    # Independent compute with the same canonical JSON form.
    import json as _json

    payload_str = _json.dumps(body, separators=(",", ":"))
    expected = base64.b64encode(
        hmac.new(
            b"s",
            f"{ts}post/v3/futures/cross/orders/new{payload_str}".encode(),
            hashlib.sha256,
        ).digest()
    ).decode()
    assert sig == expected


def test_build_headers_shape():
    h = build_auth_headers(
        key="k",
        secret="s",
        passphrase="p",
        method="get",
        path="/v3/account",
        body=None,
        query="",
        timestamp_ms=100,
    )
    assert set(h.keys()) == {
        "LNM-ACCESS-KEY",
        "LNM-ACCESS-PASSPHRASE",
        "LNM-ACCESS-TIMESTAMP",
        "LNM-ACCESS-SIGNATURE",
    }
    assert h["LNM-ACCESS-TIMESTAMP"] == "100"
    assert h["LNM-ACCESS-KEY"] == "k"


def test_post_headers_include_content_type():
    h = build_auth_headers(
        key="k",
        secret="s",
        passphrase="p",
        method="post",
        path="/v3/x",
        body={"a": 1},
        query="",
        timestamp_ms=100,
    )
    assert h["Content-Type"] == "application/json"


def test_canonical_json_matches_javascript_integral_number_encoding():
    assert canonical_json({"leverage": 1.0, "quantity": 1, "side": "buy"}) == (
        '{"leverage":1,"quantity":1,"side":"buy"}'
    )


def test_client_signs_the_complete_v3_url_path():
    client = LnmRestClient(
        base_url="https://api.signet.lnmarkets.com/v3",
        access_key="k",
        access_secret="s",
        access_passphrase="p",
        authed=True,
    )
    try:
        assert client._signed_path("/account") == "/v3/account"
    finally:
        import asyncio

        asyncio.run(client.aclose())
