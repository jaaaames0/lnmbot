"""Local, read-only operational dashboard for the bot SQLite database."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import websockets

from lnmarkets_bot.api.account import AccountApi
from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTradesApi
from lnmarkets_bot.portfolio.store import read_overview

TIMEFRAMES = ("1d", "4h")
MA_STRATEGY_NAME = "lnmarkets_bot.strategy.ma_cross.MaCross"
BREAKOUT_STRATEGY_NAME = "lnmarkets_bot.strategy.close_range_live.CloseRangeLive"
BREAKOUT_INSTANCE_ID = "btc_close_range_v1"
CONSTANT_NOTIONAL_USD = 100.0
BINANCE_HOURLY_CACHE = Path(__file__).resolve().parents[1] / "data/cache/btcusdt_perp_1h_4y.parquet"
BINANCE_DAILY_CACHE = Path(__file__).resolve().parents[1] / "data/cache/btcusdt_perp_1d_4y.parquet"
LNM_DAILY_SEED_CACHE = (
    Path(__file__).resolve().parents[1]
    / "data/cache/lnmarkets_btc_1d_2019-09-09_2026-09-13.parquet"
)
BREAKOUT_CAMPAIGN_SEED = (
    Path(__file__).resolve().parents[1] / "docs/btc-close-range-lnm-live-seed-2026-09-13.json"
)
BREAKOUT_PAPER_REFERENCE = (
    Path(__file__).resolve().parents[1] / "docs/btc-close-range-lnm-paper-reference-2026-09-13.json"
)
SAT_TOKEN = "__SAT_SYMBOL__"
SAT_ICON = '<i class="fak fa-satoshisymbol-solidtilt sat-symbol" aria-label="sats"></i>'
POSITIVE_OPEN = "__POSITIVE_OPEN__"
NEGATIVE_OPEN = "__NEGATIVE_OPEN__"
VALUE_CLOSE = "__VALUE_CLOSE__"
_LOG = logging.getLogger("lnmarkets_bot.dashboard")


class SafeHtml(str):
    """A dashboard-generated cell that must not be escaped again."""


@dataclass(frozen=True)
class LivePrice:
    price: float
    ts: datetime


class DashboardPriceStream:
    """Public, dashboard-only last-price stream with reconnecting fallback."""

    _TOPIC = "futures/inverse/btc_usd/lastPrice"

    def __init__(self) -> None:
        self._latest: LivePrice | None = None
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._url = ""

    def start(self, url: str) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._url = url
        self._thread = threading.Thread(target=self._run, name="lnmbot-price", daemon=True)
        self._thread.start()

    def latest(self) -> LivePrice | None:
        with self._lock:
            return self._latest

    def _record_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
            data = payload["params"]["data"]
            if payload["method"] != "subscription":
                return
            price = float(data["lastPrice"])
            ts = datetime.fromtimestamp(float(data["time"]) / 1000, tz=UTC)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, OSError):
            return
        with self._lock:
            self._latest = LivePrice(price=price, ts=ts)

    def _run(self) -> None:
        asyncio.run(self._listen())

    async def _listen(self) -> None:
        delay = 1.0
        while True:
            try:
                async with websockets.connect(self._url, open_timeout=10, ping_interval=20) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 1,
                                "method": "subscribe",
                                "params": {"topics": [self._TOPIC]},
                            }
                        )
                    )
                    delay = 1.0
                    async for message in ws:
                        self._record_message(message)
            except Exception as exc:
                _LOG.warning("dashboard.price_stream_reconnecting: %s", exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)


_PRICE_STREAM = DashboardPriceStream()


@dataclass(frozen=True)
class ExchangeTrade:
    margin_sats: int
    maintenance_margin_sats: int
    pl_sats: int


@dataclass(frozen=True)
class ExchangeSnapshot:
    available_sats: int
    total_sats: int
    margin_used_sats: int
    maintenance_margin_sats: int
    running_pl_sats: int
    trades: dict[str, ExchangeTrade]
    fetched_at: datetime
    funding_rate: float | None = None
    funding_rate_ts: datetime | None = None
    deposits_sats: int = 0
    withdrawals_sats: int = 0


class ExchangeSnapshotCache:
    """Small server-side cache for the dashboard's read-only LNM credential."""

    def __init__(self, ttl_seconds: float = 10.0) -> None:
        self._ttl_seconds = ttl_seconds
        self._snapshot: ExchangeSnapshot | None = None
        self._fetched_monotonic = 0.0
        self._lock = threading.Lock()

    def get(self) -> ExchangeSnapshot | None:
        if not _dashboard_credentials_present():
            return None
        with self._lock:
            if self._snapshot and time.monotonic() - self._fetched_monotonic < self._ttl_seconds:
                return self._snapshot
            try:
                self._snapshot = asyncio.run(_fetch_exchange_snapshot())
                self._fetched_monotonic = time.monotonic()
            except Exception as exc:
                _LOG.warning("dashboard.exchange_snapshot_failed: %s", exc)
            return self._snapshot


def _dashboard_credentials_present() -> bool:
    return all(
        os.getenv(key)
        for key in ("LNM_DASHBOARD_KEY", "LNM_DASHBOARD_SECRET", "LNM_DASHBOARD_PASSPHRASE")
    )


async def _fetch_exchange_snapshot() -> ExchangeSnapshot:
    client = LnmRestClient(
        base_url=os.getenv("LNM_DASHBOARD_BASE_URL", "https://api.lnmarkets.com/v3"),
        access_key=os.environ["LNM_DASHBOARD_KEY"],
        access_secret=os.environ["LNM_DASHBOARD_SECRET"],
        access_passphrase=os.environ["LNM_DASHBOARD_PASSPHRASE"],
        authed=True,
        timeout=10.0,
    )
    try:
        (
            account,
            running,
            funding_response,
            deposits_lightning,
            deposits_onchain,
            withdrawals_lightning,
            withdrawals_onchain,
        ) = await asyncio.gather(
            AccountApi(client).get_balance(),
            IsolatedTradesApi(client).get_running_trades(),
            client.get("/futures/funding-settlements", params={"symbol": "BTCUSD", "limit": 1}),
            client.get("/account/deposits/lightning", params={"limit": 1000}),
            client.get("/account/deposits/on-chain", params={"limit": 1000}),
            client.get("/account/withdrawals/lightning", params={"limit": 1000}),
            client.get("/account/withdrawals/on-chain", params={"limit": 1000}),
        )
    finally:
        await client.aclose()
    trades = {
        trade.id: ExchangeTrade(
            margin_sats=int(trade.margin or 0),
            maintenance_margin_sats=int(trade.maintenance_margin or 0),
            pl_sats=int(trade.pl or 0),
        )
        for trade in running
        if trade.id
    }
    margin_used = sum(trade.margin_sats for trade in trades.values())
    maintenance_margin = sum(trade.maintenance_margin_sats for trade in trades.values())
    running_pl = sum(trade.pl_sats for trade in trades.values())
    available = int(account.get("balance", 0))
    funding_data = funding_response.get("data", []) if isinstance(funding_response, dict) else []
    latest_funding = funding_data[0] if isinstance(funding_data, list) and funding_data else {}
    funding_rate = latest_funding.get("fundingRate") if isinstance(latest_funding, dict) else None
    try:
        funding_rate = float(funding_rate)
    except (TypeError, ValueError):
        funding_rate = None

    def cashflow_total(response: object) -> int:
        data = response.get("data", []) if isinstance(response, dict) else []
        if not isinstance(data, list):
            return 0
        return sum(int(item.get("amount") or 0) for item in data if isinstance(item, dict))

    return ExchangeSnapshot(
        available_sats=available,
        total_sats=available + margin_used + maintenance_margin + running_pl,
        margin_used_sats=margin_used,
        maintenance_margin_sats=maintenance_margin,
        running_pl_sats=running_pl,
        trades=trades,
        fetched_at=datetime.now(UTC),
        funding_rate=funding_rate,
        funding_rate_ts=_parse_ts(latest_funding.get("time"))
        if isinstance(latest_funding, dict)
        else None,
        deposits_sats=cashflow_total(deposits_lightning) + cashflow_total(deposits_onchain),
        withdrawals_sats=cashflow_total(withdrawals_lightning)
        + cashflow_total(withdrawals_onchain),
    )


_EXCHANGE_CACHE = ExchangeSnapshotCache()


def _query(db_path: Path, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(sql, params).fetchall()


def _table_columns(db_path: Path, table: str) -> set[str]:
    if not table.replace("_", "").isalnum():
        raise ValueError("invalid table name")
    return {str(row["name"]) for row in _query(db_path, f"PRAGMA table_info({table})")}


def _parse_ts(value: object) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def _metadata(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _format_timestamp(value: object) -> str:
    ts = _parse_ts(value)
    return ts.strftime("%Y-%m-%d %H:%M UTC") if ts else str(value or "")


def _format_amount(sats: object, denomination: str, btc_price: float | None) -> str:
    try:
        amount = int(sats)
    except (TypeError, ValueError):
        return "-"
    if denomination == "usd" and btc_price:
        return f"${amount * btc_price / 1e8:,.2f}"
    return f"{amount:,} {SAT_TOKEN}"


def _amount_html(sats: object, denomination: str, btc_price: float | None) -> SafeHtml:
    return SafeHtml(_format_amount(sats, denomination, btc_price).replace(SAT_TOKEN, SAT_ICON))


def _format_signed_amount(
    sats: object,
    denomination: str,
    btc_price: float | None,
    *,
    invert: bool = False,
) -> str:
    """Format an account P&L contribution; positive is always beneficial."""
    try:
        amount = int(sats)
    except (TypeError, ValueError):
        return "-"
    amount = -amount if invert else amount
    if denomination == "usd" and btc_price:
        display = f"{'+' if amount > 0 else '-' if amount < 0 else ''}${abs(amount) * btc_price / 1e8:,.2f}"
    else:
        display = f"{'+' if amount > 0 else '-' if amount < 0 else ''}{abs(amount):,} {SAT_TOKEN}"
    if amount > 0:
        return f"{POSITIVE_OPEN}{display}{VALUE_CLOSE}"
    if amount < 0:
        return f"{NEGATIVE_OPEN}{display}{VALUE_CLOSE}"
    return display


def _signed_amount_html(
    sats: object, denomination: str, btc_price: float | None, *, invert: bool = False
) -> str:
    return (
        _format_signed_amount(sats, denomination, btc_price, invert=invert)
        .replace(SAT_TOKEN, SAT_ICON)
        .replace(POSITIVE_OPEN, '<span class="positive">')
        .replace(NEGATIVE_OPEN, '<span class="negative">')
        .replace(VALUE_CLOSE, "</span>")
    )


def _signed_percent_html(value: object) -> SafeHtml:
    try:
        percentage = float(value)
    except (TypeError, ValueError):
        return SafeHtml("-")
    display = f"{percentage:+.2f}%" if percentage else "0.00%"
    if percentage > 0:
        return SafeHtml(f'<span class="positive">{display}</span>')
    if percentage < 0:
        return SafeHtml(f'<span class="negative">{display}</span>')
    return SafeHtml(display)


def _signed_usd_html(value: object) -> SafeHtml:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return SafeHtml("-")
    display = f"{'+' if amount > 0 else '-' if amount < 0 else ''}${abs(amount):,.2f}"
    if amount > 0:
        return SafeHtml(f'<span class="positive">{display}</span>')
    if amount < 0:
        return SafeHtml(f'<span class="negative">{display}</span>')
    return SafeHtml(display)


def _render_cell_value(value: object) -> str:
    if isinstance(value, SafeHtml):
        return str(value)
    return (
        html.escape(str(value if value is not None else ""))
        .replace(SAT_TOKEN, SAT_ICON)
        .replace(POSITIVE_OPEN, "<span class=positive>")
        .replace(NEGATIVE_OPEN, "<span class=negative>")
        .replace(VALUE_CLOSE, "</span>")
    )


def _table(
    title: str,
    rows: list[dict[str, object]],
    columns: tuple[str, ...],
    *,
    compact: bool = False,
) -> str:
    if not rows:
        return f"<section><h2>{html.escape(title)}</h2><p class=muted>None recorded.</p></section>"
    headers = "".join(f"<th>{html.escape(column.replace('_', ' '))}</th>" for column in columns)

    def cell(row: dict[str, object], column: str) -> str:
        if column == "trade_id" and row.get("trade_id_copy"):
            trade_id = str(row["trade_id_copy"])
            short_id = str(row.get("trade_id") or trade_id)
            return (
                f"<code>{html.escape(short_id)}</code> "
                f'<button class="copy-id" type="button" data-trade-id="{html.escape(trade_id, quote=True)}" '
                "onclick=\"navigator.clipboard.writeText(this.dataset.tradeId);this.textContent='Copied'\">Copy</button>"
            )
        value = row.get(column)
        if isinstance(value, SafeHtml):
            return value
        if column == "ts" or column.endswith("_ts"):
            return _format_timestamp(value)
        return str(value if value is not None else "")

    body = "".join(
        "<tr>"
        + "".join(
            f"<td>{cell(row, column) if column == 'trade_id' and row.get('trade_id_copy') else _render_cell_value(cell(row, column))}</td>"
            for column in columns
        )
        + "</tr>"
        + (
            f'<tr class="stack-detail-row"><td colspan="{len(columns)}">{row["_details_html"]}</td></tr>'
            if isinstance(row.get("_details_html"), SafeHtml)
            else ""
        )
        for row in rows
    )
    class_name = " compact-table" if compact else ""
    return f"<section class=table-section{class_name}><h2>{html.escape(title)}</h2><div class=table-wrap><table><thead><tr>{headers}</tr></thead><tbody>{body}</tbody></table></div></section>"


def _card(label: str, value: object, detail: str = "") -> str:
    return (
        "<article class=card>"
        f"<p>{html.escape(label)}</p><strong>{html.escape(str(value))}</strong>"
        f"<small>{html.escape(detail)}</small></article>"
    )


def _format_price(value: object) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"


def _ma_detail(levels: dict[str, object] | None, side: str | None = None) -> str:
    if not levels:
        return "Trigger levels awaiting enough recorded history"
    if side == "long":
        action = f"exit below {_format_price(levels['short_trigger'])}"
    elif side == "short":
        action = f"exit above {_format_price(levels['long_trigger'])}"
    else:
        action = (
            f"long above {_format_price(levels['long_trigger'])} · "
            f"short below {_format_price(levels['short_trigger'])}"
        )
    source = "Binance warmup · " if levels.get("bootstrap_source") == "binance" else ""
    return f"{source}{action}"


def _position_card(
    timeframe: str,
    position: dict[str, object] | None,
    denomination: str,
    btc_price: float | None,
    levels: dict[str, object] | None,
    cooldown: dict[str, int],
) -> str:
    winner = cooldown.get("winner", 0)
    loss = cooldown.get("loss", 0)
    remaining = max(winner, loss)
    cooldown_detail = ""
    if remaining:
        labels = []
        if winner:
            labels.append(f"winner {winner}")
        if loss:
            labels.append(f"loss {loss}")
        cooldown_detail = (
            f"Cool-off · {remaining} verdict {'change' if remaining == 1 else 'changes'} left"
            f" ({' · '.join(labels)})"
        )
    if position is None:
        return (
            f'<article class="card position-card flat"><p>MA {timeframe} position</p>'
            f"<strong>{'Cool-off active' if remaining else 'Flat'}</strong>"
            f"<small>{cooldown_detail or 'Ready for the next verdict transition'}</small></article>"
        )
    side = str(position["side"])
    estimate = _signed_amount_html(position["estimated_unrealized_sats"], denomination, btc_price)
    move_display = _signed_percent_html(position.get("position_change_pct"))
    return (
        f'<article class="card position-card {side}"><p>MA {timeframe} position</p>'
        "<div class=position-card-body><div class=position-static>"
        f"<strong>{side.title()}</strong><small>${position['contracts']:,} · {position['leverage']}x"
        f"{' · ' + cooldown_detail if cooldown_detail else ''}</small>"
        "</div><div class=position-dynamic>"
        f"<strong>{estimate}</strong><small>{move_display}</small>"
        "</div></div></article>"
    )


def _pnl_card(
    pnl: list[dict[str, object]], denomination: str, window: str, btc_price: float | None
) -> str:
    chosen = next((row for row in pnl if row["key"] == window), pnl[0])
    controls = "".join(
        f'<a class="pnl-toggle{" active" if key == window else ""}" '
        f'href="/?denom={denomination}&pnl_window={key}">{label}</a>'
        for key, label in (
            ("1day", "1d"),
            ("7days", "7d"),
            ("30days", "30d"),
            ("alltime", "All"),
        )
    )
    return (
        '<article class="card pnl-card"><p>Net P&amp;L</p>'
        f"<strong>{_signed_amount_html(chosen['net'], denomination, btc_price)}</strong>"
        f"<small>{controls}</small></article>"
    )


def _active_run(db_path: Path) -> dict[str, object] | None:
    rows = _query(
        db_path,
        "SELECT id, mode, status, started_at, ended_at, strategy_params_json, config_json FROM runs "
        # Manual recovery rows are audit events, not bot sessions.  They can
        # be inserted after a live run has begun, so a highest-id lookup would
        # falsely label a healthy service as stopped and show recovery metadata
        # as the active configuration.
        "WHERE mode IN ('live', 'paper') "
        "ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, started_at DESC, id DESC LIMIT 1",
    )
    return dict(rows[0]) if rows else None


def _signals(
    db_path: Path, run_id: int | None = None, tf: str | None = None
) -> list[dict[str, object]]:
    where = "WHERE run_id = ?" if run_id is not None else ""
    params: tuple[object, ...] = (run_id,) if run_id is not None else ()
    columns = _table_columns(db_path, "signals")
    strategy_column = (
        "strategy_instance_id"
        if "strategy_instance_id" in columns
        else "'' AS strategy_instance_id"
    )
    position_column = "position_key" if "position_key" in columns else "'' AS position_key"
    rows = _query(
        db_path,
        "SELECT id, ts, kind, side, target_size_usd, target_leverage, reason, "
        f"{strategy_column}, {position_column}, metadata_json "
        f"FROM signals {where} ORDER BY id DESC LIMIT 500",
        params,
    )
    result: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        meta = _metadata(item.pop("metadata_json"))
        trigger_tf = str(meta.get("trigger_tf") or "")
        strategy = item.get("strategy_instance_id") or "legacy_ma"
        if tf == "breakout" and strategy != BREAKOUT_INSTANCE_ID:
            continue
        if tf in TIMEFRAMES and (
            trigger_tf != tf or strategy not in {"legacy_ma", "ma_cross_primary"}
        ):
            continue
        item["timeframe"] = trigger_tf or "-"
        item["strategy"] = item.get("strategy_instance_id") or "legacy_ma"
        item["slot"] = item.get("position_key") or trigger_tf or "-"
        item["signal_ts"] = meta.get("signal_ts") or item["ts"]
        item["signal_close"] = meta.get("signal_close")
        item["range_boundary"] = meta.get("boundary")
        item["distance_ema_atr"] = meta.get("distance_ema_atr")
        item["average_overlap10"] = meta.get("average_overlap10")
        item["chop_regime"] = str(meta.get("chop_regime") or "-")
        chop_value = meta.get("chop_value")
        item["chop_value"] = f"{float(chop_value):.2f}" if chop_value is not None else "-"
        multiplier = meta.get("entry_size_multiplier")
        item["entry_size_multiplier"] = (
            f"{float(multiplier):.2f}x" if multiplier is not None else "-"
        )
        if item["kind"] != "entry":
            item["target_leverage"] = "-"
        result.append(item)
    return result


def _orders(
    db_path: Path, run_id: int | None = None, tf: str | None = None
) -> list[dict[str, object]]:
    where = "WHERE orders.run_id = ?" if run_id is not None else ""
    params: tuple[object, ...] = (run_id,) if run_id is not None else ()
    columns = _table_columns(db_path, "orders")
    strategy_column = (
        "orders.strategy_instance_id"
        if "strategy_instance_id" in columns
        else "'' AS strategy_instance_id"
    )
    position_column = "orders.position_key" if "position_key" in columns else "'' AS position_key"
    rows = _query(
        db_path,
        f"SELECT orders.id, orders.ts, orders.trigger_tf, {strategy_column}, "
        f"{position_column}, orders.side, orders.qty_sats, "
        "orders.leverage, orders.price_usd, orders.status, orders.lnm_order_id, "
        "orders.rejection_reason, orders.metadata_json, signals.metadata_json AS signal_metadata_json "
        "FROM orders LEFT JOIN signals ON signals.id = orders.signal_id "
        f"{where} ORDER BY orders.id DESC LIMIT 500",
        params,
    )
    result: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        strategy = item.get("strategy_instance_id") or "legacy_ma"
        if tf == "breakout" and strategy != BREAKOUT_INSTANCE_ID:
            continue
        if tf in TIMEFRAMES and (
            item["trigger_tf"] != tf or strategy not in {"legacy_ma", "ma_cross_primary"}
        ):
            continue
        meta = _metadata(item.pop("metadata_json"))
        signal_meta = _metadata(item.pop("signal_metadata_json"))
        item["action"] = str(meta.get("isolated_action") or "-")
        item["strategy"] = item.get("strategy_instance_id") or "legacy_ma"
        item["slot"] = item.get("position_key") or item.get("trigger_tf") or "-"
        item["fee_sats"] = meta.get("opening_fee_sats", meta.get("closing_fee_sats", ""))
        item["trade_id"] = str(meta.get("lnm_trade_id") or item.get("lnm_order_id") or "")
        item["opening_fee_sats"] = int(meta.get("opening_fee_sats") or 0)
        item["closing_fee_sats"] = int(meta.get("closing_fee_sats") or 0)
        item["gross_pl_sats"] = int(meta.get("gross_pl_sats") or 0)
        item["chop_regime"] = str(signal_meta.get("chop_regime") or "-")
        item["entry_size_multiplier"] = float(signal_meta.get("entry_size_multiplier", 1.0))
        result.append(item)
    return result


def _funding_by_trade(db_path: Path) -> dict[str, int]:
    rows = _query(
        db_path,
        "SELECT trade_id, SUM(fee_sats) AS fee_sats FROM funding_fees GROUP BY trade_id",
    )
    return {str(row["trade_id"]): int(row["fee_sats"] or 0) for row in rows}


def _trade_owners(orders: list[dict[str, object]]) -> dict[str, tuple[str, str]]:
    return {
        str(row["trade_id"]): (str(row["strategy"]), str(row["slot"]))
        for row in orders
        if row.get("action") == "open" and row.get("trade_id")
    }


def _open_positions(
    db_path: Path,
    orders: list[dict[str, object]],
    price: float | None,
    exchange: ExchangeSnapshot | None = None,
) -> list[dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for row in reversed(orders):
        trade_id = str(row.get("lnm_order_id") or "")
        if trade_id:
            latest[trade_id] = row
    positions: list[dict[str, object]] = []
    funding_by_trade = _funding_by_trade(db_path)
    for trade_id, row in latest.items():
        if row.get("action") != "open":
            continue
        entry = float(row["price_usd"] or 0)
        quantity = int(row["qty_sats"] or 0)
        side = "long" if row.get("side") == "buy" else "short"
        estimate = None
        margin_sats = None
        position_change_pct = None
        if price and entry > 0:
            signed = 1 if side == "long" else -1
            estimate = round(signed * quantity * (1 / entry - 1 / price) * 1e8)
            position_change_pct = (
                signed * ((price / entry) - 1.0) * 100.0 * float(row["leverage"] or 1.0)
            )
        remote = exchange.trades.get(trade_id) if exchange else None
        if remote is not None:
            estimate = remote.pl_sats
            margin_sats = remote.margin_sats
        positions.append(
            {
                "timeframe": row.get("trigger_tf"),
                "strategy": row.get("strategy"),
                "slot": row.get("slot"),
                "side": side,
                "contracts": quantity,
                "leverage": row.get("leverage"),
                "entry_price": entry,
                "entry_ts": row.get("ts"),
                "estimated_unrealized_sats": estimate if estimate is not None else "-",
                "margin_sats": margin_sats if margin_sats is not None else "-",
                "pnl_source": "LN Markets" if remote is not None else "local estimate",
                "position_change_pct": position_change_pct,
                "accumulated_funding_sats": funding_by_trade.get(trade_id, 0),
                "opening_fee_sats": int(row.get("opening_fee_sats") or 0),
                "entry_adjustment": (
                    f"CHOP *{float(row['entry_size_multiplier']):.2f}"
                    if row.get("chop_regime") == "high_chop"
                    else ""
                ),
                "trade_id": trade_id,
            }
        )
    return sorted(positions, key=lambda row: (str(row["strategy"]), str(row["slot"])))


def _trade_history_rows(
    db_path: Path, *, tf: str | None, denomination: str, btc_price: float | None
) -> list[dict[str, object]]:
    """One readable ledger row per isolated LNM trade, not per API action."""
    grouped: dict[str, dict[str, object]] = {}
    for order in reversed(_orders(db_path, tf=tf)):
        trade_id = str(order.get("trade_id") or "")
        if not trade_id:
            continue
        trade = grouped.setdefault(trade_id, {"trade_id": trade_id})
        if order["action"] == "open":
            trade["open"] = order
        elif order["action"] in {"close", "external_close"}:
            trade["close"] = order
    funding = _funding_by_trade(db_path)
    rows: list[dict[str, object]] = []
    for trade_id, trade in grouped.items():
        opened = trade.get("open")
        if not isinstance(opened, dict):
            continue
        closed = trade.get("close")
        close = closed if isinstance(closed, dict) else None
        opening_fee = int(opened.get("opening_fee_sats") or 0)
        closing_fee = int(close.get("closing_fee_sats") or 0) if close else 0
        gross_pl = int(close.get("gross_pl_sats") or 0) if close else 0
        funding_sats = funding.get(trade_id, 0)
        completed = close is not None
        net_sats = gross_pl - opening_fee - closing_fee - funding_sats if completed else None
        net_return_pct = None
        if completed:
            notional_usd = int(opened.get("qty_sats") or 0)
            exit_price = float(close.get("price_usd") or 0)
            if notional_usd > 0 and exit_price > 0:
                net_return_pct = net_sats * exit_price / 1e8 / notional_usd * 100
        rows.append(
            {
                "trade_id": f"{trade_id[:8]}…{trade_id[-4:]}",
                "trade_id_copy": trade_id,
                "timeframe": opened.get("trigger_tf", "-"),
                "strategy": opened.get("strategy", "legacy_ma"),
                "slot": opened.get("slot", opened.get("trigger_tf", "-")),
                "position": (
                    f"{'Long' if opened.get('side') == 'buy' else 'Short'} · "
                    f"${int(opened.get('qty_sats') or 0):,} · {opened.get('leverage', '-')}x"
                ),
                "opened_ts": opened.get("ts", "-"),
                "closed_ts": close.get("ts", "-") if close else "open",
                "entry_price": _format_price(opened.get("price_usd")),
                "exit_price": _format_price(close.get("price_usd")) if close else "-",
                "gross_pl": _format_signed_amount(gross_pl, denomination, btc_price)
                if completed
                else "-",
                "trading_fees": _format_signed_amount(
                    -(opening_fee + closing_fee), denomination, btc_price
                ),
                "funding": _format_signed_amount(
                    funding_sats, denomination, btc_price, invert=True
                ),
                "net_pl": _format_signed_amount(net_sats, denomination, btc_price)
                if net_sats is not None
                else "-",
                "net_return": _signed_percent_html(net_return_pct),
            }
        )
    return sorted(rows, key=lambda row: str(row["opened_ts"]), reverse=True)


def _market_context(db_path: Path) -> tuple[float | None, list[dict[str, object]], datetime | None]:
    rows = _query(
        db_path,
        "SELECT ts, close FROM bars WHERE id IN (SELECT MAX(id) FROM bars GROUP BY ts) "
        "ORDER BY ts DESC LIMIT 10100",
    )
    if not rows:
        return None, [], None
    recorded = (
        pd.DataFrame(
            [(parsed, float(row["close"])) for row in rows if (parsed := _parse_ts(row["ts"]))],
            columns=("ts", "close"),
        )
        .set_index("ts")
        .sort_index()
    )
    if recorded.empty:
        return None, [], None
    last_bar_ts = recorded.index[-1].to_pydatetime()
    live_price = _PRICE_STREAM.latest()
    market_ts = live_price.ts if live_price else last_bar_ts
    price = live_price.price if live_price else float(recorded.iloc[-1]["close"])
    # The bot's candles always win where available; Binance only supplies
    # pre-start history so longer rolling market deltas work immediately.
    history_start = recorded.index[-1] - pd.Timedelta(days=8)
    binance = _binance_hourly_close_history()
    binance = binance.loc[(binance.index >= history_start) & (binance.index <= recorded.index[-1])]
    history = pd.concat((binance, recorded))
    history = history.loc[~history.index.duplicated(keep="last")].sort_index()
    history = history.loc[history.index <= recorded.index[-1]]
    changes: list[dict[str, object]] = []
    for label, period in (
        ("1h", timedelta(hours=1)),
        ("4h", timedelta(hours=4)),
        ("1d", timedelta(days=1)),
        ("1w", timedelta(days=7)),
    ):
        target = pd.Timestamp(market_ts) - period
        prior_history = history.loc[history.index <= target]
        prior = float(prior_history.iloc[-1]["close"]) if not prior_history.empty else None
        if prior is None and label == "1w":
            daily_history = _binance_daily_close_history()
            daily_prior = daily_history.loc[daily_history.index <= target]
            prior = float(daily_prior.iloc[-1]["close"]) if not daily_prior.empty else None
        change = "-" if prior is None else f"{((price / prior) - 1) * 100:+.2f}%"
        changes.append({"period": label, "change": change})
    return price, changes, last_bar_ts


def _strategy_params(run: dict[str, object], instance_id: str = "ma_cross_primary") -> dict:
    params = _metadata(run.get("strategy_params_json"))
    nested = params.get(instance_id)
    if isinstance(nested, dict) and isinstance(nested.get("params"), dict):
        return nested["params"]
    return params


def _strategy_tolerance(run: dict[str, object]) -> float:
    params = _strategy_params(run)
    try:
        return float(params.get("tolerance_pct", 0.005))
    except (TypeError, ValueError):
        return 0.005


@lru_cache(maxsize=1)
def _binance_hourly_close_history() -> pd.DataFrame:
    """Small historical close series for dashboard-only context and MA warmup."""
    if not BINANCE_HOURLY_CACHE.exists():
        return pd.DataFrame(columns=("close",), index=pd.DatetimeIndex([], tz="UTC"))
    frame = pd.read_parquet(BINANCE_HOURLY_CACHE, columns=["ts", "close"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    return frame.set_index("ts").sort_index()[["close"]]


@lru_cache(maxsize=1)
def _binance_daily_close_history() -> pd.DataFrame:
    """Tiny daily fallback when the hourly cache predates the bot's start."""
    if not BINANCE_DAILY_CACHE.exists():
        return pd.DataFrame(columns=("close",), index=pd.DatetimeIndex([], tz="UTC"))
    frame = pd.read_parquet(BINANCE_DAILY_CACHE, columns=["ts", "close"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True) + pd.Timedelta(days=1)
    return frame.set_index("ts").sort_index()[["close"]]


def _recorded_close_history(db_path: Path) -> pd.DataFrame:
    """Latest recorded close for each timestamp, spanning restarts."""
    rows = _query(
        db_path,
        "SELECT ts, close FROM bars WHERE id IN (SELECT MAX(id) FROM bars GROUP BY ts) "
        "ORDER BY ts DESC LIMIT 50000",
    )
    if not rows:
        return pd.DataFrame(columns=("close",))
    frame = pd.DataFrame(
        [(parsed, float(row["close"])) for row in rows if (parsed := _parse_ts(row["ts"]))],
        columns=("ts", "close"),
    )
    if frame.empty:
        return pd.DataFrame(columns=("close",))
    return frame.set_index("ts").sort_index()


def _persisted_strategy_levels(db_path: Path, tolerance_pct: float) -> dict[str, dict[str, object]]:
    """Return the exact MA state used by the live strategy, when available."""
    try:
        rows = _query(
            db_path,
            "SELECT ts, state_json FROM strategy_state_snapshots "
            "WHERE mode = 'live' AND strategy_name = ? ORDER BY ts DESC LIMIT 1",
            (MA_STRATEGY_NAME,),
        )
    except sqlite3.Error:
        return {}
    if not rows:
        return {}
    state = _metadata(rows[0]["state_json"])
    timeframes = state.get("timeframes")
    if not isinstance(timeframes, dict):
        return {}
    levels: dict[str, dict[str, object]] = {}
    for timeframe in TIMEFRAMES:
        value = timeframes.get(timeframe)
        if not isinstance(value, dict):
            continue
        try:
            sma = float(value["sma"])
            ema = float(value["ema"])
            completed_bar_ts = str(value["last_bar_ts"])
        except (KeyError, TypeError, ValueError):
            continue
        levels[timeframe] = {
            "sma20": sma,
            "ema21": ema,
            "long_trigger": max(sma, ema) * (1 + tolerance_pct),
            "short_trigger": min(sma, ema) * (1 - tolerance_pct),
            "completed_bar_ts": completed_bar_ts,
            "bootstrap_source": "persisted_live_state",
        }
    return levels


def _persisted_cooldowns(db_path: Path) -> dict[str, dict[str, int]]:
    """Return the live strategy's remaining per-timeframe cool-off slots."""
    empty = {timeframe: {"winner": 0, "loss": 0} for timeframe in TIMEFRAMES}
    try:
        rows = _query(
            db_path,
            "SELECT state_json FROM strategy_state_snapshots "
            "WHERE mode = 'live' AND strategy_name = ? ORDER BY ts DESC LIMIT 1",
            (MA_STRATEGY_NAME,),
        )
    except sqlite3.Error:
        return empty
    if not rows:
        return empty
    state = _metadata(rows[0]["state_json"])
    winner = state.get("winner_suppressed_signals")
    loss = state.get("loss_suppressed_signals")
    if not isinstance(winner, dict) or not isinstance(loss, dict):
        return empty
    for timeframe in TIMEFRAMES:
        try:
            empty[timeframe] = {
                "winner": max(0, int(winner.get(timeframe, 0))),
                "loss": max(0, int(loss.get(timeframe, 0))),
            }
        except (TypeError, ValueError):
            continue
    return empty


def _persisted_breakout_state(db_path: Path) -> dict[str, object] | None:
    """Read the funded breakout machine's own snapshot, if it exists."""
    try:
        rows = _query(
            db_path,
            "SELECT ts, state_json FROM strategy_state_snapshots "
            "WHERE mode = 'live' AND strategy_name = ? ORDER BY ts DESC LIMIT 1",
            (BREAKOUT_STRATEGY_NAME,),
        )
    except sqlite3.Error:
        return None
    if not rows:
        return None
    state = _metadata(rows[0]["state_json"])
    state["snapshot_ts"] = rows[0]["ts"]
    return state


@lru_cache(maxsize=1)
def _historical_breakout_reference() -> dict[str, object] | None:
    """Load the build-time replay only when its seed candles still match."""
    if not all(path.is_file() for path in (LNM_DAILY_SEED_CACHE, BREAKOUT_CAMPAIGN_SEED, BREAKOUT_PAPER_REFERENCE)):
        return None
    try:
        reference = json.loads(BREAKOUT_PAPER_REFERENCE.read_text())
        seed = json.loads(BREAKOUT_CAMPAIGN_SEED.read_text())
        expected = seed["active_hypothetical_stack"]
        if (
            hashlib.sha256(BREAKOUT_CAMPAIGN_SEED.read_bytes()).hexdigest()
            != reference["seed_sha256"]
            or hashlib.sha256(LNM_DAILY_SEED_CACHE.read_bytes()).hexdigest()
            != reference["candles_sha256"]
            or reference["source_as_of"] != seed["as_of_close"]
            or reference["campaign_id"] != expected["parent_id"]
            or reference["entry_ts"] != expected["entry_ts"]
            or not math.isclose(reference["boundary"], expected["boundary"], abs_tol=1e-6)
            or len(reference["units"]) != expected["active_units"]
            or not math.isclose(
                reference["units"][0]["entry_price"], expected["entry_price"], abs_tol=1e-6
            )
        ):
            return None
        return reference
    except (KeyError, IndexError, TypeError, ValueError, OSError) as exc:
        _LOG.warning("dashboard.historical_breakout_reference_unavailable: %s", exc)
        return None


def _breakout_context(
    state: dict[str, object] | None, positions: list[dict[str, object]]
) -> dict[str, object]:
    owned = [position for position in positions if position.get("strategy") == BREAKOUT_INSTANCE_ID]
    machine = state.get("machine") if state else None
    machine = machine if isinstance(machine, dict) else {}
    campaign = machine.get("campaign")
    campaign = campaign if isinstance(campaign, dict) else None
    return {
        "owned": owned,
        "campaign": campaign,
        "last_daily_bar": machine.get("last_bar_ts"),
        "snapshot_ts": state.get("snapshot_ts") if state else None,
        "closing_slots": state.get("closing_slots", []) if state else [],
        "unit_notional_usd": state.get("unit_notional_usd", 100) if state else 100,
        "historical_unit_notional_usd": (
            state.get("historical_unit_notional_usd", state.get("unit_notional_usd", 100))
            if state else 100
        ),
        "leverage": state.get("leverage", 5) if state else 5,
        "pending_exit": machine.get("pending_exit"),
        "available": state is not None,
    }


def _historical_paper_position(
    context: dict[str, object], mark: float | None
) -> dict[str, object] | None:
    """Mark a verified, unowned campaign without touching funded accounting."""
    campaign = context.get("campaign")
    if (
        not isinstance(campaign, dict)
        or campaign.get("origin") != "historical"
        or context.get("owned")
    ):
        return None
    reference = _historical_breakout_reference()
    if reference is None:
        return None
    units = reference["units"]
    if not isinstance(units, list) or not units:
        return None
    parent = units[0]
    if (
        campaign.get("campaign_id") != reference["campaign_id"]
        or campaign.get("entry_ts") != reference["entry_ts"]
        or not math.isclose(float(campaign.get("boundary") or 0), float(reference["boundary"]))
        or not math.isclose(
            float(campaign.get("units", [{}])[0].get("entry_price") or 0),
            float(parent["entry_price"]),
            abs_tol=1e-6,
        )
        or int(campaign.get("lifetime_units") or 0) != len(units)
    ):
        return None
    notional = float(context["historical_unit_notional_usd"])
    leverage = float(context["leverage"])
    if notional <= 0 or leverage <= 0:
        return None
    mark = float(mark) if mark is not None else None
    if mark is not None and (not math.isfinite(mark) or mark <= 0):
        mark = None
    side = int(campaign["side"])
    weighted_entry = len(units) / sum(1 / float(unit["entry_price"]) for unit in units)
    gross_btc = (
        sum(
            side * notional * (1 / float(unit["entry_price"]) - 1 / mark)
            for unit in units
        )
        if mark is not None
        else None
    )
    return {
        **reference,
        "mark": mark,
        "side": side,
        "unit_notional_usd": notional,
        "leverage": leverage,
        "total_notional_usd": notional * len(units),
        "initial_margin_usd": notional * len(units) / leverage,
        "weighted_entry": weighted_entry,
        "gross_sats": round(gross_btc * 1e8) if gross_btc is not None else None,
        "gross_usd": gross_btc * mark if gross_btc is not None and mark else None,
        "held_days": int(campaign.get("held_days") or 0),
        "peak_favorable_pct": float(campaign.get("peak_favorable") or 0) * 100,
        "pending_exit": context.get("pending_exit"),
    }


def _breakout_unit_details(rows: list[dict[str, object]], *, paper: bool = False) -> SafeHtml:
    """Keep the breakout stack's K slots below its single summary row."""
    label = "paper" if paper else "funded"
    note = (
        "<p class=muted>Paper gross estimates exclude fees, funding and liquidation. "
        "No venue trades or wallet P&amp;L are recorded for this historical campaign.</p>"
        if paper else ""
    )
    return SafeHtml(
        '<details data-preserve-open class="stack-drilldown">'
        f'<summary>Show {len(rows)} {label} K units</summary>'
        + _table(
            "Breakout K units",
            rows,
            ("slot", "status", "entry_ts", "contracts", "leverage", "entry_price", "margin", "funding", "mark_pnl"),
            compact=True,
        )
        + note
        + "</details>"
    )


def _breakout_card(context: dict[str, object], denomination: str, btc_price: float | None) -> str:
    campaign = context["campaign"]
    owned = context["owned"]
    assert isinstance(owned, list)
    if not context["available"]:
        status = "Awaiting state"
        detail = "No breakout strategy snapshot yet"
        extra = ""
        card_class = "flat"
    elif not isinstance(campaign, dict):
        status = "State mismatch" if owned else "Flat"
        detail = (
            "Funded units exist without a campaign snapshot"
            if owned
            else "Watching completed daily candles"
        )
        extra = ""
        card_class = "flat"
    else:
        origin = str(campaign.get("origin") or "")
        side = "Long" if campaign.get("side") == 1 else "Short"
        campaign_id = html.escape(str(campaign.get("campaign_id") or "—"))
        boundary = _format_price(campaign.get("boundary"))
        if origin == "historical":
            status = "State mismatch" if owned else "Historical campaign"
            paper = _historical_paper_position(context, btc_price)
            detail = (
                "Historical state has funded units"
                if owned
                else f"{side} {campaign_id} · no funded units · new parent blocked"
            )
            if paper is not None:
                detail += f" · {len(paper['units'])} paper units"
            card_class = "flat"
        else:
            status = f"{side} · {len(owned)}/4 units" if owned else "Closing campaign"
            notional = sum(int(position.get("contracts") or 0) for position in owned)
            detail = f"{campaign_id} · ${notional:,} notional"
            card_class = side.lower() if owned else "flat"
        extra = (
            f"<small>Original range {boundary} · "
            f"{int(campaign.get('lifetime_units') or 0)}/4 lifetime entries</small>"
        )
    last_daily = context.get("last_daily_bar")
    updated = (
        f"<small>Latest daily candle {html.escape(str(last_daily)[:10])}</small>"
        if last_daily
        else ""
    )
    pnl = ""
    if owned:
        values = [position.get("estimated_unrealized_sats") for position in owned]
        if all(isinstance(value, int) for value in values):
            pnl = (
                "<small>Open P&amp;L "
                + _signed_amount_html(sum(values), denomination, btc_price)
                + "</small>"
            )
    elif isinstance(campaign, dict) and campaign.get("origin") == "historical":
        paper = _historical_paper_position(context, btc_price)
        if paper is not None:
            pnl = (
                "<small>Paper gross mark "
                + _signed_amount_html(paper["gross_sats"], denomination, btc_price)
                + " · excluded from account totals</small>"
            )
    return (
        f'<article class="card position-card breakout-card {card_class}">'
        "<p>Close-range breakout</p>"
        f"<strong>{html.escape(status)}</strong>"
        f"<small>{detail}</small>{extra}{pnl}{updated}</article>"
    )


def _breakout_activity_rows(
    db_path: Path, state: dict[str, object] | None, paper: dict[str, object] | None
) -> list[dict[str, object]]:
    """Put emitted signals, recent decisions and the active seed trail in one timeline."""
    rows: list[dict[str, object]] = []

    def metric(value: object, places: int) -> str:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return "-"
        return f"{number:.{places}f}" if math.isfinite(number) else "-"

    def add(
        *, source: str, signal_ts: object, action_ts: object, slot: object,
        kind: object, reason: object, signal_close: object = None,
        boundary: object = None, distance: object = None, overlap: object = None,
    ) -> None:
        rows.append(
            {
                "source": source,
                "signal_ts": signal_ts or "-",
                "action_ts": action_ts or "-",
                "slot": slot or "-",
                "kind": kind or "-",
                "reason": reason or "-",
                "signal_close": _format_price(signal_close),
                "range_boundary": _format_price(boundary),
                "ema_distance_atr": metric(distance, 2),
                "overlap_10": metric(overlap, 3),
            }
        )

    emitted = _signals(db_path, tf="breakout")[:5]
    for signal in emitted:
        add(
            source="Recorded signal", signal_ts=signal["signal_ts"], action_ts=signal["ts"],
            slot=signal["slot"], kind=signal["kind"], reason=signal["reason"],
            signal_close=signal["signal_close"], boundary=signal["range_boundary"],
            distance=signal["distance_ema_atr"], overlap=signal["average_overlap10"],
        )
    emitted_actions = {(str(row["ts"]), str(row["slot"])) for row in emitted}
    decisions = state.get("recent_decisions", []) if state else []
    if isinstance(decisions, list):
        for decision in decisions[-8:]:
            if not isinstance(decision, dict):
                continue
            slot = f"k{decision['k']}" if decision.get("k") is not None else "-"
            if decision.get("kind") in {"paper_parent", "paper_addon", "campaign_exit"} and (
                str(decision.get("ts")), slot
            ) in emitted_actions:
                continue
            meta = _metadata(decision.get("metadata"))
            add(
                source="Live decision", signal_ts=meta.get("signal_ts") or decision.get("ts"),
                action_ts=decision.get("ts"), slot=slot, kind=decision.get("kind"),
                reason=decision.get("reason"), signal_close=meta.get("signal_close"),
                boundary=meta.get("boundary"), distance=meta.get("distance_ema_atr"),
                overlap=meta.get("average_overlap10"),
            )
    if paper is not None:
        for event in paper["signal_trail"]:
            result = str(event["result"])
            add(
                source="Historical replay", signal_ts=event["signal_ts"],
                action_ts=event["action_ts"],
                slot=result[:2].lower() if result.startswith("K") else "-",
                kind="paper entry" if result.startswith("K") else "paper decision",
                reason=result, signal_close=event["signal_close"],
                boundary=event["range_boundary"], distance=event["distance_ema_atr"],
                overlap=event["average_overlap10"],
            )
    rows.sort(
        key=lambda row: _parse_ts(row["action_ts"]) or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    return rows


def _ma_levels(db_path: Path, tolerance_pct: float) -> dict[str, dict[str, object]]:
    """Return strategy MA levels, preferring the state used for execution."""
    persisted = _persisted_strategy_levels(db_path, tolerance_pct)
    if persisted:
        return persisted
    recorded = _recorded_close_history(db_path)
    if recorded.empty:
        return {}
    last_source_ts = recorded.index[-1]
    warmup_start = last_source_ts - pd.Timedelta(days=35)
    binance = _binance_hourly_close_history()
    bootstrap = binance.loc[(binance.index >= warmup_start) & (binance.index <= last_source_ts)]
    frame = pd.concat((bootstrap, recorded))
    frame = frame.loc[~frame.index.duplicated(keep="last")].sort_index()
    out: dict[str, dict[str, object]] = {}
    for timeframe, frequency in (("4h", "4h"), ("1d", "1D")):
        closes = frame["close"].resample(frequency, label="right", closed="left").last().dropna()
        closes = closes[closes.index <= last_source_ts]
        if len(closes) < 21:
            continue
        values = closes.tolist()
        sma = sum(values[-20:]) / 20
        ema = sum(values[:21]) / 21
        alpha = 2 / 22
        for close in values[21:]:
            ema = close * alpha + ema * (1 - alpha)
        out[timeframe] = {
            "sma20": sma,
            "ema21": ema,
            "long_trigger": max(sma, ema) * (1 + tolerance_pct),
            "short_trigger": min(sma, ema) * (1 - tolerance_pct),
            "completed_bar_ts": closes.index[-1].isoformat(),
            "bootstrap_source": "binance" if not bootstrap.empty else "recorded",
        }
    return out


def _position_status_rows(
    positions: list[dict[str, object]],
    levels: dict[str, dict[str, object]],
    denomination: str,
    btc_price: float | None,
    breakout: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    by_timeframe = {
        str(position["timeframe"]): position
        for position in positions
        if position.get("strategy") in {"legacy_ma", "ma_cross_primary"}
    }
    rows: list[dict[str, object]] = []
    for timeframe in TIMEFRAMES:
        position = by_timeframe.get(timeframe)
        level = levels.get(timeframe)
        side = str(position["side"]) if position else "flat"
        rows.append(
            {
                "strategy": position.get("strategy", "ma_cross_primary")
                if position
                else "ma_cross_primary",
                "status": "Open" if position else "Flat",
                "slot": position.get("slot", timeframe) if position else timeframe,
                "timeframe": timeframe,
                "side": side,
                "contracts": (
                    f"${int(position['contracts']):,}"
                    + (
                        f" · {position['entry_adjustment']}"
                        if position.get("entry_adjustment")
                        else ""
                    )
                    if position
                    else "-"
                ),
                "leverage": position.get("leverage", "-") if position else "-",
                "entry_ts": position.get("entry_ts", "-") if position else "-",
                "entry_price": _format_price(position.get("entry_price")) if position else "-",
                "mark_pnl": (
                    _format_signed_amount(
                        position.get("estimated_unrealized_sats"), denomination, btc_price
                    )
                    if position
                    else "-"
                ),
                "margin": (
                    _format_amount(position.get("margin_sats"), denomination, btc_price)
                    if position
                    else "-"
                ),
                "funding": (
                    _format_signed_amount(
                        position.get("accumulated_funding_sats"),
                        denomination,
                        btc_price,
                        invert=True,
                    )
                    if position
                    else "-"
                ),
                "long_trigger": _format_price(level["long_trigger"]) if level else "-",
                "short_trigger": _format_price(level["short_trigger"]) if level else "-",
                "exit_trigger": (
                    _format_price(level["short_trigger"])
                    if level and side == "long"
                    else _format_price(level["long_trigger"])
                    if level and side == "short"
                    else "-"
                ),
            }
        )
    if breakout is not None:
        campaign = breakout["campaign"]
        owned = breakout["owned"]
        assert isinstance(owned, list)
        paper = _historical_paper_position(breakout, btc_price)
        side = (
            "long"
            if isinstance(campaign, dict) and campaign.get("side") == 1
            else "short"
            if isinstance(campaign, dict)
            else str(owned[0]["side"])
            if owned and len({position["side"] for position in owned}) == 1
            else "mixed"
            if owned
            else "flat"
        )
        if not breakout["available"]:
            status = "State mismatch" if owned else "Awaiting state"
        elif isinstance(campaign, dict) and campaign.get("origin") == "historical":
            status = "State mismatch" if owned else "Historical · no funded trade"
            if paper is not None:
                status += (
                    f" · {len(paper['units'])} paper units · day {paper['held_days']}/120"
                    f" · peak +{paper['peak_favorable_pct']:.2f}%"
                )
        elif isinstance(campaign, dict):
            status = f"Funded · {len(owned)}/4 units" if owned else "Closing"
        else:
            status = "State mismatch" if owned else "Flat"

        funded_notional = sum(int(position.get("contracts") or 0) for position in owned)
        funded_prices = [
            (int(position.get("contracts") or 0), float(position.get("entry_price") or 0))
            for position in owned
        ]
        funded_entry = (
            funded_notional / sum(size / price for size, price in funded_prices)
            if funded_notional and all(size > 0 and price > 0 for size, price in funded_prices)
            else None
        )
        funded_leverages = {position.get("leverage") for position in owned}
        funded_pnl = [position.get("estimated_unrealized_sats") for position in owned]
        funded_margins = [position.get("margin_sats") for position in owned]
        campaign_row = {
            "strategy": BREAKOUT_INSTANCE_ID,
            "status": status,
            "slot": "campaign",
            "timeframe": "1d",
            "side": side,
            "contracts": (
                f"${paper['total_notional_usd']:,.0f} paper"
                if paper is not None else f"${funded_notional:,}" if owned else "-"
            ),
            "leverage": (
                f"{paper['leverage']:g}x paper"
                if paper is not None else next(iter(funded_leverages))
                if len(funded_leverages) == 1 else "mixed" if owned else "-"
            ),
            "entry_ts": campaign.get("entry_ts", "-") if isinstance(campaign, dict) else "-",
            "entry_price": _format_price(
                paper["weighted_entry"] if paper is not None else funded_entry
            ),
            "mark_pnl": (
                f"{_format_signed_amount(paper['gross_sats'], denomination, btc_price)} paper gross"
                if paper is not None else _format_signed_amount(
                    sum(funded_pnl) if all(isinstance(value, int) for value in funded_pnl) else None,
                    denomination, btc_price,
                ) if owned else "-"
            ),
            "margin": (
                f"${paper['initial_margin_usd']:,.0f} paper"
                if paper is not None else _format_amount(
                    sum(funded_margins) if all(isinstance(value, int) for value in funded_margins) else None,
                    denomination, btc_price,
                ) if owned else "-"
            ),
            "funding": (
                "not modeled" if paper is not None else _format_signed_amount(
                    sum(int(position.get("accumulated_funding_sats") or 0) for position in owned),
                    denomination, btc_price, invert=True,
                ) if owned else "-"
            ),
            "long_trigger": "-",
            "short_trigger": "-",
            "exit_trigger": (
                f"range {_format_price(campaign.get('boundary'))} / recovery d85 / 120d / liquidation"
                if isinstance(campaign, dict) else "-"
            ),
        }
        if paper is not None:
            mark = paper["mark"]
            notional = float(paper["unit_notional_usd"])
            unit_rows = []
            for unit in paper["units"]:
                entry = float(unit["entry_price"])
                gross_sats = (
                    round(paper["side"] * notional * (1 / entry - 1 / mark) * 1e8)
                    if mark else None
                )
                unit_rows.append(
                    {
                        "slot": f"k{unit['k']}",
                        "status": "Paper parent" if unit["k"] == 0 else "Paper add-on",
                        "entry_ts": unit["entry_ts"],
                        "contracts": f"${notional:,.0f} paper",
                        "leverage": f"{paper['leverage']:g}x paper",
                        "entry_price": _format_price(entry),
                        "margin": f"${notional / paper['leverage']:,.0f} paper",
                        "funding": "not modeled",
                        "mark_pnl": (
                            f"{_format_signed_amount(gross_sats, denomination, btc_price)} paper gross"
                            if mark else "-"
                        ),
                    }
                )
            campaign_row["_details_html"] = _breakout_unit_details(unit_rows, paper=True)
        elif owned:
            unit_rows = [
                {
                    "slot": position.get("slot", "-"),
                    "status": "Open",
                    "entry_ts": position.get("entry_ts", "-"),
                    "contracts": f"${int(position['contracts']):,}",
                    "leverage": position.get("leverage", "-"),
                    "entry_price": _format_price(position.get("entry_price")),
                    "margin": _format_amount(position.get("margin_sats"), denomination, btc_price),
                    "funding": _format_signed_amount(
                        position.get("accumulated_funding_sats"), denomination, btc_price,
                        invert=True,
                    ),
                    "mark_pnl": _format_signed_amount(
                        position.get("estimated_unrealized_sats"), denomination, btc_price
                    ),
                }
                for position in sorted(owned, key=lambda position: str(position.get("slot")))
            ]
            campaign_row["_details_html"] = _breakout_unit_details(unit_rows)
        rows.append(campaign_row)
    return rows


def _closed_trade_components(db_path: Path) -> list[dict[str, object]]:
    """Return exact completed isolated-trade P&L components at close time."""
    grouped: dict[str, dict[str, dict[str, object]]] = {}
    for order in reversed(_orders(db_path)):
        trade_id = str(order.get("trade_id") or "")
        if not trade_id:
            continue
        trade = grouped.setdefault(trade_id, {})
        if order["action"] == "open":
            trade["open"] = order
        elif order["action"] in {"close", "external_close"}:
            trade["close"] = order
    funding = _funding_by_trade(db_path)
    events: list[dict[str, object]] = []
    for trade_id, trade in grouped.items():
        opened = trade.get("open")
        closed = trade.get("close")
        if not opened or not closed:
            continue
        closed_ts = _parse_ts(closed.get("ts"))
        if closed_ts is None:
            continue
        gross = int(closed.get("gross_pl_sats") or 0)
        trading_fees = -(
            int(opened.get("opening_fee_sats") or 0) + int(closed.get("closing_fee_sats") or 0)
        )
        funding_pnl = -funding.get(trade_id, 0)
        opened_ts = _parse_ts(opened.get("ts"))
        entry_notional_usd = int(opened.get("qty_sats") or 0)
        exit_price_usd = float(closed.get("price_usd") or 0)
        return_scale = (
            exit_price_usd / 1e8 / entry_notional_usd * 100
            if entry_notional_usd > 0 and exit_price_usd > 0
            else 0.0
        )
        events.append(
            {
                "closed_at": closed_ts,
                "opened_at": opened_ts,
                "timeframe": opened.get("trigger_tf", "-"),
                "strategy": opened.get("strategy", "legacy_ma"),
                "slot": opened.get("slot", opened.get("trigger_tf", "-")),
                "gross": gross,
                "trading_fees": trading_fees,
                "funding": funding_pnl,
                "net": gross + trading_fees + funding_pnl,
                "entry_notional_usd": entry_notional_usd,
                "exit_price_usd": exit_price_usd,
                "gross_return_pct": gross * return_scale,
                "trading_fees_return_pct": trading_fees * return_scale,
                "funding_return_pct": funding_pnl * return_scale,
                "net_return_pct": (gross + trading_fees + funding_pnl) * return_scale,
                "hold_hours": (closed_ts - opened_ts).total_seconds() / 3600 if opened_ts else None,
            }
        )
    return events


def _closed_trade_pnl_events(db_path: Path) -> list[tuple[datetime, int]]:
    return [
        (event["closed_at"], int(event["net"]))
        for event in _closed_trade_components(db_path)
        if isinstance(event.get("closed_at"), datetime)
    ]


def _pnl_summary(
    db_path: Path, positions: list[dict[str, object]], now: datetime | None = None
) -> list[dict[str, object]]:
    now = now or datetime.now(UTC)
    closed_events = _closed_trade_components(db_path)
    open_gross = sum(
        int(position["estimated_unrealized_sats"])
        for position in positions
        if isinstance(position.get("estimated_unrealized_sats"), int)
    )
    open_trading_fees = -sum(int(position.get("opening_fee_sats") or 0) for position in positions)
    open_funding = -sum(
        int(position.get("accumulated_funding_sats") or 0) for position in positions
    )
    result: list[dict[str, object]] = []
    for key, label, window in (
        ("1day", "1 day", timedelta(days=1)),
        ("7days", "7 days", timedelta(days=7)),
        ("30days", "30 days", timedelta(days=30)),
        ("alltime", "All time", None),
    ):
        selected = [
            event for event in closed_events if window is None or event["closed_at"] >= now - window
        ]
        gross = sum(int(event["gross"]) for event in selected) + open_gross
        trading_fees = sum(int(event["trading_fees"]) for event in selected) + open_trading_fees
        funding = sum(int(event["funding"]) for event in selected) + open_funding
        result.append(
            {
                "key": key,
                "period": label,
                "gross": gross,
                "trading_fees": trading_fees,
                "funding": funding,
                "net": gross + trading_fees + funding,
            }
        )
    return result


def _open_return_components(
    positions: list[dict[str, object]], btc_price: float | None
) -> dict[str, float]:
    components = {"gross": 0.0, "trading_fees": 0.0, "funding": 0.0}
    if not btc_price:
        return components
    for position in positions:
        notional_usd = int(position.get("contracts") or 0)
        gross_sats = position.get("estimated_unrealized_sats")
        if notional_usd <= 0 or not isinstance(gross_sats, int):
            continue
        scale = btc_price / 1e8 / notional_usd
        components["gross"] += gross_sats * scale
        components["trading_fees"] -= int(position.get("opening_fee_sats") or 0) * scale
        components["funding"] -= int(position.get("accumulated_funding_sats") or 0) * scale
    return components


def _constant_notional_pnl_summary(
    db_path: Path,
    positions: list[dict[str, object]],
    nominal_usd: float,
    btc_price: float | None,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    """Replay each trade at one USD notional, independent of actual sizing."""
    now = now or datetime.now(UTC)
    closed_events = _closed_trade_components(db_path)
    open_returns = _open_return_components(positions, btc_price)
    result: list[dict[str, object]] = []
    for key, label, window in (
        ("1day", "1 day", timedelta(days=1)),
        ("7days", "7 days", timedelta(days=7)),
        ("30days", "30 days", timedelta(days=30)),
        ("alltime", "All time", None),
    ):
        selected = [
            event for event in closed_events if window is None or event["closed_at"] >= now - window
        ]
        gross = nominal_usd * (
            sum(float(event["gross_return_pct"]) for event in selected) / 100
            + open_returns["gross"]
        )
        trading_fees = nominal_usd * (
            sum(float(event["trading_fees_return_pct"]) for event in selected) / 100
            + open_returns["trading_fees"]
        )
        funding = nominal_usd * (
            sum(float(event["funding_return_pct"]) for event in selected) / 100
            + open_returns["funding"]
        )
        net = gross + trading_fees + funding
        result.append(
            {
                "key": key,
                "period": label,
                "gross": gross,
                "trading_fees": trading_fees,
                "funding": funding,
                "net": net,
            }
        )
    return result


def _calendar_pnl_rows(db_path: Path, granularity: str) -> list[dict[str, object]]:
    """Aggregate realized trading P&L and funding by calendar period."""
    realized_rows = _query(
        db_path,
        "SELECT date, SUM(realized_pnl_sats) AS realized FROM daily_pnl GROUP BY date",
    )
    funding_rows = _query(
        db_path,
        "SELECT substr(ts, 1, 10) AS date, -SUM(fee_sats) AS funding "
        "FROM funding_fees GROUP BY substr(ts, 1, 10)",
    )
    by_date: dict[str, int] = {str(row["date"]): int(row["realized"] or 0) for row in realized_rows}
    for row in funding_rows:
        date = str(row["date"])
        by_date[date] = by_date.get(date, 0) + int(row["funding"] or 0)

    periods: dict[str, int] = {}
    for date, net in by_date.items():
        try:
            parsed = datetime.fromisoformat(date).date()
        except ValueError:
            continue
        if granularity == "weekly":
            iso_year, iso_week, _ = parsed.isocalendar()
            label = f"{iso_year}-W{iso_week:02d}"
        elif granularity == "monthly":
            label = parsed.strftime("%Y-%m")
        else:
            label = parsed.isoformat()
        periods[label] = periods.get(label, 0) + net
    return [{"period": period, "net": net} for period, net in sorted(periods.items(), reverse=True)]


def _constant_notional_calendar_pnl_rows(
    db_path: Path, granularity: str, nominal_usd: float
) -> list[dict[str, object]]:
    periods: dict[str, float] = {}
    for event in _closed_trade_components(db_path):
        closed_at = event.get("closed_at")
        if not isinstance(closed_at, datetime):
            continue
        if granularity == "weekly":
            iso_year, iso_week, _ = closed_at.date().isocalendar()
            label = f"{iso_year}-W{iso_week:02d}"
        elif granularity == "monthly":
            label = closed_at.strftime("%Y-%m")
        else:
            label = closed_at.date().isoformat()
        periods[label] = periods.get(label, 0.0) + (
            float(event["net_return_pct"]) / 100 * nominal_usd
        )
    return [{"period": period, "net": net} for period, net in sorted(periods.items(), reverse=True)]


def _paginate(
    rows: list[dict[str, object]], page: int, page_size: int
) -> tuple[list[dict[str, object]], int, int]:
    total_pages = max(1, (len(rows) + page_size - 1) // page_size)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * page_size
    return rows[start : start + page_size], page, total_pages


def _pagination(
    page: int,
    total_pages: int,
    denomination: str,
    granularity: str,
    pnl_basis: str,
) -> str:
    if total_pages <= 1:
        return ""
    base = {"denom": denomination, "pnl_granularity": granularity}
    if pnl_basis == "constant":
        base["pnl_basis"] = "constant"
    previous = ""
    if page > 1:
        previous_query = {**base, "pnl_page": str(page - 1)}
        previous = f'<a href="/pnl?{urlencode(previous_query)}">← Newer</a>'
    following = ""
    if page < total_pages:
        next_query = {**base, "pnl_page": str(page + 1)}
        following = f'<a href="/pnl?{urlencode(next_query)}">Older →</a>'
    return f'<nav class="pagination">{previous}<span>Page {page} of {total_pages}</span>{following}</nav>'


def _periodic_pnl_table(
    title: str,
    rows: list[dict[str, object]],
    controls: str,
) -> str:
    """Render twelve calendar periods as three four-row period/net columns."""
    headers = "<th>period</th><th>net</th>" * 3
    body_rows: list[str] = []
    for row_index in range(4):
        cells: list[str] = []
        for column_index in range(3):
            item_index = column_index * 4 + row_index
            item = rows[item_index] if item_index < len(rows) else None
            period = html.escape(str(item["period"])) if item else ""
            net = _render_cell_value(item["net"]) if item else ""
            cells.extend((f"<td>{period}</td>", f"<td>{net}</td>"))
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        '<section class="periodic-pnl"><div class="table-heading">'
        f"<h2>{html.escape(title)}</h2><div class=period-controls>{controls}</div></div>"
        "<div class=table-wrap><table><thead><tr>"
        f"{headers}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div></section>"
    )


def _strategy_performance_rows(
    db_path: Path,
    denomination: str,
    btc_price: float | None,
    nominal_usd: float | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    events = sorted(_closed_trade_components(db_path), key=lambda event: event["closed_at"])

    def group(strategy: object, timeframe: object) -> str:
        if strategy == BREAKOUT_INSTANCE_ID:
            return "Breakout units"
        if strategy in {"ma_cross_primary", "legacy_ma"} and timeframe in TIMEFRAMES:
            return f"MA {timeframe}"
        return str(strategy or "Other")

    labels = ("MA 1d", "MA 4h", "Breakout units", "Combined")
    open_started: dict[str, list[datetime]] = {label: [] for label in labels}
    grouped: dict[str, dict[str, dict[str, object]]] = {}
    for order in reversed(_orders(db_path)):
        trade_id = str(order.get("trade_id") or "")
        if not trade_id or order.get("action") not in {"open", "close"}:
            continue
        grouped.setdefault(trade_id, {})[str(order["action"])] = order
    for trade in grouped.values():
        opened = trade.get("open")
        if not opened or trade.get("close"):
            continue
        opened_at = _parse_ts(opened.get("ts"))
        label = group(opened.get("strategy"), opened.get("trigger_tf"))
        if opened_at and label in open_started:
            open_started[label].append(opened_at)

    quality_rows: list[dict[str, object]] = []
    risk_rows: list[dict[str, object]] = []
    now = datetime.now(UTC)
    for timeframe in labels:
        selected = (
            events
            if timeframe == "Combined"
            else [
                event
                for event in events
                if group(event["strategy"], event["timeframe"]) == timeframe
            ]
        )
        if not selected:
            continue
        nets: list[float] = (
            [float(event["net_return_pct"]) / 100 * nominal_usd for event in selected]
            if nominal_usd is not None
            else [float(event["net"]) for event in selected]
        )
        winners = [net for net in nets if net > 0]
        losers = [net for net in nets if net < 0]
        hold_hours = [
            float(event["hold_hours"]) for event in selected if event["hold_hours"] is not None
        ]
        profit_factor = sum(winners) / abs(sum(losers)) if losers else None
        payoff_ratio = (
            (sum(winners) / len(winners)) / abs(sum(losers) / len(losers))
            if winners and losers
            else None
        )

        def format_pnl(value: float) -> SafeHtml | str:
            if nominal_usd is not None:
                return _signed_usd_html(value)
            return _format_signed_amount(round(value), denomination, btc_price)

        quality_row: dict[str, object] = {
            "timeframe": timeframe,
            "closed_trades": len(selected),
            "win_rate": f"{len(winners) / len(selected):.1%}",
            "avg_winner": format_pnl(sum(winners) / len(winners)) if winners else "-",
            "avg_loser": format_pnl(sum(losers) / len(losers)) if losers else "-",
            "payoff_ratio": f"{payoff_ratio:.2f}" if payoff_ratio is not None else "-",
            "profit_factor": f"{profit_factor:.2f}" if profit_factor is not None else "-",
        }
        quality_rows.append(quality_row)

        equity = 0
        peak = 0
        max_drawdown = 0
        win_streak = loss_streak = max_win_streak = max_loss_streak = 0
        for net in nets:
            equity += net
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)
            if net > 0:
                win_streak += 1
                loss_streak = 0
            elif net < 0:
                loss_streak += 1
                win_streak = 0
            else:
                win_streak = loss_streak = 0
            max_win_streak = max(max_win_streak, win_streak)
            max_loss_streak = max(max_loss_streak, loss_streak)

        intervals = [
            (event["opened_at"], event["closed_at"])
            for event in selected
            if isinstance(event.get("opened_at"), datetime)
            and isinstance(event.get("closed_at"), datetime)
        ]
        if timeframe == "Combined":
            active_starts = [start for values in open_started.values() for start in values]
        else:
            active_starts = open_started[timeframe]
        intervals.extend((start, now) for start in active_starts)
        intervals.sort(key=lambda interval: interval[0])
        merged: list[tuple[datetime, datetime]] = []
        for start, end in intervals:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        elapsed_hours = (now - merged[0][0]).total_seconds() / 3600 if merged else 0
        exposure_hours = sum((end - start).total_seconds() / 3600 for start, end in merged)
        exposure = exposure_hours / elapsed_hours * 100 if elapsed_hours else None
        risk_rows.append(
            {
                "timeframe": timeframe,
                "avg_trade": format_pnl(sum(nets) / len(nets)),
                "best_trade": format_pnl(max(nets)),
                "worst_trade": format_pnl(min(nets)),
                "max_closed_drawdown": format_pnl(-max_drawdown),
                "longest_streaks": f"W{max_win_streak} · L{max_loss_streak}",
                "avg_hold": f"{sum(hold_hours) / len(hold_hours):.1f}h" if hold_hours else "-",
                "time_in_market": f"{exposure:.1f}%" if exposure is not None else "-",
            }
        )
    return quality_rows, risk_rows


def _account_profitability_rows(
    exchange: ExchangeSnapshot | None, denomination: str, btc_price: float | None
) -> list[dict[str, object]]:
    if exchange is None:
        return []
    net_deposits = exchange.deposits_sats - exchange.withdrawals_sats
    adjusted_pnl = exchange.total_sats - net_deposits
    return_pct = adjusted_pnl / net_deposits * 100 if net_deposits > 0 else None
    return [
        {
            "equity": _amount_html(exchange.total_sats, denomination, btc_price),
            "deposits": _amount_html(exchange.deposits_sats, denomination, btc_price),
            "withdrawals": _amount_html(exchange.withdrawals_sats, denomination, btc_price),
            "net_deposits": _amount_html(net_deposits, denomination, btc_price),
            "cashflow_adjusted_pnl": _format_signed_amount(adjusted_pnl, denomination, btc_price),
            "return_on_net_deposits": _signed_percent_html(return_pct),
        }
    ]


def _pnl_basis_controls(denomination: str, granularity: str, pnl_basis: str) -> str:
    common = {"denom": denomination, "pnl_granularity": granularity}
    actual_url = "/pnl?" + urlencode(common)
    constant_query = {**common, "pnl_basis": "constant"}
    constant_url = "/pnl?" + urlencode(constant_query)
    return (
        '<nav class="pnl-basis-actions" aria-label="P&amp;L basis">'
        f'<a class="period-toggle{" active" if pnl_basis == "actual" else ""}" '
        f'href="{html.escape(actual_url, quote=True)}">Actual</a>'
        f'<a class="period-toggle{" active" if pnl_basis == "constant" else ""}" '
        f'href="{html.escape(constant_url, quote=True)}">Constant notional</a>'
        "</nav>"
    )


def _portfolio_panel() -> str:
    """Optional read-only development book; never mixed into live account totals."""
    configured = os.environ.get("LNMBOT_PORTFOLIO_SHADOW_DB")
    if not configured:
        return ""
    try:
        overview = read_overview(Path(configured))
    except (sqlite3.Error, ValueError, OSError):
        return "<section><h2>Breakout shadow</h2><p>Shadow book unavailable.</p></section>"
    occupancy = []
    for seed in overview["seeds"]:
        campaign = seed["observation"].get("active_hypothetical_stack") or {}
        observed = _parse_ts(seed["close_ts"])
        fresh = observed is not None and datetime.now(UTC) - observed <= timedelta(hours=36)
        occupancy.append(
            {
                "strategy": seed["instance_id"],
                "campaign": campaign.get("parent_id", "None"),
                "side": campaign.get("side", "—"),
                "parent_entry": campaign.get("entry_ts", "—"),
                "boundary": _format_price(campaign.get("boundary")),
                "new_parent": "Blocked by historical campaign"
                if seed["parent_occupied"]
                else "Requires runtime admission",
                "observed_through": seed["close_ts"],
                "freshness": "Recent snapshot" if fresh else "Stale snapshot",
            }
        )
    runtimes = []
    for runtime in overview["runtimes"]:
        campaign = runtime["state"].get("campaign") or {}
        observed = _parse_ts(runtime["last_candle_ts"])
        fresh = observed is not None and datetime.now(UTC) - observed <= timedelta(hours=36)
        runtimes.append(
            {
                "strategy": runtime["instance_id"],
                "last_candle": runtime["last_candle_ts"],
                "campaign": campaign.get("campaign_id", "None"),
                "origin": campaign.get("origin", "—"),
                "lifetime_units": campaign.get("lifetime_units", 0),
                "known_fills": len(campaign.get("units", [])),
                "pending_exit": runtime["state"].get("pending_exit") or "—",
                "freshness": "Recent state" if fresh else "Stale state",
            }
        )
    return (
        "<section><h2>Breakout shadow — no orders</h2>"
        "<p>Historical occupancy is hypothetical. It has no owned trades, collateral "
        "or live profit. Forward accounting below is separate from MA and live account totals.</p>"
        + _table(
            "Historical campaign context",
            occupancy,
            (
                "strategy",
                "campaign",
                "side",
                "parent_entry",
                "boundary",
                "new_parent",
                "observed_through",
                "freshness",
            ),
        )
        + _table(
            "Forward machine",
            runtimes,
            (
                "strategy",
                "last_candle",
                "campaign",
                "origin",
                "lifetime_units",
                "known_fills",
                "pending_exit",
                "freshness",
            ),
        )
        + _table(
            "Attributed book accounting (sats)",
            overview["strategies"],
            (
                "id",
                "mode",
                "realized_sats",
                "fee_sats",
                "funding_sats",
                "net_sats",
                "closed_trades",
                "winning_trades",
            ),
        )
        + "</section>"
    )


def _strategy_accounting_panel(
    db_path: Path, denomination: str, btc_price: float | None, *, breakout_enabled: bool = False
) -> str:
    """Integrated live attribution from the shared funded execution path."""
    try:
        rows = _query(
            db_path,
            "SELECT strategy_instance_id, COUNT(DISTINCT trade_id) AS trades, "
            "SUM(CASE WHEN kind='opening_fee' THEN amount_sats ELSE 0 END) AS opening_fees, "
            "SUM(CASE WHEN kind='funding' THEN amount_sats ELSE 0 END) AS funding, "
            "SUM(CASE WHEN kind IN ('close_net_pl','liquidation','external_close_net_pl') "
            "THEN amount_sats ELSE 0 END) AS closed_pl, SUM(amount_sats) AS net "
            "FROM strategy_pnl_events GROUP BY strategy_instance_id "
            "ORDER BY strategy_instance_id",
        )
    except sqlite3.Error:
        return ""
    if not rows and not breakout_enabled:
        return ""
    by_strategy = {str(row["strategy_instance_id"]): row for row in rows}
    display = []
    strategy_ids = (
        ["ma_cross_primary", BREAKOUT_INSTANCE_ID]
        + [name for name in by_strategy if name not in {"ma_cross_primary", BREAKOUT_INSTANCE_ID}]
        if breakout_enabled
        else list(by_strategy)
    )
    for strategy_id in strategy_ids:
        row = by_strategy.get(strategy_id)
        display.append(
            {
                "strategy": strategy_id or "legacy_ma",
                "trades": row["trades"] if row else 0,
                "opening_fees": _format_signed_amount(
                    row["opening_fees"] if row else 0, denomination, btc_price
                ),
                "funding": _format_signed_amount(
                    row["funding"] if row else 0, denomination, btc_price
                ),
                "closed_pl": _format_signed_amount(
                    row["closed_pl"] if row else 0, denomination, btc_price
                ),
                "net": _format_signed_amount(row["net"] if row else 0, denomination, btc_price),
            }
        )
    return (
        "<section><h2>Live strategy attribution</h2>"
        "<p>Signed results recorded since the integrated executor was enabled. "
        "The strategies share wallet cash; these rows attribute trading outcomes.</p>"
        + _table(
            "Strategy P&L",
            display,
            ("strategy", "trades", "opening_fees", "funding", "closed_pl", "net"),
        )
        + "</section>"
    )


def _overview(
    db_path: Path,
    run: dict[str, object],
    denomination: str,
    pnl_window: str,
    exchange: ExchangeSnapshot | None,
) -> str:
    price, _, _ = _market_context(db_path)
    orders = _orders(db_path)
    positions = _open_positions(db_path, orders, price, exchange)
    levels = _ma_levels(db_path, _strategy_tolerance(run))
    breakout_state = _persisted_breakout_state(db_path)
    breakout = _breakout_context(breakout_state, positions)
    breakout_enabled = (
        bool(_metadata(run.get("config_json")).get("strategy_breakout_enabled"))
        or breakout_state is not None
        or bool(breakout["owned"])
    )
    pnl = _pnl_summary(db_path, positions)
    by_timeframe = {
        str(position["timeframe"]): position
        for position in positions
        if position.get("strategy") in {"legacy_ma", "ma_cross_primary"}
    }
    cooldowns = _persisted_cooldowns(db_path)
    cards = "".join(
        (
            _position_card(
                "1d", by_timeframe.get("1d"), denomination, price, levels.get("1d"), cooldowns["1d"]
            ),
            _position_card(
                "4h", by_timeframe.get("4h"), denomination, price, levels.get("4h"), cooldowns["4h"]
            ),
            _breakout_card(breakout, denomination, price) if breakout_enabled else "",
            _pnl_card(pnl, denomination, pnl_window, price),
        )
    )
    signal_rows_by_tf = {timeframe: _signals(db_path, tf=timeframe)[:5] for timeframe in TIMEFRAMES}
    paper = _historical_paper_position(breakout, price) if breakout_enabled else None
    breakout_signals = _breakout_activity_rows(db_path, breakout_state, paper)
    funding_rows = [
        dict(row)
        for row in _query(
            db_path, "SELECT ts, trade_id, fee_sats FROM funding_fees ORDER BY id DESC LIMIT 5"
        )
    ]
    owners = _trade_owners(orders)
    funding_display = [
        {
            "ts": row["ts"],
            "strategy": owners.get(str(row["trade_id"]), ("unknown", "-"))[0],
            "slot": owners.get(str(row["trade_id"]), ("unknown", "-"))[1],
            "funding": _format_signed_amount(row["fee_sats"], denomination, price, invert=True),
        }
        for row in funding_rows
    ]
    return "".join(
        (
            "<h1>Operational overview</h1><div class=cards>",
            cards,
            "</div><div class=overview-grid><div class=full-width>",
            _table(
                "Active positions",
                _position_status_rows(
                    positions, levels, denomination, price, breakout if breakout_enabled else None
                ),
                (
                    "strategy",
                    "status",
                    "slot",
                    "timeframe",
                    "side",
                    "entry_ts",
                    "contracts",
                    "leverage",
                    "entry_price",
                    "exit_trigger",
                    "margin",
                    "funding",
                    "mark_pnl",
                ),
            ),
            "</div><div class=activity-grid>",
            _table(
                "Latest MA 1d signals",
                signal_rows_by_tf["1d"],
                ("ts", "kind", "reason"),
            ),
            _table(
                "Latest MA 4h signals",
                signal_rows_by_tf["4h"],
                ("ts", "kind", "reason"),
            ),
            '<div class="breakout-activity">',
            _table(
                "Latest breakout signals",
                breakout_signals,
                (
                    "signal_ts", "action_ts", "slot", "source", "kind", "reason",
                    "signal_close", "range_boundary", "ema_distance_atr", "overlap_10",
                ),
            ),
            "</div>",
            _table("Latest funding", funding_display, ("ts", "strategy", "slot", "funding")),
            "</div></div>",
            _portfolio_panel(),
            _strategy_accounting_panel(
                db_path, denomination, price, breakout_enabled=breakout_enabled
            ),
        )
    )


def _sidebar_status(run: dict[str, object], last_bar: datetime | None) -> str:
    freshness = (datetime.now(UTC) - last_bar).total_seconds() if last_bar else float("inf")
    healthy = str(run.get("status")) == "running" and freshness <= 180
    status = "LIVE · receiving bars" if healthy else "STALE OR STOPPED"
    return (
        f'<span class="status-dot{" healthy" if healthy else " stale"}"></span>'
        f"<span>{status}</span>"
    )


def _topbar(
    db_path: Path, run: dict[str, object], denomination: str, exchange: ExchangeSnapshot | None
) -> str:
    price, changes, _ = _market_context(db_path)
    snapshots = _query(
        db_path,
        "SELECT ts, balance_sats, equity_sats, margin_used_sats FROM account_snapshots ORDER BY id DESC LIMIT 1",
    )
    snapshot = dict(snapshots[0]) if snapshots else {}
    positions = _open_positions(db_path, _orders(db_path), price, exchange)
    local_unrealized = sum(
        int(position["estimated_unrealized_sats"])
        for position in positions
        if isinstance(position.get("estimated_unrealized_sats"), int)
    )
    balance = int(snapshot.get("balance_sats") or 0)
    local_margin = int(snapshot.get("margin_used_sats") or 0)
    total = exchange.total_sats if exchange else balance + local_margin + local_unrealized
    available = exchange.available_sats if exchange else balance
    running_pl = exchange.running_pl_sats if exchange else local_unrealized
    change_html = (
        "".join(
            f"<span><b>{row['period']}</b> {_signed_percent_html(str(row['change']).rstrip('%'))}</span>"
            if row["change"] != "-"
            else f"<span><b>{row['period']}</b> -</span>"
            for row in changes
        )
        or "<span>Awaiting enough history for price changes.</span>"
    )
    return (
        '<div class="topbar-market"><span>BTC/USD</span><div class="market-main"><strong data-live-price>'
        f"{f'${price:,.2f}' if price else 'Awaiting price'}</strong>"
        f"<div class=market-changes>{change_html}</div></div></div>"
        '<div class="topbar-metric"><div class="equity-main"><span>Total equity</span><strong>'
        f"{_amount_html(total, denomination, price)}</strong></div>"
        '<div class="equity-main"><span>Mark P&amp;L</span><strong>'
        f"{_signed_amount_html(running_pl, denomination, price)}</strong></div>"
        f"<small>available {_amount_html(available, denomination, price)}</small></div>"
    )


def _active_config(run: dict[str, object]) -> str:
    config = _metadata(run.get("config_json"))
    strategy = _strategy_params(run)
    if not config:
        return "<section><h2>Active run configuration</h2><p class=muted>Configuration unavailable.</p></section>"

    def value(key: str, *, inactive_when: bool = False) -> object:
        raw = config.get(key)
        if inactive_when:
            return "inactive"
        if isinstance(raw, dict):
            return ", ".join(f"{name}: {weight}" for name, weight in raw.items())
        return raw if raw is not None else "unlimited"

    def group(title: str, rows: list[tuple[str, object]]) -> str:
        items = "".join(
            f"<dt>{html.escape(label)}</dt><dd>{html.escape(str(setting))}</dd>"
            for label, setting in rows
        )
        return (
            f"<article class=config-group><h2>{html.escape(title)}</h2><dl>{items}</dl></article>"
        )

    def strategy_value(key: str, timeframe: str, *, percent: bool = False) -> object:
        raw = strategy.get(key)
        value_for_tf = raw.get(timeframe) if isinstance(raw, dict) else raw
        if value_for_tf is None:
            return "-"
        if percent:
            try:
                return f"{float(value_for_tf):.2%}"
            except (TypeError, ValueError):
                return value_for_tf
        return value_for_tf

    fixed = str(config.get("sizing_mode")) == "fixed_notional"
    sizing_rows = [("Mode", value("sizing_mode")), ("Leverage", value("sizing_leverage"))]
    if fixed:
        sizing_rows.append(("Fixed notional / TF", value("sizing_fixed_notional_usd")))
    else:
        sizing_rows.extend(
            (
                ("Margin fraction", value("sizing_total_margin_fraction")),
                ("Timeframe weights", value("sizing_timeframe_weights")),
                ("Equity haircut", value("sizing_equity_haircut")),
            )
        )
    chop_enabled = bool(config.get("strategy_4h_chop_reduce_enabled"))
    return (
        "<section><h2>Active run configuration</h2><div class=config-grid>"
        + group("Sizing", sizing_rows)
        + group(
            "4h CHOP overlay",
            [
                ("Enabled", "yes" if chop_enabled else "no"),
                ("Lookback", value("strategy_chop_lookback")),
                ("High threshold", value("strategy_chop_high_threshold")),
                ("High-CHOP entry size", value("strategy_chop_high_size_multiplier")),
            ],
        )
        + group(
            "Strategy rules",
            [
                ("Tolerance", strategy_value("tolerance_pct", "1d", percent=True)),
                (
                    "1d winner cooldown",
                    f"{strategy_value('cooldown_threshold_pct', '1d', percent=True)} · {strategy_value('cooldown_signal_count', '1d')} signals",
                ),
                (
                    "4h winner cooldown",
                    f"{strategy_value('cooldown_threshold_pct', '4h', percent=True)} · {strategy_value('cooldown_signal_count', '4h')} signals",
                ),
                (
                    "1d loss cooldown",
                    f"{strategy_value('loss_cooldown_threshold_pct', '1d', percent=True)} · {strategy_value('loss_cooldown_signal_count', '1d')} signals",
                ),
                (
                    "4h loss cooldown",
                    f"{strategy_value('loss_cooldown_threshold_pct', '4h', percent=True)} · {strategy_value('loss_cooldown_signal_count', '4h')} signals",
                ),
            ],
        )
        + group(
            "Hard risk limits",
            [
                ("Position notional", value("risk_max_position_usd")),
                ("Leverage", value("risk_max_leverage")),
                ("Daily loss entry brake", value("risk_max_daily_loss_usd")),
                ("Orders / minute", value("risk_max_orders_per_minute")),
                ("Aggregate notional", value("risk_max_total_notional_usd")),
                ("Aggregate margin", value("risk_max_total_margin_usd")),
            ],
        )
        + "</div></section>"
    )


def _strategy_explainer(run: dict[str, object]) -> str:
    """Describe all active strategy rules without exposing implementation jargon."""
    all_strategies = _metadata(run.get("strategy_params_json"))
    strategy = _strategy_params(run)
    config = _metadata(run.get("config_json"))
    try:
        tolerance = float(strategy.get("tolerance_pct", 0.005))
    except (TypeError, ValueError):
        tolerance = 0.005
    mode = str(strategy.get("cooldown_mode", "verdict_transition"))
    if mode == "verdict_transition":
        consumption = "every later verdict change, including a move to or from Flat"
    elif mode == "directional_transition":
        consumption = "every later change into an Up or Down verdict"
    else:
        consumption = "every later change that would otherwise open or flip a position"

    def threshold(key: str, timeframe: str) -> str:
        values = strategy.get(key, {})
        raw = values.get(timeframe) if isinstance(values, dict) else values
        try:
            return f"{float(raw):.0%}"
        except (TypeError, ValueError):
            return "the configured threshold"

    def count(key: str, timeframe: str) -> str:
        values = strategy.get(key, {})
        raw = values.get(timeframe) if isinstance(values, dict) else values
        try:
            return str(int(raw))
        except (TypeError, ValueError):
            return "the configured number of"

    chop_note = (
        " When 4h CHOP is high, new 4h entries use the configured reduced size; exits and cool-offs are unchanged."
        if config.get("strategy_4h_chop_reduce_enabled")
        else ""
    )
    breakout = all_strategies.get("btc_close_range_v1")
    breakout_note = ""
    if isinstance(breakout, dict) and isinstance(breakout.get("params"), dict):
        params = breakout["params"]
        breakout_note = (
            "<p><b>Close-range breakout:</b> completed LN Markets daily candles define "
            "a prior-20-close breakout. Parents require the frozen EMA/ATR and candle-overlap "
            "structure filter; qualifying same-side add-ons use K0-K3 and the 15% parent-entry "
            "distance cap. Campaign exits are the original range close, day-85 peak recovery, "
            "day-120 cap, or isolated liquidation. The configured unit is "
            f"${float(params.get('unit_notional_usd', 0)):,.0f} at "
            f"{float(params.get('leverage', 0)):g}x.</p>"
        )
    return (
        "<section class=strategy-explainer><h2>How the active strategies behave</h2>"
        "<p><b>MA strategy:</b> the 1d and 4h timeframes operate independently, "
        "each with at most one isolated position. "
        f"After a completed candle, the bot is bullish only when the close is more than {tolerance:.2%} above both the 20-period SMA and 21-period EMA; "
        "it is bearish only when it is more than that tolerance below both. Otherwise its verdict is Flat.</p>"
        "<p>A change to bullish opens or holds a long; a change to bearish opens or holds a short. "
        "If the verdict reverses an existing position, the bot closes it and normally flips on the same completed candle.</p>"
        "<p><b>Cool-off:</b> closing a trade starts a winner cool-off after a gain of at least "
        f"{threshold('cooldown_threshold_pct', '1d')} (1d) or {threshold('cooldown_threshold_pct', '4h')} (4h), "
        "or a loss cool-off after a loss of at least "
        f"{threshold('loss_cooldown_threshold_pct', '1d')} (1d) or {threshold('loss_cooldown_threshold_pct', '4h')} (4h). "
        "The closing/triggering verdict does <b>not</b> spend a slot. It then suppresses "
        f"{count('cooldown_signal_count', '1d')} winner or {count('loss_cooldown_signal_count', '1d')} loss changes on 1d, and "
        f"{count('cooldown_signal_count', '4h')} winner or {count('loss_cooldown_signal_count', '4h')} loss changes on 4h. "
        f"A slot is spent by {consumption}. An exposure-reducing close is still allowed during cool-off; only its replacement entry is suppressed.{chop_note}</p>"
        f"{breakout_note}"
        "</section>"
    )


def _presentation_style() -> str:
    return """<style>
.page-header{display:none}:root{--sidebar-width:176px}html,body{font-size:12px}.brand{font-size:1.15rem}.brand-sub,.nav-label{font-size:.72rem}.nav-link{font-size:.9rem}.content{padding-top:1.7rem}h1{font-size:1.7rem}.cards{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}.card strong{font-size:1.45rem}.card p,.card small{font-size:.8rem}table{font-size:.9rem}th{font-size:.72rem}.sat-symbol{font-style:normal;margin-left:.08em}.market-changes{display:flex;gap:.35rem;flex-wrap:wrap;color:var(--muted);font-size:.7rem;margin:.4rem 0}.market-changes b{color:var(--text);margin-right:.1rem}.sidebar-controls{display:flex;gap:.75rem;align-items:end;flex-wrap:wrap;padding:0 .5rem}.sidebar-control-group{display:flex;flex-direction:column;gap:.35rem}.denom-controls{display:flex;gap:.35rem}.denom-toggle,.pnl-toggle,.period-toggle{border:1px solid var(--border-hover);border-radius:4px;color:var(--muted);padding:.18rem .42rem;text-decoration:none;font-size:.74rem}.denom-toggle:hover,.denom-toggle.active,.pnl-toggle:hover,.pnl-toggle.active,.period-toggle:hover,.period-toggle.active{border-color:var(--accent);background:var(--accent-dim);color:var(--accent)}.position-card.long{border-color:var(--accent)}.position-card.short{border-color:#f87171}.position-card.short strong{color:#f87171}.position-card.flat{opacity:.62}.pnl-card small{display:flex;gap:.3rem;align-items:center;flex-wrap:wrap}.overview-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 1.2rem}.overview-grid .full-width{grid-column:1/-1}.overview-grid .full-width .table-wrap{width:100%}.activity-grid{grid-column:1/-1;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0 1.2rem}.activity-grid .table-wrap{width:100%}.compact-table .table-wrap{width:max-content;max-width:100%}.compact-table table{width:auto}.copy-id{border:1px solid var(--border-hover);border-radius:3px;background:var(--surface-2);color:var(--muted);font:inherit;font-size:.72rem;padding:.08rem .3rem;cursor:pointer}.copy-id:hover{border-color:var(--accent);color:var(--accent)}.period-controls{display:flex;gap:.35rem;flex-wrap:wrap;margin:-.15rem 0 1rem}.pagination{display:flex;align-items:center;gap:.65rem;margin-top:.65rem;color:var(--muted)}.pagination a{color:var(--accent);text-decoration:none}.topbar{display:flex;align-items:center;gap:1.4rem;flex-wrap:wrap;border-bottom:1px solid var(--border);padding:0 0 1rem;margin-bottom:1.7rem}.topbar-status,.topbar-metric,.topbar-market{display:flex;flex-direction:column;gap:.1rem}.topbar-status{flex-direction:row;align-items:center;gap:.55rem;margin-right:auto}.topbar span{color:var(--muted);font-size:.68rem;letter-spacing:.06em;text-transform:uppercase}.topbar b,.topbar strong{font-size:.92rem}.topbar small{color:var(--muted);font-size:.7rem}.status-dot{width:.58rem;height:.58rem;border-radius:99px;background:#f87171;box-shadow:0 0 0 3px rgba(248,113,113,.12)}.status-dot.healthy{background:var(--accent);box-shadow:0 0 0 3px var(--accent-dim)}.config-grid{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:.8rem}.config-group{background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:.9rem}.config-group h2{margin:0 0 .6rem;font-size:.72rem}.config-group dl{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.38rem .7rem;font-size:.8rem}.config-group dt{color:var(--muted)}.config-group dd{text-align:right}.strategy-explainer{max-width:76rem}.strategy-explainer p{color:var(--muted);margin:.65rem 0;line-height:1.7}.strategy-explainer b{color:var(--text)}@media(max-width:1100px){.activity-grid{grid-template-columns:1fr}.overview-grid,.config-grid{grid-template-columns:1fr}.topbar-status{margin-right:0;width:100%}}@media(max-width:700px){html,body{font-size:12px}.content{padding:1.25rem}.topbar{gap:.9rem}.topbar-status{width:100%}}
.pnl-page-heading{display:flex;align-items:center;justify-content:space-between;gap:1rem;margin-bottom:1.25rem}.pnl-page-heading h1{margin:0}.pnl-basis-actions{display:flex;align-items:center;justify-content:flex-end;gap:.4rem;flex-wrap:wrap}@media(max-width:700px){.pnl-page-heading{align-items:flex-start;flex-direction:column}.pnl-basis-actions{justify-content:flex-start}}
.config-grid{grid-template-columns:repeat(4,minmax(0,1fr))}
</style>"""


def _detail_page(
    db_path: Path,
    run: dict[str, object],
    page: str,
    tf: str | None,
    denomination: str,
    exchange: ExchangeSnapshot | None,
    pnl_granularity: str = "daily",
    pnl_page: int = 1,
    pnl_basis: str = "actual",
) -> str:
    run_id = int(run["id"])
    suffix = f" · {tf}" if tf else ""
    if page == "signals":
        return _table(
            "Signals" + suffix,
            _signals(db_path, tf=tf),
            (
                "id",
                "ts",
                "strategy",
                "slot",
                "timeframe",
                "kind",
                "side",
                "target_size_usd",
                "target_leverage",
                "chop_regime",
                "chop_value",
                "entry_size_multiplier",
                "reason",
            ),
        )
    if page == "trades":
        price, _, _ = _market_context(db_path)
        return _table(
            "Isolated trade ledger" + suffix,
            _trade_history_rows(db_path, tf=tf, denomination=denomination, btc_price=price),
            (
                "trade_id",
                "strategy",
                "slot",
                "timeframe",
                "position",
                "opened_ts",
                "closed_ts",
                "entry_price",
                "exit_price",
                "gross_pl",
                "trading_fees",
                "funding",
                "net_pl",
                "net_return",
            ),
        )
    if page == "funding":
        price, _, _ = _market_context(db_path)
        owners = _trade_owners(_orders(db_path))
        rows = [
            {
                **dict(row),
                "strategy": owners.get(str(row["trade_id"]), ("unknown", "-"))[0],
                "slot": owners.get(str(row["trade_id"]), ("unknown", "-"))[1],
                "funding_pnl": _format_signed_amount(
                    row["fee_sats"], denomination, price, invert=True
                ),
            }
            for row in _query(
                db_path,
                "SELECT ts, trade_id, settlement_id, fee_sats FROM funding_fees ORDER BY id DESC LIMIT 500",
            )
        ]
        rate_detail = "Latest settled funding rate unavailable."
        if exchange and exchange.funding_rate is not None:
            rate_detail = f"Latest settled funding rate: {exchange.funding_rate:+.4%}" + (
                f" at {_format_timestamp(exchange.funding_rate_ts)}"
                if exchange.funding_rate_ts
                else ""
            )
        return f'<p class="muted funding-rate">{html.escape(rate_detail)}</p>' + _table(
            "Funding settlements",
            rows,
            ("ts", "strategy", "slot", "trade_id", "settlement_id", "funding_pnl"),
        )
    if page == "pnl":
        price, _, _ = _market_context(db_path)
        positions = _open_positions(db_path, _orders(db_path), price, exchange)
        constant = pnl_basis == "constant"
        nominal_usd = CONSTANT_NOTIONAL_USD
        raw_summary = (
            _constant_notional_pnl_summary(db_path, positions, nominal_usd, price)
            if constant
            else _pnl_summary(db_path, positions)
        )
        summary = [
            {
                "period": row["period"],
                "gross_pnl": (
                    _signed_usd_html(row["gross"])
                    if constant
                    else _format_signed_amount(row["gross"], denomination, price)
                ),
                "trading_fees": (
                    _signed_usd_html(row["trading_fees"])
                    if constant
                    else _format_signed_amount(row["trading_fees"], denomination, price)
                ),
                "funding_pnl": (
                    _signed_usd_html(row["funding"])
                    if constant
                    else _format_signed_amount(row["funding"], denomination, price)
                ),
                "net": (
                    _signed_usd_html(row["net"])
                    if constant
                    else _format_signed_amount(row["net"], denomination, price)
                ),
            }
            for row in raw_summary
        ]
        labels = {"daily": "Daily", "weekly": "Weekly", "monthly": "Monthly"}
        controls = "".join(
            f'<a class="period-toggle{" active" if key == pnl_granularity else ""}" '
            f'href="/pnl?{urlencode({"denom": denomination, "pnl_granularity": key, **({"pnl_basis": "constant"} if constant else {})})}">{label}</a>'
            for key, label in labels.items()
        )
        calendar_rows = (
            _constant_notional_calendar_pnl_rows(db_path, pnl_granularity, nominal_usd)
            if constant
            else _calendar_pnl_rows(db_path, pnl_granularity)
        )
        calendar_rows, current_page, total_pages = _paginate(calendar_rows, pnl_page, 12)
        display_rows = [
            {
                "period": row["period"],
                "net": (
                    _signed_usd_html(row["net"])
                    if constant
                    else _format_signed_amount(row["net"], denomination, price)
                ),
            }
            for row in calendar_rows
        ]
        strategy_quality, strategy_risk = _strategy_performance_rows(
            db_path, denomination, price, nominal_usd if constant else None
        )
        summary_columns = ("period", "gross_pnl", "trading_fees", "funding_pnl", "net")
        quality_columns = (
            "timeframe",
            "closed_trades",
            "win_rate",
            "avg_winner",
            "avg_loser",
            "payoff_ratio",
            "profit_factor",
        )
        replay_label = f" · ${nominal_usd:g} per trade" if constant else ""
        return (
            (
                "<p class=muted>Constant notional scales each isolated trade to $100. "
                "It is a trade comparison, not a return on shared wallet capital.</p>"
                if constant
                else ""
            )
            + '<div class="pnl-grid">'
            + _table(
                "Rolling P&L" + replay_label,
                summary,
                summary_columns,
                compact=True,
            )
            + '<div class="calendar-pnl">'
            + _periodic_pnl_table(
                f"{labels[pnl_granularity]} P&L{replay_label}", display_rows, controls
            )
            + _pagination(
                current_page,
                total_pages,
                denomination,
                pnl_granularity,
                pnl_basis,
            )
            + "</div></div>"
            + _table(
                "Strategy · trade quality" + replay_label,
                strategy_quality,
                quality_columns,
                compact=True,
            )
            + _table(
                "Strategy · risk & holding" + replay_label,
                strategy_risk,
                (
                    "timeframe",
                    "avg_trade",
                    "best_trade",
                    "worst_trade",
                    "max_closed_drawdown",
                    "longest_streaks",
                    "avg_hold",
                    "time_in_market",
                ),
                compact=True,
            )
            + _table(
                "Account profitability · actual",
                _account_profitability_rows(exchange, denomination, price),
                (
                    "equity",
                    "deposits",
                    "withdrawals",
                    "net_deposits",
                    "cashflow_adjusted_pnl",
                    "return_on_net_deposits",
                ),
                compact=True,
            )
        )
    if page == "runs":
        active_details = [
            {
                "run": run_id,
                "mode": run.get("mode", "-"),
                "status": run.get("status", "-"),
                "started_at": run.get("started_at", "-"),
                "strategy": str(run.get("strategy_name", "-")).rsplit(".", maxsplit=1)[-1],
            }
        ]
        return (
            _table(
                "Active run",
                active_details,
                ("run", "mode", "status", "started_at", "strategy"),
                compact=True,
            )
            + _active_config(run)
            + _strategy_explainer(run)
        )
    if page == "health":
        price, _, last_bar = _market_context(db_path)
        risk_events = [
            dict(row)
            for row in _query(
                db_path,
                "SELECT ts, kind, detail_json FROM risk_events WHERE run_id = ? ORDER BY id DESC LIMIT 50",
                (run_id,),
            )
        ]
        health = [
            {
                "run": run_id,
                "status": run["status"],
                "last_1m_bar": last_bar.isoformat() if last_bar else "-",
                "btc_usd": price or "-",
            }
        ]
        return _table("Run health", health, ("run", "status", "last_1m_bar", "btc_usd")) + _table(
            "Risk events", risk_events, ("ts", "kind", "detail_json")
        )
    return "<h1>Not found</h1>"


def _render(
    db_path: Path,
    page: str,
    tf: str | None,
    denomination: str = "sats",
    pnl_window: str = "7days",
    pnl_granularity: str = "daily",
    pnl_page: int = 1,
    pnl_basis: str = "actual",
) -> str:
    run: dict[str, object] | None = None
    exchange: ExchangeSnapshot | None = None
    try:
        run = _active_run(db_path)
        if run is None:
            content = (
                _presentation_style()
                + "<h1>LN Markets Bot</h1><p>No runs have been recorded yet.</p>"
            )
        elif page == "overview":
            exchange = _EXCHANGE_CACHE.get()
            content = _presentation_style() + _overview(
                db_path, run, denomination, pnl_window, exchange
            )
        else:
            exchange = _EXCHANGE_CACHE.get()
            title = {
                "signals": "Signals",
                "trades": "Trades",
                "funding": "Funding",
                "pnl": "P&L",
                "runs": "Runs",
                "health": "Health",
            }[page]
            suffix = f" · {tf}" if tf else ""
            heading = (
                '<div class="pnl-page-heading"><h1>P&amp;L</h1>'
                + _pnl_basis_controls(denomination, pnl_granularity, pnl_basis)
                + "</div>"
                if page == "pnl"
                else f"<h1>{title}{suffix}</h1>"
            )
            content = (
                _presentation_style()
                + heading
                + _detail_page(
                    db_path,
                    run,
                    page,
                    tf,
                    denomination,
                    exchange,
                    pnl_granularity,
                    pnl_page,
                    pnl_basis,
                )
            )
    except sqlite3.Error as exc:
        content = f"<h1>LN Markets Bot</h1><p>Database unavailable: {html.escape(str(exc))}</p>"
    filterable_pages = {"signals", "trades"}

    def href(target: str, *, target_tf: str | None = tf) -> str:
        path = "/" if target == "overview" else f"/{target}"
        query: dict[str, str] = {}
        if denomination != "sats":
            query["denom"] = denomination
        if target == "overview" and pnl_window != "7days":
            query["pnl_window"] = pnl_window
        if target == "pnl":
            if pnl_granularity != "daily":
                query["pnl_granularity"] = pnl_granularity
            if pnl_page != 1:
                query["pnl_page"] = str(pnl_page)
            if pnl_basis == "constant":
                query["pnl_basis"] = "constant"
        if target in filterable_pages and target_tf:
            query["tf"] = target_tf
        return f"{path}?{urlencode(query)}" if query else path

    def nav_link(target: str, label: str, icon: str) -> str:
        active = " active" if page == target else ""
        return (
            f'<a class="nav-link{active}" href="{href(target)}">'
            f"<span class=nav-icon>{icon}</span>{html.escape(label)}</a>"
        )

    scope_links = [
        f'<a class="scope-link{" active" if tf is None else ""}" '
        f'href="{href(page, target_tf=None)}">All</a>'
    ]
    for timeframe, label in (("1d", "MA 1d"), ("4h", "MA 4h"), ("breakout", "Breakout")):
        target = page if page in filterable_pages else "signals"
        active = " active" if tf == timeframe else ""
        scope_links.append(
            f'<a class="scope-link{active}" href="{href(target, target_tf=timeframe)}">{label}</a>'
        )
    scope_links_html = "".join(scope_links)
    template = """<!doctype html>
<html><head><meta charset="utf-8"><title>LN Markets Bot</title><script src="https://kit.fontawesome.com/090ca49637.js" crossorigin="anonymous"></script><style>
:root{{--bg:#0d1117;--surface:#161b22;--surface-2:#1c2128;--surface-3:#21262d;--border:#21262d;--border-hover:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#34d399;--accent-dim:rgba(52,211,153,.08);--sidebar-width:220px}}*{{box-sizing:border-box;margin:0;padding:0}}html,body{{min-height:100%;background:var(--bg);color:var(--text);font-family:ui-monospace,'Cascadia Code','JetBrains Mono','Fira Code',monospace;font-size:13px;line-height:1.5;color-scheme:dark}}.layout{{display:flex;min-height:100vh}}.sidebar{{width:var(--sidebar-width);flex-shrink:0;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;inset:0 auto 0 0;padding:1.5rem 0}}.sidebar-top{{padding:0 1.25rem 1.5rem;border-bottom:1px solid var(--border);margin-bottom:1.25rem}}.brand{{color:var(--accent);font-size:1.05rem;font-weight:700;letter-spacing:.02em}}.brand-sub,.nav-label,.muted,.scope-note{{color:var(--muted)}}.brand-sub,.nav-label{{font-size:.65rem;letter-spacing:.1em;text-transform:uppercase}}.nav-section{{padding:0 .75rem;display:flex;flex-direction:column;gap:2px}}.nav-label{{font-weight:600;padding:0 .5rem;margin:.6rem 0 .35rem}}.nav-link{{display:flex;align-items:center;gap:.55rem;padding:.45rem .5rem;border-radius:5px;color:var(--text);text-decoration:none;font-size:.82rem}}.nav-link:hover,.nav-link.active{{background:var(--surface-2);color:var(--accent)}}.nav-link.active{{box-shadow:inset 2px 0 var(--accent)}}.nav-icon{{color:var(--muted);width:1rem;text-align:center}}.sidebar-bottom{{padding:1rem .75rem 0;border-top:1px solid var(--border);margin-top:auto}}.scope-links{{display:flex;gap:.35rem;padding:0 .5rem;flex-wrap:wrap}}.scope-link{{border:1px solid var(--border-hover);border-radius:4px;color:var(--muted);padding:.2rem .42rem;text-decoration:none;font-size:.72rem}}.scope-link:hover,.scope-link.active{{border-color:var(--accent);background:var(--accent-dim);color:var(--accent)}}.scope-note{{display:block;font-size:.68rem;padding:.65rem .5rem 0}}.content{{margin-left:var(--sidebar-width);flex:1;padding:2rem 2.5rem;max-width:1700px}}.page-header{{display:flex;justify-content:space-between;gap:1rem;align-items:end;border-bottom:1px solid var(--border);padding-bottom:1rem;margin-bottom:1.6rem}}.eyebrow{{color:var(--accent);font-size:.68rem;font-weight:600;letter-spacing:.1em;text-transform:uppercase}}.page-header p{{color:var(--muted);font-size:.78rem;max-width:38rem;text-align:right}}h1{{font-size:1.45rem;line-height:1.2;margin-bottom:1.25rem}}h2{{font-size:.85rem;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);margin:2rem 0 .65rem}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:.8rem}}.card{{background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:1rem}}.card:hover{{border-color:var(--border-hover)}}.card p,.card small{{display:block;color:var(--muted)}}.card p{{font-size:.72rem;text-transform:uppercase;letter-spacing:.06em}}.card strong{{display:block;font-size:1.3rem;margin:.4rem 0;font-weight:600}}.card small{{font-size:.72rem;min-height:1.1em}}.table-wrap{{overflow-x:auto;border:1px solid var(--border);border-radius:7px;background:var(--surface)}}table{{border-collapse:collapse;width:100%;font-size:.8rem}}th,td{{padding:.6rem .7rem;border-bottom:1px solid var(--border);vertical-align:top;text-align:left;white-space:nowrap}}td:last-child{{white-space:normal}}th{{color:var(--muted);font-size:.67rem;text-transform:uppercase;letter-spacing:.06em;background:var(--surface-2)}}tbody tr:last-child td{{border-bottom:0}}tbody tr:hover{{background:var(--surface-2)}}::-webkit-scrollbar{{width:5px}}::-webkit-scrollbar-track{{background:transparent}}::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}@media(max-width:700px){{.sidebar{{position:static;width:100%;height:auto;padding:1rem;flex-direction:row;flex-wrap:wrap;gap:.5rem;border-right:0;border-bottom:1px solid var(--border)}}.sidebar-top{{padding:0;border:0;margin:0}.nav-section{{flex-direction:row;flex-wrap:wrap;padding:0}.nav-label{{display:none}.sidebar-bottom{{border:0;padding:0;margin:0}.scope-note{{display:none}.content{{margin-left:0;padding:1.25rem}}.page-header{{display:block}}.page-header p{{text-align:left;margin-top:.4rem}}}}
</style></head><body><div class=layout><aside class=sidebar><div class=sidebar-top><div class=brand>LN Markets Bot</div><div class=brand-sub>read-only operations</div></div><div class=sidebar-status data-refresh-region=status>{sidebar_status}</div><nav class=nav-section><span class=nav-label>Monitor</span>{nav_link('overview', 'Overview', '◉')}<span class=nav-label>Activity</span>{nav_link('trades', 'Trades', '⇄')}{nav_link('signals', 'Signals', '↯')}{nav_link('pnl', 'P&L', '±')}{nav_link('funding', 'Funding', '₿')}<span class=nav-label>System</span>{nav_link('runs', 'Runs', '◌')}{nav_link('health', 'Health', '✓')}</nav><div class=sidebar-bottom><div class=sidebar-controls><div class=sidebar-control-group><span class=nav-label>Display</span><div class=denom-controls>{denomination_links}</div></div><div class=sidebar-control-group><span class=nav-label>Strategy</span><div class=scope-links>{scope_links_html}</div></div></div></div></aside><main class=content data-refresh-region=content><header class=topbar>{topbar}</header>{content}</main></div>{refresh_script}</body></html>"""
    # The stylesheet originated in an f-string, where CSS braces were doubled.
    # It is now a plain template so the browser needs ordinary CSS braces.
    template = template.replace("{{", "{").replace("}}", "}")
    template = template.replace(
        "</style>",
        """.positive,.topbar .positive{color:var(--accent)}.negative,.topbar .negative{color:#f87171}.position-card-body{display:flex;justify-content:space-between;gap:1rem;align-items:end}.position-static,.position-dynamic{display:flex;flex-direction:column}.position-dynamic{text-align:right}.position-card .position-static strong,.position-card .position-dynamic strong{margin:.4rem 0 .15rem}.position-card .position-dynamic strong{font-size:1.1rem}.topbar{justify-content:space-between}.topbar-market{align-items:flex-start}.market-main{display:flex;align-items:baseline;gap:.7rem}.market-changes{margin:0}.topbar-metric{flex-direction:row;align-items:baseline;gap:1rem;text-align:right}.equity-main{display:flex;align-items:baseline;gap:.35rem}.topbar-metric small{margin-left:.15rem}.sidebar-controls .nav-label,.sidebar-controls .scope-links{padding-left:0;padding-right:0}.config-grid{grid-template-columns:repeat(4,minmax(0,1fr))}.pnl-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 1.2rem;align-items:start}.pnl-grid .compact-table .table-wrap,.periodic-pnl .table-wrap{width:100%}.pnl-grid .compact-table table,.periodic-pnl table{width:100%;table-layout:fixed}.pnl-grid .table-section h2,.periodic-pnl h2{margin-top:2rem}.table-heading{display:flex;align-items:baseline;justify-content:space-between;gap:.7rem}.table-heading .period-controls{margin:0}.account-note{margin-top:.65rem}.sidebar-status{display:flex;align-items:center;gap:.55rem;color:var(--muted);font-size:.72rem;letter-spacing:.06em;text-transform:uppercase;padding:0 1.25rem 1rem;margin-bottom:.8rem;border-bottom:1px solid var(--border)}@media(max-width:1100px){.config-grid{grid-template-columns:repeat(4,minmax(0,1fr))}.pnl-grid{grid-template-columns:1fr}}@media(max-width:700px){.sidebar-status{border:0;padding:0;margin:0}.topbar-metric{align-items:flex-start;text-align:left;flex-wrap:wrap}.market-main{flex-wrap:wrap;gap:.3rem}.config-grid{grid-template-columns:repeat(4,minmax(0,1fr))}}\n</style>""",
    )
    template = template.replace(
        "</style>",
        ".stack-detail-row>td{padding:0;white-space:normal;background:var(--surface)}"
        ".stack-drilldown{padding:.55rem .75rem}"
        ".stack-drilldown summary{cursor:pointer;color:var(--accent);font-weight:600}"
        ".stack-drilldown p{margin:.8rem 0;color:var(--muted);white-space:normal}"
        ".stack-drilldown .table-section h2{margin:1rem 0 .4rem}"
        ".stack-drilldown .table-wrap{max-width:100%}</style>",
    )
    template = template.replace(
        "</style>",
        ".activity-grid .breakout-activity{grid-column:1/-1;min-width:0}"
        ".activity-grid .breakout-activity .table-wrap{max-width:100%}</style>",
    )
    refresh_script = """<script>
(()=>{
  const sync=(current,next)=>{
    if(current.nodeType!==next.nodeType||current.nodeName!==next.nodeName){current.replaceWith(next.cloneNode(true));return}
    if(current.nodeType===Node.TEXT_NODE){if(current.nodeValue!==next.nodeValue)current.nodeValue=next.nodeValue;return}
    const wasOpen=current.nodeName==='DETAILS'&&current.hasAttribute('data-preserve-open')?current.open:null
    for(const attribute of [...current.attributes])if(!next.hasAttribute(attribute.name))current.removeAttribute(attribute.name)
    for(const attribute of [...next.attributes])if(current.getAttribute(attribute.name)!==attribute.value)current.setAttribute(attribute.name,attribute.value)
    if(wasOpen!==null)current.open=wasOpen
    const oldChildren=[...current.childNodes],newChildren=[...next.childNodes]
    for(let index=0;index<Math.max(oldChildren.length,newChildren.length);index+=1){
      if(!oldChildren[index])current.appendChild(newChildren[index].cloneNode(true))
      else if(!newChildren[index])oldChildren[index].remove()
      else sync(oldChildren[index],newChildren[index])
    }
  }
  const refresh=async()=>{
    if(document.hidden)return
    try{
      const response=await fetch(window.location.href,{cache:'no-store'})
      if(!response.ok)return
      const fresh=new DOMParser().parseFromString(await response.text(),'text/html')
      for(const region of document.querySelectorAll('[data-refresh-region]')){
        const updated=fresh.querySelector(`[data-refresh-region="${region.dataset.refreshRegion}"]`)
        if(updated)sync(region,updated)
      }
    }catch(_error){}
  }
  const refreshPrice=async()=>{
    try{
      const response=await fetch('/api/live-price',{cache:'no-store'})
      const tick=await response.json()
      if(typeof tick.price!=='number')return
      for(const element of document.querySelectorAll('[data-live-price]')){
        element.textContent=new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2}).format(tick.price)
      }
    }catch(_error){}
  }
  window.setInterval(refresh,10000)
  window.setInterval(refreshPrice,1000)
  refreshPrice()
})()
</script>"""
    topbar = _topbar(db_path, run, denomination, exchange) if run is not None else ""
    last_bar = _market_context(db_path)[2] if run is not None else None
    sidebar_status = _sidebar_status(run, last_bar) if run is not None else ""
    replacements = {
        "{nav_link('overview', 'Overview', '◉')}": nav_link("overview", "Overview", "◉"),
        "{nav_link('signals', 'Signals', '↯')}": nav_link("signals", "Signals", "↯"),
        "{nav_link('trades', 'Trades', '⇄')}": nav_link("trades", "Trades", "⇄"),
        "{nav_link('funding', 'Funding', '₿')}": nav_link("funding", "Funding", "₿"),
        "{nav_link('pnl', 'P&L', '±')}": nav_link("pnl", "P&L", "±"),
        "{nav_link('runs', 'Runs', '◌')}": nav_link("runs", "Runs", "◌"),
        "{nav_link('health', 'Health', '✓')}": nav_link("health", "Health", "✓"),
        "{scope_links_html}": scope_links_html,
        "{topbar}": topbar,
        "{sidebar_status}": sidebar_status,
        "{content}": content,
        "{refresh_script}": refresh_script,
    }
    for source, replacement in replacements.items():
        template = template.replace(source, replacement)
    denomination_links = []
    for value, label in (("sats", SAT_ICON), ("usd", "USD")):
        query: dict[str, str] = {"denom": value}
        if page == "overview" and pnl_window != "7days":
            query["pnl_window"] = pnl_window
        if page == "pnl":
            if pnl_granularity != "daily":
                query["pnl_granularity"] = pnl_granularity
            if pnl_page != 1:
                query["pnl_page"] = str(pnl_page)
            if pnl_basis == "constant":
                query["pnl_basis"] = "constant"
        if page in filterable_pages and tf:
            query["tf"] = tf
        path = "/" if page == "overview" else f"/{page}"
        denomination_links.append(
            f'<a class="denom-toggle{" active" if denomination == value else ""}" '
            f'href="{path}?{urlencode(query)}">{label}</a>'
        )
    template = template.replace("{denomination_links}", "".join(denomination_links))
    scope_note = ""
    return template.replace(
        "{overview_scope if page == 'overview' else '<span class=scope-note>Filters signals and trades.</span>'}",
        scope_note,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    _PRICE_STREAM.start(os.getenv("LNM_DASHBOARD_WS_URL", "wss://stream.lnmarkets.com/v1"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            page = parsed.path.strip("/") or "overview"
            if page == "healthz":
                payload = b"ok\n"
                content_type = "text/plain"
            elif page == "api/live-price":
                tick = _PRICE_STREAM.latest()
                payload = json.dumps(
                    {"price": tick.price, "ts": tick.ts.isoformat()} if tick else {"price": None}
                ).encode()
                content_type = "application/json"
            elif page in {
                "overview",
                "signals",
                "trades",
                "funding",
                "pnl",
                "runs",
                "health",
            }:
                requested_tf = parse_qs(parsed.query).get("tf", [None])[0]
                query = parse_qs(parsed.query)
                tf = (
                    requested_tf
                    if page in {"signals", "trades"} and requested_tf in (*TIMEFRAMES, "breakout")
                    else None
                )
                denomination = query.get("denom", ["sats"])[0]
                pnl_window = query.get("pnl_window", ["7days"])[0]
                pnl_granularity = query.get("pnl_granularity", ["daily"])[0]
                pnl_basis = query.get("pnl_basis", ["actual"])[0]
                try:
                    pnl_page = int(query.get("pnl_page", ["1"])[0])
                except ValueError:
                    pnl_page = 1
                if denomination not in {"sats", "usd"}:
                    denomination = "sats"
                if pnl_window not in {"1day", "7days", "30days", "alltime"}:
                    pnl_window = "7days"
                if pnl_granularity not in {"daily", "weekly", "monthly"}:
                    pnl_granularity = "daily"
                if pnl_basis not in {"actual", "constant"}:
                    pnl_basis = "actual"
                payload = _render(
                    args.db,
                    page,
                    tf,
                    denomination,
                    pnl_window,
                    pnl_granularity,
                    pnl_page,
                    pnl_basis,
                ).encode()
                content_type = "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"dashboard listening on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
