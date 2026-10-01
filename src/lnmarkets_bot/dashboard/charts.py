"""Bounded read-only chart data, independent of strategy execution code.

Adapters expose display geometry and provenance, never executable decisions.
Snapshots are current state; unavailable historical geometry stays absent.
"""

from __future__ import annotations

import copy
import json
import math
import sqlite3
import threading
import time
from collections import OrderedDict
from datetime import UTC, datetime
from itertools import pairwise
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

OWNERS = {
    "ma": "ma_cross_primary",
    "breakout": "btc_close_range_v1",
    "range": "btc_impulse_range_v1",
}
LEGACY_MA = "lnmarkets_bot.strategy.ma_cross.MaCross"
PERIODS = {"1m": 60, "4h": 14400, "1d": 86400}
MAX_CANDLES = 1500
MAX_MARKERS = 2000
_CACHE: OrderedDict = OrderedDict()
_LOCK = threading.Lock()


def stamp(value: object) -> int | None:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(
            (dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)).timestamp()
        )
    except (ValueError, TypeError, OverflowError):
        return None


def number(value: object) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def metadata(value: object) -> dict:
    if isinstance(value, dict):
        return value
    try:
        result = json.loads(str(value))
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def options(query: dict) -> dict:
    """Allowlist and bound all public chart parameters before touching SQLite."""
    strategy = query.get("strategy", ["ma"])[0]
    tf = query.get("tf", ["4h" if strategy == "range" else "1d"])[0]
    days = query.get("days", ["1" if tf == "1m" else "30"])[0]
    if strategy not in OWNERS or tf not in PERIODS or days not in {"1", "7", "30", "90"}:
        raise ValueError("Choose a supported strategy, timeframe and window")
    if tf == "1m" and days != "1":
        raise ValueError("Minute inspection is limited to one day")
    ma_tf = query.get("ma_tf", [tf if tf != "1m" else "4h"])[0]
    if ma_tf not in {"1d", "4h"}:
        raise ValueError("MA timeframe must be 1d or 4h")
    end_text = query.get("end", [None])[0]
    end = stamp(end_text) if end_text else None
    if end_text and end is None:
        raise ValueError("Invalid UTC end timestamp")
    return {"strategy": strategy, "tf": tf, "days": int(days), "ma_tf": ma_tf, "end": end}


def _columns(db, table):
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def _snapshot(db, owner):
    if not _columns(db, "strategy_state_snapshots"):
        return {}, None, None
    names = (owner, LEGACY_MA) if owner == OWNERS["ma"] else (owner, owner)
    row = db.execute(
        "SELECT run_id,ts,state_json FROM strategy_state_snapshots "
        "WHERE mode='live' AND strategy_name IN (?,?) "
        "ORDER BY CASE strategy_name WHEN ? THEN 0 ELSE 1 END, ts DESC LIMIT 1",
        (*names, owner),
    ).fetchone()
    return (metadata(row[2]), stamp(row[1]), row[0]) if row else ({}, None, None)


def _candles(db, start, end, tf):
    """Deduplicate restart rows, aggregate on UTC starts, and count real minutes."""
    sql_start = datetime.fromtimestamp(start, UTC).replace(tzinfo=None).isoformat(" ")
    sql_end = datetime.fromtimestamp(end, UTC).replace(tzinfo=None).isoformat(" ")
    rows = db.execute(
        "SELECT id,ts,open,high,low,close,volume FROM bars WHERE ts>=? AND ts<? ORDER BY ts,id",
        (sql_start, sql_end),
    )
    period = PERIODS[tf]
    grouped = {}

    def aggregate(t, prices, volume):
        key = t - t % period
        o, h, low, c = prices
        if key not in grouped:
            grouped[key] = {
                "time": key,
                "close_time": key + period,
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "volume": volume,
                "minutes": 1,
                "source": "Recorded LN Markets minutes",
            }
        else:
            bar = grouped[key]
            bar.update(
                high=max(bar["high"], h),
                low=min(bar["low"], low),
                close=c,
                volume=bar["volume"] + volume,
                minutes=bar["minutes"] + 1,
            )

    # Rows are ordered by timestamp/id: keep only the last valid restart row
    # for one minute, then fold it into its candle. Long windows need not retain
    # a Python object for every source minute, including concurrent requests.
    pending = None
    recorded = 0
    for row in rows:
        t = stamp(row[1])
        values = [number(x) for x in row[2:6]]
        if t is None or not all(x is not None and x > 0 for x in values):
            continue
        if pending is not None and t != pending[0]:
            aggregate(*pending)
            recorded += 1
        pending = (t, values, number(row[6]) or 0)
    if pending is not None:
        aggregate(*pending)
        recorded += 1
    for bar in grouped.values():
        bar["missing_minutes"] = period // 60 - bar["minutes"]
        bar["complete"] = bar["missing_minutes"] == 0 and bar["close_time"] <= end
    result = list(grouped.values())[-MAX_CANDLES:]
    expected = max(0, (end - start) // 60)
    return result, max(0, expected - recorded)


def _segment(
    view, label, role, start, end, value, *, origin="Derived from recorded state", eligible=True
):
    value = number(value)
    if value is None or start is None or end <= start:
        return
    view["segments"].append(
        {
            "id": f"{role}-{len(view['segments'])}",
            "label": label,
            "role": role,
            "start": start,
            "end": end,
            "value": value,
            "origin": origin,
            "eligible": bool(eligible),
        }
    )


def _marker(
    view,
    label,
    kind,
    observed,
    known,
    price=None,
    *,
    origin="Recorded model event",
    slot="",
    detail="",
):
    if known is None:
        return
    view["markers"].append(
        {
            "id": f"{kind}-{len(view['markers'])}",
            "label": label,
            "kind": kind,
            "observed_at": observed,
            "time": known,
            "known_at": known,
            "price": number(price),
            "origin": origin,
            "slot": str(slot),
            "detail": str(detail),
        }
    )


def _range_geometry(view, lo, hi, start, end, params, eligible):
    lo, hi = number(lo), number(hi)
    if lo is None or hi is None or hi <= lo:
        return
    width = hi - lo
    zone, tol = number(params.get("zone", 0.15)), number(params.get("tolerance", 0.1))
    if zone is None or tol is None:
        return
    buy, sell = lo + zone * width, hi - zone * width
    for label, role, value in [
        ("Channel low", "channel", lo),
        ("Channel high", "channel", hi),
        ("Buy ≤", "entry", buy),
        ("Sell ≥", "entry", sell),
        ("Midpoint target", "target", (lo + hi) / 2),
        ("4h close stop below", "stop", lo - tol * width),
        ("4h close stop above", "stop", hi + tol * width),
    ]:
        _segment(view, label, role, start, end, value, eligible=eligible)
    if start is not None and end > start:
        for label, role, lower, upper in [
            ("Buy zone", "entry", lo, buy),
            ("Sell zone", "entry", sell, hi),
            ("Lower stop band", "stop", lo - tol * width, lo),
            ("Upper stop band", "stop", hi, hi + tol * width),
        ]:
            view["bands"].append(
                {
                    "label": label,
                    "role": role,
                    "start": start,
                    "end": end,
                    "lower": lower,
                    "upper": upper,
                    "eligible": bool(eligible),
                    "origin": "Derived from recorded edges",
                }
            )


def _range(view, state, as_of, end):
    machine = metadata(state.get("machine"))
    params = metadata(machine.get("params"))
    channel = metadata(machine.get("channel"))
    setup = metadata(machine.get("setup"))
    view["status"].update(
        phase=str(machine.get("state", "Awaiting state")),
        mode=str(state.get("mode", "Unknown")),
        pending_exit=bool(state.get("closing")),
    )
    if channel:
        eligible = channel.get("tradeable", True) and not channel.get("expanding")
        er = number(channel.get("er_at_confirm"))
        view["status"].update(
            channel=f"{channel.get('lo')} to {channel.get('hi')}",
            confirmation_er=er,
            redraws=channel.get("redraws", 0),
            direction_mode=params.get("direction_mode", "both"),
            chop_threshold=params.get("chop_threshold", 0.22),
            admission="Chop skipped"
            if channel.get("tradeable") is False
            else "Expanding · entries paused"
            if channel.get("expanding")
            else "Eligible channel",
        )
        if channel.get("expanding"):
            extreme = number(channel.get("new_extreme"))
            below = channel["expanding"] == -1
            other = number(channel.get("hi" if below else "lo"))
            if extreme is not None and other is not None:
                _segment(view, "Expansion extreme (current)", "formation", as_of, end, extreme)
                _segment(
                    view,
                    "4h redraw close (current)",
                    "formation",
                    as_of,
                    end,
                    extreme + (other - extreme) / 3,
                )
                cap = number(params.get("max_width", 0.4))
                if cap is not None:
                    _segment(
                        view,
                        "Expansion width cap",
                        "stop",
                        as_of,
                        end,
                        other / (1 + cap) if below else other * (1 + cap),
                    )
        else:
            _range_geometry(
                view, channel.get("lo"), channel.get("hi"), as_of, end, params, eligible
            )
        confirmed = stamp(channel.get("confirmed_ts"))
        age_days = number(params.get("max_age_days", 120))
        if confirmed and age_days:
            view["status"]["expiry_utc"] = datetime.fromtimestamp(
                confirmed + int(age_days * 86400), UTC
            ).isoformat()
            _marker(
                view,
                "Range expiry",
                "expiry",
                confirmed,
                confirmed + int(age_days * 86400),
                origin="Derived schedule · not a fill",
            )
    if setup:
        side, extreme, swing = (
            setup.get("side", 1),
            number(setup.get("extreme")),
            number(setup.get("swing")),
        )
        _segment(view, "Impulse extreme (current)", "formation", as_of, end, extreme)
        if extreme is not None:
            if setup.get("pulled") and swing is not None:
                _segment(view, "Swing (current)", "formation", as_of, end, swing)
                _segment(
                    view,
                    "4h confirmation close (current)",
                    "formation",
                    as_of,
                    end,
                    swing + side * abs(extreme - swing) / 3,
                )
            else:
                pullback = number(params.get("pullback", 0.08))
                if pullback is not None:
                    _segment(
                        view,
                        "Pullback threshold (current)",
                        "formation",
                        as_of,
                        end,
                        extreme * (1 - side * pullback),
                    )
    events = state.get("events", [])
    events = events if isinstance(events, list) else []
    # close_bar event timestamps describe the source candle start, not knowledge time.
    close_events = {"pullback", "setup_cancel", "confirm", "redraw"}
    edges, active_since, active_id, skipped = None, None, None, False
    for event in events[-200:]:
        if not isinstance(event, dict):
            continue
        kind, t, detail = (
            str(event.get("kind", "")),
            stamp(event.get("ts")),
            metadata(event.get("detail")),
        )
        if t is None:
            continue
        known = (
            t + 14400
            if kind in close_events or (kind == "range_end" and detail.get("reason") == "trend")
            else t
        )
        if (
            kind in {"confirm", "redraw", "break", "range_end", "chop_skip", "impulse"}
            and edges
            and active_since
        ):
            _range_geometry(view, *edges, active_since, known, params, not skipped)
            active_since = known
        if kind == "confirm":
            edges = (detail.get("lo"), detail.get("hi"))
            active_since, active_id, skipped = known, detail.get("id"), False
        elif kind == "redraw":
            edges = (detail.get("lo"), detail.get("hi"))
            active_since = known
        elif kind in {"break", "range_end", "impulse"}:
            edges, active_since = None, None
        elif kind == "chop_skip":
            skipped = True
        if kind not in {"impulse_signal", "state_migrated"}:
            _marker(
                view,
                kind.replace("_", " ").title(),
                kind,
                t,
                known,
                detail.get("price", detail.get("extreme", detail.get("swing"))),
                detail=" · ".join(
                    f"{k}: {detail[k]}"
                    for k in (
                        "side",
                        "id",
                        "lo",
                        "hi",
                        "extreme",
                        "swing",
                        "er",
                        "reason",
                        "dir",
                        "redraws",
                    )
                    if k in detail
                ),
            )
        if kind == "pullback":
            _marker(view, "Pullback swing", "pullback", t, known, detail.get("swing"))
    if edges and active_since and channel.get("id") == active_id:
        _range_geometry(view, *edges, active_since, min(as_of or end, end), params, not skipped)
    view["coverage"]["warnings"].append(
        "Range events retain at most 200 observations. Continuous swing/expansion extremes are unavailable; current formation levels start at the snapshot."
    )
    if channel.get("tradeable") is False:
        view["coverage"]["warnings"].append(
            "Muted zones: this channel was skipped at confirmation; a later higher ER does not admit it."
        )
    if not state.get("model_complete", True):
        view["status"]["admission"] = "Model incomplete · entries blocked"
        view["coverage"]["warnings"].append(
            str(state.get("incomplete_reason") or "Range model incomplete")
        )
    if not state.get("entries_enabled", True):
        view["status"]["admission"] = "Entries disabled · owned exits continue"
    if state.get("mode") == "off":
        view["status"]["admission"] = "Disabled · owned exits continue"
    trades = state.get("paper_trades", [])
    for trade in trades[-500:] if isinstance(trades, list) else []:
        if not isinstance(trade, dict):
            continue
        for action in ("entry", "exit"):
            t = stamp(trade.get(f"{action}_ts"))
            _marker(
                view,
                f"Shadow {action}",
                f"shadow_{action}",
                t,
                t,
                trade.get(action, trade.get(f"{action}_price")),
                origin="Shadow · not funded",
                slot="r0",
            )
    paper = metadata(state.get("paper_position"))
    if paper:
        entered = stamp(paper.get("entry_ts"))
        _marker(
            view,
            "Open shadow entry",
            "shadow_entry",
            entered,
            entered,
            paper.get("entry"),
            origin="Shadow · not funded",
            slot="r0",
        )
        _segment(
            view,
            "Open shadow entry reference",
            "position",
            as_of,
            end,
            paper.get("entry"),
            origin="Current shadow snapshot · not funded",
        )
    holding = metadata(machine.get("position"))
    if holding and not paper:
        _segment(
            view,
            "Current model entry reference",
            "position",
            as_of,
            end,
            holding.get("entry_price"),
            origin="Current model snapshot · see confirmed execution markers",
        )


def _breakout(view, state, as_of, end):
    machine = metadata(state.get("machine"))
    campaign = metadata(machine.get("campaign"))
    view["status"].update(
        mode=str(state.get("direction_mode", "Unknown")),
        phase="Historical campaign"
        if campaign.get("origin") == "historical"
        else "Campaign"
        if campaign
        else "Watching daily closes",
        pending_exit=bool(machine.get("pending_exit") or state.get("closing_slots")),
    )
    # Prior close extrema are only known for the NEXT daily candle.
    daily = machine.get("candles", [])
    daily = daily if isinstance(daily, list) else []
    valid = [
        (stamp(c.get("ts")), number(c.get("close"))) for c in daily[-256:] if isinstance(c, dict)
    ]
    valid = [(t, c) for t, c in valid if t is not None and c is not None]
    for i in range(19, len(valid)):
        window = valid[i - 19 : i + 1]
        if any(b[0] - a[0] != 86400 for a, b in pairwise(window)):
            continue
        start = valid[i][0] + 86400
        for label, value in [
            ("Prior 20-close upper boundary", max(x[1] for x in window)),
            ("Prior 20-close lower boundary", min(x[1] for x in window)),
        ]:
            _segment(
                view,
                label,
                "trigger",
                start,
                min(start + 86400, end),
                value,
                origin="Derived from saved daily candles · structure filters also required",
            )
    if campaign:
        origin = (
            "Historical model · not funded"
            if campaign.get("origin") == "historical"
            else "Recorded campaign"
        )
        started = stamp(campaign.get("entry_ts"))
        _segment(
            view,
            "Campaign daily-close exit boundary",
            "stop",
            started,
            end,
            campaign.get("boundary"),
            origin=origin,
        )
        units = campaign.get("units", [])
        units = units if isinstance(units, list) else []
        for unit in units:
            if not isinstance(unit, dict):
                continue
            t, price = stamp(unit.get("entry_ts")), number(unit.get("entry_price"))
            slot = f"k{unit.get('k', '?')}"
            unit_origin = (
                "Historical model · not funded"
                if unit.get("origin") == "historical"
                else "Recorded campaign reference · see confirmed fills"
            )
            _segment(
                view,
                f"{slot.upper()} entry reference",
                "position",
                t,
                end,
                price,
                origin=unit_origin,
            )
            if unit.get("origin") == "historical":
                _marker(
                    view,
                    f"Modelled {slot.upper()} entry",
                    "model_entry",
                    t,
                    t,
                    price,
                    origin=unit_origin,
                    slot=slot,
                )
        parent = units[0] if units and isinstance(units[0], dict) else {}
        entry, peak = number(parent.get("entry_price")), number(campaign.get("peak_favorable"))
        side = campaign.get("side", 1)
        if entry is not None:
            _segment(
                view,
                "Add-on fill displacement limit (not a trigger)",
                "trigger",
                started,
                end,
                entry * (1 + side * 0.15),
                origin="Derived constraint · fresh daily signal required",
            )
            if peak is not None and peak > 0 and int(campaign.get("held_days", 0)) >= 85:
                _segment(
                    view,
                    "97%-of-peak recovery close (current)",
                    "target",
                    as_of,
                    end,
                    entry * (1 + side * 0.97 * peak),
                )
        if started:
            for days, label in [(85, "Recovery condition starts"), (120, "Maximum hold")]:
                view["status"][label] = datetime.fromtimestamp(
                    started + days * 86400, UTC
                ).isoformat()
                _marker(
                    view,
                    label,
                    "expiry",
                    started,
                    started + days * 86400,
                    origin="Derived schedule · not a fill",
                )
        view["status"].update(
            campaign=str(campaign.get("campaign_id", "")),
            held_days=campaign.get("held_days"),
            admission="New parent blocked by historical campaign"
            if campaign.get("origin") == "historical"
            else "Campaign occupied",
        )
    decisions = state.get("recent_decisions", [])
    for decision in decisions[-256:] if isinstance(decisions, list) else []:
        if not isinstance(decision, dict):
            continue
        t, meta = stamp(decision.get("ts")), metadata(decision.get("metadata"))
        _marker(
            view,
            str(decision.get("reason", "Decision")),
            "decision",
            stamp(meta.get("signal_ts")),
            t,
            meta.get("signal_close", decision.get("price")),
            origin="Recorded model decision · not a fill",
        )
    view["coverage"]["warnings"].append(
        "Add-ons need a fresh same-side daily breakout, structure/direction/cap checks and ≤15% parent displacement; there is no fixed K-price ladder. Historical recovery peaks are not a recorded series."
    )
    if not machine.get("historical_model_complete", True) or not machine.get(
        "historical_funding_available", True
    ):
        view["status"]["admission"] = "Historical evidence pending/incomplete · entries paused"


def _ma(view, state, as_of, end, ma_tf, candles):
    params = metadata(state.get("strategy_params"))
    tol = number(params.get("tolerance_pct", 0.005))
    current = metadata(metadata(state.get("timeframes")).get(ma_tf))
    sma, ema = number(current.get("sma")), number(current.get("ema"))
    completed = stamp(current.get("last_bar_ts"))
    view["status"].update(
        phase=str(current.get("verdict", "Awaiting indicators")),
        decision_timeframe=ma_tf,
        winner_remaining=metadata(state.get("winner_suppressed_signals")).get(ma_tf, 0),
        loss_remaining=metadata(state.get("loss_suppressed_signals")).get(ma_tf, 0),
        manual_hold=metadata(state.get("manual_flat_hold")).get(ma_tf),
        pending_exit=metadata(state.get("pending_position_reconciliation")).get(ma_tf),
    )
    if sma is None or ema is None or tol is None or completed is None:
        return
    for label, role, value in [
        ("SMA20 (last completed)", "average", sma),
        ("EMA21 (last completed)", "average", ema),
        ("Up verdict threshold (last completed)", "trigger", max(sma, ema) * (1 + tol)),
        ("Down verdict threshold (last completed)", "trigger", min(sma, ema) * (1 - tol)),
    ]:
        _segment(
            view,
            label,
            role,
            completed,
            end,
            value,
            origin="Recorded current indicator / derived threshold",
        )
    if end > completed:
        view["bands"].append(
            {
                "label": "Current tolerance / FLAT band",
                "role": "trigger",
                "start": completed,
                "end": end,
                "lower": min(sma, ema) * (1 - tol),
                "upper": max(sma, ema) * (1 + tol),
                "eligible": True,
                "origin": "Derived current decision thresholds",
            }
        )
    closes = current.get("closes", [])
    closes = [number(c) for c in closes[-64:]] if isinstance(closes, list) else []
    period = PERIODS[ma_tf]
    bars_by_end = {c["close_time"]: c for c in candles}
    points = {
        key: [] for key in ("SMA20", "EMA21", "Up verdict threshold", "Down verdict threshold")
    }
    anchor = ema
    # Invert the bounded saved close window, anchored at the recorded EMA.
    # Only publish points whose completed candle is independently present/complete.
    for i in range(len(closes) - 1, -1, -1):
        if closes[i] is None:
            break
        t = completed - (len(closes) - 1 - i) * period
        bar = bars_by_end.get(t)
        recent = closes[max(0, i - 19) : i + 1]
        if bar and bar["complete"] and abs(bar["close"] - closes[i]) < 1e-6:
            points["EMA21"].append({"time": t, "value": anchor})
            if len(recent) == 20 and all(c is not None for c in recent):
                s = sum(recent) / 20
                for key, value in [
                    ("SMA20", s),
                    ("Up verdict threshold", max(s, anchor) * (1 + tol)),
                    ("Down verdict threshold", min(s, anchor) * (1 - tol)),
                ]:
                    points[key].append({"time": t, "value": value})
        anchor = (anchor - closes[i] / 11) / (10 / 11)
    for label, values in points.items():
        view["series"].append(
            {
                "label": label,
                "role": "average" if label in {"SMA20", "EMA21"} else "trigger",
                "points": list(reversed(values)),
                "period": period,
                "origin": "Derived saved-close window · anchored to recorded EMA; matched complete local candles",
            }
        )
    down = {p["time"]: p["value"] for p in points["Down verdict threshold"]}
    for point in points["Up verdict threshold"]:
        t = point["time"]
        if t < completed:
            view["bands"].append(
                {
                    "label": "Reconstructed tolerance / FLAT band",
                    "role": "trigger",
                    "start": t,
                    "end": min(t + period, completed, end),
                    "lower": down[t],
                    "upper": point["value"],
                    "eligible": True,
                    "origin": "Derived saved-close decision thresholds",
                }
            )
    view["coverage"]["warnings"].append(
        "MA uses a completed close above/below both averages by the tolerance, not an SMA/EMA crossover. Only directional verdict transitions act; FLAT alone is not an exit. Cooldown blocks replacement entries, not owned exits."
    )
    view["coverage"]["warnings"].append(
        "MA history is a bounded display reconstruction, not an authoritative per-bar audit. Current thresholds describe the last decision; averages move at the next close."
    )


def _execution_markers(view, db, owner, start, end, ma_tf):
    cols = _columns(db, "orders")
    if cols and "strategy_instance_id" in cols:
        has_fills = bool(_columns(db, "fills"))
        price = (
            "COALESCE((SELECT price_usd FROM fills WHERE order_id=orders.id ORDER BY id DESC LIMIT 1),price_usd)"
            if has_fills
            else "price_usd"
        )
        rows = db.execute(
            f"SELECT ts,status,side,position_key,metadata_json,{price} FROM orders "
            "WHERE strategy_instance_id=? ORDER BY id DESC LIMIT 2000",
            (owner,),
        )
        for row in rows:
            t, meta = stamp(row[0]), metadata(row[4])
            if t is None or not start <= t < end or (owner == OWNERS["ma"] and row[3] != ma_tf):
                continue
            action = meta.get("isolated_action")
            confirmed = row[1] == "filled" and action in {"open", "close", "external_close"}
            if confirmed:
                kind = "entry" if action == "open" else "exit"
                _marker(
                    view,
                    f"Confirmed {kind} · {row[2]}",
                    kind,
                    t,
                    t,
                    row[5],
                    origin="Recorded execution",
                    slot=row[3],
                )
            else:
                _marker(
                    view,
                    f"Order {row[1]}",
                    "order_status",
                    t,
                    t,
                    row[5],
                    origin="Order outcome · not a confirmed fill",
                    slot=row[3],
                )
    cols = _columns(db, "signals")
    if cols and "strategy_instance_id" in cols:
        for row in db.execute(
            "SELECT ts,kind,reason,position_key,metadata_json FROM signals "
            "WHERE strategy_instance_id=? ORDER BY id DESC LIMIT 2000",
            (owner,),
        ):
            t, meta = stamp(row[0]), metadata(row[4])
            if t is None or not start <= t < end or (owner == OWNERS["ma"] and row[3] != ma_tf):
                continue
            _marker(
                view,
                str(row[2]),
                "signal",
                stamp(meta.get("signal_ts")),
                t,
                meta.get("signal_close", meta.get("close")),
                origin="Recorded intent · not a fill",
                slot=row[3],
                detail=str(row[1]),
            )


def chart_data(path: Path, *, strategy="ma", tf="1d", days=30, ma_tf="1d", end=None) -> dict:
    """Build a bounded versioned view. Opening a missing DB fails without creation."""
    opts = options(
        {
            "strategy": [strategy],
            "tf": [tf],
            "days": [str(days)],
            "ma_tf": [ma_tf],
            "end": [datetime.fromtimestamp(end, UTC).isoformat() if end is not None else None],
        }
    )
    owner, period = OWNERS[strategy], PERIODS[tf]
    days = opts["days"]
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3) as db:
        db.execute("PRAGMA query_only=ON")
        # One transaction: market rows and overlays describe the same SQLite view.
        db.execute("BEGIN")
        latest = db.execute("SELECT MAX(ts),MAX(id) FROM bars").fetchone()
        latest_ts = stamp(latest[0])
        state, as_of, run_id = _snapshot(db, owner)
        run = db.execute(
            "SELECT id,config_json FROM runs WHERE mode='live' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        fingerprint = (
            str(path.resolve()),
            tuple(opts.values()),
            latest[1],
            json.dumps(state, sort_keys=True),
            tuple(
                db.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
                if _columns(db, table)
                else 0
                for table in ("orders", "signals", "fills")
            ),
            (run[0], hash(run[1] or "")) if run else None,
            as_of,
            run_id,
        )
        # Cache keys store a bounded hash instead of retaining entire snapshots.
        fingerprint = (*fingerprint[:3], hash(fingerprint[3]), *fingerprint[4:])
        with _LOCK:
            cached = _CACHE.get(fingerprint)
            if cached and time.monotonic() - cached[0] < 10:
                _CACHE.move_to_end(fingerprint)
                return copy.deepcopy(cached[1])
        end = min(
            end or (latest_ts + 60 if latest_ts else int(datetime.now(UTC).timestamp())),
            latest_ts + 60 if latest_ts else int(datetime.now(UTC).timestamp()),
        )
        end = end - end % 60
        start = end - days * 86400
        query_start = start - start % period
        candles, missing = _candles(db, query_start, end, tf)
        view = {
            "schema_version": 1,
            "strategy": strategy,
            "instance_id": owner,
            "snapshot_version": state.get("version"),
            "rule_version": metadata(state.get("machine")).get("version"),
            "run_id": run_id,
            "state_as_of": as_of,
            "candles_as_of": latest_ts,
            "timeframe": tf,
            "start": start,
            "end": end,
            "candles": candles,
            "series": [],
            "segments": [],
            "bands": [],
            "markers": [],
            "status": {},
            "coverage": {"missing_minutes": missing, "warnings": []},
        }
        warnings = view["coverage"]["warnings"]
        if missing:
            warnings.append(
                f"{missing:,} minutes absent from recorded price history in this window; partial candles are marked. This is not a live model-health verdict."
            )
        supported = state.get("version") in ((1, 2) if strategy == "range" else (1,))
        rule_supported = strategy != "range" or metadata(state.get("machine")).get("version") in (
            1,
            2,
            3,
        )
        if not state:
            warnings.append("No saved strategy snapshot. Price history remains available.")
        elif not supported or not rule_supported:
            warnings.append("Unsupported snapshot/rule version; strategy overlays withheld.")
        else:
            try:
                if strategy == "range":
                    _range(view, state, as_of, end)
                elif strategy == "breakout":
                    _breakout(view, state, as_of, end)
                else:
                    ma_candles = (
                        candles
                        if tf == ma_tf
                        else _candles(db, query_start - 64 * PERIODS[ma_tf], end, ma_tf)[0]
                    )
                    _ma(view, state, as_of, end, ma_tf, ma_candles)
            except (ValueError, TypeError, KeyError, OverflowError):
                for field in ("series", "segments", "bands", "markers"):
                    view[field] = []
                view["status"] = {"admission": "Malformed snapshot · overlays withheld"}
                warnings.append(
                    "Saved strategy geometry is malformed; overlays withheld. Recorded prices and execution evidence remain available."
                )
        if state and run and run_id != run[0]:
            warnings.append(
                "Snapshot belongs to an earlier run; overlays are historical evidence, not current execution readiness."
            )
            view["status"]["admission"] = "Earlier-run snapshot · current admission unknown"
        if state.get("engine_data_health"):
            view["status"]["admission"] = "Market evidence incomplete · entries blocked"
        if run and not metadata(run[1]).get("live_entries_enabled", True):
            view["status"]["admission"] = "Global entries disabled · owned exits continue"
        if as_of and end - as_of > 86400:
            warnings.append(
                "Saved state is over a day behind displayed prices; inspect feed/owner health before relying on current levels."
            )
        _execution_markers(view, db, owner, start, end, ma_tf)
        view["segments"] = [s for s in view["segments"] if s["end"] > start and s["start"] < end]
        view["bands"] = [s for s in view["bands"] if s["end"] > start and s["start"] < end]
        view["markers"] = sorted(
            (m for m in view["markers"] if start <= m["time"] < end), key=lambda m: m["time"]
        )[-MAX_MARKERS:]
        for series in view["series"]:
            series["points"] = [p for p in series["points"] if start <= p["time"] <= end]
        warnings[:] = list(dict.fromkeys(warnings))
    with _LOCK:
        _CACHE[fingerprint] = (time.monotonic(), copy.deepcopy(view))
        while len(_CACHE) > 8:
            _CACHE.popitem(last=False)
    return view


def page(strategy="ma", tf="1d", days=30, ma_tf="1d", end=None) -> str:
    """Fixed HTML shell; URL parameters and DB prose are only handled as data."""
    options({"strategy": [strategy], "tf": [tf], "days": [str(days)], "ma_tf": [ma_tf]})
    return """<link rel="stylesheet" href="/assets/dashboard_chart.css">
<section id="strategy-chart" data-preserve-chart>
<h1>Strategy chart</h1><p class="muted">Recorded LN Markets price · UTC · levels explain decisions; they are not venue stop orders.</p>
<form id="chart-controls" class="chart-controls">
<label>Strategy <select name="strategy"><option value="ma">MA cross</option><option value="breakout">Close-range breakout</option><option value="range">Impulse range</option></select></label>
<label>Candles <select name="tf"><option>1d</option><option>4h</option><option>1m</option></select></label>
<label>MA decision timeframe <select name="ma_tf"><option>1d</option><option>4h</option></select></label>
<label>Window <select name="days"><option value="1">1 day</option><option value="7">7 days</option><option value="30">30 days</option><option value="90">90 days</option></select></label>
<button type="button" id="chart-latest">Latest / reset zoom</button><span class="muted">Wheel to zoom · drag to pan</span>
</form><div id="chart-layers" class="chart-controls" aria-label="Chart layers"></div>
<p class="chart-key muted">▲ confirmed executions · ○ model observations, intents or order outcomes · hover for source · muted zones mean skipped entries</p>
<p id="chart-error" role="status"></p>
<div class="chart-layout"><div class="chart-main"><div class="chart-canvas-wrap"><canvas id="chart-canvas" tabindex="0" aria-label="Price candles with strategy levels; use the level and event tables below for text"></canvas><div id="chart-tooltip" hidden></div></div><p id="chart-coverage" class="muted"></p></div>
<aside class="chart-inspector"><h2>Latest saved state</h2><dl id="chart-status"></dl><h2>Visible levels</h2><div id="chart-levels"></div></aside></div>
<details data-preserve-open><summary>Coverage and history limitations</summary><ul id="chart-warnings"></ul></details>
<details data-preserve-open><summary>Recorded events · select to inspect</summary><div id="chart-events"></div></details>
<script src="/assets/dashboard_chart.js" defer></script></section>"""
