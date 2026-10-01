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
H4 = PERIODS["4h"]
MAX_CANDLES = 1500
MAX_MARKERS = 2000
MA_WARMUP = 80  # candles before the window for SMA20/EMA21 display convergence
CACHE_SECONDS = 120
_CACHE: OrderedDict = OrderedDict()
_LOCK = threading.Lock()
_BUILD = threading.Lock()


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
    tf = query.get("tf", ["1d" if strategy == "ma" else "4h"])[0]
    days = query.get("days", ["1" if tf == "1m" else "90"])[0]
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
    period = PERIODS[tf]
    # SQLite aggregates in C: the last valid restart row wins for each minute,
    # then minutes fold into UTC candles. Python only sees the bounded result.
    rows = db.execute(
        """
        WITH m AS MATERIALIZED (
            SELECT ts AS minute, MAX(id) AS id FROM bars
            WHERE ts >= ? AND ts < ? AND open > 0 AND high > 0 AND low > 0 AND close > 0
            GROUP BY 1
        ), g AS (
            SELECT unixepoch(m.minute) / ? AS k, MIN(m.minute) AS first, MAX(m.minute) AS last,
                MAX(b.high) AS high, MIN(b.low) AS low, SUM(b.volume) AS volume, COUNT(*) AS n
            FROM m JOIN bars b ON b.id = m.id GROUP BY 1 HAVING k IS NOT NULL
        )
        SELECT g.k, o.open, g.high, g.low, c.close, g.volume, g.n FROM g
        JOIN m mo ON mo.minute = g.first JOIN bars o ON o.id = mo.id
        JOIN m mc ON mc.minute = g.last JOIN bars c ON c.id = mc.id
        ORDER BY g.k
        """,
        (sql_start, sql_end, period),
    ).fetchall()
    result, recorded = [], 0
    for k, o, h, low, c, volume, minutes in rows:
        key = k * period
        recorded += minutes
        result.append(
            {
                "time": key,
                "close_time": key + period,
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "volume": number(volume) or 0,
                "minutes": minutes,
                "source": "Recorded LN Markets minutes",
                "missing_minutes": period // 60 - minutes,
                "complete": minutes == period // 60 and key + period <= end,
            }
        )
    expected = max(0, (end - start) // 60)
    return result[-MAX_CANDLES:], max(0, expected - recorded)


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
    layer="model",
    direction=None,
    closed=False,
):
    """Record an event; `anchor` is the instant whose candle caused it.

    Close-based events become known at a candle boundary, so they anchor one
    second earlier, inside the candle that closed, on every display timeframe.
    """
    if known is None:
        return
    view["markers"].append(
        {
            "id": f"{kind}-{len(view['markers'])}",
            "label": label,
            "kind": kind,
            "layer": layer,
            "direction": direction if direction in (1, -1) else None,
            "observed_at": observed,
            "time": known,
            "known_at": known,
            "anchor": known - 1 if closed else known,
            "price": number(price),
            "origin": origin,
            "slot": str(slot),
            "detail": str(detail),
        }
    )


def _steps(view, rows, origin):
    """Emit per-bar display levels as merged step segments and zone bands.

    `rows` holds (start, end, levels, zones): levels map a label to
    (role, value, eligible); zones map a label to (role, lower, upper, eligible).
    """
    open_levels, open_zones = {}, {}

    def flush_level(label):
        start, end, role, value, eligible = open_levels.pop(label)
        _segment(view, label, role, start, end, value, origin=origin, eligible=eligible)

    def flush_zone(label):
        start, end, role, lower, upper, eligible = open_zones.pop(label)
        view["bands"].append(
            {
                "label": label,
                "role": role,
                "start": start,
                "end": end,
                "lower": lower,
                "upper": upper,
                "eligible": eligible,
                "origin": origin,
            }
        )

    for start, end, levels, zones in rows:
        for label in [k for k in open_levels if k not in levels]:
            flush_level(label)
        for label in [k for k in open_zones if k not in zones]:
            flush_zone(label)
        for label, (role, value, eligible) in levels.items():
            run = open_levels.get(label)
            if run and run[1] == start and run[3:] == (value, eligible):
                open_levels[label] = (*run[:1], end, *run[2:])
                continue
            if run:
                flush_level(label)
            open_levels[label] = (start, end, role, value, eligible)
        for label, (role, lower, upper, eligible) in zones.items():
            run = open_zones.get(label)
            if run and run[1] == start and run[3:] == (lower, upper, eligible):
                open_zones[label] = (*run[:1], end, *run[2:])
                continue
            if run:
                flush_zone(label)
            open_zones[label] = (start, end, role, lower, upper, eligible)
    for label in list(open_levels):
        flush_level(label)
    for label in list(open_zones):
        flush_zone(label)


# Range close-candle event timestamps describe the source 4h candle start.
RANGE_CLOSE_EVENTS = {"pullback", "setup_cancel", "confirm", "redraw"}
RANGE_MODEL_EVENTS = {
    "impulse",
    "pullback",
    "setup_cancel",
    "confirm",
    "chop_skip",
    "break",
    "redraw",
    "range_end",
}


def _channel_levels(lo, hi, params, eligible):
    """One bar of tradeable channel geometry: (levels, zones)."""
    width = hi - lo
    zone, tol = number(params.get("zone", 0.15)), number(params.get("tolerance", 0.1))
    if zone is None or tol is None or width <= 0:
        return {}, {}
    buy, sell = lo + zone * width, hi - zone * width
    levels = {
        "Channel low": ("channel", lo, eligible),
        "Channel high": ("channel", hi, eligible),
        "Buy ≤": ("entry", buy, eligible),
        "Sell ≥": ("entry", sell, eligible),
        "Midpoint target": ("target", (lo + hi) / 2, eligible),
        "4h close stop below": ("stop", lo - tol * width, eligible),
        "4h close stop above": ("stop", hi + tol * width, eligible),
    }
    zones = {
        "Buy zone": ("entry", lo, buy, eligible),
        "Sell zone": ("entry", sell, hi, eligible),
        "Lower stop band": ("stop", lo - tol * width, lo, eligible),
        "Upper stop band": ("stop", hi, hi + tol * width, eligible),
    }
    return levels, zones


def _expansion_levels(lo, hi, direction, extreme, params):
    """Broken channel: edges stay visible, entries pause, the new extreme is tracked."""
    levels = {"Channel low": ("channel", lo, False), "Channel high": ("channel", hi, False)}
    if extreme is None:
        return levels
    other = hi if direction == -1 else lo
    levels["Expansion extreme"] = ("formation", extreme, True)
    levels["4h redraw close"] = ("formation", extreme + (other - extreme) / 3, True)
    cap = number(params.get("max_width", 0.4))
    if cap is not None:
        levels["Expansion width cap"] = (
            "stop",
            other / (1 + cap) if direction == -1 else other * (1 + cap),
            True,
        )
    return levels


def _formation_levels(side, extreme, swing, params):
    if extreme is None:
        return {}
    levels = {"Impulse extreme": ("formation", extreme, True)}
    if swing is not None:
        levels["Swing"] = ("formation", swing, True)
        levels["4h confirmation close"] = (
            "formation",
            swing + side * abs(extreme - swing) / 3,
            True,
        )
    else:
        pullback = number(params.get("pullback", 0.08))
        if pullback is not None:
            levels["Pullback threshold"] = ("formation", extreme * (1 - side * pullback), True)
    return levels


def _range_geometry(view, lo, hi, start, end, params, eligible):
    lo, hi = number(lo), number(hi)
    if lo is None or hi is None or hi <= lo or start is None or end <= start:
        return
    _steps(
        view,
        [(start, end, *_channel_levels(lo, hi, params, bool(eligible)))],
        "Derived from recorded state",
    )


def _range_known(kind, t, detail):
    closed = kind in RANGE_CLOSE_EVENTS or (kind == "range_end" and detail.get("reason") == "trend")
    return t + H4 if closed else t


def _range_history(view, events, params, bars, until):
    """Replay retained machine events over recorded 4h candles, display only.

    Events fix every level the machine reported (impulse extreme, swing,
    channel edges). Between events, extremes are extended with recorded candle
    highs/lows exactly as the machine's close rules do; a bar shows the level
    after its own candle, like the research replay. Missing candles hold the
    previous level rather than leaving a gap.
    """
    queue = sorted(
        ((_range_known(kind, t, detail), kind, detail) for kind, t, detail in events),
        key=lambda e: e[0],
    )
    if not queue or until is None:
        return
    bar = queue[0][0] - queue[0][0] % H4
    formation = channel = None
    rows, i = [], 0
    while bar < until:
        while i < len(queue) and queue[i][0] <= bar:
            _, kind, detail = queue[i]
            i += 1
            if kind == "impulse":
                side = 1 if detail.get("side", 1) == 1 else -1
                formation = {"side": side, "extreme": number(detail.get("extreme")), "swing": None}
                channel = None
            elif kind == "pullback":
                extreme, swing = number(detail.get("extreme")), number(detail.get("swing"))
                side = (
                    formation["side"] if formation else 1 if (extreme or 0) > (swing or 0) else -1
                )
                formation = {"side": side, "extreme": extreme, "swing": swing}
            elif kind == "setup_cancel":
                formation = None
            elif kind in {"confirm", "redraw"}:
                lo, hi = number(detail.get("lo")), number(detail.get("hi"))
                skipped = bool(channel and channel["skipped"]) if kind == "redraw" else False
                formation = None
                channel = (
                    {"lo": lo, "hi": hi, "dir": 0, "extreme": None, "skipped": skipped}
                    if lo is not None and hi is not None and hi > lo
                    else None
                )
            elif kind == "chop_skip" and channel:
                channel["skipped"] = True
            elif kind == "break" and channel:
                channel["dir"] = -1 if detail.get("dir") == -1 else 1
                candle = bars.get(bar)
                channel["extreme"] = candle["open"] if candle else None
            elif kind == "range_end":
                channel = None
        candle = bars.get(bar)
        if candle and formation and formation["extreme"] is not None:
            side, pick = formation["side"], max if formation["side"] == 1 else min
            if formation["swing"] is None:
                formation["extreme"] = pick(
                    formation["extreme"], candle["high" if side == 1 else "low"]
                )
            else:
                other = min if side == 1 else max
                formation["swing"] = other(
                    formation["swing"], candle["low" if side == 1 else "high"]
                )
        if candle and channel and channel["dir"]:
            d = channel["dir"]
            edge = candle["low" if d == -1 else "high"]
            current = channel["extreme"]
            channel["extreme"] = (
                edge if current is None else min(current, edge) if d == -1 else max(current, edge)
            )
        levels, zones = {}, {}
        if channel and channel["dir"]:
            levels = _expansion_levels(
                channel["lo"], channel["hi"], channel["dir"], channel["extreme"], params
            )
        elif channel:
            levels, zones = _channel_levels(
                channel["lo"], channel["hi"], params, not channel["skipped"]
            )
        elif formation:
            levels = _formation_levels(
                formation["side"], formation["extreme"], formation["swing"], params
            )
        rows.append((bar, min(bar + H4, until), levels, zones))
        bar += H4
    _steps(view, rows, "Replayed from recorded events and 4h candles · display only")


def _range(view, state, as_of, end, load_4h, since):
    machine = metadata(state.get("machine"))
    params = metadata(machine.get("params"))
    channel = metadata(machine.get("channel"))
    setup = metadata(machine.get("setup"))
    view["status"].update(
        phase=str(machine.get("state", "Awaiting state")),
        mode=str(state.get("mode", "Unknown")),
        pending_exit=bool(state.get("closing")),
    )
    current = as_of if as_of is not None and as_of < end else end
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
            lo, hi = number(channel.get("lo")), number(channel.get("hi"))
            if lo is not None and hi is not None:
                _steps(
                    view,
                    [
                        (
                            current,
                            end,
                            _expansion_levels(
                                lo,
                                hi,
                                -1 if channel["expanding"] == -1 else 1,
                                number(channel.get("new_extreme")),
                                params,
                            ),
                            {},
                        )
                    ]
                    if end > current
                    else [],
                    "Derived from recorded state",
                )
        else:
            _range_geometry(
                view, channel.get("lo"), channel.get("hi"), current, end, params, eligible
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
                layer="schedule",
            )
    if setup and end > current:
        side = -1 if setup.get("side", 1) == -1 else 1
        swing = number(setup.get("swing")) if setup.get("pulled") else None
        _steps(
            view,
            [
                (
                    current,
                    end,
                    _formation_levels(side, number(setup.get("extreme")), swing, params),
                    {},
                )
            ],
            "Derived from recorded state",
        )
    raw = state.get("events", [])
    events = []
    for event in raw[-200:] if isinstance(raw, list) else []:
        if not isinstance(event, dict):
            continue
        kind, t = str(event.get("kind", "")), stamp(event.get("ts"))
        if t is not None:
            events.append((kind, t, metadata(event.get("detail"))))
    if events:
        # Every replayed level is reset by an event, so candles are only needed
        # from the last event before the window (or the first retained event).
        known = sorted(_range_known(*event) for event in events)
        load_from = max((k for k in known if k <= since), default=known[0])
        if current > load_from:
            bars = {c["time"]: c for c in load_4h(load_from - load_from % H4, current)}
            _range_history(view, events, params, bars, current)
    for kind, t, detail in events:
        if kind in {"impulse_signal", "state_migrated"}:
            continue
        known = _range_known(kind, t, detail)
        closed = known != t
        if kind == "pullback":
            price = detail.get("swing")
        else:
            price = detail.get("price", detail.get("extreme"))
        _marker(
            view,
            kind.replace("_", " ").title(),
            f"model_{kind}" if kind in {"entry", "exit"} else kind,
            t,
            known,
            price,
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
            layer="model" if kind in RANGE_MODEL_EVENTS else "diagnostic",
            direction=detail.get("side") if kind == "impulse" else None,
            closed=closed,
        )
    view["coverage"]["warnings"].append(
        "Range events retain at most 200 observations. Formation and expansion levels between events are replayed from recorded 4h candles for display; the saved snapshot defines current levels."
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
                direction=trade.get("side"),
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
            direction=paper.get("side"),
        )
        _segment(
            view,
            "Open shadow entry reference",
            "position",
            current,
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
            current,
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
    rows = []
    for i in range(19, len(valid)):
        window = valid[i - 19 : i + 1]
        if any(b[0] - a[0] != 86400 for a, b in pairwise(window)):
            continue
        start = valid[i][0] + 86400
        if start < end:
            closes = [x[1] for x in window]
            rows.append(
                (
                    start,
                    min(start + 86400, end),
                    {
                        "Prior 20-close upper boundary": ("trigger", max(closes), True),
                        "Prior 20-close lower boundary": ("trigger", min(closes), True),
                    },
                    {},
                )
            )
    _steps(
        view,
        rows,
        "Derived from saved daily candles · structure filters also required",
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
                    direction=campaign.get("side", 1),
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
                    layer="schedule",
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
        signal = stamp(meta.get("signal_ts"))
        _marker(
            view,
            str(decision.get("reason", "Decision")),
            "decision",
            signal,
            t,
            meta.get("signal_close", decision.get("price")),
            origin="Recorded model decision · not a fill",
            # Decisions follow the daily close of the signal candle.
            closed=signal is not None and t is not None and signal < t,
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
        ("SMA20", "average", sma),
        ("EMA21", "average", ema),
        ("Up verdict threshold", "trigger", max(sma, ema) * (1 + tol)),
        ("Down verdict threshold", "trigger", min(sma, ema) * (1 - tol)),
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
    # The live rule aggregates the same recorded minutes into right-labelled
    # candles: SMA20, and EMA21 seeded with SMA21. Partial candles keep their
    # last recorded close rather than leaving a hole in the averages.
    period = PERIODS[ma_tf]
    closes = [(c["close_time"], c["close"]) for c in candles if c["close_time"] <= completed]
    points = {
        key: [] for key in ("SMA20", "EMA21", "Up verdict threshold", "Down verdict threshold")
    }
    average = None
    for i in range(20, len(closes)):
        t, close = closes[i]
        s = sum(c for _, c in closes[i - 19 : i + 1]) / 20
        average = (
            sum(c for _, c in closes[i - 20 : i + 1]) / 21
            if average is None
            else close * (2 / 22) + average * (20 / 22)
        )
        for key, value in [
            ("SMA20", s),
            ("EMA21", average),
            ("Up verdict threshold", max(s, average) * (1 + tol)),
            ("Down verdict threshold", min(s, average) * (1 - tol)),
        ]:
            points[key].append({"time": t, "value": value})
    for label, values in points.items():
        view["series"].append(
            {
                "label": label,
                "role": "average" if label in {"SMA20", "EMA21"} else "trigger",
                "points": values,
                "period": period,
                "origin": "Recomputed from recorded candles · display only",
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
                    "origin": "Recomputed decision thresholds",
                }
            )
    last = points["EMA21"][-1] if points["EMA21"] else None
    if last and last["time"] == completed and abs(last["value"] / ema - 1) > 0.0025:
        view["coverage"]["warnings"].append(
            f"Recomputed EMA21 differs from the saved indicator by {abs(last['value'] / ema - 1):.2%}; recorded minute gaps or seeded history differ from the live warm-up."
        )
    view["coverage"]["warnings"].append(
        "MA uses a completed close above/below both averages by the tolerance, not an SMA/EMA crossover. Only directional verdict transitions act; FLAT alone is not an exit. Cooldown blocks replacement entries, not owned exits."
    )
    view["coverage"]["warnings"].append(
        "MA history is recomputed from recorded candles for display, not an authoritative per-bar audit. Levels after the last completed close are the saved indicator; averages move at the next close."
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
                # Entries buy long / sell short; exits sell to close long / buy to close short.
                buy = str(row[2]).lower() == "buy"
                direction = (1 if buy else -1) if kind == "entry" else (-1 if buy else 1)
                _marker(
                    view,
                    f"Confirmed {'long' if direction == 1 else 'short'} {kind} · {row[2]}",
                    kind,
                    t,
                    t,
                    row[5],
                    origin="Recorded execution",
                    slot=row[3],
                    layer="execution",
                    direction=direction,
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
                    layer="diagnostic",
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
            reason = str(row[2])
            signal = stamp(meta.get("signal_ts"))
            slot_period = PERIODS.get(str(row[3]))
            # MA intents are stamped at the decision candle's right-labelled close.
            closed = (signal is not None and signal < t) or bool(
                slot_period and t % slot_period == 0
            )
            routine = any(
                word in reason
                for word in ("cool_off", "restart_state_aligned", "already_matches", "manual")
            )
            _marker(
                view,
                reason,
                "signal",
                signal,
                t,
                meta.get("signal_close", meta.get("close")),
                origin="Recorded intent · not a fill",
                slot=row[3],
                detail=str(row[1]),
                layer="diagnostic" if routine else "intent",
                closed=closed,
            )


def chart_data(path: Path, *, strategy="ma", tf="1d", days=30, ma_tf="1d", end=None) -> dict:
    """Build a bounded versioned view. Opening a missing DB fails without creation.

    Builds run one at a time: concurrent long-window aggregations would each
    hold SQLite sort memory inside the dashboard's 160 MiB cap, while a queued
    request costs ~150 ms or is answered by the cache the previous build filled.
    """
    with _BUILD:
        return _chart_data(path, strategy=strategy, tf=tf, days=days, ma_tf=ma_tf, end=end)


def _chart_data(path, *, strategy, tf, days, ma_tf, end):
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
        # A small page cache bounds aggregation memory at no measured speed cost.
        db.execute("PRAGMA cache_size=-512")
        # One transaction: market rows and overlays describe the same SQLite view.
        db.execute("BEGIN")
        # bars.ts is unindexed; recent ids bound the latest-minute lookup.
        latest = db.execute(
            "SELECT MAX(ts),MAX(id) FROM (SELECT ts,id FROM bars ORDER BY id DESC LIMIT 5000)"
        ).fetchone()
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
            # Content-addressed by bar/state/journal fingerprints; age only bounds memory.
            if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
                _CACHE.move_to_end(fingerprint)
                return copy.deepcopy(cached[1])
        end = min(
            end or (latest_ts + 60 if latest_ts else int(datetime.now(UTC).timestamp())),
            latest_ts + 60 if latest_ts else int(datetime.now(UTC).timestamp()),
        )
        end = end - end % 60
        start = end - days * 86400
        query_start = start - start % period
        if strategy == "ma" and tf == ma_tf:
            # One aggregation serves both the display and the indicator warm-up.
            warm = _candles(db, query_start - MA_WARMUP * period, end, tf)[0]
            candles = [c for c in warm if c["time"] >= query_start]
            missing = max(0, (end - query_start) // 60 - sum(c["minutes"] for c in candles))
        else:
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
                    _range(
                        view,
                        state,
                        as_of,
                        end,
                        lambda a, b: (
                            [c for c in candles if a <= c["time"] < b]
                            if tf == "4h" and a >= query_start
                            else _candles(db, a, b, "4h")[0]
                        ),
                        query_start,
                    )
                elif strategy == "breakout":
                    _breakout(view, state, as_of, end)
                else:
                    ma_candles = (
                        warm
                        if tf == ma_tf
                        else _candles(db, query_start - MA_WARMUP * PERIODS[ma_tf], end, ma_tf)[0]
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
            # Keep one point before the window so lines enter from the left edge.
            series["points"] = [
                p for p in series["points"] if start - series["period"] <= p["time"] <= end
            ]
        warnings[:] = list(dict.fromkeys(warnings))
    with _LOCK:
        _CACHE[fingerprint] = (time.monotonic(), copy.deepcopy(view))
        while len(_CACHE) > 8:
            _CACHE.popitem(last=False)
    return view


def page(strategy="ma", tf="1d", days=90, ma_tf="1d", end=None) -> str:
    """Fixed HTML shell; URL parameters and DB prose are only handled as data."""
    options({"strategy": [strategy], "tf": [tf], "days": [str(days)], "ma_tf": [ma_tf]})
    return """<link rel="stylesheet" href="/assets/dashboard_chart.css">
<section id="strategy-chart" data-preserve-chart>
<div class="chart-head"><h1>Strategy chart</h1>
<form id="chart-controls" class="chart-controls">
<label>Strategy <select name="strategy"><option value="ma">MA cross</option><option value="breakout">Close-range breakout</option><option value="range">Impulse range</option></select></label>
<label id="chart-tf">Timeframe <select name="tf"><option>1d</option><option>4h</option></select></label>
<button type="button" id="chart-latest">Reset zoom</button>
</form></div>
<dl id="chart-status" class="chart-state" aria-label="Latest saved state"></dl>
<div id="chart-layers" class="chart-controls chart-layers" aria-label="Chart layers"></div>
<p class="chart-key muted"><b class="up">▲</b> long entry · <b class="down">▼</b> short entry · <b>●</b> exit (ring = side) · <b class="model">△ ○</b> unfunded model · <b class="model">◆</b> model event · <b class="intent">○</b> intent · <b class="schedule">┊</b> schedule · grey dashed = entries skipped/paused</p>
<p id="chart-error" role="status"></p>
<div class="chart-layout"><div class="chart-canvas-wrap"><canvas id="chart-canvas" tabindex="0" aria-label="Price candles with strategy levels; the levels panel and events table give the values as text"></canvas><div id="chart-tooltip" hidden></div></div>
<aside class="chart-inspector"><h2>Levels <span id="chart-levels-time"></span></h2><div id="chart-levels"></div></aside></div>
<section class="chart-events-panel"><h2>Events</h2><div class="chart-events-wrap"><table><thead><tr><th>Time (UTC)</th><th>Event</th><th>Slot</th><th>Price</th><th>Source</th></tr></thead><tbody id="chart-events"></tbody></table></div></section>
<script src="/assets/dashboard_chart.js" defer></script></section>"""
