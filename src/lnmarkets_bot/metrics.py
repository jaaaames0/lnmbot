"""Trade pairing + comparison metrics from recorded fills.

Pure functions over lists of fills. No DB I/O — the caller passes the fills.

v1.1: under isolated margin each subscribed TF maintains its own position.
Trades must be paired **per TF** (a 1d trade and a 4h trade are independent
and have separate P&L). The `FillRow.trigger_tf` field is the join key.

`pair_fills_into_trades(fills)` is kept for the v0 single-TF case (fills
without a `trigger_tf`). New code should use `pair_fills_by_tf(fills)`
which returns a `dict[str, list[Trade]]` keyed by timeframe.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev


@dataclass
class FillRow:
    ts: object  # datetime
    order_id: int
    qty_sats: int
    price_usd: float
    fee_sats: int
    side: str  # "buy" | "sell"
    trigger_tf: str = ""  # "" for v0 single-TF case


@dataclass
class Trade:
    side: str  # "long" | "short"
    entry_ts: object
    exit_ts: object
    qty_sats: int
    entry_price_usd: float
    exit_price_usd: float
    fees_sats: int
    pnl_usd: float  # realized, after fees
    trigger_tf: str = ""


def _pair_walker(fills: list[FillRow]) -> list[Trade]:
    """Pair (entry, exit) fills in order. Used internally by both
    `pair_fills_into_trades` (single TF) and `pair_fills_by_tf` (per-TF)."""
    trades: list[Trade] = []
    open_side: str | None = None
    open_qty = 0
    open_price = 0.0
    open_ts = None
    open_fees = 0
    open_tf = ""

    def close_trade(exit_fill: FillRow) -> Trade:
        pnl_per_sat = (exit_fill.price_usd - open_price) / 1e8
        if open_side == "short":
            pnl_per_sat = -pnl_per_sat
        # Fees are recorded in BTC sats by LN Markets.  Convert each fill at
        # its own fill price before subtracting from USD P&L; subtracting sats
        # directly from dollars understated trading costs by roughly BTC/USD.
        fee_usd = (open_fees * open_price + exit_fill.fee_sats * exit_fill.price_usd) / 1e8
        pnl_usd = pnl_per_sat * open_qty - fee_usd
        return Trade(
            side=open_side or "long",
            entry_ts=open_ts,
            exit_ts=exit_fill.ts,
            qty_sats=open_qty,
            entry_price_usd=open_price,
            exit_price_usd=exit_fill.price_usd,
            fees_sats=open_fees + exit_fill.fee_sats,
            pnl_usd=pnl_usd,
            trigger_tf=open_tf,
        )

    for f in fills:
        if open_side is None:
            open_side = "long" if f.side == "buy" else "short"
            open_qty = f.qty_sats
            open_price = f.price_usd
            open_ts = f.ts
            open_fees = f.fee_sats
            open_tf = f.trigger_tf
            continue
        if (open_side == "long" and f.side == "buy") or (open_side == "short" and f.side == "sell"):
            new_qty = open_qty + f.qty_sats
            open_price = (open_price * open_qty + f.price_usd * f.qty_sats) / new_qty
            open_qty = new_qty
            open_fees += f.fee_sats
            continue
        if f.qty_sats >= open_qty:
            trades.append(close_trade(f))
            remainder = f.qty_sats - open_qty
            if remainder > 0:
                open_side = "long" if f.side == "buy" else "short"
                open_qty = remainder
                open_price = f.price_usd
                open_ts = f.ts
                open_fees = 0
            else:
                open_side = None
                open_qty = 0
                open_price = 0.0
                open_ts = None
                open_fees = 0
        else:
            partial = FillRow(
                ts=f.ts,
                order_id=f.order_id,
                qty_sats=open_qty,
                price_usd=f.price_usd,
                fee_sats=int(f.fee_sats * open_qty / f.qty_sats) if f.qty_sats else 0,
                side=("sell" if open_side == "long" else "buy"),
                trigger_tf=open_tf,
            )
            trades.append(close_trade(partial))
            open_qty -= f.qty_sats
            open_fees = int(open_fees * open_qty / (open_qty + f.qty_sats))
    return trades


def pair_fills_into_trades(fills: list[FillRow]) -> list[Trade]:
    """Pair fills into trades (v0 single-TF). For per-TF use `pair_fills_by_tf`."""
    return _pair_walker(fills)


def pair_fills_by_tf(fills: list[FillRow]) -> dict[str, list[Trade]]:
    """Pair fills into trades per timeframe.

    Returns: `{"1d": [Trade, ...], "4h": [...]}`. A fill with `trigger_tf=""`
    is bucketed under "" (the v0 single-TF case). Each TF's trades are
    independent: a 1d close doesn't touch a 4h trade.
    """
    by_tf: dict[str, list[FillRow]] = {}
    for f in fills:
        by_tf.setdefault(f.trigger_tf or "", []).append(f)
    out: dict[str, list[Trade]] = {}
    for tf, tf_fills in by_tf.items():
        out[tf] = _pair_walker(tf_fills)
    return out


def chop_buckets(
    trades: list[Trade],
    *,
    notional_per_trade_usd: float,
    thresholds_pct: tuple[float, ...] = (0.01, 0.02),
) -> dict[float, dict[str, float | int]]:
    """Bucket trades by |P&L| < threshold % of notional.

    Returns: { 0.01: {count, sum_pnl_usd, median_pnl_usd, median_hold_minutes}, ... }
    """
    out: dict[float, dict[str, float | int]] = {}
    for thr in thresholds_pct:
        bucket = [t for t in trades if abs(t.pnl_usd) < thr * notional_per_trade_usd]
        if not bucket:
            out[thr] = {
                "count": 0,
                "sum_pnl_usd": 0.0,
                "median_pnl_usd": 0.0,
                "median_hold_minutes": 0.0,
            }
            continue
        pnls = sorted(t.pnl_usd for t in bucket)
        med = (
            pnls[len(pnls) // 2]
            if len(pnls) % 2
            else (pnls[len(pnls) // 2 - 1] + pnls[len(pnls) // 2]) / 2
        )
        holds_min = sorted((t.exit_ts - t.entry_ts).total_seconds() / 60.0 for t in bucket)
        med_hold = holds_min[len(holds_min) // 2]
        out[thr] = {
            "count": len(bucket),
            "sum_pnl_usd": sum(t.pnl_usd for t in bucket),
            "median_pnl_usd": med,
            "median_hold_minutes": med_hold,
        }
    return out


def sharpe_from_returns(returns: list[float], *, periods_per_year: float) -> float:
    if len(returns) < 2:
        return 0.0
    mu = mean(returns)
    sigma = pstdev(returns)
    if sigma == 0:
        return 0.0
    return (mu / sigma) * sqrt(periods_per_year)


def max_drawdown_pct(equity: list[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd
    return mdd


def win_rate(trades: list[Trade]) -> float:
    if not trades:
        return 0.0
    return sum(1 for t in trades if t.pnl_usd > 0) / len(trades)


def median_hold_minutes(trades: list[Trade]) -> float:
    if not trades:
        return 0.0
    holds = sorted((t.exit_ts - t.entry_ts).total_seconds() / 60.0 for t in trades)
    return holds[len(holds) // 2]


def avg_pnl_usd(trades: list[Trade]) -> float:
    if not trades:
        return 0.0
    return sum(t.pnl_usd for t in trades) / len(trades)


def max_consecutive_losers(trades: list[Trade]) -> int:
    max_run = 0
    cur = 0
    for t in trades:
        if t.pnl_usd < 0:
            cur += 1
            if cur > max_run:
                max_run = cur
        else:
            cur = 0
    return max_run


def exposure_pct(account_snapshots: list[tuple[object, int, int, int, int]]) -> float:
    """`account_snapshots` rows: (ts, balance_sats, equity_sats, margin_used_sats, unrealized_pnl_sats).
    Exposure = fraction of snapshots with non-zero margin.
    """
    if not account_snapshots:
        return 0.0
    in_pos = sum(1 for r in account_snapshots if r[2] != 0)
    return in_pos / len(account_snapshots)


def per_tf_summary(
    by_tf_trades: dict[str, list[Trade]],
    *,
    notional_per_trade_usd: float,
) -> dict[str, dict]:
    """Compute a full metric summary per TF + an aggregate.

    Returns:
        {
            "by_tf": {
                "1d": {n_trades, total_pnl_usd, win_rate, ...},
                "4h": {...}
            },
            "aggregate": {n_trades, total_pnl_usd, ...}
        }
    """
    out: dict[str, dict] = {"by_tf": {}, "aggregate": {}}
    all_trades: list[Trade] = []
    for tf, trades in sorted(by_tf_trades.items()):
        bucket = {
            "n_trades": len(trades),
            "total_pnl_usd": sum(t.pnl_usd for t in trades),
            "win_rate": win_rate(trades),
            "avg_pnl_usd": avg_pnl_usd(trades),
            "median_hold_minutes": median_hold_minutes(trades),
            "max_consecutive_losers": max_consecutive_losers(trades),
            "chop_lt_1pct": chop_buckets(trades, notional_per_trade_usd=notional_per_trade_usd)[
                0.01
            ],
            "chop_lt_2pct": chop_buckets(trades, notional_per_trade_usd=notional_per_trade_usd)[
                0.02
            ],
        }
        out["by_tf"][tf] = bucket
        all_trades.extend(trades)
    if all_trades:
        out["aggregate"] = {
            "n_trades": len(all_trades),
            "total_pnl_usd": sum(t.pnl_usd for t in all_trades),
            "win_rate": win_rate(all_trades),
            "avg_pnl_usd": avg_pnl_usd(all_trades),
            "median_hold_minutes": median_hold_minutes(all_trades),
            "max_consecutive_losers": max_consecutive_losers(all_trades),
            "chop_lt_1pct": chop_buckets(all_trades, notional_per_trade_usd=notional_per_trade_usd)[
                0.01
            ],
            "chop_lt_2pct": chop_buckets(all_trades, notional_per_trade_usd=notional_per_trade_usd)[
                0.02
            ],
        }
    return out
