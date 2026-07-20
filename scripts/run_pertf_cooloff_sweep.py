"""Per-TF cool-off parameter sweep.

1d and 4h have very different signal densities (180 1d bars vs 1080 4h
bars in 6 months). The optimal cool-off threshold and signal count are
likely different. This sweep varies them independently.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.inmemory import run_inmemory
from lnmarkets_bot.logging import configure_logging
from lnmarkets_bot.metrics import (
    FillRow,
    max_drawdown_pct,
    pair_fills_by_tf,
    per_tf_summary,
)
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots, fills, orders as orders_t
from lnmarkets_bot.strategy.ma_cross import MaCross


# Default grid: coarse-to-refine.
DEFAULT_1D_THRESHOLDS = [0.05, 0.08, 0.10]
DEFAULT_1D_COUNTS = [4, 6, 8]
DEFAULT_4H_THRESHOLDS = [0.02, 0.03, 0.05]
DEFAULT_4H_COUNTS = [1, 2, 3]


def _load_fills_and_equity(engine, run_id: int) -> tuple[list[FillRow], list[float]]:
    fac = make_session_factory(engine)
    fill_rows: list[FillRow] = []
    with fac() as s:
        for r in s.execute(
            select(
                fills.c.ts, fills.c.order_id, fills.c.qty_sats,
                fills.c.price_usd, fills.c.fee_sats, orders_t.c.side,
                orders_t.c.trigger_tf,
            )
            .join(orders_t, fills.c.order_id == orders_t.c.id)
            .where(orders_t.c.run_id == run_id)
            .order_by(fills.c.ts.asc(), fills.c.order_id.asc())
        ).fetchall():
            fill_rows.append(FillRow(
                ts=r.ts, order_id=r.order_id, qty_sats=r.qty_sats,
                price_usd=r.price_usd, fee_sats=r.fee_sats,
                side=r.side, trigger_tf=r.trigger_tf or "",
            ))
        eq_rows = s.execute(
            select(account_snapshots.c.equity_sats)
            .where(account_snapshots.c.run_id == run_id)
            .order_by(account_snapshots.c.ts.asc())
        ).fetchall()
    return fill_rows, [r[0] / 1e8 for r in eq_rows]


async def run_one(
    *, cfg: BotConfig, parquet: Path,
    threshold_1d: float, count_1d: int,
    threshold_4h: float, count_4h: int,
    base_size: float, base_leverage: float,
) -> dict[str, Any]:
    ds = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h"),
    )
    strat = MaCross(params={
        "tolerance_pct": 0.003,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "base_size_usd": base_size,
        "base_leverage": base_leverage,
        "cooldown_threshold_pct": {"1d": threshold_1d, "4h": threshold_4h},
        "cooldown_signal_count": {"1d": count_1d, "4h": count_4h},
    })
    run_id = await run_inmemory(
        cfg=cfg, data_source=ds, strategy=strat, install_signal_handlers=False,
    )
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    fills_data, eq = _load_fills_and_equity(engine, run_id)
    by_tf_trades = pair_fills_by_tf(fills_data)
    summary = per_tf_summary(by_tf_trades, notional_per_trade_usd=base_size)
    return {
        "thr_1d": threshold_1d, "cnt_1d": count_1d,
        "thr_4h": threshold_4h, "cnt_4h": count_4h,
        "summary": summary,
        "max_dd_pct": max_drawdown_pct(eq),
    }


def print_table(rows: list[dict]) -> None:
    cols = [
        ("1d_thr", 6), ("1d_n", 4), ("4h_thr", 6), ("4h_n", 4),
        ("1d_n_tr", 6), ("1d_pnl", 9),
        ("4h_n_tr", 6), ("4h_pnl", 9),
        ("agg_pnl", 9), ("max_dd%", 7),
    ]
    header = "  ".join(f"{n:<{w}}" for n, w in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        s = r["summary"]
        o1 = s["by_tf"].get("1d", {})
        o4 = s["by_tf"].get("4h", {})
        a = s.get("aggregate", {})
        row = [
            (f"{r['thr_1d']:.2%}", 6),
            (r["cnt_1d"], 4),
            (f"{r['thr_4h']:.2%}", 6),
            (r["cnt_4h"], 4),
            (o1.get("n_trades", 0), 6),
            (f"{o1.get('total_pnl_usd', 0):+.0f}", 9),
            (o4.get("n_trades", 0), 6),
            (f"{o4.get('total_pnl_usd', 0):+.0f}", 9),
            (f"{a.get('total_pnl_usd', 0):+.0f}", 9),
            (f"{r['max_dd_pct']:.1%}", 7),
        ]
        print("  ".join(f"{str(v):<{w}}" for v, w in row))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path,
                        default=Path("./data/cache/btcusdt_perp_1m_6m.parquet"))
    parser.add_argument("--base-size", type=float, default=1000.0)
    parser.add_argument("--base-leverage", type=float, default=2.0)
    parser.add_argument("--db-dir", type=Path, default=Path("./runs"))
    parser.add_argument("--1d-thresholds", type=float, nargs="+",
                        default=DEFAULT_1D_THRESHOLDS)
    parser.add_argument("--1d-counts", type=int, nargs="+", default=DEFAULT_1D_COUNTS)
    parser.add_argument("--4h-thresholds", type=float, nargs="+",
                        default=DEFAULT_4H_THRESHOLDS)
    parser.add_argument("--4h-counts", type=int, nargs="+", default=DEFAULT_4H_COUNTS)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    configure_logging("WARNING")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    total = (
        len(args._1d_thresholds if hasattr(args, "_1d_thresholds") else args.__dict__.get("1d_thresholds", DEFAULT_1D_THRESHOLDS))
        * len(args._1d_counts if hasattr(args, "_1d_counts") else args.__dict__.get("1d_counts", DEFAULT_1D_COUNTS))
        * len(args._4h_thresholds if hasattr(args, "_4h_thresholds") else args.__dict__.get("4h_thresholds", DEFAULT_4H_THRESHOLDS))
        * len(args._4h_counts if hasattr(args, "_4h_counts") else args.__dict__.get("4h_counts", DEFAULT_4H_COUNTS))
    )
    # Use the actual flag names from argparse
    td = args.__dict__.get("1d_thresholds", DEFAULT_1D_THRESHOLDS)
    cd = args.__dict__.get("1d_counts", DEFAULT_1D_COUNTS)
    th = args.__dict__.get("4h_thresholds", DEFAULT_4H_THRESHOLDS)
    ch = args.__dict__.get("4h_counts", DEFAULT_4H_COUNTS)
    total = len(td) * len(cd) * len(th) * len(ch)
    n = 0
    for thr1 in td:
        for cnt1 in cd:
            for thr4 in th:
                for cnt4 in ch:
                    n += 1
                    db_path = args.db_dir / f"ptc_{n}.sqlite"
                    cfg = BotConfig(
                        storage_db_path=db_path,
                        strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
                        initial_balance_usd=10_000.0,
                        risk_max_position_usd=10_000.0,
                        risk_max_leverage=10.0,
                        risk_max_daily_loss_usd=1_000_000.0,
                        risk_max_orders_per_minute=10_000,
                    )
                    print(f"[ptc] {n}/{total} 1d:({thr1},{cnt1}) 4h:({thr4},{cnt4}) ...", flush=True)
                    rows.append(asyncio.run(run_one(
                        cfg=cfg, parquet=args.data,
                        threshold_1d=thr1, count_1d=cnt1,
                        threshold_4h=thr4, count_4h=cnt4,
                        base_size=args.base_size, base_leverage=args.base_leverage,
                    )))

    # Sort by aggregate P&L
    rows.sort(key=lambda r: r["summary"].get("aggregate", {}).get("total_pnl_usd", 0), reverse=True)

    print()
    print_table(rows)
    if args.json_out:
        args.json_out.write_text(json.dumps(rows, indent=2, default=str))
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()