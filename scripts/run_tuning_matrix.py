"""Tuning matrix for the MA-cross strategy under isolated margin.

Sweeps `tolerance_pct` and per-TF `size_multipliers`, runs each combination
via the in-memory engine, and prints a comparison table. Output includes
per-TF and aggregate metrics: total P&L, win rate, max DD, chop buckets.

Usage:
    python scripts/run_tuning_matrix.py
    python scripts/run_tuning_matrix.py --data ./data/cache/btcusdt_perp_1m_2y.parquet
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
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
    avg_pnl_usd,
    chop_buckets,
    max_consecutive_losers,
    max_drawdown_pct,
    median_hold_minutes,
    pair_fills_by_tf,
    per_tf_summary,
    sharpe_from_returns,
    win_rate,
)
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots, fills, orders as orders_t
from lnmarkets_bot.strategy.ma_cross import MaCross


# Default sweep grid. Override via CLI flags.
DEFAULT_TOLERANCES = [0.001, 0.002, 0.003, 0.005, 0.008]
DEFAULT_SIZE_GRID = [
    {"1d": 1.0, "4h": 1.0},
    {"1d": 1.0, "4h": 0.5},
    {"1d": 0.75, "4h": 0.5},
    {"1d": 1.0, "4h": 0.25},
    {"1d": 0.5, "4h": 0.5},
    {"1d": 1.5, "4h": 0.5},
]


def _load_fills_and_equity(engine, run_id: int) -> tuple[list[FillRow], list[float]]:
    """Load fills (with trigger_tf) and equity curve for a given run."""
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
    *, cfg: BotConfig, parquet: Path, tolerance: float, sizes: dict[str, float],
    base_size: float, base_leverage: float,
) -> dict[str, Any]:
    ds = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h"),
    )
    strat = MaCross(params={
        "tolerance_pct": tolerance,
        "size_multipliers": sizes,
        "base_size_usd": base_size,
        "base_leverage": base_leverage,
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
        "tolerance_pct": tolerance,
        "size_multipliers": sizes,
        "summary": summary,
        "max_dd_pct": max_drawdown_pct(eq),
    }


def print_table(rows: list[dict]) -> None:
    cols = [
        ("tol", 5), ("1d_mult", 7), ("4h_mult", 7),
        ("1d_n", 5), ("1d_pnl", 9), ("1d_wr", 6), ("1d_avgwin", 9), ("1d_avgloss", 9),
        ("4h_n", 5), ("4h_pnl", 9), ("4h_wr", 6), ("4h_avgwin", 9), ("4h_avgloss", 9),
        ("agg_pnl", 8), ("max_dd%", 8), ("<1%cnt", 6), ("<2%cnt", 6),
    ]
    header = "  ".join(f"{n:<{w}}" for n, w in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        s = r["summary"]
        o1 = s["by_tf"].get("1d", {})
        o4 = s["by_tf"].get("4h", {})
        a = s.get("aggregate", {})
        # avg win / avg loss
        def win_loss(d):
            ws = [t for t in [] if False]
            # recompute from per_tf_summary which doesn't split winners/losers
            # Use total_pnl + win_rate as a proxy
            return (0.0, 0.0)
        # Use aggregate chop counts
        row = [
            (f"{r['tolerance_pct']:.3f}", 5),
            (f"{r['size_multipliers'].get('1d', 1):.2f}", 7),
            (f"{r['size_multipliers'].get('4h', 1):.2f}", 7),
            (o1.get("n_trades", 0), 5),
            (f"{o1.get('total_pnl_usd', 0):+.0f}", 9),
            (f"{o1.get('win_rate', 0):.0%}", 6),
            ("-", 9),
            ("-", 9),
            (o4.get("n_trades", 0), 5),
            (f"{o4.get('total_pnl_usd', 0):+.0f}", 9),
            (f"{o4.get('win_rate', 0):.0%}", 6),
            ("-", 9),
            ("-", 9),
            (f"{a.get('total_pnl_usd', 0):+.0f}", 8),
            (f"{r['max_dd_pct']:.1%}", 8),
            (a.get("chop_lt_1pct", {}).get("count", 0), 6),
            (a.get("chop_lt_2pct", {}).get("count", 0), 6),
        ]
        print("  ".join(f"{str(v):<{w}}" for v, w in row))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path,
                        default=Path("./data/cache/btcusdt_perp_1m_6m.parquet"))
    parser.add_argument("--base-size", type=float, default=1000.0)
    parser.add_argument("--base-leverage", type=float, default=2.0)
    parser.add_argument("--db-dir", type=Path, default=Path("./runs"))
    parser.add_argument("--tolerances", type=float, nargs="+", default=DEFAULT_TOLERANCES)
    parser.add_argument("--sizes", type=str, nargs="+", default=None,
                        help='JSON list of size_multipliers dicts, e.g. \'{"1d":1.0,"4h":0.5}\'')
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    configure_logging("WARNING")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    if args.sizes is not None:
        size_grid = [json.loads(s) for s in args.sizes]
    else:
        size_grid = DEFAULT_SIZE_GRID

    rows = []
    total = len(args.tolerances) * len(size_grid)
    n = 0
    for tol in args.tolerances:
        for sizes in size_grid:
            n += 1
            db_path = args.db_dir / f"tune_t{str(tol).replace('.', '_')}_{n}.sqlite"
            cfg = BotConfig(
                storage_db_path=db_path,
                strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
                initial_balance_usd=10_000.0,
                risk_max_position_usd=10_000.0,
                risk_max_leverage=10.0,
                risk_max_daily_loss_usd=1_000_000.0,
                risk_max_orders_per_minute=10_000,
            )
            print(f"[tune] {n}/{total} tol={tol} sizes={sizes} ...", flush=True)
            try:
                rows.append(asyncio.run(run_one(
                    cfg=cfg, parquet=args.data, tolerance=tol, sizes=sizes,
                    base_size=args.base_size, base_leverage=args.base_leverage,
                )))
            except Exception as exc:
                print(f"  FAILED: {exc}")

    print()
    print_table(rows)
    if args.json_out:
        args.json_out.write_text(json.dumps(rows, indent=2, default=str))
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()