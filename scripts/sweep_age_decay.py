"""Sweep age_decay_days ∈ {3, 7, 14, 30} for tfs=("1d","4h") with overlap_rule="age_decay".

Identical setup to run_overlap_matrix.py — used for tuning the rule after the
matrix has identified the winning overlap_rule. Reuses the same metrics layer.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.backtest import run_backtest
from lnmarkets_bot.logging import configure_logging
from lnmarkets_bot.metrics import (
    FillRow,
    avg_pnl_usd,
    chop_buckets,
    exposure_pct,
    max_consecutive_losers,
    max_drawdown_pct,
    median_hold_minutes,
    pair_fills_into_trades,
    sharpe_from_returns,
    win_rate,
)
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import (
    account_snapshots,
    fills,
    orders as orders_t,
    runs,
)
from lnmarkets_bot.strategy.ma_cross import MaCross


def _build_data_source(parquet_path: Path):
    base = BacktestReplay(parquet_path, cadence="instant")
    return MultiTimeframeDataSource(base, higher_timeframes=("1d", "4h"))


def _load_fills(engine, run_id: int) -> list[FillRow]:
    fac = make_session_factory(engine)
    with fac() as s:
        rows = s.execute(
            select(
                fills.c.ts, fills.c.order_id, fills.c.qty_sats,
                fills.c.price_usd, fills.c.fee_sats, orders_t.c.side,
            )
            .join(orders_t, fills.c.order_id == orders_t.c.id)
            .where(orders_t.c.run_id == run_id)
            .order_by(fills.c.ts.asc(), fills.c.order_id.asc())
        ).fetchall()
    return [
        FillRow(ts=r.ts, order_id=r.order_id, qty_sats=r.qty_sats,
                price_usd=r.price_usd, fee_sats=r.fee_sats, side=r.side)
        for r in rows
    ]


def _load_equity(engine, run_id: int):
    fac = make_session_factory(engine)
    with fac() as s:
        rows = s.execute(
            select(account_snapshots.c.equity_sats)
            .where(account_snapshots.c.run_id == run_id)
            .order_by(account_snapshots.c.ts.asc())
        ).fetchall()
    return [r[0] / 1e8 for r in rows]


def run_one(*, cfg: BotConfig, parquet_path: Path, age_decay_days: float, base_size: float) -> dict:
    ds = _build_data_source(parquet_path)
    strat = MaCross(
        params={
            "overlap_rule": "age_decay",
            "age_decay_days": age_decay_days,
            "base_size_usd": base_size,
            "base_leverage": 2.0,
        }
    )
    run_id = asyncio.run(
        run_backtest(cfg=cfg, data_source=ds, strategy=strat, install_signal_handlers=False)
    )
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    f_rows = _load_fills(engine, run_id)
    eq = _load_equity(engine, run_id)
    trades = pair_fills_into_trades(f_rows)
    chop = chop_buckets(trades, notional_per_trade_usd=base_size)
    return {
        "age_decay_days": age_decay_days,
        "run_id": run_id,
        "n_orders": len(f_rows),
        "n_trades": len(trades),
        "total_pnl_usd": sum(t.pnl_usd for t in trades),
        "win_rate": win_rate(trades),
        "avg_pnl_usd": avg_pnl_usd(trades),
        "median_hold_minutes": median_hold_minutes(trades),
        "max_drawdown_pct": max_drawdown_pct(eq),
        "max_consecutive_losers": max_consecutive_losers(trades),
        "chop_lt_1pct": chop[0.01],
        "chop_lt_2pct": chop[0.02],
    }


def print_table(rows: list[dict]) -> None:
    cols = [
        ("age_decay_days", 14),
        ("n_trades", 7),
        ("total_pnl_usd", 12),
        ("win_rate", 8),
        ("avg_pnl_usd", 10),
        ("max_dd%", 8),
        ("med_hold_hr", 11),
        ("<1%cnt", 7),
        ("<1%pnl", 10),
        ("<2%cnt", 7),
        ("<2%pnl", 10),
    ]
    header = "  ".join(f"{name:<{w}}" for name, w in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        c1 = r["chop_lt_1pct"]
        c2 = r["chop_lt_2pct"]
        row = [
            (r["age_decay_days"], 14),
            (r["n_trades"], 7),
            (f"{r['total_pnl_usd']:.2f}", 12),
            (f"{r['win_rate']:.2%}", 8),
            (f"{r['avg_pnl_usd']:.2f}", 10),
            (f"{r['max_drawdown_pct']:.2%}", 8),
            (f"{r['median_hold_minutes']/60:.0f}", 11),
            (c1["count"], 7),
            (f"{c1['sum_pnl_usd']:.2f}", 10),
            (c2["count"], 7),
            (f"{c2['sum_pnl_usd']:.2f}", 10),
        ]
        print("  ".join(f"{str(v):<{w}}" for v, w in row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path,
                        default=Path("./data/cache/btcusdt_perp_1m_6m.parquet"))
    parser.add_argument("--base-size", type=float, default=1000.0)
    parser.add_argument("--db-dir", type=Path, default=Path("./runs"))
    parser.add_argument("--days", type=float, nargs="+", default=[3, 7, 14, 30])
    args = parser.parse_args()

    configure_logging("INFO")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for days in args.days:
        cfg = BotConfig(
            storage_db_path=args.db_dir / f"sweep_ad_{int(days)}.sqlite",
            strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
            risk_max_position_usd=10000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )
        print(f"[sweep] age_decay_days={days} ...", flush=True)
        rows.append(run_one(
            cfg=cfg, parquet_path=args.data,
            age_decay_days=days, base_size=args.base_size,
        ))
        print(f"[sweep] done days={days} run_id={rows[-1]['run_id']}", flush=True)

    print()
    print_table(rows)


if __name__ == "__main__":
    main()