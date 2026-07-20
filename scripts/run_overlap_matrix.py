"""Run the multi-TF MA-cross strategy against the 2y fixture under each of the
four overlap-rule candidates and present a comparison table.

Outputs both a human-readable table and a JSON dump.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

# Make `lnmarkets_bot` importable when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select

from lnmarkets_bot.config import BotConfig, load_config
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
                fills.c.ts,
                fills.c.order_id,
                fills.c.qty_sats,
                fills.c.price_usd,
                fills.c.fee_sats,
                orders_t.c.side,
            )
            .join(orders_t, fills.c.order_id == orders_t.c.id)
            .where(orders_t.c.run_id == run_id)
            .order_by(fills.c.ts.asc(), fills.c.order_id.asc())
        ).fetchall()
    return [
        FillRow(
            ts=r.ts,
            order_id=r.order_id,
            qty_sats=r.qty_sats,
            price_usd=r.price_usd,
            fee_sats=r.fee_sats,
            side=r.side,
        )
        for r in rows
    ]


def _load_account_snapshots(engine, run_id: int):
    fac = make_session_factory(engine)
    with fac() as s:
        rows = s.execute(
            select(
                account_snapshots.c.ts,
                account_snapshots.c.balance_sats,
                account_snapshots.c.equity_sats,
                account_snapshots.c.margin_used_sats,
                account_snapshots.c.unrealized_pnl_sats,
            )
            .where(account_snapshots.c.run_id == run_id)
            .order_by(account_snapshots.c.ts.asc())
        ).fetchall()
    return [(r.ts, r.balance_sats, r.equity_sats, r.margin_used_sats, r.unrealized_pnl_sats)
            for r in rows]


def _run_one(
    *, cfg: BotConfig, parquet_path: Path, overlap_rule: str, base_size: float, base_leverage: float,
) -> dict:
    ds = _build_data_source(parquet_path)
    strat = MaCross(
        params={
            "overlap_rule": overlap_rule,
            "base_size_usd": base_size,
            "base_leverage": base_leverage,
        }
    )
    run_id = asyncio.run(
        run_backtest(cfg=cfg, data_source=ds, strategy=strat, install_signal_handlers=False)
    )
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    fac = make_session_factory(engine)

    with fac() as s:
        n_orders = s.execute(
            select(__import__("sqlalchemy").func.count())
            .select_from(orders_t).where(orders_t.c.run_id == run_id)
        ).scalar()

    f_rows = _load_fills(engine, run_id)
    snaps = _load_account_snapshots(engine, run_id)
    trades = pair_fills_into_trades(f_rows)
    chop = chop_buckets(trades, notional_per_trade_usd=base_size)

    # Per-trade returns for sharpe
    rets = [t.pnl_usd / base_size for t in trades]
    equity_curve = [
        (sn[2] / 1e8) for sn in snaps
    ]  # equity_sats → USD
    return {
        "overlap_rule": overlap_rule,
        "run_id": run_id,
        "n_orders": int(n_orders or 0),
        "n_trades": len(trades),
        "total_pnl_usd": sum(t.pnl_usd for t in trades),
        "win_rate": win_rate(trades),
        "avg_pnl_usd": avg_pnl_usd(trades),
        "sharpe_per_trade_annualized": sharpe_from_returns(
            rets, periods_per_year=max(1, len(trades))
        ),
        "median_hold_minutes": median_hold_minutes(trades),
        "max_drawdown_pct": max_drawdown_pct(equity_curve),
        "exposure_pct": exposure_pct(snaps),
        "max_consecutive_losers": max_consecutive_losers(trades),
        "chop_lt_1pct": chop[0.01],
        "chop_lt_2pct": chop[0.02],
    }


def _print_table(rows: list[dict]) -> None:
    cols = [
        ("overlap_rule", 22),
        ("n_trades", 7),
        ("total_pnl_usd", 12),
        ("win_rate", 8),
        ("avg_pnl_usd", 10),
        ("max_dd%", 8),
        ("med_hold_min", 12),
        ("exp%", 6),
        ("max_consec_loss", 14),
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
            (r["overlap_rule"], 22),
            (r["n_trades"], 7),
            (f"{r['total_pnl_usd']:.2f}", 12),
            (f"{r['win_rate']:.2%}", 8),
            (f"{r['avg_pnl_usd']:.2f}", 10),
            (f"{r['max_drawdown_pct']:.2%}", 8),
            (f"{r['median_hold_minutes']:.0f}", 12),
            (f"{r['exposure_pct']:.2%}", 6),
            (r["max_consecutive_losers"], 14),
            (c1["count"], 7),
            (f"{c1['sum_pnl_usd']:.2f}", 10),
            (c2["count"], 7),
            (f"{c2['sum_pnl_usd']:.2f}", 10),
        ]
        print("  ".join(f"{str(v):<{w}}" for v, w in row))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("./data/cache/btcusdt_perp_1m_2y.parquet"),
    )
    parser.add_argument("--base-size", type=float, default=1000.0)
    parser.add_argument("--base-leverage", type=float, default=2.0)
    parser.add_argument("--db-dir", type=Path, default=Path("./runs"))
    parser.add_argument("--rules", nargs="+",
                        default=["none", "higher_tf_blocks_lower", "direction_bias", "age_decay"])
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    configure_logging("INFO")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for rule in args.rules:
        cfg = BotConfig(
            storage_db_path=args.db_dir / f"matrix_{rule}.sqlite",
            strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
            risk_max_position_usd=10000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,  # effectively disable for the matrix
            risk_max_orders_per_minute=10_000,
        )
        print(f"[matrix] running overlap_rule={rule} ...", flush=True)
        rows.append(
            _run_one(
                cfg=cfg, parquet_path=args.data, overlap_rule=rule,
                base_size=args.base_size, base_leverage=args.base_leverage,
            )
        )
        print(f"[matrix] done rule={rule} run_id={rows[-1]['run_id']}", flush=True)

    print()
    _print_table(rows)

    if args.json_out:
        args.json_out.write_text(json.dumps(rows, indent=2, default=str))
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()