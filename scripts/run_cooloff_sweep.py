"""Cool-off heuristic sweep.

After a per-TF trade closes with P&L >= threshold, suppress the next
`cooldown_signal_count` transitions on that TF. Sweeps over thresholds
and counts, comparing against the base (cooldown disabled) and across
even vs odd N.

The user's intuition: the first MA-cross after a big move rarely produces
a real reversal — usually there's continuation or settling first.
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


# Default sweep grids
DEFAULT_THRESHOLDS = [0.03, 0.05, 0.08]
DEFAULT_COUNTS = [1, 2, 3, 4, 6]


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
    *, cfg: BotConfig, parquet: Path, threshold: float, count: int,
    base_size: float, base_leverage: float,
) -> dict[str, Any]:
    ds = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h"),
    )
    strat = MaCross(params={
        "tolerance_pct": 0.003,  # base default
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "base_size_usd": base_size,
        "base_leverage": base_leverage,
        "cooldown_threshold_pct": threshold,
        "cooldown_signal_count": count,
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
        "threshold_pct": threshold,
        "signal_count": count,
        "summary": summary,
        "max_dd_pct": max_drawdown_pct(eq),
    }


def print_table(rows: list[dict]) -> None:
    cols = [
        ("thr", 6), ("n", 3), ("odd?", 4),
        ("1d_n", 5), ("1d_pnl", 9), ("1d_<1%", 7), ("1d_<2%", 7),
        ("4h_n", 5), ("4h_pnl", 9), ("4h_<1%", 7), ("4h_<2%", 7),
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
            (f"{r['threshold_pct']:.2%}", 6),
            (r["signal_count"], 3),
            ("Y" if r["signal_count"] % 2 == 1 else "N", 4),
            (o1.get("n_trades", 0), 5),
            (f"{o1.get('total_pnl_usd', 0):+.0f}", 9),
            (o1.get("chop_lt_1pct", {}).get("count", 0), 7),
            (o1.get("chop_lt_2pct", {}).get("count", 0), 7),
            (o4.get("n_trades", 0), 5),
            (f"{o4.get('total_pnl_usd', 0):+.0f}", 9),
            (o4.get("chop_lt_1pct", {}).get("count", 0), 7),
            (o4.get("chop_lt_2pct", {}).get("count", 0), 7),
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
    parser.add_argument("--thresholds", type=float, nargs="+", default=DEFAULT_THRESHOLDS)
    parser.add_argument("--counts", type=int, nargs="+", default=DEFAULT_COUNTS)
    parser.add_argument("--include-base", action="store_true", default=True,
                        help="Run the base (cooldown disabled) for reference")
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    configure_logging("WARNING")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    if args.include_base:
        cfg = BotConfig(
            storage_db_path=args.db_dir / "cooloff_base.sqlite",
            strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
            initial_balance_usd=10_000.0,
            risk_max_position_usd=10_000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )
        print("[cooloff] base (no cooldown) ...", flush=True)
        rows.append({
            "threshold_pct": 0.0,
            "signal_count": 0,
            "summary": per_tf_summary(
                pair_fills_by_tf(_load_fills_and_equity(
                    make_engine(cfg.storage_db_path), cfg.storage_db_path
                )[0] if False else _load_fills_and_equity(
                    init_schema(make_engine(cfg.storage_db_path)) or make_engine(cfg.storage_db_path), 0
                )[0]),
                notional_per_trade_usd=args.base_size,
            ) if False else None,  # placeholder; real base run below
            "max_dd_pct": 0.0,
        })
        # Actually run the base
        cfg = BotConfig(
            storage_db_path=args.db_dir / "cooloff_base.sqlite",
            strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
            initial_balance_usd=10_000.0,
            risk_max_position_usd=10_000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )
        rows[-1] = asyncio.run(run_one(
            cfg=cfg, parquet=args.data, threshold=0.0, count=0,
            base_size=args.base_size, base_leverage=args.base_leverage,
        ))

    total = len(args.thresholds) * len(args.counts)
    n = 0
    for thr in args.thresholds:
        for cnt in args.counts:
            n += 1
            db_path = args.db_dir / f"cooloff_t{str(thr).replace('.','')}_c{cnt}.sqlite"
            cfg = BotConfig(
                storage_db_path=db_path,
                strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
                initial_balance_usd=10_000.0,
                risk_max_position_usd=10_000.0,
                risk_max_leverage=10.0,
                risk_max_daily_loss_usd=1_000_000.0,
                risk_max_orders_per_minute=10_000,
            )
            print(f"[cooloff] {n}/{total} thr={thr} count={cnt} ...", flush=True)
            rows.append(asyncio.run(run_one(
                cfg=cfg, parquet=args.data, threshold=thr, count=cnt,
                base_size=args.base_size, base_leverage=args.base_leverage,
            )))

    # Sort by aggregate P&L descending
    rows.sort(key=lambda r: r["summary"].get("aggregate", {}).get("total_pnl_usd", 0), reverse=True)

    print()
    print_table(rows)
    if args.json_out:
        args.json_out.write_text(json.dumps(rows, indent=2, default=str))
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()