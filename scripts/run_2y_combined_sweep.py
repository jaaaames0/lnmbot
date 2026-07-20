"""2-year combined sweep: tolerance × per-TF cool-off.

Three-stage sweep:
  1. Sweep tolerance_pct alone (with v1.2 cool-off defaults) — find best tolerance.
  2. With best tolerance, sweep 1d cool-off (threshold × count).
  3. With best tolerance + best 1d cool-off, sweep 4h cool-off.

Each stage is small enough to run in a few minutes on the 2y fixture.
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
    FillRow, max_drawdown_pct, pair_fills_by_tf, per_tf_summary,
)
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import account_snapshots, fills, orders as orders_t
from lnmarkets_bot.strategy.ma_cross import MaCross


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
    tolerance: float,
    thr_1d: float, cnt_1d: int, thr_4h: float, cnt_4h: int,
    base_size: float, base_leverage: float,
) -> dict[str, Any]:
    ds = MultiTimeframeDataSource(
        BacktestReplay(parquet, cadence="instant"),
        higher_timeframes=("1d", "4h"),
    )
    strat = MaCross(params={
        "tolerance_pct": tolerance,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "base_size_usd": base_size,
        "base_leverage": base_leverage,
        "cooldown_threshold_pct": {"1d": thr_1d, "4h": thr_4h},
        "cooldown_signal_count": {"1d": cnt_1d, "4h": cnt_4h},
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
        "tolerance": tolerance,
        "thr_1d": thr_1d, "cnt_1d": cnt_1d,
        "thr_4h": thr_4h, "cnt_4h": cnt_4h,
        "summary": summary,
        "max_dd_pct": max_drawdown_pct(eq),
    }


def _agg_pnl(r: dict) -> float:
    return r["summary"].get("aggregate", {}).get("total_pnl_usd", 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path,
                        default=Path("./data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument("--base-size", type=float, default=1000.0)
    parser.add_argument("--base-leverage", type=float, default=2.0)
    parser.add_argument("--db-dir", type=Path, default=Path("./runs"))
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    configure_logging("WARNING")
    args.db_dir.mkdir(parents=True, exist_ok=True)

    def make_cfg(name: str) -> BotConfig:
        return BotConfig(
            storage_db_path=args.db_dir / f"v13_{name}.sqlite",
            strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
            initial_balance_usd=10_000.0,
            risk_max_position_usd=10_000.0,
            risk_max_leverage=10.0,
            risk_max_daily_loss_usd=1_000_000.0,
            risk_max_orders_per_minute=10_000,
        )

    # Stage 1: tolerance sweep with v1.2 cool-off defaults
    print("\n=== STAGE 1: TOLERANCE SWEEP (cool-off defaults: 1d 5%/6, 4h 5%/4) ===")
    tolerances = [0.0005, 0.001, 0.002, 0.003, 0.005, 0.008, 0.01]
    stage1 = []
    for i, tol in enumerate(tolerances, 1):
        print(f"  [{i}/{len(tolerances)}] tolerance={tol} ...", flush=True)
        r = asyncio.run(run_one(
            cfg=make_cfg(f"tol_{i}"), parquet=args.data, tolerance=tol,
            thr_1d=0.05, cnt_1d=6, thr_4h=0.05, cnt_4h=4,
            base_size=args.base_size, base_leverage=args.base_leverage,
        ))
        r["stage"] = "tol"
        stage1.append(r)
    stage1.sort(key=_agg_pnl, reverse=True)
    best_tol = stage1[0]["tolerance"]
    print(f"\n  best tolerance: {best_tol} (agg P&L = ${_agg_pnl(stage1[0]):+.0f})\n")
    for r in stage1:
        o1 = r["summary"]["by_tf"].get("1d", {})
        o4 = r["summary"]["by_tf"].get("4h", {})
        a = r["summary"].get("aggregate", {})
        print(f"  tol={r['tolerance']:<8} 1d={o1.get('total_pnl_usd', 0):+.0f}({o1.get('n_trades', 0)}t) "
              f"4h={o4.get('total_pnl_usd', 0):+.0f}({o4.get('n_trades', 0)}t) "
              f"agg={a.get('total_pnl_usd', 0):+.0f} DD={r['max_dd_pct']:.1%}")

    # Stage 2: 1d cool-off sweep with best tolerance
    print(f"\n=== STAGE 2: 1D COOL-OFF (tol={best_tol}, 4h defaults 5%/4) ===")
    one_d_grid = [
        (0.03, 2), (0.03, 4), (0.03, 6), (0.05, 2), (0.05, 4), (0.05, 6), (0.08, 2), (0.08, 4), (0.08, 6),
    ]
    stage2 = []
    for i, (thr, cnt) in enumerate(one_d_grid, 1):
        print(f"  [{i}/{len(one_d_grid)}] 1d:({thr},{cnt}) ...", flush=True)
        r = asyncio.run(run_one(
            cfg=make_cfg(f"1d_{i}"), parquet=args.data, tolerance=best_tol,
            thr_1d=thr, cnt_1d=cnt, thr_4h=0.05, cnt_4h=4,
            base_size=args.base_size, base_leverage=args.base_leverage,
        ))
        r["stage"] = "1d"
        stage2.append(r)
    stage2.sort(key=_agg_pnl, reverse=True)
    best_1d = (stage2[0]["thr_1d"], stage2[0]["cnt_1d"])
    print(f"\n  best 1d cool-off: threshold={best_1d[0]}, count={best_1d[1]} "
          f"(agg P&L = ${_agg_pnl(stage2[0]):+.0f})\n")
    for r in stage2:
        o1 = r["summary"]["by_tf"].get("1d", {})
        o4 = r["summary"]["by_tf"].get("4h", {})
        a = r["summary"].get("aggregate", {})
        print(f"  1d:({r['thr_1d']:.2%},{r['cnt_1d']})  1d={o1.get('total_pnl_usd', 0):+.0f}({o1.get('n_trades', 0)}t) "
              f"4h={o4.get('total_pnl_usd', 0):+.0f}({o4.get('n_trades', 0)}t) "
              f"agg={a.get('total_pnl_usd', 0):+.0f}")

    # Stage 3: 4h cool-off sweep with best tolerance + best 1d
    print(f"\n=== STAGE 3: 4H COOL-OFF (tol={best_tol}, 1d best {best_1d[0]:.2%}/{best_1d[1]}) ===")
    four_h_grid = [
        (0.02, 1), (0.02, 2), (0.02, 4), (0.03, 1), (0.03, 2), (0.03, 4), (0.05, 1), (0.05, 2), (0.05, 4),
    ]
    stage3 = []
    for i, (thr, cnt) in enumerate(four_h_grid, 1):
        print(f"  [{i}/{len(four_h_grid)}] 4h:({thr},{cnt}) ...", flush=True)
        r = asyncio.run(run_one(
            cfg=make_cfg(f"4h_{i}"), parquet=args.data, tolerance=best_tol,
            thr_1d=best_1d[0], cnt_1d=best_1d[1], thr_4h=thr, cnt_4h=cnt,
            base_size=args.base_size, base_leverage=args.base_leverage,
        ))
        r["stage"] = "4h"
        stage3.append(r)
    stage3.sort(key=_agg_pnl, reverse=True)
    best_4h = (stage3[0]["thr_4h"], stage3[0]["cnt_4h"])
    print(f"\n  best 4h cool-off: threshold={best_4h[0]}, count={best_4h[1]} "
          f"(agg P&L = ${_agg_pnl(stage3[0]):+.0f})\n")
    for r in stage3:
        o1 = r["summary"]["by_tf"].get("1d", {})
        o4 = r["summary"]["by_tf"].get("4h", {})
        a = r["summary"].get("aggregate", {})
        print(f"  4h:({r['thr_4h']:.2%},{r['cnt_4h']})  1d={o1.get('total_pnl_usd', 0):+.0f}({o1.get('n_trades', 0)}t) "
              f"4h={o4.get('total_pnl_usd', 0):+.0f}({o4.get('n_trades', 0)}t) "
              f"agg={a.get('total_pnl_usd', 0):+.0f}")

    print(f"\n=== FINAL v1.3 RECOMMENDATION ===")
    print(f"  tolerance_pct = {best_tol}")
    print(f"  1d cool-off: threshold={best_1d[0]}, count={best_1d[1]}")
    print(f"  4h cool-off: threshold={best_4h[0]}, count={best_4h[1]}")
    print(f"  aggregate P&L: ${_agg_pnl(stage3[0]):+.0f}")

    if args.json_out:
        args.json_out.write_text(json.dumps(
            {"stage1": stage1, "stage2": stage2, "stage3": stage3,
             "best": {"tolerance": best_tol, "1d": best_1d, "4h": best_4h}},
            indent=2, default=str,
        ))
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()