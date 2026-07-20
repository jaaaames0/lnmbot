"""Metrics-only 2-year cooldown-mode sweep.

It uses the same MaCross, RiskGuard, and PaperFillExecutor path as a normal
backtest but intentionally does not persist the million 1m bars per run.
This makes the per-timeframe cooldown matrix practical without creating
hundreds of ~400 MB SQLite databases.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.metrics import (
    FillRow,
    max_drawdown_pct,
    pair_fills_by_tf,
    per_tf_summary,
)
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config as limits_from_config
from lnmarkets_bot.strategy import StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross


@dataclass
class MetricsRecorder:
    """Minimal Recorder compatible with RiskGuard and PaperFillExecutor."""

    orders: dict[int, tuple[str, str]] = field(default_factory=dict)
    fills: list[FillRow] = field(default_factory=list)
    equity_points: list[tuple[Any, float]] = field(default_factory=list)
    _next_signal_id: int = 1
    _next_order_id: int = 1

    def record_signal(self, run_id: int, **_: Any) -> int:
        signal_id = self._next_signal_id
        self._next_signal_id += 1
        return signal_id

    def record_order(
        self, run_id: int, *, side: str, trigger_tf: str = "", **_: Any,
    ) -> int:
        order_id = self._next_order_id
        self._next_order_id += 1
        self.orders[order_id] = (side, trigger_tf)
        return order_id

    def record_fill(
        self, order_id: int, *, ts, qty_sats: int, price_usd: float, fee_sats: int,
    ) -> int:
        side, trigger_tf = self.orders[order_id]
        self.fills.append(FillRow(
            ts=ts, order_id=order_id, qty_sats=qty_sats, price_usd=price_usd,
            fee_sats=fee_sats, side=side, trigger_tf=trigger_tf,
        ))
        return len(self.fills)

    def record_risk_event(self, run_id: int, **_: Any) -> None:
        return None

    def upsert_daily_pnl(self, run_id: int, **_: Any) -> None:
        return None


async def run_one(
    *, data: Path, mode: str, count_1d: int, count_4h: int,
    loss_threshold_1d: float = 0.0, loss_count_1d: int = 0,
    loss_threshold_4h: float = 0.0, loss_count_4h: int = 0,
    include_trades: bool = False,
) -> dict[str, Any]:
    cfg = BotConfig(
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=10.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=10_000,
    )
    recorder = MetricsRecorder()
    executor = PaperFillExecutor(recorder=recorder, run_id=1)
    guard = RiskGuard(limits=limits_from_config(cfg), recorder=recorder, executor=executor)
    strategy = MaCross(params={
        "tfs": ("1d", "4h"),
        "tolerance_pct": 0.005,
        "base_size_usd": 1000.0,
        "base_leverage": 2.0,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "same_bar_flip": True,
        "warmup_bars_per_tf": 21,
        "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
        "cooldown_signal_count": {"1d": count_1d, "4h": count_4h},
        "cooldown_mode": mode,
        "loss_cooldown_threshold_pct": {
            "1d": loss_threshold_1d, "4h": loss_threshold_4h,
        },
        "loss_cooldown_signal_count": {"1d": loss_count_1d, "4h": loss_count_4h},
    })
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    for tf in strategy.tfs:
        state.positions[tf] = TfPosition()
    strategy.on_startup(state)
    source = MultiTimeframeDataSource(
        BacktestReplay(data, cadence="instant"), higher_timeframes=("1d", "4h"),
    )

    async for bar in source.stream():
        executor.update_price(bar.close)
        guard.current_price_usd = bar.close
        for intent in intents_to_list(strategy.on_bar(bar, state)):
            signal_id = recorder.record_signal(1)
            decision = await guard.submit(intent=intent, signal_id=signal_id, run_id=1, ts=bar.ts)
            if decision.order_id is not None and decision.order_id > 0:
                guard.record_realized_pnl(executor.total_realized_pnl_usd(), bar.ts)

        total_qty_sats = 0
        for tf, pos in state.positions.items():
            pos.qty_sats = executor.position_qty_sats(tf)
            pos.entry_price_usd = executor.position_entry_price(tf)
            exec_pos = executor.positions.get(tf)
            if exec_pos is not None:
                pos.leverage = exec_pos.leverage
            total_qty_sats += pos.qty_sats
        state.equity_sats = int(state.balance_sats + total_qty_sats * bar.close)
        if bar.timeframe == "1m":
            recorder.equity_points.append((bar.ts, state.equity_sats / 1e8))

    by_tf_trades = pair_fills_by_tf(recorder.fills)
    result: dict[str, Any] = {
        "mode": mode,
        "count_1d": count_1d,
        "count_4h": count_4h,
        "summary": per_tf_summary(by_tf_trades, notional_per_trade_usd=1000.0),
        "max_dd_pct": max_drawdown_pct([value for _, value in recorder.equity_points]),
    }
    if include_trades:
        result["trades_by_tf"] = by_tf_trades
        result["equity_points"] = recorder.equity_points
    return result


def _pnl(result: dict[str, Any], tf: str) -> float:
    return float(result["summary"]["by_tf"].get(tf, {}).get("total_pnl_usd", 0.0))


async def sweep(data: Path, counts: list[int], modes: list[str], top_n: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for mode in modes:
        print(f"\n[{mode}] sweeping 1d and 4h counts independently", flush=True)
        one_d = {0: await run_one(data=data, mode=mode, count_1d=0, count_4h=0)}
        four_h = {0: one_d[0]}
        for count in counts:
            if count == 0:
                continue
            print(f"  1d count={count}", flush=True)
            one_d[count] = await run_one(data=data, mode=mode, count_1d=count, count_4h=0)
            print(f"  4h count={count}", flush=True)
            four_h[count] = await run_one(data=data, mode=mode, count_1d=0, count_4h=count)

        candidates = []
        for count_1d in counts:
            for count_4h in counts:
                candidates.append({
                    "mode": mode,
                    "count_1d": count_1d,
                    "count_4h": count_4h,
                    # Per-TF trading is independent, so this sum is exact.
                    "aggregate_pnl_usd": _pnl(one_d[count_1d], "1d") + _pnl(four_h[count_4h], "4h"),
                })
        candidates.sort(key=lambda row: row["aggregate_pnl_usd"], reverse=True)

        print(f"  validating drawdown for the top {top_n} combinations", flush=True)
        for candidate in candidates[:top_n]:
            direct = await run_one(
                data=data, mode=mode,
                count_1d=candidate["count_1d"], count_4h=candidate["count_4h"],
            )
            candidate["max_dd_pct"] = direct["max_dd_pct"]
            candidate["summary"] = direct["summary"]
            results.append(candidate)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument("--counts", type=int, nargs="+", default=list(range(0, 13)))
    parser.add_argument(
        "--modes", nargs="+",
        default=["verdict_transition", "directional_transition", "order_opportunity"],
    )
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--json-out", type=Path, default=Path("runs/cooldown_mode_matrix_2y.json"))
    args = parser.parse_args()
    rows = asyncio.run(sweep(args.data, args.counts, args.modes, args.top_n))
    rows.sort(key=lambda row: row["aggregate_pnl_usd"], reverse=True)
    args.json_out.write_text(json.dumps(rows, indent=2, default=str) + "\n")
    print("\nmode                 1d  4h  P&L       max DD")
    for row in rows:
        print(
            f"{row['mode']:<20} {row['count_1d']:>2}  {row['count_4h']:>2}  "
            f"${row['aggregate_pnl_usd']:>+8.0f}  {row['max_dd_pct']:.1%}"
        )
    print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()
