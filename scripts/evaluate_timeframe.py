"""Evaluate one independent MA-cross timeframe on the two-year candle fixture.

This is an experimental research tool. It uses the same strategy, risk guard,
and paper executor as the production backtest path, with 10 bps charged on
each fill and 5 bps simulated slippage. The candle fixture has no historical
funding series, so funding is deliberately reported only as a holding-time
sensitivity rather than included in net P&L.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.metrics import FillRow, Trade, pair_fills_by_tf, per_tf_summary
from lnmarkets_bot.risk.guard import RiskGuard
from lnmarkets_bot.risk.limits import from_config
from lnmarkets_bot.strategy import Bar, StrategyState, intents_to_list
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross


@dataclass
class MetricsRecorder:
    orders: dict[int, tuple[str, str]] = field(default_factory=dict)
    fills: list[FillRow] = field(default_factory=list)
    _next_signal_id: int = 1
    _next_order_id: int = 1

    def record_signal(self, _run_id: int, **_: Any) -> int:
        signal_id = self._next_signal_id
        self._next_signal_id += 1
        return signal_id

    def record_order(self, _run_id: int, *, side: str, trigger_tf: str = "", **_: Any) -> int:
        order_id = self._next_order_id
        self._next_order_id += 1
        self.orders[order_id] = (side, trigger_tf)
        return order_id

    def record_fill(
        self, order_id: int, *, ts, qty_sats: int, price_usd: float, fee_sats: int
    ) -> int:
        side, trigger_tf = self.orders[order_id]
        self.fills.append(
            FillRow(
                ts=ts,
                order_id=order_id,
                qty_sats=qty_sats,
                price_usd=price_usd,
                fee_sats=fee_sats,
                side=side,
                trigger_tf=trigger_tf,
            )
        )
        return len(self.fills)

    def record_risk_event(self, _run_id: int, **_: Any) -> None:
        return None

    def upsert_daily_pnl(self, _run_id: int, **_: Any) -> None:
        return None


def _summary(trades: list[Trade], *, timeframe: str, notional_usd: float) -> dict[str, Any]:
    return per_tf_summary({timeframe: trades}, notional_per_trade_usd=notional_usd)["by_tf"].get(
        timeframe, {}
    )


def _resampled_bars(data: Path, timeframe: str, *, input_timeframe: str | None = None) -> list[Bar]:
    """Derive closed higher-timeframe bars without iterating every source 1m bar.

    This matches ``MultiTimeframeDataSource``'s right-labelled, left-closed
    aggregation. A single-timeframe strategy ignores the intervening 1m bars,
    so omitting them makes large parameter sweeps practical without changing
    signals or fills.
    """
    frequencies = {
        "5m": "5min",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "6h": "6h",
        "8h": "8h",
        "12h": "12h",
        "1d": "1D",
        "1w": "W-MON",
    }
    durations = {
        "5m": pd.Timedelta(minutes=5),
        "1h": pd.Timedelta(hours=1),
        "2h": pd.Timedelta(hours=2),
        "4h": pd.Timedelta(hours=4),
        "6h": pd.Timedelta(hours=6),
        "8h": pd.Timedelta(hours=8),
        "12h": pd.Timedelta(hours=12),
        "1d": pd.Timedelta(days=1),
        "1w": pd.Timedelta(weeks=1),
    }
    if timeframe not in frequencies:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    df = pd.read_parquet(data).sort_values("ts")
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize("UTC")
    else:
        df["ts"] = df["ts"].dt.tz_convert("UTC")
    if input_timeframe == timeframe:
        # Binance timestamps native candles at their open.  The live source
        # hands the strategy a completed bar at the close, so shift by one
        # interval to preserve the strategy's event-time semantics.
        return [
            Bar(
                ts=(row.ts + durations[timeframe]).to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                timeframe=timeframe,
            )
            for row in df.itertuples(index=False)
        ]
    last_source_ts = df["ts"].iloc[-1]
    agg = (
        df.set_index("ts")
        .resample(frequencies[timeframe], label="right", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
    )
    # The normal multi-timeframe source emits a derived bar only once a source
    # bar exists at that right-edge timestamp; exclude the final incomplete
    # bucket for exact replay semantics.
    agg = agg[agg.index <= last_source_ts]
    return [
        Bar(
            ts=ts.to_pydatetime(),
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
            timeframe=timeframe,
        )
        for ts, row in agg.iterrows()
    ]


async def evaluate(
    *,
    data: Path,
    timeframe: str,
    tolerance: float,
    winner_threshold: float,
    winner_count: int,
    loss_threshold: float,
    loss_count: int,
    notional_usd: float,
    leverage: float,
    input_timeframe: str | None = None,
    end: datetime | None = None,
    bars: list[Bar] | None = None,
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
    guard = RiskGuard(limits=from_config(cfg), recorder=recorder, executor=executor)
    strategy = MaCross(
        params={
            "tfs": (timeframe,),
            "tolerance_pct": tolerance,
            "base_size_usd": notional_usd,
            "base_leverage": leverage,
            "size_multipliers": {timeframe: 1.0},
            "same_bar_flip": True,
            "warmup_bars_per_tf": 21,
            "cooldown_threshold_pct": {timeframe: winner_threshold},
            "cooldown_signal_count": {timeframe: winner_count},
            "loss_cooldown_threshold_pct": {timeframe: loss_threshold},
            "loss_cooldown_signal_count": {timeframe: loss_count},
            "cooldown_mode": "verdict_transition",
        }
    )
    state = StrategyState(balance_sats=int(cfg.initial_balance_usd * 1e8))
    state.positions[timeframe] = TfPosition()
    strategy.on_startup(state)
    source_bars = (
        bars
        if bars is not None
        else _resampled_bars(data, timeframe, input_timeframe=input_timeframe)
    )
    for bar in source_bars:
        if end is not None and bar.ts > end:
            break
        executor.update_price(bar.close)
        guard.current_price_usd = bar.close
        for intent in intents_to_list(strategy.on_bar(bar, state)):
            signal_id = recorder.record_signal(1)
            decision = await guard.submit(intent=intent, signal_id=signal_id, run_id=1, ts=bar.ts)
            if decision.order_id is not None and decision.order_id > 0:
                guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
        pos = state.position(timeframe)
        pos.side = executor.position_side(timeframe)
        pos.qty_sats = executor.position_qty_sats(timeframe)
        pos.entry_price_usd = executor.position_entry_price(timeframe)

    trades = pair_fills_by_tf(recorder.fills).get(timeframe, [])
    split = datetime(2025, 7, 11, tzinfo=UTC)
    train = [trade for trade in trades if trade.exit_ts < split]
    holdout = [trade for trade in trades if trade.exit_ts >= split]
    fee_usd = sum(fill.fee_sats * fill.price_usd / 1e8 for fill in recorder.fills)
    total_hold_hours = sum(
        (trade.exit_ts - trade.entry_ts).total_seconds() / 3600.0 for trade in trades
    )
    # One basis point per eight-hour settlement, assuming every trade pays.
    # This is a worst-side sensitivity, not a claim about realised funding.
    funding_cost_1bp_8h_usd = notional_usd * total_hold_hours / 8.0 * 0.0001
    return {
        "parameters": {
            "timeframe": timeframe,
            "input_timeframe": input_timeframe or "1m",
            "tolerance_pct": tolerance,
            "winner_cooldown": {"threshold_pct": winner_threshold, "count": winner_count},
            "loss_cooldown": {"threshold_pct": loss_threshold, "count": loss_count},
            "notional_usd": notional_usd,
            "leverage": leverage,
            "fees": "10 bps per fill",
            "slippage": "5 bps per fill",
            "funding": "not included; fixture contains candles only",
        },
        "summary": _summary(trades, timeframe=timeframe, notional_usd=notional_usd),
        "train_summary": _summary(train, timeframe=timeframe, notional_usd=notional_usd),
        "holdout_summary": _summary(holdout, timeframe=timeframe, notional_usd=notional_usd),
        "costs": {
            "simulated_trading_fees_usd": fee_usd,
            "total_closed_trade_hold_hours": total_hold_hours,
            "funding_cost_usd_at_1bp_per_8h_if_always_paying": funding_cost_1bp_8h_usd,
        },
        "open_position_at_end": asdict(executor.positions.get(timeframe))
        if timeframe in executor.positions
        else None,
        # Kept in memory for multi-window research scripts.  The CLI removes
        # it before serialising a report.
        "_trades": trades,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument("--timeframe", default="1h")
    parser.add_argument(
        "--input-timeframe",
        default=None,
        help="Candle timeframe already stored in --data; omit for 1m input to resample.",
    )
    parser.add_argument("--tolerance", type=float, default=0.0)
    parser.add_argument("--winner-threshold", type=float, default=0.0)
    parser.add_argument("--winner-count", type=int, default=0)
    parser.add_argument("--loss-threshold", type=float, default=0.0)
    parser.add_argument("--loss-count", type=int, default=0)
    parser.add_argument("--notional", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=2.0)
    parser.add_argument("--json-out", type=Path, default=Path("runs/1h_baseline_2y.json"))
    args = parser.parse_args()
    result = asyncio.run(
        evaluate(
            data=args.data,
            timeframe=args.timeframe,
            tolerance=args.tolerance,
            winner_threshold=args.winner_threshold,
            winner_count=args.winner_count,
            loss_threshold=args.loss_threshold,
            loss_count=args.loss_count,
            notional_usd=args.notional,
            leverage=args.leverage,
            input_timeframe=args.input_timeframe,
        )
    )
    report = {key: value for key, value in result.items() if key != "_trades"}
    args.json_out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    summary = result["summary"]
    costs = result["costs"]
    print(
        f"{args.timeframe} tolerance={args.tolerance:.3%}: "
        f"{summary.get('n_trades', 0)} closed trades, "
        f"${summary.get('total_pnl_usd', 0.0):+.2f} after simulated fees/slippage"
    )
    print(
        f"simulated trading fees=${costs['simulated_trading_fees_usd']:.2f}; "
        f"hold={costs['total_closed_trade_hold_hours']:.1f}h; "
        f"1bp/8h funding sensitivity=${costs['funding_cost_usd_at_1bp_per_8h_if_always_paying']:.2f}"
    )
    print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
