"""Rebuild the canonical v1.3 two-year verification database.

This is intentionally separate from the tuning sweeps: it runs exactly the
locked v1.3 parameters used by the signal-audit report.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data import BacktestReplay, MultiTimeframeDataSource
from lnmarkets_bot.engine.inmemory import run_inmemory
from lnmarkets_bot.logging import configure_logging
from lnmarkets_bot.strategy.ma_cross import MaCross


async def run(data: Path, db: Path) -> int:
    cfg = BotConfig(
        storage_db_path=db,
        strategy="lnmarkets_bot.strategy.ma_cross:MaCross",
        initial_balance_usd=10_000.0,
        risk_max_position_usd=10_000.0,
        risk_max_leverage=10.0,
        risk_max_daily_loss_usd=1_000_000.0,
        risk_max_orders_per_minute=10_000,
    )
    strategy = MaCross(params={
        "tfs": ("1d", "4h"),
        "tolerance_pct": 0.005,
        "base_size_usd": 1000.0,
        "base_leverage": 2.0,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "same_bar_flip": True,
        "warmup_bars_per_tf": 21,
        "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
        "cooldown_signal_count": {"1d": 12, "4h": 11},
        "loss_cooldown_threshold_pct": {"1d": 0.05, "4h": 0.02},
        "loss_cooldown_signal_count": {"1d": 3, "4h": 4},
    })
    data_source = MultiTimeframeDataSource(
        BacktestReplay(data, cadence="instant"), higher_timeframes=("1d", "4h"),
    )
    return await run_inmemory(
        cfg=cfg, data_source=data_source, strategy=strategy,
        install_signal_handlers=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/cache/btcusdt_perp_1m_2y.parquet"))
    parser.add_argument("--db", type=Path, default=Path("runs/v13_verify.sqlite"))
    args = parser.parse_args()
    configure_logging("WARNING")
    run_id = asyncio.run(run(args.data, args.db))
    print(f"v1.3 verification complete: run_id={run_id} db={args.db}")


if __name__ == "__main__":
    main()
