"""Live deployment runner.

Wires the full pipeline with real LNM API:
  LnmLiveStream (polling) → MultiTimeframeDataSource → MaCross
  → RiskGuard → LiveExecutor (real orders)

Reads config from environment (via BotConfig) and .env file. Catches
SIGTERM/SIGINT for clean shutdown. Suitable for optiplex systemd or
interactive use.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.api.account import AccountApi
from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTradesApi
from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data.live import LnmLiveStream
from lnmarkets_bot.data.multitimeframe import MultiTimeframeDataSource
from lnmarkets_bot.engine.live import run_paper
from lnmarkets_bot.engine.live_account import LiveAccountBalanceProvider
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.logging import configure_logging, get_logger
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.risk.guard import SizingPolicy
from lnmarkets_bot.strategy.ma_cross import MaCross


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=Path,
        default=Path(".env"),
        help="Path to .env file with LNM credentials (default: .env)",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=None,
        help="Stop after N seconds (default: run forever)",
    )
    parser.add_argument(
        "--allow-orders",
        action="store_true",
        help="Submit real isolated orders (default is observe-only paper execution)",
    )
    parser.add_argument(
        "--confirm-mainnet",
        action="store_true",
        help="Required in addition to --allow-orders when LNM_NETWORK=mainnet",
    )
    parser.add_argument(
        "--test-5m",
        action="store_true",
        help="Run the opt-in 5m execution-observation profile, never the production 1d/4h profile",
    )
    parser.add_argument(
        "--confirm-test-profile",
        action="store_true",
        help="Required with --test-5m --allow-orders; acknowledges it is not the locked strategy",
    )
    parser.add_argument(
        "--test-5m-cooldown-probe",
        action="store_true",
        help="With --test-5m, use tiny test-only cool-off thresholds and two suppressed transitions",
    )
    args = parser.parse_args()

    if args.test_5m_cooldown_probe and not args.test_5m:
        parser.error("--test-5m-cooldown-probe requires --test-5m")

    cfg = BotConfig(_env_file=str(args.env) if args.env.exists() else None)
    configure_logging(cfg.storage_log_level, cfg.storage_log_path)
    log = get_logger("live")
    log.info(
        "live.start",
        network=cfg.lnm_network.value,
        base_url=cfg.effective_base_url(),
        ws_url=cfg.effective_ws_url(),
        has_credentials=cfg.has_credentials(),
    )

    if args.allow_orders and not cfg.has_credentials():
        log.error("live.no_credentials", message="real orders require LNM API credentials")
        return 1
    if args.allow_orders and cfg.lnm_network.value == "mainnet" and not args.confirm_mainnet:
        log.error("live.mainnet_confirmation_required")
        return 1
    if args.test_5m and args.allow_orders and not args.confirm_test_profile:
        log.error("live.test_profile_confirmation_required")
        return 1

    # Initialize LNM REST client
    client = LnmRestClient(
        base_url=cfg.effective_base_url(),
        access_key=cfg.lnm_access_key,
        access_secret=cfg.lnm_access_secret,
        access_passphrase=cfg.lnm_access_passphrase,
        authed=cfg.has_credentials(),
    )
    # Each timeframe owns one isolated LNM trade. Do not use the legacy
    # cross-margin API here: it exposes one net position per symbol.
    trades_api = IsolatedTradesApi(client)

    # Initialize DB
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)

    # Build data source: LNM polling → MultiTimeframeDataSource
    base_stream = LnmLiveStream(client, symbol="BTCUSD", poll_seconds=10.0)
    # Strategy logic remains locked; only its explicit live sizing request is
    # supplied by configuration and then independently capped by RiskGuard.
    strategy_params = {
        "base_size_usd": cfg.sizing_fixed_notional_usd,
        "base_leverage": cfg.sizing_leverage,
        "chop_4h_reduce_enabled": cfg.strategy_4h_chop_reduce_enabled,
        "chop_lookback": cfg.strategy_chop_lookback,
        "chop_high_threshold": cfg.strategy_chop_high_threshold,
        "chop_high_size_multiplier": cfg.strategy_chop_high_size_multiplier,
    }
    if args.test_5m:
        # This is an execution-observation profile, not a validated strategy
        # variant. It keeps MA and verdict-transition logic unchanged, but maps
        # the active 4h cool-off parameters onto the faster 5m stream.  The
        # 0.5% production tolerance is intentionally too wide for 5m bars;
        # 0.01% is a signal-frequency probe, not a strategy optimisation.
        strategy_params.update(
            {
                "tfs": ("5m",),
                "tolerance_pct": 0.0001,
                "cooldown_threshold_pct": {"5m": 0.05},
                "cooldown_signal_count": {"5m": 11},
                "loss_cooldown_threshold_pct": {"5m": 0.02},
                "loss_cooldown_signal_count": {"5m": 4},
            }
        )
        if args.test_5m_cooldown_probe:
            # Exercise the production cool-off state machine promptly without
            # changing it: a non-negative close starts the winner cool-off;
            # virtually any negative close starts the loss cool-off.
            strategy_params.update(
                {
                    "cooldown_threshold_pct": {"5m": 0.0},
                    "cooldown_signal_count": {"5m": 2},
                    "loss_cooldown_threshold_pct": {"5m": 0.000001},
                    "loss_cooldown_signal_count": {"5m": 2},
                }
            )
    strat = MaCross(params=strategy_params)
    profile = (
        "test-5m-cooldown-probe"
        if args.test_5m_cooldown_probe
        else "test-5m"
        if args.test_5m
        else "production"
    )
    log.info("live.strategy_profile", profile=profile)
    ds = MultiTimeframeDataSource(base_stream, higher_timeframes=strat.tfs)

    # Wire LiveExecutor via factory. run_paper creates a Recorder against the
    # same database, so using this recorder keeps order writes on that DB.
    executor = None
    account_balance_provider = None
    if args.allow_orders:
        executor = LiveExecutor(
            trades_api=trades_api,
            recorder=recorder,
            run_id=-1,
            symbol="BTCUSD",
        )
        account_balance_provider = LiveAccountBalanceProvider(
            account_api=AccountApi(client), recorder=recorder
        )

    # Run
    try:
        if executor is not None:
            await executor.reconcile()
        run_id = await run_paper(
            cfg=cfg,
            data_source=ds,
            strategy=strat,
            duration_seconds=args.max_runtime,
            install_signal_handlers=True,
            executor_factory=(lambda: executor) if executor is not None else None,
            recorder_override=recorder if executor is not None else None,
            sizing_policy=SizingPolicy(
                mode=cfg.sizing_mode,
                total_margin_fraction=cfg.sizing_total_margin_fraction,
                timeframe_weights=cfg.sizing_timeframe_weights,
                equity_haircut=cfg.sizing_equity_haircut,
            ),
            account_balance_provider=account_balance_provider,
            run_mode="live" if executor is not None else "paper",
        )
        log.info("live.complete", run_id=run_id)
        return 0
    except Exception as exc:
        log.error("live.failed", error=str(exc), exc_info=True)
        return 1
    finally:
        await client.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
