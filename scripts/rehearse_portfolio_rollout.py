"""Read-only preflight for migrating the funded trader to portfolio routing.

The command only accepts a disposable database copy. It applies additive
schema migrations there, authenticates to read running isolated trades, and
proves that every live trade maps to the incumbent MA strategy namespace.
It never starts a strategy loop and has no order-submission call.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTradesApi
from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy.close_range_live import load_seed_machine
from lnmarkets_bot.strategy.ma_cross import MaCross

LIVE_DATABASE = Path("/var/lib/lnmbot/lnmarkets.sqlite")


async def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, required=True)
    parser.add_argument("--database-copy", type=Path, required=True)
    parser.add_argument("--seed-daily", type=Path, required=True)
    parser.add_argument("--seed-campaign", type=Path, required=True)
    args = parser.parse_args()

    database = args.database_copy.resolve()
    if database == LIVE_DATABASE or not database.is_file():
        parser.error("--database-copy must be an existing disposable database copy")

    cfg = BotConfig(_env_file=str(args.env))
    if not cfg.has_credentials():
        parser.error("the protected environment does not contain LNM credentials")

    engine = make_engine(database)
    init_schema(engine)
    recorder = Recorder(make_session_factory(engine))
    client = LnmRestClient(
        base_url=cfg.effective_base_url(),
        access_key=cfg.lnm_access_key,
        access_secret=cfg.lnm_access_secret,
        access_passphrase=cfg.lnm_access_passphrase,
        authed=True,
    )
    try:
        executor = LiveExecutor(
            trades_api=IsolatedTradesApi(client),
            recorder=recorder,
            run_id=-1,
            legacy_strategy_instance_id="ma_cross_primary",
        )
        await executor.reconcile()
    finally:
        await client.aclose()

    keys = sorted(key for key, position in executor.positions.items() if position.trade_id)
    if any(not key.startswith("ma_cross_primary:") for key in keys):
        raise RuntimeError("an existing live trade did not map to the MA strategy")

    ma = MaCross(
        params={
            "base_size_usd": cfg.sizing_fixed_notional_usd,
            "base_leverage": cfg.sizing_leverage,
            "chop_4h_reduce_enabled": cfg.strategy_4h_chop_reduce_enabled,
            "chop_lookback": cfg.strategy_chop_lookback,
            "chop_high_threshold": cfg.strategy_chop_high_threshold,
            "chop_high_size_multiplier": cfg.strategy_chop_high_size_multiplier,
        }
    )
    snapshot = recorder.latest_strategy_state(
        mode="live", strategy_name=f"{type(ma).__module__}.{type(ma).__name__}"
    )
    if snapshot is None or not ma.restore_persistent_state(snapshot["state"]):
        raise RuntimeError("the incumbent MA state snapshot is absent or incompatible")

    breakout = load_seed_machine(args.seed_daily, args.seed_campaign)
    campaign = breakout.campaign
    if campaign is None or campaign.origin != "historical":
        raise RuntimeError("the breakout seed does not preserve historical occupancy")

    print(
        json.dumps(
            {
                "schema_migration": "ok",
                "remote_reconciliation": "ok",
                "managed_open_trade_count": len(keys),
                "execution_keys": keys,
                "ma_snapshot": "compatible",
                "breakout_seed_last_completed_day": breakout.last_bar_ts.date().isoformat(),
                "breakout_historical_campaign": campaign.campaign_id,
                "breakout_lifetime_units": campaign.lifetime_units,
                "order_capability": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
