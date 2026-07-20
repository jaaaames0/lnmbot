"""Exercise the bot's isolated-trade open, restart reconciliation, and close path.

This is intentionally a one-contract mainnet smoke test, not the strategy
runner. It opens a bot-recorded USD 1 isolated contract at 1x, constructs a
fresh executor, reconciles the remote running trade to local state, and closes
it through that restored executor.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTradesApi
from lnmarkets_bot.config import BotConfig, Network
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy import OrderIntent


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-mainnet", action="store_true")
    args = parser.parse_args()

    cfg = BotConfig(_env_file=str(args.env))
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    if not args.execute:
        print(
            f"database schema ready at {cfg.storage_db_path}; "
            "add --execute to submit the one-contract test"
        )
        return 0
    if not cfg.has_credentials():
        print("error: LNM API credentials are required", file=sys.stderr)
        return 1
    if cfg.lnm_network is Network.MAINNET and not args.confirm_mainnet:
        print("error: mainnet execution also requires --confirm-mainnet", file=sys.stderr)
        return 1

    client = LnmRestClient(
        base_url=cfg.effective_base_url(),
        access_key=cfg.lnm_access_key,
        access_secret=cfg.lnm_access_secret,
        access_passphrase=cfg.lnm_access_passphrase,
        authed=True,
    )
    try:
        api = IsolatedTradesApi(client)
        if await api.get_running_trades():
            print(
                "error: refusing test while an isolated trade is already running", file=sys.stderr
            )
            return 1

        recorder = Recorder(make_session_factory(engine))
        now = datetime.now(UTC)
        run_id = recorder.start_run(
            mode="live_smoke",
            strategy_name="live_reconcile_smoke",
            strategy_params={},
            config={"quantity_contracts": 1, "leverage": 1},
            started_at=now,
        )
        original = LiveExecutor(trades_api=api, recorder=recorder, run_id=run_id)
        original.update_price(100_000.0)
        signal_id = recorder.record_signal(
            run_id,
            ts=now,
            kind="entry",
            side="long",
            target_size_usd=1,
            target_leverage=1,
            reason="mainnet live-reconcile smoke entry",
        )
        order_id, meta = await original.submit(
            intent=OrderIntent.enter_long("smoke", 1, 1, reason="live-reconcile smoke"),
            signal_id=signal_id,
            run_id=run_id,
            ts=now,
            size_usd=1,
            leverage=1,
        )
        if order_id < 0:
            raise RuntimeError(f"open was not submitted: {meta}")
        trade_id = str(meta["lnm_trade_id"])
        print(f"opened bot-recorded trade id={trade_id} run_id={run_id}")

        restored = LiveExecutor(trades_api=api, recorder=recorder, run_id=run_id)
        await restored.reconcile()
        if restored.position_side("smoke") != "long":
            raise RuntimeError("reconciliation did not restore the expected long position")
        print(f"reconciled trade id={trade_id} into a fresh executor")

        exit_signal_id = recorder.record_signal(
            run_id,
            ts=datetime.now(UTC),
            kind="exit",
            reason="mainnet live-reconcile smoke exit",
        )
        restored.update_price(100_000.0)
        close_order_id, close_meta = await restored.submit(
            intent=OrderIntent.exit("smoke", reason="live-reconcile smoke"),
            signal_id=exit_signal_id,
            run_id=run_id,
            ts=datetime.now(UTC),
            size_usd=0,
            leverage=1,
        )
        if close_order_id < 0:
            print(f"CRITICAL: close failed for {trade_id}; close it manually", file=sys.stderr)
            raise RuntimeError(str(close_meta))
        recorder.end_run(run_id, status="done", ended_at=datetime.now(UTC))
        print(f"closed reconciled trade id={trade_id} realized_pl_sats={close_meta['pl_sats']}")
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
