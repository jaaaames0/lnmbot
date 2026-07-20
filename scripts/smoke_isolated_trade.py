"""Preflight and deliberately tiny isolated-trade smoke test.

The default mode only verifies authenticated account access and confirms that
there are no running isolated trades.  `--execute --confirm-mainnet` opens
exactly one USD 1 market contract at 1x and immediately closes that same
trade.  It is intentionally separate from the strategy runner.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lnmarkets_bot.api.client import LnmRestClient
from lnmarkets_bot.api.isolated import IsolatedTradesApi, NewIsolatedTradeParams
from lnmarkets_bot.config import BotConfig, Network


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, required=True)
    parser.add_argument(
        "--execute", action="store_true", help="Open and immediately close one contract"
    )
    parser.add_argument("--confirm-mainnet", action="store_true")
    args = parser.parse_args()

    cfg = BotConfig(_env_file=str(args.env))
    if not cfg.has_credentials():
        print("error: LNM API credentials are required", file=sys.stderr)
        return 1
    if args.execute and cfg.lnm_network is Network.MAINNET and not args.confirm_mainnet:
        print("error: --execute on mainnet also requires --confirm-mainnet", file=sys.stderr)
        return 1

    client = LnmRestClient(
        base_url=cfg.effective_base_url(),
        access_key=cfg.lnm_access_key,
        access_secret=cfg.lnm_access_secret,
        access_passphrase=cfg.lnm_access_passphrase,
        authed=True,
    )
    try:
        account = await client.get("/account")
        trades = IsolatedTradesApi(client)
        running = await trades.get_running_trades()
        print(
            f"network={cfg.lnm_network.value} balance_sats={account.get('balance')} running_isolated={len(running)}"
        )
        if running:
            print(
                "error: refusing smoke test while isolated trades are already running",
                file=sys.stderr,
            )
            return 1
        if not args.execute:
            print("preflight passed; no order was submitted")
            return 0

        trade = await trades.new_trade(
            NewIsolatedTradeParams(
                type="market",
                side="buy",
                quantity=1,
                leverage=1.0,
            )
        )
        print(f"opened isolated trade id={trade.id} quantity_contracts={trade.quantity}")
        try:
            closed = await trades.close_trade(trade.id)
        except Exception:
            print(
                f"CRITICAL: close failed; manually close isolated trade {trade.id}", file=sys.stderr
            )
            raise
        print(f"closed isolated trade id={closed.id} realized_pl_sats={closed.pl}")
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
