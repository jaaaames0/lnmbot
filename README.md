# LN Markets MA-cross bot

An isolated-margin BTC/USD futures bot for LN Markets. It runs a locked,
two-timeframe moving-average strategy (1d and 4h) from closed 1m candles.
Each timeframe owns its own isolated trade; a 1d action never closes or
resizes a 4h trade.

The normal operating model is deliberately simple:

```text
/etc/lnmbot/env  -> reviewed configuration
systemd          -> always-on observe-only or real-order process
local dashboard  -> read-only monitoring
journalctl       -> diagnostics
```

## Safety model

- Observe-only is the default. `run_live.py` only sends orders with
  `--allow-orders`; mainnet additionally requires `--confirm-mainnet`.
- Real isolated positions are reconciled at startup. An unknown remote trade
  fails closed rather than allowing a duplicate position.
- A same-direction entry is idempotent: it cannot create a second isolated
  trade for the same timeframe. If an entry response is lost, the bot checks
  running trades immediately and exits rather than continuing with ambiguous
  exposure.
- If a restored position is opposite the first confirmed post-restart verdict,
  the bot closes it and waits for a fresh transition before re-entering. It
  never recreates a stale entry that may have occurred during downtime.
- Strategy requests pass through independent hard risk caps: per-position
  size/leverage, daily loss, order rate, optional aggregate notional, and
  optional aggregate margin.
- `HALTED=1` or the presence of `HALT_FILE` stops new processing within a
  second; use `/var/lib/lnmbot/HALT` in production and remove it to permit a
  subsequent restart.
- Dashboard access is loopback-only and read-only.

## Daily operation

Monitor the service:

```bash
systemctl status lnmbot
journalctl -u lnmbot -f
```

Monitor recorded signals and paper/live orders locally:

```bash
cd ~/srv/tradingbot
uv run python scripts/run_dashboard.py --db /var/lib/lnmbot/lnmarkets.sqlite
```

Open `http://127.0.0.1:8080`, or tunnel it from another machine:

```bash
ssh -L 8080:127.0.0.1:8080 optiplex
```

The overview shows active isolated positions, the latest 1d/4h signal,
available balance and total equity, net realised P&L after trading fees,
funding, and recent BTC/USD price changes. Detailed signals, trades, funding,
P&L, run history, and health each have their own read-only page. The optional
dashboard key is a separate LN Markets **Read**-only credential in
`/etc/lnmbot/.env.dashboard`; never reuse the trading key there.

To keep the dashboard running independently, install
[`scripts/lnmbot-dashboard.service`](scripts/lnmbot-dashboard.service) as the
`lnmbot-dashboard` systemd unit; the full commands are in
[DEPLOYMENT.md](DEPLOYMENT.md).

To change configuration, edit `/etc/lnmbot/env`, then restart and verify the
new run in the dashboard and journal:

```bash
sudo systemctl restart lnmbot
```

Do not use a dashboard control to alter position sizing or enable trading.
Those changes must be explicit configuration plus restart.

## Sizing

`SIZING_MODE=fixed_notional` is the appropriate initial live setting. The
requested contracts and leverage come from `SIZING_FIXED_NOTIONAL_USD` and
`SIZING_LEVERAGE`, then remain subject to `RISK_*` hard caps.

`SIZING_MODE=equity_fraction` obtains the authenticated account balance before
each real entry, applies the configured haircut and timeframe allocation, and
calculates whole USD contracts. It should not be enabled until its
compounding/liquidation simulation is complete.

## Documentation

[DEPLOYMENT.md](DEPLOYMENT.md) is the full installation, configuration,
smoke-test, recovery, and live-operation runbook. The planned dashboard scope
is in [docs/dashboard-roadmap.md](docs/dashboard-roadmap.md).
