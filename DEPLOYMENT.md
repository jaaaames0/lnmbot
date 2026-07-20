# Deployment and operations runbook

This document describes the repository as it is now: the production `1d` and
`4h` isolated-margin MA-cross strategy, the `lnmbot` systemd service, and the
separate read-only dashboard.  Use the paths and service names consistently;
older `lnm-bot` paths are not part of this runbook.

## 1. What runs in production

`scripts/run_live.py` connects to LN Markets, polls completed `BTCUSD` one-
minute candles, aggregates them into `4h` and `1d` bars, and runs `MaCross`.
Each timeframe has independent strategy and isolated-position state.

The strategy uses SMA(20), EMA(21), a 0.5% tolerance band, per-timeframe
winner and loss cool-offs, and optional 4h high-CHOP entry-size reduction.
The locked rules are code defaults in
[`src/lnmarkets_bot/strategy/ma_cross.py`](src/lnmarkets_bot/strategy/ma_cross.py).
Runtime sizing and hard risk limits come from `/etc/lnmbot/env`.

The bot deliberately polls rather than consumes a trading WebSocket.  A
polling failure retains the last confirmed candle and later catches up.  Three
consecutive failures emit an error-level journal event; recovery is logged.

## 2. Files, paths, and service names

| Purpose | Production value |
|---|---|
| Project checkout | `/home/james/srv/tradingbot` |
| Trading configuration | `/etc/lnmbot/env` |
| Dashboard read-only credentials | `/etc/lnmbot/.env.dashboard` |
| Bot database | `/var/lib/lnmbot/lnmarkets.sqlite` |
| Halt file | `/var/lib/lnmbot/HALT` |
| Trading service | `lnmbot.service` |
| Dashboard service | `lnmbot-dashboard.service` |

The included service files assume user and group `james`, and the checkout
path above.  Change both values before installing them on a different host.

## 3. Configuration reference

Copy [`.env.example`](.env.example) to `/etc/lnmbot/env`, set mode `600`, and
keep it outside Git.  Blank optional values are disabled.  The service injects
`STORAGE_DB_PATH=/var/lib/lnmbot/lnmarkets.sqlite`, so that value takes
precedence over the same variable in the env file.

### Connection and process control

| Variable | Meaning |
|---|---|
| `LNM_NETWORK` | `mainnet` or `testnet`; selects the default LN Markets REST and stream endpoints. |
| `LNM_BASE_URL`, `LNM_WS_URL` | Optional endpoint overrides. Leave blank in normal use. |
| `LNM_ACCESS_KEY`, `LNM_ACCESS_SECRET`, `LNM_ACCESS_PASSPHRASE` | Trading API credential. All three are needed for authenticated execution. |
| `HALTED` | Set to `1` to stop processing. Remove or clear it before a later restart. |
| `HALT_FILE` | Presence halts processing. Production uses `/var/lib/lnmbot/HALT`. |
| `STORAGE_LOG_PATH` | Optional JSONL log path. Leave blank to use journald only. With the supplied hardened unit, place it under `/var/lib/lnmbot` or extend `ReadWritePaths`. |
| `STORAGE_LOG_LEVEL` | Python logging level, normally `INFO`. |

### Sizing

| Variable | Used when | Meaning |
|---|---|---|
| `SIZING_MODE` | Always | `fixed_notional` or `equity_fraction`. |
| `SIZING_FIXED_NOTIONAL_USD` | `fixed_notional` | Requested whole USD contracts per new entry, before hard caps. |
| `SIZING_LEVERAGE` | Always | Requested leverage for a new entry, before `RISK_MAX_LEVERAGE`. |
| `SIZING_TOTAL_MARGIN_FRACTION` | `equity_fraction` | Fraction of usable equity allocated across timeframes. Inert in fixed-notional mode. |
| `SIZING_TIMEFRAME_WEIGHTS` | `equity_fraction` | JSON object allocating the fraction across `1d` and `4h`; for example `{"1d":0.6,"4h":0.4}`. Inert in fixed-notional mode. |
| `SIZING_EQUITY_HAIRCUT` | `equity_fraction` | Further conservative multiplier on equity before allocation. Inert in fixed-notional mode. |

Changing sizing or leverage does not resize a trade that is already running.
After a restart, that trade is reconciled and stays open until its natural
strategy exit (or a restart catch-up exit).  New settings apply only to later
entries.

### Hard risk caps

| Variable | Meaning |
|---|---|
| `RISK_MAX_POSITION_USD` | Maximum requested notional for one timeframe position. |
| `RISK_MAX_LEVERAGE` | Maximum leverage accepted by the guard. |
| `RISK_MAX_DAILY_LOSS_USD` | Entry circuit breaker based on recorded realised P&L and funding. It is not an exchange-side stop-loss. |
| `RISK_MAX_ORDERS_PER_MINUTE` | Maximum order submissions across the process. Exits still pass through. |
| `RISK_MAX_TOTAL_NOTIONAL_USD` | Optional aggregate cap across active `1d` and `4h` positions. |
| `RISK_MAX_TOTAL_MARGIN_USD` | Optional aggregate margin cap across active positions. |

For live operation, set both aggregate caps deliberately.  They remain useful
even when fixed-notional sizing is used, because they prevent combined
exposure from exceeding the intended account allocation.

### Optional 4h CHOP overlay

The following setting is disabled by default.  When enabled, a completed 4h
bar with CHOP(14) above the threshold requests half-sized **new 4h entries**.
It does not affect 1d, exits, leverage, cool-off state, or an already-open
trade.

```dotenv
STRATEGY_4H_CHOP_REDUCE_ENABLED=true
STRATEGY_CHOP_LOOKBACK=14
STRATEGY_CHOP_HIGH_THRESHOLD=61.8
STRATEGY_CHOP_HIGH_SIZE_MULTIPLIER=0.5
```

## 4. First-time installation

Install dependencies from the checkout:

```bash
cd /home/james/srv/tradingbot
uv sync --extra dev --extra backfill
```

Create the required directories and trading configuration:

```bash
sudo install -d -o james -g james -m 700 /etc/lnmbot /var/lib/lnmbot
sudo install -o james -g james -m 600 .env.example /etc/lnmbot/env
sudoedit /etc/lnmbot/env
```

Set `LNM_NETWORK`, credentials, sizing, and conservative hard caps before
continuing.  Do not put API credentials in the checkout or Git.

Verify authenticated access without placing an order:

```bash
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/env
```

It must report the account and `running_isolated=0`.  If a trade is already
running, investigate it; do not run a smoke test or start a second executor
against that account.

For an explicit tiny mainnet order-path test, this opens exactly one USD 1
contract at 1x and immediately closes it:

```bash
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/env \
  --execute --confirm-mainnet
```

The reconciliation smoke test additionally verifies an open trade can be
restored into a fresh executor and closed.  Run it only with no other isolated
trades running:

```bash
uv run python scripts/smoke_live_reconcile.py --env /etc/lnmbot/env \
  --execute --confirm-mainnet
```

## 5. Install the services

Install the template units:

```bash
sudo install -m 644 scripts/lnmbot.service /etc/systemd/system/lnmbot.service
sudo install -m 644 scripts/lnmbot-dashboard.service /etc/systemd/system/lnmbot-dashboard.service
sudo systemctl daemon-reload
```

### Trading service: observe-only first

The checked-in `lnmbot.service` has no `--allow-orders`, so it is safe to use
for an observation run:

```bash
sudo systemctl enable --now lnmbot
systemctl status lnmbot
journalctl -u lnmbot -f
```

To enable real mainnet orders only after the smoke tests and observation have
been completed, create an explicit systemd override:

```bash
sudo systemctl edit lnmbot
```

Enter exactly:

```ini
[Service]
ExecStart=
ExecStart=/home/james/srv/tradingbot/.venv/bin/python /home/james/srv/tradingbot/scripts/run_live.py --env /etc/lnmbot/env --allow-orders --confirm-mainnet
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart lnmbot
```

Check the startup journal record.  A real-order run is recorded with
`"mode": "live"`; observe-only runs are `paper`.

To return to observe-only operation, remove the override with
`sudo systemctl revert lnmbot`, then reload and restart.

### Dashboard service

The dashboard is independently restartable and read-only.  Give it a separate
LN Markets key restricted to **Read** permission; never copy the trading key
into its env file.

```bash
sudo install -m 600 scripts/lnmbot-dashboard.env.example /etc/lnmbot/.env.dashboard
sudoedit /etc/lnmbot/.env.dashboard
sudo systemctl enable --now lnmbot-dashboard
curl http://127.0.0.1:8080/healthz
```

The repository unit listens on `127.0.0.1:8080`.  If you intentionally choose
a different port in the installed unit, use that port for the health check and
SSH tunnel.  The dashboard uses the separate key for authoritative account
snapshots and a public WebSocket for the visual BTC/USD ticker; neither path
can submit orders.

## 6. Normal operation

### Monitor

```bash
systemctl is-active lnmbot
systemctl is-active lnmbot-dashboard
journalctl -u lnmbot -n 100 --no-pager
journalctl -u lnmbot-dashboard -n 100 --no-pager
```

Use an SSH tunnel for off-host access:

```bash
ssh -L 8080:127.0.0.1:8080 optiplex
```

### Change configuration

1. Inspect any running positions in the dashboard and LN Markets.
2. Edit `/etc/lnmbot/env`.
3. Restart `lnmbot`.
4. Confirm a fresh run starts, reconciles positions, and displays the intended
   configuration on the dashboard's Runs page.

```bash
sudoedit /etc/lnmbot/env
sudo systemctl restart lnmbot
journalctl -u lnmbot -n 80 --no-pager
```

Do not change sizing with an expectation that current positions will be
resized.  Do not run a second live runner against the same account while the
service is active.

### Halt new processing

To halt via the file switch:

```bash
sudo touch /var/lib/lnmbot/HALT
sudo systemctl restart lnmbot
```

This does not close positions automatically.  Inspect LN Markets and close a
position manually if required.  To permit a later restart, remove the file:

```bash
sudo rm /var/lib/lnmbot/HALT
```

Alternatively set `HALTED=1` in `/etc/lnmbot/env` and restart.  Clear it
before resuming.

### If the service is down with an open position

1. Check the LN Markets isolated-trades interface immediately.
2. Decide whether to close the position manually; the bot cannot provide a
   missed strategy exit while it is offline.
3. Restore service/network health.
4. Start the service and read the journal.  Startup reconciliation refuses an
   unrecorded or ambiguous remote trade rather than opening another one.
5. If the restored trade is opposite the first confirmed directional verdict,
   the bot emits a `restart_catch_up` exit and waits for a later fresh
   transition before entering again.

The dashboard is useful but not an independent uptime monitor.  External
monitoring from the VPS is the priority deferred safeguard; see
[docs/dashboard-roadmap.md](docs/dashboard-roadmap.md).

## 7. Test-only 5m profile

`--test-5m` exists to exercise live data, execution, reconciliation, and
cool-off mechanics more frequently.  It is not a validated trading strategy
and must not run alongside the production service using the same account.

Use a separate database for paper-only experiments:

```bash
STORAGE_DB_PATH="$HOME/srv/tradingbot/runs/test-5m.sqlite" \
  uv run python scripts/run_live.py --env /etc/lnmbot/env --test-5m
```

It stays observe-only without `--allow-orders`.  Mainnet execution also needs
both `--confirm-mainnet` and `--confirm-test-profile`.  Stop the production
service first, keep strict limits, and ensure no isolated trade remains before
returning to production.

`--test-5m-cooldown-probe` is an additional test-only profile that sets tiny
thresholds and two suppressed transitions.  It validates cool-off recording,
counter depletion, and resumption; it is never a production calibration.

## 8. Accounting notes

- Trade records use actual LN Markets opening and closing fees returned by the
  isolated-trade API.
- Funding is recorded as a signed settlement.  LN Markets reports paid funding
  as positive and received funding as negative; dashboard funding P&L presents
  the inverse, so positive means the account received funding.
- Mark P&L on an open trade is exchange mark-to-market.  Net P&L includes
  recorded fees and funding only; it does not invent a future closing fee.
- The dashboard's account header uses the read-only API key when available.
  Local database values are a fallback only.

## 9. Repository map

| Path | Role |
|---|---|
| `scripts/run_live.py` | Production runner and explicit test profiles |
| `scripts/smoke_isolated_trade.py` | Minimal account/open/close smoke test |
| `scripts/smoke_live_reconcile.py` | Live executor reconciliation smoke test |
| `scripts/run_dashboard.py` | Read-only local dashboard |
| `scripts/lnmbot.service` | Observe-only systemd template |
| `scripts/lnmbot-dashboard.service` | Loopback read-only dashboard unit |
| `src/lnmarkets_bot/strategy/ma_cross.py` | Locked strategy defaults |
| `src/lnmarkets_bot/engine/live_executor.py` | Isolated-order execution and reconciliation |
| `src/lnmarkets_bot/risk/guard.py` | Hard limits and sizing guard |
| `docs/dashboard-roadmap.md` | Dashboard scope and deferred safeguards |
