# Deployment and operations runbook

This document describes the repository as it is now: the production `1d` and
`4h` isolated-margin MA-cross strategy and optional daily close-range breakout, the `lnmbot` systemd service, and the
separate read-only dashboard.  Replace every angle-bracket placeholder with a
value for the target host.

## 1. What runs in production

`scripts/run_live.py` connects to LN Markets, polls completed `BTCUSD` one-
minute candles, aggregates them into `4h` and `1d` bars, and runs `MaCross`.
Each timeframe has independent strategy and isolated-position state.

The strategy uses SMA(20), EMA(21), a 0.5% tolerance band, per-timeframe
winner and loss cool-offs, and optional 4h high-CHOP entry-size reduction.
The locked rules are code defaults in
[`src/lnmarkets_bot/strategy/ma_cross.py`](src/lnmarkets_bot/strategy/ma_cross.py).
Runtime sizing and hard risk limits come from the deployment environment file.

The bot deliberately polls rather than consumes a trading WebSocket.  A
polling failure retains the last confirmed candle and later catches up.  Three
consecutive failures emit an error-level journal event; recovery is logged.

## 2. Files, paths, and service names

| Purpose | Example value |
|---|---|
| Editable source | `/home/james/src/lnmbot` |
| Immutable trader release | `/usr/local/lib/lnmbot/<release>` |
| Immutable dashboard release | `/usr/local/lib/lnmbot-dashboard/<release>` |
| Trading configuration | `/etc/lnmbot/trader.env` |
| Dashboard read-only credentials | `/etc/lnmbot-dashboard/dashboard.env` |
| Bot database | `/var/lib/lnmbot/lnmarkets.sqlite` |
| Halt file | `/var/lib/lnmbot/HALT` |
| Trading service account | `lnmbot` |
| Dashboard service account | `lnmbot-dashboard` |
| Trading service | `lnmbot.service` |
| Dashboard service | `lnmbot-dashboard.service` |

The checked-in service templates deliberately contain the invalid placeholders
`@TRADER_RELEASE_DIR@` and `@DASHBOARD_RELEASE_DIR@`. Render those placeholders
to two independently built immutable release directories before installation.
Never point either production service at the editable source checkout, and do
not switch the trader merely because a dashboard release is ready.

## 3. Configuration reference

Copy [`.env.example`](.env.example) to the trading environment-file path, set
mode `600`, and keep it outside Git.  Blank optional values are disabled.  The
example service injects
`STORAGE_DB_PATH=/var/lib/lnmbot/lnmarkets.sqlite`, so that value takes
precedence over the same variable in the env file.

### Connection and process control

| Variable | Meaning |
|---|---|
| `LNM_NETWORK` | `mainnet` or `testnet`; selects the default LN Markets REST and stream endpoints. |
| `LNM_BASE_URL`, `LNM_WS_URL` | Optional endpoint overrides. Leave blank in normal use. |
| `LNM_ACCESS_KEY`, `LNM_ACCESS_SECRET`, `LNM_ACCESS_PASSPHRASE` | Trading API credential. All three are needed for authenticated execution. |
| `HALTED` | Set to `1` to stop processing. Remove or clear it before a later restart. |
| `HALT_FILE` | Presence halts processing. Set it to a path in the service-writable state directory. |
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
cd /home/james/src/lnmbot
uv sync --extra dev --extra dashboard --extra backfill
```

Create the required directories and trading configuration:

```bash
sudo install -d -o root -g lnmbot -m 750 /etc/lnmbot
sudo install -d -o root -g lnmbot-dashboard -m 750 /etc/lnmbot-dashboard
sudo install -d -o lnmbot -g lnmbot-db -m 750 /var/lib/lnmbot
sudo install -o root -g lnmbot -m 640 .env.example /etc/lnmbot/trader.env
sudoedit /etc/lnmbot/trader.env
```

Set `LNM_NETWORK`, credentials, sizing, and conservative hard caps before
continuing.  Do not put API credentials in the checkout or Git.

Verify authenticated access without placing an order:

```bash
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/trader.env
```

It must report the account and `running_isolated=0`.  If a trade is already
running, investigate it; do not run a smoke test or start a second executor
against that account.

For an explicit tiny mainnet order-path test, this opens exactly one USD 1
contract at 1x and immediately closes it:

```bash
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/trader.env \
  --execute --confirm-mainnet
```

The reconciliation smoke test additionally verifies an open trade can be
restored into a fresh executor and closed.  Run it only with no other isolated
trades running:

```bash
uv run python scripts/smoke_live_reconcile.py --env /etc/lnmbot/trader.env \
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
ExecStart=<trader-release>/.venv/bin/python <trader-release>/scripts/run_live.py --env /etc/lnmbot/trader.env --allow-orders --confirm-mainnet
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
sudo install -o root -g lnmbot-dashboard -m 640 \
  scripts/lnmbot-dashboard.env.example /etc/lnmbot-dashboard/dashboard.env
sudoedit /etc/lnmbot-dashboard/dashboard.env
sudo systemctl enable --now lnmbot-dashboard
curl http://127.0.0.1:8082/healthz
```

The Optiplex unit listens on wildcard port `8082`, while the host firewall
admits it only from the intended LAN. A loopback bind plus SSH tunnel is a good
default on hosts without that firewall boundary. The dashboard uses the
separate key for authoritative account snapshots and a public WebSocket for
the visual BTC/USD ticker; neither path can submit orders.

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
ssh -L 8082:127.0.0.1:8082 <bot-host>
```

### Change configuration

1. Inspect any running positions in the dashboard and LN Markets.
2. Edit `/etc/lnmbot/trader.env`.
3. Restart `lnmbot`.
4. Confirm a fresh run starts, reconciles positions, and displays the intended
   configuration on the dashboard's Runs page.

```bash
sudoedit /etc/lnmbot/trader.env
sudo systemctl restart lnmbot
journalctl -u lnmbot -n 80 --no-pager
```

Do not change sizing with an expectation that current positions will be
resized.  Do not run a second live runner against the same account while the
service is active.

### Indicator continuity on restart

The first live start after installing the current strategy state schema loads
100 days of LN Markets candle history. This gives the daily EMA(21) enough
recursive updates after its seed to closely match a continuously calculated
EMA. LN Markets currently supplies this historical endpoint as 1-minute
candles, which the bot aggregates locally; startup can therefore take longer
than a normal restart.

After the first live minute and each completed 1d or 4h bar, the bot stores its
indicator, verdict, cool-off, manual-hold, and pending-execution state in the
configured SQLite database. Signal state is committed before an API order and
again after the executor position is mirrored. A later restart restores that
state and skips overlapping warmup bars, so it does not reseed or mutate the
EMA a second time. A strategy-parameter change intentionally invalidates the
old snapshot and performs the deep bootstrap again.

The funded runner always restores the MA strategy under `ma_cross_primary`.
Setting `STRATEGY_BREAKOUT_ENABLED=false` prevents new breakout entries but
continues to reconcile and exit any funded breakout K units already open.
At an open that closes a breakout campaign for `recover`, the strategy rejects
only a qualifying new parent in the **same direction at that same open**.
Later daily signals remain eligible, as does an opposite-direction parent.
The rejection is recorded as `recovery_same_open`; existing add-ons and all
campaign exits retain their ordinary rules. No timer or saved cooldown state
is introduced.
`STRATEGY_BREAKOUT_DIRECTION_MODE` accepts `both` (the default), `long_only`,
or `short_only`. It controls admission of new breakout parents and funded
add-ons, including a deferred reversal that has not yet been submitted. A
disallowed signal is recorded as a blocked breakout decision; it does not
occupy the campaign slot. A mode change never closes or rewrites an existing
campaign. Its existing range, recovery, cap, and liquidation exits still run.
The historical seeded campaign remains a reference to the original
both-direction rules, including its hypothetical add-ons. The mode and any
change timestamp appear in the breakout snapshot; the effective mode also
appears on the dashboard and in new run parameters. Invalid values prevent
startup. To switch modes, review the current funded campaign and pending
reversal, edit only this variable in `/etc/lnmbot/trader.env`, then follow the
normal versioned deployment/restart procedure above and verify the displayed
mode, campaign state, and venue positions. Reverting the variable to `both`
restores normal admission for subsequent signals; it does not recreate
previously rejected entries.
After a restart, compare the venue and local open sets, verify the MA cool-off
and breakout campaign snapshots, and wait for a new live minute bar before
accepting the restart. A missed breakout campaign exit found during warmup is
sent when live bars resume. Missing one-minute candles in an uncommitted day
stop the funded feed rather than forming an incomplete 4h or daily signal;
investigate the gap before restarting again. Older committed history gaps do
not invalidate the restored strategy state. The 100-day candle fetch can hit
LN Markets rate limits even when both strategy snapshots restore successfully.

An unchanged directional verdict is not itself an entry signal. The bot only
retries under an unchanged verdict when the durable snapshot says that a
previously emitted order remains unconfirmed. This prevents a cold restart or
an intentionally flat position from manufacturing a late MA-cross entry.

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

Alternatively set `HALTED=1` in `/etc/lnmbot/trader.env` and restart.  Clear it
before resuming.

### If the service is down with an open position

1. Check the LN Markets isolated-trades interface immediately.
2. Decide whether to close the position manually; the bot cannot provide a
   missed strategy exit while it is offline.
3. Restore service/network health.
4. Start the service and read the journal.  Startup reconciliation refuses an
   unrecorded or ambiguous remote trade rather than opening another one.
5. If the restored trade is opposite the first confirmed directional verdict,
   the bot applies the normal transition logic: it closes the restored trade
   and, when same-bar flips are enabled and no cool-off is triggered, opens
   the new direction. The recorded signal metadata includes the LNM close,
   SMA, EMA, tolerance, and distances used for that decision.

The dashboard is useful but not an independent uptime monitor.  External
monitoring from the VPS is the priority deferred safeguard; see
[docs/runtime/dashboard-roadmap.md](docs/runtime/dashboard-roadmap.md).

## 7. Test-only 5m profile

`--test-5m` exists to exercise live data, execution, reconciliation, and
cool-off mechanics more frequently.  It is not a validated trading strategy
and must not run alongside the production service using the same account.

Use a separate database for paper-only experiments:

```bash
STORAGE_DB_PATH="$HOME/src/lnmbot/runs/test-5m.sqlite" \
  uv run python scripts/run_live.py --env /etc/lnmbot/trader.env --test-5m
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
| `docs/runtime/dashboard-roadmap.md` | Dashboard scope and deferred safeguards |


## Audit remediation release (27 September 2026)

See [acceptance evidence and cutover procedure](docs/runtime/remediation-release-2026-09-27.md).
Stable snapshot names are `ma_cross_primary` and `btc_close_range_v1`; readers
prefer them over retained legacy class-name rows. Never select an arbitrary
newest strategy snapshot.

Confirmed external MA liquidation starts the configured loss cooldown even
when funding caused liquidation before the price-loss threshold. An external
closure whose cause is unavailable also starts that cooldown, while its
accounting remains honestly unclassified. A confirmed manual closure resets
only its owning timeframe, without cooldown, and reconsiders eligibility at its
next live timeframe boundary. Warmup and minute reconciliation never open the
replacement trade. REST closed history has a numeric liquidation price rather
than a reliable cause flag; price alone does not classify a closure.

An operator with definitive cause evidence may review
`scripts/classify_ma_external_close.py --db <consistent-copy> --timeframe <1d|4h>
--trade-id <id> --cause <manual|liquidation>`. Default is read-only. To apply,
stop the trader, take a consistent backup, add `--apply --evidence-note <note>`,
and verify the snapshot, delivery receipt and attribution before restarting.
The utility refuses outstanding commands and a later trade on that slot. It
changes lifecycle attribution, never fills, fees or P&L quantities.

For recovery, `RISK_MAX_POSITION_USD=0` in the trader's **process environment**
blocks admissions through the hard size cap while known exits continue. Do not
use the whole-engine halt for this purpose. Reverting to pre-remediation code
or restoring an old database after new writes requires a separate review; the
independent cutover recovery timer uses compatible code with this zero-entry
cap and retains the current database.
