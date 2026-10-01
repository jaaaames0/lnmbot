# Deployment and operations runbook

This guide covers the live `1d`/`4h` MA-cross strategy, the optional funded
daily close-range breakout, and the separate read-only dashboard. The example
configuration disables breakout; the accepted September 27 production release
had it enabled. Replace every angle-bracket placeholder for the target host.
See [CHANGELOG.md](CHANGELOG.md) for dated deployment and dashboard history.

## 1. What runs in production

`scripts/run_live.py` connects to LN Markets, polls completed `BTCUSD` one-
minute candles, aggregates them into `4h` and `1d` bars, and runs `MaCross`.
Each timeframe has independent strategy and isolated-position state.
When `STRATEGY_BREAKOUT_ENABLED=true`, the same funded runner also manages a
daily close-range breakout campaign with separately owned K units. It requires
`--allow-orders`; the runner rejects a breakout-enabled observe-only start.
Turning new breakout entries off does not abandon funded units already open.

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

The checked-in service templates contain unresolved placeholders
`@TRADER_RELEASE_DIR@` and `@DASHBOARD_RELEASE_DIR@`. Build each pinned release
with its own `.venv` from `uv.lock`, then render its template with its absolute
release path before installation. Keep production services on immutable
releases rather than the editable checkout. Trader and dashboard revisions can
be changed independently.

## 3. Configuration reference

Copy [`.env.example`](.env.example) to the trading environment-file path, set
root ownership and mode `640` with group access for `lnmbot`, and keep it
outside Git. Blank optional values are disabled. The
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
| `RISK_MAX_POSITION_USD` | Maximum requested notional for one position slot. |
| `RISK_MAX_LEVERAGE` | Maximum leverage accepted by the guard. |
| `RISK_MAX_DAILY_LOSS_USD` | Entry circuit breaker based on recorded realised P&L and funding. It is not an exchange-side stop-loss. |
| `RISK_MAX_ORDERS_PER_MINUTE` | Maximum order submissions across the process. Exits still pass through. |
| `RISK_MAX_TOTAL_NOTIONAL_USD` | Optional aggregate cap across active funded positions. |
| `RISK_MAX_TOTAL_MARGIN_USD` | Optional aggregate margin cap across active funded positions. |

For live operation, decide explicitly whether to set both aggregate caps.
They remain useful even with fixed-notional sizing because they prevent
combined exposure from exceeding the intended account allocation.
The September 27 accepted production configuration left both optional caps
unset; adding them is a separate risk-policy change.

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

### Optional funded close-range breakout

`STRATEGY_BREAKOUT_ENABLED` controls admission of new breakout entries. Set it
to `false` for an observe-only service. When enabled, the strategy uses the
seed files in `config/seeds/` and shares the live account and risk guard with
MA-cross. Set `STRATEGY_BREAKOUT_UNIT_NOTIONAL_USD` and
`STRATEGY_BREAKOUT_LEVERAGE` deliberately. `STRATEGY_BREAKOUT_DIRECTION_MODE`
accepts `both`, `long_only`, or `short_only` for **new** parents and add-ons;
it does not rewrite or close an existing campaign. On a recovery exit, a
qualifying same-direction parent is rejected at that same open, while later
signals and existing exits follow their ordinary rules.

The seeded historical campaign and order-incapable shadow book are references
only; neither is a funded position or part of funded P&L. Review owned venue
inventory and campaign state before changing breakout settings.
At each eight-hour boundary, the trader verifies historical funding before
advancing the seeded campaign. At the daily decision it waits up to three
seconds for a newly published settlement. If funding remains late, the breakout
model pauses and retries while the trader buffers its price bars. Once funding is
complete, it replays those bars in order. LN Markets normally publishes a
settlement two to three minutes after its boundary, so a daily decision held for
up to ten minutes still acts, at the then-current price; a later recovery
resumes at the next live decision and never places missed entries late. Waits
under 15 minutes are logged as info and `/readyz` reports them under `pending`
rather than as errors; longer waits are warnings and readiness failures. The paused state survives a trader
restart through its saved checkpoint and the live feed's candle backfill. If
price evidence is missing or the buffer exceeds roughly three days, it needs
verified reconstruction before new breakout entries. MA trading and owned
breakout exits continue throughout. In that case, verify the missing funding
and candle evidence, then restart only the trader to rebuild from its saved
checkpoint; do not clear the model flags by hand.

### Optional impulse-range strategy

`STRATEGY_RANGE_MODE` accepts `off` (default), `shadow`, or `funded`. Any mode
other than `off` requires an order-enabled run. The strategy waits for a
structure-passing daily breakout (the same candidate rule as the breakout
strategy), confirms a swing channel after an 8% pullback, and trades from the
channel edges to its midpoint with market orders on a 1m close. A 4h close
beyond an edge exits, and the channel is redrawn; a range expanding beyond 40%
width is abandoned, and its entry size tapers to zero over 120 days. A range may
confirm wider than 40% (as tested); at 5x its stop can then lie beyond isolated
liquidation, which bounds the loss at the margin. After any venue-side close
the strategy does not re-enter within the same 4h bar.

- `shadow` records paper fills at the next minute's open, with fees and
  slippage but no funding, and places no orders. Paper fills appear as
  `shadow_entry` and `shadow_exit` signals in the audit trail.
- `funded` owns one isolated trade (`btc_impulse_range_v1:r0`) sized at
  `STRATEGY_RANGE_UNIT_NOTIONAL_USD` times the age taper, at
  `STRATEGY_RANGE_LEVERAGE`, under the shared risk guard.
- With `STRATEGY_RANGE_CHOP_FILTER=true`, a range whose 20-day efficiency ratio
  at confirmation is below `STRATEGY_RANGE_CHOP_THRESHOLD` is tracked but not
  traded. `STRATEGY_RANGE_DIRECTION_MODE` limits new entries.
- Mode, size, leverage, direction, and filter settings may change across a
  restart. The range-construction rules are fixed; a saved state built with
  different construction rules is refused.

On the first start, the daily detector is warmed from
`STRATEGY_RANGE_SEED_DAILY_PATH` up to the start of the live feed's 100-day
warmup, and the warmup rebuilds recent range state. A range that began before
that window is not recognised; the strategy stays idle until the next impulse.
If the seed does not reach the warmup start, the strategy is not started and
`live.range_cold_start_unavailable` is logged; supply a newer daily seed.
Later restarts restore the saved state instead. A cold range strategy does not
make the feed's whole warmup strict: gaps before its first saved state only
affect the rebuilt range, and a later missing daily or 4h bar blocks its entries.

Entries are never taken on replayed bars; an exit that fell due while the
trader was stopped is sent on the first live bar. A missing daily or 4h bar
marks the model incomplete: new entries stop and owned exits continue. There is
no automated rebuild yet; `incomplete_reason` in the saved state records the cause.

The dashboard overview shows a Range card and position row whenever the mode
is not `off`, a range snapshot exists, or a range trade is owned. It shows the
current channel and levels, the shadow book (excluded from account totals),
and recent range events. The Execution indicator reports an incomplete range
model or an owned position that the saved range state does not record.

## 4. First-time installation

For local checks, install dependencies in the checkout:

```bash
cd /home/james/src/lnmbot
uv sync --extra dev --extra dashboard --extra backfill
```

Build separate immutable trader and dashboard releases from a reviewed Git
commit, retaining `config/seeds/`, the service scripts, and `uv.lock`. Run
`uv sync --frozen` in each release directory, with the extras required there.
The service accounts `lnmbot`, `lnmbot-dashboard`, and the shared `lnmbot-db`
group must exist before installing state directories or units. Do not point a
production service at the editable checkout.

Create the required directories and trading configuration:

```bash
sudo install -d -o root -g lnmbot -m 750 /etc/lnmbot
sudo install -d -o root -g lnmbot-dashboard -m 750 /etc/lnmbot-dashboard
sudo install -d -o lnmbot -g lnmbot-db -m 750 /var/lib/lnmbot
sudo install -o root -g lnmbot -m 640 .env.example /etc/lnmbot/trader.env
sudoedit /etc/lnmbot/trader.env
```

Set `LNM_NETWORK`, credentials, sizing, and conservative hard caps before
continuing. Set `STORAGE_LOG_PATH` blank for journald-only logging or to a
writable path under `/var/lib/lnmbot`; the example's relative `./logs/` path
is unsuitable for the hardened release unit. Do not put API credentials in
the checkout or Git.

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

Render the template units using the absolute directories of the two built
releases, then install the rendered files. For example, after setting
`TRADER_RELEASE_DIR` and `DASHBOARD_RELEASE_DIR` to those paths:

```bash
sed "s|@TRADER_RELEASE_DIR@|${TRADER_RELEASE_DIR:?set release path}|g" scripts/lnmbot.service \
  | sudo tee /etc/systemd/system/lnmbot.service >/dev/null
sed "s|@DASHBOARD_RELEASE_DIR@|${DASHBOARD_RELEASE_DIR:?set release path}|g" scripts/lnmbot-dashboard.service \
  | sudo tee /etc/systemd/system/lnmbot-dashboard.service >/dev/null
sudo chmod 644 /etc/systemd/system/lnmbot.service /etc/systemd/system/lnmbot-dashboard.service
sudo systemd-analyze verify /etc/systemd/system/lnmbot.service /etc/systemd/system/lnmbot-dashboard.service
sudo systemctl daemon-reload
```

Check that no `@...@` placeholder remains in either installed unit. The
trader unit is observe-only and must use `STRATEGY_BREAKOUT_ENABLED=false` in
that mode. An existing funded breakout campaign needs the order-enabled
service to continue its exits; use a separate reviewed recovery procedure
rather than switching it to observe-only.

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
ExecStart=/usr/bin/env STORAGE_DB_PATH=/var/lib/lnmbot/lnmarkets.sqlite <trader-release>/.venv/bin/python <trader-release>/scripts/run_live.py --env /etc/lnmbot/trader.env --allow-orders --confirm-mainnet
```

Replace both `<trader-release>` values with the installed trader release path.
Keep the `STORAGE_DB_PATH` assignment: replacing `ExecStart` also removes the
database-path assignment from the template command.

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

The chart viewer at `/charts` reads only recorded candles and strategy snapshots.
`/api/chart` provides the bounded display interface; chart JavaScript and CSS are
served from fixed `/assets/dashboard_chart.*` routes. No node toolchain, new
credentials or schema migration is required. Keep the package's local chart
assets in dashboard release artifacts.

Preview source changes against a consistent SQLite backup, with the copied file
made read-only, without loading protected env files:

```bash
uv run python scripts/run_dashboard.py --offline \
  --db /path/to/read-only-copy.sqlite --host 127.0.0.1 --port 8099
```

`--offline` disables venue credential use and the public price stream; account
and execution labels reflect the copied evidence. Preview state ages normally.

Dashboard deployment is a separate operation. For a dashboard-only change, use
the host handbook's independent immutable dashboard release procedure and leave
the trader release and process running. `scripts/deploy_range_remediation.py`
restarts both services and is unsuitable for this purpose. Verify service paths,
dashboard readiness and the unchanged trader PID/release before and after the
change. Avoid the 00/04/08/12/16/20 UTC boundaries, including funding at 00/08/16.

The 1 October dashboard-only release is `prod-20261001.2-g7709dd300b89`; the trader
remains on `prod-20261001.1-g0b1650b10779`. The readiness oneshot's Python
and script paths follow the dashboard release and must switch with it. Preserve
the installed unit's identity, sandbox, credential path and **160 MiB** cap;
the repository template's memory value is not the current host baseline.

For the next dashboard-only release:

1. Commit tested source and export tracked package, launcher, readiness script,
   seeds and locked build inputs into a new empty release directory. Run
   `uv sync --frozen --no-dev --no-editable` at that final path, then make
   runtime files root-owned and non-writable.
2. Checkpoint the two dashboard/probe units and a consistent read-only SQLite
   backup. Stage an offline candidate on a private copy under the installed
   sandbox/memory cap; check concurrent long-window chart requests.
3. Prepare unit candidates by changing only dashboard release paths. Arm an
   independent timed rollback that restores those units and restarts only the
   dashboard; never restore the database or touch the trader/configuration.
4. Between boundaries, stop only the dashboard, install the two units, reload
   systemd and start the dashboard. Allow startup time before probing HTTP.
5. Verify LAN browser pages/charts, readiness/probe, read-only boundaries,
   unchanged trader PID/release/config and execution journal, encrypted backup
   and monitoring. Record acceptance, disarm rollback and retain the old release.

The dated transaction and detailed evidence are in the local ignored
`docs/operations/2026-10-01-dashboard-release.md` archive.

An optional, separately installed `lnmbot-breakout-shadow.service` and timer
can advance an order-incapable daily breakout book from completed public
Binance candles. Render its `@SHADOW_RELEASE_DIR@` placeholder before
installation. Its state belongs under `/var/lib/lnmbot-shadow`, never in the
funded trader database. The dashboard can read that book through the optional
`LNMBOT_PORTFOLIO_SHADOW_DB` setting in its own env file; it keeps hypothetical
results separate from funded accounting.

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

The dashboard is useful but not an independent uptime monitor. An external
monitor should check host, VPN, dashboard, trader process, and data freshness;
the `/healthz` endpoint alone cannot detect every live-but-stalled condition.
Other deferred dashboard work includes prominent halt/risk/reconciliation
status and read-only notifications. Any future emergency halt control must
remain unable to enable orders or alter sizing or strategy parameters.

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
| `scripts/lnmbot-dashboard.service` | Read-only dashboard unit; wildcard listener needs a firewall or loopback edit |
| `scripts/lnmbot-breakout-shadow.service` | Optional order-incapable shadow unit and timer |
| `src/lnmarkets_bot/strategy/ma_cross.py` | Locked strategy defaults |
| `src/lnmarkets_bot/engine/live_executor.py` | Isolated-order execution and reconciliation |
| `src/lnmarkets_bot/risk/guard.py` | Hard limits and sizing guard |
| `CHANGELOG.md` | Dated production and dashboard release history |


## 10. Recovery behavior introduced in the September 2026 remediation

The release history and dated acceptance result are in
[CHANGELOG.md](CHANGELOG.md); the full transcript remains in the local,
Git-ignored operations archive.
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

## 11. Range remediation release and recovery

Use `scripts/deploy_range_remediation.py` for the September 30 remediation and
compatible follow-up releases. Run the focused regressions, default pytest
suite, scoped Ruff and import-linter; commit reviewed source before building.
The helper refuses tracked worktree changes and a tag that lacks the source
commit suffix. Preserve local ignored research and unrelated untracked scripts.

```bash
sudo python3 scripts/deploy_range_remediation.py build --tag prod-YYYYMMDD.N-g<12-character-commit>
sudo python3 scripts/deploy_range_remediation.py cutover
curl -fsS http://127.0.0.1:8082/readyz
sudo python3 scripts/deploy_range_remediation.py accept
```

Run root Python validation with `PYTHONDONTWRITEBYTECODE=1` so root does not
generate writable caches inside immutable runtimes.

Build creates root-owned immutable trader, dashboard and compatible recovery
runtimes, preserving an archive, Git bundle and manifests in a protected
checkpoint. Cutover takes a consistent SQLite copy, validates seed hashes by
input role, preserves the existing funded range/risk settings, checks candidate
units and runs an encrypted backup. It arms and verifies a 30-minute compatible
recovery timer **before** changing live files. Acceptance asserts both services,
current bindings/snapshot provenance, fresh feed and venue inventory, resolved
commands, model health and the range HTTP route; it checks again after backup
and disarms recovery last. Failed acceptance leaves the timer armed.

The checkpoint's `recover.sh` uses corrected, range-capable code with
`LIVE_ENTRIES_ENABLED=false`, retaining the current database and all owned exit
managers. It is available for attended recovery after acceptance. Never use the
`rollback.sh` in the first 30 September checkpoint
(`lnmbot-impulse-range-20260930T132213Z`) to return to pre-range code.
Returning to code without a range binding requires stopping the trader first,
resolving every submitted/received entry command, and independently verifying
both running and pending venue inventory are flat for range. Local order count
alone is insufficient. Never roll the ledger back to an earlier database.

Install `config/systemd/lnmbot-readiness.service` after replacing
`@DASHBOARD_RELEASE@` with the immutable dashboard path, and its timer. The
read-only probe fails on unhealthy `/readyz`, so the existing infrastructure
monitor's failed-unit check detects trader/model failures even when dashboard
liveness succeeds. Readiness failures require diagnosis, not clearing state.

The fixed September 13 daily seed supports a first 100-day bootstrap only
through December 22, 2026 UTC. A normal saved-state restart does not need a new
range seed. Before a future cold rebuild, refresh from authoritative **closed**
local daily candles using the dry-run tool; it refuses gaps and duplicates:

```bash
uv run python scripts/rebuild_impulse_range.py --daily <verified-daily.parquet> --refresh-seed --output <new-seed.parquet>
uv run python scripts/rebuild_impulse_range.py --daily <new-seed.parquet> --minutes <contiguous-UTC-whole-day-minutes.parquet> --copied-db <consistent-copy.sqlite> --output <candidate-range.json>
```

Reconstruction makes no exchange requests and never changes the live database.
The output records input hashes and a flat range snapshot. It refuses outstanding
commands and locally open range trades; independently verify fresh venue running
and pending inventory too. Start minute replay after the daily seed seam and
include the full construction interval, which may be longer than 100 days.
Review channel, ER, rule version, MA/breakout continuity and P&L against the
existing state before any stopped-trader snapshot replacement. Snapshot repair
is a separate attended transaction with backup and compatible recovery; never
hand-clear `model_complete`, delete snapshots or alter accounting to manufacture
readiness. Track the refreshed range seed in `config/seeds/`, publish it through
an immutable release, and repoint only the range input role. Keep breakout
historical seed/reference hashes unchanged. Scan protected input references
before retiring any older runtime.
