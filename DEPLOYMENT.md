# v1.3 deployment — what you have and what to do

## v1.3 strategy (locked)

| Param | Value |
|---|---|
| `tolerance_pct` | 0.005 |
| `size_multipliers` | `{"1d": 1.0, "4h": 1.0}` |
| `cooldown_threshold_pct` | `{"1d": 0.03, "4h": 0.05}` |
| `cooldown_signal_count` | `{"1d": 12, "4h": 11}` |
| `cooldown_mode` | `verdict_transition` |
| `loss_cooldown_threshold_pct` | `{"1d": 0.05, "4h": 0.02}` |
| `loss_cooldown_signal_count` | `{"1d": 3, "4h": 4}` |
| 4h CHOP reduction | disabled by default; when enabled, CHOP(14) > 61.8 uses 0.5x new-entry notional |
| `base_size_usd` | 1000 (override via .env) |
| `base_leverage` | 2 (override via .env) |
| `same_bar_flip` | True |

**2y backtest (BTCUSDT 1m, 2024-07-11 to 2026-07-11):**
- Historical figures from the original two-year sweep are superseded by the
  corrected four-year replay in `runs/four-year-validation.md`. Do not use
  archived pre-four-year reports for sizing: they understated sat-denominated
  trading fees when expressed in USD.
- **Aggregate: +$2,157 before funding**
- Win rate 39.9%, max DD 38.9%.

### Optional 4h CHOP entry-size reduction

The CHOP overlay is intentionally narrow: on a completed 4h bar only, when
CHOP(14) is above 61.8, the next **new 4h entry** requests half the usual
notional. It applies in both `fixed_notional` and `equity_fraction` sizing
modes, before the normal hard caps and whole-contract rounding. It does not
change 1d, exits, leverage, MA/cool-off state, or an already-open position.
The signal metadata records the CHOP value, regime, and applied size multiplier
for audit.

Enable the studied setting explicitly in `/etc/lnmbot/env`:

```dotenv
STRATEGY_4H_CHOP_REDUCE_ENABLED=true
STRATEGY_CHOP_LOOKBACK=14
STRATEGY_CHOP_HIGH_THRESHOLD=61.8
STRATEGY_CHOP_HIGH_SIZE_MULTIPLIER=0.5
```

Then restart the service. A restart cannot resize an existing trade; the change
applies only to later 4h entries. See `runs/choppiness-regime-verdict.md` for
the research result and limits of the evidence.

## Code that's ready

- ✓ `MaCross` strategy with per-TF cool-off, same-bar-flip bug fix
- ✓ `LnmLiveStream` — polls LNM REST for the latest 1m candle
- ✓ `LiveExecutor` — calls LNM REST for real orders, fake-API testable
- ✓ `RiskGuard` — per-TF sizing, daily loss, rate limit, exit-passes-through
- ✓ `KillSwitch` — env var + halt file
- ✓ `run_live.py` — deployment runner
- ✓ `lnm-bot.service` — systemd unit file
- ✓ 35 passing tests including full live integration test with FakeTradesApi

## What you need to do

### 1. Testnet first (required)

Start with a testnet-only environment file. The service and runner are
**observe-only by default**: they fetch and aggregate candles but use the paper
executor, so no order request is sent.

```bash
# /etc/lnm-bot/env-testnet
LNM_NETWORK=testnet
RISK_MAX_POSITION_USD=10
RISK_MAX_LEVERAGE=2
```

Run a bounded observation session and confirm that bars, warmup, and signals
are recorded:

```bash
uv run python scripts/run_live.py --env /etc/lnm-bot/env-testnet --max-runtime 120
```

For a meaningful strategy observation period, leave `--allow-orders` absent
and run the service continuously for at least one week. The 4h and 1d
timeframes can legitimately produce no transition in a short interactive run.

Only after that succeeds, add testnet API credentials with isolated-futures
permissions and execute one deliberately small open/close cycle:

```bash
uv run python scripts/run_live.py --env /etc/lnm-bot/env-testnet --max-runtime 900 --allow-orders
```

The runner reconciles any existing isolated trade IDs at startup. If it finds a
remote trade it cannot map to a local timeframe, it aborts rather than opening
another trade.

### Mainnet-only smoke test

If testnet is unavailable, do not begin with the strategy runner. Fund the
mainnet account with enough sats for fees and use the dedicated one-contract
smoke test first. It checks account access and refuses to act when any
isolated trade is already open. `--execute` opens exactly USD 1 notional at
1x and immediately closes that same trade.

```bash
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/env
uv run python scripts/smoke_isolated_trade.py --env /etc/lnmbot/env \
  --execute --confirm-mainnet
```

### 2. Set up optiplex (5-10 min)
```bash
# SSH into optiplex
ssh optiplex

# Install uv if not already there
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repo
git clone <your-fork-or-this-repo> /opt/lnm-bot
cd /opt/lnm-bot
uv sync --extra dev --extra backfill

# Create the .env (NEVER commit this)
sudo mkdir -p /etc/lnm-bot
sudo cp .env.example /etc/lnm-bot/env
sudo chmod 600 /etc/lnm-bot/env
sudo chown -R lnm:lnm /etc/lnm-bot
# Edit /etc/lnm-bot/env with signet credentials first:
#   LNM_NETWORK=testnet
#   # Leave LNM_BASE_URL and LNM_WS_URL unset: the application selects
#   # https://api.signet.lnmarkets.com/v3 and wss://stream.signet.lnmarkets.com/v1.
#   LNM_ACCESS_KEY=...
#   LNM_ACCESS_SECRET=...
#   LNM_ACCESS_PASSPHRASE=...
# Set RISK_MAX_POSITION_USD=10 for the initial testnet smoke test
```

### 3. Test the connection (1 min)
```bash
uv run /opt/lnm-bot/.venv/bin/python -c "
import asyncio
from lnmarkets_bot.config import load_config
from lnmarkets_bot.api.client import LnmRestClient
cfg = load_config(env_file='/etc/lnm-bot/env')
client = LnmRestClient(base_url=cfg.effective_base_url(),
                      access_key=cfg.lnm_access_key,
                      access_secret=cfg.lnm_access_secret,
                      access_passphrase=cfg.lnm_access_passphrase,
                      authed=True)
async def main():
    bal = await client.get('/account')
    print('balance:', bal)
    await client.aclose()
asyncio.run(main())
"
```

### 4. Install the systemd service (2 min)
```bash
sudo cp /opt/lnm-bot/scripts/lnm-bot.service /etc/systemd/system/lnm-bot.service
sudo systemctl daemon-reload
sudo systemctl enable lnm-bot
sudo systemctl start lnm-bot

# Watch logs
journalctl -u lnm-bot -f
```

### 5. Verify the first 24h
- Check `/var/lib/lnm-bot/lnmarkets.sqlite` for the recorded run
- Verify the observe-only run records closed 1m candles and warmup without orders
- Verify the isolated testnet order appears in the LNM UI only after explicitly using `--allow-orders`
- Confirm the kill switch works: `TRADINGBOT_HALTED=1 /etc/lnm-bot/env` or `touch /etc/lnm-bot/HALT` then `systemctl restart lnm-bot`

### Local read-only dashboard

Run the dashboard on the bot host against the same SQLite database as the
service. It binds to loopback only by default, has no write routes, and
refreshes every 30 seconds.

```bash
cd /home/james/srv/tradingbot
uv run python scripts/run_dashboard.py --db /var/lib/lnm-bot/lnmarkets.sqlite
```

Open `http://127.0.0.1:8080` on the host. From another machine, use an SSH
tunnel rather than exposing the dashboard publicly:

```bash
ssh -L 8080:127.0.0.1:8080 optiplex
```

It shows the most recent runs, emitted signals, recorded orders, and funding
settlements. Funding is shown as the raw signed satoshi amount returned by the
LN Markets API; it is deliberately separate from realised trade P&L.

#### Run the dashboard as a service

The dashboard reads `/var/lib/lnmbot/lnmarkets.sqlite` through SQLite's
read-only mode and binds only to `127.0.0.1:8080`. To show authoritative
**Available** balance and **Total equity**, give it a *separate* LN Markets
API key restricted to **Read** permission. It never receives the bot's
trading credential and the browser never receives either credential.

```bash
sudo install -d -m 700 /etc/lnmbot
sudo install -m 600 scripts/lnmbot-dashboard.env.example /etc/lnmbot/.env.dashboard
sudoedit /etc/lnmbot/.env.dashboard
sudo install -m 644 scripts/lnmbot-dashboard.service /etc/systemd/system/lnmbot-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable --now lnmbot-dashboard

systemctl status lnmbot-dashboard
curl http://127.0.0.1:8080/healthz
```

Use `journalctl -u lnmbot-dashboard -f` for dashboard diagnostics. It is safe
to restart this service independently while the trading service is running.

### Fast 5m execution-observation profile

The production profile is the locked 1d/4h strategy. To exercise the same MA
cross, verdict-transition, risk-guard, persistence, and isolated-execution
path more frequently, use the explicitly opt-in 5m profile in a separate
interactive process:

```bash
cd ~/srv/tradingbot
uv run python scripts/run_live.py --env /etc/lnmbot/env --test-5m
```

It remains observe-only unless `--allow-orders` is supplied. The 5m profile is
not a performance-tested replacement for the 1d/4h strategy: it maps the 4h
cool-off values onto 5m solely to test operational behaviour faster. It also
uses a 0.01% tolerance rather than the production 0.5%, because the latter is
too wide for ordinary 5m price movement. Neither test-profile value changes
the locked production strategy.

While the production service remains running, give the 5m paper process its
own database to avoid two processes competing for SQLite writes. Run a second
dashboard against it:

```bash
mkdir -p ~/srv/tradingbot/runs
STORAGE_DB_PATH="$HOME/srv/tradingbot/runs/test-5m.sqlite" \
  uv run python scripts/run_live.py --env /etc/lnmbot/env --test-5m
uv run python scripts/run_dashboard.py \
  --db "$HOME/srv/tradingbot/runs/test-5m.sqlite" --port 8081
```

If, after paper observation, you deliberately use it for one-contract
mainnet execution testing, stop the production service first and retain the
reduced hard limits. A 5m trade may stay open; it must be closed before
returning to the production service.

```bash
sudo systemctl stop lnmbot
uv run python scripts/run_live.py --env /etc/lnmbot/env --test-5m \
  --allow-orders --confirm-mainnet --confirm-test-profile
```

Do not install the 5m profile as the systemd service and do not run its
real-order mode in parallel with the production service. Restart `lnmbot`
only after the 5m isolated trade list is empty.

To exercise the cool-off state machine after the standard 5m execution test,
add `--test-5m-cooldown-probe`. It uses test-only tiny winner/loss thresholds
and suppresses the next two verdict transitions after a close. This is not a
production calibration; it is a way to verify the recorded `cool_off` no-ops,
counter depletion, and later resumption of normal entries.

```bash
uv run python scripts/run_live.py --env /etc/lnmbot/env --test-5m \
  --test-5m-cooldown-probe --allow-orders \
  --confirm-mainnet --confirm-test-profile
```

## Cool-off rule (locked after 2y comparison)

After a per-timeframe winner reaches the configured threshold (1D: 3%; 4H:
5%), the strategy suppresses the next per-timeframe verdict transitions (1D:
12; 4H: 11). A verdict transition includes `UP_TRUE`, `DOWN_TRUE`, and `FLAT`.
Therefore transitions into or out of the moving-average band consume a slot.

This differs from the originally intended rule, which would suppress only the
next N directional entry/reversal opportunities. On the locked 2-year fixture,
the all-verdict rule produced +$1,809 with 38.9% maximum drawdown, versus
+$1,588 and 39.3% drawdown for directional-only suppression. The default is
kept as `cooldown_mode=verdict_transition` to preserve the higher-performing
tested rule.

## Loss cool-off rule (locked after temporal holdout)

After a per-timeframe realized loss reaches the configured magnitude (1D: 5%;
4H: 2%), the strategy independently suppresses the next verdict transitions
(1D: 3; 4H: 4). This uses the same `verdict_transition` definition as the
winner rule, including transitions into and out of `FLAT`.

The loss configuration was selected only from the first year of the fixture
with the winner rule fixed, then evaluated once on the untouched second year.
On that holdout it produced +$842 versus +$748 for the winner-only baseline,
with 34.77% versus 34.91% maximum drawdown and 81 versus 98 closed trades.
The longer maximum loss streak (14 versus 12) was not used as a rejection
criterion because sizing is fixed and both P&L and drawdown improved.

## Important caveats

### Live sizing and risk controls

`run_live.py` now passes explicit sizing settings into the locked strategy;
it no longer relies on a hidden $1,000 strategy request being clamped by a
risk limit. The default is deliberately conservative:

```bash
SIZING_MODE=fixed_notional
SIZING_FIXED_NOTIONAL_USD=1
SIZING_LEVERAGE=1
```

For balance-based sizing, use `SIZING_MODE=equity_fraction`. Immediately
before an entry the bot fetches `/account`, converts its reported satoshi
balance using the latest BTC/USD close, applies `SIZING_EQUITY_HAIRCUT`, and
allocates `SIZING_TOTAL_MARGIN_FRACTION` by `SIZING_TIMEFRAME_WEIGHTS`.
`RISK_MAX_POSITION_USD`, `RISK_MAX_LEVERAGE`, and the optional aggregate
`RISK_MAX_TOTAL_NOTIONAL_USD` / `RISK_MAX_TOTAL_MARGIN_USD` remain independent
hard ceilings.

Set both aggregate ceilings in live operation. They constrain combined 1d and
4h exposure even if a sizing setting is later changed. The daily-loss guard is
also restored from bot-recorded realized P&L and funding after a service
restart; it is an entry circuit breaker, not an exchange-side stop-loss.

Do not set balance-based sizing or lift the reduced hard caps during the
observation period. A configuration change requires a service restart and is
recorded in the run metadata.

### Isolated-margin execution model
Each timeframe owns an independent LN Markets isolated trade. The strategy's
per-TF state (`state.positions["1d"]`, `state.positions["4h"]`) maps to the
corresponding LNM trade ID held by `LiveExecutor`. An exit closes only that
timeframe's trade; a 1d signal must never close or resize the 4h trade.

The executor's trade-ID map is process-local. If the process is restarted while
trades are open, reconcile the running isolated trades before enabling new
signals; automatic timeframe assignment is not currently possible unless the
LNM trade metadata includes the timeframe.

After warmup, a restored position that is opposite a confirmed directional
verdict is closed with a `restart_catch_up` signal. The bot deliberately does
not open the opposite trade until a new transition occurs: a missed entry from
downtime is stale, while leaving a position on the wrong side is unsafe.

### Funding collection
For real isolated trades, the executor polls isolated funding history every
15 minutes and again immediately before closing a position. It records each
settlement once, keyed by its LNM settlement and trade IDs, and exposes the
raw signed satoshi amount in the dashboard. Funding is intentionally reported
separately from realised trade P&L rather than silently folding an API sign
convention into it.

### Trading fees and net P&L
The executor records LN Markets' actual `openingFee` and `closingFee` from the
isolated-trade responses as fills. Daily realised P&L is net of those fees;
funding remains a separate signed field, so dashboard totals can show both
trade net P&L and funding explicitly. LN Markets reports a paid funding fee
as positive and received funding as negative; the bot stores that raw value
but presents its inverse as funding P&L (positive means received). The API's `pl` is treated as gross
trade P&L and the closing fee is deducted once.

### Dashboard account snapshots
On a credentialed live run, the bot records a read-only LN Markets account
snapshot every 15 minutes. This is retained as a local fallback. When the
dashboard has its own read-only key in `/etc/lnmbot/.env.dashboard`, it also
refreshes `/account` and the running isolated trades directly. It presents
`available + isolated initial margin + isolated running P/L` as Total equity,
matching the LN Markets interface; it still cannot place or alter orders.

### Poll failure health
Transient polling failures retain the last delivered candle timestamp and the
next successful poll catches up from that point. Three consecutive failures
emit an error-level `live.poll_failure_alert` journal event; recovery emits
`live.poll_recovered` with the number of failed polls.

### Slippage on market orders
Live market orders fill at the actual LNM price at the moment of execution, not the bar close we saw. With 5-bps default slippage in the executor, the live P&L will differ slightly from the backtest. The live executor uses real LNM fills, so the slippage assumption is moot in production — `entry_price_usd` in the position state will be the actual fill price.

### WebSocket
The `LnmLiveStream` polls REST every 10s (default). Lower latency (~100ms) would require implementing the LNM WebSocket. v1 uses polling which is fine for the 1m cadence strategy.

## Future work (when you want to)

| Item | Why you'd do it | Effort |
|---|---|---|
| External bot heartbeat / VPS monitoring | Alert on host, VPN, or stale-data failure | ~1–2 hours |
| Implement LNM WebSocket | Lower-latency candle ingestion | ~3-4 hours |
| Add position stop-losses | The user said skip for v1; revisit if max DD matters | ~2-3 hours |
| Add alerts (Discord/Telegram) | Get notified when trades fire | ~2 hours |

See [docs/dashboard-roadmap.md](docs/dashboard-roadmap.md) for the deliberate
read-only dashboard expansion plan.

## Files to know

| Path | What |
|---|---|
| `src/lnmarkets_bot/strategy/ma_cross.py` | The strategy (v1.3 defaults) |
| `src/lnmarkets_bot/engine/live_executor.py` | Real LNM trade calls |
| `src/lnmarkets_bot/data/live.py` | LNM REST polling (1m candles) |
| `src/lnmarkets_bot/engine/live.py` | `run_paper` engine loop, now async-aware |
| `src/lnmarkets_bot/risk/guard.py` | Per-tf risk limits, exit-passes-through |
| `src/lnmarkets_bot/control/kill.py` | Halt switch (env var or file) |
| `scripts/run_live.py` | Deployment runner |
| `scripts/lnm-bot.service` | systemd unit |
| `runs/v13_trades_2y.md` | 2y trade log for review |
