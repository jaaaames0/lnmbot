# LN Markets bot

An isolated-margin BTC/USD futures bot with a read-only operations dashboard.
The live runner always operates the MA-cross strategy independently on `4h`
and `1d`. It can also operate a daily close-range breakout campaign with
separately owned units in the same funded account, and an impulse-range
strategy that trades post-breakout consolidations, either as an
order-incapable shadow book or with one funded slot. The breakout strategy is
disabled in the example configuration; enabling it requires an order-enabled
run. Historical breakout and shadow results remain separate from funded P&L.

The runner polls completed LN Markets one-minute candles and forms strategy
bars locally. The dashboard's public price ticker is for display only. SQLite
holds the local audit trail and restart state; systemd runs the trader and
dashboard as separate services.

## Operating safeguards

- `scripts/run_live.py` is observe-only unless `--allow-orders` is supplied.
  Mainnet orders also require `--confirm-mainnet`. The checked-in trader unit
  has neither flag; a deliberate override is needed for funded operation.
- The MA timeframes and funded breakout units have distinct position owners.
  Startup reconciles local ownership with venue inventory and blocks new
  admission when an untracked or ambiguous remote trade needs attention.
- Risk caps and available cash constrain entries. Known exits can continue
  when entry admission is blocked; the halt switch stops processing and does
  not automatically close positions.
- `HALTED=1` or the configured `HALT_FILE` stops the runner. The dashboard
  cannot place orders or change configuration and should use a separate LN
  Markets key with **Read** permission only.

These controls cannot prevent market loss, venue failure, or liquidation.

## Run and monitor

The supplied templates are `scripts/lnmbot.service` and
`scripts/lnmbot-dashboard.service`. Render their release-directory placeholders
before installation; [DEPLOYMENT.md](DEPLOYMENT.md) covers the env files,
service setup, recovery, and the current strategy options.

```bash
systemctl status lnmbot lnmbot-dashboard
journalctl -u lnmbot -f
curl http://127.0.0.1:8082/healthz
```

The example dashboard unit binds port `8082` on all interfaces. Restrict it
with the host firewall or change the unit to loopback. For a loopback listener,
use `ssh -L 8082:127.0.0.1:8082 <bot-host>` and open
`http://127.0.0.1:8082` locally. The dashboard shows account context, strategy
positions, signals, funding, P&L, run configuration, and health. The top bar
on every page carries, left to right, execution state, price, funded net P&L
with its window toggle, and equity.
Overview gives each strategy a card with its current state, a positions table
listing every fundable slot (flat or open, so its shape never changes) and the
latest signals; MA slots read 4h then 1d. Historical campaigns and shadow trades stay out of funded
positions. The sidebar links straight to `/strategies/ma`, `/strategies/breakout`
and `/strategies/range`; each shows that strategy's state, its last five signals
and events (linking to the Signals page filtered to that strategy), its slice of
the run configuration as full-width rows, and how it trades.
Health shows the active run, readiness, account-wide settings and hard risk
limits. `/strategies` and `/runs` redirect to Overview and Health.

A *signal* is a decision to change exposure: an entry, exit or breakout
parent/add-on decision, with its outcome (filled, rejected, blocked, suppressed
by cooldown or model-only). A directional verdict that a cooldown suppressed is
still a signal; moves to Flat, no-ops, restart alignment, range lifecycle and
control changes are *events*, listed on the owning strategy's page. The
Signals page lists signals only until "Show non-op events" is ticked, which
merges events into the same table.

The P&L page shows rolling and calendar account P&L, net P&L per strategy, and
trade-quality and risk tables with a row for every strategy and breakout unit
(k0–k3), traded or not. *Return on margin* divides each trade's net P&L by the
isolated margin it posted and compounds it per slot: the return of an account
holding only the margin it needed, unaffected by deposits or idle balance.
Groups weight slots by average margin. After 30 days a strategy also shows an
extrapolated CAGR, which assumes the period so far repeats and is not a
forecast.

The Capital page puts all sizing in one place. For every slot it shows the next
entry's notional, leverage and posted margin at current equity, including the
4h CHOP multiplier, the range taper and any clip by `RISK_MAX_POSITION_USD`.
Isolated margin is the most a position can lose, so the worst case is every slot
open and liquidated together; an observed case applies each strategy's worst
margin-only drawdown so far (100% without history). A planner turns a risk
budget and a MA / breakout / range mix into suggested `SIZING_*` and unit
notional settings, and works back from desired sizes to the equity they need.
It only suggests values; changing them is a trader configuration change.

`/charts` overlays strategy levels on recorded LN Markets candles over a 90-day
window: MA on its 1d or 4h decision candles, breakout and range on 4h. A
fixed-size levels panel beside the chart and an events table below it follow
the hovered candle; a compact strip summarizes the saved state. It uses native Canvas, local assets
and no JavaScript build step or chart price API. All three strategies contribute
to the same read-only chart interface. Markers sit on the candle that caused
them: ▲/▼ long/short entries, ● exits, hollow symbols for shadow or historical
model trades, ◆ model events, ○ intents; routine diagnostics are off by default.
MA averages are recomputed from recorded candles for display. Range formation,
channel and expansion levels are replayed from the retained machine events over
recorded 4h candles; the saved snapshot defines current levels. Historical
breakout recovery peaks are not a recorded series. The chart is a visual aid,
not an audit, and makes no trader-side changes.

Changing sizing or direction settings requires editing the trader env file
and restarting its service. Existing venue positions are reconciled, not
resized. Check the dashboard, venue inventory, and journal after restart.

## Development

Install the project and run the default local suite:

```bash
uv sync --extra dev
uv run pytest -q
```

The default suite uses synthetic data, temporary databases, and fake venue
APIs. Historical strategy investigations under the local, Git-ignored `docs/`,
`scripts/research/`, and `tests/research/` archives are outside normal test
collection. See [CHANGELOG.md](CHANGELOG.md) for production and dashboard
history.

The local [quarterly strategy audit protocol](docs/research/2026-10-01-strategy-review-protocol/README.md)
defines repeatable checks of execution, profitability expectations, market
change and forward challengers, with comprehensive evidence and action notes.
Reviews recommend changes through explicit gates; they do not automatically
retune funded strategies after a losing quarter. The protocol and audit
artifacts live in the Git-ignored local research archive; no timer is installed.

Funded range recovery retains an owned close obligation until venue flatness,
including restart during submission. Range construction rule version 3 is the
tested research rule: the 40% width cap ends an expanding channel, while a
channel may confirm wider. In such a channel the isolated margin, not the
close-based stop, can bound a loss; a liquidation blocks re-entry for that 4h bar.
Changing the chop filter recomputes eligibility from the saved confirmation ER.

Missing minute evidence is carried on aggregated candles. Each affected owner
blocks admission durably and freezes unverified timeframe decisions; available
minutes still drive inventory reconciliation, funding and owed close retries.
The range retains its last verified owned target and time-based expiry. A
restored owner ignores gaps wholly before its committed state. Recovery requires
verified replay or reconstruction; later complete candles alone do not repair a
missing indicator chain. `/healthz` is dashboard liveness; `/readyz` checks the
active trader, fresh feed, current owner snapshots, commands and venue inventory.
`LIVE_ENTRIES_ENABLED=false` is an admission-only compatible recovery setting.
