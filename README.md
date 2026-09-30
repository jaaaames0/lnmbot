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
positions, signals, funding, P&L, run configuration, and health.

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
