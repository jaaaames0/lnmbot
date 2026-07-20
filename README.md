# LN Markets MA-cross bot

An isolated-margin BTC/USD futures bot for LN Markets.  It runs one locked
moving-average strategy independently on the `4h` and `1d` timeframes: each
timeframe owns at most one isolated trade, so an action on one never closes,
resizes, or otherwise changes the other.

The intended operating model is deliberately boring:

```text
deployment env file   reviewed trading configuration
systemd               one always-on strategy process
SQLite                local audit trail
dashboard             loopback-only, read-only monitoring
journalctl            diagnostics and incident evidence
```

The bot acts only on completed one-minute candles.  Those are aggregated into
the two production timeframes; the dashboard's price ticker is separate and
presentation-only.

## Safety properties

- `run_live.py` is observe-only unless `--allow-orders` is supplied.  Mainnet
  execution additionally requires `--confirm-mainnet`.
- The systemd template in this repository is intentionally observe-only.  A
  deliberate service override is required to enable production orders.
- Each entry, exit, signal, funding settlement, and run is recorded locally.
- Real isolated positions are reconciled on startup.  An untracked or
  ambiguously mapped remote trade causes live startup to fail closed rather
  than risk duplicate exposure.
- A same-direction entry is idempotent.  If an entry response is ambiguous,
  the executor checks remote running trades and stops rather than blindly
  retrying.
- `HALTED=1` or the presence of `HALT_FILE` prevents new processing.
- The dashboard is read-only, binds to loopback, and should use a separate LN
  Markets API key with **Read** permission only.

This is risk-control infrastructure, not a guarantee against market loss,
exchange failure, liquidation, or operational mistakes.

## Everyday operation

The included service templates use the following names.  Choose installation
paths and a service account appropriate to the host; the full adaptation steps
are in [DEPLOYMENT.md](DEPLOYMENT.md).

| Item | Value |
|---|---|
| Trading service | `lnmbot.service` |
| Dashboard service | `lnmbot-dashboard.service` |
| Trading configuration | a root-owned env file outside the repository |
| Dashboard credentials | a separate root-owned read-only env file |
| Database and halt file | a service-writable state directory |

Monitor the bot:

```bash
systemctl status lnmbot
journalctl -u lnmbot -f
```

Monitor the dashboard:

```bash
systemctl status lnmbot-dashboard
journalctl -u lnmbot-dashboard -f
```

The dashboard is served on the loopback host and port configured in its
systemd unit (the template uses `127.0.0.1:8080`).  From another machine, use
an SSH tunnel rather than exposing it publicly:

```bash
ssh -L 8080:127.0.0.1:8080 <bot-host>
```

Then open `http://127.0.0.1:8080` locally.  It shows combined account context
plus timeframe-specific signals, positions, trade history, funding, P&L,
active configuration, and health.  It cannot enable trading or change sizing.

For configuration changes, edit the trading environment file, restart the
service, then verify the new run and configuration in the dashboard and
journal:

```bash
sudo systemctl restart lnmbot
```

A restart reconciles any running isolated trades.  It cannot resize an already
open trade; new size/leverage settings apply to later entries.

## Sizing in brief

`SIZING_MODE=fixed_notional` requests
`SIZING_FIXED_NOTIONAL_USD` whole USD contracts per new timeframe entry at
`SIZING_LEVERAGE`.  `RISK_*` settings remain independent hard ceilings.

`SIZING_MODE=equity_fraction` reads the account before each real entry,
applies its haircut and margin allocation, then converts the result to whole
contracts.  It is still capped by every applicable `RISK_*` setting.

See [DEPLOYMENT.md](DEPLOYMENT.md) for the complete configuration reference,
safe installation, smoke tests, recovery procedure, and production service
setup.  The deliberately read-only dashboard scope and deferred ideas live in
[docs/dashboard-roadmap.md](docs/dashboard-roadmap.md).
