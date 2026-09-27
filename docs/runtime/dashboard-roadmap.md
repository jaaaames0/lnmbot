# Dashboard roadmap

The dashboard is the bot's local, read-only monitoring surface. Configuration
and lifecycle control remain in `/etc/lnmbot/.env` and systemd so that enabling
orders or changing risk limits always requires an explicit reviewed restart.

## Current

- Operational overview: MA positions on 1d/4h, the breakout campaign and its
  K-unit stack, live BTC/USD context, funding, rolling P&L, and separate
  strategy/account statistics.
- Timeframe-filtered signals and grouped isolated-trade history, including
  funding and net P&L.
- Funding, P&L, run-history, and health pages, with the active run's sizing,
  risk, CHOP, tolerance, and cooldown configuration.
- Actual P&L remains the accounting view; the P&L page also provides a fixed
  $100 constant-notional replay and per-trade return-on-notional so
  strategy quality can be compared across sizing and deposit changes.
- A separate read-only LN Markets key supplies authoritative available balance,
  isolated margin, running P&L, and account cash-flow history. The dashboard
  has no write capability.
- A lightweight 10-second in-place refresh updates the dashboard without a
  full-page reload.
- A public LN Markets last-price WebSocket keeps the BTC/USD ticker live; it
  is presentation-only and falls back to the recorded 1-minute candle price.

The dashboard is available on the operator's LAN and remains read-only. It
must never expose API credentials or write directly to the trading database.

## Deployed revision (24 September 2026)

- Group the two MA timeframes, breakout campaign, and shared wallet in the
  overview. Keep a single recent-signals timeline with strategy, slot, event,
  detail, and live/decision/replay source; group funded campaign exits for
  display while retaining every unit-level signal and order in SQLite.
- Show current execution alignment from the saved MA/breakout states and a
  fresh read-only venue snapshot. Report pending actions, mismatches, and
  unavailable or stale venue data separately; do not infer alignment from an
  old restart log row.
- Stop emitting `restart_state_aligned` as a strategy signal and hide historical
  rows of that kind. Future MA cooldown signals carry their original total so
  the display can say, for example, `winner 2/12 · Up → Flat`. Older rows
  without a saved total continue to show the remaining count.

The dashboard runs from `prod-20260924.1-g9d121c6dcc24`; the integrated trader
runs from `prod-20260924.2-g9d121c6dcc24`. The clean source commit is preserved
as a Git bundle in both protected rollback checkpoints. The dashboard pages and
health endpoint returned HTTP 200, both funded strategy snapshots restored,
the MA cooldown and historical breakout campaign remained unchanged, execution
reported aligned, and the order journal stayed at 40 through restart. The
required encrypted backup completed successfully.

## Deferred stretch goals

1. **External uptime monitoring — priority.** The dashboard exposes a minimal
   `/healthz` endpoint. Run an Uptime Kuma monitor on the VPS over the VPN;
   alert on host, VPN, dashboard, or service failure. As capital or operational
   reliance increases, add an outbound bot heartbeat that includes process
   health and market-data freshness, detecting a live-but-stalled bot too.
2. **Safety visibility.** Surface kill-switch state, clamps/rejections,
   daily-loss state, and reconciliation failures prominently in the dashboard.
3. **Read-only alerts.** Send a Discord or Telegram notification when a trade
   opens/closes or the bot enters a halted/stale state.
4. **Emergency halt control.** One deliberate write control could create the
   existing halt file; it must never enable orders or alter sizing, leverage,
   or strategy parameters.
