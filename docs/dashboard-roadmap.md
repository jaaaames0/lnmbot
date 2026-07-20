# Dashboard roadmap

The dashboard is the bot's local, read-only monitoring surface. Configuration
and lifecycle control remain in `/etc/lnmbot/.env` and systemd so that enabling
orders or changing risk limits always requires an explicit reviewed restart.

## Current

- Operational overview: active isolated positions, latest 1d/4h signals,
  live BTC/USD context, funding, rolling P&L, and strategy/account statistics.
- Timeframe-filtered signals and grouped isolated-trade history, including
  funding and net P&L.
- Funding, P&L, run-history, and health pages, with the active run's sizing,
  risk, CHOP, tolerance, and cooldown configuration.
- A separate read-only LN Markets key supplies authoritative available balance,
  isolated margin, running P&L, and account cash-flow history. The dashboard
  has no write capability.
- A lightweight 10-second in-place refresh updates the dashboard without a
  full-page reload.
- A public LN Markets last-price WebSocket keeps the BTC/USD ticker live; it
  is presentation-only and falls back to the recorded 1-minute candle price.

The dashboard remains loopback-only unless deliberately accessed through an
SSH tunnel. It must never expose API credentials or write directly to the
trading database.

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
