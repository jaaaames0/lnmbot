# Changelog

This file records operationally meaningful changes to the live bot. Dates are
UTC and entries describe deployed behaviour rather than every internal refactor.

## Unreleased

### Dashboard operational context

- Add fixed-notional P&L replay, return-on-notional, cool-off visibility, and
  a plain-language explanation of the active strategy configuration.
- Keep actual P&L as the accounting view while allowing strategy quality to be
  compared across sizing and deposit changes.

### Service and source hygiene

- Avoid redundant database initialization when the live runner supplies its
  already-initialized recorder.
- Separate editable source from immutable trader and dashboard releases, and
  remove retired `~/srv/tradingbot` paths from current templates and tests.
- Correct the project copyright holder name.

### Restart-safe indicator and execution recovery

- Bootstrap a new live strategy from 100 days of LN Markets 1-minute candles,
  aggregated locally, so the daily EMA(21) closely matches a continuously
  calculated EMA.
- Persist and restore indicator history, SMA/EMA values, verdicts, cooldowns,
  manual holds, and unconfirmed execution targets. Snapshot parameters are
  normalised through JSON so valid snapshots are not rejected on restart.
- Save recovery state before and after order handling. Restarts now reconcile a
  restored position on the first live minute, rather than waiting for another
  4h or 1d boundary.
- Prevent an unchanged directional verdict from creating a late entry after a
  cold restart. It may retry only a durable target from an order that was
  emitted but not confirmed by the executor.
- Keep cooldown semantics intact on restart: cooldowns suppress replacement
  entries but never an exposure-reducing close.
- Use LN Markets' actual entry price for live position state, recorded orders,
  fills, and fee conversion. A remote close that succeeds but cannot be
  persisted locally fails closed instead of being retried.
- Sort delayed candle responses before advancing the live cursor, and apply
  the 4h CHOP size metadata to short entries as well as longs.
- Add an idempotent manual-close recovery utility which can restore a manual
  timeframe hold without duplicating close accounting.
- Make the dashboard prefer the persisted live SMA/EMA state over a separate
  fallback calculation.

## 2026-07-27 — Live execution recovery and equity sizing

Commit: `6dccf36` (`Harden live execution recovery and equity sizing`)

- Added retry handling for failed exposure-reducing isolated closes and made
  funding persistence best-effort rather than an execution prerequisite.
- Reconciled restored isolated-trade timestamps as UTC-aware values.
- Used account equity, including isolated margin and unrealised P/L, for
  equity-fraction sizing; retained hard risk caps and rejected entries when
  sizing data is unavailable.
- Improved dashboard selection of the active live run around manual-recovery
  audit records.

## 2026-07-26 — Documentation and operations refresh

Commits: `3f836ed`, `e97ab63` (`update documentation`)

- Consolidated the README and deployment guide around the isolated-margin
  production model, service setup, safety controls, dashboard operation, and
  configuration reference.
