# Changelog

Dates are UTC. Dated entries record operational releases; `Unreleased` records
source changes that have not been installed as a production release.

## Unreleased

### Historical funding admission

- Keep the historical breakout model at its last verified point when funding is
  late. Give the daily decision a brief grace period, then retry in the
  background. Replay missed price bars
  in order after the settlement arrives, including across restarts, before
  admitting future breakout entries. Expire missed entry signals instead of
  placing late orders. Missing price evidence still requires reconstruction;
  MA trading and funded exposure management continue. The dashboard distinguishes
  a retrying funding delay from a model that needs operator attention.

### Repository and documentation

- Keep local research and operations evidence under the Git-ignored `docs/`
  archive. The README and deployment guide now describe the funded breakout
  path and current service templates directly.
- Make the default test suite self-contained and exclude archived research
  checks that require local market data. The default suite runs in about 30
  seconds; it is not a live-venue or profitability test.

## 2026-09-27 — Funded multi-strategy remediation accepted

Production tag: `production/prod-20260927.2-gc0cb72dac281`; source commits
`be69aae` and `c0cb72d`.

- The MA timeframes and funded daily close-range campaign now use distinct,
  durable position ownership and shared-wallet cash admission. Known exits
  continue when new entries are blocked. Breakout campaigns keep their own
  parent and unit state; disabling breakout prevents new entries while allowing
  funded units already open to reconcile and exit.
- Repair entry, close, funding, fee, and daily-loss accounting across retries,
  partial closes, and restarts. Venue inventory and available cash remain
  authoritative; historical shadow trades stay outside funded accounting.
- Normalize real LN Markets trade timestamps at the API boundary. Classify
  external MA closures as confirmed manual, confirmed liquidation, or unknown;
  unknown causes remain unclassified while starting the loss cooldown.
- Include the eight-hour funding settlement at a venue endpoint's exclusive
  upper bound before the modeled price observation. Add a dry-run-first
  operator utility for evidence-backed external-close classification.
- The September 27 cutover used a consistent database backup and a guarded,
  evidence-bound ledger repair, with no diagnostic or startup order. At
  acceptance the venue had no running or pending trades, the order journal
  remained at 40, and the repaired owned ledger held 351 events and 325,969
  net sats. The trader and dashboard passed health and snapshot checks; the
  encrypted backup completed. These are dated acceptance facts, not current
  account balances or evidence of future profitability.

The release did not exercise real partial fills or exchange outages. Its
funded results do not make the generic paper engine an inverse-contract,
shared-wallet simulator. The full acceptance transcript remains in the local,
Git-ignored operations archive.

## 2026-09-24 — Multi-strategy dashboard rollout

The dashboard used `prod-20260924.1-g9d121c6dcc24`; the integrated trader
used `prod-20260924.2-g9d121c6dcc24`.

- Group MA timeframes, the funded breakout campaign, and shared wallet in the
  overview. Keep a combined causal signal timeline and group campaign exits
  for display while retaining unit-level SQLite records.
- Compare saved strategy states with a fresh read-only venue snapshot for
  execution alignment. Show pending, mismatched, and unavailable states
  separately, and remove the obsolete `restart_state_aligned` signal.
- Show cooldown ordinal context and keep hypothetical seeded campaigns
  separate from funded positions and P&L.
- The deployed trader and dashboard revisions were accepted after service,
  snapshot, health, order-journal, and encrypted-backup checks. Deferred
  monitoring and dashboard safeguards are described in [DEPLOYMENT.md](DEPLOYMENT.md).

## 2026-08-31 — Dashboard and release hygiene

Commit: `a085624` (`Separate source from trader and dashboard releases`)

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

## 2026-07-29 — Restart-safe indicator and execution recovery

Commit: `33695fb` (`Harden restart state and EMA continuity`)

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
