# Changelog

## Unreleased — Dashboard chart fidelity, overview top bar and render cost

Source only; not deployed. Trading behavior, risk and the database schema are
unchanged.

- Charts: anchor markers to the candle that caused them instead of the next
  candle boundary; draw price-less events above their candle rather than at the
  chart top; direction-aware ▲/▼ entries and ● exits; separate execution, model,
  intent, diagnostic (off by default) and schedule layers; hide intents already
  shown as fills. Connect step levels, tag current levels on the price axis,
  keep distant levels from flattening candles, and only flag candles missing
  over 10% of their minutes.
- Range: replay formation (impulse extreme, swing, thresholds) and expansion
  (new extreme, redraw close, width cap) between retained events over recorded
  4h candles, and keep broken channel edges visible until redraw. Replayed
  values match the machine's recorded swing and redraw edges on the live copy.
- MA: recompute SMA20/EMA21 and thresholds across the whole window from recorded
  candles with an 80-candle warm-up, so partial candles no longer leave holes.
  On the live copy the final values match the saved indicators within 0.01%.
- Performance: aggregate chart candles in SQLite (90-day cold build about
  350 ms to 150 ms); content-addressed chart cache kept 120 s; browser polling
  15 s with requestAnimationFrame drawing. Bound the per-render market-context
  query to recent bar ids (page renders roughly 3–4× faster).
- Overview: funded net P&L and its window toggle move to the top bar on every
  page and keep the current page; the runner/feed/entry strip is removed (shown
  in the sidebar status, top bar Execution and Health).

## 2026-10-01 — Dashboard Overview and strategy charts

Dashboard-only release `prod-20261001.2-g7709dd300b89`, source `7709dd300b89`,
accepted at 02:24 UTC. Protected checkpoint:
`/data/security-backups/lnmbot-dashboard-charts-20261001T021734Z`. The trader
remains on `prod-20261001.1-g0b1650b10779`, PID 1411724, run 71; its unit,
configuration, 40 orders and 40 fills were unchanged. Readiness, desktop/mobile
LAN browser checks, the encrypted backup and infrastructure monitor passed
(**127 passes, zero warnings/failures**). Timed rollback was disarmed; the
preceding dashboard release remains available.

Validation: **424 tests passed**, scoped Ruff clean. Streaming candle
aggregation kept four simultaneous cold chart requests below the unchanged
160 MiB cap (about 140 MiB peak) and preserved chart values.

- Compact Overview with funded P&L, execution/feed/entry context, consistent
  strategy summaries, funded inventory and six recent activity rows. Collapse
  repeated cooldown activity and include range formation observations.
- Move full MA, campaign, channel and shadow-book detail to strategy pages.
- Add one native Canvas viewer and a bounded read-only JSON interface for MA,
  breakout and range levels on 1d/4h candles or one-day 1m inspection. Preserve
  zoom and layer choices during refresh; label timing, provenance and incomplete
  history, including chop-skipped channels and model-only campaign units.
- Add offline database-copy previews. Trading behavior, risk and the database
  schema are unchanged; only the dashboard and its readiness-probe paths moved.

## 2026-10-01 — Routine funding publication delay

Accepted trader and dashboard runtime: `prod-20261001.1-g0b1650b10779`, source
`0b1650b10779`, run 71, at 00:26 UTC. Protected checkpoint:
`/data/security-backups/lnmbot-remediation-20261001T002246Z`. Release
validation: **411 tests passed**, scoped Ruff clean, 3 import contracts kept.
Readiness passed before and after the encrypted backup; recovery was disarmed
last. Strategy state, settings (apart from release paths), 40 orders and zero
open trades were unchanged.

- Breakout historical funding: LN Markets publishes each 8h settlement two to
  three minutes late. Waits under 15 minutes now log as info, and `/readyz`
  lists them under `pending` instead of failing (previously three false
  readiness failures a day). A daily decision held by that wait now acts if
  funding arrives within ten minutes of the daily close, instead of being
  dropped as a missed entry; longer outages keep the never-late rule.

## 2026-10-01 — Range rule version 3 (tested width rule)

Accepted trader and dashboard runtime: `prod-20260930.4-gccc5170a16a1`, source
`ccc5170a16a1`, run 70, at 00:09 UTC. Protected checkpoint:
`/data/security-backups/lnmbot-remediation-20260930T233839Z`. Release
validation: **405 tests passed**, research parity 4/4, scoped Ruff clean, 3
import contracts kept. Readiness passed before and after the encrypted backup;
recovery was disarmed last. All owners restored, MA state identical, range
channel/detector unchanged (choppy, ineligible), 40 orders, zero open trades.

- Range rule version 3 restores the tested width rule: the 40% cap ends a
  channel that expands beyond it, but a channel may confirm wider (as in the
  research, after crash impulses). Version 1 and 2 snapshots restore unchanged.
  The research parity suite again matches trade for trade (filtered 132 trades,
  160.64%; unfiltered 188, 98.23%). An isolated liquidation that precedes the
  4h-close stop blocks range re-entry for the rest of that 4h bar.

## 2026-09-30 — Funded range audit remediation

Accepted trader and dashboard runtime: `prod-20260930.3-g9aac08e365ce`,
source `9aac08e365ce`, run 69. Protected checkpoint:
`/data/security-backups/lnmbot-remediation-20260930T150127Z`.
Release validation: **396 tests passed**, scoped Ruff clean, 3 import contracts
kept. Readiness 200, all owners restored, zero restarts/unresolved commands,
40 orders and unchanged P&L, zero running/pending venue inventory; encrypted
backup succeeded and recovery was disarmed. Funded range admissions and risk
policy were retained. Readiness monitoring is enabled. Independent fresh minute and native daily/4h
replays reproduce the migrated current channel/detector exactly, with complete
coverage of all 143,460 expected minute candles. Post-acceptance source checks
passed 397 default tests, plus the verified dry-run reconstruction regression.

- Persist and recover owned close obligations and submitted close commands;
  block admissions for any owned slot or external callback without a binding.
- Carry minute coverage into aggregate candles, perform bounded gap backfill,
  preserve reconciliation/owed closes during gaps and persist owner admission
  health until verified reconstruction. Warmup cannot submit funded orders.
- Range rule version 2 enforces initial width as well as expanding width;
  migrate legacy snapshots without losing owned exits. Recompute cached chop
  eligibility when policy changes.
- Correct the HTTP range filter and effective exits-only display; add `/readyz`
  with current-run owner/model/feed/command/venue checks and a monitoring probe.
- Add immutable candidate and compatible recovery deployment tooling, asserted
  acceptance after backup, role-based seed repointing and dry-run reconstruction
  / seed refresh. `LIVE_ENTRIES_ENABLED=false` blocks all new entries while
  retaining owned managers and the current database.
- Dashboard: show the configured range mode before the first range snapshot.
- Cached 2020–2026 4h research replay rejects three initial oversized setups.
  Filtered trades change 132 → 134; summed per-trade return 160.64% → 139.99%.
  Unfiltered trades change 188 → 186; return 98.23% → 76.68%. These are local
  research execution results, not live account returns or a new parity claim.

Dates are UTC. Dated entries record operational releases; `Unreleased` records
source changes that have not been installed as a production release.

## Unreleased

## 2026-09-30 — Impulse-range strategy funded

Release `prod-20260930.1-g61c98f3592fd` for trader and dashboard, from commit
`61c98f3`. Settings: `STRATEGY_RANGE_MODE=funded`, USD 100 at 5x, both
directions, choppiness filter at 0.22. The shared `RISK_MAX_DAILY_LOSS_USD` was
raised from 100 to 2,000 as an emergency-only brake. MA and breakout settings are
unchanged. At acceptance the range model rebuilt the current channel as not
tradeable (choppy start); no range trade had been placed.

- Add a third strategy for post-impulse consolidations. It uses the breakout
  impulse rule, then a swing channel and edge-to-midpoint trades, with a
  close-based stop, channel redraws, a 40% width cap, and a tapered 120-day
  expiry. A choppiness filter (20-day efficiency ratio below 0.22 at
  confirmation) is on by default.
- The pure machine reproduces the 2026-09-29 research simulator trade for trade.
  The live adapter's shadow book and funded orders match the validated
  one-minute replay trade for trade on feed-ordered synthetic data.
- `STRATEGY_RANGE_MODE` (`off`, `shadow`, `funded`) wires it into the funded
  live process with its own owned slot and fixed-notional sizing. It is `off` by
  default.
- Add a dashboard range panel with the channel, entry, target and stop levels,
  the efficiency ratio at confirmation, redraws, and the age taper. It also
  shows the paper or funded position, the shadow book and recent range events.
  The panel adds a Range signal scope, range settings in the run
  configuration, and execution-alignment checks for an incomplete model or an
  unmodelled funded position.
- A cold range strategy no longer makes the funded feed's whole 100-day
  warmup strict, so a historical minute gap cannot stop the trader.
- Releases are built `--no-editable` and now ship `config/seeds/`, which the
  breakout historical rebuild reads.

## 2026-09-28 — Historical funding admission

Release `prod-20260928.1-g716fbee17776` (trader), from commit `716fbee`.

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
