# Remediation release acceptance — 27 September 2026

Status: source and copy-based acceptance passed; live switch pending. This is
an implementation/operations repair, not evidence of future profitability.
The [initial audit](independent-audit-2026-09-26.md) and
[source remediation report](remediation-2026-09-27.md) retain dated evidence.

## Approved behavior

- New entries check their own margin and fee cash requirement. No reservation
  of future breakout units is introduced.
- Historical isolated funding debits reduce margin; credits remain wallet
  cash. Paper units and results never enter funded ownership/accounting.
- Confirmed MA liquidation and unknown external closure start the configured
  loss cooldown, regardless the ordinary price-loss trigger. Unknown causes
  stay unclassified in accounting/dashboard. Confirmed manual close resets
  only its timeframe without cooldown; eligibility is reconsidered at its next
  scheduled live boundary, using the normal qualifiers.

Real venue responses revealed ISO timestamp strings, a numeric `liquidation`
price and no closure-cause field. API timestamps now normalize to UTC and entry
execution uses `filledAt` before `createdAt`. Invalid nonempty timestamps fail
closed. Cause handling uses three states; a numeric liquidation threshold is
never interpreted as a boolean cause. A live GET preflight also established
that the funding endpoint excludes its upper bound. The runner requests one
second beyond the required settlement and filters back to the inclusive model
boundary, so every eight-hour settlement precedes its modeled price observation. Operator classification is dry-run by
default, requires a stopped trader for production application and refuses a
later slot trade or outstanding execution command.

## Independent production evidence

Read-only authenticated GETs, complete pagination and online SQLite backup
were refreshed after the interruption. No credentials are printed or stored
with source. Private evidence is under `/tmp/lnmbot-release-20260927/acceptance`.

- Venue: zero running/pending trades, 38 closed trades. Exactly 20 locally
  recorded trades were independently matched; 18 other account trades remain
  unassigned. No ownership adoption or diagnostic order is performed.
- All 40 fill prices/fees, closed gross P&L and all 311 funding records agree
  with venue history. The migration aligns 23 quoted order prices to verified
  fills, fixes fill timestamps to venue execution times, attributes 38 legacy
  orders and attached signals, and inserts 344 missing strategy P&L events.
- Seven early funding aggregates had reversed signs. Exact fee rows support
  their correction; repaired net owned accounting is 325,969 sats. Fill
  quantities, prices, fees, order decision timestamps and money transfers are
  otherwise preserved.
- Public minute history was independently grouped into 37 daily candles:
  53,280 complete minutes, with all stored daily OHLC matching. Seed/reference
  hashes, all four historical units, holding day 36, peak, boundary and funding
  coverage through the September 26 completed daily candle were verified.
  Campaign `20260822L` retains four lifetime units, all surviving and unowned.
  Minute continuation obtains subsequent funding/prices after startup.
- Copy-based repair, additive schema initialization, state restoration and the
  read-only readiness checker pass. MA indicator/cooldown state is preserved;
  stable aliases supersede legacy snapshots without deleting their evidence.

Production retains its USD 2,500 per-position cap, 5x maximum leverage,
USD 100 daily-loss limit and four admissions/minute. Aggregate notional and
margin caps are supported but **unset** in the protected production config;
this release does not invent new limits. Equity sizing, 4h CHOP adjustment,
USD 100/5x breakout units and `long_only` remain selected. Fees/reserve admission
is conservative, but venue inventory and available cash remain authoritative.

## Validation and release gates

The actual dirty checkout's offline suite passed 368 tests in 69.96 seconds,
from an empty environment and neutral directory. Authenticated/funded modules
`test_isolated_positions.py` and `test_live_integration.py` are excluded.
Subsequent focused checks cover UTC timestamp normalization/refusal and the
operator classification receipt/ledger transaction and the exclusive venue
funding boundary. Repository-wide Ruff also reports 30 pre-existing findings in
unchanged files; changed runtime/new regression files pass the scoped check. Ruff and import contracts
are checked for the changed production implementation.

Keep unrelated research/editable work. Selectively commit the repaired runtime,
its focused tests and documents; export allowlisted production inputs from that
commit, never the entire checkout. Seed/reference artifacts are necessary
runtime inputs and retain their verified hashes. Build frozen dependencies at
the final immutable release paths and preserve each service's identity,
protected config, database authority and containment.

The live transaction takes another consistent backup, stops only trader and
dashboard, verifies sole-executor/flat venue evidence and unchanged critical
state, and applies the exact evidence-bound SQL plan under `BEGIN IMMEDIATE`.
A changed fingerprint aborts the repair; no database replacement is used.
The final plan/result, source provenance and prior units live in a protected
checkpoint. An independent timer starts compatible candidate code with
`RISK_MAX_POSITION_USD=0` if acceptance is interrupted. This blocks new risk
while permitting owned exits, and never restores an old database. Old releases
remain retained but are not safe automatic funded rollback targets.

Acceptance requires both services healthy with one executor, restored snapshots,
no startup orders, fresh live market/account data, consistent funded accounting,
separate complete paper occupancy, honest dashboard rendering and a successful
encrypted backup. Record final release paths, commit, checkpoint and timer
status below after those checks; do not mistake local readiness for approval.

## Limits

No funded diagnostic order, real partial-fill/outage test, stress test or
strategy parameter search was performed. Deterministic faults establish local
recovery behavior but cannot guarantee exchange availability or execution.
Historical OHLC ordering, slippage, liquidation book/reference differences and
funding granularity remain model limitations. The linear generic paper engine
is not a funded inverse-contract/shared-wallet simulator. Full-history funding
and dashboard queries may need measured optimization as history grows.
