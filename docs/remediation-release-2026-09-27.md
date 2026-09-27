# Remediation release acceptance — 27 September 2026

Status: **accepted live at 10:40 UTC on 27 September 2026**. Both services
run normal production mode from the immutable release recorded below. This is
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

## Additional confirmed defects from release preflight

These are resolved in the deployed source; the initial audit remains frozen.

| Severity / confidence | Trigger, intended versus observed behavior | Evidence and consequences | Repair, coverage and position care |
|---|---|---|---|
| High / confirmed | Real `createdAt` / `closedAt` strings reached code expecting datetime objects. Authoritative fills/closures should persist; receipt/reconciliation instead raised an attribute error. | `src/lnmarkets_bot/api/isolated.py:166`, `engine/live_executor.py:434` and `:787`; the real REST-shaped external-close test reproduced the crash. An accepted entry could remain ambiguous, or closure management fail. | Normalize UTC at the API boundary (`isolated.py:173`), prefer actual `filledAt` and reject malformed nonempty values. `tests/test_ma_external_closures.py:136` and `:198` cover receipt/closure, offset timestamps, replay, classification and malformed inputs. Legacy fill timestamps were corrected only with venue evidence; the account was flat at cutover. |
| Medium / confirmed | At the eight-hour boundary, the runner queried history with `to` equal to that settlement. The model requires the boundary settlement first; the exclusive API end omitted it. | The authenticated GET-only preflight independently reproduced the omitted September 27 08:00 settlement. Paper completeness would fail and breakout admission stay blocked despite available data; known funded exits continued. | `scripts/run_live.py:44` requests one second past the boundary and filters out future settlements. `tests/test_live_funding_boundary.py:11` simulates the exclusive endpoint; a subsequent real GET preflight and live warmup verified coverage through 08:00. Preserve campaign state and verified funding checkpoints; no funded-order replay or parameter change. |

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

After the timestamp/cause and real funding-boundary repairs, the actual dirty
checkout's final offline suite passed **375 tests in 73.14 seconds**,
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


## Final live acceptance

Source commits are `be69aae1e214` (execution/accounting/campaign repairs) and
`c0cb72dac281` (confirmed exclusive funding endpoint correction). Both trader and
dashboard now use `prod-20260927.2-gc0cb72dac281` in their separate
`/usr/local/lib/lnmbot` and `/usr/local/lib/lnmbot-dashboard` directories.
The allowlisted Git archive SHA-256 is
`3b9a27b063a5697e3304fec503f5e98f14939ac76a5db8d17521ab329a686fb4`.
All 64 exported input files match the commit-derived manifest. Frozen offline
`uv.lock` builds and real service-user import checks passed. An earlier candidate
was never activated and remains retained as preflight evidence.

The protected checkpoint is
`/data/security-backups/lnmbot-remediation-20260927T102913Z`. The critical-state
fingerprint matched immediately before and after the SQL transaction. A
mid-transaction diagnostic on a disposable copy proved full rollback; changed
ledger and repeated application were refused. No live SQLite replacement or
old-state restoration occurred. Venue GET checks before stop, at cutover, before
admission and at acceptance all confirmed no running/pending trades and the
same closed inventory. Exact protected evidence and source Git bundle are
retained there; credentials remain outside source.

Acceptance first used the compatible unit with a zero entry cap. After live
warmup/restore checks passed, the normal unit restored the existing USD 2,500
cap; the remaining settings were unchanged. The final funded run is 64, with
trader PID 308202 and dashboard PID 306548, both active/running with zero
automatic restarts at acceptance. Exactly one funded runner was observed.
Current-run account and both strategy snapshots, fresh completed-minute bars,
full historical funding through September 27 08:00 UTC and complete four-unit
paper state passed. Opening candle timestamps can precede process start by
part of a minute; current-run ownership and freshness establish post-restart
observation rather than an incorrect strict timestamp-after-start comparison.

The order journal stayed at **40 orders** and the repaired ledger at **351
events / 325,969 net sats**. There were no startup or diagnostic orders.
Daily/four-hour MA winner cooldowns stayed at **11/7**. Historical campaign
`20260822L` remained at holding day 36, with four surviving historical units,
no funded ownership and `long_only` preserved. New execution commands are
empty; no ambiguous result or undelivered closure needs operator disposition.

`/healthz`, `/health`, overview, signals, trades, funding, P&L and runs each
returned HTTP 200 without a database error. The dashboard cannot write the DB
or read the trader credential; the trader cannot read the dashboard credential;
neither service can write its immutable code. Service identities, containment,
listener and separate environments are retained. Core, LND and nginx process
state matched the pre-switch checkpoint; they were not restarted. Existing
monitor/watcher definitions retained their unit/identity/listener expectations.

`lnmbot-backup.service` finished with `Result=success`, `ExecMainStatus=0`.
The independent recovery timer was disarmed without execution after acceptance.
Prior runtimes, backups and compatible zero-entry recovery unit remain retained;
only a reviewed future transaction should use them. The topology, handbook and
source/runtime inventory were updated. Changes and the production tag are local;
no remote push occurred. Unrelated dirty research remains preserved, including
corrected selected-model research scripts and generated evidence outside the
runtime export. Authored reports refer to those retained workspace artifacts;
they were not published as production inputs.

The remaining limits are the real exchange/outage/partial-fill and model issues
listed above, plus browser/UI behavior beyond server-rendered HTTP checks. No
claim is made that every future venue response or failure will behave like a
simulated test, or that a correct implementation guarantees profitability.


Host privilege closeout: `sudo -k` invalidated cached authentication, but
`sudo -n true` still succeeded for UID 1000 (`james`); read-only authorization
inspection confirmed pre-existing `(ALL) NOPASSWD: ALL`. No sudo rule was
introduced or changed by this deployment. The application release is accepted,
but this host exception remains open. `/home/james/AGENTS.md` requires:
“Temporary privilege exceptions must be explicit, attended and closed with an
invalidated-credential `sudo -n true` failure.” Closing the pre-existing
host-wide policy is separate from this application transaction and requires
operator disposition; it must not be mistaken for a successful privilege reset.
