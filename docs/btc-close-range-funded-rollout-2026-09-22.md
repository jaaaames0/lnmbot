# BTC close-range funded rollout

Date: 2026-09-22. Status: accepted live rollout.

## Accepted operating model

The funded trader remains one `lnmbot.service`. One LN Markets client and one
1-minute candle stream feed two independent strategy instances through a
single serialized risk guard and isolated-trade executor:

- `ma_cross_primary` owns the incumbent `1d` and `4h` slots. Its parameters,
  durable strategy snapshot and currently open trade are preserved.
- `btc_close_range_v1` owns `k0` through `k3`. Each slot is a separate isolated
  LN Markets trade. The initial target is USD 100 notional per unit at 5x,
  for at most USD 400 gross notional and roughly USD 80 initial margin before
  fees and funding when all four units are occupied.

Both strategies draw from the same wallet. Existing account-wide position,
leverage, daily-loss, order-rate, total-notional and total-margin limits remain
the admission authority. Orders, fills, funding and realized results carry an
explicit strategy and slot identity. The new attribution ledger starts at the
integrated rollout; it does not invent attribution for earlier MA costs.

## Breakout activation state

The immutable initial evidence consists of LN Markets daily candles through
2026-09-12 and the historical `20260822L` campaign seed. That campaign is
hypothetical, has four lifetime units and owns no venue trade. At startup the
ordinary 100-day LN Markets candle warmup advances the state through the latest
completed day. Historical decisions never submit an order. If the historical
campaign is still occupied, it blocks a new parent and no add-on can be funded
without an owned `k0`. There is no backdated entry.

The funded rules remain the frozen structure-parent/raw-add-on candidate:
prior-20-close breakout, parent EMA/ATR and overlap structure filter,
same-direction raw add-ons, four lifetime K slots, maximum 15% favorable
displacement from the parent, original range boundary, day-85 97%-of-peak
recovery, day-120 maximum hold and venue isolated liquidation. A parent
liquidation immediately closes surviving children; child liquidation does not
restore its lifetime slot.

## Source changes

- `scripts/run_live.py` selects the old single-MA runner when breakout is
  disabled and the integrated dispatcher when it is enabled.
- `engine/portfolio_live.py` serializes both strategies over the same feed,
  risk guard and executor while retaining separate durable state.
- `strategy/close_range_live.py` adapts completed LN Markets daily aggregates
  to the pure close-range state machine and converts decisions into K-slot
  intents.
- `engine/live_executor.py` routes by `strategy_instance_id:position_key`,
  migrates legacy open MA rows at reconciliation, detects venue liquidation or
  manual closure, and records strategy attribution.
- SQLite receives additive strategy/slot columns and a
  `strategy_pnl_events` table. The old release tolerates these additions.
- The dashboard keeps the MA cards, adds strategy/slot ownership to positions
  and journals, and reports signed live attribution separately.
- Run audit metadata excludes LN Markets API credentials. The rollout scrubbed
  the three credential keys from all 48 historical rows that contained them.

`STRATEGY_BREAKOUT_ENABLED` defaults to false. The rollout additionally sets
the unit to 100 and leverage to 5 and points the service at release-contained
seed files. The 5-minute test profile is rejected while breakout is enabled.

## Rehearsal evidence

A consistent online copy of `/var/lib/lnmbot/lnmarkets.sqlite` passed SQLite
integrity before and after the additive migration. It contained 51 runs, 39
order rows and one locally open `4h` MA trade at rehearsal time.

`scripts/rehearse_portfolio_rollout.py` then used the protected production
credential only for authenticated reads. It submitted no order and confirmed:

- the venue running set exactly reconciled with the disposable database copy;
- the open trade mapped to `ma_cross_primary:4h`;
- the incumbent MA snapshot accepted the unchanged effective parameters;
- the breakout seed restored `20260822L`, four lifetime units and no ownership.

The focused dashboard, portfolio, close-range, migration, executor and risk
suite passes. Ruff and strict mypy pass on the new runtime. The repository's
existing live-integration/full-suite paths can hang after their deterministic
cases; they are not counted as successful evidence.

## Accepted deployment

The initial sole funded executor ran from
`/usr/local/lib/lnmbot/prod-20260922.3-g12d9baac7b2a`. The dashboard runs from
`/usr/local/lib/lnmbot-dashboard/prod-20260922.3-g8df824e73760`. Both retain
their locked identities, protected environments and shared database boundary.

Acceptance proved that both services are active with zero restarts; the
39-row order journal did not change; the existing remote trade exactly
reconciles to `ma_cross_primary:4h`; both strategy snapshots restored and
checkpointed; historical campaign `20260822L` restored with four lifetime
units and no venue ownership; SQLite passes `quick_check`; and no startup or
backdated order was submitted. Core, LND, the backup timer and infrastructure
monitor remained active, and a fresh encrypted backup completed successfully.

The dashboard's first editable-package candidate failed inside its systemd
sandbox. Its exercised rollback restored the old release before the corrected
non-editable package was accepted. Root-only evidence is retained at
`/data/security-backups/lnmbot-breakout-rollout-20260922T133100Z`,
`/data/security-backups/lnmbot-dashboard-multistrategy-20260922T134900Z` and
`/data/security-backups/lnmbot-credential-redaction-20260922T135824Z`.

A post-rollout dashboard correction scopes MA levels and cool-off state to the
MA snapshot instead of whichever strategy snapshot has the newest timestamp.
Production strategy state always retained the 1d winner cool-off with 11
verdict changes remaining; only its presentation was wrong. The accepted
dashboard-only checkpoint is
`/data/security-backups/lnmbot-dashboard-cooldown-fix-20260922T222211Z`.

The dashboard now runs from
`/usr/local/lib/lnmbot-dashboard/prod-20260922.4-ga4f678874592`.
Its overview presents three separate position cards: MA 1d, MA 4h and the
close-range campaign. The historical `20260822L` campaign is shown as occupied
but unfunded, so it cannot be mistaken for a live venue position. Future funded
K units appear separately with their slot, notional and open P&L. Signals,
trades, funding and P&L are attributed to the owning strategy; MA timeframe
filters exclude breakout rows. The constant-notional comparison uses USD 100
per isolated trade and no longer reports a misleading two-MA-slot portfolio
return for the combined system.

The dashboard-only cutover retained `lnmbot.service` PID 3246389 with zero
restarts and 39 order rows. All dashboard routes returned HTTP 200, the MA 1d
card showed 11 verdict changes of winner cool-off remaining, both strategy
snapshots were present and SQLite `quick_check` passed. The accepted release
has a root-only rollback checkpoint at
`/data/security-backups/lnmbot-dashboard-three-position-20260922T230545Z`.

The historical campaign display uses a versioned replay of the four
hypothetical units of `20260822L` from the immutable LN Markets daily seed
feed. Its seed and candle hashes must match before the dashboard shows the
child entries; an independent test repeats the replay and compares every unit.
An expandable position row presents K0
through K3, the qualifying signal dates and the earlier blocked August 19–20
signals. It marks the stack with the current BTC price using gross inverse
contract arithmetic and the configured USD 100 per-unit notional. These are
paper estimates: fees, funding and liquidation are not modeled; no synthetic
order is inserted into the funded positions, wallet balance or P&L totals.

## Funded reversal execution correction — 2026-09-23

The August 19 and 20 long breakout signals passed the structure test, but the
historical `20260602S` short still occupied the sole campaign. Its range-close
exit and the August 21 long signal coincided at the August 22 open. The pure
state machine closed the short and modeled `20260822L` correctly, while the
live adapter would have suppressed a new funded parent while old funded slots
were marked as closing, leaving modeled occupancy without a venue position.

The adapter now durably retains a same-open parent signal, submits exits for all
old funded slots first, and proposes the new `k0` entry on a later 1-minute bar
only when every old slot is confirmed flat. It discards the signal if that
handoff takes more than five minutes, rather than entering late. Rejected or
failed parent entries clear unfunded modeled occupancy. Restart recovery
resumes pending closes and recognizes a parent fill completed before its final
strategy snapshot, without submitting a duplicate.

The tested code is commit `59bbe62a20ff`, deployed only to `lnmbot.service` at
`/usr/local/lib/lnmbot/prod-20260923.1-g59bbe62a20ff`. The dashboard service
was not restarted. Focused strategy, machine, portfolio and executor checks
passed (45 tests), as did Ruff and strict mypy. A disposable copy of the live
database restored both strategies and reconciled the one venue trade to
`ma_cross_primary:4h` before and after the switch. The accepted trader has
zero restarts, SQLite `quick_check` passes, the order journal remains at 39,
and a fresh encrypted backup succeeded. The root-only checkpoint is
`/data/security-backups/lnmbot-breakout-reversal-20260923T002615Z`.

The old MA-only runtime cannot manage K-slot positions. After any breakout
fill, keep the integrated executor active or deliberately drain every breakout
unit before reverting. Never restore the database merely to undo the additive
schema.
