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

The sole funded executor now runs from
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

The old MA-only runtime cannot manage K-slot positions. After any breakout
fill, keep the integrated executor active or deliberately drain every breakout
unit before reverting. Never restore the database merely to undo the additive
schema.
