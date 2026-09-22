# BTC close-range funded rollout

Date: 2026-09-22. Status: source implementation and read-only production
rehearsal complete; live service not yet changed.

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

## Deployment transaction

Authoritative inputs and state:

- source: `/home/james/src/lnmbot`;
- trader unit: `/etc/systemd/system/lnmbot.service`;
- protected environment: `/etc/lnmbot/trader.env`;
- mutable database: `/var/lib/lnmbot/lnmarkets.sqlite`;
- current rollback release:
  `/usr/local/lib/lnmbot/prod-20260901.1-gcc3abc36a1f1`;
- dashboard unit/release remain a separate second transaction.

Before the switch, take a consistent SQLite backup and record its integrity and
hash, current unit definition, PID/restart count, latest MA snapshot and exact
local/remote open-trade set. Export a clean reviewed commit into a new empty,
root-owned release, build dependencies offline from the existing locked set,
and validate import/help/schema against a disposable copy.

Switch only `lnmbot.service`, retaining `--allow-orders --confirm-mainnet` and
the protected environment. Startup must map every legacy open trade to MA,
restore the MA snapshot, reconstruct breakout historical occupancy and create
no startup order. Acceptance requires an active service, one funded executor,
unchanged pre-existing remote trade IDs/count, a current portfolio run and both
strategy snapshots, fresh account/candle writes, zero unknown trades, no
duplicate order and an unchanged Core/LND process baseline.

Rollback before any breakout fill is to restore the prior unit and environment
and restart it; the additive schema is backward-compatible. After a breakout
trade exists, the prior MA-only runtime cannot manage it. Keep the integrated
executor active or deliberately drain every breakout unit before reverting.
Never restore the database merely to undo an additive schema migration.

After trader acceptance, deploy the dashboard from its own immutable release,
then update the system topology, operator handbook, monitor expectations and
LNMarkets backup restore evidence to cover the added table and seed/config
files.
