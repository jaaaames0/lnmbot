# Breakout implementation: foundation and integrated rollout

2026-09-22. The original separate shadow foundation below remains development
history. The accepted direction is now the single-process funded rollout
implemented and rehearsed in `btc-close-range-funded-rollout-2026-09-22.md`.
It is not yet deployed.

## Operator decisions incorporated

- Preserve the funded MA service and its open trades during implementation.
- Use existing trading capital as one shared pool, with strategy attribution;
  fixed MA/breakout wallet partitions are not required.
- Seed historical context rather than treating breakout as strategically flat.
  An active hypothetical parent blocks a new parent.
- No special live-add-on policy for a historical parent is introduced. The
  ordinary displacement threshold remains part of the frozen strategy.
- Historical reconstructed profits, forward paper profits and actual funded
  profits must remain distinct.

## Implemented

`src/lnmarkets_bot/portfolio/store.py` provides a separate SQLite book with
stable strategy instance identities, immutable position ownership, shared-cash
observations, atomic reservations and signed fee/funding/realized-P&L events.
Units have campaign and K identities; venue IDs cannot be assigned twice.
Paper and live books cannot consume one another's balances or reservations.

Reservation retries are idempotent and conflicting identities are rejected.
Unconfirmed submissions keep their reservation. Posted collateral continues to
count against stale cash until an explicitly reconciled later cash observation
acknowledges it. Recording a close does not invent a wallet credit. Funding is
attributed separately from realized trading P&L; positive received funding is
not implicitly added to isolated margin.

The seed importer retains immutable completed-day research observations and
their rule hash. It rejects forming candles, conflicting revisions, rule
changes and missing daily observations after initialization. A pending exit
still counts as occupied. The parent-admission gate rejects missing/stale
context and reports an active hypothetical campaign. It is only one gate:
passing it does not authorize or submit an order.

The historical seed has no position ownership, collateral reservation or
accounting events. The imported research P&L is retained as source evidence
only, never summed into live or paper trading P&L. No unowned position can
receive a funded accounting event through this store.

`scripts/seed_breakout_portfolio.py` initializes the separate development book:

```bash
.venv/bin/python scripts/seed_breakout_portfolio.py \
  --snapshot runs/btc-close-range-shadow-latest.json \
  --database runs/portfolio-shadow.sqlite
```

The command refuses production paths below `/var/lib`; the store also refuses
to initialize over a database belonging to another application. It has no
exchange client or order capability. It does not modify the existing shadow
snapshot. The script is an import command, not a scheduled market-data worker.

The development book was seeded with campaign `20260822L`, parent entry
2026-08-22, boundary 72,998.70, four hypothetical units, observed through the
2026-09-22 00:00 UTC close of the September 21 candle. At import, attributed
trades and P&L are both zero. This date comes from the cached research replay;
it has not been independently re-established by a production strategy engine.

`src/lnmarkets_bot/strategy/close_range.py` now contains a pure production
state machine for the frozen daily rules. It does not import pandas, research
scripts, persistence or an exchange client. Its raw signals and structure
features match the frozen research matrix over the complete cached history:
20-close boundaries, prior EMA20, simple prior ATR14, prior-10 candle overlap,
1.5 ATR distance and 0.55 overlap threshold. It implements structure-filtered
parents, raw same-side add-ons, four lifetime K slots, the 15% displacement
limit, original parent boundary, 85-day/97%-of-peak recovery and 120-day cap.
External parent liquidations have an explicit state transition; intraday
liquidation detection remains the coordinator/venue layer's responsibility.

The compact historical seed knows the parent fill and the lifetime unit count,
but not every child's fill. The state therefore stores four lifetime units and
one known fill instead of fabricating three child prices. A historical campaign
and its exit decisions are always labelled unowned. The machine evaluates an
otherwise qualifying add-on using the ordinary rules and labels its result as
paper context; it makes no decision about whether such an add-on could become
a funded trade.

`scripts/run_breakout_forward_shadow.py` initializes the machine from completed
daily history plus the campaign seed and advances it one UTC day at a time.
Machine state and all decisions for a candle commit atomically. Re-running a
completed candle must reproduce the identical state and decision set; gaps,
stale writers and changed history fail closed. A chained fingerprint covers
the complete processed candle history, so a restart rejects a revised or
truncated source before advancing. The runner contains no exchange
client and refuses databases under `/var/lib`.

The current development machine is initialized through the September 21 candle
(close boundary 2026-09-22 00:00 UTC), retains its signal for the next open,
and remains in historical campaign `20260822L`. There was no newer completed
candle in the local cache, so the first forward run advanced zero days. A
second invocation was idempotent. Superseded generated databases were moved to
`/tmp/portfolio-shadow-pre-lifetime-units.sqlite` and
`/tmp/portfolio-shadow-pre-source-digest.sqlite`; they can be discarded after
review because all versions are reproducible from source data and snapshots.

`scripts/run_dashboard.py` has an optional read-only panel enabled by
`LNMBOT_PORTFOLIO_SHADOW_DB`. With the variable unset, the existing dashboard
view is unchanged. The panel labels historical occupancy, displays its
freshness and separates attributed accounting from MA/live totals. An
unavailable shadow database produces an unavailable panel and is never created
by the dashboard. No production environment or service was changed.
The example dashboard environment documents the optional path but leaves it
disabled. Enabling it in the deployed dashboard remains a later release step.

## Validation

Tests cover simultaneous reservation attempts, separate paper/live cash,
duplicate/conflicting events across restart, received and paid funding,
per-strategy closed-trade profitability, stale cash, posted-margin reconciliation,
historical occupancy without invented trades/profit, pending exit occupancy,
revised/forming/gapped observations, refusal to initialize a trader database,
read-only dashboard rendering, and HTML escaping of imported data.

State-machine tests additionally compare every eligible cached raw signal and
all frozen structure features with the research implementation, exercise the
real current seed, verify historical exit ownership, ordinary seeded-add-on
evaluation, liquidation handling, contiguous-day enforcement and JSON restart
round trips. Store tests cover atomic step rollback and exact retry semantics.

The repository-wide test run reaches the pre-existing
`test_equity_tracks_balance_plus_position_notional` and then hangs; the same
test hangs when isolated. The relevant new/MA/dashboard/risk/executor suite is
therefore the acceptance gate for this slice. The funded `lnmbot` unit was
checked read-only after development: active, PID 647590, zero restarts, active
since 2026-09-12 06:39:10 UTC.

Existing MA state persistence, strategy isolation, dashboard and risk/executor
regressions are run alongside these tests. Ruff and strict mypy checks cover
the new portfolio module. Tests do not connect to the funded account.

## Market data and scheduler candidate

`src/lnmarkets_bot/data/binance_cache.py` now owns the forward daily market
cache contract. It re-fetches a three-candle overlap, excludes the forming
candle, replaces revised tail rows, requires a contiguous UTC series through
the latest expected close and swaps a validated Parquet file atomically. The
state machine separately fingerprints every processed OHLC candle, so a later
revision fails closed instead of silently changing prior decisions.

`scripts/run_breakout_shadow_job.py` serializes refresh and state advancement
under a nonblocking file lock. It has no API credentials or order client. Its
installed-mode escape hatch accepts only `/var/lib/lnmbot-shadow` state and a
`/run/lnmbot-shadow` lock; it refuses the funded database path even when that
mode is selected.

The candidate systemd service and timer run under `lnmbot-shadow` with shared
read group `lnmbot-shadow-db`. The service explicitly cannot access the funded
trader's config or database. It runs at 00:10, 01:10 and 02:10 UTC so two later
attempts recover from a transient source failure. The unit templates and full
candidate procedure are in `btc-close-range-shadow-operations-2026-09.md`.
They have not been installed or enabled.

The rendered service/timer pass `systemd-analyze verify`. A disposable complete
job was run twice against Binance's real public futures endpoint. Both runs
produced 2,570 completed candles through 2026-09-21 with zero gaps/duplicates,
restored historical campaign `20260822L`, placed no orders and advanced no
duplicate decisions on retry.

## Remaining implementation, before any funded use

This is the accounting and seed-context foundation, not the complete
multi-strategy runtime. In particular, cash observations and reflected posted
reservations currently require a caller; there is no venue reconciliation
adapter feeding this book and no live MA history imported into it.

Next install the forward shadow candidate as a separately reviewed immutable
release, or continue source work with verified 4h/venue liquidation
observations. After the observation phase, build the explicit owned-position
coordinator, enforcing the parent occupancy gate, account limits and
shared-cash reservations. Existing MA routing remains unchanged until that
integration passes isolated replay.

Full performance still needs unrealized marks, funding reconciliation,
cash-flow-adjusted portfolio equity, capital usage and drawdown statistics.
The current projection reports attributed recorded cash P&L components and
closed-trade counts only; it does not claim a strategy ROI or backtest.

The research accounting discrepancies and rejected trailing-exit bug recorded
in the design remain open. The seed is provisional research context. Resolve
those issues, perform finite-shared-capital combined replay, and rehearse
migration/rollback with open MA trades before preparing a funded release.

Any eventual shared-pool rollout can affect future MA entries when capital is
scarce. Keeping MA uninterrupted during development does not promise unlimited
capital or identical future fills after a second funded strategy is enabled.
