# Independent lnmbot audit — 26 September 2026

> This is the frozen pre-remediation audit. Evidence line numbers refer to the 26 September checkout/deployed snapshots, not the subsequently edited source. Implementation and remaining acceptance gates are recorded in [remediation-2026-09-27.md](remediation-2026-09-27.md).

## Overall assessment

The multi-strategy integration has working ownership separation and substantial successful-path coverage, but **confirmed execution, reconciliation and accounting defects prevent a reliable unattended-operation assurance**. Increasing capital or treating the current passing tests as a production acceptance gate would be premature.

The highest priorities are ambiguous order submission, simultaneous campaign closures and recoverable persistence. These defects are present in the deployed trader, not merely in research or dirty checkout changes. This audit found no evidence that they have already caused a production loss; production database access was unavailable.

No fixes, deployments, service restarts, orders or production-state changes were performed. Diagnostics and this report were written only to `/tmp/lnmbot-audit-20260926-v7qaky8b`. The actual dirty checkout was copied for testing; 475 original files checked against the initial SHA-256 manifest remained unchanged.

## Authority and deployment baseline

Applicable `/home/james/AGENTS.md` and its required topology, operator-handbook and source/runtime-hygiene documents were read. Documentary assertions and earlier audit conclusions were treated as leads, not proof.

Read-only service inspection established:

| Consumer | Actual immutable release | Observed status |
|---|---|---|
| Trader | `/usr/local/lib/lnmbot/prod-20260925.2-ga189b7001a5b` | Active, running, zero service restarts; mainnet orders enabled in invocation |
| Dashboard | `/usr/local/lib/lnmbot-dashboard/prod-20260925.4-ge73c58b1406a` | Active, running, zero service restarts |
| Editable checkout | HEAD `244810cbd8c1`, substantial tracked and untracked changes | Neither deployed release can be identified simply with this HEAD |

Production uses `/var/lib/lnmbot/lnmarkets.sqlite` and protected configuration under `/etc/lnmbot`. No credential contents were accessed. Read-only SQLite access failed under the available identity; no permission changes were attempted.

The localhost dashboard returned HTTP 200 and reported execution aligned, zero recorded positions verified at the venue, long-only breakout admission, and historical long campaign `20260822L` with four paper units and no funded units. Its latest daily candle was September 25 and its parent boundary was 72,968. **Those are application-reported observations, not an independent authenticated exchange reconciliation.** They do not prove that the account is presently flat or that its ledger is complete.

File comparison found deployed and checkout breakout machine/adapter, portfolio engine, API, risk, persistence and live runner equivalent. Python differences in deployed trader versus checkout were confined to `engine/fills.py`, `engine/live_executor.py` and `strategy/ma_cross.py`: the checkout includes an adverse-exit-slippage correction, a close-leverage reporting correction and a research EMA5 option. The checkout dashboard matches the separately deployed dashboard. In particular, the checkout's close-leverage correction is **not** in the deployed trader. Dated September 23/24 documentation must not override the inspected September 25 runtime paths.

### Intended rules established

The selected candidate, final investigation protocol and funded-rollout amendments establish the following hierarchy:

* [docs/btc-close-range-final-investigation-protocol-2026-09.md:26](/home/james/src/lnmbot/docs/btc-close-range-final-investigation-protocol-2026-09.md:26) defines the continuation signal stream and campaign mechanics; `docs/btc-close-range-shadow-candidate-2026-09.json` identifies the selected filter. The funded rollout and later explicit operational amendments govern live sizing, direction modes and execution handoff.
* Breakout uses completed daily closes outside the preceding 20 closes. Every continuation is eligible; there is no volume filter, first-candle-only filter or post-winner skip. The parent structure filter uses prior EMA20/ATR14 distance at least 1.5 and preceding-ten-candle overlap at most 0.55. Entry is at the next open; a parent open back through its own signal boundary is explicitly permitted.
* One parent owns up to three later same-side raw add-ons: four **lifetime** slots, no replacement after liquidation. An add-on reference open must remain outside the original parent boundary and its adverse-slippage fill must be within 15% favorable displacement of the parent fill. Live actual-fill checks can require an immediate abort close.
* Campaign exits are range-close, then day-85-or-later 97%-of-peak recovery, then day-120 cap; decisions execute at the next open. Parent price excursion and actual funded parent fill matter. Each unit has isolated liquidation; parent liquidation ends the campaign and surviving children must close at executable prices.
* Research used $1,000 units; funded rollout documented $100 units at 5x. Later sizing amendments permit new parents **and new add-ons** to use changed configured size while preserving existing quantities and the historical reference size. The earlier proposal to freeze campaign size is superseded for this purpose.
* [DEPLOYMENT.md:283](/home/james/src/lnmbot/DEPLOYMENT.md:283)–305 and the September 25 operator entries approve `both`, `long_only`, `short_only` admission for parents, funded add-ons and deferred reversals. Existing exits remain active; historical reference behavior remains both-direction. A recovery exit blocks only a same-side parent at that same open, with no later-day cooldown. Code and focused tests implement these restrictions.
* MA uses independent daily/four-hour SMA20/EMA21 state and tolerance bands. Default winner cooldowns are daily 3%/12 verdict changes and four-hour 5%/11; loser cooldowns are daily 5%/3 and four-hour 2%/4. A verdict change, including FLAT, consumes cooldown; FLAT alone does not close a position. These are price-return thresholds, not necessarily realized net-return thresholds.

Older challenger-v2 volume, deduplication and skip rules are superseded, not missing live features. Exploratory EMA5, cycle filters and sizing searches are not approved replacements. Proposed four-unit capital reservations have not been established as an adopted live requirement. Historical liquidation semantics, incompatible MA parameter migration and whether external MA closures should create a cooldown require explicit decisions; they cannot safely be inferred from successful tests.

## Confirmed findings, in operational priority order

Severity describes practical operational impact. Confidence describes evidence, not the probability of encountering the trigger. Unless stated otherwise, findings affect both the inspected checkout and deployed trader.

### F1 — High; high confidence: mutating retries can create unowned exposure

**Evidence:** [src/lnmarkets_bot/api/client.py:130](/home/james/src/lnmbot/src/lnmarkets_bot/api/client.py:130), [src/lnmarkets_bot/engine/live_executor.py:444](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:444), [src/lnmarkets_bot/engine/live_executor.py:591](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:591). Diagnostic tests `test_mutating_post_retried_after_500` and `test_new_remote_trade_is_ignored_during_runtime`.

**Trigger and observed behavior:** A mock venue commits a trade but returns HTTP 500. The generic client retries the POST; the venue creates a second trade and returns success. The caller learns only the second ID. Runtime reconciliation checks missing locally owned trades, not extra running venue trades. With four owned $100 units and an unknown $500 trade, local exposure remains $400 while the venue total is $900. Startup reconciliation rejects unknown trades, but continuous operation does not.

**Intended behavior/consequence:** A trading decision must have at most one funded effect, or remain explicitly unresolved until reconciled. Here an ambiguous response can create an unmanaged position, understated exposure and no strategy exit. The existing ambiguous-entry exception handler does not run after a successful automatic retry; a single running-trade lookup is also insufficient proof that a failed request never took effect.

**Remediation/coverage:** Separate safe-read retry policy from mutating requests. Persist a unique decision/command identity before submission and its unresolved/accepted/rejected outcome. Use exchange idempotency only if its contract is verified; otherwise reconcile ambiguous outcomes before further admission. Compare the complete remote and owned sets during runtime, without guessing ownership. Exercise commit-then-500, timeout with delayed visibility, duplicate success, already-closed results and restart at every submission boundary. Enforce one authorized executor instance.

**Deployment/data care:** Inventory all venue trades against owned IDs before enabling the revised path. Unknown positions require explicit operator disposition; never silently assign or close them. Introduce the durable command schema additively.

### F2 — High; high confidence: simultaneous parent/child closure interrupts survivor exits

**Evidence:** [src/lnmarkets_bot/engine/portfolio_live.py:166](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:166), [src/lnmarkets_bot/strategy/close_range_live.py:354](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/close_range_live.py:354). Diagnostic `test_parent_and_child_liquidation_same_poll_strands_survivors`.

**Trigger and observed behavior:** Fill k0–k3, then liquidate k0 and k1 before one reconciliation poll. The engine first mirrors both flat. The parent callback clears the campaign and queues k2/k3; the k1 callback then raises `externally closed breakout unit has no campaign`. Both surviving venue positions remain open and no close requests were sent before the exception.

**Intended behavior/consequence:** All observed closures should be applied consistently, followed promptly by closure of surviving children. Instead the entire trader crashes during a correlated adverse event. The diagnostic also establishes that a clean restart can restore the saved survivor-close state and close k2/k3: this is a dangerous interruption, not proof of permanently stranded positions in this particular sequence.

**Remediation/coverage:** Reconcile campaign changes as an idempotent batch, independent of event ordering. Separate the observed closed set, survivors and campaign completion. Test every nonempty subset of simultaneously closed units, permutations, manual/venue mixed closures and restart between notifications. Preserve the actual reason: the current adapter also treats manual external parent closure as liquidation.

**Deployment/data care:** Review pending closing-slot state and funded ownership before snapshot migration. Verify recovery with existing campaigns, including partially closed campaigns; do not reset them to flat by deleting snapshots.

### F3 — High; high confidence: partial persistence loses accounting permanently on restart

**Evidence:** [src/lnmarkets_bot/engine/live_executor.py:233](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:233), [src/lnmarkets_bot/persistence/recorder.py:195](/home/james/src/lnmbot/src/lnmarkets_bot/persistence/recorder.py:195), [src/lnmarkets_bot/persistence/recorder.py:253](/home/james/src/lnmbot/src/lnmarkets_bot/persistence/recorder.py:253), [src/lnmarkets_bot/persistence/recorder.py:312](/home/james/src/lnmbot/src/lnmarkets_bot/persistence/recorder.py:312), [src/lnmarkets_bot/persistence/recorder.py:398](/home/james/src/lnmbot/src/lnmarkets_bot/persistence/recorder.py:398). Diagnostic `test_close_accounting_commit_failure_is_not_repaired`.

**Trigger and observed behavior:** In a full campaign range exit, the venue closes k0 and the local close-order row commits; inject an error writing its fill. After restart, the remaining three units close successfully, but the database has eight orders, seven fills and seven strategy P&L events. The latest close-order row makes k0 terminal for restoration, so missing accounting is not repaired.

**Intended behavior/consequence:** A completed remote action must eventually produce complete, exactly-once local accounting and strategy notification. Separate commits can instead lose fills, P&L, daily-loss inputs or lifecycle delivery. External closure persistence preceding callbacks creates a related crash window. Compensating entry closes likewise need durable, recoverable records.

**Remediation/coverage:** Introduce a durable execution result and transactional local application: order/fill, fee/P&L, ownership and an undelivered strategy event. Replay idempotently using exchange/event identities. Remote action and SQLite cannot share a transaction, so explicitly model that boundary. Inject failure after every local write and before/after strategy delivery; assert eventual equality of venue state, fills, daily totals and strategy totals without repeated remote effects.

**Deployment/data care:** Add consistency scans and evidence-based backfill for incomplete historical records. Repair from exchange results, not estimated fills. Back up and migrate forward; reverting the database after funded actions would itself corrupt ownership.

### F4 — High; high confidence from source trace: backlog bars remain eligible for fresh market orders

**Evidence:** [src/lnmarkets_bot/data/live.py:104](/home/james/src/lnmbot/src/lnmarkets_bot/data/live.py:104), [src/lnmarkets_bot/data/live.py:142](/home/james/src/lnmbot/src/lnmarkets_bot/data/live.py:142), [src/lnmarkets_bot/engine/portfolio_live.py:163](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:163), [src/lnmarkets_bot/strategy/close_range_live.py:167](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/close_range_live.py:167).

**Trigger and observed behavior:** Connectivity fails across a signal boundary and resumes hours later. Catch-up candles are emitted with `warmup=False`; the engine uses their historical closes for execution context and risk valuation. There is no wall-clock entry freshness gate. Deferred reversal expiry compares the replayed bar timestamp with the pending entry timestamp, so replaying a logically timely minute can pass the five-minute limit long after the actual open. Order-rate accounting also uses bar timestamps, allowing a rapid backlog to appear separated in time.

**Intended behavior/consequence:** Next-open admission and reversal expiry should constrain actual submission time. Late market fills can represent a materially different setup, and historical marks can misstate equity conversion and sizing. This is a source-confirmed missing safeguard; no real outage or funded late order was induced.

**Remediation/coverage:** Distinguish event time, decision time and actual submission time. Replay missed state causally but gate new entries using wall-clock age and a fresh quote. Continue to reconcile and manage existing exposure. Test outages of six minutes, six hours and multiple days across daily/four-hour boundaries; assert no stale admissions and equivalent restored state. Define the general entry-lateness policy explicitly; retain the approved reversal expiry.

**Deployment/data care:** Do not replay expired historical entries when switching versions. Review persisted pending reversals against actual time and preserve outstanding exits.

### F5 — High; high confidence: final funding can disappear from attribution

**Evidence:** [src/lnmarkets_bot/engine/live_executor.py:475](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:475), [src/lnmarkets_bot/engine/live_executor.py:591](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:591), [src/lnmarkets_bot/engine/portfolio_live.py:216](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:216), [src/lnmarkets_bot/persistence/recorder.py:447](/home/james/src/lnmbot/src/lnmarkets_bot/persistence/recorder.py:447). Diagnostic `test_final_funding_lost_after_external_close`.

**Trigger and observed behavior:** A final 100-sat funding fee exists for k0 when it closes externally. Reconciliation removes the trade from managed positions before funding sync. The parent closure and survivor exits complete, but the funding table remains empty. A failed or delayed final funding fetch on an ordinary close has the same structural gap: closed trades cease to be candidates. Funding recording and subsequent aggregate updates are also separate transactions.

**Intended behavior/consequence:** Every owned trade's funding must remain attributable after closure. Net strategy P&L and risk history otherwise overstate performance when debits are missed; credits can be missed too.

**Remediation/coverage:** Maintain an independent account funding cursor and durable ownership history, including recently closed trades until settlement is complete. Process all pages and apply fee plus attribution atomically/idempotently. Test settlement before/after closure, transient API failure, delayed visibility, duplicates and midnight boundaries.

**Deployment/data care:** Reconcile historical funding against venue history and repair strategy/daily aggregates together. Do not merely start a cursor at deployment time and call the historical ledger complete.

### F6 — High; high confidence: daily-loss enforcement depends on event order and restart

**Evidence:** [src/lnmarkets_bot/risk/guard.py:124](/home/james/src/lnmbot/src/lnmarkets_bot/risk/guard.py:124), [src/lnmarkets_bot/risk/guard.py:146](/home/james/src/lnmbot/src/lnmarkets_bot/risk/guard.py:146), [src/lnmarkets_bot/engine/live_executor.py:475](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:475), [src/lnmarkets_bot/engine/portfolio_live.py:170](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:170). Diagnostics `test_funding_does_not_update_running_daily_loss_guard` and `test_first_external_loss_of_day_is_double_counted`.

**Trigger and observed behavior:** Initialize the guard, then record 1,000,000 sats of funding debit at a $70,000 mark. The persisted loss is $700, beyond the fixture's $200 limit, but the continuously running guard does not trip; a new guard does. Separately, if a $100 external loss is persisted before the day's first guard restoration, restoration reads that loss and the explicit delta adds it again, producing $200.

**Intended behavior/consequence:** Identical realized economic events must enforce the same limit regardless of restart or arrival order. Missing funding permits excess losses; double-counting profits can permit excess risk, while double-counting losses can stop admission early.

**Remediation/coverage:** Use one authoritative, exactly-once realized-event stream or recompute the relevant aggregate. Include opening/closing fees and funding consistently and define USD conversion timing. Test first event of day, initialization before/after persistence, credit/debit sequences, midnight and restart invariance.

**Deployment/data care:** Repair the ledger under F3/F5 before rebuilding risk state. Treat the existing displayed daily guard value as potentially inconsistent, not as an independent safety guarantee.

### F7 — High; high confidence on missing mechanism: historical occupancy ignores hypothetical liquidation

**Evidence:** [src/lnmarkets_bot/strategy/close_range.py:145](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/close_range.py:145), [src/lnmarkets_bot/strategy/close_range.py:218](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/close_range.py:218), [src/lnmarkets_bot/strategy/close_range_live.py:354](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/close_range_live.py:354), [scripts/run_dashboard.py:1058](/home/james/src/lnmbot/scripts/run_dashboard.py:1058). Diagnostic `test_seed_parent_liquidation_wick_not_applied`.

**Trigger and observed behavior:** Seed a four-unit historical long campaign with parent entry 100 and boundary 90; advance a daily candle with low 70 and close 100. At 5x, that adverse excursion requires liquidation under the intended inverse proxy, but the machine retains occupied campaign state. Historical state has no complete per-unit collateral/settlement model and no coordinator injects hypothetical liquidation events. Live liquidation callbacks depend on actual owned venue positions.

**Intended behavior/consequence:** Historical occupancy should reflect the selected campaign rules without creating funded orders. A hypothetical liquidated parent can instead keep blocking new funded parents; historical children can remain counted after their modeled liquidation. The dashboard labels gross hypothetical P&L as excluding fees/funding/liquidation and keeps it out of funded totals—good accounting separation, but that disclaimer does not repair occupancy.

**Remediation/coverage:** Specify the historical liquidation model and reconstruct all seeded units with the necessary entry and collateral inputs. Run isolated hypothetical events through a paper campaign coordinator, never through funded ownership or P&L tables. Cover parent-only, child-only, multiple-unit liquidation, surviving children and handover across seed/restart boundaries.

**Deployment/data care:** Rebuild the current historical state from its immutable reference history before migrating. A correction may change whether new funded entry is permitted; it must not cause a retroactive funded order. The absence of liquidation processing is confirmed; exact proxy/funding inputs still require an explicit approved definition.

### F8 — Medium; high confidence: MA warmup skips missed cooldown consumption

**Evidence:** [src/lnmarkets_bot/strategy/ma_cross.py:364](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/ma_cross.py:364). Diagnostic `test_ma_warmup_does_not_consume_missed_cooldown_transition`.

**Trigger and observed behavior:** Restore a cooldown with three transitions remaining, then process a previously unseen FLAT-to-UP transition. Continuous execution reduces the counter to two; replaying the same bar as warmup leaves three. The warmup return precedes cooldown consumption despite updating signal state.

**Intended behavior/consequence:** Restart should suppress historical orders while reproducing causal strategy state. Missed cooldown transitions instead extend suppression and change later admission depending on uptime.

**Remediation/coverage:** Separate state advancement from order emission; replay unseen transitions for restored state without inventing historical positions. Compare full continuous/restored snapshots across winner/loser cooldowns, FLAT transitions and duplicate candles.

**Deployment/data care:** Preserve funded positions and cooldown provenance. Do not clear all cooldowns to make snapshots load; correct counters only from a reproducible replay.

### F9 — Medium; high confidence: MA sizing changes can prevent startup

**Evidence:** [src/lnmarkets_bot/strategy/ma_cross.py:211](/home/james/src/lnmbot/src/lnmarkets_bot/strategy/ma_cross.py:211), [scripts/run_live.py:166](/home/james/src/lnmbot/scripts/run_live.py:166), [src/lnmarkets_bot/engine/portfolio_live.py:105](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:105), [DEPLOYMENT.md:75](/home/james/src/lnmbot/DEPLOYMENT.md:75). Diagnostic `test_ma_size_change_rejects_existing_snapshot`.

**Trigger and observed behavior:** Change MA `base_size_usd` from 100 to 200 and restore the previous snapshot. Exact comparison of the complete strategy parameter dictionary rejects it; the portfolio startup path raises rather than applying the documented new-entry sizing behavior. Deployment text also suggests a deep-bootstrap fallback for parameter changes that this path does not provide.

**Intended behavior/consequence:** New sizing should affect new orders without resizing existing positions or losing state. An ordinary size/configuration change can instead leave the trader unable to start and manage existing exposure.

**Remediation/coverage:** Separate operational sizing from strategy-rule compatibility; version and migrate snapshots deliberately. Test size and leverage changes with existing daily/four-hour positions and cooldowns. For actual rule changes, require an explicit supported migration or a clear preflight rejection before switching the service.

**Deployment/data care:** Never solve this by deleting the snapshot. Preserve actual entry quantity/leverage, pending state and cooldown history; add pre-deployment restore validation against a safe database copy.

### F10 — Medium; high confidence: dashboard trade prices can disagree with actual fills

**Evidence:** [src/lnmarkets_bot/engine/live_executor.py:233](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:233), [scripts/run_dashboard.py:613](/home/james/src/lnmbot/scripts/run_dashboard.py:613), [scripts/run_dashboard.py:736](/home/james/src/lnmbot/scripts/run_dashboard.py:736), [scripts/run_dashboard.py:1789](/home/james/src/lnmbot/scripts/run_dashboard.py:1789). Diagnostic `test_exit_order_price_disagrees_with_actual_fill`.

**Trigger and observed behavior:** Close four units with a reference price of 72,968 and actual venue fill of 70,000. All four order rows retain 72,968 while their fill rows correctly record 70,000. Dashboard history/closed-trade calculations read order prices. Exchange-reported BTC P&L can remain correct while displayed exit prices and derived return/USD quantities are misleading.

The deployed trader also records close-intent leverage, commonly 1x, rather than the original funded position's 5x. The dirty checkout fixes this particular leverage error by capturing `pos.leverage`; it does not fix the price/read-model discrepancy.

**Remediation/coverage:** Build reporting from canonical execution facts: actual fill, actual quantity/leverage and actual venue event time. Distinguish quote/decision timestamps from fills. Test slippage, mixed unit sizes, delayed external closure and positions spanning a configuration change.

**Deployment/data care:** Deploy the reviewed leverage correction as part of a tested release, not by copying the whole dirty tree. Backfill incorrect historical presentation only from retained execution evidence. Externally observed closures also need actual close time for correct daily attribution after an outage.

### F11 — Medium; high confidence: research liquidation funding treatment differs from the venue

**Evidence:** [scripts/replay_btc_close_range_native.py:78](/home/james/src/lnmbot/scripts/replay_btc_close_range_native.py:78), [scripts/investigate_btc_challenger_full_stack.py:175](/home/james/src/lnmbot/scripts/investigate_btc_challenger_full_stack.py:175). The replay adds signed cumulative funding to position margin when computing liquidation; the selected full-stack simulator uses that helper. Liquidation accounting then treats loss as initial margin, which does not correctly preserve the separate received-funding wallet cash flow.

LN Markets documents that paid funding reduces isolated margin, while received funding credits the wallet. Treating credits as extra isolated collateral therefore changes liquidation timing and survival. [Official futures documentation](https://docs.lnmarkets.com/en/futures).

**Intended behavior/consequence:** A replay presented as venue-native isolated behavior must separate wallet credits from collateral debits. Model mismatch weakens historical campaign survival and profitability claims. This is distinct from the live executor's use of actual venue liquidation and is not evidence of an actual production accounting loss.

**Remediation/coverage:** Separate collateral, wallet cash, fees, funding and realized inverse P&L in the existing simulator. Use deterministic long/short fixtures with positive/negative funding, fee reserves and liquidation. Re-run fixed approved scenarios and clearly version affected outputs; no new parameter search is necessary.

**Deployment/data care:** Do not change funded positions based on corrected research. Revisit any historical seed whose occupancy depends on the corrected model under F7.

## Other risks, unresolved requirements and targeted maintenance

These are not additional claims of observed production failures.

1. **Shared capital policy needs a precise contract.** Namespaced execution keys correctly separate MA daily/four-hour positions and breakout k0–k3; gross exposure is not netted across opposite strategies. Fixed breakout sizing and equity-based MA sizing share the wallet. However, the proposed portfolio reservation store is not integrated into funded admission, and there is no demonstrated full-campaign cash reservation. Fixed entries rely partly on venue rejection. [src/lnmarkets_bot/engine/live_executor.py:728](/home/james/src/lnmbot/src/lnmarkets_bot/engine/live_executor.py:728) estimates open margin as contracts/leverage, not current BTC collateral valued at the current mark. For example, 20,000 sats initially worth $20 become $40 at a doubled BTC price while this estimate remains $20. Whether a configured limit means initial margin or marked collateral is unresolved. Define cash available, gross notional, collateral valuation, reservation and MA-versus-breakout priority before implementing additional limits. Serial execution prevents an in-process parallel admission race but does not substitute for that policy.
2. **Closed-trade pagination and cash-flow completeness:** recovery must exhaust venue history, not assume the first page includes every relevant close. Dashboard wallet profitability based on a limited deposit/withdrawal view can be misleading with internal transfers or more than one page of history. The official API documents cursor pagination and bounded results. [Official API documentation](https://docs.lnmarkets.com/en/api). Verify all required cash-flow categories and label any incomplete metric; no such production account history was accessible here.
3. **MA external closure semantics:** missing external-close strategy hooks are logged. Decide whether manual close/liquidation should trigger cooldown, which return basis to use and whether same-side re-entry is allowed. Current signal-close price-based winner/loss classification is not automatically equivalent to net realized profitability. Do not silently reinterpret the approved rule during a refactor.
4. **Persistence instance identity:** [src/lnmarkets_bot/engine/portfolio_live.py:37](/home/james/src/lnmbot/src/lnmarkets_bot/engine/portfolio_live.py:37) keys strategy snapshots by class name, despite execution ownership being instance-based. Current MA and breakout classes differ, so no present collision was reproduced. Two instances of one class overwrite the same persistence namespace; an existing test even accepts this behavior. Move to stable instance IDs before permitting such configurations, with a one-time legacy mapping.
5. **Backtest claims exceed generic engine evidence:** [src/lnmarkets_bot/engine/backtest.py:154](/home/james/src/lnmbot/src/lnmarkets_bot/engine/backtest.py:154) calculates equity as balance plus signed notional rather than inverse unrealized P&L. The generic paper engine also lacks funded portfolio equivalence in collateral/funding/liquidation. `tests/test_engines_equivalence.py` exercises a no-order strategy; it cannot establish execution equivalence. Keep generic MA tooling, selected breakout replay and funded portfolio traces clearly distinguished. Correct accounting with small deterministic examples before using outputs for profitability claims.
6. **Refactor only at correctness boundaries:** centralize execution-event application and accounting; separate campaign state transitions from venue effects; isolate historical paper state; centralize snapshot compatibility and reporting read models. Consolidate duplicated liquidation/P&L math after its contract is tested. Avoid a broad rewrite of indicators or strategy research.
7. **Schema and concurrency limits:** existing migration tests pass, but that does not establish safe upgrades of the inaccessible production database or crash consistency across recorder transactions. The shared loop serializes this process; a second authorized runner would still require an explicit singleton/ownership mechanism. No second trader process was demonstrated.

## Deterministic exit and failure evidence

The existing full-stack fixture opens all four units through the adapter, execution engine, fake venue and SQLite recorder. I inspected its assertions, then exercised it independently with fault injection.

| Scenario | Observed result | Evidential limit |
|---|---|---|
| Four-unit range close | All four close; eight orders and expected per-unit accounting | Deterministic fake venue |
| Four-unit day-85 recovery | Same; correct recovery reason and campaign termination | Funding fixture does not model settlement races |
| Four-unit day-120 cap | Same; maximum-hold exit reaches all units | Historical bar simulation |
| Parent-only liquidation | Parent accounted; three survivors close | Does not cover simultaneous missing IDs |
| Each child-only liquidation | Parent and other children survive; later range exit closes only survivors; no slot reuse | One external event per poll |
| Four-unit short campaign | Symmetric entries and range exit pass | Not every short failure permutation tested |
| One survivor close repeatedly fails | Other units close; restart retries survivor successfully, eight final orders | Ordinary connection failure |
| Parent + child liquidate together | Trader exception before survivor close; clean restart recovers survivors | F2, confirmed |
| Fill persistence fails after close-order commit | Restart leaves eight orders but seven fills/P&L events | F3, confirmed |
| Final funding arrives with external close | Funding omitted | F5, confirmed |
| POST commit followed by HTTP 500 | Two mock remote trades, one returned ID | F1, confirmed client behavior; actual venue occurrence unknown |

Range, recovery and cap traces assert per-unit net sats respectively `(-9335,-10382,-10455,-12410)`, `(5760,4713,4640,2685)` and `(2711,1665,1592,-364)` under their fixed prices and fixture fees. Those numbers validate the fixture's accounting paths, not expected production returns.

Completed-bar filtering, sorted catch-up, duplicate timestamp rejection, daily/four-hour aggregation, prior-feature construction, direction modes, actual-fill addon rejection, same-open recovery blocking, seed hash checks and ordinary restart paths were inspected. Focused tests support their successful cases; F4 and F8 describe important gaps around that baseline. No general claim of lookahead-free research across every exploratory script is made.

## Remediation plan and acceptance gates

### Pass 1 — Make exposure and execution outcomes recoverable

Address F1–F3 together because submission identity, remote reconciliation, transactional result application and strategy-event replay share a persistence boundary. First add deterministic fault tests that require corrected behavior, then implement a durable command/result ledger and batch campaign reconciliation. Retain simple namespaced strategy ownership.

**Acceptance:** At every simulated timeout, HTTP error, process-crash and database-failure boundary, the final owned venue set equals the restored executor set; each economic event is counted exactly once; survivors are closed; unknown exposure prevents further admission without disabling management of known positions. All simultaneous closure subsets and event permutations pass.

### Pass 2 — Repair accounting and risk enforcement

Build F5/F6/F10 on the recoverable ledger. Add a funding cursor covering closed trades, atomic attribution, canonical fill-based reporting and restart-invariant daily-loss state. Audit existing records for incomplete closes and funding gaps before declaring corrected profitability.

**Acceptance:** Venue-derived sample statements reconcile to total and per-strategy sats, including fees and funding; restart/midnight/event-order permutations yield the same risk decisions; reporting shows actual fills and original quantities/leverage. Historical repairs have a dry-run diff and explicit provenance.

### Pass 3 — Restore causal strategy state and timely admission

Address F4/F8/F9, then F7 with an approved hypothetical liquidation model. Separate replay from permission to submit new entries. Add snapshot schema compatibility and stable instance identities. Preserve the approved direction/same-open rules rather than incorporating exploratory filters.

**Acceptance:** Continuous and restarted runs produce identical causal strategy state; expired signals never become late entries; configuration changes affect new size only; paper campaigns can end/liquidate without writing funded execution/P&L. Corrected historical occupancy cannot submit backdated funded orders.

### Pass 4 — Define portfolio capacity and validate fixed models

Resolve the capital-policy questions and implement only agreed limits/reservations. Correct F11 and generic backtest accounting before presenting equivalent performance claims. Use deterministic shared-wallet scenarios and the fixed approved strategy; do not run a new parameter search.

**Acceptance:** Two MA positions plus all four breakout units can be traced through admission, fees, funding, losses, rejection and exits under finite collateral. Affordability and exposure calculations have explicit units and agree with the chosen venue model.

### Release procedure for all passes

Create a reviewed, immutable release from explicitly selected changes, not the entire dirty checkout. Validate restore and migrations on an isolated copy of the actual database when authorized access is available. Record current positions and pending actions, maintain an operator rollback plan that preserves new trading records, and verify both restored state and venue ownership after switching. Do not deploy a version whose older snapshot or database format cannot understand the new state. These are recommendations; no release action was performed during this audit.

## Immediate precautions

* Avoid increasing capital or expanding strategy scope until F1–F6 are remediated and their failure tests pass.
* Have the operator independently compare all venue trades with recorded ownership after any submission ambiguity, trader crash or external campaign closure. The dashboard's aligned status alone is insufficient.
* Treat current strategy net P&L, daily-loss enforcement and historical occupancy as potentially incomplete in the identified conditions.
* Do not use a general halt/stop as a substitute for a reviewed entry-only pause: stopping the engine also stops management of existing exposure. Any operational precaution should preserve owned-position exits. No pause or stop was made here.
* Preserve the dirty checkout and the current database. Do not delete snapshots or restore an old database to bypass a startup failure.

## Checks performed and remaining limits

* Inspected applicable instructions, operational documents, release paths, selected protocols/candidate/rollout history, dirty changes, runtime strategy/data/execution/risk/persistence code, dashboard calculations, representative research accounting and associated tests.
* Compared actual deployed Python sources with the checkout and reran diagnostics against a copied deployed source tree.
* **130 focused existing tests passed in 12.99 seconds**, covering close-range machine/live adapter, funded campaign traces, portfolio execution, live executor, state persistence, cooldowns, dashboard, risk, migrations, live data and runner routing.
* **12 independent diagnostic tests passed against checkout (2.39 seconds) and deployed trader code (2.20 seconds).** Their assertions intentionally document the observed defects and one successful partial-close recovery; “passed” does not mean those defects are acceptable. Source: `tests/test_independent_audit.py` in this audit workspace only.
* Broader suite attempts did **not** complete within their time limits. One threaded aggregation attempt stalled under the sandbox; an outside-sandbox attempt advanced farther but also did not produce a final suite result. These attempts are not reported as passing, and their cause was not resolved. An initial isolated-copy failure was a missing copied reference fixture, corrected before the successful focused run; it was not a repository defect.
* No direct production DB integrity/migration scan, authenticated independent venue inventory, complete funding/cash-flow reconciliation, protected configuration audit, real outage simulation, real partial-fill behavior, load/concurrency stress test or exhaustive inspection of every research artifact was possible/performed. No production credentials were loaded, and no real trading request was made.
* Production flatness, historical occurrence of the defects, exact currently configured risk caps and complete economic totals remain unverified. Official venue documentation supports model checks but does not establish actual account state or a mutating-request idempotency guarantee.
* Passing tests and implementation correctness are separate from evidence that either strategy will remain profitable. This audit makes no profitability forecast.
