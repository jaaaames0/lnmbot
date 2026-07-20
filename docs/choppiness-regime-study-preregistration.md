# Choppiness-regime study: preregistration

## Question

Does a live-available, non-directional measure of recent BTC market structure
identify conditions in which the locked 1d or 4h MA/cool-off strategy has
meaningfully different net outcomes?

This is exploratory research only. It does not change the live bot or its
configuration.

## Indicator

For each completed strategy bar, calculate Choppiness Index (CHOP):

```
100 * log10(sum(true_range, n) / (highest_high(n) - lowest_low(n))) / log10(n)
```

where true range uses the prior completed close. Only the current and earlier
completed bars are used. The fixed lookbacks are 14, 30, and 90 bars. The
fixed classifications are:

- `trend`: CHOP < 38.2
- `neutral`: 38.2 <= CHOP <= 61.8
- `chop`: CHOP > 61.8

## Phase 1: descriptive conditional outcomes

Replay the locked production rule over July 2022--July 2026. Assign every
closed trade its CHOP classification at entry, then report count, net P&L,
mean/median P&L, win rate, and conservative 1 bp per 8h funding-stressed P&L
by classification and annual window.

No conclusion is drawn from a single period or annual window.

## Phase 2: limited sizing overlays

The base strategy's entries, exits, tolerance, and cooldown state are held
unchanged. Only realised trade notional is scaled according to its entry CHOP
classification. This is intentionally a linear attribution test, not a claim
that the bot may yet use dynamic sizing.

The complete candidate set is:

| Name | trend | neutral | chop |
| --- | ---: | ---: | ---: |
| baseline | 1.00x | 1.00x | 1.00x |
| reduce_chop | 1.00x | 1.00x | 0.50x |
| boost_trend | 1.25x | 1.00x | 1.00x |
| trend_and_reduce_chop | 1.25x | 1.00x | 0.50x |

For each timeframe, select one `(lookback, overlay)` by the highest median
annual conservative net P&L in 2022-23 and 2023-24; ties break by total
training P&L then lower lookback then the less aggressive overlay order shown
above. Evaluate the selected candidate unchanged in 2024-25 and 2025-26.

The fixed live rule is reported as a control. Trading costs already embedded in
the backtest are 10 bps per fill plus 5 bps slippage. Historical funding is not
available in the candle fixture, so the conservative one-sided 1 bp per 8h
stress assumption is applied equally to every candidate.

## Decision rule

No overlay is eligible for implementation unless it beats the baseline in both
untouched annual holdout windows after the funding stress assumption, retains
at least the baseline's closed-trade drawdown behaviour, and has a plausible
nearby parameter plateau. Otherwise the regime hypothesis is rejected or
deferred.
