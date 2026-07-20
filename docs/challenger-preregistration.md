# Donchian challenger pre-registration

Registered before downloading or evaluating the pre-July-2022 development
dataset. This experiment is research-only and cannot change the live strategy.

## Hypothesis

BTC trends persist after a close escapes its established price range. Entering
only on a close outside a long Donchian channel, then exiting through a shorter
opposite channel, should capture trends while the channel hysteresis suppresses
chop. The strategy uses no moving averages, cool-offs, macro data, volume,
machine learning, stop loss, or intrabar entry signal.

## Fixed mechanics

- Independent isolated 1d and 4h positions.
- Long entry: close above the highest high of the previous N bars.
- Short entry: close below the lowest low of the previous N bars.
- Long exit: close below the lowest low of the previous M bars.
- Short exit: close above the highest high of the previous M bars.
- Current bar is excluded from every channel.
- Same-bar reversal is allowed only if the close also breaks the opposite
  N-bar entry channel.
- Fixed $1,000 notional per timeframe and 5x leverage for comparison.
- 10 bps fee and 5 bps slippage on every fill.
- Funding stress: 1 bp per eight hours, always treated as paid while open.

## Candidate limit

Exactly three entry/exit pairs will be considered: 20/10, 55/20, and 100/50.
No additional pair or indicator may be added after seeing results.

Each timeframe is selected independently using only these development windows:

1. 2019-09-09 through 2020-07-10
2. 2020-07-11 through 2021-07-10
3. 2021-07-11 through 2022-07-10

Ranking is lexicographic: most positive windows, then highest median annual
net P&L, then highest total net P&L. The selected pair is frozen before any
challenger result from 2022-07-11 onward is calculated.

## Untouched mechanical holdout

The frozen per-timeframe selections are evaluated once over four windows from
2022-07-11 through 2026-07-11. Although the BTC chart and locked strategy have
already been studied in these years, the challenger implementation and
selection cannot inspect this period before freezing.

## Acceptance test

The challenger replaces nothing unless all conditions hold:

1. Combined holdout P&L after trading costs and funding stress exceeds the
   locked MA/cool-off strategy at identical $1,000-per-TF notional.
2. Marked-to-market maximum drawdown is no worse than the locked strategy.
3. At least three of four combined annual holdout windows are net profitable.
4. No open challenger position touches its theoretical 5x isolated-liquidation
   level using native 4h highs/lows.

Failing any condition means the current strategy remains champion. A profitable
but inferior challenger may be retained for research but not deployed.
