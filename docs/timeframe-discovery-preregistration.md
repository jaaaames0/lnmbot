# Non-standard timeframe discovery protocol

## Purpose

Evaluate whether the existing SMA20/EMA21, tolerance-band, isolated-position
MA/cool-off framework has a robust positive result on conventional but
currently untraded BTC timeframes. This is research only. It must not change
the live 1d/4h strategy, sizing, risk caps, or execution code.

## Frozen universe

The full candidate universe is: `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`,
and `1w`. Existing 1d and 4h are reference controls, while 1h is a known weak
control. The discovery question concerns `2h`, `6h`, `8h`, `12h`, and `1w`.

Data is Binance BTCUSDT perpetual candles from 2022-07-11 through 2026-07-11.
Native one-minute candles are first aggregated to one-hour candles, then all
sub-daily candidate bars are derived from that cached one-hour source using
UTC, right-labelled, left-closed bars. Weekly candles are derived from the
native daily cache with UTC Monday 00:00 closes. A strategy receives a bar at
the bar's close time, matching live-source semantics.

## Strategy and cost model

Every candidate uses the existing MA-cross mechanics:

- SMA20 and EMA21 on closes;
- same-bar flips enabled;
- winner and loss cool-offs independently consume every verdict transition;
- $1,000 fixed notional and 5x leverage for comparability only.

The evaluator applies 10 bps trading fee and 5 bps slippage per fill. Funding
is a conservative one-sided sensitivity: every open hour pays 1 bp per eight
hours on current position notional. It is included in both selection and
validation ranking.

## Candidate grids

Tolerances for all timeframes are `0.2%`, `0.3%`, `0.4%`, `0.5%`, `0.6%`, and
`0.8%`.

For `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, and `1d`, winner cool-offs are none;
`2% / 4`, `3% / 8`, `3% / 12`, `5% / 8`, `5% / 12`, and `8% / 12`. Loss
cool-offs are none; `2% / 3`; `3% / 3`; and `5% / 3`.

Weekly winner cool-offs are none; `5% / 2`; `10% / 2`; and `10% / 4`.
Weekly loss cool-offs are none; `5% / 1`; `10% / 1`; and `10% / 2`.

The candidate rank is median annual stressed P&L in the first two windows,
then their combined stressed P&L, then lower tolerance. No later-window
information may select a rule.

## Temporal evaluation

- Selection: 2022-07-11 to 2024-07-11 (`2022-23`, `2023-24`).
- Unchanged validation: `2024-25` and `2025-26`.

The 2024-26 period is read only after each timeframe's rule has been selected.
This is an exploratory multiple-timeframe search, so a passing result is not
claimed as definitive out-of-sample proof.

## Viability gate

A non-standard timeframe is only labelled **candidate for further research**
when its selected rule has positive stressed P&L in both validation years,
positive stressed P&L in at least three of all four annual windows, positive
aggregate stressed P&L, and at least 12 closed trades across the four years.

No result may be added to live trading without a separate portfolio-overlap,
drawdown, and isolated-liquidation-wick study. Additional BTC timeframes are
correlated exposure, not diversification.
