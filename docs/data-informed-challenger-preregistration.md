# Data-informed momentum-burst challenger pre-registration

Registered after pre-July-2022 feature exploration but before calculating this
strategy's development-period or holdout trading P&L. Research only; it cannot
change production automatically.

## Exploration boundary and search ledger

Only native Binance perpetual bars closing at or before 2022-07-11 were loaded.
The complete first-stage search covered seven declared feature families and
four forward horizons per timeframe, recorded in
`runs/data_informed_exploration_pre2022.json`:

- five-bar SMA20/EMA21 slope alignment;
- current daily-direction agreement for 4h events;
- prior 20-bar breakout;
- ATR20 volatility regime;
- signal-candle impulse divided by ATR20;
- distance beyond the SMA20/EMA21 midpoint;
- alignment of the preceding five-bar return.

A second-stage check examined exactly five masks around the observed seven-day
continuation relationship: slope alone, prior momentum alone, slope plus prior
momentum, slope plus non-small impulse, and slope plus >2% MA distance.

## Observed development relationship

For raw 0.5%-tolerance MA directional transitions:

- 1d slope + non-small impulse had mean signed seven-day forward returns of
  +5.07%, +4.42%, and +3.64% across the three development windows.
- 4h slope + five-bar momentum had mean signed 42-bar forward returns of
  +0.77%, +1.36%, and +0.70%.
- The 1d continuation was not stable at 14 days, motivating a fixed seven-day
  holding period rather than an indefinite trend position.

## Frozen strategy

- Independent 1d and 4h isolated positions.
- Raw trend verdict matches production: close more than 0.5% above both SMA20
  and EMA21 is up; more than 0.5% below both is down; otherwise flat.
- An event exists only when a directional verdict differs from the preceding
  verdict, including a transition from flat.
- Both SMA20 and EMA21 must have moved in the event direction over five bars.
- 1d additionally requires signed one-bar return / ATR20 percentage > 0.5.
- 4h additionally requires the preceding five-bar return to have the event's
  sign.
- Enter only while flat. Ignore all signals while occupied.
- Exit exactly seven 1d bars or 42 4h bars after entry. No early reversal,
  stop, cooldown, resize, or same-bar re-entry.
- Fixed $1,000 notional per timeframe at 5x leverage.
- 10 bps trading fee and 5 bps slippage per fill.
- Funding stress is always-paid 1 bp per eight hours while open.

### Pre-P&L methodology correction

Before calculating any strategy P&L, the exploratory EMA was changed from
pandas' default initialization to the production strategy's exact SMA(21)
seed. The event study was regenerated. The selected relationships and 1d
statistics were unchanged; 4h slope-plus-momentum means became +1.01%, +1.36%,
and +0.70%. No strategy mechanic changed as a result.

## Development gate

The complete strategy is run over the three pre-2022 windows. It proceeds to
holdout only if combined stressed P&L is positive in at least two of three
windows and positive in aggregate. No mechanics may be changed after seeing
development trading P&L.

## Holdout acceptance

Using the same 2022-2026 holdout and locked champion benchmark as the first
challenger, all conditions must hold:

1. Higher combined P&L after corrected trading costs and funding stress.
2. No worse marked-to-market maximum drawdown.
3. At least three of four combined annual windows are profitable.
4. No theoretical 5x isolated-liquidation wick using native 4h highs/lows.

Failing any condition leaves the locked MA/cool-off strategy in production.
