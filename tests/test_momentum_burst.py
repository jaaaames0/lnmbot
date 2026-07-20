from __future__ import annotations

from datetime import UTC, datetime, timedelta

from lnmarkets_bot.strategy.base import Bar, StrategyState, TfPosition
from lnmarkets_bot.strategy.momentum_burst import MomentumBurst


def _bar(number: int, close: float, timeframe: str = "1d") -> Bar:
    return Bar(
        ts=datetime(2020, 1, 1, tzinfo=UTC) + timedelta(days=number),
        open=close,
        high=close * 1.002,
        low=close * 0.998,
        close=close,
        volume=1,
        timeframe=timeframe,
    )


def test_qualifier_requires_aligned_sma_and_ema_slopes() -> None:
    strategy = MomentumBurst(params={"tfs": ("4h",), "hold_bars": {"4h": 42}})
    indicator = strategy._state["4h"]
    indicator.sma_history.extend([100, 101, 102, 103, 104, 105])
    indicator.ema_history.extend([100, 101, 102, 103, 104, 105])
    indicator.closes.extend([100, 101, 102, 103, 104, 105])
    indicator.true_ranges.extend([1] * 20)

    assert strategy._qualifies(tf="4h", direction=1)
    assert not strategy._qualifies(tf="4h", direction=-1)


def test_position_exits_after_exact_registered_hold() -> None:
    strategy = MomentumBurst(params={"tfs": ("1d",), "hold_bars": {"1d": 7}})
    state = StrategyState(positions={"1d": TfPosition(side="long")})
    strategy.on_startup(state)
    indicator = strategy._state["1d"]
    indicator.closes.extend([100] * 21)
    indicator.true_ranges.extend([1] * 21)
    indicator.ema = 100

    for number in range(6):
        assert strategy.on_bar(_bar(number, 100), state) == []
    intents = strategy.on_bar(_bar(6, 100), state)

    assert [intent.kind.value for intent in intents] == ["exit"]
    assert state.position("1d").side is None


def test_signals_are_ignored_while_position_is_occupied() -> None:
    strategy = MomentumBurst(params={"tfs": ("1d",), "hold_bars": {"1d": 7}})
    state = StrategyState(positions={"1d": TfPosition(side="short")})
    strategy.on_startup(state)
    indicator = strategy._state["1d"]
    indicator.closes.extend([100] * 21)
    indicator.true_ranges.extend([1] * 21)
    indicator.ema = 100

    assert strategy.on_bar(_bar(0, 110), state) == []
    assert state.position("1d").side == "short"
