from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lnmarkets_bot.strategy.base import Bar, StrategyState
from lnmarkets_bot.strategy.donchian_breakout import DonchianBreakout


def _bar(number: int, *, high: float, low: float, close: float) -> Bar:
    return Bar(
        ts=datetime(2020, 1, 1, tzinfo=UTC) + timedelta(days=number),
        open=close,
        high=high,
        low=low,
        close=close,
        volume=1.0,
        timeframe="1d",
    )


def test_requires_longer_entry_channel_than_exit_channel() -> None:
    with pytest.raises(ValueError):
        DonchianBreakout(params={"entry_window": 10, "exit_window": 10})


def test_current_bar_is_excluded_from_entry_channel() -> None:
    strategy = DonchianBreakout(params={"tfs": ("1d",), "entry_window": 3, "exit_window": 2})
    state = StrategyState()
    strategy.on_startup(state)
    for number, price in enumerate((10.0, 11.0, 12.0)):
        assert strategy.on_bar(_bar(number, high=price, low=price - 1, close=price), state) == []

    intents = strategy.on_bar(_bar(3, high=14.0, low=12.0, close=13.0), state)

    assert [intent.kind.value for intent in intents] == ["entry"]
    assert intents[0].side.value == "long"
    assert intents[0].metadata["entry_high"] == 12.0


def test_short_exit_can_reverse_long_on_same_close() -> None:
    strategy = DonchianBreakout(params={"tfs": ("1d",), "entry_window": 3, "exit_window": 2})
    state = StrategyState()
    strategy.on_startup(state)
    for number, price in enumerate((10.0, 11.0, 12.0)):
        strategy.on_bar(_bar(number, high=price, low=price - 1, close=price), state)
    strategy.on_bar(_bar(3, high=14.0, low=12.0, close=13.0), state)

    intents = strategy.on_bar(_bar(4, high=12.0, low=7.0, close=8.0), state)

    assert [intent.kind.value for intent in intents] == ["exit", "entry"]
    assert intents[1].side.value == "short"
    assert state.position("1d").side == "short"
