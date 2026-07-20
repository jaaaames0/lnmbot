"""The placeholder strategy. Truly does nothing — emits zero intents.

This is the proof the backtest->live code path is identical: this same class
runs under both engines with the same input and produces zero signals in both.
"""
from __future__ import annotations

from typing import Any

from .base import Bar, Strategy, StrategyState
from .intents import OrderIntent


class DoNothing(Strategy):
    def __init__(self, params: dict[str, Any] | None = None) -> None:
        super().__init__(params)

    def on_startup(self, state: StrategyState) -> None:
        return None

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        state.push_bar(bar)
        return []
