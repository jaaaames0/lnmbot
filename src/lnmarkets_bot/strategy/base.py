"""Strategy abstract base + Bar + StrategyState.

This is the contract the engines use. The architecture rule says: the same
Strategy implementation runs in backtest and in live. To make that hold,
the strategy interface must accept everything it could ever need from a
data feed (bar, fills, account state) and must NOT accept anything that's
specific to one mode (no httpx requests, no websockets, no asyncio).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable

from .intents import OrderIntent

if TYPE_CHECKING:
    from .state import FillEvent  # noqa: F401  (re-export)


@dataclass(frozen=True)
class Bar:
    """One bar of price data — strategy-input-neutral.

    `timeframe` is a free-form string ("1m", "4h", "1d", ...). It's a tag, not
    a validated enum: the strategy interprets it and the engine doesn't enforce.
    Default "1m" preserves backward compatibility with v0 callers that pass
    raw 1m bars.
    """

    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str = "1m"
    # Historical bars used only to initialize a live strategy's indicators.
    # They update strategy state but must never create live orders.
    warmup: bool = False


@dataclass
class TfPosition:
    """One timeframe's independent (isolated-margin) position.

    Under isolated margin each subscribed timeframe maintains its own position
    with its own collateral and P&L. A signal on timeframe X only mutates
    `state.positions["X"]` — never any other TF.
    """

    side: str | None = None  # "long" | "short" | None
    qty_sats: int = 0  # signed: + long, - short, 0 flat
    entry_price_usd: float | None = None
    entry_ts: datetime | None = None
    leverage: float = 1.0


@dataclass
class StrategyState:
    """Live state passed to every `on_bar` call.

    Strategies may mutate but must not depend on its starting shape beyond
    what's documented here.
    """

    # Per-TF positions, keyed by timeframe ("1d", "4h", ...). Empty by default;
    # the engine populates this from `cfg.tfs` at startup.
    positions: dict[str, TfPosition] = field(default_factory=dict)

    # Equity (engine computes from account snapshots)
    equity_sats: int = 0
    balance_sats: int = 0

    # Recent fills available for inspection
    last_fill_price_usd: float | None = None

    # Sliding window of the most recent N bars, oldest-first
    history_size: int = 256
    history: deque[Bar] = field(default_factory=lambda: deque(maxlen=256))

    def push_bar(self, bar: Bar) -> None:
        # Initialize history with the right maxlen if it was zero before.
        if self.history.maxlen != self.history_size:
            self.history = deque(self.history, maxlen=self.history_size)
        self.history.append(bar)

    def position(self, tf: str) -> TfPosition:
        """Get-or-create the per-TF position. Strategies use this to access
        their TF's state cleanly."""
        pos = self.positions.get(tf)
        if pos is None:
            pos = TfPosition()
            self.positions[tf] = pos
        return pos


class Strategy(ABC):
    """The contract.

    Lifecycle:
      strategy = MyStrategy(params)
      strategy.on_startup(state)               # once, before any bars
      for bar in feed: strategy.on_bar(bar, state) -> [OrderIntent, ...]
      for fill in fills: strategy.on_fill(fill, state)
      strategy.on_shutdown(state)              # once, at end (clean or halt)
    """

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        self.params = dict(params or {})

    @abstractmethod
    def on_startup(self, state: StrategyState) -> None:
        ...

    @abstractmethod
    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        ...

    def on_fill(self, fill: Any, state: StrategyState) -> None:
        """Optional. Default does nothing."""
        return None

    def on_shutdown(self, state: StrategyState) -> None:
        """Optional. Default does nothing."""
        return None


def import_strategy(dotted: str) -> Strategy:
    """Resolve `module:ClassName` to an instantiated strategy."""
    if ":" not in dotted:
        raise ValueError(
            f"strategy spec must be 'module.path:ClassName', got {dotted!r}"
        )
    module_name, class_name = dotted.split(":", 1)
    import importlib

    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name)
    if not issubclass(cls, Strategy):
        raise TypeError(f"{class_name} is not a Strategy subclass")
    return cls()


def intents_to_list(intents: Iterable[OrderIntent] | OrderIntent | None) -> list[OrderIntent]:
    if intents is None:
        return []
    if isinstance(intents, OrderIntent):
        return [intents]
    return list(intents)
