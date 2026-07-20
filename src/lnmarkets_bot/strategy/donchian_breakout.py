"""Research-only Donchian close-breakout challenger.

The strategy is intentionally small and independent from the production
MA/cool-off strategy.  It is not wired into the live entrypoint.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .base import Bar, Strategy, StrategyState, TfPosition
from .intents import OrderIntent


@dataclass
class _ChannelState:
    highs: deque[float] = field(default_factory=deque)
    lows: deque[float] = field(default_factory=deque)


class DonchianBreakout(Strategy):
    """Enter on an N-bar close breakout and exit through an M-bar channel."""

    DEFAULTS: ClassVar[dict[str, Any]] = {
        "tfs": ("1d", "4h"),
        "entry_window": 55,
        "exit_window": 20,
        "base_size_usd": 1000.0,
        "base_leverage": 5.0,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
    }

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        merged = {**self.DEFAULTS, **(params or {})}
        super().__init__(merged)
        self.tfs = tuple(merged["tfs"])
        self.entry_window = int(merged["entry_window"])
        self.exit_window = int(merged["exit_window"])
        if self.entry_window <= self.exit_window or self.exit_window <= 0:
            raise ValueError("entry_window must be greater than exit_window > 0")
        self.base_size_usd = float(merged["base_size_usd"])
        self.base_leverage = float(merged["base_leverage"])
        self.size_multipliers = dict(merged["size_multipliers"])
        self._channels = {
            tf: _ChannelState(
                highs=deque(maxlen=self.entry_window),
                lows=deque(maxlen=self.entry_window),
            )
            for tf in self.tfs
        }

    def on_startup(self, state: StrategyState) -> None:
        for tf in self.tfs:
            state.positions.setdefault(tf, TfPosition())

    def on_shutdown(self, state: StrategyState) -> None:
        return None

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        tf = bar.timeframe
        if tf not in self._channels:
            return []
        channel = self._channels[tf]
        if len(channel.highs) < self.entry_window:
            channel.highs.append(bar.high)
            channel.lows.append(bar.low)
            return []

        # Channels use only completed bars preceding the current bar.
        entry_high = max(channel.highs)
        entry_low = min(channel.lows)
        exit_high = max(list(channel.highs)[-self.exit_window :])
        exit_low = min(list(channel.lows)[-self.exit_window :])
        channel.highs.append(bar.high)
        channel.lows.append(bar.low)
        if bar.warmup:
            return []

        pos = state.position(tf)
        size = self.base_size_usd * self.size_multipliers.get(tf, 1.0)
        metadata = {
            "entry_window": self.entry_window,
            "exit_window": self.exit_window,
            "entry_high": entry_high,
            "entry_low": entry_low,
            "exit_high": exit_high,
            "exit_low": exit_low,
        }

        if pos.side is None:
            if bar.close > entry_high:
                pos.side = "long"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
                return [
                    OrderIntent.enter_long(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} Donchian {self.entry_window}-bar upside breakout",
                        metadata=metadata,
                    )
                ]
            if bar.close < entry_low:
                pos.side = "short"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
                return [
                    OrderIntent.enter_short(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} Donchian {self.entry_window}-bar downside breakout",
                        metadata=metadata,
                    )
                ]
            return []

        intents: list[OrderIntent] = []
        if pos.side == "long" and bar.close < exit_low:
            intents.append(
                OrderIntent.exit(
                    trigger_tf=tf,
                    reason=f"{tf} Donchian {self.exit_window}-bar long exit",
                    metadata=metadata,
                )
            )
            pos.side = None
            pos.entry_ts = None
            if bar.close < entry_low:
                intents.append(
                    OrderIntent.enter_short(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} Donchian same-bar reversal to short",
                        metadata=metadata,
                    )
                )
                pos.side = "short"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
        elif pos.side == "short" and bar.close > exit_high:
            intents.append(
                OrderIntent.exit(
                    trigger_tf=tf,
                    reason=f"{tf} Donchian {self.exit_window}-bar short exit",
                    metadata=metadata,
                )
            )
            pos.side = None
            pos.entry_ts = None
            if bar.close > entry_high:
                intents.append(
                    OrderIntent.enter_long(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} Donchian same-bar reversal to long",
                        metadata=metadata,
                    )
                )
                pos.side = "long"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
        return intents
