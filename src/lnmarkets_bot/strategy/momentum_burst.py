"""Research-only data-informed seven-day momentum-burst challenger."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .base import Bar, Strategy, StrategyState, TfPosition
from .intents import OrderIntent


@dataclass
class _BurstState:
    closes: deque[float] = field(default_factory=lambda: deque(maxlen=128))
    true_ranges: deque[float] = field(default_factory=lambda: deque(maxlen=128))
    sma_history: deque[float] = field(default_factory=lambda: deque(maxlen=32))
    ema_history: deque[float] = field(default_factory=lambda: deque(maxlen=32))
    ema: float | None = None
    verdict: int = 0
    bars_held: int = 0


class MomentumBurst(Strategy):
    """Trade filtered MA transitions for exactly seven calendar days."""

    DEFAULTS: ClassVar[dict[str, Any]] = {
        "tfs": ("1d", "4h"),
        "tolerance_pct": 0.005,
        "slope_bars": 5,
        "hold_bars": {"1d": 7, "4h": 42},
        "impulse_atr_min_1d": 0.5,
        "base_size_usd": 1000.0,
        "base_leverage": 5.0,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
    }

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        merged = {**self.DEFAULTS, **(params or {})}
        super().__init__(merged)
        self.tfs = tuple(merged["tfs"])
        self.tolerance_pct = float(merged["tolerance_pct"])
        self.slope_bars = int(merged["slope_bars"])
        self.hold_bars = {tf: int(merged["hold_bars"][tf]) for tf in self.tfs}
        self.impulse_atr_min_1d = float(merged["impulse_atr_min_1d"])
        self.base_size_usd = float(merged["base_size_usd"])
        self.base_leverage = float(merged["base_leverage"])
        self.size_multipliers = dict(merged["size_multipliers"])
        self._state = {tf: _BurstState() for tf in self.tfs}

    def on_startup(self, state: StrategyState) -> None:
        for tf in self.tfs:
            state.positions.setdefault(tf, TfPosition())

    def on_shutdown(self, state: StrategyState) -> None:
        return None

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        tf = bar.timeframe
        if tf not in self._state:
            return []
        indicator = self._state[tf]
        previous_close = indicator.closes[-1] if indicator.closes else None
        true_range = bar.high - bar.low
        if previous_close is not None:
            true_range = max(
                true_range, abs(bar.high - previous_close), abs(bar.low - previous_close)
            )
        indicator.closes.append(bar.close)
        indicator.true_ranges.append(true_range)
        if len(indicator.closes) < 21:
            return []

        closes = list(indicator.closes)
        sma = sum(closes[-20:]) / 20
        if indicator.ema is None:
            indicator.ema = sum(closes[-21:]) / 21
        else:
            indicator.ema = bar.close * (2 / 22) + indicator.ema * (20 / 22)
        indicator.sma_history.append(sma)
        indicator.ema_history.append(indicator.ema)

        tolerance = self.tolerance_pct
        if bar.close > sma * (1 + tolerance) and bar.close > indicator.ema * (1 + tolerance):
            verdict = 1
        elif bar.close < sma * (1 - tolerance) and bar.close < indicator.ema * (1 - tolerance):
            verdict = -1
        else:
            verdict = 0
        previous_verdict = indicator.verdict
        indicator.verdict = verdict

        pos = state.position(tf)
        if pos.side is not None:
            indicator.bars_held += 1
            if indicator.bars_held >= self.hold_bars[tf]:
                old_side = pos.side
                pos.side = None
                pos.qty_sats = 0
                pos.entry_ts = None
                indicator.bars_held = 0
                return [
                    OrderIntent.exit(
                        trigger_tf=tf,
                        reason=f"{tf} momentum burst timed exit after {self.hold_bars[tf]} bars",
                        metadata={"closed_side": old_side, "bars_held": self.hold_bars[tf]},
                    )
                ]
            return []

        if bar.warmup or verdict == 0 or verdict == previous_verdict:
            return []
        if not self._qualifies(tf=tf, direction=verdict):
            return []

        side = "long" if verdict > 0 else "short"
        pos.side = side
        pos.entry_ts = bar.ts
        pos.leverage = self.base_leverage
        indicator.bars_held = 0
        size = self.base_size_usd * self.size_multipliers.get(tf, 1.0)
        metadata = {
            "direction": verdict,
            "slope_bars": self.slope_bars,
            "hold_bars": self.hold_bars[tf],
        }
        if verdict > 0:
            return [
                OrderIntent.enter_long(
                    trigger_tf=tf,
                    size_usd=size,
                    leverage=self.base_leverage,
                    reason=f"{tf} filtered momentum burst long",
                    metadata=metadata,
                )
            ]
        return [
            OrderIntent.enter_short(
                trigger_tf=tf,
                size_usd=size,
                leverage=self.base_leverage,
                reason=f"{tf} filtered momentum burst short",
                metadata=metadata,
            )
        ]

    def _qualifies(self, *, tf: str, direction: int) -> bool:
        indicator = self._state[tf]
        offset = self.slope_bars + 1
        if (
            len(indicator.sma_history) < offset
            or len(indicator.ema_history) < offset
            or len(indicator.closes) < 6
            or len(indicator.true_ranges) < 20
        ):
            return False
        sma_slope = indicator.sma_history[-1] / indicator.sma_history[-offset] - 1
        ema_slope = indicator.ema_history[-1] / indicator.ema_history[-offset] - 1
        if direction * sma_slope <= 0 or direction * ema_slope <= 0:
            return False
        closes = list(indicator.closes)
        if tf == "1d":
            bar_return = closes[-1] / closes[-2] - 1
            atr_pct = sum(list(indicator.true_ranges)[-20:]) / 20 / closes[-1]
            return direction * bar_return / atr_pct > self.impulse_atr_min_1d
        prior_return = closes[-1] / closes[-6] - 1
        return direction * prior_return > 0
