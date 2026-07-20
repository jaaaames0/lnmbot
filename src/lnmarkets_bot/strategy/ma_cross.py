"""Multi-timeframe MA-cross trend strategy, v1.1.

Per-TF isolated positions. Each subscribed timeframe (1d, 4h, ...) maintains
its own independent position. A signal on TF X only mutates
`state.positions[X]` — never any other TF.

Idea (paraphrased from the strategy discussion):
  - For each subscribed TF, compute SMA(20) and EMA(21) on closes.
  - Per bar: verdict is UP_TRUE / DOWN_TRUE / FLAT, gated by a tolerance band.
  - On every verdict transition (UP_FIRST, DOWN_FIRST) the strategy emits an
    OrderIntent with `trigger_tf=bar.timeframe`. The engine routes the intent
    to that TF's position slot.
  - Same-bar flips are allowed and stay within the same TF.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .base import Bar, Strategy, StrategyState, TfPosition
from .intents import OrderIntent


@dataclass
class _TfState:
    closes: deque[float] = field(default_factory=lambda: deque(maxlen=64))
    highs: deque[float] = field(default_factory=lambda: deque(maxlen=128))
    lows: deque[float] = field(default_factory=lambda: deque(maxlen=128))
    true_ranges: deque[float] = field(default_factory=lambda: deque(maxlen=128))
    previous_close: float | None = None
    sma: float | None = None
    ema: float | None = None
    ema_seeded: bool = False
    verdict: str = "FLAT"  # "UP_TRUE" | "DOWN_TRUE" | "FLAT"
    chop: float | None = None


class MaCross(Strategy):
    """Multi-TF MA-cross trend follower with isolated per-TF positions.

    Param keys (all optional; defaults shown):
        tfs:                tuple[str, ...]   ("1d", "4h")
        tolerance_pct:      float             0.002
        base_size_usd:      float             1000.0
        base_leverage:      float             2.0
        size_multipliers:   dict[str, float]  {"1d":1.0, "4h":1.0}
        same_bar_flip:      bool              True
        warmup_bars_per_tf: int               21
    """

    DEFAULTS: ClassVar[dict[str, Any]] = {
        "tfs": ("1d", "4h"),
        "tolerance_pct": 0.005,  # v1.3 2y matrix winner
        "base_size_usd": 1000.0,
        "base_leverage": 2.0,
        "size_multipliers": {"1d": 1.0, "4h": 1.0},
        "same_bar_flip": True,
        "warmup_bars_per_tf": 21,
        # Cool-off heuristic (per-TF): after a per-TF trade closes with P&L
        # >= cooldown_threshold_pct[tf], suppress the next
        # cooldown_signal_count[tf] transitions on that TF.
        # v1.3 2y matrix winner: every verdict transition consumes a slot.
        # 1d=3%/12, 4h=5%/11. This deliberately includes transitions to and
        # from FLAT; see DEPLOYMENT.md for the rationale and comparison.
        "cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
        "cooldown_signal_count": {"1d": 12, "4h": 11},
        # Loss cool-off is independent from the winner cool-off above. Its
        # values were selected on the first year and passed the second-year
        # holdout: 1d=5%/3, 4h=2%/4.
        "loss_cooldown_threshold_pct": {"1d": 0.05, "4h": 0.02},
        "loss_cooldown_signal_count": {"1d": 3, "4h": 4},
        # Optional 4h-only regime overlay. It deliberately changes only the
        # requested notional of a new entry; exits and cooldown state remain
        # exactly the locked production rule.
        "chop_4h_reduce_enabled": False,
        "chop_lookback": 14,
        "chop_high_threshold": 61.8,
        "chop_high_size_multiplier": 0.5,
        # What consumes a cool-off slot:
        # - verdict_transition: every verdict change, including FLAT (v1.3)
        # - directional_transition: only a change into UP_TRUE/DOWN_TRUE
        # - order_opportunity: only a change that would place an order
        "cooldown_mode": "verdict_transition",
    }

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        merged = {**self.DEFAULTS, **(params or {})}
        super().__init__(merged)
        self.tfs: tuple[str, ...] = tuple(merged["tfs"])
        self.tolerance_pct = float(merged["tolerance_pct"])
        self.base_size_usd = float(merged["base_size_usd"])
        self.base_leverage = float(merged["base_leverage"])
        self.size_multipliers: dict[str, float] = dict(merged["size_multipliers"])
        self.same_bar_flip = bool(merged["same_bar_flip"])
        self.warmup = int(merged["warmup_bars_per_tf"])
        self.cooldown_mode = str(merged["cooldown_mode"])
        valid_cooldown_modes = {
            "verdict_transition",
            "directional_transition",
            "order_opportunity",
        }
        if self.cooldown_mode not in valid_cooldown_modes:
            raise ValueError(
                f"cooldown_mode must be one of {sorted(valid_cooldown_modes)}, "
                f"got {self.cooldown_mode!r}"
            )
        self.cooldown_threshold_pct: dict[str, float] = (
            {tf: float(merged["cooldown_threshold_pct"].get(tf, 0.0)) for tf in self.tfs}
            if isinstance(merged["cooldown_threshold_pct"], dict)
            else {tf: float(merged["cooldown_threshold_pct"]) for tf in self.tfs}
        )
        self.cooldown_signal_count: dict[str, int] = (
            {tf: int(merged["cooldown_signal_count"].get(tf, 0)) for tf in self.tfs}
            if isinstance(merged["cooldown_signal_count"], dict)
            else {tf: int(merged["cooldown_signal_count"]) for tf in self.tfs}
        )
        self.loss_cooldown_threshold_pct: dict[str, float] = (
            {tf: float(merged["loss_cooldown_threshold_pct"].get(tf, 0.0)) for tf in self.tfs}
            if isinstance(merged["loss_cooldown_threshold_pct"], dict)
            else {tf: float(merged["loss_cooldown_threshold_pct"]) for tf in self.tfs}
        )
        self.loss_cooldown_signal_count: dict[str, int] = (
            {tf: int(merged["loss_cooldown_signal_count"].get(tf, 0)) for tf in self.tfs}
            if isinstance(merged["loss_cooldown_signal_count"], dict)
            else {tf: int(merged["loss_cooldown_signal_count"]) for tf in self.tfs}
        )
        self.chop_4h_reduce_enabled = bool(merged["chop_4h_reduce_enabled"])
        self.chop_lookback = int(merged["chop_lookback"])
        self.chop_high_threshold = float(merged["chop_high_threshold"])
        self.chop_high_size_multiplier = float(merged["chop_high_size_multiplier"])
        if not 2 <= self.chop_lookback <= 128:
            raise ValueError("chop_lookback must be between 2 and 128")
        if not 0.0 <= self.chop_high_threshold <= 100.0:
            raise ValueError("chop_high_threshold must be between 0 and 100")
        if not 0.0 < self.chop_high_size_multiplier <= 1.0:
            raise ValueError("chop_high_size_multiplier must be in (0, 1]")

        # Per-TF indicator + verdict state (one state machine per TF)
        self.tf_state: dict[str, _TfState] = {tf: _TfState() for tf in self.tfs}
        # Cool-off tracking per TF: when set > 0, the next N transitions on
        # this TF are suppressed (N = cooldown_signal_count at trigger time).
        self._suppressed_signals: dict[str, int] = {tf: 0 for tf in self.tfs}
        self._loss_suppressed_signals: dict[str, int] = {tf: 0 for tf in self.tfs}
        # Last per-TF closed trade P&L as a fraction of notional.
        self._last_trade_pnl_pct: dict[str, float] = {tf: 0.0 for tf in self.tfs}
        self._restart_pending: set[str] = set()

    # ---- lifecycle ----

    def on_startup(self, state: StrategyState) -> None:
        for tf in self.tfs:
            state.positions.setdefault(tf, TfPosition())
            if state.position(tf).side is not None:
                self._restart_pending.add(tf)

    def on_shutdown(self, state: StrategyState) -> None:
        return None

    # ---- per-bar logic ----

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        if bar.timeframe == "1m":
            state.push_bar(bar)

        tf = bar.timeframe
        if tf not in self.tf_state:
            return []  # not subscribed to this TF

        ts = self.tf_state[tf]
        ts.closes.append(bar.close)
        self._update_chop(ts, bar)

        if len(ts.closes) < self.warmup:
            return []  # warmup

        # Compute SMA20
        closes = list(ts.closes)
        sma = sum(closes[-20:]) / 20.0
        ts.sma = sma

        # Compute EMA21 (seed with SMA(21) on first computation)
        if not ts.ema_seeded:
            ts.ema = sum(closes[-21:]) / 21.0
            ts.ema_seeded = True
        else:
            alpha = 2.0 / 22.0
            ts.ema = bar.close * alpha + ts.ema * (1.0 - alpha)

        # Verdict for THIS TF only
        tol = self.tolerance_pct
        if bar.close > ts.sma * (1 + tol) and bar.close > ts.ema * (1 + tol):
            verdict = "UP_TRUE"
        elif bar.close < ts.sma * (1 - tol) and bar.close < ts.ema * (1 - tol):
            verdict = "DOWN_TRUE"
        else:
            verdict = "FLAT"

        prev = ts.verdict
        ts.verdict = verdict
        if bar.warmup:
            return []
        if tf in self._restart_pending:
            self._restart_pending.remove(tf)
            return self._restart_catch_up(tf=tf, verdict=verdict, bar=bar, state=state)
        if verdict == prev:
            return []

        # Cool-off: if this TF recently closed a big winner, suppress
        # transitions until the suppression counter runs out.
        cooldowns_before = self._active_cooldowns(tf)
        if cooldowns_before and self._cooldown_consumes(tf=tf, verdict=verdict, state=state):
            for cooldown_type in cooldowns_before:
                if cooldown_type == "winner":
                    self._suppressed_signals[tf] -= 1
                else:
                    self._loss_suppressed_signals[tf] -= 1
            # Record every suppressed verdict transition. FLAT transitions
            # consume a slot too; retaining the verdict makes that explicit in
            # the signal audit rather than silently hiding it in a trade log.
            return [
                OrderIntent.noop(
                    trigger_tf=tf,
                    reason="cool_off",
                    metadata={
                        "previous_verdict": prev,
                        "verdict": verdict,
                        "cooldown_types": sorted(cooldowns_before),
                        "winner_remaining_before": cooldowns_before.get("winner", 0),
                        "winner_remaining_after": self._suppressed_signals[tf],
                        "loss_remaining_before": cooldowns_before.get("loss", 0),
                        "loss_remaining_after": self._loss_suppressed_signals[tf],
                    },
                )
            ]

        # A neutral transition does not place an order, but it is still a
        # strategy event and must be visible when reconciling against a chart.
        if verdict == "FLAT":
            return [
                OrderIntent.noop(
                    trigger_tf=tf,
                    reason="verdict_flat",
                    metadata={"previous_verdict": prev, "verdict": verdict},
                )
            ]

        # Transition on this TF — only mutate THIS TF's position
        return self._on_transition(
            tf=tf,
            previous_verdict=prev,
            side=verdict,
            bar=bar,
            state=state,
        )

    def _restart_catch_up(
        self, *, tf: str, verdict: str, bar: Bar, state: StrategyState
    ) -> list[OrderIntent]:
        """Safely resolve a restored position after a restart gap."""
        pos = state.position(tf)
        target_side = {"UP_TRUE": "long", "DOWN_TRUE": "short"}.get(verdict)
        if pos.side is not None and target_side is not None and pos.side != target_side:
            old_side = pos.side
            pos.side = None
            pos.qty_sats = 0
            pos.entry_ts = None
            return [
                OrderIntent.exit(
                    trigger_tf=tf,
                    reason=f"restart_catch_up closes {old_side} against {verdict}",
                    metadata={
                        "restart_catch_up": True,
                        "restored_side": old_side,
                        "verdict": verdict,
                        "bar_ts": bar.ts.isoformat(),
                    },
                )
            ]
        return [
            OrderIntent.noop(
                trigger_tf=tf,
                reason="restart_state_aligned",
                metadata={
                    "restart_catch_up": True,
                    "position_side": pos.side,
                    "verdict": verdict,
                    "bar_ts": bar.ts.isoformat(),
                },
            )
        ]

    def _cooldown_consumes(
        self,
        *,
        tf: str,
        verdict: str,
        state: StrategyState,
    ) -> bool:
        """Whether this verdict transition spends one cool-off slot."""
        if self.cooldown_mode == "verdict_transition":
            return True
        if self.cooldown_mode == "directional_transition":
            return verdict in {"UP_TRUE", "DOWN_TRUE"}
        return self._would_place_order(tf=tf, side=verdict, state=state)

    def _active_cooldowns(self, tf: str) -> dict[str, int]:
        """Return active winner/loss cool-offs for one timeframe."""
        active: dict[str, int] = {}
        if self._suppressed_signals[tf] > 0:
            active["winner"] = self._suppressed_signals[tf]
        if self._loss_suppressed_signals[tf] > 0:
            active["loss"] = self._loss_suppressed_signals[tf]
        return active

    @staticmethod
    def _would_place_order(*, tf: str, side: str, state: StrategyState) -> bool:
        """Return whether applying this directional transition would create an order."""
        if side not in {"UP_TRUE", "DOWN_TRUE"}:
            return False
        pos = state.position(tf)
        target_side = "long" if side == "UP_TRUE" else "short"
        return pos.side != target_side

    # ---- transition handler ----

    def _update_chop(self, tf_state: _TfState, bar: Bar) -> None:
        """Update CHOP from completed bars, without any look-ahead."""
        true_range = bar.high - bar.low
        if tf_state.previous_close is not None:
            true_range = max(
                true_range,
                abs(bar.high - tf_state.previous_close),
                abs(bar.low - tf_state.previous_close),
            )
        tf_state.highs.append(bar.high)
        tf_state.lows.append(bar.low)
        tf_state.true_ranges.append(true_range)
        tf_state.previous_close = bar.close
        if len(tf_state.true_ranges) < self.chop_lookback:
            tf_state.chop = None
            return
        total_range = max(list(tf_state.highs)[-self.chop_lookback :]) - min(
            list(tf_state.lows)[-self.chop_lookback :]
        )
        travelled = sum(list(tf_state.true_ranges)[-self.chop_lookback :])
        if total_range <= 0.0 or travelled <= 0.0:
            tf_state.chop = None
            return
        # CHOP = 100 * log10(sum(TR) / range) / log10(n).  Using natural
        # logs is algebraically identical and avoids a base-specific helper.
        from math import log

        tf_state.chop = 100.0 * log(travelled / total_range) / log(self.chop_lookback)

    def _entry_size_and_metadata(self, tf: str) -> tuple[float, dict[str, Any]]:
        """Return new-entry notional plus auditable CHOP regime metadata."""
        base_size = self.base_size_usd * self.size_multipliers.get(tf, 1.0)
        chop = self.tf_state[tf].chop
        multiplier = 1.0
        regime = "not_applicable"
        if tf == "4h" and self.chop_4h_reduce_enabled:
            regime = "unknown" if chop is None else "neutral_or_trend"
            if chop is not None and chop > self.chop_high_threshold:
                multiplier = self.chop_high_size_multiplier
                regime = "high_chop"
        return (
            base_size * multiplier,
            {
                "entry_size_base_usd": base_size,
                "entry_size_multiplier": multiplier,
                "chop_4h_reduce_enabled": self.chop_4h_reduce_enabled,
                "chop_lookback": self.chop_lookback if tf == "4h" else None,
                "chop_value": chop if tf == "4h" else None,
                "chop_regime": regime,
            },
        )

    def _on_transition(
        self,
        *,
        tf: str,
        previous_verdict: str,
        side: str,
        bar: Bar,
        state: StrategyState,
    ) -> list[OrderIntent]:
        """Apply a verdict transition to THIS TF's position only."""
        pos = state.position(tf)
        size, entry_metadata = self._entry_size_and_metadata(tf)
        intents: list[OrderIntent] = []

        if side == "UP_TRUE":
            if pos.side is None:
                intents.append(
                    OrderIntent.enter_long(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} MA-cross ↑ at {bar.ts.isoformat()}",
                        metadata={
                            "previous_verdict": previous_verdict,
                            "verdict": side,
                            **entry_metadata,
                        },
                    )
                )
                pos.side = "long"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
            elif pos.side == "short":
                # Closing a short — compute P&L% and maybe trigger cool-off.
                entry_price = pos.entry_price_usd
                pnl_pct = 0.0
                if entry_price:
                    pnl_pct = (entry_price - bar.close) / entry_price
                self._last_trade_pnl_pct[tf] = pnl_pct
                cool_off_triggers, cooldown_types = self._start_cooldowns(tf, pnl_pct)
                intents.append(
                    OrderIntent.exit(
                        trigger_tf=tf,
                        reason=f"{tf} MA-cross ↑ closes short",
                        metadata={
                            "previous_verdict": previous_verdict,
                            "verdict": side,
                            "closed_side": "short",
                            "trade_pnl_pct": pnl_pct,
                            "cool_off_started": cool_off_triggers,
                            "cooldown_types": cooldown_types,
                        },
                    )
                )
                pos.side = None
                pos.qty_sats = 0
                pos.entry_ts = None
                # Same-bar flip: only enter the new direction if cool-off
                # didn't trigger. Otherwise the next-N signals suppression
                # should already include this entry.
                if self.same_bar_flip and not cool_off_triggers:
                    intents.append(
                        OrderIntent.enter_long(
                            trigger_tf=tf,
                            size_usd=size,
                            leverage=self.base_leverage,
                            reason=f"{tf} MA-cross ↑ flip to long",
                            metadata={
                                "previous_verdict": previous_verdict,
                                "verdict": side,
                                **entry_metadata,
                            },
                        )
                    )
                    pos.side = "long"
                    pos.entry_ts = bar.ts
                    pos.leverage = self.base_leverage
                elif self.same_bar_flip and cool_off_triggers:
                    intents.append(
                        OrderIntent.noop(
                            trigger_tf=tf,
                            reason="cool_off_same_bar_flip",
                            metadata={
                                "previous_verdict": previous_verdict,
                                "verdict": side,
                                "suppressed_action": "enter_long",
                                "cooldown_types": cooldown_types,
                            },
                        )
                    )
            # already long: verdict transition but no order opportunity

        elif side == "DOWN_TRUE":
            if pos.side is None:
                intents.append(
                    OrderIntent.enter_short(
                        trigger_tf=tf,
                        size_usd=size,
                        leverage=self.base_leverage,
                        reason=f"{tf} MA-cross ↓ at {bar.ts.isoformat()}",
                        metadata={"previous_verdict": previous_verdict, "verdict": side},
                    )
                )
                pos.side = "short"
                pos.entry_ts = bar.ts
                pos.leverage = self.base_leverage
            elif pos.side == "long":
                # Closing a long — compute P&L% and maybe trigger cool-off.
                entry_price = pos.entry_price_usd
                pnl_pct = 0.0
                if entry_price:
                    pnl_pct = (bar.close - entry_price) / entry_price
                self._last_trade_pnl_pct[tf] = pnl_pct
                cool_off_triggers, cooldown_types = self._start_cooldowns(tf, pnl_pct)
                intents.append(
                    OrderIntent.exit(
                        trigger_tf=tf,
                        reason=f"{tf} MA-cross ↓ closes long",
                        metadata={
                            "previous_verdict": previous_verdict,
                            "verdict": side,
                            "closed_side": "long",
                            "trade_pnl_pct": pnl_pct,
                            "cool_off_started": cool_off_triggers,
                            "cooldown_types": cooldown_types,
                        },
                    )
                )
                pos.side = None
                pos.qty_sats = 0
                pos.entry_ts = None
                # Same-bar flip: only enter the new direction if cool-off
                # didn't trigger.
                if self.same_bar_flip and not cool_off_triggers:
                    intents.append(
                        OrderIntent.enter_short(
                            trigger_tf=tf,
                            size_usd=size,
                            leverage=self.base_leverage,
                            reason=f"{tf} MA-cross ↓ flip to short",
                            metadata={
                                "previous_verdict": previous_verdict,
                                "verdict": side,
                                **entry_metadata,
                            },
                        )
                    )
                    pos.side = "short"
                    pos.entry_ts = bar.ts
                    pos.leverage = self.base_leverage
                elif self.same_bar_flip and cool_off_triggers:
                    intents.append(
                        OrderIntent.noop(
                            trigger_tf=tf,
                            reason="cool_off_same_bar_flip",
                            metadata={
                                "previous_verdict": previous_verdict,
                                "verdict": side,
                                "suppressed_action": "enter_short",
                                "cooldown_types": cooldown_types,
                            },
                        )
                    )
            # already short: verdict transition but no order opportunity

        if not intents:
            intents.append(
                OrderIntent.noop(
                    trigger_tf=tf,
                    reason="position_already_matches_verdict",
                    metadata={
                        "previous_verdict": previous_verdict,
                        "verdict": side,
                        "position_side": pos.side,
                    },
                )
            )

        return intents

    def _start_cooldowns(self, tf: str, pnl_pct: float) -> tuple[bool, list[str]]:
        """Start the independent winner/loss cooldown triggered by this exit."""
        types: list[str] = []
        winner_threshold = self.cooldown_threshold_pct.get(tf, 0.0)
        winner_count = self.cooldown_signal_count.get(tf, 0)
        if pnl_pct >= winner_threshold and winner_count > 0:
            self._suppressed_signals[tf] = winner_count
            types.append("winner")

        loss_threshold = self.loss_cooldown_threshold_pct.get(tf, 0.0)
        loss_count = self.loss_cooldown_signal_count.get(tf, 0)
        if loss_threshold > 0 and pnl_pct <= -loss_threshold and loss_count > 0:
            self._loss_suppressed_signals[tf] = loss_count
            types.append("loss")
        return bool(types), types
