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

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
    last_bar_ts: datetime | None = None


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
        # Operator-set hold after deliberately declining a missed entry. The
        # hold is released only when the verdict changes away from this value.
        self._manual_flat_hold: dict[str, str | None] = {tf: None for tf in self.tfs}
        # Desired final position for an emitted order sequence until the
        # executor confirms it. This is the only authority for retrying an
        # order under an unchanged verdict.
        self._pending_position_reconciliation: dict[str, str | None] = {tf: None for tf in self.tfs}
        self._restart_pending: set[str] = set()
        self._startup_reconciliation_pending = False

    # ---- lifecycle ----

    def on_startup(self, state: StrategyState) -> None:
        for tf in self.tfs:
            state.positions.setdefault(tf, TfPosition())
            if state.position(tf).side is not None:
                self._restart_pending.add(tf)
        self._startup_reconciliation_pending = bool(
            self._restart_pending or any(self._pending_position_reconciliation.values())
        )

    def persistent_state(self) -> dict[str, Any] | None:
        """Serialize indicator and cool-off state for restart-continuous live EMA."""
        return {
            "version": 1,
            # Persist and compare the JSON representation. SQLite JSON turns
            # tuples (notably ``tfs``) into lists, so comparing the raw Python
            # objects would reject every otherwise-compatible live snapshot.
            "strategy_params": self._json_strategy_params(),
            "timeframes": {
                tf: {
                    "closes": list(item.closes),
                    "highs": list(item.highs),
                    "lows": list(item.lows),
                    "true_ranges": list(item.true_ranges),
                    "previous_close": item.previous_close,
                    "sma": item.sma,
                    "ema": item.ema,
                    "ema_seeded": item.ema_seeded,
                    "verdict": item.verdict,
                    "chop": item.chop,
                    "last_bar_ts": item.last_bar_ts.isoformat() if item.last_bar_ts else None,
                }
                for tf, item in self.tf_state.items()
            },
            "winner_suppressed_signals": dict(self._suppressed_signals),
            "loss_suppressed_signals": dict(self._loss_suppressed_signals),
            "last_trade_pnl_pct": dict(self._last_trade_pnl_pct),
            "manual_flat_hold": dict(self._manual_flat_hold),
            "pending_position_reconciliation": dict(self._pending_position_reconciliation),
        }

    def restore_persistent_state(self, snapshot: dict[str, Any]) -> bool:
        """Restore a compatible snapshot without accepting partial/corrupt state."""
        try:
            if snapshot.get("version") != 1:
                return False
            if snapshot.get("strategy_params") != self._json_strategy_params():
                return False
            timeframes = snapshot["timeframes"]
            if set(timeframes) != set(self.tfs):
                return False
            restored: dict[str, _TfState] = {}
            for tf in self.tfs:
                value = timeframes[tf]
                last_bar_raw = value.get("last_bar_ts")
                last_bar_ts = (
                    datetime.fromisoformat(last_bar_raw.replace("Z", "+00:00")).astimezone(UTC)
                    if last_bar_raw
                    else None
                )
                restored[tf] = _TfState(
                    closes=deque((float(x) for x in value["closes"]), maxlen=64),
                    highs=deque((float(x) for x in value["highs"]), maxlen=128),
                    lows=deque((float(x) for x in value["lows"]), maxlen=128),
                    true_ranges=deque((float(x) for x in value["true_ranges"]), maxlen=128),
                    previous_close=(
                        float(value["previous_close"])
                        if value["previous_close"] is not None
                        else None
                    ),
                    sma=float(value["sma"]) if value["sma"] is not None else None,
                    ema=float(value["ema"]) if value["ema"] is not None else None,
                    ema_seeded=bool(value["ema_seeded"]),
                    verdict=str(value["verdict"]),
                    chop=float(value["chop"]) if value["chop"] is not None else None,
                    last_bar_ts=last_bar_ts,
                )
            winner = {tf: int(snapshot["winner_suppressed_signals"][tf]) for tf in self.tfs}
            loss = {tf: int(snapshot["loss_suppressed_signals"][tf]) for tf in self.tfs}
            if any(value < 0 for value in (*winner.values(), *loss.values())):
                return False
            pnl = {tf: float(snapshot["last_trade_pnl_pct"][tf]) for tf in self.tfs}
            manual_hold = {
                tf: (
                    str(snapshot.get("manual_flat_hold", {}).get(tf))
                    if snapshot.get("manual_flat_hold", {}).get(tf) is not None
                    else None
                )
                for tf in self.tfs
            }
            if any(
                value not in {None, "UP_TRUE", "DOWN_TRUE", "FLAT"}
                for value in manual_hold.values()
            ):
                return False
            pending = {
                tf: (
                    str(snapshot.get("pending_position_reconciliation", {}).get(tf))
                    if snapshot.get("pending_position_reconciliation", {}).get(tf) is not None
                    else None
                )
                for tf in self.tfs
            }
            if any(value not in {None, "long", "short", "flat"} for value in pending.values()):
                return False
        except (KeyError, TypeError, ValueError, AttributeError):
            return False
        self.tf_state = restored
        self._suppressed_signals = winner
        self._loss_suppressed_signals = loss
        self._last_trade_pnl_pct = pnl
        self._manual_flat_hold = manual_hold
        self._pending_position_reconciliation = pending
        return True

    def _json_strategy_params(self) -> dict[str, Any]:
        """Return params in the exact shape produced by a JSON DB round-trip."""
        return json.loads(json.dumps(self.params, sort_keys=True))

    def reconcile_execution_state(self, state: StrategyState) -> None:
        """Clear durable pending targets once the executor mirrors them."""
        for tf, target in self._pending_position_reconciliation.items():
            if target is None:
                continue
            actual = state.position(tf).side or "flat"
            if actual == target:
                self._pending_position_reconciliation[tf] = None

    def on_intent_rejected(self, intent: OrderIntent) -> None:
        """Risk rejection is a deliberate suppression, not an execution failure."""
        if intent.kind.value == "entry" and intent.trigger_tf in self.tf_state:
            self._pending_position_reconciliation[intent.trigger_tf] = None

    def on_shutdown(self, state: StrategyState) -> None:
        return None

    # ---- per-bar logic ----

    def on_bar(self, bar: Bar, state: StrategyState) -> list[OrderIntent]:
        if bar.timeframe == "1m":
            state.push_bar(bar)
            if not bar.warmup and self._startup_reconciliation_pending:
                self._startup_reconciliation_pending = False
                return self._reconcile_startup_positions(state)

        tf = bar.timeframe
        if tf not in self.tf_state:
            return []  # not subscribed to this TF

        ts = self.tf_state[tf]
        # Warmup replay deliberately overlaps a restored snapshot. Never
        # replay an already committed completed TF bar, or a restart would
        # mutate the EMA a second time.
        if ts.last_bar_ts is not None and bar.ts <= ts.last_bar_ts:
            return []
        ts.closes.append(bar.close)
        self._update_chop(ts, bar)
        ts.last_bar_ts = bar.ts

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
        if verdict != prev:
            # A pending action for an older verdict is no longer a valid
            # retry. Normal processing below decides the new target.
            self._pending_position_reconciliation[tf] = None
        if bar.warmup:
            return []
        held_verdict = self._manual_flat_hold[tf]
        if held_verdict == verdict:
            # The position was manually flattened after a missed entry. Do
            # not recreate that late entry merely because the same verdict
            # persists across a restart or subsequent completed bar.
            self._restart_pending.discard(tf)
            self._pending_position_reconciliation[tf] = None
            return [
                OrderIntent.noop(
                    trigger_tf=tf,
                    reason="manual_flat_hold",
                    metadata={
                        "held_verdict": held_verdict,
                        "verdict": verdict,
                        "bar_ts": bar.ts.isoformat(),
                    },
                )
            ]
        if held_verdict is not None:
            # A new verdict resumes normal strategy operation. A fresh UP or
            # DOWN verdict can therefore create an ordinary entry.
            self._manual_flat_hold[tf] = None
        cooldowns_before = self._active_cooldowns(tf)
        restart_audit: dict[str, Any] = {}
        if tf in self._restart_pending:
            restart_audit = self._restart_audit_metadata(
                tf=tf, verdict=verdict, bar=bar, pos=state.position(tf)
            )
        if cooldowns_before and self._position_opposes_verdict(tf=tf, verdict=verdict, state=state):
            # Cool-off suppresses the replacement entry, never the
            # exposure-reducing close. This path also recovers a close that
            # reached strategy state but failed (or was interrupted) at LNM.
            self._restart_pending.discard(tf)
            intents = self._cooldown_exposure_exit(
                tf=tf,
                previous_verdict=prev,
                verdict=verdict,
                bar=bar,
                state=state,
                cooldowns_before=cooldowns_before,
                metadata=restart_audit,
            )
            if verdict != prev and self._cooldown_consumes(tf=tf, verdict=verdict, state=state):
                intents.extend(
                    self._consume_cooldown(
                        tf=tf,
                        previous_verdict=prev,
                        verdict=verdict,
                        cooldowns_before=cooldowns_before,
                        metadata=restart_audit,
                    )
                )
            return intents
        pending_target = self._pending_position_reconciliation[tf]
        if pending_target == "flat" and state.position(tf).side is not None:
            self._restart_pending.discard(tf)
            pos = state.position(tf)
            intent = OrderIntent.exit(
                trigger_tf=tf,
                reason=f"{tf} retries pending exposure-reducing close",
                metadata={
                    "previous_verdict": prev,
                    "verdict": verdict,
                    "closed_side": pos.side,
                    "pending_position_reconciliation": "flat",
                    **restart_audit,
                },
            )
            pos.side = None
            pos.qty_sats = 0
            pos.entry_ts = None
            return [intent]
        if tf in self._restart_pending:
            self._restart_pending.remove(tf)
            if (
                verdict != prev
                and cooldowns_before
                and self._cooldown_consumes(tf=tf, verdict=verdict, state=state)
            ):
                return self._consume_cooldown(
                    tf=tf,
                    previous_verdict=prev,
                    verdict=verdict,
                    cooldowns_before=cooldowns_before,
                    metadata=self._restart_audit_metadata(
                        tf=tf, verdict=verdict, bar=bar, pos=state.position(tf)
                    ),
                )
            if (
                verdict == prev
                and cooldowns_before
                and self._would_place_order(tf=tf, side=verdict, state=state)
            ):
                return [
                    OrderIntent.noop(
                        trigger_tf=tf,
                        reason="cool_off_pending_position_reconciliation",
                        metadata={
                            **self._restart_audit_metadata(
                                tf=tf, verdict=verdict, bar=bar, pos=state.position(tf)
                            ),
                            "cooldown_types": sorted(cooldowns_before),
                            "winner_remaining": cooldowns_before.get("winner", 0),
                            "loss_remaining": cooldowns_before.get("loss", 0),
                        },
                    )
                ]
            return self._restart_catch_up(tf=tf, verdict=verdict, bar=bar, state=state)
        if verdict == prev:
            if (
                pending_target in {"long", "short"}
                and cooldowns_before
                and self._would_place_order(tf=tf, side=verdict, state=state)
            ):
                # A failed/missed flip can leave a flat position under an
                # unchanged directional verdict. Do not let the resilience
                # reconciliation bypass a still-active cool-off. This is not
                # a verdict transition, so it must not spend a cool-off slot.
                return [
                    OrderIntent.noop(
                        trigger_tf=tf,
                        reason="cool_off_pending_position_reconciliation",
                        metadata={
                            "verdict": verdict,
                            "cooldown_types": sorted(cooldowns_before),
                            "winner_remaining": cooldowns_before.get("winner", 0),
                            "loss_remaining": cooldowns_before.get("loss", 0),
                        },
                    )
                ]
            # Retry only an explicitly persisted target from an order that was
            # emitted but not confirmed. A mere flat/verdict mismatch after a
            # cold restart must not manufacture a late entry.
            if pending_target in {"long", "short"} and self._would_place_order(
                tf=tf, side=verdict, state=state
            ):
                return self._on_transition(
                    tf=tf,
                    previous_verdict=prev,
                    side=verdict,
                    bar=bar,
                    state=state,
                )
            return []

        # Cool-off: if this TF recently closed a big winner, suppress
        # transitions until the suppression counter runs out.
        if cooldowns_before and self._cooldown_consumes(tf=tf, verdict=verdict, state=state):
            return self._consume_cooldown(
                tf=tf,
                previous_verdict=prev,
                verdict=verdict,
                cooldowns_before=cooldowns_before,
            )

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

    def _reconcile_startup_positions(self, state: StrategyState) -> list[OrderIntent]:
        """Reconcile once after warmup, without replaying historical trades."""
        intents: list[OrderIntent] = []
        for tf in self.tfs:
            pending_target = self._pending_position_reconciliation[tf]
            if tf not in self._restart_pending and pending_target is None:
                continue
            indicator = self.tf_state[tf]
            if indicator.last_bar_ts is None or not indicator.closes:
                continue
            bar = Bar(
                ts=indicator.last_bar_ts,
                open=indicator.closes[-1],
                high=indicator.highs[-1] if indicator.highs else indicator.closes[-1],
                low=indicator.lows[-1] if indicator.lows else indicator.closes[-1],
                close=indicator.closes[-1],
                volume=0.0,
                timeframe=tf,
            )
            verdict = indicator.verdict
            cooldowns = self._active_cooldowns(tf)
            pos = state.position(tf)
            if pending_target == "flat" and pos.side is not None:
                self._restart_pending.discard(tf)
                intents.append(
                    OrderIntent.exit(
                        trigger_tf=tf,
                        reason=f"{tf} retries pending exposure-reducing close",
                        metadata={
                            "previous_verdict": verdict,
                            "verdict": verdict,
                            "closed_side": pos.side,
                            "pending_position_reconciliation": "flat",
                            **self._restart_audit_metadata(
                                tf=tf, verdict=verdict, bar=bar, pos=pos
                            ),
                        },
                    )
                )
                pos.side = None
                pos.qty_sats = 0
                pos.entry_ts = None
                continue
            if pending_target in {"long", "short"} and pos.side != pending_target:
                self._restart_pending.discard(tf)
                if cooldowns:
                    intents.append(
                        OrderIntent.noop(
                            trigger_tf=tf,
                            reason="cool_off_pending_position_reconciliation",
                            metadata={
                                "verdict": verdict,
                                "cooldown_types": sorted(cooldowns),
                                "winner_remaining": cooldowns.get("winner", 0),
                                "loss_remaining": cooldowns.get("loss", 0),
                                **self._restart_audit_metadata(
                                    tf=tf, verdict=verdict, bar=bar, pos=pos
                                ),
                            },
                        )
                    )
                else:
                    intents.extend(
                        self._on_transition(
                            tf=tf,
                            previous_verdict=verdict,
                            side=verdict,
                            bar=bar,
                            state=state,
                        )
                    )
                continue
            if cooldowns and self._position_opposes_verdict(tf=tf, verdict=verdict, state=state):
                self._restart_pending.discard(tf)
                intents.extend(
                    self._cooldown_exposure_exit(
                        tf=tf,
                        previous_verdict=verdict,
                        verdict=verdict,
                        bar=bar,
                        state=state,
                        cooldowns_before=cooldowns,
                        metadata=self._restart_audit_metadata(
                            tf=tf, verdict=verdict, bar=bar, pos=state.position(tf)
                        ),
                    )
                )
                continue
            self._restart_pending.discard(tf)
            intents.extend(self._restart_catch_up(tf=tf, verdict=verdict, bar=bar, state=state))
        return intents

    def _restart_catch_up(
        self, *, tf: str, verdict: str, bar: Bar, state: StrategyState
    ) -> list[OrderIntent]:
        """Apply normal transition semantics to a restored position.

        A restart must not make an opposite confirmed verdict weaker than it
        would have been in a continuous run.  In particular, an enabled
        same-bar flip and the usual cool-off rules both apply here.
        """
        pos = state.position(tf)
        target_side = {"UP_TRUE": "long", "DOWN_TRUE": "short"}.get(verdict)
        audit = self._restart_audit_metadata(tf=tf, verdict=verdict, bar=bar, pos=pos)
        if pos.side is not None and target_side is not None and pos.side != target_side:
            intents = self._on_transition(
                tf=tf,
                previous_verdict="RESTART_UNKNOWN",
                side=verdict,
                bar=bar,
                state=state,
            )
            for intent in intents:
                intent.metadata.update(audit)
            return intents
        return [
            OrderIntent.noop(
                trigger_tf=tf,
                reason="restart_state_aligned",
                metadata=audit,
            )
        ]

    def _restart_audit_metadata(
        self, *, tf: str, verdict: str, bar: Bar, pos: TfPosition
    ) -> dict[str, Any]:
        """Return enough context to audit a restart decision from the DB."""
        indicator = self.tf_state[tf]
        sma = indicator.sma
        ema = indicator.ema
        return {
            "restart_catch_up": True,
            "restored_side": pos.side,
            "verdict": verdict,
            "bar_ts": bar.ts.isoformat(),
            "close": bar.close,
            "sma": sma,
            "ema": ema,
            "tolerance_pct": self.tolerance_pct,
            "close_vs_sma_pct": (bar.close / sma - 1.0) if sma else None,
            "close_vs_ema_pct": (bar.close / ema - 1.0) if ema else None,
        }

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

    def _consume_cooldown(
        self,
        *,
        tf: str,
        previous_verdict: str,
        verdict: str,
        cooldowns_before: dict[str, int],
        metadata: dict[str, Any] | None = None,
    ) -> list[OrderIntent]:
        """Spend one slot and record an auditable suppressed transition."""
        for cooldown_type in cooldowns_before:
            if cooldown_type == "winner":
                self._suppressed_signals[tf] -= 1
            else:
                self._loss_suppressed_signals[tf] -= 1
        return [
            OrderIntent.noop(
                trigger_tf=tf,
                reason="cool_off",
                metadata={
                    "previous_verdict": previous_verdict,
                    "verdict": verdict,
                    "cooldown_types": sorted(cooldowns_before),
                    "winner_remaining_before": cooldowns_before.get("winner", 0),
                    "winner_remaining_after": self._suppressed_signals[tf],
                    "loss_remaining_before": cooldowns_before.get("loss", 0),
                    "loss_remaining_after": self._loss_suppressed_signals[tf],
                    **(metadata or {}),
                },
            )
        ]

    @staticmethod
    def _position_opposes_verdict(*, tf: str, verdict: str, state: StrategyState) -> bool:
        """Return whether the current exposure points against a directional verdict."""
        target_side = {"UP_TRUE": "long", "DOWN_TRUE": "short"}.get(verdict)
        pos_side = state.position(tf).side
        return pos_side is not None and target_side is not None and pos_side != target_side

    def _cooldown_exposure_exit(
        self,
        *,
        tf: str,
        previous_verdict: str,
        verdict: str,
        bar: Bar,
        state: StrategyState,
        cooldowns_before: dict[str, int],
        metadata: dict[str, Any],
    ) -> list[OrderIntent]:
        """Close contrary exposure while leaving its replacement suppressed."""
        pos = state.position(tf)
        closed_side = pos.side
        entry_price = pos.entry_price_usd
        pnl_pct = 0.0
        if entry_price and closed_side == "long":
            pnl_pct = (bar.close - entry_price) / entry_price
        elif entry_price and closed_side == "short":
            pnl_pct = (entry_price - bar.close) / entry_price
        intent = OrderIntent.exit(
            trigger_tf=tf,
            reason=f"{tf} cool-off reconciliation closes {closed_side}",
            metadata={
                "previous_verdict": previous_verdict,
                "verdict": verdict,
                "closed_side": closed_side,
                "trade_pnl_pct": pnl_pct,
                "cooldown_types": sorted(cooldowns_before),
                "suppressed_replacement": ("long" if verdict == "UP_TRUE" else "short"),
                **metadata,
            },
        )
        pos.side = None
        pos.qty_sats = 0
        pos.entry_ts = None
        self._pending_position_reconciliation[tf] = "flat"
        return [intent]

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

        if any(intent.kind.value in {"entry", "exit"} for intent in intents):
            self._pending_position_reconciliation[tf] = pos.side or "flat"

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
