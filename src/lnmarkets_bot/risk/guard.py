"""The order-execution guard.

Sits between the engine and any executor (backtest fill model or live trades
API). Two guarantees:
  1. Every OrderIntent is checked against hard limits; violations are
     recorded as risk_events and either clamp the size or reject the order.
  2. The strategy code never touches the executor directly — only the
     engine does, via this guard.

This file imports from the executor interface defined in engine/ — it's the
*only* module allowed to bridge engine intents to either backtest or live.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from math import floor
from typing import TYPE_CHECKING, Any, Protocol

from ..strategy import OrderIntent, SignalKind

if TYPE_CHECKING:
    from ..persistence.recorder import Recorder
    from .limits import RiskLimits


class Decision(Enum):
    SUBMITTED = "submitted"
    CLAMPED = "clamped"
    REJECTED = "rejected"


@dataclass
class OrderDecision:
    decision: Decision
    order_id: int | None
    detail: dict[str, Any]


class Executor(Protocol):
    """What the guard hands a *clamped/passed* intent to.

    Backtest implements it as a fill simulator; live implements it as
    trades/TradesApi.new_order(). Async because the live path makes HTTP
    calls to LNM.
    """

    async def submit(
        self,
        *,
        intent: OrderIntent,
        signal_id: int,
        run_id: int,
        ts: datetime,
        size_usd: float,
        leverage: float,
    ) -> tuple[int, dict[str, Any]]:
        """Return (order_id, metadata). Metadata may include price, lnm_id, etc."""
        ...

    def consume_realized_pnl_usd(self) -> float:
        """Return newly realized P&L since the preceding call."""
        ...


class AccountBalanceProvider(Protocol):
    async def balance_usd(
        self,
        *,
        run_id: int,
        ts: datetime,
        price_usd: float,
        margin_used_usd: float,
    ) -> float:
        """Return the reported account balance converted to USD."""
        ...


@dataclass(frozen=True)
class SizingPolicy:
    mode: str = "fixed_notional"
    total_margin_fraction: float = 0.50
    timeframe_weights: dict[str, float] | None = None
    equity_haircut: float = 0.95


class RiskGuard:
    def __init__(
        self,
        *,
        limits: RiskLimits,
        recorder: Recorder,
        executor: Executor,
        sizing_policy: SizingPolicy | None = None,
        account_balance_provider: AccountBalanceProvider | None = None,
        clock: callable = lambda: datetime.now(UTC),
    ) -> None:
        self.limits = limits
        self.recorder = recorder
        self.executor = executor
        self.sizing_policy = sizing_policy or SizingPolicy()
        self.account_balance_provider = account_balance_provider
        self._clock = clock
        # Current BTC/USD price in USD. Engine updates this from each bar.
        self.current_price_usd: float | None = None
        # Sliding window of (ts, run_id) for orders submitted, used for the
        # max_orders_per_minute check.
        self._recent_orders: deque[datetime] = deque(maxlen=limits.max_orders_per_minute * 2)
        # Tally of realized P&L for the current UTC day, in USD.
        self._today_realized_pnl_usd: float = 0.0
        self._today_date: str = ""
        self._today_pnl_restored = False

    def _rollover_day(self, ts: datetime) -> None:
        date_str = ts.astimezone(UTC).strftime("%Y-%m-%d")
        if date_str != self._today_date:
            self._today_date = date_str
            self._today_realized_pnl_usd = 0.0
            self._today_pnl_restored = False
        if (
            self._today_pnl_restored
            or self.current_price_usd is None
            or self.current_price_usd <= 0
        ):
            return
        # Daily P&L is persisted in sats; the live guard is USD-denominated.
        # Convert historical bot P&L using the first current mark after
        # startup.  This is deliberately conservative operational state, not
        # a reporting value (the database remains the source of truth).
        getter = getattr(self.recorder, "net_daily_pnl_sats", None)
        if getter is not None:
            net_sats = int(getter(date_str))
            self._today_realized_pnl_usd = net_sats * self.current_price_usd / 1e8
        self._today_pnl_restored = True

    def record_realized_pnl(self, delta_usd: float, ts: datetime) -> None:
        self._rollover_day(ts)
        self._today_realized_pnl_usd += delta_usd

    def _open_notional_usd(self, exclude_tf: str) -> float:
        getter = getattr(self.executor, "open_notional_usd", None)
        return float(getter(exclude_tf=exclude_tf)) if getter is not None else 0.0

    def _open_margin_usd(self, exclude_tf: str) -> float:
        getter = getattr(self.executor, "open_margin_usd", None)
        return float(getter(exclude_tf=exclude_tf)) if getter is not None else 0.0

    async def _sized_notional_usd(
        self, *, intent: OrderIntent, run_id: int, ts: datetime
    ) -> float | None:
        if intent.kind == SignalKind.EXIT:
            return 0.0
        if self.sizing_policy.mode == "fixed_notional":
            return intent.size_usd
        if self.sizing_policy.mode != "equity_fraction":
            raise ValueError(f"unknown sizing mode: {self.sizing_policy.mode}")
        if self.current_price_usd is None or self.current_price_usd <= 0:
            return None
        if self.account_balance_provider is None:
            return None
        tf = intent.trigger_tf or "default"
        weight = (self.sizing_policy.timeframe_weights or {}).get(tf, 0.0)
        if weight <= 0:
            return None
        balance_usd = await self.account_balance_provider.balance_usd(
            run_id=run_id,
            ts=ts,
            price_usd=self.current_price_usd,
            margin_used_usd=self._open_margin_usd(exclude_tf=tf),
        )
        margin_budget_usd = (
            balance_usd
            * self.sizing_policy.total_margin_fraction
            * weight
            * self.sizing_policy.equity_haircut
        )
        calculated_notional = float(floor(margin_budget_usd * intent.leverage))
        # A strategy may attach an auditable multiplier for a bounded regime
        # overlay. Fixed-notional mode already receives it in ``size_usd``;
        # apply it here too so equity-fraction sizing does not silently make
        # the overlay inert. Hard caps are still enforced by ``submit``.
        multiplier = float(intent.metadata.get("entry_size_multiplier", 1.0))
        if multiplier <= 0.0:
            return None
        return calculated_notional * multiplier

    async def submit(
        self,
        *,
        intent: OrderIntent,
        signal_id: int,
        run_id: int,
        ts: datetime,
    ) -> OrderDecision:
        if intent.kind == SignalKind.NOOP:
            return OrderDecision(decision=Decision.SUBMITTED, order_id=None, detail={"noop": True})

        # Exits reduce exposure. They must always be allowed, including after
        # the daily-loss tripwire and beside an entry in a same-bar flip.
        is_exit = intent.kind == SignalKind.EXIT
        if not is_exit:
            # Daily loss trip-wire (USD-native; limit and tally both in USD)
            self._rollover_day(ts)
            if self._today_realized_pnl_usd <= -self.limits.max_daily_loss_usd:
                self.recorder.record_risk_event(
                    run_id,
                    ts=ts,
                    kind="daily_loss",
                    signal_id=signal_id,
                    detail={
                        "today_pnl_usd": self._today_realized_pnl_usd,
                        "limit_usd": self.limits.max_daily_loss_usd,
                    },
                )
                return OrderDecision(
                    decision=Decision.REJECTED,
                    order_id=None,
                    detail={"reason": "daily_loss_exceeded"},
                )

            # Rate-limit only risk-increasing entries. A close followed by an
            # entry on the same bar is one legitimate same-bar flip.
            cutoff = ts - timedelta(seconds=60)
            while self._recent_orders and self._recent_orders[0] < cutoff:
                self._recent_orders.popleft()
            if len(self._recent_orders) >= self.limits.max_orders_per_minute:
                self.recorder.record_risk_event(
                    run_id,
                    ts=ts,
                    kind="rate_limit",
                    signal_id=signal_id,
                    detail={"orders_last_minute": len(self._recent_orders)},
                )
                return OrderDecision(
                    decision=Decision.REJECTED,
                    order_id=None,
                    detail={"reason": "rate_limit_exceeded"},
                )

        # Resolve dynamic sizing before the independent hard caps below.
        size_usd = await self._sized_notional_usd(intent=intent, run_id=run_id, ts=ts)
        leverage = intent.leverage
        if size_usd is None:
            self.recorder.record_risk_event(
                run_id,
                ts=ts,
                kind="reject",
                signal_id=signal_id,
                detail={"reason": "sizing_data_unavailable", "mode": self.sizing_policy.mode},
            )
            return OrderDecision(
                decision=Decision.REJECTED,
                order_id=None,
                detail={"reason": "sizing_data_unavailable"},
            )
        clamped = False
        if size_usd > self.limits.max_position_usd:
            size_usd = self.limits.max_position_usd
            clamped = True
        if leverage > self.limits.max_leverage:
            leverage = self.limits.max_leverage
            clamped = True
        if intent.kind.value == "exit":
            # Exits have size_usd = 0 by design (the executor closes whatever
            # is open at the current TF). Skip the size check and pass through.
            leverage = intent.leverage or 1.0
        elif size_usd <= 0:
            self.recorder.record_risk_event(
                run_id,
                ts=ts,
                kind="reject",
                signal_id=signal_id,
                detail={"reason": "non_positive_size", "intent_size_usd": intent.size_usd},
            )
            return OrderDecision(
                decision=Decision.REJECTED,
                order_id=None,
                detail={"reason": "non_positive_size"},
            )

        if intent.kind.value != "exit":
            tf = intent.trigger_tf or "default"
            open_notional_usd = self._open_notional_usd(exclude_tf=tf)
            open_margin_usd = self._open_margin_usd(exclude_tf=tf)
            if self.limits.max_total_notional_usd is not None:
                remaining_notional = self.limits.max_total_notional_usd - open_notional_usd
                if remaining_notional <= 0:
                    return self._reject_exposure(run_id, ts, signal_id, "total_notional_exceeded")
                if size_usd > remaining_notional:
                    size_usd = remaining_notional
                    clamped = True
            if self.limits.max_total_margin_usd is not None:
                remaining_margin = self.limits.max_total_margin_usd - open_margin_usd
                if remaining_margin <= 0:
                    return self._reject_exposure(run_id, ts, signal_id, "total_margin_exceeded")
                max_by_margin = floor(remaining_margin * leverage)
                if max_by_margin <= 0:
                    return self._reject_exposure(run_id, ts, signal_id, "total_margin_exceeded")
                if size_usd > max_by_margin:
                    size_usd = float(max_by_margin)
                    clamped = True

        if clamped:
            self.recorder.record_risk_event(
                run_id,
                ts=ts,
                kind="clamp",
                signal_id=signal_id,
                detail={
                    "requested_size_usd": intent.size_usd,
                    "clamped_size_usd": size_usd,
                    "requested_leverage": intent.leverage,
                    "clamped_leverage": leverage,
                },
            )

        order_id, meta = await self.executor.submit(
            intent=intent,
            signal_id=signal_id,
            run_id=run_id,
            ts=ts,
            size_usd=size_usd,
            leverage=leverage,
        )
        if not is_exit:
            self._recent_orders.append(ts)
        merged = dict(meta)
        if clamped:
            merged.update(
                {
                    "requested_size_usd": intent.size_usd,
                    "clamped_size_usd": size_usd,
                    "requested_leverage": intent.leverage,
                    "clamped_leverage": leverage,
                }
            )
        return OrderDecision(
            decision=Decision.CLAMPED if clamped else Decision.SUBMITTED,
            order_id=order_id,
            detail=merged,
        )

    def _reject_exposure(
        self, run_id: int, ts: datetime, signal_id: int, reason: str
    ) -> OrderDecision:
        self.recorder.record_risk_event(
            run_id,
            ts=ts,
            kind="reject",
            signal_id=signal_id,
            detail={"reason": reason},
        )
        return OrderDecision(
            decision=Decision.REJECTED,
            order_id=None,
            detail={"reason": reason},
        )

    def is_daily_loss_tripped(self, ts: datetime) -> bool:
        self._rollover_day(ts)
        return self._today_realized_pnl_usd <= -self.limits.max_daily_loss_usd
