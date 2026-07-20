"""Risk guard tests — every limit type must fire and cannot be bypassed."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from lnmarkets_bot.risk.guard import Decision, RiskGuard, SizingPolicy
from lnmarkets_bot.risk.limits import RiskLimits
from lnmarkets_bot.strategy import OrderIntent

if TYPE_CHECKING:
    from lnmarkets_bot.persistence.recorder import Recorder


class StubExecutor:
    def __init__(self, recorder: Recorder, run_id_holder: list[int]) -> None:
        self.recorder = recorder
        self.calls = 0
        self.run_id_holder = run_id_holder
        self.last_size_usd: float | None = None

    async def submit(  # type: ignore[override]
        self,
        *,
        intent,
        signal_id,
        run_id,
        ts,
        size_usd,
        leverage,
    ) -> tuple[int, dict[str, Any]]:
        self.calls += 1
        self.last_size_usd = size_usd
        oid = self.recorder.record_order(
            run_id,
            signal_id=signal_id,
            ts=ts,
            side=intent.side.value if intent.side else "buy",
            qty_sats=int(size_usd * 1e8 / 60_000.0),
            leverage=leverage,
            status="filled",
            price_usd=60_000.0,
        )
        return oid, {"price_usd": 60_000.0}


@pytest.fixture
def limits() -> RiskLimits:
    return RiskLimits(
        max_position_usd=1000.0,
        max_leverage=10.0,
        max_daily_loss_usd=200.0,
        max_orders_per_minute=3,
    )


async def test_noop_intent_is_submitted_without_executor_call(cfg, recorder, limits) -> None:
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
    )
    run_id = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    sig = recorder.record_signal(run_id, ts=datetime.now(UTC), kind="noop", reason="quiet")
    d = await guard.submit(
        intent=OrderIntent.noop("1d", "quiet"),
        signal_id=sig,
        run_id=run_id,
        ts=datetime.now(UTC),
    )
    assert d.decision == Decision.SUBMITTED and d.order_id is None
    assert guard.executor.calls == 0  # type: ignore[attr-defined]


async def test_position_size_is_clamped(cfg, recorder, limits) -> None:
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    sig = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=5000.0,
        target_leverage=2.0,
        reason="big",
    )
    d = await guard.submit(
        intent=OrderIntent.enter_long("1d", 5000.0, 2.0, reason="big"),
        signal_id=sig,
        run_id=run_id,
        ts=datetime.now(UTC),
    )
    assert d.decision == Decision.CLAMPED
    assert d.detail["clamped_size_usd"] == 1000.0
    assert d.detail["clamped_leverage"] == 2.0  # 2.0 ≤ 10.0 → not clamped


async def test_leverage_is_clamped(cfg, recorder, limits) -> None:
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    sig = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=10.0,
        target_leverage=99.0,
        reason="hi_lev",
    )
    d = await guard.submit(
        intent=OrderIntent.enter_long("1d", 10.0, 99.0, reason="hi_lev"),
        signal_id=sig,
        run_id=run_id,
        ts=datetime.now(UTC),
    )
    assert d.decision == Decision.CLAMPED
    assert d.detail["clamped_leverage"] == 10.0


async def test_daily_loss_trips(cfg, recorder, limits) -> None:
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    guard.record_realized_pnl(-300.0, datetime.now(UTC))  # exceed 200 USD limit
    sig = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=10.0,
        target_leverage=1.0,
        reason="after_loss",
    )
    d = await guard.submit(
        intent=OrderIntent.enter_long("1d", 10.0, 1.0, reason="after_loss"),
        signal_id=sig,
        run_id=run_id,
        ts=datetime.now(UTC),
    )
    assert d.decision == Decision.REJECTED
    assert d.detail["reason"] == "daily_loss_exceeded"


async def test_daily_loss_is_restored_across_restart(recorder, limits) -> None:
    ts = datetime(2026, 7, 16, 12, tzinfo=UTC)
    previous_run = recorder.start_run(
        mode="live", strategy_name="t", strategy_params={}, config={}, started_at=ts
    )
    # -400,000 sats at a $50,000 mark is -$200: enough to trip the limit.
    recorder.upsert_daily_pnl(
        previous_run, date_str=ts.date().isoformat(), realized_delta_sats=-400_000
    )
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
        clock=lambda: ts,
    )
    guard.current_price_usd = 50_000.0
    run_id = recorder.start_run(
        mode="live", strategy_name="t", strategy_params={}, config={}, started_at=ts
    )
    signal_id = recorder.record_signal(run_id, ts=ts, kind="entry", side="long", reason="retry")

    decision = await guard.submit(
        intent=OrderIntent.enter_long("1d", 1, 1),
        signal_id=signal_id,
        run_id=run_id,
        ts=ts,
    )

    assert decision.decision == Decision.REJECTED
    assert decision.detail["reason"] == "daily_loss_exceeded"


async def test_exit_bypasses_daily_loss_tripwire(recorder, limits) -> None:
    executor = StubExecutor(recorder, [])
    guard = RiskGuard(limits=limits, recorder=recorder, executor=executor)  # type: ignore[arg-type]
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    guard.record_realized_pnl(-300.0, datetime.now(UTC))
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="exit", reason="reduce exposure"
    )

    decision = await guard.submit(
        intent=OrderIntent.exit("1d"),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
    )

    assert decision.decision == Decision.SUBMITTED
    assert executor.calls == 1


async def test_orders_per_minute_trips(cfg, recorder, limits) -> None:
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=StubExecutor(recorder, []),  # type: ignore[arg-type]
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    for i in range(limits.max_orders_per_minute):
        sig = recorder.record_signal(
            run_id,
            ts=datetime.now(UTC),
            kind="entry",
            side="long",
            target_size_usd=10.0,
            target_leverage=1.0,
            reason=f"r{i}",
        )
        d = await guard.submit(
            intent=OrderIntent.enter_long("1d", 10.0, 1.0, reason=f"r{i}"),
            signal_id=sig,
            run_id=run_id,
            ts=datetime.now(UTC),
        )
        assert d.decision in (Decision.SUBMITTED, Decision.CLAMPED), d
    # One more should be rejected.
    sig = recorder.record_signal(
        run_id,
        ts=datetime.now(UTC),
        kind="entry",
        side="long",
        target_size_usd=10.0,
        target_leverage=1.0,
        reason="over",
    )
    d = await guard.submit(
        intent=OrderIntent.enter_long("1d", 10.0, 1.0, reason="over"),
        signal_id=sig,
        run_id=run_id,
        ts=datetime.now(UTC),
    )
    assert d.decision == Decision.REJECTED
    assert d.detail["reason"] == "rate_limit_exceeded"


async def test_same_bar_exit_does_not_consume_entry_rate_limit(recorder) -> None:
    limits = RiskLimits(
        max_position_usd=1000.0,
        max_leverage=10.0,
        max_daily_loss_usd=200.0,
        max_orders_per_minute=1,
    )
    executor = StubExecutor(recorder, [])
    guard = RiskGuard(limits=limits, recorder=recorder, executor=executor)  # type: ignore[arg-type]
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    ts = datetime.now(UTC)
    exit_signal = recorder.record_signal(run_id, ts=ts, kind="exit", reason="flip close")
    entry_signal = recorder.record_signal(
        run_id, ts=ts, kind="entry", side="short", reason="flip open"
    )

    close = await guard.submit(
        intent=OrderIntent.exit("5m"), signal_id=exit_signal, run_id=run_id, ts=ts
    )
    open_ = await guard.submit(
        intent=OrderIntent.enter_short("5m", 1.0, 1.0),
        signal_id=entry_signal,
        run_id=run_id,
        ts=ts,
    )

    assert close.decision == Decision.SUBMITTED
    assert open_.decision == Decision.SUBMITTED
    assert executor.calls == 2


async def test_equity_fraction_sizing_uses_balance_and_timeframe_weight(recorder, limits) -> None:
    class BalanceProvider:
        async def balance_usd(self, **_kwargs) -> float:
            return 100.0

    executor = StubExecutor(recorder, [])
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=executor,  # type: ignore[arg-type]
        sizing_policy=SizingPolicy(
            mode="equity_fraction",
            total_margin_fraction=0.50,
            timeframe_weights={"1d": 0.5},
            equity_haircut=0.80,
        ),
        account_balance_provider=BalanceProvider(),
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long", reason="entry"
    )

    decision = await guard.submit(
        intent=OrderIntent.enter_long("1d", 1.0, 2.0),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
    )

    assert decision.decision == Decision.SUBMITTED
    # $100 x 50% total margin x 50% 1d allocation x 80% haircut x 2 leverage.
    assert executor.last_size_usd == 40.0


async def test_equity_fraction_honours_strategy_entry_size_multiplier(recorder, limits) -> None:
    class BalanceProvider:
        async def balance_usd(self, **_kwargs) -> float:
            return 100.0

    executor = StubExecutor(recorder, [])
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=executor,  # type: ignore[arg-type]
        sizing_policy=SizingPolicy(
            mode="equity_fraction",
            total_margin_fraction=0.50,
            timeframe_weights={"4h": 0.5},
            equity_haircut=0.80,
        ),
        account_balance_provider=BalanceProvider(),
    )
    guard.current_price_usd = 60_000.0
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long", reason="entry"
    )

    decision = await guard.submit(
        intent=OrderIntent.enter_long("4h", 1.0, 2.0, metadata={"entry_size_multiplier": 0.5}),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
    )

    assert decision.decision == Decision.SUBMITTED
    # Base equity allocation is $40; CHOP's 0.5x overlay applies afterwards.
    assert executor.last_size_usd == 20.0


async def test_total_notional_cap_includes_other_timeframes(recorder) -> None:
    class ExposedExecutor(StubExecutor):
        def open_notional_usd(self, *, exclude_tf: str) -> float:
            assert exclude_tf == "4h"
            return 8.0

    limits = RiskLimits(
        max_position_usd=100.0,
        max_leverage=10.0,
        max_daily_loss_usd=100.0,
        max_orders_per_minute=10,
        max_total_notional_usd=10.0,
    )
    executor = ExposedExecutor(recorder, [])
    guard = RiskGuard(limits=limits, recorder=recorder, executor=executor)  # type: ignore[arg-type]
    run_id = recorder.start_run(
        mode="paper", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    signal_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long", reason="entry"
    )

    decision = await guard.submit(
        intent=OrderIntent.enter_long("4h", 5.0, 1.0),
        signal_id=signal_id,
        run_id=run_id,
        ts=datetime.now(UTC),
    )

    assert decision.decision == Decision.CLAMPED
    assert executor.last_size_usd == 2.0
