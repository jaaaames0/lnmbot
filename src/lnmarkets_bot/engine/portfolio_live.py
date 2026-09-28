"""One live loop for multiple independently owned strategies.

The data feed, risk guard and executor are shared.  Each strategy retains its
own state and durable snapshot, while every order is routed through a globally
unique ``strategy_instance_id:position_key`` address.
"""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from ..control.kill import KillSwitch
from ..control.lifecycle import run_session
from ..logging import get_logger
from ..risk.guard import RiskGuard
from ..risk.limits import from_config as limits_from_config
from ..strategy import Bar, StrategyState, intents_to_list
from ..strategy.base import TfPosition

if TYPE_CHECKING:
    from ..config import BotConfig
    from ..data.source import DataSource
    from ..persistence.recorder import Recorder
    from ..strategy import Strategy


_HISTORICAL_FUNDING_RETRY_SECONDS = 30.0
_HISTORICAL_FUNDING_INITIAL_TIMEOUT_SECONDS = 2.0
_HISTORICAL_FUNDING_BACKGROUND_TIMEOUT_SECONDS = 10.0
_HISTORICAL_FUNDING_DAILY_GRACE_SECONDS = 3.0
_HISTORICAL_FUNDING_DAILY_RETRY_SECONDS = 1.0
_MAX_HISTORICAL_CATCHUP_BARS = 4500  # About three days of minute and aggregate bars.
_HISTORICAL_FUNDING_FAILURE_REASONS = frozenset(
    {
        "historical funding provider unavailable",
        "historical funding gap",
        "historical funding source incomplete",
        "historical funding invalid",
        "API pagination cursor did not advance",
    }
)


def _funding_boundary(ts: datetime) -> datetime:
    stamp = ts.astimezone(UTC)
    return stamp.replace(hour=(stamp.hour // 8) * 8, minute=0, second=0, microsecond=0)


async def _verified_historical_funding(
    provider: Any, start: datetime, boundary: datetime
) -> list[tuple[datetime, float, float]]:
    if provider is None:
        raise RuntimeError("historical funding provider unavailable")
    rows = await provider(start, boundary)
    expected = start + timedelta(hours=8)
    verified = []
    for stamp, rate, fixing in sorted(rows, key=lambda row: row[0]):
        if stamp <= start:
            continue
        if stamp != expected:
            raise RuntimeError("historical funding gap")
        if not math.isfinite(rate) or not math.isfinite(fixing) or fixing <= 0:
            raise ValueError("historical funding invalid")
        verified.append((stamp, rate, fixing))
        expected += timedelta(hours=8)
    if expected <= boundary:
        raise RuntimeError("historical funding source incomplete")
    return verified


async def _timed_historical_funding(
    provider: Any, start: datetime, boundary: datetime, timeout: float
) -> list[tuple[datetime, float, float]]:
    return await asyncio.wait_for(
        _verified_historical_funding(provider, start, boundary), timeout=timeout
    )


async def _initial_historical_funding(
    provider: Any, start: datetime, boundary: datetime, *, daily_decision: bool
) -> list[tuple[datetime, float, float]]:
    if not daily_decision or provider is None:
        return await _timed_historical_funding(
            provider, start, boundary, _HISTORICAL_FUNDING_INITIAL_TIMEOUT_SECONDS
        )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _HISTORICAL_FUNDING_DAILY_GRACE_SECONDS
    last_error: Exception | None = None
    for attempt in range(3):
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            return await _timed_historical_funding(
                provider,
                start,
                boundary,
                min(_HISTORICAL_FUNDING_INITIAL_TIMEOUT_SECONDS, remaining),
            )
        except Exception as exc:
            last_error = exc
            if attempt < 2 and deadline - loop.time() > _HISTORICAL_FUNDING_DAILY_RETRY_SECONDS:
                await asyncio.sleep(_HISTORICAL_FUNDING_DAILY_RETRY_SECONDS)
            else:
                break
    if last_error is None:
        raise TimeoutError("historical funding daily grace elapsed")
    raise last_error


def _apply_historical_funding(machine: Any, rows: list[tuple[datetime, float, float]]) -> None:
    for stamp, rate, fixing in rows:
        machine.apply_historical_funding(stamp, rate, fixing)
    machine.historical_funding_available = True


def _replay_historical_bars(
    strategy: Any,
    state: StrategyState,
    bars: list[Bar],
    rows: list[tuple[datetime, float, float]],
) -> None:
    machine = strategy.machine
    last_minute = machine.last_historical_price_ts
    row_index = 0
    machine.historical_funding_available = True
    for bar in bars:
        boundary = _funding_boundary(bar.ts)
        while row_index < len(rows) and rows[row_index][0] <= boundary:
            machine.apply_historical_funding(*rows[row_index])
            row_index += 1
        if bar.timeframe == "1m":
            if last_minute is not None and bar.ts > last_minute + timedelta(minutes=1):
                raise RuntimeError("historical price gap during funding catch-up")
            last_minute = bar.ts
        # A missed decision is evidence for model state, never a late order.
        if intents_to_list(strategy.on_bar(replace(bar, warmup=True), state)):
            raise RuntimeError("historical catch-up emitted an order intent")


@dataclass
class _HistoricalFundingWait:
    bars: list[Bar]
    task: asyncio.Task[list[tuple[datetime, float, float]]] | None = None
    requested_boundary: datetime | None = None
    retry_at: float = 0.0


@dataclass(frozen=True)
class StrategyBinding:
    instance_id: str
    strategy: Strategy

    @property
    def state_name(self) -> str:
        return self.instance_id

    @property
    def subscribed_timeframes(self) -> tuple[str, ...]:
        configured = getattr(self.strategy, "tfs", None)
        if configured:
            return tuple(configured)
        defaults = getattr(type(self.strategy), "DEFAULTS", {})
        return tuple(defaults.get("tfs", ("1d", "4h")))

    @property
    def position_slots(self) -> tuple[str, ...]:
        configured = getattr(self.strategy, "position_slots", None)
        return tuple(configured) if configured else self.subscribed_timeframes

    def execution_key(self, local_key: str) -> str:
        return f"{self.instance_id}:{local_key}"


def _mirror_positions(binding: StrategyBinding, state: StrategyState, executor: Any) -> None:
    for local_key in binding.position_slots:
        global_key = binding.execution_key(local_key)
        pos = state.position(local_key)
        pos.side = executor.position_side(global_key)
        pos.qty_sats = executor.position_qty_sats(global_key)
        pos.entry_price_usd = executor.position_entry_price(global_key)
        exec_pos = executor.positions.get(global_key)
        if exec_pos is not None:
            pos.leverage = exec_pos.leverage
            pos.entry_ts = exec_pos.entry_ts


def _deliver_external(binding, state, events, recorder, run_id):
    """Commit callback state and delivery acknowledgement as one local unit."""
    batch_hook = getattr(binding.strategy, "on_external_positions_closed", None)
    hook = getattr(binding.strategy, "on_external_position_closed", None)
    if batch_hook is not None:
        batch_hook(events, state)
    elif hook is not None:
        for event in events:
            hook(event, state)
    with recorder.atomic():
        snapshot = binding.strategy.persistent_state()
        if snapshot is not None:
            recorder.save_strategy_state(
                run_id,
                mode="live",
                strategy_name=binding.state_name,
                ts=max(e.observed_at for e in events),
                state=snapshot,
            )
        recorder.acknowledge_commands([f"close:{e.trade_id}" for e in events])


async def run_portfolio_live(
    *,
    cfg: BotConfig,
    data_source: DataSource,
    bindings: tuple[StrategyBinding, ...],
    executor: Any,
    recorder: Recorder,
    sizing_policy: Any,
    account_balance_provider: Any,
    duration_seconds: float | None = None,
    install_signal_handlers: bool = True,
    historical_funding_provider=None,
    historical_hydrator=None,
) -> int:
    """Run several strategies through one serialized live execution path."""
    if not bindings:
        raise ValueError("at least one strategy binding is required")
    ids = [binding.instance_id for binding in bindings]
    if len(ids) != len(set(ids)) or any(not value or ":" in value for value in ids):
        raise ValueError("strategy instance ids must be non-empty, unique, and contain no colon")

    guard = RiskGuard(
        limits=limits_from_config(cfg),
        recorder=recorder,
        executor=executor,
        sizing_policy=sizing_policy,
        account_balance_provider=account_balance_provider,
    )
    kill = KillSwitch(cfg=cfg)
    states: dict[str, StrategyState] = {}
    for binding in bindings:
        state = StrategyState()
        state.balance_sats = int(cfg.initial_balance_usd * 1e8)
        for slot in binding.position_slots:
            state.positions[slot] = TfPosition()
        _mirror_positions(binding, state, executor)
        snapshot = recorder.latest_strategy_state(mode="live", strategy_name=binding.state_name)
        if snapshot is None:
            legacy_name = f"{type(binding.strategy).__module__}.{type(binding.strategy).__name__}"
            if sum(type(b.strategy) is type(binding.strategy) for b in bindings) == 1:
                snapshot = recorder.latest_strategy_state(mode="live", strategy_name=legacy_name)
        if snapshot is not None:
            if binding.strategy.restore_persistent_state(snapshot["state"]):
                get_logger("live").info(
                    "live.strategy_state_restored",
                    strategy_instance_id=binding.instance_id,
                    strategy=binding.state_name,
                    snapshot_ts=snapshot["ts"].isoformat(),
                )
            else:
                raise RuntimeError(
                    f"strategy {binding.instance_id} rejected its persisted live state"
                )
        machine = getattr(binding.strategy, "machine", None)
        if (
            machine is not None
            and not machine.historical_model_complete
            and historical_hydrator is not None
        ):
            try:
                await historical_hydrator(machine)
            except Exception as exc:
                # Keep admission blocked and continue funded exposure management.
                get_logger("live").error("live.historical_rebuild_required", error=str(exc))
        binding.strategy.reconcile_execution_state(state)
        states[binding.instance_id] = state
        pending = getattr(executor, "pending_external_events", lambda: [])()
        owned_events = [e for e in pending if e.strategy_instance_id == binding.instance_id]
        if owned_events:
            _deliver_external(binding, state, owned_events, recorder, executor.run_id)
        binding.strategy.on_startup(state)

    portfolio_params = {
        binding.instance_id: {
            "strategy": binding.state_name,
            "params": binding.strategy.params,
        }
        for binding in bindings
    }
    with run_session(
        recorder,
        cfg=cfg,
        mode="live",
        strategy_name="lnmarkets_bot.portfolio",
        strategy_params=portfolio_params,
        install_signal_handlers=install_signal_handlers,
    ) as run:
        run_id = run.run_id
        executor.run_id = run_id
        log = get_logger("live")
        log.info("live.portfolio_start", run_id=run_id, strategy_instances=ids)
        deadline = None
        if duration_seconds is not None:
            deadline = asyncio.get_running_loop().time() + duration_seconds
        last_account_snapshot_ts = None
        strategy_snapshot_saved = {binding.instance_id: False for binding in bindings}
        historical_waits: dict[str, _HistoricalFundingWait] = {}
        try:
            async for bar in data_source.stream():
                if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                    break
                if run.should_stop() or kill.is_halted():
                    log.warning("live.halt", run_id=run_id)
                    break

                if not bar.warmup:
                    recorder.record_bar(
                        run_id,
                        ts=bar.ts,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                    )
                executor.update_price(bar.close)
                guard.current_price_usd = bar.close

                reconcile_external = getattr(executor, "reconcile_external_closures", None)
                if reconcile_external is not None and not bar.warmup and bar.timeframe == "1m":
                    external_events = await reconcile_external(run_id=run_id, ts=bar.ts)
                    if external_events:
                        guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
                        for binding in bindings:
                            _mirror_positions(binding, states[binding.instance_id], executor)
                        bindings_by_id = {value.instance_id: value for value in bindings}
                        if any(
                            e.strategy_instance_id not in bindings_by_id for e in external_events
                        ):
                            raise RuntimeError(
                                "externally closed trade has no owning strategy binding"
                            )
                        for binding in bindings:
                            owned_events = [
                                e
                                for e in external_events
                                if e.strategy_instance_id == binding.instance_id
                            ]
                            if owned_events:
                                _deliver_external(
                                    binding,
                                    states[binding.instance_id],
                                    owned_events,
                                    recorder,
                                    run_id,
                                )

                retry_pending_exits = getattr(executor, "retry_pending_exits", None)
                if retry_pending_exits is not None and not bar.warmup:
                    for position_key, order_id, detail in await retry_pending_exits(
                        run_id=run_id, ts=bar.ts
                    ):
                        guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
                        log.warning(
                            "live.pending_exit_processed",
                            run_id=run_id,
                            position_key=position_key,
                            order_id=order_id,
                            detail=detail,
                        )
                sync_funding = getattr(executor, "sync_funding", None)
                if sync_funding is not None and not bar.warmup:
                    await sync_funding(bar.ts)
                if (
                    account_balance_provider is not None
                    and not bar.warmup
                    and (
                        last_account_snapshot_ts is None
                        or bar.ts - last_account_snapshot_ts >= timedelta(minutes=15)
                    )
                ):
                    try:
                        await account_balance_provider.snapshot(
                            run_id=run_id,
                            ts=bar.ts,
                            price_usd=bar.close,
                            margin_used_usd=executor.open_margin_usd(),
                        )
                        last_account_snapshot_ts = bar.ts
                    except Exception as exc:
                        log.warning("live.account_snapshot_failed", error=str(exc))

                for binding in bindings:
                    strategy = binding.strategy
                    state = states[binding.instance_id]
                    machine = getattr(strategy, "machine", None)
                    campaign = getattr(machine, "campaign", None)
                    historical_health_before = (
                        getattr(machine, "historical_model_complete", True),
                        getattr(machine, "historical_funding_available", True),
                    )
                    skip_strategy_bar = False
                    if (
                        machine is not None
                        and campaign is not None
                        and campaign.origin == "historical"
                    ):
                        skip_strategy_bar = True
                        if machine.historical_model_complete:
                            boundary = _funding_boundary(bar.ts)
                            last = machine.last_historical_funding_ts or campaign.entry_ts
                            wait = historical_waits.get(binding.instance_id)
                            if (
                                wait is None
                                and machine.historical_funding_available
                                and last < boundary
                            ):
                                try:
                                    rows = await _initial_historical_funding(
                                        historical_funding_provider,
                                        last,
                                        boundary,
                                        daily_decision=bar.timeframe == "1d",
                                    )
                                    _apply_historical_funding(machine, rows)
                                except Exception as exc:
                                    machine.historical_funding_available = False
                                    if historical_funding_provider is None:
                                        machine.historical_model_complete = False
                                    else:
                                        wait = _HistoricalFundingWait([])
                                        historical_waits[binding.instance_id] = wait
                                    log.warning(
                                        "live.historical_funding_unavailable",
                                        error_type=type(exc).__name__,
                                        reason=(
                                            str(exc)
                                            if str(exc) in _HISTORICAL_FUNDING_FAILURE_REASONS
                                            else "funding read failed"
                                        ),
                                        boundary=boundary.isoformat(),
                                        retrying=wait is not None,
                                    )
                            elif wait is None and not machine.historical_funding_available:
                                # A restart restores the last verified model and
                                # replays the missing interval from the live feed.
                                wait = _HistoricalFundingWait([])
                                historical_waits[binding.instance_id] = wait

                            if wait is not None and bar.ts >= last + timedelta(hours=8):
                                wait.bars.append(bar)
                                if len(wait.bars) > _MAX_HISTORICAL_CATCHUP_BARS:
                                    if wait.task is not None:
                                        wait.task.cancel()
                                        await asyncio.gather(wait.task, return_exceptions=True)
                                    historical_waits.pop(binding.instance_id)
                                    machine.historical_model_complete = False
                                    machine.historical_funding_available = False
                                    log.error(
                                        "live.historical_rebuild_required",
                                        error="historical catch-up exceeded buffered bar limit",
                                    )
                                else:
                                    loop = asyncio.get_running_loop()
                                    if wait.task is not None and wait.task.done():
                                        task = wait.task
                                        wait.task = None
                                        try:
                                            rows = task.result()
                                        except Exception as exc:
                                            wait.retry_at = (
                                                loop.time() + _HISTORICAL_FUNDING_RETRY_SECONDS
                                            )
                                            log.warning(
                                                "live.historical_funding_retry_failed",
                                                error_type=type(exc).__name__,
                                                reason=(
                                                    str(exc)
                                                    if str(exc)
                                                    in _HISTORICAL_FUNDING_FAILURE_REASONS
                                                    else "funding read failed"
                                                ),
                                                boundary=wait.requested_boundary.isoformat()
                                                if wait.requested_boundary
                                                else None,
                                            )
                                        else:
                                            if (
                                                wait.requested_boundary is not None
                                                and wait.requested_boundary >= boundary
                                            ):
                                                checkpoint = strategy.persistent_state()
                                                try:
                                                    _replay_historical_bars(
                                                        strategy, state, wait.bars, rows
                                                    )
                                                except Exception as exc:
                                                    if (
                                                        checkpoint is None
                                                        or not strategy.restore_persistent_state(
                                                            checkpoint
                                                        )
                                                    ):
                                                        raise RuntimeError(
                                                            "historical replay rollback failed"
                                                        ) from exc
                                                    machine = strategy.machine
                                                    machine.historical_model_complete = False
                                                    machine.historical_funding_available = False
                                                    historical_waits.pop(binding.instance_id)
                                                    log.error(
                                                        "live.historical_rebuild_required",
                                                        error_type=type(exc).__name__,
                                                    )
                                                else:
                                                    historical_waits.pop(binding.instance_id)
                                                    log.info(
                                                        "live.historical_funding_recovered",
                                                        boundary=boundary.isoformat(),
                                                        replayed_bars=len(wait.bars),
                                                    )
                                            else:
                                                wait.retry_at = loop.time()
                                    if (
                                        binding.instance_id in historical_waits
                                        and wait.task is None
                                        and loop.time() >= wait.retry_at
                                    ):
                                        wait.requested_boundary = boundary
                                        wait.task = asyncio.create_task(
                                            _timed_historical_funding(
                                                historical_funding_provider,
                                                last,
                                                boundary,
                                                _HISTORICAL_FUNDING_BACKGROUND_TIMEOUT_SECONDS,
                                            ),
                                        )
                            if (
                                wait is None
                                and machine.historical_model_complete
                                and machine.historical_funding_available
                            ):
                                skip_strategy_bar = False
                        else:
                            historical_waits.pop(binding.instance_id, None)
                    intents = (
                        [] if skip_strategy_bar else intents_to_list(strategy.on_bar(bar, state))
                    )
                    snapshot_due = not bar.warmup and (
                        bar.timeframe in binding.subscribed_timeframes
                        or bool(intents)
                        or not strategy_snapshot_saved[binding.instance_id]
                        or historical_health_before
                        != (
                            getattr(machine, "historical_model_complete", True),
                            getattr(machine, "historical_funding_available", True),
                        )
                    )
                    # Persist the decision state before any remote submission.
                    # A restart must never rediscover the same transition from
                    # a pre-signal snapshot and submit it a second time.
                    if snapshot_due:
                        persistent = strategy.persistent_state()
                        if persistent is not None:
                            recorder.save_strategy_state(
                                run_id,
                                mode="live",
                                strategy_name=binding.state_name,
                                ts=bar.ts,
                                state=persistent,
                            )
                            strategy_snapshot_saved[binding.instance_id] = True

                    pending_intents = list(intents)
                    for original in pending_intents:
                        local_key = original.position_key or original.trigger_tf
                        intent = replace(
                            original,
                            strategy_instance_id=binding.instance_id,
                            position_key=local_key,
                        )
                        signal_id = recorder.record_signal(
                            run_id,
                            ts=bar.ts,
                            kind=intent.kind.value,
                            side=intent.side.value if intent.side else None,
                            target_size_usd=intent.size_usd or None,
                            target_leverage=intent.leverage or None,
                            reason=intent.reason,
                            strategy_instance_id=binding.instance_id,
                            position_key=local_key,
                            metadata={**intent.metadata, "trigger_tf": intent.trigger_tf},
                        )
                        log.info(
                            "strategy.signal",
                            run_id=run_id,
                            signal_id=signal_id,
                            strategy_instance_id=binding.instance_id,
                            position_key=local_key,
                            kind=intent.kind.value,
                            trigger_tf=intent.trigger_tf,
                            side=intent.side.value if intent.side else None,
                            size_usd=intent.size_usd,
                            leverage=intent.leverage,
                            reason=intent.reason,
                        )
                        decision = await guard.submit(
                            intent=intent,
                            signal_id=signal_id,
                            run_id=run_id,
                            ts=bar.ts,
                        )
                        if decision.decision.value == "rejected":
                            strategy.on_intent_rejected(intent)
                        result_hook = getattr(strategy, "on_order_result", None)
                        if result_hook is not None:
                            result_hook(intent, decision, state)
                        if (
                            original.kind.value == "entry"
                            and decision.order_id
                            and decision.order_id > 0
                        ):
                            followup_hook = getattr(strategy, "post_entry_exits", None)
                            if followup_hook is not None:
                                followups = intents_to_list(followup_hook(intent, decision, bar))
                                if any(value.kind.value != "exit" for value in followups):
                                    raise RuntimeError("post-entry followups must reduce exposure")
                                pending_intents.extend(followups)
                        if decision.order_id is not None and decision.order_id > 0:
                            guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
                            log.info(
                                "strategy.order_processed",
                                run_id=run_id,
                                signal_id=signal_id,
                                order_id=decision.order_id,
                                strategy_instance_id=binding.instance_id,
                                position_key=local_key,
                                decision=decision.decision.value,
                                detail=decision.detail,
                            )

                    _mirror_positions(binding, state, executor)
                    strategy.reconcile_execution_state(state)
                    if snapshot_due:
                        persistent = strategy.persistent_state()
                        if persistent is not None:
                            recorder.save_strategy_state(
                                run_id,
                                mode="live",
                                strategy_name=binding.state_name,
                                ts=bar.ts,
                                state=persistent,
                            )
                            strategy_snapshot_saved[binding.instance_id] = True
        finally:
            pending_funding = [
                wait.task for wait in historical_waits.values() if wait.task is not None
            ]
            for task in pending_funding:
                task.cancel()
            if pending_funding:
                await asyncio.gather(*pending_funding, return_exceptions=True)
            with suppress(Exception):
                await data_source.close()
            for binding in bindings:
                binding.strategy.on_shutdown(states[binding.instance_id])

        log.info("live.portfolio_done", run_id=run_id)
        return int(run_id)


__all__ = ["StrategyBinding", "run_portfolio_live"]
