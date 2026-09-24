"""One live loop for multiple independently owned strategies.

The data feed, risk guard and executor are shared.  Each strategy retains its
own state and durable snapshot, while every order is routed through a globally
unique ``strategy_instance_id:position_key`` address.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ..control.kill import KillSwitch
from ..control.lifecycle import run_session
from ..logging import get_logger
from ..risk.guard import RiskGuard
from ..risk.limits import from_config as limits_from_config
from ..strategy import StrategyState, intents_to_list
from ..strategy.base import TfPosition

if TYPE_CHECKING:
    from ..config import BotConfig
    from ..data.source import DataSource
    from ..persistence.recorder import Recorder
    from ..strategy import Strategy


@dataclass(frozen=True)
class StrategyBinding:
    instance_id: str
    strategy: Strategy

    @property
    def state_name(self) -> str:
        return f"{type(self.strategy).__module__}.{type(self.strategy).__name__}"

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
        binding.strategy.reconcile_execution_state(state)
        binding.strategy.on_startup(state)
        states[binding.instance_id] = state

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
                        for event in external_events:
                            owner_binding = bindings_by_id.get(event.strategy_instance_id)
                            if owner_binding is None:
                                raise RuntimeError(
                                    "externally closed trade has no owning strategy binding"
                                )
                            hook = getattr(
                                owner_binding.strategy, "on_external_position_closed", None
                            )
                            if hook is None:
                                log.critical(
                                    "live.external_close_unhandled",
                                    strategy_instance_id=event.strategy_instance_id,
                                    position_key=event.position_key,
                                    trade_id=event.trade_id,
                                )
                            else:
                                hook(event, states[owner_binding.instance_id])
                                persistent = owner_binding.strategy.persistent_state()
                                if persistent is not None:
                                    recorder.save_strategy_state(
                                        run_id,
                                        mode="live",
                                        strategy_name=owner_binding.state_name,
                                        ts=bar.ts,
                                        state=persistent,
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
                    intents = intents_to_list(strategy.on_bar(bar, state))
                    snapshot_due = not bar.warmup and (
                        bar.timeframe in binding.subscribed_timeframes
                        or bool(intents)
                        or not strategy_snapshot_saved[binding.instance_id]
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
            with suppress(Exception):
                await data_source.close()
            for binding in bindings:
                binding.strategy.on_shutdown(states[binding.instance_id])

        log.info("live.portfolio_done", run_id=run_id)
        return int(run_id)


__all__ = ["StrategyBinding", "run_portfolio_live"]
