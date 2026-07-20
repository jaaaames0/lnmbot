"""Live engine — paper-mode in v0, real-trades in v1.1.

Same shape as the backtest engine: DataSource -> Strategy -> RiskGuard -> Executor.
The executor is pluggable; default is PaperFillExecutor (paper), pass a
LiveExecutor (or any Executor instance) for real LNM orders.

The strategy module never knows which executor is in use. This is the
architecture rule.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING

from ..control.kill import KillSwitch
from ..control.lifecycle import run_session
from ..logging import get_logger
from ..persistence.db import init_schema, make_engine, make_session_factory
from ..persistence.recorder import Recorder
from ..risk.guard import RiskGuard
from ..risk.limits import from_config as limits_from_config
from ..strategy import StrategyState, intents_to_list
from .fills import PaperFillExecutor

if TYPE_CHECKING:
    from ..config import BotConfig
    from ..data.source import DataSource
    from ..strategy import Strategy


async def run_paper(
    *,
    cfg: BotConfig,
    data_source: DataSource,
    strategy: Strategy,
    duration_seconds: float | None = None,
    install_signal_handlers: bool = True,
    executor_factory=None,
    recorder_override=None,
    sizing_policy=None,
    account_balance_provider=None,
    run_mode: str = "paper",
) -> int:
    """Same loop as backtest; the point of v0 is to prove it's *literally* the same code.

    Args:
        executor_factory: optional callable `() -> Executor`. If None, uses
            PaperFillExecutor (paper mode). For real-trades mode, pass
            `lambda: LiveExecutor(trades_api=..., recorder=..., run_id=-1)`.
            The factory is called once the run_id is known.
        run_mode: persisted and logged execution mode, either ``paper`` or
            ``live``. The engine loop is shared; the executor determines
            whether orders are simulated or sent to LN Markets.
    """
    if run_mode not in {"paper", "live"}:
        raise ValueError(f"unsupported run mode: {run_mode!r}")
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = recorder_override or Recorder(factory)
    limits = limits_from_config(cfg)
    if executor_factory is None:
        executor = PaperFillExecutor(recorder=recorder, run_id=-1)
    else:
        executor = executor_factory()
    guard = RiskGuard(
        limits=limits,
        recorder=recorder,
        executor=executor,
        sizing_policy=sizing_policy,
        account_balance_provider=account_balance_provider,
    )
    kill = KillSwitch(cfg=cfg)
    state = StrategyState()
    # Same USD-sats convention as backtest — see engine/backtest.py.
    state.balance_sats = int(cfg.initial_balance_usd * 1e8)
    # v1.1 isolated margin: initialize per-TF position slots.
    from lnmarkets_bot.strategy.base import TfPosition

    subscribed_tfs = getattr(type(strategy), "DEFAULTS", {}).get("tfs", ("1d", "4h"))
    if hasattr(strategy, "tfs"):
        subscribed_tfs = strategy.tfs
    for tf in subscribed_tfs:
        state.positions.setdefault(tf, TfPosition())
    # On a reconciled live restart, strategy state must begin as a mirror of
    # the restored executor positions *before* on_startup. MaCross uses the
    # startup hook to mark restored positions for its first confirmed-bar
    # catch-up check.
    for tf, pos in state.positions.items():
        pos.side = executor.position_side(tf)
        pos.qty_sats = executor.position_qty_sats(tf)
        pos.entry_price_usd = executor.position_entry_price(tf)
        exec_pos = executor.positions.get(tf)
        if exec_pos is not None:
            pos.leverage = exec_pos.leverage
    strategy.on_startup(state)

    strategy_name = f"{type(strategy).__module__}.{type(strategy).__name__}"
    with run_session(
        recorder,
        cfg=cfg,
        mode=run_mode,
        strategy_name=strategy_name,
        strategy_params=strategy.params,
        install_signal_handlers=install_signal_handlers,
    ) as run:
        run_id = run.run_id
        executor.run_id = run_id  # type: ignore[attr-defined]
        log = get_logger(run_mode)
        log.info(f"{run_mode}.start", run_id=run_id, strategy=strategy_name)

        # Bound the run for v0 demonstration.
        deadline = None
        if duration_seconds is not None:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + duration_seconds

        n_bars = 0
        n_intents = 0
        last_account_snapshot_ts = None
        try:
            async for bar in data_source.stream():
                if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                    log.info(f"{run_mode}.duration_reached", run_id=run_id)
                    break
                if run.should_stop() or kill.is_halted():
                    log.warning(f"{run_mode}.halt", run_id=run_id)
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

                intents = intents_to_list(strategy.on_bar(bar, state))
                if not bar.warmup:
                    n_bars += 1
                n_intents += len(intents)
                for intent in intents:
                    sig_id = recorder.record_signal(
                        run_id,
                        ts=bar.ts,
                        kind=intent.kind.value,
                        side=intent.side.value if intent.side else None,
                        target_size_usd=intent.size_usd or None,
                        target_leverage=intent.leverage or None,
                        reason=intent.reason,
                        metadata={**intent.metadata, "trigger_tf": intent.trigger_tf},
                    )
                    log.info(
                        "strategy.signal",
                        run_id=run_id,
                        signal_id=sig_id,
                        kind=intent.kind.value,
                        trigger_tf=intent.trigger_tf,
                        side=intent.side.value if intent.side else None,
                        size_usd=intent.size_usd,
                        leverage=intent.leverage,
                        reason=intent.reason,
                    )
                    decision = await guard.submit(
                        intent=intent,
                        signal_id=sig_id,
                        run_id=run_id,
                        ts=bar.ts,
                    )
                    if decision.order_id is not None and decision.order_id > 0:
                        guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
                        log.info(
                            "strategy.order_processed",
                            run_id=run_id,
                            signal_id=sig_id,
                            order_id=decision.order_id,
                            decision=decision.decision.value,
                            detail=decision.detail,
                        )
                # Mirror executor per-TF state into strategy state
                for tf in state.positions:
                    pos = state.positions[tf]
                    pos.side = executor.position_side(tf)
                    pos.qty_sats = executor.position_qty_sats(tf)
                    pos.entry_price_usd = executor.position_entry_price(tf)
                    exec_pos = executor.positions.get(tf)
                    if exec_pos is not None:
                        pos.leverage = exec_pos.leverage
        finally:
            with suppress(Exception):
                await data_source.close()
            strategy.on_shutdown(state)

        log.info(
            f"{run_mode}.done",
            run_id=run_id,
            n_bars=n_bars,
            n_intents=n_intents,
            status="done",
        )
        return run_id
