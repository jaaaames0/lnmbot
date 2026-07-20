"""Backtest engine.

Drives the Strategy off a historical DataSource. Routes intents through
RiskGuard -> PaperFillExecutor. Same Strategy + same RiskGuard code as live.

The executor here is simulated (network-free); in live mode it's still a
PaperFillExecutor unless a real TradesApi is plugged in.
"""
from __future__ import annotations

import asyncio
import logging

from ..config import BotConfig
from ..control.kill import KillSwitch
from ..control.lifecycle import run_session
from ..data.source import DataSource
from ..logging import get_logger
from ..persistence.db import make_engine, init_schema, make_session_factory
from ..persistence.recorder import Recorder
from ..risk.guard import RiskGuard
from ..risk.limits import from_config as limits_from_config
from ..strategy import Strategy, StrategyState, intents_to_list
from ..strategy.base import import_strategy as _import
from .fills import PaperFillExecutor


_log = logging.getLogger("lnmarkets_bot.engine.backtest")


async def run_backtest(
    *,
    cfg: BotConfig,
    data_source: DataSource,
    strategy: Strategy,
    duration_seconds: float | None = None,
    install_signal_handlers: bool = True,
) -> int:
    """Drive the backtest loop. Returns the run_id."""
    configure = {
        "max_position_usd": cfg.risk_max_position_usd,
        "max_leverage": cfg.risk_max_leverage,
        "max_daily_loss_usd": cfg.risk_max_daily_loss_usd,
        "max_orders_per_minute": cfg.risk_max_orders_per_minute,
        "data_source": type(data_source).__name__,
        "strategy": type(strategy).__name__,
        "duration_seconds": duration_seconds,
    }
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    limits = limits_from_config(cfg)
    executor = PaperFillExecutor(recorder=recorder, run_id=-1)  # run_id injected below
    guard = RiskGuard(limits=limits, recorder=recorder, executor=executor)
    kill = KillSwitch(cfg=cfg)

    state = StrategyState()
    # Convention for *_sats fields in account_snapshots: USD × 1e8 (i.e. micro-USD).
    # Cross-margin BTC perps report equity in USD; we keep integer math.
    state.balance_sats = int(cfg.initial_balance_usd * 1e8)
    # v1.1 isolated margin: initialize per-TF position slots so the strategy
    # and executor have somewhere to track each TF's independent state.
    from lnmarkets_bot.strategy.base import TfPosition
    subscribed_tfs = getattr(type(strategy), "DEFAULTS", {}).get("tfs", ("1d", "4h"))
    if hasattr(strategy, "tfs"):
        subscribed_tfs = strategy.tfs
    for tf in subscribed_tfs:
        state.positions.setdefault(tf, TfPosition())
    strategy.on_startup(state)

    strategy_name = f"{type(strategy).__module__}.{type(strategy).__name__}"
    with run_session(
        recorder,
        cfg=cfg,
        mode="backtest",
        strategy_name=strategy_name,
        strategy_params=strategy.params,
        install_signal_handlers=install_signal_handlers,
    ) as run:
        run_id = run.run_id
        executor.run_id = run_id  # type: ignore[attr-defined]
        log = get_logger("backtest")
        log.info("backtest.start", run_id=run_id, strategy=strategy_name)

        n_bars = 0
        n_intents = 0
        try:
            async for bar in data_source.stream():
                if run.should_stop():
                    log.warning("backtest.stop_signal run_id=%d", run_id)
                    break
                if kill.is_halted():
                    log.warning("backtest.kill_switch run_id=%d", run_id)
                    recorder.record_risk_event(
                        run_id, ts=bar.ts, kind="kill",
                        detail={"reason": "halt_file_or_env"},
                    )
                    break

                is_exec_bar = bar.timeframe == "1m"

                # 1. Settle fills at this bar's open (queue from prev bar) — v0 no queue.
                # 2. Record the bar — only on the execution (1m) timeline. Higher-TF
                # bars carry the same close as the 1m bar at the boundary and are
                # reconstructed by the multi-TF source if needed.
                if is_exec_bar:
                    recorder.record_bar(
                        run_id, ts=bar.ts,
                        open=bar.open, high=bar.high, low=bar.low,
                        close=bar.close, volume=bar.volume,
                    )
                else:
                    # Diagnostic: log the first few higher-TF bars to see what's emitted
                    if n_bars == 0 and bar.timeframe not in ("1d", "4h"):
                        log.warning("unexpected_bar_tf", tf=bar.timeframe, ts=bar.ts.isoformat())
                executor.update_price(bar.close)
                guard.current_price_usd = bar.close

                # 3. Strategy
                intents = intents_to_list(strategy.on_bar(bar, state))
                if is_exec_bar:
                    n_bars += 1
                n_intents += len(intents)
                for intent in intents:
                    sig_id = recorder.record_signal(
                        run_id, ts=bar.ts, kind=intent.kind.value,
                        side=intent.side.value if intent.side else None,
                        target_size_usd=intent.size_usd or None,
                        target_leverage=intent.leverage or None,
                        reason=intent.reason,
                        metadata={**intent.metadata, "trigger_tf": intent.trigger_tf},
                    )
                    decision = await guard.submit(
                        intent=intent, signal_id=sig_id, run_id=run_id, ts=bar.ts,
                    )
                    if decision.order_id is not None and decision.order_id > 0:
                        # Update executor's realized P&L into the guard
                        guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)
                # Mirror executor per-TF state into strategy state.
                # v1.1 isolated margin: each TF has its own position slot.
                # Aggregate notional across TFs is used for equity calc.
                total_qty_sats = 0
                for tf in state.positions:
                    pos = state.positions[tf]
                    pos.side = executor.position_side(tf)
                    pos.qty_sats = executor.position_qty_sats(tf)
                    pos.entry_price_usd = executor.position_entry_price(tf)
                    pos.leverage = executor.positions.get(tf).leverage if executor.positions.get(tf) else 1.0
                    total_qty_sats += pos.qty_sats
                # equity_sats = balance + sum of per-TF position notionals
                # (= sum(qty_sats_signed * close) — long positive, short negative)
                # For v1 we approximate as net signed qty × close.
                state.equity_sats = int(state.balance_sats + total_qty_sats * bar.close)
                if is_exec_bar:
                    recorder.record_account_snapshot(
                        run_id, ts=bar.ts,
                        balance_sats=state.balance_sats,
                        equity_sats=state.equity_sats,
                        margin_used_sats=abs(total_qty_sats) * 1,
                        unrealized_pnl_sats=0,
                    )
        finally:
            strategy.on_shutdown(state)

        log.info(
            "backtest.done", run_id=run_id,
            n_bars=n_bars, n_intents=n_intents,
            equity_sats=state.equity_sats, status="done",
        )
        return run_id
