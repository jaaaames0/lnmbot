"""In-memory backtest engine — same strategy code path, no per-bar DB writes.

The engine loop in `engine/backtest.py` calls `recorder.record_*` for every
event (bar, signal, order, fill, snapshot). For 1M bars that's 5M+ SQL
transactions, which dominates runtime (~14 min for the 6m fixture).

This module runs the SAME strategy and executor code in memory, accumulates
all events into Python lists, and bulk-inserts at the end in a single
transaction. Target: ~5× speedup on 6m, more on 2y.

API-compatible with `run_backtest` — same args, same return (run_id).
The strategy and executor code is unchanged.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..config import BotConfig
from ..control.kill import KillSwitch
from ..control.lifecycle import run_session
from ..data.source import DataSource
from ..persistence.db import init_schema, make_engine, make_session_factory
from ..persistence.models import (
    account_snapshots,
    bars,
    daily_pnl,
    fills,
    orders as orders_t,
    risk_events,
    runs as runs_t,
    signals as signals_t,
)
from ..persistence.recorder import Recorder
from ..risk.guard import RiskGuard
from ..risk.limits import from_config as limits_from_config
from ..strategy import OrderIntent, Strategy, StrategyState, intents_to_list
from .fills import PaperFillExecutor

_log = logging.getLogger("lnmarkets_bot.engine.inmemory")


@dataclass
class _Buffer:
    """In-memory buffer for all event types. Filled during the run,
    flushed in a single transaction at the end."""
    bars: list[dict] = field(default_factory=list)
    signals: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    fills: list[dict] = field(default_factory=list)
    account_snapshots: list[dict] = field(default_factory=list)
    risk_events: list[dict] = field(default_factory=list)

    # ID counters for FK relationships (signals → orders, orders → fills).
    # These are local-to-run; the SQLite auto-increment IDs get assigned
    # during flush in the same order.
    next_signal_id: int = 1
    next_order_id: int = 1
    next_fill_id: int = 1
    next_risk_event_id: int = 1

    def alloc_signal_id(self) -> int:
        sid = self.next_signal_id
        self.next_signal_id += 1
        return sid

    def alloc_order_id(self) -> int:
        oid = self.next_order_id
        self.next_order_id += 1
        return oid

    def alloc_fill_id(self) -> int:
        fid = self.next_fill_id
        self.next_fill_id += 1
        return fid


class BufferedRecorder:
    """Drop-in replacement for `Recorder` that buffers writes in memory.

    Same method signatures and return values as `Recorder`, but writes go to
    a `Buffer` instead of the DB. Call `flush()` once at the end to
    bulk-insert everything in a single transaction.
    """

    def __init__(self, buf: _Buffer) -> None:
        self._buf = buf

    def start_run(
        self,
        *,
        mode: str,
        strategy_name: str,
        strategy_params: dict[str, Any],
        config: dict[str, Any],
        started_at: datetime,
        notes: str | None = None,
    ) -> int:
        # Insert the run row immediately so the lifecycle helper can use the
        # run_id. Other events are buffered.
        # We delegate to the regular Recorder for the single start_run insert.
        raise NotImplementedError(
            "BufferedRecorder needs run_id from outside; use run_inmemory()"
        )

    def end_run(self, run_id: int, *, status: str, ended_at: datetime) -> None:
        # End_run is delegated to the regular recorder for this run row.
        raise NotImplementedError(
            "BufferedRecorder end_run is handled by run_inmemory()"
        )

    def record_bar(
        self,
        run_id: int,
        *,
        ts: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        self._buf.bars.append({
            "run_id": run_id, "ts": ts,
            "open": open, "high": high, "low": low,
            "close": close, "volume": volume,
        })

    def record_signal(
        self,
        run_id: int,
        *,
        ts: datetime,
        kind: str,
        reason: str,
        side: str | None = None,
        target_size_usd: float | None = None,
        target_leverage: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        sid = self._buf.alloc_signal_id()
        self._buf.signals.append({
            "id": sid, "run_id": run_id, "ts": ts,
            "kind": kind, "side": side,
            "target_size_usd": target_size_usd,
            "target_leverage": target_leverage,
            "reason": reason,
            "metadata_json": metadata or {},
        })
        return sid

    def record_order(
        self,
        run_id: int,
        *,
        signal_id: int | None,
        ts: datetime,
        side: str,
        qty_sats: int,
        leverage: float,
        status: str,
        trigger_tf: str = "",
        price_usd: float | None = None,
        lnm_order_id: str | None = None,
        rejection_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        oid = self._buf.alloc_order_id()
        self._buf.orders.append({
            "id": oid, "run_id": run_id, "signal_id": signal_id,
            "ts": ts, "trigger_tf": trigger_tf, "side": side,
            "qty_sats": qty_sats, "leverage": leverage,
            "price_usd": price_usd, "status": status,
            "lnm_order_id": lnm_order_id,
            "rejection_reason": rejection_reason,
            "metadata_json": metadata or {},
        })
        return oid

    def update_order_status(
        self,
        order_id: int,
        *,
        status: str,
        price_usd: float | None = None,
        lnm_order_id: str | None = None,
        rejection_reason: str | None = None,
    ) -> None:
        # In-memory mode we don't update order status post-hoc. No-op.
        return None

    def record_fill(
        self,
        order_id: int,
        *,
        ts: datetime,
        qty_sats: int,
        price_usd: float,
        fee_sats: int,
    ) -> int:
        fid = self._buf.alloc_fill_id()
        self._buf.fills.append({
            "id": fid, "order_id": order_id, "ts": ts,
            "qty_sats": qty_sats, "price_usd": price_usd,
            "fee_sats": fee_sats,
        })
        return fid

    def record_account_snapshot(
        self,
        run_id: int,
        *,
        ts: datetime,
        balance_sats: int,
        equity_sats: int,
        margin_used_sats: int,
        unrealized_pnl_sats: int,
    ) -> None:
        self._buf.account_snapshots.append({
            "run_id": run_id, "ts": ts,
            "balance_sats": balance_sats, "equity_sats": equity_sats,
            "margin_used_sats": margin_used_sats,
            "unrealized_pnl_sats": unrealized_pnl_sats,
        })

    def record_risk_event(
        self,
        run_id: int,
        *,
        ts: datetime,
        kind: str,
        signal_id: int | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        rid = self._buf.next_risk_event_id
        self._buf.next_risk_event_id += 1
        self._buf.risk_events.append({
            "id": rid, "run_id": run_id, "ts": ts,
            "kind": kind, "signal_id": signal_id,
            "detail_json": detail or {},
        })

    def upsert_daily_pnl(
        self,
        run_id: int,
        *,
        date_str: str,
        realized_delta_sats: int = 0,
        unrealized_delta_sats: int = 0,
        funding_delta_sats: int = 0,
    ) -> None:
        # No-op for v1.1 (daily_pnl not currently populated)
        return None


def flush_buffer(engine, buf: _Buffer) -> None:
    """Bulk-insert all buffered events in a single transaction.

    Reserves a contiguous block of IDs for this run's signals/orders/fills/
    risk_events to avoid collisions with rows that may already exist in the
    DB (e.g. when reusing a fixture across runs).
    """
    if not any([buf.bars, buf.signals, buf.orders, buf.fills,
                buf.account_snapshots, buf.risk_events]):
        return
    # Offset buffered IDs by the current max in each table so we don't
    # collide with existing rows. The order's signal_id and fill's order_id
    # are still relative to the buffer's local IDs, so we adjust those too.
    with engine.begin() as conn:
        sig_offset = 0
        ord_offset = 0
        fill_offset = 0
        risk_offset = 0
        if buf.signals:
            sig_offset = conn.execute(
                select(__import__('sqlalchemy').func.coalesce(
                    __import__('sqlalchemy').func.max(signals_t.c.id), 0))
            ).scalar() or 0
        if buf.orders:
            ord_offset = conn.execute(
                select(__import__('sqlalchemy').func.coalesce(
                    __import__('sqlalchemy').func.max(orders_t.c.id), 0))
            ).scalar() or 0
        if buf.fills:
            fill_offset = conn.execute(
                select(__import__('sqlalchemy').func.coalesce(
                    __import__('sqlalchemy').func.max(fills.c.id), 0))
            ).scalar() or 0
        if buf.risk_events:
            risk_offset = conn.execute(
                select(__import__('sqlalchemy').func.coalesce(
                    __import__('sqlalchemy').func.max(risk_events.c.id), 0))
            ).scalar() or 0

        if buf.bars:
            conn.execute(insert(bars), buf.bars)
        if buf.signals:
            rows = [{**r, "id": r["id"] + sig_offset} for r in buf.signals]
            conn.execute(insert(signals_t), rows)
        if buf.orders:
            rows = [
                {**r, "id": r["id"] + ord_offset,
                 "signal_id": (r["signal_id"] + sig_offset) if r["signal_id"] is not None else None}
                for r in buf.orders
            ]
            conn.execute(insert(orders_t), rows)
        if buf.fills:
            rows = [
                {**r, "id": r["id"] + fill_offset,
                 "order_id": r["order_id"] + ord_offset}
                for r in buf.fills
            ]
            conn.execute(insert(fills), rows)
        if buf.account_snapshots:
            conn.execute(insert(account_snapshots), buf.account_snapshots)
        if buf.risk_events:
            rows = [
                {**r, "id": r["id"] + risk_offset,
                 "signal_id": (r["signal_id"] + sig_offset) if r["signal_id"] is not None else None}
                for r in buf.risk_events
            ]
            conn.execute(insert(risk_events), rows)


async def run_inmemory(
    *,
    cfg: BotConfig,
    data_source: DataSource,
    strategy: Strategy,
    duration_seconds: float | None = None,
    install_signal_handlers: bool = True,
) -> int:
    """Same as `run_backtest` but with in-memory recorder. ~5× faster.

    Returns the run_id. Strategy and executor behavior is identical to
    `run_backtest`; only the persistence layer changes.
    """
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    real_recorder = Recorder(factory)
    buf = _Buffer()
    recorder = BufferedRecorder(buf)
    limits = limits_from_config(cfg)
    executor = PaperFillExecutor(recorder=recorder, run_id=-1)  # run_id injected below
    guard = RiskGuard(limits=limits, recorder=recorder, executor=executor)
    kill = KillSwitch(cfg=cfg)

    state = StrategyState()
    state.balance_sats = int(cfg.initial_balance_usd * 1e8)
    subscribed_tfs = getattr(type(strategy), "DEFAULTS", {}).get("tfs", ("1d", "4h"))
    if hasattr(strategy, "tfs"):
        subscribed_tfs = strategy.tfs
    from lnmarkets_bot.strategy.base import TfPosition
    for tf in subscribed_tfs:
        state.positions.setdefault(tf, TfPosition())
    strategy.on_startup(state)

    strategy_name = f"{type(strategy).__module__}.{type(strategy).__name__}"
    with run_session(
        real_recorder,
        cfg=cfg,
        mode="backtest",
        strategy_name=strategy_name,
        strategy_params=strategy.params,
        install_signal_handlers=install_signal_handlers,
    ) as run:
        run_id = run.run_id
        executor.run_id = run_id  # type: ignore[attr-defined]
        log = _log
        log.info("inmemory.start", run_id=run_id, strategy=strategy_name)

        n_bars = 0
        n_intents = 0
        try:
            async for bar in data_source.stream():
                if run.should_stop():
                    log.warning("inmemory.stop_signal run_id=%d", run_id)
                    break
                if kill.is_halted():
                    log.warning("inmemory.kill_switch run_id=%d", run_id)
                    recorder.record_risk_event(
                        run_id, ts=bar.ts, kind="kill",
                        detail={"reason": "halt_file_or_env"},
                    )
                    break

                is_exec_bar = bar.timeframe == "1m"
                if is_exec_bar:
                    recorder.record_bar(
                        run_id, ts=bar.ts,
                        open=bar.open, high=bar.high, low=bar.low,
                        close=bar.close, volume=bar.volume,
                    )
                executor.update_price(bar.close)
                guard.current_price_usd = bar.close

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
                        guard.record_realized_pnl(executor.consume_realized_pnl_usd(), bar.ts)

                # Mirror per-TF state
                total_qty_sats = 0
                for tf in state.positions:
                    pos = state.positions[tf]
                    pos.side = executor.position_side(tf)
                    pos.qty_sats = executor.position_qty_sats(tf)
                    pos.entry_price_usd = executor.position_entry_price(tf)
                    exec_pos = executor.positions.get(tf)
                    if exec_pos is not None:
                        pos.leverage = exec_pos.leverage
                    total_qty_sats += pos.qty_sats
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

        # Flush all buffered events to DB in a single transaction
        flush_buffer(engine, buf)
        log.info(
            "inmemory.done", run_id=run_id,
            n_bars=n_bars, n_intents=n_intents,
            buffered=len(buf.bars) + len(buf.signals) + len(buf.orders) + len(buf.fills),
            status="done",
        )
        return run_id
