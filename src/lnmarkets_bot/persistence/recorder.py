"""Recorder — the only thing engines write to.

One method per event type. Sync API. All values stored in sats/UTC as per the
table schema. Use the dataclasses in `domain.py` if you want typed payloads,
but they're optional — kwargs are the contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, insert, select

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session, sessionmaker

from .models import (
    account_snapshots,
    bars,
    daily_pnl,
    fills,
    funding_fees,
    orders,
    risk_events,
    runs,
    signals,
)


@dataclass
class _InsertResult:
    id: int


class Recorder:
    """Thin wrapper over a SQLAlchemy session factory."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._factory = session_factory

    # ---- Run lifecycle ----

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
        with self._factory() as session, session.begin():
            result = session.execute(
                insert(runs).values(
                    mode=mode,
                    strategy_name=strategy_name,
                    strategy_params_json=strategy_params,
                    config_json=config,
                    started_at=started_at,
                    status="running",
                    notes=notes,
                )
            )
            return int(result.inserted_primary_key[0])

    def end_run(self, run_id: int, *, status: str, ended_at: datetime) -> None:
        with self._factory() as session, session.begin():
            session.execute(
                runs.update().where(runs.c.id == run_id).values(status=status, ended_at=ended_at)
            )

    # ---- Per-bar / per-signal ----

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
        with self._factory() as session, session.begin():
            # Idempotent: if the (run_id, ts) pair exists already, update it.
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(bars).values(
                run_id=run_id,
                ts=ts,
                open=open,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[bars.c.run_id, bars.c.ts],
                set_=dict(
                    open=stmt.excluded.open,
                    high=stmt.excluded.high,
                    low=stmt.excluded.low,
                    close=stmt.excluded.close,
                    volume=stmt.excluded.volume,
                ),
            )
            session.execute(stmt)

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
        with self._factory() as session, session.begin():
            result = session.execute(
                insert(signals).values(
                    run_id=run_id,
                    ts=ts,
                    kind=kind,
                    side=side,
                    target_size_usd=target_size_usd,
                    target_leverage=target_leverage,
                    reason=reason,
                    metadata_json=metadata or {},
                )
            )
            return int(result.inserted_primary_key[0])

    # ---- Orders / fills ----

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
        with self._factory() as session, session.begin():
            result = session.execute(
                insert(orders).values(
                    run_id=run_id,
                    signal_id=signal_id,
                    ts=ts,
                    trigger_tf=trigger_tf,
                    side=side,
                    qty_sats=qty_sats,
                    leverage=leverage,
                    price_usd=price_usd,
                    status=status,
                    lnm_order_id=lnm_order_id,
                    rejection_reason=rejection_reason,
                    metadata_json=metadata or {},
                )
            )
            return int(result.inserted_primary_key[0])

    def update_order_status(
        self,
        order_id: int,
        *,
        status: str,
        price_usd: float | None = None,
        lnm_order_id: str | None = None,
        rejection_reason: str | None = None,
    ) -> None:
        with self._factory() as session, session.begin():
            values: dict[str, Any] = {"status": status}
            if price_usd is not None:
                values["price_usd"] = price_usd
            if lnm_order_id is not None:
                values["lnm_order_id"] = lnm_order_id
            if rejection_reason is not None:
                values["rejection_reason"] = rejection_reason
            session.execute(orders.update().where(orders.c.id == order_id).values(**values))

    def record_fill(
        self,
        order_id: int,
        *,
        ts: datetime,
        qty_sats: int,
        price_usd: float,
        fee_sats: int,
    ) -> int:
        with self._factory() as session, session.begin():
            result = session.execute(
                insert(fills).values(
                    order_id=order_id,
                    ts=ts,
                    qty_sats=qty_sats,
                    price_usd=price_usd,
                    fee_sats=fee_sats,
                )
            )
            return int(result.inserted_primary_key[0])

    def latest_orders_for_lnm_trades(self, trade_ids: set[str]) -> dict[str, dict[str, Any]]:
        """Return the latest locally recorded action for each isolated trade ID."""
        if not trade_ids:
            return {}
        latest: dict[str, dict[str, Any]] = {}
        with self._factory() as session:
            rows = session.execute(
                select(
                    orders.c.id,
                    orders.c.ts,
                    orders.c.lnm_order_id,
                    orders.c.trigger_tf,
                    orders.c.side,
                    orders.c.qty_sats,
                    orders.c.leverage,
                    orders.c.price_usd,
                    orders.c.metadata_json,
                )
                .where(orders.c.lnm_order_id.in_(trade_ids))
                .order_by(orders.c.id.asc())
            ).all()
        for row in rows:
            if row.lnm_order_id:
                latest[row.lnm_order_id] = {
                    "ts": row.ts,
                    "trigger_tf": row.trigger_tf,
                    "side": row.side,
                    "qty_sats": row.qty_sats,
                    "leverage": row.leverage,
                    "price_usd": row.price_usd,
                    "metadata": row.metadata_json if isinstance(row.metadata_json, dict) else {},
                }
        return latest

    # ---- Account / risk ----

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
        with self._factory() as session, session.begin():
            session.execute(
                insert(account_snapshots).values(
                    run_id=run_id,
                    ts=ts,
                    balance_sats=balance_sats,
                    equity_sats=equity_sats,
                    margin_used_sats=margin_used_sats,
                    unrealized_pnl_sats=unrealized_pnl_sats,
                )
            )

    def record_risk_event(
        self,
        run_id: int,
        *,
        ts: datetime,
        kind: str,
        signal_id: int | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        with self._factory() as session, session.begin():
            session.execute(
                insert(risk_events).values(
                    run_id=run_id,
                    ts=ts,
                    kind=kind,
                    signal_id=signal_id,
                    detail_json=detail or {},
                )
            )

    def upsert_daily_pnl(
        self,
        run_id: int,
        *,
        date_str: str,
        realized_delta_sats: int = 0,
        unrealized_delta_sats: int = 0,
        funding_delta_sats: int = 0,
    ) -> None:
        """Add to the day's running totals (used by both backtest and live)."""
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        with self._factory() as session, session.begin():
            stmt = sqlite_insert(daily_pnl).values(
                run_id=run_id,
                date=date_str,
                realized_pnl_sats=realized_delta_sats,
                unrealized_pnl_sats=unrealized_delta_sats,
                funding_pnl_sats=funding_delta_sats,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[daily_pnl.c.run_id, daily_pnl.c.date],
                set_=dict(
                    realized_pnl_sats=daily_pnl.c.realized_pnl_sats
                    + stmt.excluded.realized_pnl_sats,
                    unrealized_pnl_sats=daily_pnl.c.unrealized_pnl_sats
                    + stmt.excluded.unrealized_pnl_sats,
                    funding_pnl_sats=daily_pnl.c.funding_pnl_sats + stmt.excluded.funding_pnl_sats,
                ),
            )
            session.execute(stmt)

    def net_daily_pnl_sats(self, date_str: str) -> int:
        """Return all bot-recorded realized P&L plus funding for one UTC day.

        This intentionally spans runs so a service restart cannot reset the
        daily-loss circuit breaker.
        """
        with self._factory() as session:
            value = session.execute(
                select(
                    func.coalesce(
                        func.sum(daily_pnl.c.realized_pnl_sats + daily_pnl.c.funding_pnl_sats),
                        0,
                    )
                ).where(daily_pnl.c.date == date_str)
            ).scalar_one()
        return int(value)

    def record_funding_fee(
        self,
        run_id: int,
        *,
        trade_id: str | None,
        settlement_id: str,
        ts: datetime,
        fee_sats: int,
        raw: dict[str, Any],
    ) -> bool:
        """Insert a funding settlement once; return whether it was new."""
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        with self._factory() as session, session.begin():
            stmt = sqlite_insert(funding_fees).values(
                run_id=run_id,
                trade_id=trade_id,
                settlement_id=settlement_id,
                ts=ts,
                fee_sats=fee_sats,
                raw_json=raw,
            )
            result = session.execute(
                stmt.on_conflict_do_nothing(index_elements=["trade_id", "settlement_id"])
            )
            return bool(result.rowcount)
