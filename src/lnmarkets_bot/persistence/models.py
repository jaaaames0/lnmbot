"""SQLAlchemy Core tables for run/signal/order/fill/snapshot persistence.

All amounts are stored in **satoshis** (1 BTC = 1e8 sats) except fields explicitly
named *_usd. Times are stored as ISO 8601 UTC strings — sort-friendly without tz surprises.

A future dashboard reads from these tables directly via SQLAlchemy Core `select()`.
No ORM; raw tables.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


runs = Table(
    "runs",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mode", String, nullable=False),  # backtest | paper | live
    Column("strategy_name", String, nullable=False),
    Column("strategy_params_json", JSON, nullable=False, default=dict),
    Column("config_json", JSON, nullable=False, default=dict),
    Column("started_at", DateTime, nullable=False),
    Column("ended_at", DateTime, nullable=True),
    Column("status", String, nullable=False, default="running"),  # running | done | halted | error
    Column("notes", String, nullable=True),
)

bars = Table(
    "bars",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Float, nullable=False),
    UniqueConstraint("run_id", "ts", name="uq_bars_run_ts"),
)

signals = Table(
    "signals",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("kind", String, nullable=False),  # entry_long | entry_short | exit | resize | noop
    Column("side", String, nullable=True),  # long | short | None
    Column("target_size_usd", Float, nullable=True),
    Column("target_leverage", Float, nullable=True),
    Column("reason", String, nullable=False),
    Column("metadata_json", JSON, nullable=False, default=dict),
)

orders = Table(
    "orders",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("signal_id", Integer, ForeignKey("signals.id"), nullable=True),
    Column("ts", DateTime, nullable=False),
    Column("trigger_tf", String, nullable=False, default=""),
    Column("side", String, nullable=False),  # buy | sell
    Column("qty_sats", Integer, nullable=False),
    Column("leverage", Float, nullable=False),
    Column("price_usd", Float, nullable=True),  # None at submission, set on fill
    Column("status", String, nullable=False),  # pending | open | filled | rejected | canceled
    Column("lnm_order_id", String, nullable=True),
    Column("rejection_reason", String, nullable=True),
    Column("metadata_json", JSON, nullable=False, default=dict),
)

fills = Table(
    "fills",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("order_id", Integer, ForeignKey("orders.id"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("qty_sats", Integer, nullable=False),
    Column("price_usd", Float, nullable=False),
    Column("fee_sats", Integer, nullable=False),
)

account_snapshots = Table(
    "account_snapshots",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("balance_sats", Integer, nullable=False),
    Column("equity_sats", Integer, nullable=False),
    Column("margin_used_sats", Integer, nullable=False),
    Column("unrealized_pnl_sats", Integer, nullable=False),
)

daily_pnl = Table(
    "daily_pnl",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("date", String, nullable=False),  # YYYY-MM-DD UTC
    Column("realized_pnl_sats", Integer, nullable=False, default=0),
    Column("unrealized_pnl_sats", Integer, nullable=False, default=0),
    Column("funding_pnl_sats", Integer, nullable=False, default=0),
    UniqueConstraint("run_id", "date", name="uq_dailypnl_run_date"),
)

funding_fees = Table(
    "funding_fees",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("trade_id", String, nullable=True, index=True),
    Column("settlement_id", String, nullable=False),
    Column("ts", DateTime, nullable=False),
    Column("fee_sats", Integer, nullable=False),
    Column("raw_json", JSON, nullable=False, default=dict),
    UniqueConstraint("trade_id", "settlement_id", name="uq_funding_trade_settlement"),
)

risk_events = Table(
    "risk_events",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("runs.id"), nullable=False, index=True),
    Column("ts", DateTime, nullable=False),
    Column("kind", String, nullable=False),  # clamp | reject | daily_loss | rate_limit | kill
    Column("signal_id", Integer, ForeignKey("signals.id"), nullable=True),
    Column("detail_json", JSON, nullable=False, default=dict),
)
