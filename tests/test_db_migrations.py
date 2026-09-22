"""Schema upgrades for existing SQLite databases."""

from __future__ import annotations

from sqlalchemy import inspect, text

from lnmarkets_bot.persistence.db import init_schema, make_engine


def test_init_schema_adds_portfolio_identity_to_existing_tables(tmp_path):
    engine = make_engine(tmp_path / "legacy.sqlite")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE signals (id INTEGER PRIMARY KEY)"))

    init_schema(engine)

    inspector = inspect(engine)
    order_columns = {column["name"] for column in inspector.get_columns("orders")}
    signal_columns = {column["name"] for column in inspector.get_columns("signals")}
    assert {"trigger_tf", "strategy_instance_id", "position_key"} <= order_columns
    assert {"strategy_instance_id", "position_key"} <= signal_columns
    assert "strategy_pnl_events" in inspector.get_table_names()
