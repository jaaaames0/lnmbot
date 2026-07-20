"""Schema upgrades for existing SQLite databases."""

from __future__ import annotations

from sqlalchemy import inspect, text

from lnmarkets_bot.persistence.db import init_schema, make_engine


def test_init_schema_adds_trigger_tf_to_an_existing_orders_table(tmp_path):
    engine = make_engine(tmp_path / "legacy.sqlite")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY)"))

    init_schema(engine)

    columns = {column["name"] for column in inspect(engine).get_columns("orders")}
    assert "trigger_tf" in columns
