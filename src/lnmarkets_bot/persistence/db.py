"""SQLAlchemy engine + session helpers."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

if TYPE_CHECKING:
    from collections.abc import Iterator


def make_engine(db_path: str | Path) -> Engine:
    """Create a sync SQLite engine. Creates parent directories as needed."""
    p = Path(db_path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    # `check_same_thread=False` lets us use the engine across async boundaries (we don't,
    # but it costs nothing and removes one footgun).
    url = f"sqlite:///{p}"
    return _engine_factory(url)


def init_schema(engine: Engine) -> None:
    """Create tables and apply lightweight, idempotent SQLite migrations."""
    Base.metadata.create_all(engine)
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    order_columns = {column["name"] for column in inspector.get_columns("orders")}
    signal_columns = {column["name"] for column in inspector.get_columns("signals")}
    with engine.begin() as connection:
        if "trigger_tf" not in order_columns:
            connection.execute(
                text("ALTER TABLE orders ADD COLUMN trigger_tf VARCHAR NOT NULL DEFAULT ''")
            )
        if "strategy_instance_id" not in order_columns:
            connection.execute(
                text(
                    "ALTER TABLE orders ADD COLUMN strategy_instance_id VARCHAR NOT NULL DEFAULT ''"
                )
            )
        if "position_key" not in order_columns:
            connection.execute(
                text("ALTER TABLE orders ADD COLUMN position_key VARCHAR NOT NULL DEFAULT ''")
            )
        if "strategy_instance_id" not in signal_columns:
            connection.execute(
                text(
                    "ALTER TABLE signals ADD COLUMN strategy_instance_id "
                    "VARCHAR NOT NULL DEFAULT ''"
                )
            )
        if "position_key" not in signal_columns:
            connection.execute(
                text("ALTER TABLE signals ADD COLUMN position_key VARCHAR NOT NULL DEFAULT ''")
            )


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Indirection so tests can swap factories.
def _engine_factory(url: str) -> Engine:
    from sqlalchemy import create_engine

    return create_engine(url, future=True, echo=False)
