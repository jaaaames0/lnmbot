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
    columns = {column["name"] for column in inspect(engine).get_columns("orders")}
    if "trigger_tf" not in columns:
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE orders ADD COLUMN trigger_tf VARCHAR NOT NULL DEFAULT ''")
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
