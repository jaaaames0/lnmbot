"""Structured logging via structlog.

All log lines are JSON-rendered to stdout (systemd/journald-friendly).
When `LOG_LEVEL=DEBUG`, a colored console renderer is used instead — useful
for local development; never relied on for production.
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from pathlib import Path


def configure_logging(level: str = "INFO", log_path: Path | None = None) -> None:
    """Configure structlog + stdlib logging.

    Stdlib root logger is wired to structlog so any `logging.getLogger(...)`
    calls produce the same JSON output.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    pretty = log_level <= logging.DEBUG

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: structlog.types.Processor
    if pretty:
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Redirect stdlib root logger to stdout so any stray logging calls land in the same stream.
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)
    root.setLevel(log_level)
    # Successful REST polls are routine operational noise; retain warnings and
    # errors without writing tens of thousands of HTTP request lines per week.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Optional: also mirror to a JSON-lines file (e.g. for offline analysis).
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(log_level)
        fh.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(fh)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
