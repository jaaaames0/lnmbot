"""Data sources.

The strategy/ package receives `Bar` events from one of these. Engines pick
the right DataSource for the mode (backtest → historical parquet replay,
live → LNM WS or a mock when no creds are available).
"""
from __future__ import annotations

from .historical import BacktestReplay
from .mock import MockLiveStream
from .multitimeframe import MultiTimeframeDataSource
from .source import DataSource

__all__ = [
    "BacktestReplay",
    "DataSource",
    "MockLiveStream",
    "MultiTimeframeDataSource",
]
