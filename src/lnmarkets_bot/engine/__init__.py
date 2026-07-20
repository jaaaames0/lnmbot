"""Engines — backtest + live (paper-mode in v0).

The architecture rule says: the same Strategy implementation runs in both.
These engines implement that contract. They differ only in:
  - which DataSource they consume
  - how they react to the executor's response (backtest can do fill-queuing
    for realism; paper mode fills immediately for the placeholder).
"""
from __future__ import annotations
