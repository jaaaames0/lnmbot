"""Run lifecycle + kill switch.

The kill switch is checked at startup AND each tick of the engine loop.
Setting `TRADINGBOT_HALTED=1` or creating the halt file halts immediately.
"""
from __future__ import annotations
