"""Risk limits and the order-execution guard.

The guard is the *only* path between an `OrderIntent` and a real or simulated
order. Strategies cannot bypass it.
"""
from __future__ import annotations
