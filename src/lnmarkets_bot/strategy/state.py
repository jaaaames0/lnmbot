"""Fill events the engine hands to strategies."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillEvent:
    ts: object  # datetime — imported lazily; we don't import here to avoid cycles
    qty_sats: int
    price_usd: float
    fee_sats: int
    side: str  # "buy" | "sell"
