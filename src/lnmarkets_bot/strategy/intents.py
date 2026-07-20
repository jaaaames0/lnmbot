"""Order intent types.

A strategy never builds an API request — only an `OrderIntent`. The engine
translates intents into either simulated fills (backtest) or signed HTTP calls
(live). The risk guard sits between the engine and the live API; strategies
cannot bypass it.

Under **isolated margin** (v1.1), each timeframe maintains its own independent
position. `trigger_tf` records which TF's signal produced this intent — the
risk guard and executor use it to route the intent to the correct per-TF
position slot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


class SignalKind(str, Enum):
    NOOP = "noop"
    ENTRY = "entry"
    EXIT = "exit"
    RESIZE = "resize"


@dataclass(frozen=True)
class OrderIntent:
    """What the strategy wants to happen, expressed in strategy-neutral terms.

    `trigger_tf` is the subscribed timeframe whose bar produced this signal
    (e.g. "1d", "4h"). Under isolated margin each TF has its own position;
    the engine uses `trigger_tf` to route the intent to the right slot.

    `size_usd` is the *target* position size (notional USD). For ENTRY: target
    size after fill. For RESIZE: new target size. For EXIT: ignored.
    `leverage` is the requested leverage.
    `metadata` is a free-form JSON bag (e.g. regime tag, reason codes) that
    flows into the recorder but doesn't influence execution.
    """

    kind: SignalKind
    trigger_tf: str
    side: Side | None = None
    size_usd: float = 0.0
    leverage: float = 1.0
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def noop(
        cls,
        trigger_tf: str,
        reason: str = "no_signal",
        metadata: dict[str, Any] | None = None,
    ) -> "OrderIntent":
        return cls(
            kind=SignalKind.NOOP,
            trigger_tf=trigger_tf,
            reason=reason,
            metadata=metadata or {},
        )

    @classmethod
    def enter_long(
        cls,
        trigger_tf: str,
        size_usd: float,
        leverage: float,
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "OrderIntent":
        return cls(
            kind=SignalKind.ENTRY,
            trigger_tf=trigger_tf,
            side=Side.LONG,
            size_usd=size_usd,
            leverage=leverage,
            reason=reason,
            metadata=metadata or {},
        )

    @classmethod
    def enter_short(
        cls,
        trigger_tf: str,
        size_usd: float,
        leverage: float,
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "OrderIntent":
        return cls(
            kind=SignalKind.ENTRY,
            trigger_tf=trigger_tf,
            side=Side.SHORT,
            size_usd=size_usd,
            leverage=leverage,
            reason=reason,
            metadata=metadata or {},
        )

    @classmethod
    def exit(
        cls,
        trigger_tf: str,
        reason: str = "exit",
        metadata: dict[str, Any] | None = None,
    ) -> "OrderIntent":
        return cls(
            kind=SignalKind.EXIT,
            trigger_tf=trigger_tf,
            reason=reason,
            metadata=metadata or {},
        )

    @classmethod
    def resize(
        cls,
        trigger_tf: str,
        side: Side,
        size_usd: float,
        leverage: float,
        reason: str = "resize",
        metadata: dict[str, Any] | None = None,
    ) -> "OrderIntent":
        return cls(
            kind=SignalKind.RESIZE,
            trigger_tf=trigger_tf,
            side=side,
            size_usd=size_usd,
            leverage=leverage,
            reason=reason,
            metadata=metadata or {},
        )