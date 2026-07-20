"""Fill simulator (paper mode) for backtest and live.

v1.1: **isolated-margin multi-position executor**. One position per
trigger_tf (the timeframe that produced the signal). Strategies never
see other TFs' positions; the executor owns per-TF position state and
fills each intent immediately at the current price plus slippage.

This implements the `Executor` protocol declared in `risk/guard.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..strategy import OrderIntent, Side

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class FillResult:
    order_id: int
    fill_id: int
    filled_qty_sats: int
    fill_price_usd: float
    fee_sats: int
    side: str  # "buy" | "sell"
    trigger_tf: str


@dataclass
class _Position:
    side: str | None = None  # "long" | "short" | None
    qty_sats: int = 0  # signed: + long, - short, 0 flat
    entry_price_usd: float | None = None
    leverage: float = 1.0
    realized_pnl_usd: float = 0.0


class PaperFillExecutor:
    """Records order + fill synchronously. Always fills fully. One position per TF."""

    def __init__(
        self,
        *,
        recorder,
        run_id: int,
        slippage_bps: float = 5.0,
        fee_bps: float = 10.0,
    ) -> None:
        self.recorder = recorder
        self.run_id = run_id
        self.slippage_bps = slippage_bps
        self.fee_bps = fee_bps
        # Per-TF position state, keyed by timeframe
        self.positions: dict[str, _Position] = {}
        self._last_close: float | None = None
        self._unreported_realized_pnl_usd = 0.0

    def update_price(self, price_usd: float) -> None:
        self._last_close = price_usd

    def _ensure_pos(self, tf: str) -> _Position:
        pos = self.positions.get(tf)
        if pos is None:
            pos = _Position()
            self.positions[tf] = pos
        return pos

    def _apply_fill(self, fill: FillResult) -> None:
        """Update the per-TF position state from a fill, computing realized P&L."""
        pos = self._ensure_pos(fill.trigger_tf)
        prev_qty = pos.qty_sats
        prev_side = pos.side
        prev_entry = pos.entry_price_usd

        new_qty_signed = fill.filled_qty_sats * (1 if fill.side == "buy" else -1)
        combined = prev_qty + new_qty_signed

        # Compute realized P&L on the portion that closes or flips
        if prev_qty > 0 and fill.side == "sell" and prev_entry is not None:
            closing_qty = min(prev_qty, fill.filled_qty_sats)
            pnl_per_sat_usd = (fill.fill_price_usd - prev_entry) / 1e8
            realized = pnl_per_sat_usd * closing_qty
            pos.realized_pnl_usd += realized
            self._unreported_realized_pnl_usd += realized
        elif prev_qty < 0 and fill.side == "buy" and prev_entry is not None:
            closing_qty = min(-prev_qty, fill.filled_qty_sats)
            pnl_per_sat_usd = (prev_entry - fill.fill_price_usd) / 1e8
            realized = pnl_per_sat_usd * closing_qty
            pos.realized_pnl_usd += realized
            self._unreported_realized_pnl_usd += realized

        # Update state
        if combined == 0:
            pos.side = None
            pos.qty_sats = 0
            pos.entry_price_usd = None
        elif combined > 0:
            if prev_side == "long" and prev_entry is not None:
                pos.entry_price_usd = (
                    prev_entry * prev_qty + fill.fill_price_usd * fill.filled_qty_sats
                ) / combined
            else:
                pos.entry_price_usd = fill.fill_price_usd
            pos.qty_sats = combined
            pos.side = "long"
        else:
            if prev_side == "short" and prev_entry is not None:
                pos.entry_price_usd = (
                    prev_entry * (-prev_qty) + fill.fill_price_usd * fill.filled_qty_sats
                ) / (-combined)
            else:
                pos.entry_price_usd = fill.fill_price_usd
            pos.qty_sats = combined
            pos.side = "short"

    async def submit(
        self,
        *,
        intent: OrderIntent,
        signal_id: int,
        run_id: int,
        ts: datetime,
        size_usd: float,
        leverage: float,
    ) -> tuple[int, dict[str, Any]]:
        tf = intent.trigger_tf or "default"
        if intent.kind.value == "noop":
            return -1, {"noop": True}
        # EXIT intents have side=None by design — they close whatever position is
        # currently open on this TF. Skip the side check for exits.
        if not intent.side and intent.kind.value != "exit":
            return -1, {"noop": True}

        if self._last_close is None:
            return -1, {"noop": True, "reason": "no_price"}

        pos = self._ensure_pos(tf)

        # Handle EXIT (close current TF's position)
        if intent.kind.value == "exit":
            if pos.qty_sats == 0:
                return -1, {"noop": True, "reason": "no_position"}
            close_qty_sats = abs(pos.qty_sats)
            side = "sell" if pos.side == "long" else "buy"
            fill_price = self._price_for_close(side)
            fee_sats = int(close_qty_sats * self.fee_bps / 10_000.0)
            order_id = self.recorder.record_order(
                run_id,
                signal_id=signal_id,
                ts=ts,
                trigger_tf=tf,
                side=side,
                qty_sats=close_qty_sats,
                leverage=leverage,
                status="filled",
                price_usd=fill_price,
            )
            fill_id = self.recorder.record_fill(
                order_id,
                ts=ts,
                qty_sats=close_qty_sats,
                price_usd=fill_price,
                fee_sats=fee_sats,
            )
            res = FillResult(
                order_id=order_id,
                fill_id=fill_id,
                filled_qty_sats=close_qty_sats,
                fill_price_usd=fill_price,
                fee_sats=fee_sats,
                side=side,
                trigger_tf=tf,
            )
            self._apply_fill(res)
            return order_id, {"fill_id": fill_id, "price_usd": fill_price, "trigger_tf": tf}

        # ENTRY or RESIZE on this TF
        size_per_sat_usd = self._last_close / 1e8
        qty_sats = int(size_usd / size_per_sat_usd)
        if qty_sats <= 0:
            return -1, {"noop": True, "reason": "non_positive_qty"}

        side = "buy" if intent.side == Side.LONG else "sell"
        if side == "buy":
            fill_price = self._last_close * (1.0 + self.slippage_bps / 10_000.0)
        else:
            fill_price = self._last_close * (1.0 - self.slippage_bps / 10_000.0)
        fee_sats = int(qty_sats * self.fee_bps / 10_000.0)

        order_id = self.recorder.record_order(
            run_id,
            signal_id=signal_id,
            ts=ts,
            trigger_tf=tf,
            side=side,
            qty_sats=qty_sats,
            leverage=leverage,
            status="filled",
            price_usd=fill_price,
        )
        fill_id = self.recorder.record_fill(
            order_id,
            ts=ts,
            qty_sats=qty_sats,
            price_usd=fill_price,
            fee_sats=fee_sats,
        )
        res = FillResult(
            order_id=order_id,
            fill_id=fill_id,
            filled_qty_sats=qty_sats,
            fill_price_usd=fill_price,
            fee_sats=fee_sats,
            side=side,
            trigger_tf=tf,
        )
        self._apply_fill(res)
        pos.leverage = leverage
        return order_id, {
            "fill_id": fill_id,
            "price_usd": fill_price,
            "qty_sats": qty_sats,
            "trigger_tf": tf,
        }

    def _price_for_close(self, side: str) -> float:
        if self._last_close is None:
            raise RuntimeError("no price known — call update_price() first")
        slip = self.slippage_bps / 10_000.0
        return self._last_close * (1.0 + slip if side == "sell" else 1.0 - slip)

    # ---- per-TF state accessors for the engine to mirror into strategy state ----

    def position_side(self, tf: str) -> str | None:
        return self.positions.get(tf, _Position()).side

    def position_qty_sats(self, tf: str) -> int:
        return self.positions.get(tf, _Position()).qty_sats

    def position_entry_price(self, tf: str) -> float | None:
        return self.positions.get(tf, _Position()).entry_price_usd

    def total_realized_pnl_usd(self) -> float:
        return sum(p.realized_pnl_usd for p in self.positions.values())

    def consume_realized_pnl_usd(self) -> float:
        """Return realized P&L since the last call, then reset that delta."""
        delta = self._unreported_realized_pnl_usd
        self._unreported_realized_pnl_usd = 0.0
        return delta
