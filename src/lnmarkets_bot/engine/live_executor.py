"""Live trades executor — calls LNM isolated-margin futures API.

Implements the async Executor protocol (same as PaperFillExecutor).

v1.1 design: each subscribed TF maintains its own actual LNM trade. The
state.positions[tf] in the strategy corresponds 1:1 with an LNM isolated
trade (no virtual state). On entry: open a new isolated trade via
`new_trade`. On exit: close that specific trade by id via `close_trade`.

This replaces the earlier cross-margin design that required virtual
per-TF position tracking, with the LNM having only one net position per
symbol.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ..api.isolated import (
    IsolatedCloseResponse,
    IsolatedTradesApi,
    NewIsolatedTradeParams,
)
from ..logging import get_logger
from ..strategy import OrderIntent, Side

_log = get_logger("lnmarkets_bot.engine.live_executor")


class UnsafeLiveStateError(RuntimeError):
    """A remote trade may exist but cannot be safely accounted for locally.

    This is deliberately fatal to the live process.  Continuing after an
    ambiguous write response could allow another entry and compound exposure.
    The next startup reconciliation then remains the single authority for
    deciding whether it is safe to resume.
    """


@dataclass
class _Position:
    """Per-TF position state. Maps 1:1 to an LNM isolated trade.

    `trade_id` is the LNM trade ID. While the trade is open on LNM,
    this state mirrors it. When closed, trade_id is None and qty_sats=0.
    """

    side: str | None = None  # "long" | "short" | None
    qty_sats: int = 0  # signed: + long, - short, 0 flat
    entry_price_usd: float | None = None
    entry_ts: datetime | None = None
    leverage: float = 1.0
    trade_id: str | None = None  # LNM trade ID


class LiveExecutor:
    """Real-trades executor for LNM isolated-margin futures.

    Each TF's signals open and close their own isolated trade. The strategy's
    per-TF virtual state is now identical to the LNM state — no mismatch.
    """

    def __init__(
        self,
        *,
        trades_api: IsolatedTradesApi,
        recorder,
        run_id: int,
        symbol: str = "BTCUSD",
    ) -> None:
        self._api = trades_api
        self._recorder = recorder
        self.run_id = run_id
        self.symbol = symbol
        # Per-TF isolated trade state, keyed by timeframe
        self.positions: dict[str, _Position] = {}
        # Mark price (last seen close)
        self._last_close: float | None = None
        self._unreported_realized_pnl_usd = 0.0
        self._last_funding_sync_at: datetime | None = None

    def update_price(self, price_usd: float) -> None:
        self._last_close = price_usd

    def _ensure_pos(self, tf: str) -> _Position:
        pos = self.positions.get(tf)
        if pos is None:
            pos = _Position()
            self.positions[tf] = pos
        return pos

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
        if intent.kind.value != "exit" and not intent.side:
            return -1, {"noop": True}
        if self._last_close is None:
            return -1, {"noop": True, "reason": "no_price"}

        pos = self._ensure_pos(tf)

        if intent.kind.value == "exit":
            return await self._do_exit(
                pos=pos,
                tf=tf,
                intent=intent,
                signal_id=signal_id,
                run_id=run_id,
                ts=ts,
                leverage=leverage,
            )

        # Entry / resize
        return await self._do_entry(
            pos=pos,
            tf=tf,
            intent=intent,
            signal_id=signal_id,
            run_id=run_id,
            ts=ts,
            size_usd=size_usd,
            leverage=leverage,
        )

    async def _do_exit(
        self,
        *,
        pos,
        tf: str,
        intent: OrderIntent,
        signal_id: int,
        run_id: int,
        ts: datetime,
        leverage: float,
    ) -> tuple[int, dict[str, Any]]:
        """Close the LNM isolated trade for this TF."""
        if pos.qty_sats == 0 or pos.trade_id is None:
            return -1, {"noop": True, "reason": "no_position"}
        # A settlement immediately before this close is otherwise easy to
        # miss, since the trade stops being a managed running position below.
        await self.sync_funding(ts, force=True)
        close_qty_sats = abs(pos.qty_sats)
        side = "sell" if pos.side == "long" else "buy"
        fill_price = self._last_close or 0.0
        order_id = -1
        try:
            resp: IsolatedCloseResponse = await self._api.close_trade(pos.trade_id)
            order_id = self._recorder.record_order(
                run_id,
                signal_id=signal_id,
                ts=ts,
                trigger_tf=tf,
                side=side,
                qty_sats=close_qty_sats,
                leverage=leverage,
                status="filled",
                price_usd=fill_price,
                lnm_order_id=resp.id,
                metadata={
                    "isolated_action": "close",
                    "lnm_trade_id": pos.trade_id,
                    "gross_pl_sats": resp.pl,
                    "closing_fee_sats": resp.closing_fee,
                },
            )
            exit_price = float(resp.raw.get("exitPrice") or fill_price)
            fill_id = self._recorder.record_fill(
                order_id,
                ts=ts,
                qty_sats=close_qty_sats,
                price_usd=exit_price,
                fee_sats=resp.closing_fee,
            )
            net_pl_sats = resp.pl - resp.closing_fee
            self._recorder.upsert_daily_pnl(
                run_id,
                date_str=ts.date().isoformat(),
                realized_delta_sats=net_pl_sats,
            )
            self._unreported_realized_pnl_usd += net_pl_sats * exit_price / 1e8
        except Exception as exc:
            _log.warning("live.close_failed", trade_id=pos.trade_id)
            return -1, {"noop": True, "reason": f"close_failed: {exc}"}
        # Clear the per-TF state. The LNM trade is closed.
        pos.side = None
        pos.qty_sats = 0
        pos.entry_price_usd = None
        pos.entry_ts = None
        pos.trade_id = None
        return order_id, {
            "fill_id": fill_id,
            "price_usd": exit_price,
            "lnm_trade_id": resp.id,
            "gross_pl_sats": resp.pl,
            "fee_sats": resp.closing_fee,
            "net_pl_sats": net_pl_sats,
        }

    async def _do_entry(
        self,
        *,
        pos,
        tf: str,
        intent: OrderIntent,
        signal_id: int,
        run_id: int,
        ts: datetime,
        size_usd: float,
        leverage: float,
    ) -> tuple[int, dict[str, Any]]:
        """Open a new LNM isolated trade for this TF (or close + reopen on flip)."""
        # LNM inverse futures use USD 1 contracts.  `size_usd` is already the
        # desired USD notional, so it must not be converted into BTC sats.
        quantity_contracts = int(size_usd)
        if quantity_contracts <= 0:
            return -1, {"noop": True, "reason": "non_positive_qty"}
        side = "buy" if intent.side == Side.LONG else "sell"
        fill_price = self._last_close or 0.0
        # A same-direction entry is never a resize for isolated trades.  It
        # must be idempotent: opening another isolated trade here would bypass
        # the strategy's one-position-per-timeframe invariant.
        if pos.qty_sats != 0 and pos.trade_id is not None:
            same_side = (side == "buy" and pos.qty_sats > 0) or (
                side == "sell" and pos.qty_sats < 0
            )
            if same_side:
                return -1, {"noop": True, "reason": "position_already_open"}
            close_order_id, close_meta = await self._do_exit(
                pos=pos,
                tf=tf,
                intent=intent,
                signal_id=signal_id,
                run_id=run_id,
                ts=ts,
                leverage=leverage,
            )
            if close_order_id < 0:
                return -1, close_meta

        try:
            trade = await self._api.new_trade(
                NewIsolatedTradeParams(
                    type="market",
                    side=side,
                    quantity=quantity_contracts,
                    leverage=leverage,
                )
            )
        except Exception as exc:
            _log.warning("live.entry_failed", reason=str(exc))
            await self._fail_closed_if_entry_is_ambiguous(exc)
            return -1, {"noop": True, "reason": f"order_failed: {exc}"}

        try:
            order_id = self._recorder.record_order(
                run_id,
                signal_id=signal_id,
                ts=ts,
                trigger_tf=tf,
                side=side,
                qty_sats=quantity_contracts,
                leverage=leverage,
                status="filled",
                price_usd=fill_price,
                lnm_order_id=trade.id,
                metadata={
                    "isolated_action": "open",
                    "lnm_trade_id": trade.id,
                    "quantity_contracts": quantity_contracts,
                    "opening_fee_sats": trade.opening_fee or 0,
                },
            )
            opening_fee_sats = trade.opening_fee or 0
            fill_id = self._recorder.record_fill(
                order_id,
                ts=ts,
                qty_sats=quantity_contracts,
                price_usd=float(trade.entry_price or trade.price or fill_price),
                fee_sats=opening_fee_sats,
            )
            if opening_fee_sats:
                self._recorder.upsert_daily_pnl(
                    run_id,
                    date_str=ts.date().isoformat(),
                    realized_delta_sats=-opening_fee_sats,
                )
                self._unreported_realized_pnl_usd -= opening_fee_sats * fill_price / 1e8
        except Exception as exc:
            # The remote trade is live but absent from local reconciliation
            # state. Compensate immediately; leaving it open would be unsafe.
            try:
                await self._api.close_trade(trade.id)
            except Exception as close_exc:
                _log.critical(
                    "live.unrecorded_trade_open",
                    trade_id=trade.id,
                    persistence_error=str(exc),
                    close_error=str(close_exc),
                )
                raise UnsafeLiveStateError(
                    "remote trade opened but local persistence and compensating close failed"
                ) from close_exc
            _log.error(
                "live.persistence_failed_trade_closed",
                trade_id=trade.id,
                error=str(exc),
            )
            return -1, {
                "noop": True,
                "reason": f"persistence_failed_trade_closed: {exc}",
                "lnm_trade_id": trade.id,
            }
        pos.side = "long" if side == "buy" else "short"
        # The shared strategy-state field retains its legacy name, but for
        # isolated live trades it stores signed USD-contract quantity.
        pos.qty_sats = quantity_contracts if side == "buy" else -quantity_contracts
        pos.entry_price_usd = fill_price
        pos.entry_ts = ts
        pos.leverage = leverage
        pos.trade_id = trade.id
        return order_id, {
            "fill_id": fill_id,
            "price_usd": fill_price,
            "quantity_contracts": quantity_contracts,
            "lnm_trade_id": trade.id,
            "fee_sats": opening_fee_sats,
        }

    async def _fail_closed_if_entry_is_ambiguous(self, cause: Exception) -> None:
        """Stop execution if a failed entry request might have reached LNM.

        A connection timeout is not proof that LNM rejected the order.  Query
        running isolated trades immediately; any trade not already mirrored by
        this executor is unsafe ambiguity.  Do not close it automatically:
        it could be a user-managed trade.  Crash instead, so systemd restart
        reconciliation refuses to resume until the operator resolves it.
        """
        known_trade_ids = {pos.trade_id for pos in self.positions.values() if pos.trade_id}
        try:
            running = await self._api.get_running_trades()
        except Exception as reconcile_exc:
            _log.critical(
                "live.entry_ambiguous_reconcile_failed",
                error=str(reconcile_exc),
            )
            raise UnsafeLiveStateError(
                "entry submission failed and running-trade reconciliation also failed"
            ) from cause
        unknown_ids = [trade.id for trade in running if trade.id not in known_trade_ids]
        if unknown_ids:
            _log.critical(
                "live.entry_ambiguous_remote_trade",
                trade_ids=unknown_ids,
                submission_error=str(cause),
            )
            raise UnsafeLiveStateError(
                "entry submission outcome is ambiguous; untracked remote trade present"
            ) from cause

    async def sync_funding(self, ts: datetime, *, force: bool = False) -> None:
        """Persist new funding settlements for locally managed running trades."""
        if (
            not force
            and self._last_funding_sync_at
            and ts - self._last_funding_sync_at < timedelta(minutes=15)
        ):
            return
        self._last_funding_sync_at = ts
        managed_ids = {pos.trade_id for pos in self.positions.values() if pos.trade_id}
        if not managed_ids:
            return
        from_ts = min(
            (pos.entry_ts or ts for pos in self.positions.values() if pos.trade_id),
            default=ts,
        ) - timedelta(minutes=1)
        try:
            async for row in self._api.iter_funding_fees(from_ts, ts):
                trade_id = str(row.get("tradeId", row.get("trade_id", ""))) or None
                if trade_id not in managed_ids:
                    continue
                settlement_id = str(row.get("settlementId", row.get("settlement_id", "")))
                fee_ts = _parse_lnm_timestamp(row.get("time"))
                if not settlement_id or fee_ts is None:
                    _log.warning("live.funding_row_invalid", raw=row)
                    continue
                fee_sats = int(row.get("fee", 0))
                if self._recorder.record_funding_fee(
                    self.run_id,
                    trade_id=trade_id,
                    settlement_id=settlement_id,
                    ts=fee_ts,
                    fee_sats=fee_sats,
                    raw=row,
                ):
                    self._recorder.upsert_daily_pnl(
                        self.run_id,
                        date_str=fee_ts.date().isoformat(),
                        # LN Markets reports a paid funding fee as positive and
                        # received funding as negative. P&L uses the inverse:
                        # received funding increases account value.
                        funding_delta_sats=-fee_sats,
                    )
                    _log.info(
                        "live.funding_recorded",
                        trade_id=trade_id,
                        settlement_id=settlement_id,
                        fee_sats=fee_sats,
                    )
        except Exception as exc:
            _log.warning("live.funding_sync_failed", error=str(exc))

    async def reconcile(self) -> None:
        """Restore per-timeframe state from LNM running isolated trades.

        Every remotely running trade must have a locally recorded opening
        action with a timeframe. Unknown trades are an unsafe ambiguity, so
        startup fails closed instead of opening a duplicate position.
        """
        running = await self._api.get_running_trades()
        by_id = self._recorder.latest_orders_for_lnm_trades({trade.id for trade in running})
        restored: dict[str, _Position] = {}
        for trade in running:
            local = by_id.get(trade.id)
            if local is None or local["metadata"].get("isolated_action") != "open":
                raise RuntimeError(f"unreconciled isolated trade {trade.id}; refusing live startup")
            tf = str(local["trigger_tf"] or "")
            if not tf or tf in restored:
                raise RuntimeError(
                    f"ambiguous isolated trade mapping for {trade.id}; refusing live startup"
                )
            side = "long" if trade.side == "buy" else "short"
            quantity = int(trade.quantity or local["qty_sats"])
            restored[tf] = _Position(
                side=side,
                qty_sats=quantity if side == "long" else -quantity,
                entry_price_usd=trade.entry_price or trade.price or local["price_usd"],
                entry_ts=local["ts"],
                leverage=float(trade.leverage or local["leverage"]),
                trade_id=trade.id,
            )
        self.positions = restored

    def total_realized_pnl_usd(self) -> float:
        """Cumulative realized P&L is not retained after it is consumed."""
        return 0.0

    def consume_realized_pnl_usd(self) -> float:
        """Return the realized P&L since the previous guard update."""
        delta = self._unreported_realized_pnl_usd
        self._unreported_realized_pnl_usd = 0.0
        return delta

    def position_qty_sats(self, tf: str) -> int:
        pos = self.positions.get(tf)
        return pos.qty_sats if pos else 0

    def position_side(self, tf: str) -> str | None:
        pos = self.positions.get(tf)
        return pos.side if pos else None

    def position_entry_price(self, tf: str) -> float | None:
        pos = self.positions.get(tf)
        return pos.entry_price_usd if pos else None

    def open_notional_usd(self, *, exclude_tf: str | None = None) -> float:
        """Current isolated notional, excluding a TF about to be replaced."""
        return float(
            sum(abs(pos.qty_sats) for tf, pos in self.positions.items() if tf != exclude_tf)
        )

    def open_margin_usd(self, *, exclude_tf: str | None = None) -> float:
        """Approximate current isolated margin from contract notional/leverage."""
        return sum(
            abs(pos.qty_sats) / pos.leverage
            for tf, pos in self.positions.items()
            if tf != exclude_tf and pos.qty_sats and pos.leverage > 0
        )


def _parse_lnm_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None
