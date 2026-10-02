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

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ..api.client import LnmApiError
from ..api.isolated import (
    IsolatedCloseResponse,
    IsolatedTradesApi,
    NewIsolatedTradeParams,
)
from ..logging import get_logger
from ..strategy import OrderIntent, Side
from ..strategy.intents import SignalKind

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
    strategy_instance_id: str = ""
    position_key: str = ""
    trigger_tf: str = ""
    collateral_sats: int | None = None


@dataclass(frozen=True)
class _PendingExit:
    """An exposure-reducing close that failed and must be retried."""

    intent: OrderIntent
    signal_id: int | None
    leverage: float


@dataclass(frozen=True)
class ExternalClose:
    """A locally owned trade that the venue closed without our close call."""

    execution_key: str
    strategy_instance_id: str
    position_key: str
    trigger_tf: str
    trade_id: str
    observed_at: datetime
    reason: str
    liquidated: bool | None
    price_usd: float
    net_pl_sats: int
    entry_price_usd: float | None = None
    side: str | None = None


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
        legacy_strategy_instance_id: str | None = None,
        max_entry_age_seconds: float | None = None,
        clock=None,
    ) -> None:
        self.authoritative_accounting = True
        self._clock = clock or (lambda: datetime.now(UTC))
        self.max_entry_age_seconds = max_entry_age_seconds
        self._api = trades_api
        self._recorder = recorder
        self.run_id = run_id
        self.symbol = symbol
        self.legacy_strategy_instance_id = legacy_strategy_instance_id
        # Per-TF isolated trade state, keyed by timeframe
        self.positions: dict[str, _Position] = {}
        # Mark price (last seen close)
        self._last_close: float | None = None
        self._unreported_realized_pnl_usd = 0.0
        self._last_funding_sync_at: datetime | None = None
        self._pending_exits: dict[str, _PendingExit] = {}
        self._unknown_remote: set[str] = set()
        self._missing_remote: set[str] = set()
        self._inventory_unavailable = False
        self.admissions_enabled = True
        self.unbound_owners: set[str] = set()
        self.data_health: dict[str, dict[str, str]] = {}

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
        tf = intent.execution_key
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

        blocked = self.entry_admission_reason(intent, ts)
        if blocked:
            return -1, {"noop": True, "reason": blocked}
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

    async def retry_pending_exits(
        self, *, run_id: int, ts: datetime
    ) -> list[tuple[str, int, dict[str, Any]]]:
        """Retry failed exposure-reducing closes on subsequent live bars."""
        completed: list[tuple[str, int, dict[str, Any]]] = []
        for tf, pending in list(self._pending_exits.items()):
            pos = self.positions.get(tf)
            if pos is None or pos.qty_sats == 0 or pos.trade_id is None:
                self._pending_exits.pop(tf, None)
                continue
            order_id, meta = await self._do_exit(
                pos=pos,
                tf=tf,
                intent=pending.intent,
                signal_id=pending.signal_id,
                run_id=run_id,
                ts=ts,
                leverage=pending.leverage,
            )
            if order_id > 0:
                completed.append((tf, order_id, meta))
        return completed

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
        expected_trade = intent.metadata.get("close_trade_id")
        if expected_trade and expected_trade != pos.trade_id:
            raise UnsafeLiveStateError("close obligation belongs to a different owned trade")
        if (self._inventory_unavailable or pos.trade_id in self._missing_remote) and any(
            c["command_key"] == f"close:{pos.trade_id}"
            for c in self._recorder.commands(["submitted"])
        ):
            return -1, {"noop": True, "reason": "close_outcome_unresolved"}
        close_qty_sats = abs(pos.qty_sats)
        side = "sell" if pos.side == "long" else "buy"
        fill_price = self._last_close or 0.0
        closed_trade_id = pos.trade_id
        closed_strategy_id = pos.strategy_instance_id or intent.strategy_instance_id
        closed_position_key = pos.position_key or intent.position_key or intent.trigger_tf
        closed_leverage = pos.leverage
        command_key = f"close:{closed_trade_id}"
        self._recorder.begin_command(
            command_key,
            "close",
            {
                "trade_id": closed_trade_id,
                "execution_key": tf,
                "signal_id": signal_id,
                "reason": intent.reason,
                "metadata": intent.metadata,
                "ts": ts.isoformat(),
            },
        )
        try:
            resp: IsolatedCloseResponse = await self._api.close_trade(closed_trade_id)
        except Exception as exc:
            _log.warning("live.close_failed", trade_id=closed_trade_id)
            self._pending_exits[tf] = _PendingExit(
                intent=intent, signal_id=signal_id, leverage=leverage
            )
            return -1, {"noop": True, "reason": f"close_failed: {exc}"}

        # LN Markets has accepted the exposure-reducing close. From here on,
        # never retry that remote action—even if local accounting fails.
        # Clear local execution state first so a caught fatal error cannot
        # leave this process believing the closed trade is still live.
        await self.sync_funding(ts, force=True)
        self._pending_exits.pop(tf, None)
        pos.side = None
        pos.qty_sats = 0
        pos.entry_price_usd = None
        pos.entry_ts = None
        pos.trade_id = None

        exit_price = float(resp.raw.get("exitPrice") or fill_price)
        close_ts = _parse_lnm_timestamp(resp.raw.get("closedAt")) or (
            self._clock() if self.max_entry_age_seconds is not None else ts
        )
        result = self._execution_result(
            pos_owner=(closed_strategy_id, closed_position_key, intent.trigger_tf),
            run_id=run_id,
            signal_id=signal_id,
            ts=close_ts,
            side=side,
            quantity=close_qty_sats,
            leverage=closed_leverage,
            price=exit_price,
            trade_id=closed_trade_id,
            action="close",
            fee=resp.closing_fee,
            amount=resp.pl - resp.closing_fee,
            metadata={"gross_pl_sats": resp.pl, "closing_fee_sats": resp.closing_fee},
        )
        try:
            self._recorder.command_result(command_key, result)
            order_id, fill_id = self._recorder.apply_command(command_key)
        except Exception as exc:
            raise UnsafeLiveStateError(
                "remote close accepted; durable result must be recovered"
            ) from exc
        net_pl_sats = resp.pl - resp.closing_fee
        self._unreported_realized_pnl_usd += net_pl_sats * exit_price / 1e8
        return order_id, {
            "fill_id": fill_id,
            "price_usd": exit_price,
            "lnm_trade_id": closed_trade_id,
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
            running = await self._api.get_running_trades()
            pending_api = getattr(self._api, "get_open_trades", None)
            pending = await pending_api() if pending_api is not None else []
            self._inventory_unavailable = False
        except Exception:
            self._inventory_unavailable = True
            return -1, {"noop": True, "reason": "venue_inventory_unavailable"}
        known = {value.trade_id for value in self.positions.values() if value.trade_id}
        self._unknown_remote = {t.id for t in [*running, *pending]} - known
        if self.entries_blocked_reason():
            return -1, {"noop": True, "reason": self.entries_blocked_reason()}
        identity = [
            intent.strategy_instance_id,
            intent.position_key or intent.trigger_tf,
            ts.isoformat(),
            intent.kind.value,
            intent.reason,
            intent.metadata,
        ]
        command_key = (
            "entry:"
            + hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
        )
        request = {
            "strategy_instance_id": intent.strategy_instance_id,
            "position_key": intent.position_key or intent.trigger_tf,
            "quantity": quantity_contracts,
            "leverage": leverage,
            "side": side,
            "ts": ts.isoformat(),
            "trigger_tf": intent.trigger_tf,
            "run_id": run_id,
            "signal_id": signal_id,
            "decision_metadata": intent.metadata,
            "reason": intent.reason,
        }
        snapshot = self._recorder.latest_strategy_state(
            mode="live", strategy_name=intent.strategy_instance_id
        )
        if snapshot is not None:
            request["strategy_state_before_submission"] = snapshot["state"]
        blocked = self.entry_admission_reason(intent, ts)
        if blocked:
            return -1, {"noop": True, "reason": blocked}
        if not self._recorder.begin_command(command_key, "entry", request):
            return -1, {"noop": True, "reason": "decision_already_submitted"}
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
            # Only a definitive rejection is reusable. A timeout, 429 or 5xx
            # stays unresolved, even if a single read shows no new position.
            if isinstance(exc, LnmApiError) and exc.status in {
                400,
                401,
                403,
                404,
                405,
                406,
                412,
                413,
                415,
                422,
            }:
                self._recorder.reject_command(command_key)
            _log.error(
                "live.entry_not_accepted", command_key=command_key, error_type=type(exc).__name__
            )
            return -1, {
                "noop": True,
                "reason": "entry_rejected"
                if isinstance(exc, LnmApiError)
                and exc.status in {400, 401, 403, 404, 405, 406, 412, 413, 415, 422}
                else "entry_outcome_unresolved",
            }
        if not trade.id:
            return -1, {"noop": True, "reason": "entry_outcome_unresolved"}
        actual_entry_price = float(trade.entry_price or trade.price or fill_price)
        quantity_contracts = int(trade.quantity or quantity_contracts)
        leverage = float(trade.leverage or leverage)
        opening_fee_sats = trade.opening_fee or 0
        result = self._execution_result(
            pos_owner=(
                intent.strategy_instance_id,
                intent.position_key or intent.trigger_tf,
                intent.trigger_tf,
            ),
            run_id=run_id,
            signal_id=signal_id,
            ts=trade.filled_at or trade.created_at or ts,
            side=side,
            quantity=quantity_contracts,
            leverage=leverage,
            price=actual_entry_price,
            trade_id=trade.id,
            action="open",
            fee=opening_fee_sats,
            amount=-opening_fee_sats,
            metadata={
                "quantity_contracts": quantity_contracts,
                "opening_fee_sats": opening_fee_sats,
            },
        )
        try:
            self._recorder.command_result(command_key, result)
            order_id, fill_id = self._recorder.apply_command(command_key)
        except Exception as exc:
            # Never issue an unrecorded compensating trade. Recovery replays
            # the authoritative result; if its write failed, admission remains blocked.
            raise UnsafeLiveStateError(
                "remote entry accepted; durable result must be recovered"
            ) from exc
        self._unreported_realized_pnl_usd -= opening_fee_sats * actual_entry_price / 1e8
        pos.side = "long" if side == "buy" else "short"
        # The shared strategy-state field retains its legacy name, but for
        # isolated live trades it stores signed USD-contract quantity.
        pos.qty_sats = quantity_contracts if side == "buy" else -quantity_contracts
        pos.entry_price_usd = actual_entry_price
        pos.entry_ts = trade.filled_at or trade.created_at or ts
        pos.collateral_sats = (
            int(trade.margin) + int(trade.maintenance_margin or 0)
            if trade.margin is not None
            else int(quantity_contracts / actual_entry_price / leverage * 1e8)
        )
        pos.leverage = leverage
        pos.trade_id = trade.id
        pos.strategy_instance_id = intent.strategy_instance_id
        pos.position_key = intent.position_key or intent.trigger_tf
        pos.trigger_tf = intent.trigger_tf
        return order_id, {
            "fill_id": fill_id,
            "price_usd": actual_entry_price,
            "quantity_contracts": quantity_contracts,
            "lnm_trade_id": trade.id,
            "fee_sats": opening_fee_sats,
        }

    def entry_admission_reason(self, intent, ts) -> str | None:
        if self.data_health.get(intent.strategy_instance_id):
            return "market_evidence_incomplete"
        blocked = self.entries_blocked_reason()
        if blocked:
            return blocked
        if self.max_entry_age_seconds is not None:
            intended = _parse_lnm_timestamp(intent.metadata.get("intended_entry_ts")) or ts
            age = (_as_utc(self._clock()) - _as_utc(intended)).total_seconds()
            if age < -60 or age > self.max_entry_age_seconds:
                return "entry_expired"
            quote_age = (_as_utc(self._clock()) - _as_utc(ts)).total_seconds()
            if quote_age > 90:
                return "quote_stale"
        return None

    def entries_blocked_reason(self) -> str | None:
        if not self.admissions_enabled:
            return "recovery_admissions_disabled"
        if self.unbound_owners:
            return "owned_position_without_binding"
        if self._inventory_unavailable:
            return "venue_inventory_unavailable"
        if self._missing_remote:
            return "owned_trade_outcome_unresolved"
        if self._unknown_remote:
            return "unowned_remote_exposure"
        if any(
            row["action"] == "entry" for row in self._recorder.commands(["submitted", "received"])
        ):
            return "entry_outcome_unresolved"
        return None

    @staticmethod
    def _execution_result(
        *,
        pos_owner,
        run_id,
        signal_id,
        ts,
        side,
        quantity,
        leverage,
        price,
        trade_id,
        action,
        fee,
        amount,
        metadata,
        external_event=None,
    ):
        owner, slot, timeframe = pos_owner
        stamp = _as_utc(ts).isoformat()
        return {
            "order": dict(
                run_id=run_id,
                signal_id=signal_id,
                ts=stamp,
                trigger_tf=timeframe,
                strategy_instance_id=owner,
                position_key=slot,
                side=side,
                qty_sats=quantity,
                leverage=leverage,
                status="filled",
                price_usd=price,
                lnm_order_id=trade_id,
                metadata={"isolated_action": action, "lnm_trade_id": trade_id, **metadata},
            ),
            "fill": dict(ts=stamp, qty_sats=quantity, price_usd=price, fee_sats=fee),
            "pnl": dict(
                run_id=run_id,
                event_key=("open:" if action == "open" else "close:") + trade_id,
                strategy_instance_id=owner,
                position_key=slot,
                trade_id=trade_id,
                ts=stamp,
                kind=(
                    "opening_fee"
                    if action == "open"
                    else "liquidation"
                    if metadata.get("liquidated")
                    else "external_close_net_pl"
                    if action == "external_close"
                    else "close_net_pl"
                ),
                amount_sats=amount,
                metadata=metadata,
            ),
            "external_event": external_event,
        }

    def pending_external_events(self) -> list[ExternalClose]:
        events = []
        for row in self._recorder.commands(["applied"], undelivered=True):
            raw = row["result_json"].get("external_event")
            if raw:
                raw = dict(raw)
                raw["observed_at"] = _parse_lnm_timestamp(raw["observed_at"])
                events.append(ExternalClose(**raw))
        return events

    async def sync_funding(self, ts: datetime, *, force: bool = False) -> None:
        """Best-effort funding persistence for locally managed running trades.

        Funding is accounting, never an execution prerequisite. Every part of
        this method is therefore safe to defer and retry on the next bar.
        """
        try:
            if (
                not force
                and self._last_funding_sync_at
                and ts - self._last_funding_sync_at < timedelta(minutes=15)
            ):
                return
            # Query from immutable opening history, including closed trades.
            # Full-history overlap is deliberate: no bounded lookback silently
            # drops a delayed settlement. IDs make replay exactly once.
            managed = self._recorder.owned_trade_history()
            managed_ids = set(managed)
            if not managed_ids:
                return
            from_ts = min(_as_utc(row["ts"]) for row in managed.values()) - timedelta(minutes=1)
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
                with self._recorder.atomic():
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
                        pos = managed[trade_id]
                        self._recorder.record_strategy_pnl_event(
                            self.run_id,
                            event_key=f"funding:{trade_id}:{settlement_id}",
                            strategy_instance_id=pos["strategy_instance_id"]
                            or self.legacy_strategy_instance_id
                            or "",
                            position_key=pos["position_key"] or pos["trigger_tf"],
                            trade_id=trade_id,
                            ts=fee_ts,
                            kind="funding",
                            amount_sats=-fee_sats,
                        )
                        # The full-history replay revisits every known
                        # settlement each sync; log only new ones.
                        _log.info(
                            "live.funding_recorded",
                            trade_id=trade_id,
                            settlement_id=settlement_id,
                            fee_sats=fee_sats,
                        )
            self._last_funding_sync_at = ts
        except Exception as exc:
            _log.warning("live.funding_sync_failed", error=str(exc))

    async def reconcile(self) -> None:
        """Restore per-timeframe state from LNM running isolated trades.

        Every remotely running trade must have a locally recorded opening
        action with a timeframe. Unknown trades are an unsafe ambiguity, so
        new entries are blocked while exits for known positions remain active.
        """
        for command in self._recorder.commands(["received"]):
            self._recorder.apply_command(command["command_key"])
        running = await self._api.get_running_trades()
        running_by_id = {trade.id: trade for trade in running}
        by_id = self._recorder.latest_locally_open_lnm_trades()
        self._missing_remote = set(by_id) - set(running_by_id)
        self._inventory_unavailable = False
        unknown_remote = set(running_by_id) - set(by_id)
        self._unknown_remote = unknown_remote
        restored: dict[str, _Position] = {}
        for trade_id, local in by_id.items():
            trade = running_by_id.get(trade_id)
            strategy_instance_id = str(local.get("strategy_instance_id") or "")
            if not strategy_instance_id and self.legacy_strategy_instance_id:
                strategy_instance_id = self.legacy_strategy_instance_id
            position_key = str(local.get("position_key") or local["trigger_tf"] or "")
            tf = f"{strategy_instance_id}:{position_key}" if strategy_instance_id else position_key
            if not tf or tf in restored:
                raise RuntimeError(
                    f"ambiguous isolated trade mapping for {trade_id}; refusing live startup"
                )
            remote_side = trade.side if trade is not None else str(local["side"])
            side = "long" if remote_side == "buy" else "short"
            quantity = int((trade.quantity if trade is not None else 0) or local["qty_sats"])
            restored[tf] = _Position(
                side=side,
                qty_sats=quantity if side == "long" else -quantity,
                entry_price_usd=((trade.entry_price or trade.price) if trade is not None else None)
                or local["price_usd"],
                # SQLite returns DATETIME values without tzinfo even when the
                # original live bar was UTC-aware.  Funding synchronisation
                # compares this timestamp with UTC bar timestamps, so restore
                # it through the same normalisation used for API timestamps.
                entry_ts=_as_utc(local["ts"]),
                leverage=float((trade.leverage if trade is not None else 0) or local["leverage"]),
                trade_id=trade_id,
                strategy_instance_id=strategy_instance_id,
                position_key=position_key,
                trigger_tf=str(local["trigger_tf"] or ""),
                collateral_sats=(
                    int(trade.margin) + int(trade.maintenance_margin or 0)
                    if trade is not None and trade.margin is not None
                    else int(quantity / float(local["price_usd"]) / float(local["leverage"]) * 1e8)
                ),
            )
        self.positions = restored
        for command in self._recorder.commands(["submitted"]):
            if command["action"] != "close":
                continue
            request = command["request_json"]
            key = request.get("execution_key")
            pos = restored.get(key)
            # Missing venue trades are recovered by authoritative external-close
            # reconciliation before retries; never repeat an accepted remote close.
            if pos and pos.trade_id in running_by_id:
                self._pending_exits[key] = _PendingExit(
                    intent=OrderIntent(
                        kind=SignalKind.EXIT,
                        trigger_tf=pos.trigger_tf,
                        strategy_instance_id=pos.strategy_instance_id,
                        position_key=pos.position_key,
                        reason=request.get("reason", "resume_durable_close"),
                        metadata=request.get("metadata", {}),
                    ),
                    signal_id=request.get("signal_id"),
                    leverage=pos.leverage,
                )

    async def reconcile_external_closures(
        self, *, run_id: int, ts: datetime
    ) -> list[ExternalClose]:
        """Persist venue-side liquidation/manual closure and clear local state."""
        try:
            running_trades = await self._api.get_running_trades()
        except Exception as exc:
            self._inventory_unavailable = True
            _log.warning("live.inventory_unavailable", error_type=type(exc).__name__)
            return self.pending_external_events()
        self._inventory_unavailable = False
        running_ids = {trade.id for trade in running_trades}
        remote = {trade.id: trade for trade in running_trades}
        for pos in self.positions.values():
            trade = remote.get(pos.trade_id)
            if trade is not None and trade.margin is not None:
                pos.collateral_sats = int(trade.margin) + int(trade.maintenance_margin or 0)
        known_ids = {pos.trade_id for pos in self.positions.values() if pos.trade_id}
        self._unknown_remote = running_ids - known_ids
        missing = {
            key: pos
            for key, pos in self.positions.items()
            if pos.trade_id is not None and pos.trade_id not in running_ids
        }
        self._missing_remote = {pos.trade_id for pos in missing.values()}
        if not missing:
            return self.pending_external_events()
        try:
            closed = {trade.id: trade for trade in await self._api.get_closed_trades()}
        except Exception as exc:
            _log.warning("live.closed_history_unavailable", error_type=type(exc).__name__)
            return self.pending_external_events()
        for key, pos in missing.items():
            assert pos.trade_id is not None
            trade = closed.get(pos.trade_id)
            if trade is None:
                # Venue history may lag an accepted close. Preserve ownership,
                # block admission and keep managing other owned exposure.
                _log.warning("live.owned_trade_outcome_unresolved", trade_id=pos.trade_id)
                continue
            raw_reason = str(
                trade.raw.get("closeReason")
                or trade.raw.get("close_reason")
                or trade.raw.get("reason")
                or ""
            )
            flag = trade.raw.get("liquidated")
            if "liquid" in raw_reason.lower() or trade.status == "liquidated" or flag is True:
                liquidated = True
            elif raw_reason or flag is False:
                liquidated = False
            else:
                # The real v3 closed-trade schema has a numeric `liquidation`
                # price, not a cause flag. `closed=True` proves flatness only.
                liquidated = None
            reason = (
                "liquidation"
                if liquidated is True
                else "external_close"
                if liquidated is False
                else "external_close_unclassified"
            )
            raw_reason = raw_reason or reason
            exit_price = float(
                trade.raw.get("exitPrice")
                or trade.raw.get("exit_price")
                or trade.price
                or self._last_close
                or 0.0
            )
            if exit_price <= 0:
                raise UnsafeLiveStateError(
                    f"closed managed trade {pos.trade_id} has no usable exit price"
                )
            closing_fee = int(trade.closing_fee or 0)
            gross_pl = int(trade.pl or 0)
            net_pl = gross_pl - closing_fee
            side = "sell" if pos.side == "long" else "buy"
            event_ts = trade.closed_at or (
                self._clock() if self.max_entry_age_seconds is not None else ts
            )
            event = ExternalClose(
                execution_key=key,
                strategy_instance_id=pos.strategy_instance_id,
                position_key=pos.position_key,
                trigger_tf=pos.trigger_tf,
                trade_id=pos.trade_id,
                observed_at=_as_utc(event_ts),
                reason=reason,
                liquidated=liquidated,
                price_usd=exit_price,
                net_pl_sats=net_pl,
                entry_price_usd=pos.entry_price_usd,
                side=pos.side,
            )
            event_data = asdict(event)
            event_data["observed_at"] = event.observed_at.isoformat()
            command_key = f"close:{pos.trade_id}"
            self._recorder.begin_command(
                command_key,
                "close",
                {
                    "trade_id": pos.trade_id,
                    "execution_key": key,
                    "ts": ts.isoformat(),
                },
            )
            result = self._execution_result(
                pos_owner=(pos.strategy_instance_id, pos.position_key, pos.trigger_tf),
                run_id=run_id,
                signal_id=None,
                ts=event_ts,
                side=side,
                quantity=abs(pos.qty_sats),
                leverage=pos.leverage,
                price=exit_price,
                trade_id=pos.trade_id,
                action="external_close",
                fee=closing_fee,
                amount=net_pl,
                metadata={
                    "external_reason": raw_reason,
                    "liquidated": liquidated,
                    "gross_pl_sats": gross_pl,
                    "closing_fee_sats": closing_fee,
                },
                external_event=event_data,
            )
            self._recorder.command_result(command_key, result)
            self._recorder.apply_command(command_key)
            self._unreported_realized_pnl_usd += net_pl * exit_price / 1e8
            self._missing_remote.discard(pos.trade_id)
            self.positions[key] = _Position()
            self._pending_exits.pop(key, None)
        return self.pending_external_events()

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
        """Value recorded BTC collateral at the current mark, including reserves."""
        return sum(
            (
                pos.collateral_sats * self._last_close / 1e8
                if pos.collateral_sats is not None and self._last_close is not None
                else abs(pos.qty_sats) / pos.leverage
            )
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


def _as_utc(value: datetime) -> datetime:
    """Return a UTC-aware datetime, treating legacy SQLite values as UTC."""
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
