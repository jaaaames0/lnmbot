#!/usr/bin/env python3
"""Record an operator-closed isolated trade and hold a missed entry flat.

Use only while the live service is stopped. This does not call LN Markets;
the operator supplies values from the already closed trade confirmation.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders
from lnmarkets_bot.persistence.recorder import Recorder


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--trade-id", required=True)
    parser.add_argument("--timeframe", required=True)
    parser.add_argument("--held-verdict", choices=("UP_TRUE", "DOWN_TRUE"), required=True)
    parser.add_argument("--quantity", type=int, required=True)
    parser.add_argument("--leverage", type=float, required=True)
    parser.add_argument("--exit-price", type=float, required=True)
    parser.add_argument("--gross-pl-sats", type=int, required=True)
    parser.add_argument("--closing-fee-sats", type=int, required=True)
    parser.add_argument("--closed-at", type=_parse_utc, required=True)
    args = parser.parse_args()

    engine = make_engine(args.db)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    with factory() as session:
        existing = session.execute(
            select(orders.c.metadata_json).where(orders.c.lnm_order_id == args.trade_id)
        ).scalars()
        close_already_recorded = any(
            isinstance(metadata, dict) and metadata.get("isolated_action") == "close"
            for metadata in existing
        )

    strategy_name = "lnmarkets_bot.strategy.ma_cross.MaCross"
    snapshot = recorder.latest_strategy_state(mode="live", strategy_name=strategy_name)
    if snapshot is None:
        raise SystemExit("no live strategy snapshot found; refusing to create a hold")
    state = snapshot["state"]
    timeframes = state.get("timeframes") if isinstance(state, dict) else None
    if not isinstance(timeframes, dict) or args.timeframe not in timeframes:
        raise SystemExit(f"snapshot has no {args.timeframe!r} timeframe")
    holds = state.setdefault("manual_flat_hold", {})
    if not isinstance(holds, dict):
        raise SystemExit("snapshot manual_flat_hold is malformed")
    holds[args.timeframe] = args.held_verdict

    operation_at = datetime.now(UTC)
    event_ts = operation_at if close_already_recorded else args.closed_at
    run_id = recorder.start_run(
        mode="manual_recovery",
        strategy_name=strategy_name,
        strategy_params={},
        config={},
        started_at=operation_at,
        notes="operator closed a missed-entry position and set a manual flat hold",
    )
    try:
        signal_id = recorder.record_signal(
            run_id,
            ts=event_ts,
            kind="noop" if close_already_recorded else "exit",
            reason=(
                "manual_flat_hold_restored"
                if close_already_recorded
                else "manual_close_missed_entry"
            ),
            metadata={
                "trigger_tf": args.timeframe,
                "manual_flat_hold": args.held_verdict,
                "source": "operator-supplied LN Markets closed-trade record",
            },
        )
        if not close_already_recorded:
            order_id = recorder.record_order(
                run_id,
                signal_id=signal_id,
                ts=args.closed_at,
                trigger_tf=args.timeframe,
                side="sell",
                qty_sats=args.quantity,
                leverage=args.leverage,
                status="filled",
                price_usd=args.exit_price,
                lnm_order_id=args.trade_id,
                metadata={
                    "isolated_action": "close",
                    "lnm_trade_id": args.trade_id,
                    "gross_pl_sats": args.gross_pl_sats,
                    "closing_fee_sats": args.closing_fee_sats,
                    "manual_recovery": True,
                    "manual_flat_hold": args.held_verdict,
                },
            )
            recorder.record_fill(
                order_id,
                ts=args.closed_at,
                qty_sats=args.quantity,
                price_usd=args.exit_price,
                fee_sats=args.closing_fee_sats,
            )
            recorder.upsert_daily_pnl(
                run_id,
                date_str=args.closed_at.date().isoformat(),
                realized_delta_sats=args.gross_pl_sats - args.closing_fee_sats,
            )
        recorder.save_strategy_state(
            run_id,
            mode="live",
            strategy_name=strategy_name,
            ts=event_ts,
            state=state,
        )
        recorder.record_risk_event(
            run_id,
            ts=event_ts,
            kind="manual_flat_hold",
            signal_id=signal_id,
            detail={"timeframe": args.timeframe, "held_verdict": args.held_verdict},
        )
    except Exception:
        recorder.end_run(run_id, status="error", ended_at=datetime.now(UTC))
        raise
    recorder.end_run(run_id, status="complete", ended_at=datetime.now(UTC))
    action = "restored hold for" if close_already_recorded else "recorded manual close for"
    print(f"{action} {args.trade_id} and held {args.timeframe} flat")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
