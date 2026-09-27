#!/usr/bin/env python3
"""Advance the order-incapable close-range book over completed daily candles."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd  # type: ignore[import-untyped]

from lnmarkets_bot.portfolio.store import PortfolioStore, timestamp
from lnmarkets_bot.strategy.close_range import (
    BreakoutDecision,
    CloseRangeMachine,
    DailyCandle,
)

INSTANCE = "btc_close_range_v1_shadow"


class _DailyRow(Protocol):
    ts: Any
    open: Any
    high: Any
    low: Any
    close: Any


def completed(frame: pd.DataFrame, now: datetime) -> pd.DataFrame:
    now_ts = pd.Timestamp(now.astimezone(UTC))
    result = frame[frame.ts + pd.Timedelta(days=1) <= now_ts].copy()
    return result.sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)


def as_candle(row: _DailyRow) -> DailyCandle:
    return DailyCandle(
        ts=row.ts.to_pydatetime(),
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
    )


def decision_dict(value: BreakoutDecision) -> dict[str, object]:
    result = asdict(value)
    result["ts"] = value.ts.isoformat()
    return result


def initialize(
    store: PortfolioStore,
    snapshot: dict[str, object],
    daily: pd.DataFrame,
    now: datetime,
) -> CloseRangeMachine:
    as_of = pd.Timestamp(snapshot["as_of_close"])
    history = daily[daily.ts <= as_of]
    if history.empty or history.iloc[-1].ts != as_of:
        raise ValueError("seed candle is absent from completed daily history")
    if not history.ts.diff().dropna().eq(pd.Timedelta(days=1)).all():
        raise ValueError("seed history must be contiguous")
    machine = CloseRangeMachine()
    machine.warmup([as_candle(row) for row in history.itertuples(index=False)])
    campaign = snapshot.get("active_hypothetical_stack")
    if campaign:
        if not isinstance(campaign, dict):
            raise ValueError("invalid seeded campaign")
        machine.seed_campaign(campaign)
    activated_at = timestamp(str(snapshot["next_open"]))
    if machine.last_bar_ts is None:
        raise ValueError("seed history did not initialize a completed candle")
    store.initialize_machine(
        INSTANCE,
        last_candle_ts=machine.last_bar_ts,
        state=machine.persistent_state(),
        activated_at=activated_at,
    )
    return machine


def verify_processed_source(machine: CloseRangeMachine, daily: pd.DataFrame) -> None:
    if machine.last_bar_ts is None:
        raise ValueError("stored machine has no completed candle")
    processed = daily[daily.ts <= pd.Timestamp(machine.last_bar_ts)]
    if processed.empty or processed.iloc[-1].ts != pd.Timestamp(machine.last_bar_ts):
        raise ValueError("daily source is truncated before stored machine state")
    verifier = CloseRangeMachine()
    verifier.warmup([as_candle(row) for row in processed.itertuples(index=False)])
    if (
        verifier.source_count != machine.source_count
        or verifier.source_digest != machine.source_digest
    ):
        raise ValueError("completed daily source changed after it was processed")


def advance_forward_shadow(
    *,
    snapshot_path: Path,
    daily_path: Path,
    database_path: Path,
    now: datetime,
    forbid_production_path: bool = True,
) -> dict[str, object]:
    if forbid_production_path and database_path.resolve().is_relative_to(Path("/var/lib")):
        raise ValueError("development shadow command cannot write production state")
    now = timestamp(now)
    snapshot = json.loads(snapshot_path.read_text())
    if snapshot.get("order_capability") is not False:
        raise ValueError("shadow source must be order-incapable")
    daily = completed(pd.read_parquet(daily_path), now)
    store = PortfolioStore(database_path)
    store.register(INSTANCE, "paper", str(snapshot["rules_sha256"]))
    store.import_shadow_observation(INSTANCE, snapshot, now=now)
    stored = store.load_machine(INSTANCE)
    machine = (
        CloseRangeMachine.restore(stored["state"])
        if stored
        else initialize(store, snapshot, daily, now)
    )
    verify_processed_source(machine, daily)
    stored = store.load_machine(INSTANCE)
    assert stored is not None and machine.last_bar_ts is not None
    activation = timestamp(stored["activated_at"])
    advanced = 0
    decisions = 0
    for row in daily[daily.ts > pd.Timestamp(machine.last_bar_ts)].itertuples(index=False):
        previous = machine.last_bar_ts
        assert previous is not None
        candle = as_candle(row)
        values = machine.advance(candle, activation_ts=activation)
        store.advance_machine(
            INSTANCE,
            expected_previous_ts=previous,
            candle_ts=candle.ts,
            state=machine.persistent_state(),
            decisions=[decision_dict(value) for value in values],
        )
        advanced += 1
        decisions += len(values)
    current = store.load_machine(INSTANCE)
    assert current is not None
    return {
        "mode": "forward_shadow_no_orders",
        "order_capability": False,
        "database": str(database_path),
        "last_candle_ts": current["last_candle_ts"],
        "days_advanced": advanced,
        "decisions_recorded": decisions,
        "campaign": current["state"].get("campaign"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--database", type=Path, default=Path("runs/portfolio-shadow.sqlite"))
    parser.add_argument("--now", help="UTC test clock; defaults to current time")
    args = parser.parse_args()
    try:
        summary = advance_forward_shadow(
            snapshot_path=args.snapshot,
            daily_path=args.daily,
            database_path=args.database,
            now=timestamp(args.now) if args.now else datetime.now(UTC),
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
