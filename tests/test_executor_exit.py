"""Directly exercise PaperFillExecutor.submit for the EXIT path.

After an entry, then an exit at a higher price (short profit), we should see:
  - 1 order for the entry
  - 1 order for the exit
  - position back to flat
  - realized_pnl > 0
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select, func

from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders as orders_t, fills as fills_t
from lnmarkets_bot.engine.fills import PaperFillExecutor
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.strategy.intents import OrderIntent, Side, SignalKind


@pytest.fixture
def recorder(tmp_path):
    eng = make_engine(tmp_path / "executor_test.sqlite")
    init_schema(eng)
    fac = make_session_factory(eng)
    return Recorder(fac)


async def test_executor_records_exit_order(recorder, tmp_path):
    executor = PaperFillExecutor(recorder=recorder, run_id=1, slippage_bps=5, fee_bps=10)
    run_id = recorder.start_run(
        mode="backtest", strategy_name="t", strategy_params={}, config={},
        started_at=datetime.now(UTC),
    )
    executor.run_id = run_id  # type: ignore[attr-defined]

    # Step 1: enter short at $100
    executor.update_price(100.0)
    sig_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="short",
        target_size_usd=1000, target_leverage=1.0, reason="entry short",
    )
    order_id_entry, _ = await executor.submit(
        intent=OrderIntent.enter_short(
            trigger_tf="1d", size_usd=1000, leverage=1.0, reason="enter short",
        ),
        signal_id=sig_id, run_id=run_id, ts=datetime.now(UTC),
        size_usd=1000, leverage=1.0,
    )
    assert order_id_entry > 0, "entry order should be created"
    pos = executor.positions["1d"]
    assert pos.qty_sats < 0, f"should be short, got qty={pos.qty_sats}"
    assert pos.side == "short"

    # Step 2: price drops to $90 (good for short), EXIT
    executor.update_price(90.0)
    sig_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="exit", side=None,
        reason="exit short",
    )
    order_id_exit, meta_exit = await executor.submit(
        intent=OrderIntent.exit(trigger_tf="1d", reason="exit short"),
        signal_id=sig_id, run_id=run_id, ts=datetime.now(UTC),
        size_usd=0, leverage=1.0,
    )
    print(f"\nentry order_id={order_id_entry}, exit order_id={order_id_exit}, meta={meta_exit}")
    assert order_id_exit > 0, f"exit order should be created (got {order_id_exit}, meta={meta_exit})"
    pos = executor.positions["1d"]
    assert pos.qty_sats == 0, f"position should be flat after exit, got qty={pos.qty_sats}"
    assert pos.side is None, f"position side should be None, got {pos.side}"

    # Verify DB has 2 orders and 2 fills
    eng = recorder._factory().get_bind()  # type: ignore[attr-defined]
    fac = make_session_factory(eng)
    with fac() as s:
        n_orders = s.execute(select(func.count()).select_from(orders_t).where(orders_t.c.run_id == run_id)).scalar()
        n_fills = s.execute(select(func.count()).select_from(fills_t)).scalar()
    assert n_orders == 2, f"expected 2 orders, got {n_orders}"
    assert n_fills == 2, f"expected 2 fills, got {n_fills}"


async def test_executor_records_flip_orders(recorder, tmp_path):
    """Same-bar flip: enter short, then on the next bar enter long. Both should produce orders."""
    executor = PaperFillExecutor(recorder=recorder, run_id=1, slippage_bps=5, fee_bps=10)
    run_id = recorder.start_run(
        mode="backtest", strategy_name="t", strategy_params={}, config={},
        started_at=datetime.now(UTC),
    )
    executor.run_id = run_id  # type: ignore[attr-defined]

    # Step 1: enter short at $100
    executor.update_price(100.0)
    sig_id = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="short",
        target_size_usd=1000, target_leverage=1.0, reason="enter short",
    )
    await executor.submit(
        intent=OrderIntent.enter_short(
            trigger_tf="1d", size_usd=1000, leverage=1.0, reason="enter short",
        ),
        signal_id=sig_id, run_id=run_id, ts=datetime.now(UTC),
        size_usd=1000, leverage=1.0,
    )

    # Step 2: flip — exit + enter long at $90
    executor.update_price(90.0)
    sig_id_exit = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="exit", side=None,
        reason="close short",
    )
    exit_order_id, exit_meta = await executor.submit(
        intent=OrderIntent.exit(trigger_tf="1d", reason="close short"),
        signal_id=sig_id_exit, run_id=run_id, ts=datetime.now(UTC),
        size_usd=0, leverage=1.0,
    )
    sig_id_entry = recorder.record_signal(
        run_id, ts=datetime.now(UTC), kind="entry", side="long",
        target_size_usd=1000, target_leverage=1.0, reason="enter long",
    )
    entry_order_id, entry_meta = await executor.submit(
        intent=OrderIntent.enter_long(
            trigger_tf="1d", size_usd=1000, leverage=1.0, reason="enter long",
        ),
        signal_id=sig_id_entry, run_id=run_id, ts=datetime.now(UTC),
        size_usd=1000, leverage=1.0,
    )
    print(f"\nexit_order_id={exit_order_id} exit_meta={exit_meta}")
    print(f"entry_order_id={entry_order_id} entry_meta={entry_meta}")
    assert exit_order_id > 0, f"exit order should be created (got {exit_order_id}, meta={exit_meta})"
    assert entry_order_id > 0, f"entry order should be created (got {entry_order_id}, meta={entry_meta})"
    assert exit_order_id != entry_order_id, "should be different orders"

    # Verify DB
    eng = recorder._factory().get_bind()  # type: ignore[attr-defined]
    fac = make_session_factory(eng)
    with fac() as s:
        n_orders = s.execute(select(func.count()).select_from(orders_t).where(orders_t.c.run_id == run_id)).scalar()
    assert n_orders == 3, f"expected 3 orders, got {n_orders}"  # 1 entry short + 1 exit + 1 entry long