"""Recorder round-trip — DB persistence primitives the engines rely on."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from lnmarkets_bot.persistence.db import make_session_factory
from lnmarkets_bot.persistence.models import bars, daily_pnl

if TYPE_CHECKING:
    from lnmarkets_bot.persistence.recorder import Recorder


def test_start_and_end_run(recorder: Recorder) -> None:
    rid = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
        notes="go",
    )
    recorder.end_run(rid, status="done", ended_at=datetime.now(UTC))


def test_record_bar_upserts_on_run_id_ts(recorder: Recorder) -> None:
    rid = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    ts = datetime.now(UTC)
    recorder.record_bar(rid, ts=ts, open=1.0, high=2.0, low=0.5, close=1.5, volume=10)
    recorder.record_bar(rid, ts=ts, open=2.0, high=3.0, low=1.0, close=2.5, volume=11)
    fac = make_session_factory(recorder._factory().get_bind())
    with fac() as s:
        rows = s.execute(select(bars.c.close, bars.c.volume).where(bars.c.run_id == rid)).fetchall()
    assert len(rows) == 1, f"upsert expected 1 row, got {len(rows)}"
    assert rows[0].close == 2.5
    assert rows[0].volume == 11


def test_upsert_daily_pnl_accumulates(recorder: Recorder) -> None:
    rid = recorder.start_run(
        mode="backtest",
        strategy_name="t",
        strategy_params={},
        config={},
        started_at=datetime.now(UTC),
    )
    d = "2026-01-01"
    recorder.upsert_daily_pnl(rid, date_str=d, realized_delta_sats=100)
    recorder.upsert_daily_pnl(rid, date_str=d, realized_delta_sats=-50)
    fac = make_session_factory(recorder._factory().get_bind())
    with fac() as s:
        row = s.execute(select(daily_pnl).where(daily_pnl.c.run_id == rid)).first()
    assert row is not None
    assert row.realized_pnl_sats == 50


def test_net_daily_pnl_sats_spans_runs_and_includes_funding(recorder: Recorder) -> None:
    first = recorder.start_run(
        mode="live", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    second = recorder.start_run(
        mode="live", strategy_name="t", strategy_params={}, config={}, started_at=datetime.now(UTC)
    )
    recorder.upsert_daily_pnl(first, date_str="2026-01-01", realized_delta_sats=-100)
    recorder.upsert_daily_pnl(second, date_str="2026-01-01", funding_delta_sats=-7)

    assert recorder.net_daily_pnl_sats("2026-01-01") == -107
