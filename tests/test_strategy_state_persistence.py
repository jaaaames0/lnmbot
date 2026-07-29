"""Durable indicator-state tests for restart-continuous live strategies."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lnmarkets_bot.strategy.base import Bar, StrategyState, TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross

if TYPE_CHECKING:
    from lnmarkets_bot.persistence.recorder import Recorder


def test_ma_cross_restores_ema_and_skips_an_overlapping_warmup_bar() -> None:
    original = MaCross(params={"tfs": ("1d",)})
    item = original.tf_state["1d"]
    item.closes = deque([float(value) for value in range(1, 65)], maxlen=64)
    item.ema = 123.45
    item.ema_seeded = True
    item.verdict = "UP_TRUE"
    item.last_bar_ts = datetime(2026, 7, 28, tzinfo=UTC)
    original._suppressed_signals["1d"] = 2
    original._pending_position_reconciliation["1d"] = "long"

    restored = MaCross(params={"tfs": ("1d",)})
    assert restored.restore_persistent_state(original.persistent_state() or {})

    replayed = Bar(
        ts=item.last_bar_ts,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1.0,
        timeframe="1d",
        warmup=True,
    )
    assert restored.on_bar(replayed, state=StrategyState()) == []
    assert restored.tf_state["1d"].ema == 123.45
    assert list(restored.tf_state["1d"].closes) == list(item.closes)
    assert restored._suppressed_signals["1d"] == 2
    assert restored._pending_position_reconciliation["1d"] == "long"

    restored.reconcile_execution_state(StrategyState(positions={"1d": TfPosition(side="long")}))
    assert restored._pending_position_reconciliation["1d"] is None


def test_recorder_replaces_one_live_snapshot_per_strategy(recorder: Recorder) -> None:
    first_run = recorder.start_run(
        mode="live",
        strategy_name="example.MaCross",
        strategy_params={},
        config={},
        started_at=datetime(2026, 7, 28, tzinfo=UTC),
    )
    recorder.save_strategy_state(
        first_run,
        mode="live",
        strategy_name="example.MaCross",
        ts=datetime(2026, 7, 28, tzinfo=UTC),
        state={"version": 1, "value": "old"},
    )
    second_run = recorder.start_run(
        mode="live",
        strategy_name="example.MaCross",
        strategy_params={},
        config={},
        started_at=datetime(2026, 7, 28, 4, tzinfo=UTC),
    )
    recorder.save_strategy_state(
        second_run,
        mode="live",
        strategy_name="example.MaCross",
        ts=datetime(2026, 7, 28, 4, tzinfo=UTC),
        state={"version": 1, "value": "new"},
    )

    snapshot = recorder.latest_strategy_state(mode="live", strategy_name="example.MaCross")

    assert snapshot is not None
    assert snapshot["state"] == {"version": 1, "value": "new"}
    assert snapshot["ts"].replace(tzinfo=UTC) == datetime(2026, 7, 28, 4, tzinfo=UTC)


def test_ma_cross_snapshot_restores_after_a_database_json_round_trip(
    recorder: Recorder,
) -> None:
    strategy_name = "lnmarkets_bot.strategy.ma_cross.MaCross"
    original = MaCross()
    original.tf_state["4h"].ema = 63_936.08
    original.tf_state["4h"].ema_seeded = True
    original.tf_state["4h"].verdict = "DOWN_TRUE"
    original._loss_suppressed_signals["4h"] = 4
    run_id = recorder.start_run(
        mode="live",
        strategy_name=strategy_name,
        strategy_params=original.params,
        config={},
        started_at=datetime(2026, 7, 28, tzinfo=UTC),
    )
    recorder.save_strategy_state(
        run_id,
        mode="live",
        strategy_name=strategy_name,
        ts=datetime(2026, 7, 28, 4, tzinfo=UTC),
        state=original.persistent_state() or {},
    )

    loaded = recorder.latest_strategy_state(mode="live", strategy_name=strategy_name)
    restored = MaCross()

    assert loaded is not None
    assert restored.restore_persistent_state(loaded["state"])
    assert restored.tf_state["4h"].ema == 63_936.08
    assert restored.tf_state["4h"].verdict == "DOWN_TRUE"
    assert restored._loss_suppressed_signals["4h"] == 4


def test_live_stream_defaults_to_a_100_day_ema_bootstrap() -> None:
    from lnmarkets_bot.data.live import LnmLiveStream

    assert LnmLiveStream(object()).warmup_days == 100
