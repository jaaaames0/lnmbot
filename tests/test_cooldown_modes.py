"""Cooldown-slot semantics must remain explicit and independently testable."""

from __future__ import annotations

from datetime import UTC, datetime

from lnmarkets_bot.strategy.base import Bar, StrategyState, TfPosition
from lnmarkets_bot.strategy.ma_cross import MaCross


def test_cooldown_modes_treat_flat_and_order_opportunities_differently() -> None:
    state = StrategyState(positions={"4h": TfPosition(side="long")})

    verdict_mode = MaCross(params={"cooldown_mode": "verdict_transition"})
    directional_mode = MaCross(params={"cooldown_mode": "directional_transition"})
    opportunity_mode = MaCross(params={"cooldown_mode": "order_opportunity"})

    # The legacy mode spends a slot on every verdict transition; the intended
    # directional mode does not spend one on FLAT.
    assert verdict_mode._cooldown_consumes(tf="4h", verdict="FLAT", state=state)
    assert not directional_mode._cooldown_consumes(tf="4h", verdict="FLAT", state=state)
    assert not opportunity_mode._cooldown_consumes(tf="4h", verdict="FLAT", state=state)

    # UP_TRUE agrees with the existing long and creates no order opportunity.
    assert directional_mode._cooldown_consumes(tf="4h", verdict="UP_TRUE", state=state)
    assert not opportunity_mode._cooldown_consumes(tf="4h", verdict="UP_TRUE", state=state)

    # DOWN_TRUE is directional and would close/flip the current long.
    assert opportunity_mode._cooldown_consumes(tf="4h", verdict="DOWN_TRUE", state=state)


def test_loss_cooldown_is_independent_and_can_be_disabled() -> None:
    locked = MaCross()
    triggered, types = locked._start_cooldowns("1d", -0.06)
    assert triggered
    assert types == ["loss"]
    assert locked._loss_suppressed_signals["1d"] == 3

    disabled = MaCross(
        params={
            "loss_cooldown_threshold_pct": {"1d": 0.0, "4h": 0.0},
            "loss_cooldown_signal_count": {"1d": 0, "4h": 0},
        }
    )
    assert disabled._start_cooldowns("1d", -0.10) == (False, [])

    strategy = MaCross(
        params={
            "loss_cooldown_threshold_pct": {"1d": 0.03, "4h": 0.05},
            "loss_cooldown_signal_count": {"1d": 2, "4h": 4},
        }
    )
    triggered, types = strategy._start_cooldowns("1d", -0.04)
    assert triggered
    assert types == ["loss"]
    assert strategy._suppressed_signals["1d"] == 0
    assert strategy._loss_suppressed_signals["1d"] == 2


def test_cooldown_records_the_suppressed_same_bar_flip() -> None:
    strategy = MaCross(
        params={
            "tfs": ("5m",),
            "cooldown_threshold_pct": {"5m": 1.0},
            "cooldown_signal_count": {"5m": 0},
            "loss_cooldown_threshold_pct": {"5m": 0.001},
            "loss_cooldown_signal_count": {"5m": 2},
        }
    )
    state = StrategyState(positions={"5m": TfPosition(side="long", entry_price_usd=100.0)})
    bar = Bar(
        ts=datetime.now(UTC),
        open=99.0,
        high=100.0,
        low=98.0,
        close=99.0,
        volume=1.0,
        timeframe="5m",
    )

    intents = strategy._on_transition(
        tf="5m",
        previous_verdict="UP_TRUE",
        side="DOWN_TRUE",
        bar=bar,
        state=state,
    )

    assert [intent.kind.value for intent in intents] == ["exit", "noop"]
    assert intents[1].reason == "cool_off_same_bar_flip"
    assert intents[1].metadata["suppressed_action"] == "enter_short"


def test_restart_catch_up_closes_only_a_restored_opposite_position() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    state = StrategyState(positions={"5m": TfPosition(side="long", entry_price_usd=100.0)})
    strategy.on_startup(state)
    bar = Bar(ts=datetime.now(UTC), open=99, high=100, low=89, close=90, volume=1, timeframe="5m")
    intents = strategy._restart_catch_up(tf="5m", verdict="DOWN_TRUE", bar=bar, state=state)
    assert [intent.kind.value for intent in intents] == ["exit"]
    assert intents[0].reason == "restart_catch_up closes long against DOWN_TRUE"
    assert state.position("5m").side is None


def test_startup_marks_a_restored_position_for_catch_up() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    state = StrategyState(positions={"5m": TfPosition(side="short")})

    strategy.on_startup(state)

    assert strategy._restart_pending == {"5m"}


def test_4h_high_chop_reduces_new_entry_notional_only() -> None:
    strategy = MaCross(
        params={
            "base_size_usd": 100.0,
            "size_multipliers": {"1d": 1.0, "4h": 1.0},
            "chop_4h_reduce_enabled": True,
            "chop_lookback": 14,
            "chop_high_threshold": 61.8,
            "chop_high_size_multiplier": 0.5,
        }
    )
    state = StrategyState(positions={"4h": TfPosition(), "1d": TfPosition()})
    strategy.tf_state["4h"].chop = 62.0

    intents = strategy._on_transition(
        tf="4h",
        previous_verdict="FLAT",
        side="UP_TRUE",
        bar=Bar(
            ts=datetime.now(UTC),
            open=100.0,
            high=101.0,
            low=99.0,
            close=101.0,
            volume=1.0,
            timeframe="4h",
        ),
        state=state,
    )

    assert len(intents) == 1
    assert intents[0].size_usd == 50.0
    assert intents[0].leverage == strategy.base_leverage
    assert intents[0].metadata["chop_regime"] == "high_chop"
    assert intents[0].metadata["chop_value"] == 62.0

    strategy.tf_state["1d"].chop = 90.0
    size, metadata = strategy._entry_size_and_metadata("1d")
    assert size == 100.0
    assert metadata["entry_size_multiplier"] == 1.0
    assert metadata["chop_regime"] == "not_applicable"
