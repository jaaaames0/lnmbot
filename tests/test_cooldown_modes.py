"""Cooldown-slot semantics must remain explicit and independently testable."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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


def test_restart_catch_up_applies_the_normal_same_bar_flip() -> None:
    strategy = MaCross(
        params={
            "tfs": ("5m",),
            "cooldown_threshold_pct": {"5m": 1.0},
            "cooldown_signal_count": {"5m": 0},
            "loss_cooldown_threshold_pct": {"5m": 1.0},
            "loss_cooldown_signal_count": {"5m": 0},
        }
    )
    state = StrategyState(positions={"5m": TfPosition(side="long", entry_price_usd=100.0)})
    strategy.on_startup(state)
    bar = Bar(ts=datetime.now(UTC), open=99, high=100, low=89, close=90, volume=1, timeframe="5m")
    intents = strategy._restart_catch_up(tf="5m", verdict="DOWN_TRUE", bar=bar, state=state)
    assert [intent.kind.value for intent in intents] == ["exit", "entry"]
    assert intents[0].metadata["restart_catch_up"] is True
    assert intents[1].metadata["restart_catch_up"] is True
    assert intents[1].side.value == "short"
    assert state.position("5m").side == "short"


def test_restart_catch_up_is_silent_when_aligned() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    strategy.tf_state["5m"].sma = 100.0
    strategy.tf_state["5m"].ema = 101.0
    state = StrategyState(positions={"5m": TfPosition(side="long")})
    bar = Bar(ts=datetime.now(UTC), open=100, high=101, low=99, close=100, volume=1, timeframe="5m")

    assert strategy._restart_catch_up(tf="5m", verdict="FLAT", bar=bar, state=state) == []
    assert state.position("5m").side == "long"


def test_unchanged_directional_verdict_reconciles_a_stranded_position() -> None:
    strategy = MaCross(
        params={
            "tfs": ("5m",),
            "cooldown_threshold_pct": {"5m": 1.0},
            "cooldown_signal_count": {"5m": 0},
            "loss_cooldown_threshold_pct": {"5m": 1.0},
            "loss_cooldown_signal_count": {"5m": 0},
        }
    )
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "DOWN_TRUE"
    strategy._pending_position_reconciliation["5m"] = "short"
    state = StrategyState(positions={"5m": TfPosition(side="long", entry_price_usd=100.0)})
    bar = Bar(ts=datetime.now(UTC), open=91, high=92, low=89, close=90, volume=1, timeframe="5m")

    intents = strategy.on_bar(bar, state)

    assert [intent.kind.value for intent in intents] == ["exit", "entry"]
    assert intents[1].side.value == "short"
    assert state.position("5m").side == "short"


def test_unchanged_directional_verdict_does_not_create_a_cold_start_entry() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "DOWN_TRUE"
    state = StrategyState(positions={"5m": TfPosition()})
    bar = Bar(
        ts=datetime.now(UTC),
        open=91,
        high=92,
        low=89,
        close=90,
        volume=1,
        timeframe="5m",
    )

    assert strategy.on_bar(bar, state) == []
    assert state.position("5m").side is None


def test_active_cooldown_blocks_unchanged_verdict_reconciliation_without_consuming_slot() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "DOWN_TRUE"
    strategy._loss_suppressed_signals["5m"] = 4
    strategy._pending_position_reconciliation["5m"] = "short"
    state = StrategyState(positions={"5m": TfPosition()})
    bar = Bar(ts=datetime.now(UTC), open=91, high=92, low=89, close=90, volume=1, timeframe="5m")

    intents = strategy.on_bar(bar, state)

    assert [intent.kind.value for intent in intents] == ["noop"]
    assert intents[0].reason == "cool_off_pending_position_reconciliation"
    assert intents[0].metadata["loss_remaining"] == 4
    assert strategy._loss_suppressed_signals["5m"] == 4


def test_restart_cooldown_closes_contrary_exposure_without_reentering() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "DOWN_TRUE"
    strategy._loss_suppressed_signals["5m"] = 4
    state = StrategyState(
        positions={"5m": TfPosition(side="long", qty_sats=10, entry_price_usd=100.0)}
    )
    strategy.on_startup(state)
    bar = Bar(
        ts=datetime.now(UTC),
        open=91,
        high=92,
        low=89,
        close=90,
        volume=1,
        timeframe="5m",
    )

    intents = strategy.on_bar(bar, state)

    assert [intent.kind.value for intent in intents] == ["exit"]
    assert intents[0].reason == "5m cool-off reconciliation closes long"
    assert intents[0].metadata["restart_catch_up"] is True
    assert intents[0].metadata["suppressed_replacement"] == "short"
    assert state.position("5m").side is None
    assert strategy._loss_suppressed_signals["5m"] == 4


def test_restart_transition_spends_cooldown_slot_and_does_not_flip() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "UP_TRUE"
    strategy._loss_suppressed_signals["5m"] = 4
    state = StrategyState(
        positions={"5m": TfPosition(side="long", qty_sats=10, entry_price_usd=100.0)}
    )
    strategy.on_startup(state)
    bar = Bar(
        ts=datetime.now(UTC),
        open=91,
        high=92,
        low=89,
        close=90,
        volume=1,
        timeframe="5m",
    )

    intents = strategy.on_bar(bar, state)

    assert [intent.kind.value for intent in intents] == ["exit", "noop"]
    assert intents[1].reason == "cool_off"
    assert intents[1].metadata["restart_catch_up"] is True
    assert strategy._loss_suppressed_signals["5m"] == 3


def test_cooloff_signal_records_total_for_auditable_ordinal() -> None:
    strategy = MaCross(
        params={"tfs": ("5m",), "cooldown_signal_count": {"5m": 12}}
    )
    strategy._suppressed_signals["5m"] = 11

    intent = strategy._consume_cooldown(
        tf="5m", previous_verdict="UP_TRUE", verdict="FLAT",
        cooldowns_before={"winner": 11},
    )[0]

    assert intent.metadata["winner_total"] == 12
    assert intent.metadata["winner_remaining_before"] == 11
    assert intent.metadata["winner_remaining_after"] == 10
    assert intent.metadata["previous_verdict"] == "UP_TRUE"
    assert intent.metadata["verdict"] == "FLAT"


def test_manual_flat_hold_blocks_only_the_missed_direction_until_verdict_changes() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    tf_state = strategy.tf_state["5m"]
    tf_state.closes.extend([100.0] * 21)
    tf_state.ema = 100.0
    tf_state.ema_seeded = True
    tf_state.verdict = "DOWN_TRUE"
    strategy._manual_flat_hold["5m"] = "DOWN_TRUE"
    state = StrategyState(positions={"5m": TfPosition()})
    down_bar = Bar(
        ts=datetime.now(UTC), open=91, high=92, low=89, close=90, volume=1, timeframe="5m"
    )

    held = strategy.on_bar(down_bar, state)

    assert held[0].reason == "manual_flat_hold"
    assert state.position("5m").side is None

    flat_bar = Bar(
        ts=down_bar.ts + timedelta(minutes=5),
        open=99,
        high=100,
        low=98,
        close=99.8,
        volume=1,
        timeframe="5m",
    )
    strategy.on_bar(flat_bar, state)

    assert strategy._manual_flat_hold["5m"] is None


def test_startup_marks_a_restored_position_for_catch_up() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    state = StrategyState(positions={"5m": TfPosition(side="short")})

    strategy.on_startup(state)

    assert strategy._restart_pending == {"5m"}


def test_startup_reconciles_restored_position_on_first_live_minute() -> None:
    strategy = MaCross(params={"tfs": ("5m",)})
    indicator = strategy.tf_state["5m"]
    indicator.closes.extend([100.0] * 21)
    indicator.highs.extend([101.0] * 21)
    indicator.lows.extend([99.0] * 21)
    indicator.ema = 100.0
    indicator.ema_seeded = True
    indicator.verdict = "DOWN_TRUE"
    indicator.last_bar_ts = datetime(2026, 7, 28, 4, tzinfo=UTC)
    state = StrategyState(
        positions={"5m": TfPosition(side="short", qty_sats=-10, entry_price_usd=100.0)}
    )
    strategy.on_startup(state)

    intents = strategy.on_bar(
        Bar(
            ts=datetime(2026, 7, 28, 4, 1, tzinfo=UTC),
            open=99.0,
            high=100.0,
            low=98.0,
            close=99.0,
            volume=1.0,
            timeframe="1m",
        ),
        state,
    )

    assert intents == []
    assert strategy._restart_pending == set()


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


def test_4h_short_entry_carries_chop_multiplier_for_equity_sizing() -> None:
    strategy = MaCross(
        params={
            "base_size_usd": 100.0,
            "chop_4h_reduce_enabled": True,
            "chop_high_size_multiplier": 0.5,
        }
    )
    strategy.tf_state["4h"].chop = 70.0

    intent = strategy._on_transition(
        tf="4h",
        previous_verdict="FLAT",
        side="DOWN_TRUE",
        bar=Bar(
            ts=datetime.now(UTC),
            open=100.0,
            high=101.0,
            low=98.0,
            close=99.0,
            volume=1.0,
            timeframe="4h",
        ),
        state=StrategyState(positions={"4h": TfPosition()}),
    )[0]

    assert intent.side.value == "short"
    assert intent.size_usd == 50.0
    assert intent.metadata["entry_size_multiplier"] == 0.5
    assert intent.metadata["chop_regime"] == "high_chop"
