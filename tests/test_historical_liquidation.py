import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from lnmarkets_bot.strategy.close_range import BreakoutCandidate, CloseRangeMachine


class _YieldingBars:
    def __init__(self, bars):
        self.bars = bars

    async def stream(self):
        for bar in self.bars:
            await asyncio.sleep(0)
            yield bar

    async def close(self):
        pass


def seeded(side="long"):
    m = CloseRangeMachine()
    m.seed_campaign(
        dict(
            parent_id="seed",
            side=side,
            entry_ts="2026-01-01T00:00:00+00:00",
            entry_price=100,
            boundary=90 if side == "long" else 110,
            held_days=10,
            peak_favorable_pct=0.1,
            active_units=4,
            lifetime_units=4,
            funding_complete_through="2026-01-10T00:00:00+00:00",
            units=[
                dict(
                    k=k,
                    entry_ts="2026-01-01T00:00:00+00:00",
                    entry_price=100 + k,
                    collateral_btc_per_contract=1 / (100 + k) / 5,
                )
                for k in range(4)
            ],
        )
    )
    return m


@pytest.mark.parametrize("side,low,high", [("long", 70, 100), ("short", 100, 140)])
def test_historical_parent_liquidation_clears_campaign_without_ownership(side, low, high):
    m = seeded(side)
    events = m.observe_historical_prices(datetime(2026, 1, 11, tzinfo=UTC), 100, low, high)
    assert m.campaign is None
    assert events[-1].kind == "historical_exit"
    assert all(not e.metadata["owned"] for e in events)


def test_child_liquidation_preserves_lifetime_and_restart():
    m = seeded()
    events = m.observe_historical_prices(datetime(2026, 1, 11, tzinfo=UTC), 100, 85, 105)
    assert events and all(e.k > 0 for e in events)
    assert m.campaign is not None and m.campaign.lifetime_units == 4
    restored = CloseRangeMachine.restore(m.persistent_state())
    assert [u.k for u in restored.campaign.units] == [u.k for u in m.campaign.units]
    assert restored.historical_model_complete


def test_funding_debits_margin_and_credits_do_not_change_it():
    m = seeded()
    ts = datetime(2026, 1, 11, tzinfo=UTC)
    before = m.campaign.units[0].collateral_btc_per_contract
    m.apply_historical_funding(ts, -0.01, 100)
    assert m.campaign.units[0].collateral_btc_per_contract == before
    m.apply_historical_funding(ts + timedelta(hours=8), 0.01, 100)
    assert m.campaign.units[0].collateral_btc_per_contract == pytest.approx(before - 0.0001)
    saved = m.persistent_state()
    m = CloseRangeMachine.restore(saved)
    m.apply_historical_funding(ts + timedelta(hours=8), 0.01, 100)
    assert m.persistent_state() == saved


def test_legacy_compact_seed_is_explicitly_incomplete():
    m = seeded()
    state = m.persistent_state()
    state.pop("historical_model_complete")
    state.pop("last_historical_funding_ts")
    for u in state["campaign"]["units"]:
        u.pop("collateral_btc_per_contract")
    m = CloseRangeMachine.restore(state)
    assert not m.historical_model_complete
    assert m.observe_historical_prices(datetime(2026, 1, 11, tzinfo=UTC), 100, 70, 100) == []


def test_incomplete_funding_cannot_certify_historical_seed():
    from lnmarkets_bot.strategy.historical import hydrate_historical

    m = seeded()
    m.last_bar_ts = datetime(2026, 1, 10, tzinfo=UTC)
    ref = dict(
        campaign_id="seed",
        boundary=90,
        entry_ts="2026-01-01T00:00:00+00:00",
        units=[
            dict(k=u.k, entry_ts=u.entry_ts.isoformat(), entry_price=u.entry_price)
            for u in m.campaign.units
        ],
    )
    with pytest.raises(ValueError, match="incomplete"):
        hydrate_historical(m, ref, [])


def test_exhausted_historical_collateral_liquidates_at_open_without_division_error():
    m = seeded()
    for unit in m.campaign.units:
        unit.collateral_btc_per_contract = -1 / unit.entry_price
    result = m.observe_historical_prices(datetime(2026, 1, 11, tzinfo=UTC), 100, 99, 101)
    assert len(result) == 4 and m.campaign is None


def test_legacy_funded_state_does_not_inherit_historical_admission_block():
    m = seeded()
    state = m.persistent_state()
    state.pop("historical_model_complete")
    state.pop("historical_funding_available")
    state["campaign"]["origin"] = "live"
    for unit in state["campaign"]["units"]:
        unit["origin"] = "live"
    restored = CloseRangeMachine.restore(state)
    assert restored.historical_model_complete and restored.historical_funding_available


@pytest.mark.asyncio
async def test_funding_gap_stays_pending_without_advancing_prices(cfg, recorder, monkeypatch):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    strategy = CloseRangeLive(machine=seeded())
    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    bars = [
        Bar(stamp, 100, 105, 70, 100, 1, timeframe="1m"),
        Bar(stamp + timedelta(minutes=1), 100, 105, 99, 100, 1, timeframe="1m"),
    ]
    calls = []

    async def unavailable(start, end):
        calls.append((start, end))
        return []

    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=unavailable,
    )
    assert len(calls) >= 2
    assert strategy.machine.historical_model_complete
    assert not strategy.machine.historical_funding_available
    saved = recorder.latest_strategy_state(mode="live", strategy_name="breakout")["state"][
        "machine"
    ]
    assert saved["historical_model_complete"] and not saved["historical_funding_available"]
    # A later favorable quote cannot certify survival of the missed adverse bar.
    assert strategy.machine.campaign is not None


@pytest.mark.asyncio
async def test_available_funding_advances_historical_model_on_first_bar(cfg, recorder):
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy = CloseRangeLive(machine=seeded())
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    rows = [(stamp - timedelta(hours=hours), 0.001, 100) for hours in (16, 8, 0)]

    async def available(start, end):
        return rows

    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars([Bar(stamp, 100, 105, 70, 100, 1)]),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=available,
    )
    assert strategy.machine.campaign is None
    assert strategy.machine.historical_funding_available


@pytest.mark.asyncio
async def test_daily_decision_waits_briefly_for_new_settlement(cfg, recorder, monkeypatch):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_DAILY_GRACE_SECONDS", 1.0)
    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_DAILY_RETRY_SECONDS", 0)
    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_INITIAL_TIMEOUT_SECONDS", 0.5)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy = CloseRangeLive(machine=seeded())
    strategy.machine.last_bar_ts = stamp - timedelta(days=2)
    strategy.machine.last_historical_funding_ts = stamp - timedelta(hours=8)
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    calls = 0

    async def arriving(start, end):
        nonlocal calls
        calls += 1
        return [] if calls == 1 else [(stamp, 0.001, 100)]

    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars([Bar(stamp, 100, 101, 99, 100, 1, timeframe="1d")]),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=arriving,
    )
    assert calls == 2
    assert strategy.machine.last_bar_ts == stamp - timedelta(days=1)
    assert strategy.machine.historical_funding_available


@pytest.mark.asyncio
@pytest.mark.parametrize("first_rows", [0, 1])
async def test_late_historical_funding_retries_before_observing_price(
    cfg, recorder, monkeypatch, first_rows
):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    strategy = CloseRangeLive(machine=seeded())
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    rows = [
        (stamp - timedelta(hours=hours), 0.02 if hours == 0 else 0.0, 100) for hours in (16, 8, 0)
    ]
    calls = []

    async def late(start, end):
        calls.append((start, end))
        return rows[:first_rows] if len(calls) == 1 else rows

    without_settlement = seeded()
    without_settlement.observe_historical_prices(stamp, 100, 84.5, 105)
    assert without_settlement.campaign is not None
    executor = _Executor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(
            [
                Bar(stamp, 100, 105, 84.5, 100, 1, timeframe="1m"),
                Bar(stamp + timedelta(minutes=1), 100, 105, 99, 100, 1, timeframe="1m"),
            ]
        ),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=late,
    )
    assert len(calls) == 2
    assert calls[0][1] == calls[1][1] == stamp
    assert strategy.machine.last_historical_funding_ts == stamp
    assert strategy.machine.historical_model_complete
    assert strategy.machine.historical_funding_available
    assert strategy.machine.campaign is None  # The adverse bar was actually observed.
    assert not executor.keys  # Historical liquidation never submits an order.


@pytest.mark.asyncio
async def test_longer_funding_delay_replays_every_missed_minute(cfg, recorder, monkeypatch):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy = CloseRangeLive(machine=seeded())
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    rows = [(stamp - timedelta(hours=hours), 0.001, 100) for hours in (16, 8, 0)]
    calls = []

    async def delayed(start, end):
        calls.append((start, end))
        return [] if len(calls) <= 2 else rows

    bars = [
        Bar(stamp + timedelta(minutes=i), 100, 105, 70 if i == 0 else 99, 100, 1) for i in range(4)
    ]
    executor = _Executor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=delayed,
    )
    assert len(calls) == 3
    assert strategy.machine.campaign is None
    assert strategy.machine.historical_model_complete
    assert strategy.machine.historical_funding_available
    assert executor.keys == []


@pytest.mark.asyncio
async def test_pending_funding_recovers_after_restart_from_replayed_bars(
    cfg, recorder, monkeypatch
):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    bars = [
        Bar(stamp + timedelta(minutes=i), 100, 105, 70 if i == 0 else 99, 100, 1) for i in range(3)
    ]
    first = CloseRangeLive(machine=seeded())
    first.machine.last_historical_price_ts = stamp - timedelta(minutes=1)

    async def unavailable(start, end):
        return []

    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(bars[:2]),
        bindings=(StrategyBinding("breakout", first),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=unavailable,
    )
    pending = recorder.latest_strategy_state(mode="live", strategy_name="breakout")["state"][
        "machine"
    ]
    assert pending["historical_model_complete"] and not pending["historical_funding_available"]

    rows = [(stamp - timedelta(hours=hours), 0.001, 100) for hours in (16, 8, 0)]

    async def available(start, end):
        return rows

    replayed = [
        Bar(bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume, warmup=i < 2)
        for i, bar in enumerate(bars)
    ]
    second = CloseRangeLive(machine=seeded())
    executor = _Executor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(replayed),
        bindings=(StrategyBinding("breakout", second),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=available,
    )
    assert second.machine.campaign is None
    assert second.machine.historical_model_complete
    assert second.machine.historical_funding_available
    assert executor.keys == []


def test_catchup_expires_daily_entry_instead_of_submitting_it():
    from lnmarkets_bot.engine.portfolio_live import _replay_historical_bars
    from lnmarkets_bot.strategy import Bar, StrategyState
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive

    day = datetime(2026, 1, 10, tzinfo=UTC)
    machine = seeded()
    machine.last_bar_ts = day - timedelta(days=1)
    machine.last_historical_funding_ts = day + timedelta(hours=8)
    machine.last_historical_price_ts = day + timedelta(hours=16) - timedelta(minutes=1)
    machine.pending_exit = "range_close"
    machine._candidate = lambda candle: BreakoutCandidate(
        signal_ts=candle.ts,
        side=1,
        boundary=90,
        signal_close=100,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    strategy = CloseRangeLive({"activation_ts": day.isoformat()}, machine=machine)
    bars = [Bar(day + timedelta(hours=16, minutes=i), 100, 101, 99, 100, 1) for i in range(8 * 60)]
    bars.append(Bar(day + timedelta(days=1), 100, 101, 99, 100, 1, timeframe="1d"))
    rows = [
        (day + timedelta(hours=16), 0.001, 100),
        (day + timedelta(days=1), 0.001, 100),
    ]
    _replay_historical_bars(strategy, StrategyState(), bars, rows)
    assert strategy.machine.campaign is None
    assert [event["kind"] for event in strategy._recent_decisions][-2:] == [
        "historical_exit",
        "paper_parent",
    ]


@pytest.mark.asyncio
async def test_missing_price_bar_prevents_automatic_recovery(cfg, recorder, monkeypatch):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy = CloseRangeLive(machine=seeded())
    strategy.machine.last_historical_price_ts = stamp - timedelta(minutes=1)
    rows = [(stamp - timedelta(hours=hours), 0.001, 100) for hours in (16, 8, 0)]
    calls = 0

    async def late(start, end):
        nonlocal calls
        calls += 1
        return [] if calls == 1 else rows

    bars = [
        Bar(stamp, 100, 105, 99, 100, 1),
        Bar(stamp + timedelta(minutes=2), 100, 105, 99, 100, 1),
    ]
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=late,
    )
    assert not strategy.machine.historical_model_complete
    assert not strategy.machine.historical_funding_available
    assert strategy.machine.campaign is not None


@pytest.mark.asyncio
async def test_funding_wait_does_not_block_other_strategy_orders(cfg, recorder):
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Entry, _Executor

    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    breakout = CloseRangeLive(machine=seeded())
    breakout.machine.last_historical_price_ts = stamp - timedelta(minutes=1)

    async def unavailable(start, end):
        return []

    executor = _Executor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars([Bar(stamp, 100, 101, 99, 100, 1)]),
        bindings=(StrategyBinding("ma", _Entry()), StrategyBinding("breakout", breakout)),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=unavailable,
    )
    assert executor.keys == ["ma:1m"]
    assert not breakout.machine.historical_funding_available


@pytest.mark.parametrize("side", ["long", "short"])
def test_parent_threshold_precedes_children_and_forces_survivors_at_that_price(side):
    m = seeded(side)
    m.campaign.units[0].collateral_btc_per_contract = 0.001
    events = m.observe_historical_prices(datetime(2026, 1, 11, tzinfo=UTC), 100, 70, 150)
    threshold = 1.001 / (0.001 + 1 / 100) if side == "long" else 0.999 / (1 / 100 - 0.001)
    assert len(events) == 4 and m.campaign is None
    assert [e.reason for e in events[:-1]] == ["parent_forced_exit"] * 3
    assert all(e.price == pytest.approx(threshold) for e in events)
    assert events[-1].metadata["surviving_units"] == [1, 2, 3]


def _reversal_at(stamp):
    """Historical campaign exits and a new structure parent opens on the same daily open."""
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive

    machine = seeded()
    machine.last_bar_ts = stamp - timedelta(days=2)
    machine.last_historical_funding_ts = stamp - timedelta(hours=8)
    machine.last_historical_price_ts = stamp - timedelta(minutes=2)
    machine.pending_exit = "range_close"
    machine._candidate = lambda candle: BreakoutCandidate(
        signal_ts=candle.ts,
        side=1,
        boundary=90,
        signal_close=100,
        ema20=100,
        atr14=2,
        average_overlap10=0.4,
        distance_ema_atr=3,
        structure_pass=True,
    )
    return CloseRangeLive(
        {"activation_ts": (stamp - timedelta(days=5)).isoformat()}, machine=machine
    )


async def _run_daily_publication_delay(cfg, recorder, monkeypatch, *, failed_calls, minutes):
    import lnmarkets_bot.engine.portfolio_live as portfolio_live
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from tests.test_portfolio_live import _Executor

    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_RETRY_SECONDS", 0)
    monkeypatch.setattr(portfolio_live, "_HISTORICAL_FUNDING_DAILY_GRACE_SECONDS", 0.01)
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
    strategy = _reversal_at(stamp)
    calls = []

    async def publishing(start, end):
        calls.append(end)
        return [] if len(calls) <= failed_calls else [(stamp, 0.001, 100)]

    bars = [
        Bar(stamp - timedelta(minutes=1), 100, 101, 99, 100, 1),
        Bar(stamp, 100, 101, 99, 100, 1, timeframe="1d"),
        *(Bar(stamp + timedelta(minutes=i), 100, 101, 99, 100, 1) for i in range(minutes)),
    ]
    executor = _Executor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_YieldingBars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(fixed_notional_strategy_ids=frozenset({"breakout"})),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=publishing,
    )
    return strategy, executor


@pytest.mark.asyncio
async def test_daily_decision_acts_when_settlement_publishes_minutes_late(
    cfg, recorder, monkeypatch
):
    """The routine 2-3 minute publication delay no longer drops a daily entry."""
    strategy, executor = await _run_daily_publication_delay(
        cfg, recorder, monkeypatch, failed_calls=2, minutes=6
    )
    assert strategy.machine.historical_funding_available
    assert executor.keys == ["breakout:k0"]
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.origin == "live"


@pytest.mark.asyncio
async def test_daily_decision_expires_when_settlement_is_later_than_window(
    cfg, recorder, monkeypatch
):
    strategy, executor = await _run_daily_publication_delay(
        cfg, recorder, monkeypatch, failed_calls=14, minutes=20
    )
    assert strategy.machine.historical_funding_available
    assert executor.keys == []
    assert strategy.machine.campaign is None
