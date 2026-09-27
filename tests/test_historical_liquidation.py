from datetime import UTC, datetime, timedelta

import pytest

from lnmarkets_bot.strategy.close_range import CloseRangeMachine


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
async def test_funding_gap_requires_rebuild_and_durable_block(cfg, recorder):
    from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
    from lnmarkets_bot.risk.guard import SizingPolicy
    from lnmarkets_bot.strategy import Bar
    from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
    from tests.test_portfolio_live import _Bars, _Executor

    strategy = CloseRangeLive(machine=seeded())
    stamp = datetime(2026, 1, 11, tzinfo=UTC)
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
        data_source=_Bars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=_Executor(),
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
        historical_funding_provider=unavailable,
    )
    assert len(calls) == 1
    assert not strategy.machine.historical_model_complete
    saved = recorder.latest_strategy_state(mode="live", strategy_name="breakout")["state"][
        "machine"
    ]
    assert not saved["historical_model_complete"] and not saved["historical_funding_available"]
    # A later favorable quote cannot certify survival of the missed adverse bar.
    assert strategy.machine.campaign is not None


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
