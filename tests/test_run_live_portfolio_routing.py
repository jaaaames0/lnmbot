"""The funded CLI must retain namespaced ownership when breakout is disabled."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from scripts import run_live

from lnmarkets_bot.strategy.close_range import CloseRangeMachine


def test_strict_data_check_starts_at_oldest_uncommitted_strategy_day():
    snapshots = {
        "lnmarkets_bot.strategy.ma_cross.MaCross": {
            "ts": datetime(2026, 9, 24, 0, 0),
            "state": {
                "timeframes": {
                    "1d": {"last_bar_ts": "2026-09-24T00:00:00+00:00"},
                    "4h": {"last_bar_ts": "2026-09-24T00:00:00+00:00"},
                }
            },
        },
        "lnmarkets_bot.strategy.close_range_live.CloseRangeLive": {
            "ts": datetime(2026, 9, 23, 12, 55),
            "state": {"machine": {"last_bar_ts": "2026-09-22T00:00:00+00:00"}},
        },
    }

    class Recorder:
        def latest_strategy_state(self, *, mode, strategy_name):
            assert mode == "live"
            return snapshots.get(strategy_name)

    recorder = Recorder()
    assert run_live._strict_data_from(recorder, include_breakout=True) == datetime(
        2026, 9, 23, tzinfo=UTC
    )
    assert run_live._strict_data_from(recorder, include_breakout=False) == datetime(
        2026, 9, 24, tzinfo=UTC
    )
    snapshots["lnmarkets_bot.strategy.ma_cross.MaCross"]["state"]["timeframes"]["1d"][
        "last_bar_ts"
    ] = "2026-09-22T00:00:00+00:00"
    assert run_live._strict_data_from(recorder, include_breakout=False) == datetime(
        2026, 9, 22, tzinfo=UTC
    )
    del snapshots["lnmarkets_bot.strategy.close_range_live.CloseRangeLive"]
    assert run_live._strict_data_from(recorder, include_breakout=True) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owned_key", "expected_ids"),
    [
        ("ma_cross_primary:4h", ["ma_cross_primary"]),
        ("btc_close_range_v1:k0", ["ma_cross_primary", "btc_close_range_v1"]),
    ],
)
async def test_disabled_breakout_keeps_funded_portfolio_routing(
    cfg, monkeypatch, owned_key, expected_ids
):
    config = cfg.model_copy(
        update={
            "strategy_breakout_enabled": False,
            "lnm_access_key": "test",
            "lnm_access_secret": "test",
            "lnm_access_passphrase": "test",
        }
    )
    monkeypatch.setattr(run_live, "BotConfig", lambda **kwargs: config)
    monkeypatch.setattr(run_live, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(sys, "argv", ["run_live", "--allow-orders", "--env", "/nonexistent"])

    class Client:
        def __init__(self, **kwargs):
            pass

        async def aclose(self):
            pass

    class Executor:
        def __init__(self, **kwargs):
            self.positions = {owned_key: SimpleNamespace(qty_sats=100)}

        async def reconcile(self):
            pass

    monkeypatch.setattr(run_live, "LnmRestClient", Client)
    monkeypatch.setattr(run_live, "IsolatedTradesApi", lambda client: object())
    monkeypatch.setattr(run_live, "LiveExecutor", Executor)
    monkeypatch.setattr(run_live, "LiveAccountBalanceProvider", lambda **kwargs: object())
    monkeypatch.setattr(run_live, "load_seed_machine", lambda *args: CloseRangeMachine())

    observed = {}

    async def portfolio(**kwargs):
        observed["bindings"] = kwargs["bindings"]
        return 123

    async def obsolete_loop(**kwargs):
        raise AssertionError("funded trader used the single-strategy loop")

    monkeypatch.setattr(run_live, "run_portfolio_live", portfolio)
    monkeypatch.setattr(run_live, "run_paper", obsolete_loop)
    assert await run_live.main() == 0
    assert [binding.instance_id for binding in observed["bindings"]] == expected_ids
    if len(expected_ids) > 1:
        assert not observed["bindings"][1].strategy.entries_enabled
