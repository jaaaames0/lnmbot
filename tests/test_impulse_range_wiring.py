"""Configuration, CLI routing and portfolio-runner integration of the impulse-range strategy."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError
from scripts import run_live

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
from lnmarkets_bot.risk.guard import SizingPolicy
from lnmarkets_bot.strategy import Bar
from lnmarkets_bot.strategy.close_range import CloseRangeMachine
from lnmarkets_bot.strategy.impulse_range import H4, Channel, ImpulseRangeParams
from lnmarkets_bot.strategy.impulse_range_live import SLOT, ImpulseRangeLive, load_cold_machine
from lnmarkets_bot.strategy.intents import SignalKind
from tests.test_portfolio_live import _Bars, _Executor


def test_config_defaults_and_validation():
    cfg = BotConfig(_env_file=None)
    assert cfg.strategy_range_mode == "off"
    assert cfg.strategy_range_chop_filter and cfg.strategy_range_chop_threshold == 0.22
    for bad in (
        {"strategy_range_mode": "live"},
        {"strategy_range_leverage": 0},
        {"strategy_range_chop_threshold": 1.5},
        {"strategy_range_direction_mode": "sideways"},
    ):
        with pytest.raises(ValidationError):
            BotConfig(_env_file=None, **bad)


def test_cold_machine_requires_seed_through_day(tmp_path):
    days = pd.date_range("2025-01-01", periods=200, freq="D", tz="UTC")
    frame = pd.DataFrame(
        {"ts": days, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0}
    )
    path = tmp_path / "daily.parquet"
    frame.to_parquet(path)
    through = days[149].to_pydatetime()
    m = load_cold_machine(path, ImpulseRangeParams(), through_day=through)
    assert m.detector.last_ts == through and m.detector.count == 150
    assert m.pending_impulse is None and m.state == "idle"
    # A venue row whose open lies outside its quoted range is widened, not rejected.
    frame.loc[10, "open"] = 101.5
    frame.to_parquet(path)
    assert load_cold_machine(path, ImpulseRangeParams(), through_day=through).detector.count == 150
    with pytest.raises(ValueError, match="contiguous"):
        frame.drop(index=20).to_parquet(path)
        load_cold_machine(path, ImpulseRangeParams(), through_day=through)
    frame.to_parquet(path)
    with pytest.raises(ValueError, match="newer seed"):
        load_cold_machine(
            path, ImpulseRangeParams(), through_day=days[-1].to_pydatetime() + timedelta(days=1)
        )


def test_strict_data_check_includes_range_snapshot():
    snapshots = {
        "lnmarkets_bot.strategy.ma_cross.MaCross": {
            "ts": datetime(2026, 9, 24),
            "state": {"timeframes": {"1d": {"last_bar_ts": "2026-09-24T00:00:00+00:00"}}},
        },
        run_live.RANGE_INSTANCE_ID: {
            "ts": datetime(2026, 9, 23, 12),
            "state": {
                "machine": {
                    "bar_ts": "2026-09-23T08:00:00+00:00",
                    "detector": {"last_ts": "2026-09-22T00:00:00+00:00"},
                }
            },
        },
    }

    class Recorder:
        def latest_strategy_state(self, *, mode, strategy_name):
            return snapshots.get(strategy_name)

    rec = Recorder()
    assert run_live._strict_data_from(rec, include_breakout=False, include_range=True) == datetime(
        2026, 9, 23, tzinfo=UTC
    )
    del snapshots[run_live.RANGE_INSTANCE_ID]
    assert run_live._strict_data_from(rec, include_breakout=False, include_range=True) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "owned", "seed_ok", "expected"),
    [
        ("off", False, True, None),
        ("shadow", False, True, ("shadow", False)),
        ("funded", False, True, ("funded", True)),
        ("off", True, True, ("funded", False)),
        ("funded", False, False, None),
        ("funded", True, False, ("funded", True)),
    ],
)
async def test_cli_binds_range_strategy(cfg, monkeypatch, mode, owned, seed_ok, expected):
    config = cfg.model_copy(
        update={
            "strategy_range_mode": mode,
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
            self.positions = (
                {f"{run_live.RANGE_INSTANCE_ID}:{SLOT}": SimpleNamespace(qty_sats=100)}
                if owned
                else {}
            )

        async def reconcile(self):
            pass

    def cold(path, params, *, through_day):
        if not seed_ok:
            raise ValueError("seed too old")
        return run_live.ImpulseRangeLive({}).range_machine

    monkeypatch.setattr(run_live, "LnmRestClient", Client)
    monkeypatch.setattr(run_live, "IsolatedTradesApi", lambda client: object())
    monkeypatch.setattr(run_live, "LiveExecutor", Executor)
    monkeypatch.setattr(run_live, "LiveAccountBalanceProvider", lambda **kwargs: object())
    monkeypatch.setattr(run_live, "load_seed_machine", lambda *args: CloseRangeMachine())
    monkeypatch.setattr(run_live, "load_cold_machine", cold)
    observed = {}

    async def portfolio(**kwargs):
        observed.update(kwargs)
        return 1

    monkeypatch.setattr(run_live, "run_portfolio_live", portfolio)
    assert await run_live.main() == 0
    ranges = [b for b in observed["bindings"] if b.instance_id == run_live.RANGE_INSTANCE_ID]
    if expected is None:
        assert ranges == []
    else:
        (binding,) = ranges
        assert (binding.strategy.mode, binding.strategy.entries_enabled) == expected
        assert binding.strategy.model_complete is seed_ok
    assert run_live.RANGE_INSTANCE_ID in observed["sizing_policy"].fixed_notional_strategy_ids


def test_cli_refuses_range_outside_funded_process(cfg, monkeypatch):
    config = cfg.model_copy(update={"strategy_range_mode": "shadow"})
    monkeypatch.setattr(run_live, "BotConfig", lambda **kwargs: config)
    monkeypatch.setattr(sys, "argv", ["run_live", "--env", "/nonexistent"])
    with pytest.raises(SystemExit):
        import asyncio

        asyncio.run(run_live.main())


class _RangeExecutor(_Executor):
    async def submit(self, *, intent, **kwargs):
        self.keys.append((intent.kind.value, intent.execution_key, intent.reason))
        if intent.kind == SignalKind.EXIT:
            self.positions.pop(intent.execution_key, None)
            return len(self.keys), {"price_usd": 100.0}
        self.positions[intent.execution_key] = SimpleNamespace(
            side=intent.side.value,
            qty_sats=int(intent.size_usd) * (1 if intent.side.value == "long" else -1),
            entry_price_usd=93.0,
            entry_ts=kwargs["ts"],
            leverage=intent.leverage,
        )
        return len(self.keys), {"price_usd": 93.0}


@pytest.mark.asyncio
async def test_portfolio_runner_routes_range_entry_and_target_exit(cfg, recorder):
    t0 = datetime(2026, 9, 1, tzinfo=UTC)
    strategy = ImpulseRangeLive({"mode": "funded", "unit_notional_usd": 50, "chop_filter": False})
    m = strategy.range_machine
    m.state = "active"
    m.channel = Channel(
        id=0,
        side=1,
        impulse_ts=t0 - H4,
        confirmed_ts=t0,
        lo=90.0,
        hi=110.0,
        first_lo=90.0,
        first_hi=110.0,
        er_checked=True,
    )
    m.bar_ts = t0
    minute = lambda i, px: Bar(t0 + timedelta(minutes=i), px, px, px, px, 1.0, "1m")  # noqa: E731
    executor = _RangeExecutor()
    await run_portfolio_live(
        cfg=cfg,
        data_source=_Bars([minute(0, 95), minute(1, 92.5), minute(2, 96), minute(3, 100.5)]),
        bindings=(StrategyBinding(run_live.RANGE_INSTANCE_ID, strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(
            fixed_notional_strategy_ids=frozenset({run_live.RANGE_INSTANCE_ID})
        ),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    key = f"{run_live.RANGE_INSTANCE_ID}:{SLOT}"
    assert executor.keys == [("entry", key, "range_edge"), ("exit", key, "target")]
    assert strategy.range_machine.position is None and not strategy._closing
    saved = recorder.latest_strategy_state(mode="live", strategy_name=run_live.RANGE_INSTANCE_ID)
    assert saved is not None and saved["state"]["machine"]["channel"]["id"] == 0
