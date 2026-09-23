from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from lnmarkets_bot.data.source import DataSource
from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import signals, strategy_state_snapshots
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.risk.guard import SizingPolicy
from lnmarkets_bot.strategy import Bar, OrderIntent, Strategy, StrategyState
from lnmarkets_bot.strategy.close_range_live import CloseRangeLive
from lnmarkets_bot.strategy.intents import SignalKind


class _OneBar(DataSource):
    def __init__(self, bar: Bar) -> None:
        self.bar = bar

    async def stream(self):
        yield self.bar


class _Bars(DataSource):
    def __init__(self, bars: list[Bar]) -> None:
        self.bars = bars

    async def stream(self):
        for bar in self.bars:
            yield bar


class _Entry(Strategy):
    tfs = ("1m",)

    def on_startup(self, state: StrategyState) -> None:
        pass

    def on_bar(self, bar: Bar, state: StrategyState):
        return [OrderIntent.enter_long("1m", 10, 2, reason="test")]

    def persistent_state(self):
        return {"saved": True}


class _Executor:
    def __init__(self) -> None:
        self.positions = {}
        self.keys = []
        self.run_id = -1

    def update_price(self, price):
        pass

    async def retry_pending_exits(self, **kwargs):
        return []

    async def sync_funding(self, ts):
        pass

    async def submit(self, *, intent, **kwargs):
        self.keys.append(intent.execution_key)
        self.positions[intent.execution_key] = SimpleNamespace(
            side="long", qty_sats=10, entry_price_usd=100, entry_ts=kwargs["ts"], leverage=2
        )
        return len(self.keys), {"price_usd": 100}

    def consume_realized_pnl_usd(self):
        return 0.0

    def position_side(self, key):
        return self.positions[key].side if key in self.positions else None

    def position_qty_sats(self, key):
        return self.positions[key].qty_sats if key in self.positions else 0

    def position_entry_price(self, key):
        return self.positions[key].entry_price_usd if key in self.positions else None

    def open_notional_usd(self, *, exclude_tf=None):
        return sum(
            abs(value.qty_sats) for key, value in self.positions.items() if key != exclude_tf
        )

    def open_margin_usd(self, *, exclude_tf=None):
        return sum(
            abs(value.qty_sats) / value.leverage
            for key, value in self.positions.items()
            if key != exclude_tf
        )


class _ReversalExecutor(_Executor):
    def __init__(self, ts: datetime) -> None:
        super().__init__()
        self.positions["breakout:k0"] = SimpleNamespace(
            side="short", qty_sats=-100, entry_price_usd=71_355.8, entry_ts=ts, leverage=5
        )
        self.actions: list[str] = []

    async def submit(self, *, intent, **kwargs):
        self.actions.append(intent.kind.value)
        self.keys.append(intent.execution_key)
        if intent.kind == SignalKind.EXIT:
            self.positions.pop(intent.execution_key)
        else:
            self.positions[intent.execution_key] = SimpleNamespace(
                side="long",
                qty_sats=100,
                entry_price_usd=78_301,
                entry_ts=kwargs["ts"],
                leverage=5,
            )
        return len(self.actions), {"price_usd": 78_301}


@pytest.mark.asyncio
async def test_portfolio_routes_same_timeframe_to_distinct_strategy_positions(cfg):
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    executor = _Executor()
    bar = Bar(
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        open=100,
        high=101,
        low=99,
        close=100,
        volume=1,
        timeframe="1m",
    )
    run_id = await run_portfolio_live(
        cfg=cfg,
        data_source=_OneBar(bar),
        bindings=(
            StrategyBinding("first", _Entry()),
            StrategyBinding("second", _Entry()),
        ),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert executor.keys == ["first:1m", "second:1m"]
    with factory() as session:
        rows = session.execute(
            select(signals.c.strategy_instance_id, signals.c.position_key)
            .where(signals.c.run_id == run_id)
            .order_by(signals.c.id)
        ).all()
        snapshots = (
            session.execute(
                select(strategy_state_snapshots.c.strategy_name).order_by(
                    strategy_state_snapshots.c.strategy_name
                )
            )
            .scalars()
            .all()
        )
    assert rows == [("first", "1m"), ("second", "1m")]
    # The two test instances intentionally share a class identity, so the
    # durable snapshot key is replaced rather than duplicated. The assertion
    # proves first-live-bar persistence occurs before a higher-TF boundary.
    assert snapshots == [f"{_Entry.__module__}.{_Entry.__name__}"]


@pytest.mark.asyncio
async def test_portfolio_closes_funded_campaign_before_deferred_reversal_entry(cfg):
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    recorder = Recorder(make_session_factory(engine))
    ts = datetime(2026, 8, 22, tzinfo=UTC)
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": ts.isoformat()}
    )
    strategy._closing_campaign_id = "20260602S"
    strategy._closing_slots = {"k0"}
    strategy._pending_reversal = {
        "ts": ts.isoformat(),
        "kind": "paper_parent",
        "reason": "structure_parent",
        "campaign_id": "20260822L",
        "k": 0,
        "side": 1,
        "price": 78_330.64575,
        "metadata": {
            "signal_ts": "2026-08-21T00:00:00+00:00",
            "boundary": 72_968.0,
        },
    }
    executor = _ReversalExecutor(ts)
    bars = [
        Bar(ts=ts, open=78_300, high=78_300, low=78_300, close=78_300, volume=1),
        Bar(
            ts=ts.replace(minute=1),
            open=78_301,
            high=78_301,
            low=78_301,
            close=78_301,
            volume=1,
        ),
    ]
    await run_portfolio_live(
        cfg=cfg,
        data_source=_Bars(bars),
        bindings=(StrategyBinding("breakout", strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert executor.actions == ["exit", "entry"]
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.origin == "live"
    assert strategy.machine.campaign.campaign_id == "20260822L"
