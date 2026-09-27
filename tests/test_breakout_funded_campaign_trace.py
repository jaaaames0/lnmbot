"""End-to-end, offline traces of a four-unit funded breakout campaign.

The August 2026 LN Markets candles provide the entry oracle. Future candles and
venue outcomes are deliberately synthetic so each terminal path is exact and
independent of whatever the market subsequently did.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import select

from lnmarkets_bot.api.isolated import IsolatedCloseResponse, IsolatedTrade
from lnmarkets_bot.data.source import DataSource
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import orders, signals, strategy_pnl_events
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.risk.guard import SizingPolicy
from lnmarkets_bot.strategy import Bar
from lnmarkets_bot.strategy.close_range import CloseRangeMachine, DailyCandle
from lnmarkets_bot.strategy.close_range_live import CloseRangeLive

CACHE = (
    Path(__file__).resolve().parents[1]
    / "config/seeds/lnmarkets_btc_1d_2019-09-09_2026-09-13.parquet"
)
INSTANCE = "btc_close_range_v1"
PARENT_TS = datetime(2026, 8, 22, tzinfo=UTC)
ENTRY_DATES = (22, 25, 27, 28)
ENTRY_PRICES = (78290.5, 78937.5, 78983.0, 80222.0)
BOUNDARY = 72968.0


class _TraceVenue:
    def __init__(self) -> None:
        self.mark = 0.0
        self.running: dict[str, IsolatedTrade] = {}
        self.closed: dict[str, IsolatedTrade] = {}
        self.opens: list[tuple[str, str, int, float, float]] = []
        self.closes: list[str] = []

    async def new_trade(self, params):
        trade_id = f"trace-{len(self.opens)}"
        trade = IsolatedTrade(
            id=trade_id,
            type=params.type,
            side=params.side,
            quantity=params.quantity,
            leverage=params.leverage,
            price=self.mark,
            entry_price=self.mark,
            opening_fee=7,
        )
        self.running[trade_id] = trade
        self.opens.append((trade_id, params.side, params.quantity, params.leverage, self.mark))
        return trade

    @staticmethod
    def _gross(trade: IsolatedTrade, exit_price: float) -> int:
        direction = 1 if trade.side == "buy" else -1
        return round(direction * trade.quantity * (1 / trade.entry_price - 1 / exit_price) * 1e8)

    async def close_trade(self, trade_id):
        trade = self.running.pop(trade_id)
        self.closes.append(trade_id)
        gross = self._gross(trade, self.mark)
        self.closed[trade_id] = replace(
            trade,
            status="closed",
            price=self.mark,
            pl=gross,
            closing_fee=11,
            raw={"exitPrice": self.mark},
        )
        return IsolatedCloseResponse(
            id=trade_id, pl=gross, closing_fee=11, raw={"exitPrice": self.mark}
        )

    def liquidate(self, trade_id: str, exit_price: float) -> None:
        trade = self.running.pop(trade_id)
        self.closed[trade_id] = replace(
            trade,
            status="closed",
            price=exit_price,
            pl=self._gross(trade, exit_price),
            closing_fee=0,
            raw={"closeReason": "liquidation", "exitPrice": exit_price},
        )

    async def get_running_trades(self):
        return list(self.running.values())

    async def get_closed_trades(self):
        return list(self.closed.values())

    async def iter_funding_fees(self, from_ts, to_ts):
        if False:
            yield {}


class _TraceBars(DataSource):
    def __init__(self, venue: _TraceVenue, bars: list[Bar]) -> None:
        self.venue = venue
        self.bars = bars

    async def stream(self):
        for bar in self.bars:
            self.venue.mark = bar.close
            yield bar


def _historical_entry_bars(
    warmup_through: str, first_signal: str, last_signal: str
) -> tuple[CloseRangeMachine, list[Bar]]:
    frame = pd.read_parquet(CACHE).sort_values("ts")
    frame["ts"] = pd.to_datetime(frame.ts, utc=True)
    frame["high"] = frame[["open", "high", "close"]].max(axis=1)
    frame["low"] = frame[["open", "low", "close"]].min(axis=1)
    machine = CloseRangeMachine()
    history = frame[frame.ts <= pd.Timestamp(warmup_through, tz="UTC")]
    machine.warmup(
        [
            DailyCandle(row.ts.to_pydatetime(), row.open, row.high, row.low, row.close)
            for row in history.itertuples(index=False)
        ]
    )
    opening = frame[
        (frame.ts >= pd.Timestamp(first_signal, tz="UTC"))
        & (frame.ts <= pd.Timestamp(last_signal, tz="UTC"))
    ]
    bars = [
        Bar(
            row.ts.to_pydatetime() + timedelta(days=1),
            row.open,
            row.high,
            row.low,
            row.close,
            1.0,
            timeframe="1d",
        )
        for row in opening.itertuples(index=False)
    ]
    return machine, bars


def _entry_bars() -> tuple[CloseRangeMachine, list[Bar]]:
    return _historical_entry_bars("2026-08-20", "2026-08-21", "2026-08-28")


def _day(day: int, *, close: float = 80000, high: float = 82000, low: float = 79500) -> Bar:
    return Bar(
        PARENT_TS + timedelta(days=day),
        close,
        high,
        low,
        close,
        1.0,
        timeframe="1d",
    )


def _rows(factory, table):
    with factory() as session:
        return session.execute(select(table).order_by(table.c.id)).mappings().all()


async def _funded_stack(cfg):
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    venue = _TraceVenue()
    executor = LiveExecutor(trades_api=venue, recorder=recorder, run_id=0)
    machine, bars = _entry_bars()
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": "2026-08-21T00:00:00+00:00"},
        machine=machine,
    )
    binding = StrategyBinding(INSTANCE, strategy)
    policy = SizingPolicy(mode="equity_fraction", fixed_notional_strategy_ids=frozenset({INSTANCE}))
    await run_portfolio_live(
        cfg=cfg,
        data_source=_TraceBars(venue, bars),
        bindings=(binding,),
        executor=executor,
        recorder=recorder,
        sizing_policy=policy,
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert [(o[1], o[2], o[3], o[4]) for o in venue.opens] == [
        ("buy", 100, 5, price) for price in ENTRY_PRICES
    ]
    assert [order["position_key"] for order in _rows(factory, orders)] == [
        f"k{k}" for k in range(4)
    ]
    assert [order["ts"].date().day for order in _rows(factory, orders)] == list(ENTRY_DATES)
    entry_signals = [row for row in _rows(factory, signals) if row["kind"] == "entry"]
    assert [row["reason"] for row in entry_signals] == [
        "structure_parent",
        "raw_same_side_addon",
        "raw_same_side_addon",
        "raw_same_side_addon",
    ]
    parent_features = entry_signals[0]["metadata_json"]
    assert parent_features["signal_ts"] == "2026-08-21T00:00:00+00:00"
    assert parent_features["boundary"] == BOUNDARY
    assert parent_features["distance_ema_atr"] >= 1.5
    assert parent_features["average_overlap10"] <= 0.55
    assert [row["metadata_json"]["k"] for row in entry_signals] == list(range(4))
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.campaign_id == "20260822L"
    assert strategy.machine.campaign.boundary == BOUNDARY
    assert strategy.machine.campaign.lifetime_units == 4
    assert [(u.k, u.origin, u.entry_price) for u in strategy.machine.campaign.units] == [
        (k, "live", price) for k, price in enumerate(ENTRY_PRICES)
    ]
    return venue, executor, recorder, factory, policy, strategy


async def _continue(cfg, venue, executor, recorder, policy, strategy, bars):
    return await run_portfolio_live(
        cfg=cfg,
        data_source=_TraceBars(venue, bars),
        bindings=(StrategyBinding(INSTANCE, strategy),),
        executor=executor,
        recorder=recorder,
        sizing_policy=policy,
        account_balance_provider=None,
        install_signal_handlers=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "last_day", "final_close", "expected_reason", "expected_net_sats"),
    [
        ("range", 8, BOUNDARY, "range_close", (-9335, -10382, -10455, -12410)),
        ("recovery", 85, 82000, "recover", (5760, 4713, 4640, 2685)),
        ("cap", 120, 80000, "maximum_hold", (2711, 1665, 1592, -364)),
    ],
)
async def test_funded_stack_daily_exit_paths(
    cfg, path, last_day, final_close, expected_reason, expected_net_sats
):
    venue, executor, recorder, factory, policy, strategy = await _funded_stack(cfg)
    # Restart from the saved strategy and venue-backed executor state, rather
    # than carrying in-memory state from the entry loop.
    executor = LiveExecutor(trades_api=venue, recorder=recorder, run_id=0)
    await executor.reconcile()
    strategy = CloseRangeLive(strategy.params)
    leading = [_day(day) for day in range(8, last_day)]
    if path == "range":
        terminal = _day(last_day, close=BOUNDARY, high=82000, low=72000)
    elif path == "recovery":
        terminal = _day(last_day, close=final_close, high=final_close, low=79500)
    else:
        terminal = _day(last_day)
    await _continue(cfg, venue, executor, recorder, policy, strategy, [*leading, terminal])

    assert venue.running == {}
    assert venue.closes == [f"trace-{k}" for k in range(4)]
    all_orders = _rows(factory, orders)
    assert len(all_orders) == 8
    exits = all_orders[4:]
    assert [row["position_key"] for row in exits] == [f"k{k}" for k in range(4)]
    assert [row["ts"].date() for row in exits] == [terminal.ts.date()] * 4
    assert [row["price_usd"] for row in exits] == [final_close] * 4
    assert [row["side"] for row in exits] == ["sell"] * 4
    assert [row["leverage"] for row in exits] == [5] * 4
    signal_rows = _rows(factory, signals)
    assert [row["reason"] for row in signal_rows if row["kind"] == "exit"] == [expected_reason] * 4
    events = _rows(factory, strategy_pnl_events)
    assert len(events) == 8
    for k in range(4):
        opening, closing = events[k], events[k + 4]
        assert (opening["position_key"], opening["kind"], opening["amount_sats"]) == (
            f"k{k}",
            "opening_fee",
            -7,
        )
        assert (closing["position_key"], closing["kind"]) == (f"k{k}", "close_net_pl")
        assert opening["amount_sats"] + closing["amount_sats"] == expected_net_sats[k]
    assert strategy.machine.campaign is None


@pytest.mark.asyncio
@pytest.mark.parametrize("liquidated_k", range(4))
async def test_funded_stack_each_unit_liquidation(cfg, liquidated_k):
    venue, executor, recorder, factory, policy, strategy = await _funded_stack(cfg)
    executor = LiveExecutor(trades_api=venue, recorder=recorder, run_id=0)
    await executor.reconcile()
    strategy = CloseRangeLive(strategy.params)
    liquidated_id = f"trace-{liquidated_k}"
    venue.liquidate(liquidated_id, ENTRY_PRICES[liquidated_k] * 5 / 6)
    minute = Bar(
        datetime(2026, 8, 29, 12, tzinfo=UTC),
        70000,
        70000,
        70000,
        70000,
        1.0,
        timeframe="1m",
    )
    await _continue(cfg, venue, executor, recorder, policy, strategy, [minute])

    all_orders = _rows(factory, orders)
    external = [
        row for row in all_orders if row["metadata_json"].get("isolated_action") == "external_close"
    ]
    assert len(external) == 1
    assert external[0]["position_key"] == f"k{liquidated_k}"
    assert external[0]["metadata_json"]["liquidated"] is True
    assert external[0]["leverage"] == 5
    events = _rows(factory, strategy_pnl_events)
    liquidation = [row for row in events if row["kind"] == "liquidation"]
    assert len(liquidation) == 1
    assert liquidation[0]["position_key"] == f"k{liquidated_k}"
    if liquidated_k == 0:
        assert venue.closes == ["trace-1", "trace-2", "trace-3"]
        assert venue.running == {}
        assert strategy.machine.campaign is None
        assert [row["leverage"] for row in _rows(factory, orders)[5:]] == [5] * 3
        assert [row["reason"] for row in _rows(factory, signals) if row["kind"] == "exit"] == [
            "parent_liquidation"
        ] * 3
    else:
        assert venue.closes == []
        assert sorted(venue.running) == [f"trace-{k}" for k in range(4) if k != liquidated_k]
        campaign = strategy.machine.campaign
        assert campaign is not None
        assert campaign.lifetime_units == 4
        assert [u.k for u in campaign.units] == [k for k in range(4) if k != liquidated_k]
        # A later shared range exit must close only the three surviving units;
        # the liquidated K slot is neither reopened nor closed twice.
        await _continue(
            cfg,
            venue,
            executor,
            recorder,
            policy,
            strategy,
            [_day(8, close=BOUNDARY, high=82000, low=72000)],
        )
        assert venue.running == {}
        assert venue.closes == [f"trace-{k}" for k in range(4) if k != liquidated_k]
        assert strategy.machine.campaign is None
        assert [row["reason"] for row in _rows(factory, signals) if row["kind"] == "exit"] == [
            "range_close"
        ] * 3


@pytest.mark.asyncio
async def test_four_unit_short_campaign_has_symmetric_entry_and_range_exit(cfg):
    engine = make_engine(cfg.storage_db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    venue = _TraceVenue()
    executor = LiveExecutor(trades_api=venue, recorder=recorder, run_id=0)
    machine, bars = _historical_entry_bars("2026-05-31", "2026-06-01", "2026-06-04")
    strategy = CloseRangeLive(
        {"unit_notional_usd": 100, "leverage": 5, "activation_ts": "2026-06-01T00:00:00+00:00"},
        machine=machine,
    )
    policy = SizingPolicy(mode="equity_fraction", fixed_notional_strategy_ids=frozenset({INSTANCE}))
    await _continue(cfg, venue, executor, recorder, policy, strategy, bars)
    assert venue.opens == [
        ("trace-0", "sell", 100, 5, 71297.5),
        ("trace-1", "sell", 100, 5, 66636.0),
        ("trace-2", "sell", 100, 5, 64025.5),
        ("trace-3", "sell", 100, 5, 63783.5),
    ]
    assert strategy.machine.campaign is not None
    assert strategy.machine.campaign.boundary == 73347.5
    # June 5's completed close back above the short parent's lower boundary
    # is acted on at the June 6 open, closing all four buy-to-cover slots.
    exit_bar = Bar(
        datetime(2026, 6, 6, tzinfo=UTC),
        73347.5,
        74000,
        63000,
        73347.5,
        1.0,
        timeframe="1d",
    )
    await _continue(cfg, venue, executor, recorder, policy, strategy, [exit_bar])
    assert venue.running == {}
    assert venue.closes == [f"trace-{k}" for k in range(4)]
    assert [row["side"] for row in _rows(factory, orders)[4:]] == ["buy"] * 4
    assert [row["reason"] for row in _rows(factory, signals) if row["kind"] == "exit"] == [
        "range_close"
    ] * 4
