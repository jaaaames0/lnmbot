"""Regression tests for funded range and deployment audit findings. Offline only."""

import io
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.data.multitimeframe import MultiTimeframeDataSource
from lnmarkets_bot.engine.live_executor import LiveExecutor
from lnmarkets_bot.engine.portfolio_live import StrategyBinding, run_portfolio_live
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.recorder import Recorder
from lnmarkets_bot.risk.guard import SizingPolicy
from lnmarkets_bot.strategy import Bar, StrategyState
from lnmarkets_bot.strategy.base import TfPosition
from lnmarkets_bot.strategy.impulse_range import H4, Candle, Channel, Setup
from lnmarkets_bot.strategy.impulse_range_live import SLOT, ImpulseRangeLive
from lnmarkets_bot.strategy.intents import OrderIntent, Side, SignalKind
from tests.test_dashboard import _create_multistrategy_dashboard_db
from tests.test_dashboard import _dashboard_module as _source_dashboard_module
from tests.test_live_executor import FakeIsolatedTradesApi
from tests.test_portfolio_live import _Bars, _Entry

T0 = datetime(2026, 9, 1, tzinfo=UTC)
OWNER = "btc_impulse_range_v1"
KEY = f"{OWNER}:{SLOT}"


def _dashboard_module():
    return _source_dashboard_module()


def active(*, chop_filter=False, tradeable=True, er=0.1):
    s = ImpulseRangeLive({"mode": "funded", "chop_filter": chop_filter})
    m = s.range_machine
    m.state = "active"
    m.bar_ts = T0
    m.channel = Channel(
        id=0,
        side=1,
        impulse_ts=T0 - H4,
        confirmed_ts=T0,
        lo=90,
        hi=110,
        first_lo=90,
        first_hi=110,
        er_checked=True,
        er_at_confirm=er,
        tradeable=tradeable,
    )
    return s


def minute(i, px=100, *, warmup=False):
    return Bar(T0 + timedelta(minutes=i), px, px, px, px, 0, "1m", warmup=warmup)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_close", [False, True], ids=["before-submit", "failed-close"])
async def test_close_obligation_survives_restart(tmp_path, failed_close):
    """Real executor + temp database; venue retains the owned trade across restart."""
    eng = make_engine(tmp_path / "state.sqlite")
    init_schema(eng)
    recorder = Recorder(make_session_factory(eng))
    run = recorder.start_run(
        mode="live", strategy_name="audit", strategy_params={}, config={}, started_at=T0
    )
    api = FakeIsolatedTradesApi()
    api.entry_price = 92
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=run)
    executor.update_price(92)
    entry = OrderIntent(
        kind=SignalKind.ENTRY,
        trigger_tf="1m",
        position_key=SLOT,
        strategy_instance_id=OWNER,
        side=Side.LONG,
        size_usd=100,
        leverage=2,
        reason="audit",
    )
    sig = recorder.record_signal(run, ts=T0, kind="entry", reason="audit")
    await executor.submit(intent=entry, signal_id=sig, run_id=run, ts=T0, size_usd=100, leverage=2)
    state = StrategyState()
    state.positions[SLOT] = TfPosition(side="long", qty_sats=100, entry_price_usd=92)
    s = active()
    s.range_machine.record_entry(1, 92, T0, 1)
    (exit_intent,) = s.on_bar(minute(1, 101), state)
    recorder.save_strategy_state(
        run,
        mode="live",
        strategy_name=OWNER,
        ts=T0,
        state=json.loads(json.dumps(s.persistent_state())),
    )
    if failed_close:
        close = api.close_trade

        async def fail(_trade_id):
            raise OSError("fake venue outage")

        api.close_trade = fail
        await executor.submit(
            intent=replace(exit_intent, strategy_instance_id=OWNER),
            signal_id=sig,
            run_id=run,
            ts=T0,
            size_usd=0,
            leverage=2,
        )
        api.close_trade = close
        assert executor._pending_exits
    restarted_executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=run)
    await restarted_executor.reconcile()
    await run_portfolio_live(
        cfg=BotConfig(_env_file=None, storage_db_path=tmp_path / "state.sqlite"),
        data_source=_Bars([minute(2, 101), minute(3, 101)]),
        bindings=(StrategyBinding(OWNER, ImpulseRangeLive(s.params)),),
        executor=restarted_executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(fixed_notional_strategy_ids=frozenset({OWNER})),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert api.running == {}, "Owned trade was never closed after restart"


@pytest.mark.parametrize(
    "old_filter,old_tradeable,new_filter,new_threshold,expected",
    [
        (True, False, False, 0.22, True),
        (False, True, True, 0.22, False),
        (True, False, True, 0.05, True),
    ],
    ids=["disable-filter", "enable-filter", "lower-threshold"],
)
def test_filter_change_updates_existing_channel(
    old_filter, old_tradeable, new_filter, new_threshold, expected
):
    s = active(chop_filter=old_filter, tradeable=old_tradeable)
    fresh = ImpulseRangeLive(
        {"mode": "funded", "chop_filter": new_filter, "chop_threshold": new_threshold}
    )
    assert fresh.restore_persistent_state(json.loads(json.dumps(s.persistent_state())))
    assert fresh.range_machine.channel.tradeable is expected


@pytest.mark.asyncio
async def test_missing_minute_in_nonstrict_cold_history_blocks_range():
    """Actual live aggregation, not a hand-built sequence of complete 4h candles."""
    s = active()
    state = StrategyState()
    stream = MultiTimeframeDataSource(
        _Bars([minute(i, 100, warmup=True) for i in range(480) if i != 60]),
        higher_timeframes=("4h",),
        require_complete_buckets=True,
        strict_from_ts=T0 + timedelta(days=1),
    )
    aggregate_ends = []
    async for bar in stream.stream():
        s.on_bar(bar, state)
        if bar.timeframe == "4h":
            aggregate_ends.append(bar.ts)
    assert aggregate_ends == [T0 + H4, T0 + 2 * H4]
    assert not s.model_complete, "Missing minute produced a seemingly complete 4h candle"


def test_initial_confirmation_keeps_tested_width_rule():
    """Rule 3 (as tested): a crash may confirm a channel wider than the cap."""
    m = active().range_machine
    m.state, m.channel = "seek", None
    m.setup = Setup(side=1, impulse_ts=T0 - H4, extreme=100, pulled=True, swing=50)
    m.close_bar(Candle(T0, 65, 75, 50, 70))
    m.open_bar(T0 + H4, 70)
    assert (m.channel.lo, m.channel.hi) == (50, 100)
    assert m.levels() is not None and m.levels().allowed == frozenset({1, -1})


def test_range_filter_is_passed_through_http_handler(monkeypatch):
    """Exercise real do_GET without opening a socket or starting the price stream."""
    dashboard = _dashboard_module()
    monkeypatch.setattr(dashboard._PRICE_STREAM, "start", lambda *_: None)
    monkeypatch.setattr("sys.argv", ["run_dashboard", "--db", "/not-opened.sqlite"])
    captured = {}
    monkeypatch.setattr(
        dashboard, "_render", lambda _db, page, tf, *_: captured.update(page=page, tf=tf) or "ok"
    )

    class Server:
        def __init__(self, _address, handler):
            self.handler = handler

        def serve_forever(self):
            h = object.__new__(self.handler)
            h.path = "/signals?tf=range"
            h.wfile = io.BytesIO()
            h.send_response = lambda *_: None
            h.send_header = lambda *_: None
            h.end_headers = lambda: None
            h.do_GET()

    # Production uses ThreadingHTTPServer; the module attribute is inspected below.
    monkeypatch.setattr(dashboard, "ThreadingHTTPServer", Server)
    dashboard.main()
    assert captured["tf"] == "range", "HTTP handler silently discarded the Range scope"


@pytest.mark.asyncio
async def test_unbound_owned_position_blocks_admission(tmp_path):
    """Old and current portfolio engines are identical; an omitted owner is not unknown."""
    eng = make_engine(tmp_path / "orphan.sqlite")
    init_schema(eng)
    recorder = Recorder(make_session_factory(eng))
    run = recorder.start_run(
        mode="live", strategy_name="audit", strategy_params={}, config={}, started_at=T0
    )
    api = FakeIsolatedTradesApi()
    api.entry_price = 92
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=run)
    executor.update_price(92)
    entry = OrderIntent(
        kind=SignalKind.ENTRY,
        trigger_tf="1m",
        position_key=SLOT,
        strategy_instance_id=OWNER,
        side=Side.LONG,
        size_usd=100,
        leverage=2,
        reason="audit",
    )
    sig = recorder.record_signal(run, ts=T0, kind="entry", reason="audit")
    await executor.submit(intent=entry, signal_id=sig, run_id=run, ts=T0, size_usd=100, leverage=2)
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=run)
    await executor.reconcile()
    assert executor.position_qty_sats(KEY) == 100
    assert executor.entries_blocked_reason() is None  # Locally recorded is not unknown.
    await run_portfolio_live(
        cfg=BotConfig(_env_file=None, storage_db_path=tmp_path / "orphan.sqlite"),
        data_source=_Bars([minute(1)]),
        bindings=(StrategyBinding("ma_cross_primary", _Entry()),),
        executor=executor,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert len(api.trades) == 1, "Entry submitted while an owned slot has no strategy manager"


def test_dashboard_cannot_report_aligned_when_configured_range_is_absent(tmp_path, monkeypatch):
    dashboard = _dashboard_module()
    db = tmp_path / "dashboard.sqlite"
    _create_multistrategy_dashboard_db(db, funded=False)
    with sqlite3.connect(db) as connection:
        connection.execute(
            "UPDATE runs SET config_json=?", (json.dumps({"strategy_range_mode": "funded"}),)
        )
    monkeypatch.setattr(
        dashboard,
        "_persisted_breakout_state",
        lambda _db: {"machine": {"campaign": None, "historical_model_complete": True}},
    )
    exchange = SimpleNamespace(fetched_at=datetime.now(UTC), trades={})
    status, _detail, _ = dashboard._execution_alignment(db, [], exchange)
    # The caller has no range_enabled argument; with funded range configured
    # but _range_strategy returning None, it executes exactly this same check.
    assert status != "Aligned", "Missing configured range owner was reported as aligned"


def test_disabled_range_owned_slot_is_shown_as_exits_only():
    dashboard = _dashboard_module()
    s = active()
    s.entries_enabled = False  # _range_strategy uses funded + False for off with an owned slot.
    snapshot = json.loads(json.dumps(s.persistent_state()))
    context = dashboard._range_context(snapshot, [], None)
    assert not context["entries_enabled"], "Configured off was displayed as admitting entries"


@pytest.mark.asyncio
async def test_strict_feed_gap_does_not_stop_all_funded_management():
    stream = MultiTimeframeDataSource(
        _Bars([minute(0), minute(2)]),
        higher_timeframes=("4h",),
        require_complete_buckets=True,
        strict_from_ts=T0,
    )
    delivered = []
    async for bar in stream.stream():
        delivered.append(bar.ts)
    assert minute(2).ts in delivered, (
        "The next minute is needed for reconciliation and close retries"
    )


@pytest.mark.parametrize("side", [1, -1])
@pytest.mark.parametrize("width", [0.399, 0.4, 0.401, 0.65])
def test_width_confirmation_for_both_impulses(side, width):
    m = active().range_machine
    m.state, m.channel = "seek", None
    lo, hi = 100, 100 * (1 + width)
    m.setup = Setup(
        side=side,
        impulse_ts=T0 - H4,
        extreme=hi if side == 1 else lo,
        pulled=True,
        swing=lo if side == 1 else hi,
    )
    m.close_bar(Candle(T0, 120, hi, lo, (lo + hi) / 2))
    m.open_bar(T0 + H4, (lo + hi) / 2)
    assert m.levels() is not None


def test_expansion_beyond_width_cap_still_ends_range():
    m = active().range_machine
    m.channel.expanding, m.channel.new_extreme = 1, 110
    m.close_bar(Candle(T0, 110, 127, 109, 126))
    assert m.state == "idle" and m.channel is None
    assert m.ended_ranges == 1


@pytest.mark.parametrize("rule", [1, 2])
@pytest.mark.parametrize("holding", [False, True])
def test_earlier_rule_snapshots_restore_unchanged(rule, holding):
    s = active()
    if holding:
        s.range_machine.record_entry(1, 92, T0, 1)
    snapshot = s.persistent_state()
    snapshot["version"] = min(rule, s.VERSION)
    snapshot["machine"]["version"] = rule
    snapshot["machine"]["channel"]["lo"] = 50  # wider than the cap, as tested
    restored = ImpulseRangeLive(s.params)
    assert restored.restore_persistent_state(snapshot)
    m = restored.range_machine
    assert m.state == "active" and m.channel.lo == 50
    assert m.levels().allowed == frozenset({1, -1})
    assert (m.position is not None) is holding
    assert restored.persistent_state()["machine"]["version"] == 3
    assert restored.events[-1]["detail"] == {"from": rule, "to": 3}


def test_liquidation_in_wide_range_blocks_reentry_for_the_bar():
    """Isolated liquidation can precede the 4h-close stop; do not buy straight back."""
    s = active()
    m = s.range_machine
    m.channel.lo, m.channel.first_lo = 50, 50
    state = StrategyState()
    state.positions[SLOT] = TfPosition(side=Side.LONG, qty_sats=100)
    m.record_entry(1, 60, T0, 1)
    state.positions[SLOT] = TfPosition()
    s.on_external_position_closed(SimpleNamespace(reason="liquidation"), state)
    assert m.position is None and m.state == "active"
    # Price is below the buy level (57.5) but inside the stop band (45).
    assert s.on_bar(minute(5, 48), state) == []
    assert s.on_bar(minute(6, 49), state) == []
    # Next 4h bar with a close inside the band: entries resume.
    s.on_bar(Bar(T0 + H4, 48, 55, 47, 50, 0, "4h"), state)
    intents = s.on_bar(minute(241, 50), state)
    assert [i.kind for i in intents] == [SignalKind.ENTRY]


@pytest.mark.asyncio
@pytest.mark.parametrize("window", ["ambiguous_success", "result_write", "accounting_write"])
async def test_remote_close_recovery_accounts_once_without_resubmission(
    recorder, monkeypatch, window
):
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    executor.update_price(100)
    entry = replace(
        OrderIntent.enter_long("1m", 100, 2), strategy_instance_id=OWNER, position_key=SLOT
    )
    await executor.submit(intent=entry, signal_id=1, run_id=1, ts=T0, size_usd=100, leverage=2)
    close = api.close_trade

    async def close_with_history(trade_id):
        trade = api.running[trade_id]
        result = await close(trade_id)
        trade.price = 100
        trade.status = "closed"
        trade.raw = {"exitPrice": 100, "closeReason": "manual"}
        api.closed[trade_id] = trade
        return result

    monkeypatch.setattr(api, "close_trade", close_with_history)
    if window == "ambiguous_success":

        async def accepted_then_timeout(trade_id):
            await close_with_history(trade_id)
            raise TimeoutError("response lost")

        monkeypatch.setattr(api, "close_trade", accepted_then_timeout)
    else:
        method = "command_result" if window == "result_write" else "apply_command"
        monkeypatch.setattr(recorder, method, lambda *_: (_ for _ in ()).throw(OSError("disk")))
    intent = replace(
        OrderIntent.exit("1m", reason="target"), strategy_instance_id=OWNER, position_key=SLOT
    )
    if window == "ambiguous_success":
        await executor.submit(intent=intent, signal_id=1, run_id=1, ts=T0, size_usd=0, leverage=2)
    else:
        with pytest.raises(RuntimeError):
            await executor.submit(
                intent=intent, signal_id=1, run_id=1, ts=T0, size_usd=0, leverage=2
            )
    monkeypatch.undo()
    restarted = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    await restarted.reconcile()
    await restarted.reconcile_external_closures(run_id=1, ts=T0 + timedelta(minutes=1))
    await restarted.retry_pending_exits(run_id=1, ts=T0 + timedelta(minutes=1))
    assert not api.running
    assert api.closes == ["iso-1"]
    assert (
        len([c for c in recorder.commands() if c["action"] == "close" and c["status"] == "applied"])
        == 1
    )


@pytest.mark.asyncio
async def test_gap_continues_all_owner_durable_closes_and_blocks_reentry(recorder):
    api = FakeIsolatedTradesApi()
    executor = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    executor.update_price(100)
    owners = ("ma_cross_primary", "btc_close_range_v1", OWNER)
    for owner in owners:
        intent = replace(
            OrderIntent.enter_long("1m", 100, 2), strategy_instance_id=owner, position_key="1m"
        )
        await executor.submit(intent=intent, signal_id=1, run_id=1, ts=T0, size_usd=100, leverage=2)
        pos = executor.positions[intent.execution_key]
        recorder.begin_command(
            f"close:{pos.trade_id}",
            "close",
            {"trade_id": pos.trade_id, "execution_key": intent.execution_key, "ts": T0.isoformat()},
        )
    restarted = LiveExecutor(trades_api=api, recorder=recorder, run_id=1)
    await restarted.reconcile()
    source = MultiTimeframeDataSource(
        _Bars([minute(0, warmup=True), minute(2)]),
        higher_timeframes=("4h",),
        require_complete_buckets=True,
    )
    # Every owner tries a new entry on each minute; the gap health rejects it.
    await run_portfolio_live(
        cfg=BotConfig(_env_file=None),
        data_source=source,
        bindings=tuple(StrategyBinding(o, _Entry()) for o in owners),
        executor=restarted,
        recorder=recorder,
        sizing_policy=SizingPolicy(),
        account_balance_provider=None,
        install_signal_handlers=False,
    )
    assert api.closes[:3] == ["iso-1", "iso-2", "iso-3"]
    assert all(restarted.data_health[o] for o in owners)
    # Entries can occur before the gap is observed, but none after it.
    for owner in owners:
        assert (
            restarted.entry_admission_reason(
                replace(OrderIntent.enter_long("1m", 10, 2), strategy_instance_id=owner),
                minute(2).ts,
            )
            == "market_evidence_incomplete"
        )


def test_incomplete_range_retains_owned_target_after_frozen_bar_window():
    s = active()
    s.range_machine.record_entry(1, 92, T0, 1)
    s.mark_evidence_incomplete(T0 + H4)
    state = StrategyState(positions={SLOT: TfPosition(side="long", qty_sats=100, trade_id="owned")})
    (intent,) = s.on_bar(minute(300, 101), state)
    assert intent.kind == SignalKind.EXIT and intent.reason == "target"
    assert s.persistent_state()["close_trade_id"] == "owned"
