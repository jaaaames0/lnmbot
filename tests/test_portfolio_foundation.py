from __future__ import annotations

import copy
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from lnmarkets_bot.portfolio.store import PortfolioStore, read_overview

NOW = datetime(2026, 9, 22, 5, tzinfo=UTC)


@pytest.fixture
def book(tmp_path):
    store = PortfolioStore(tmp_path / "portfolio.sqlite")
    store.register("ma", "live", "ma-v1")
    store.register("breakout", "live", "breakout-v1")
    store.register("shadow", "paper", "breakout-v1")
    return store


def test_shared_pool_serializes_competing_strategies_and_separates_paper(book):
    book.observe_cash("cash", "live", NOW, 1000)
    book.observe_cash("paper-cash", "paper", NOW, 1000)

    def reserve(strategy):
        try:
            book.reserve(strategy, strategy, "campaign", 700, now=NOW)
        except ValueError as exc:
            assert str(exc) == "insufficient shared cash"
            return False
        return True

    with ThreadPoolExecutor(2) as executor:
        assert sorted(executor.map(reserve, ["ma", "breakout"])) == [False, True]
    book.reserve("paper", "shadow", "campaign", 1000, now=NOW)
    with pytest.raises(ValueError, match="insufficient"):
        book.reserve("another", "ma", "campaign2", 301, now=NOW)


def test_unknown_submission_remains_reserved_and_cash_must_be_fresh(book):
    with pytest.raises(ValueError, match="fresh"):
        book.reserve("r", "ma", "c", 100, now=NOW)
    book.observe_cash("cash", "live", NOW, 1000)
    book.reserve("r", "ma", "c", 800, now=NOW)
    book.reserve("r", "ma", "c", 800, now=NOW)  # Retry, not another commitment.
    with pytest.raises(ValueError, match="identity"):
        book.reserve("r", "breakout", "c", 800, now=NOW)
    with pytest.raises(ValueError, match="fresh"):
        book.reserve("late", "breakout", "c", 100, now=NOW + timedelta(minutes=2))
    book.release("r")
    book.reserve("new", "breakout", "c", 1000, now=NOW)


def test_posted_collateral_requires_explicit_cash_reconciliation(book):
    book.observe_cash("cash", "live", NOW, 1000)
    book.reserve("r", "ma", "c", 600, now=NOW)
    book.record_position("ma-daily", "r", 0, "venue-trade-1")
    with pytest.raises(ValueError, match="cannot release"):
        book.release("r")
    with pytest.raises(ValueError, match="insufficient"):
        book.reserve("too-much", "breakout", "c", 401, now=NOW)
    later = NOW + timedelta(seconds=1)
    book.observe_cash("after-fill", "live", later, 400, reflected_reservations=("r",))
    book.reserve("b", "breakout", "c", 400, now=later)
    # No double counting after reconciliation, and no fresh credit from paper.
    with pytest.raises(ValueError, match="insufficient"):
        book.reserve("over", "ma", "c2", 1, now=later)


def test_attribution_restart_deduplication_and_closed_profitability(book):
    book.observe_cash("cash", "live", NOW, 1000)
    for strategy in ("ma", "breakout"):
        book.reserve(strategy, strategy, "c", 100, now=NOW)
        book.record_position(strategy, strategy, 0, f"venue-{strategy}")
    book.record_accounting("ma-fee", "ma", "fee", -10, NOW)
    book.record_accounting("ma-funding", "ma", "funding", 3, NOW)
    book.record_accounting("ma-close", "ma", "realized", 100, NOW)
    book.record_close("ma", NOW, "range_close")
    book.record_accounting("b-fee", "breakout", "fee", -10, NOW)
    book.record_accounting("b-funding", "breakout", "funding", -20, NOW)
    book.record_accounting("b-close", "breakout", "realized", -100, NOW)
    book.record_close("breakout", NOW, "liquidation")
    restarted = PortfolioStore(book.path)
    restarted.record_accounting("ma-funding", "ma", "funding", 3, NOW)
    restarted.record_close("ma", NOW, "range_close")
    with pytest.raises(ValueError, match="identity"):
        restarted.record_accounting("ma-funding", "breakout", "funding", 3, NOW)
    with pytest.raises(sqlite3.IntegrityError):
        restarted.record_accounting("unowned", "hypothetical", "realized", 100000, NOW)
    rows = {row["id"]: row for row in read_overview(book.path)["strategies"]}
    assert rows["ma"]["net_sats"] == 93
    assert rows["ma"]["winning_trades"] == 1
    assert rows["breakout"]["net_sats"] == -130
    assert rows["breakout"]["closed_trades"] == 1
    assert rows["breakout"]["winning_trades"] == 0
    assert rows["shadow"]["net_sats"] == 0


def snapshot():
    # Keep tests independent of gitignored research artifacts.
    return {
        "mode": "shadow_no_orders",
        "order_capability": False,
        "strategy": "structure_parent_addons_raw",
        "rules_sha256": "breakout-v1",
        "as_of_close": "2026-09-21T00:00:00+00:00",
        "next_open": "2026-09-22T00:00:00+00:00",
        "generated_at": NOW.isoformat(),
        "active_hypothetical_stack": {
            "parent_id": "20260822L",
            "entry_ts": "2026-08-22T00:00:00+00:00",
            "side": "long",
            "active_units": 4,
            "boundary": 72998.7,
            "pending_exit": None,
        },
        "historical_replay_metrics": {"net_btc": 100},
    }


def test_seed_is_durable_occupancy_not_owned_trades_or_profit(book):
    seed = snapshot()
    book.import_shadow_observation("shadow", seed, now=NOW)
    again = copy.deepcopy(seed)
    again["generated_at"] = (NOW + timedelta(minutes=1)).isoformat()
    book.import_shadow_observation("shadow", again, now=NOW + timedelta(minutes=1))
    result = read_overview(book.path)
    assert book.parent_admission_reason("shadow", now=NOW) == "existing_hypothetical_campaign"
    assert book.parent_admission_reason("unknown", now=NOW) == "missing_campaign_context"
    assert (
        book.parent_admission_reason("shadow", now=NOW + timedelta(days=2))
        == "stale_campaign_context"
    )
    assert result["seeds"][0]["parent_occupied"]
    assert result["seeds"][0]["close_ts"] == "2026-09-22T00:00:00+00:00"
    assert all(row["net_sats"] == 0 for row in result["strategies"])
    with book.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM owned_positions").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    revised = copy.deepcopy(seed)
    revised["active_hypothetical_stack"]["boundary"] += 1
    with pytest.raises(ValueError, match="observation changed"):
        book.import_shadow_observation("shadow", revised, now=NOW)


def test_seed_rejects_forming_data_rules_changes_and_gaps(book):
    seed = snapshot()
    with pytest.raises(ValueError, match="forming"):
        book.import_shadow_observation("shadow", seed, now=NOW - timedelta(days=1))
    wrong = copy.deepcopy(seed)
    wrong["rules_sha256"] = "changed"
    with pytest.raises(ValueError, match="matching"):
        book.import_shadow_observation("shadow", wrong, now=NOW)
    book.import_shadow_observation("shadow", seed, now=NOW)
    later = copy.deepcopy(seed)
    later.update(
        as_of_close="2026-09-23T00:00:00+00:00",
        next_open="2026-09-24T00:00:00+00:00",
        generated_at=(NOW + timedelta(days=2)).isoformat(),
    )
    with pytest.raises(ValueError, match="one completed day"):
        book.import_shadow_observation("shadow", later, now=NOW + timedelta(days=2))


def test_pending_exit_keeps_occupancy_until_later_observation(book):
    seed = snapshot()
    seed["active_hypothetical_stack"]["pending_exit"] = "recover"
    book.import_shadow_observation("shadow", seed, now=NOW)
    assert read_overview(book.path)["seeds"][0]["parent_occupied"]
    later = copy.deepcopy(seed)
    later.update(
        as_of_close="2026-09-22T00:00:00+00:00",
        next_open="2026-09-23T00:00:00+00:00",
        generated_at=(NOW + timedelta(days=1)).isoformat(),
        active_hypothetical_stack=None,
    )
    book.import_shadow_observation("shadow", later, now=NOW + timedelta(days=1))
    assert not read_overview(book.path)["seeds"][0]["parent_occupied"]


def test_refuses_trader_database_and_read_projection_never_creates_file(tmp_path):
    live = tmp_path / "trader.sqlite"
    with sqlite3.connect(live) as db:
        db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO orders VALUES (7)")
    before = live.read_bytes()
    with pytest.raises(ValueError, match="another application"):
        PortfolioStore(live)
    assert live.read_bytes() == before
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        read_overview(missing)
    assert not missing.exists()


def test_machine_steps_are_atomic_idempotent_and_single_writer(book):
    initial = {"version": 1, "campaign": {"id": "historical"}}
    book.initialize_machine(
        "shadow", last_candle_ts=NOW, state=initial, activated_at=NOW + timedelta(days=1)
    )
    book.initialize_machine(
        "shadow", last_candle_ts=NOW, state=initial, activated_at=NOW + timedelta(days=1)
    )
    decision = {
        "ts": (NOW + timedelta(days=2)).isoformat(),
        "kind": "reject",
        "reason": "addon_cap",
        "campaign_id": "historical",
        "k": None,
        "side": 1,
        "price": None,
        "metadata": {"owned": False},
    }
    next_state = {"version": 1, "campaign": {"id": "historical", "held": 2}}
    book.advance_machine(
        "shadow",
        expected_previous_ts=NOW,
        candle_ts=NOW + timedelta(days=1),
        state=next_state,
        decisions=[decision],
    )
    book.advance_machine(
        "shadow",
        expected_previous_ts=NOW,
        candle_ts=NOW + timedelta(days=1),
        state=next_state,
        decisions=[decision],
    )
    with pytest.raises(ValueError, match="changed"):
        book.advance_machine(
            "shadow",
            expected_previous_ts=NOW,
            candle_ts=NOW + timedelta(days=1),
            state={"version": 1, "campaign": None},
            decisions=[decision],
        )
    with pytest.raises(ValueError, match="stale or gapped"):
        book.advance_machine(
            "shadow",
            expected_previous_ts=NOW - timedelta(days=1),
            candle_ts=NOW,
            state=next_state,
            decisions=[decision],
        )
    with book.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM strategy_decisions").fetchone()[0] == 1
    loaded = book.load_machine("shadow")
    assert loaded is not None
    assert loaded["state"] == next_state


def test_failed_machine_step_rolls_back_decisions(book):
    book.initialize_machine("shadow", last_candle_ts=NOW, state={"version": 1}, activated_at=NOW)
    bad = {
        "ts": (NOW + timedelta(days=1)).isoformat(),
        "kind": "signal",
        "reason": "x",
        "campaign_id": None,
        "k": None,
        "side": 1,
        "price": None,
        "metadata": {},
        "unexpected": True,
    }
    with pytest.raises(ValueError, match="shape"):
        book.advance_machine(
            "shadow",
            expected_previous_ts=NOW,
            candle_ts=NOW + timedelta(days=1),
            state={"version": 1, "advanced": True},
            decisions=[bad],
        )
    assert book.load_machine("shadow")["last_candle_ts"] == NOW.isoformat()
    with book.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM strategy_decisions").fetchone()[0] == 0
