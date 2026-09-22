"""Isolated portfolio foundation. No exchange client or live database migration.

Reservations share a cash pool, while positions and accounting events have
immutable owners. Historical occupancy is deliberately outside that cash book.
The caller supplies reconciled cash: this module never estimates wallet balance
from strategy P&L, and is not yet connected to the funded executor.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_metadata (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY, mode TEXT NOT NULL CHECK(mode IN ('live','paper')),
    rules_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cash_observations (
    event_id TEXT PRIMARY KEY, mode TEXT NOT NULL, ts TEXT NOT NULL,
    available_sats INTEGER NOT NULL CHECK(available_sats >= 0),
    reflections TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY, instance_id TEXT NOT NULL REFERENCES instances(id),
    campaign_id TEXT NOT NULL, amount_sats INTEGER NOT NULL CHECK(amount_sats > 0),
    status TEXT NOT NULL CHECK(status IN ('reserved','released','posted','accounted')),
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS owned_positions (
    id TEXT PRIMARY KEY, instance_id TEXT NOT NULL REFERENCES instances(id),
    campaign_id TEXT NOT NULL, k INTEGER NOT NULL CHECK(k >= 0 AND k <= 3),
    remote_id TEXT UNIQUE, reservation_id TEXT NOT NULL UNIQUE REFERENCES reservations(id),
    UNIQUE(instance_id,campaign_id,k)
);
CREATE TABLE IF NOT EXISTS accounting_events (
    id TEXT PRIMARY KEY, position_id TEXT NOT NULL REFERENCES owned_positions(id),
    kind TEXT NOT NULL CHECK(kind IN ('realized','fee','funding')),
    sats INTEGER NOT NULL, ts TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS position_closes (
    position_id TEXT PRIMARY KEY REFERENCES owned_positions(id),
    ts TEXT NOT NULL, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    instance_id TEXT NOT NULL REFERENCES instances(id), close_ts TEXT NOT NULL,
    digest TEXT NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY(instance_id,close_ts)
);
CREATE TABLE IF NOT EXISTS seed_state (
    instance_id TEXT PRIMARY KEY REFERENCES instances(id),
    campaign_id TEXT, active INTEGER NOT NULL, activated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS machine_state (
    instance_id TEXT PRIMARY KEY REFERENCES instances(id),
    last_candle_ts TEXT NOT NULL, state_digest TEXT NOT NULL,
    state_json TEXT NOT NULL, activated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_decisions (
    id TEXT PRIMARY KEY, instance_id TEXT NOT NULL REFERENCES instances(id),
    candle_ts TEXT NOT NULL, decision_ts TEXT NOT NULL, kind TEXT NOT NULL,
    reason TEXT NOT NULL, campaign_id TEXT, k INTEGER, side INTEGER,
    price REAL, payload TEXT NOT NULL
);
"""


def timestamp(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if result.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return result.astimezone(UTC)


def encode(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def positive_sats(value: int, *, zero: bool = False) -> None:
    if type(value) is not int or value < (0 if zero else 1):
        raise ValueError("sats must be an integer with the required nonnegative/positive sign")


class PortfolioStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            tables = {
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if tables and "portfolio_metadata" not in tables:
                raise ValueError("refusing to initialize over another application's database")
            db.executescript(SCHEMA)
            versions = list(db.execute("SELECT version FROM portfolio_metadata"))
            if not versions:
                db.execute("INSERT INTO portfolio_metadata VALUES (1)")
            elif [row[0] for row in versions] != [1]:
                raise ValueError("unsupported portfolio schema")

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def register(self, instance_id: str, mode: str, rules_hash: str) -> None:
        if not instance_id or not rules_hash or mode not in ("live", "paper"):
            raise ValueError("instance identity, mode and rule hash are required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM instances WHERE id=?", (instance_id,)).fetchone()
            if old:
                if (old["mode"], old["rules_hash"]) != (mode, rules_hash):
                    raise ValueError("instance configuration cannot change implicitly")
            else:
                db.execute("INSERT INTO instances VALUES (?,?,?)", (instance_id, mode, rules_hash))

    def observe_cash(
        self,
        event_id: str,
        mode: str,
        ts: datetime,
        available_sats: int,
        *,
        reflected_reservations: tuple[str, ...] = (),
    ) -> None:
        """Available venue wallet cash excludes posted margin, includes no paper credit."""
        positive_sats(available_sats, zero=True)
        if mode not in ("live", "paper"):
            raise ValueError("invalid book")
        if len(set(reflected_reservations)) != len(reflected_reservations):
            raise ValueError("duplicate cash reflection")
        values = (
            event_id,
            mode,
            timestamp(ts).isoformat(),
            available_sats,
            encode(sorted(reflected_reservations)),
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT * FROM cash_observations WHERE event_id=?", (event_id,)
            ).fetchone()
            if old:
                if tuple(old) != values:
                    raise ValueError("cash observation identity conflict")
                return
            latest = db.execute(
                "SELECT MAX(ts) FROM cash_observations WHERE mode=?", (mode,)
            ).fetchone()[0]
            if latest and timestamp(ts) <= timestamp(latest):
                raise ValueError("cash observations must advance")
            for reservation_id in reflected_reservations:
                row = db.execute(
                    "SELECT r.status,i.mode FROM reservations r JOIN instances i ON i.id=r.instance_id WHERE r.id=?",
                    (reservation_id,),
                ).fetchone()
                if not row or row[0] != "posted" or row[1] != mode:
                    raise ValueError(
                        "cash reconciliation may acknowledge only this book's posted margin"
                    )
                db.execute(
                    "UPDATE reservations SET status='accounted' WHERE id=?", (reservation_id,)
                )
            db.execute("INSERT INTO cash_observations VALUES (?,?,?,?,?)", values)

    def reserve(
        self,
        reservation_id: str,
        instance_id: str,
        campaign_id: str,
        amount_sats: int,
        *,
        now: datetime,
        max_cash_age: timedelta = timedelta(seconds=60),
    ) -> None:
        positive_sats(amount_sats)
        now = timestamp(now)
        payload = encode([instance_id, campaign_id, amount_sats])
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT payload FROM reservations WHERE id=?", (reservation_id,)
            ).fetchone()
            if old:
                if old[0] != payload:
                    raise ValueError("reservation identity conflict")
                return
            instance = db.execute(
                "SELECT mode FROM instances WHERE id=?", (instance_id,)
            ).fetchone()
            if not instance:
                raise ValueError("unknown strategy instance")
            cash = db.execute(
                "SELECT * FROM cash_observations WHERE mode=? ORDER BY ts DESC, rowid DESC LIMIT 1",
                (instance[0],),
            ).fetchone()
            if not cash or not timedelta(0) <= now - timestamp(cash["ts"]) <= max_cash_age:
                raise ValueError("fresh reconciled cash required")
            # Include posted commitments until a later cash reconciliation. The
            # conservative double count prevents spending a stale pre-fill balance.
            committed = db.execute(
                "SELECT COALESCE(SUM(r.amount_sats),0) FROM reservations r "
                "JOIN instances i ON i.id=r.instance_id WHERE i.mode=? "
                "AND r.status IN ('reserved','posted')",
                (instance[0],),
            ).fetchone()[0]
            if amount_sats + committed > cash["available_sats"]:
                raise ValueError("insufficient shared cash")
            db.execute(
                "INSERT INTO reservations VALUES (?,?,?,?,?,?)",
                (reservation_id, instance_id, campaign_id, amount_sats, "reserved", payload),
            )

    def release(self, reservation_id: str) -> None:
        """Only cancel unsubmitted reservations; ambiguous submissions stay reserved."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT status FROM reservations WHERE id=?", (reservation_id,)
            ).fetchone()
            if not row or row[0] in ("posted", "accounted"):
                raise ValueError("cannot release unknown or posted reservation")
            db.execute("UPDATE reservations SET status='released' WHERE id=?", (reservation_id,))

    def record_position(
        self,
        position_id: str,
        reservation_id: str,
        k: int,
        remote_id: str | None = None,
    ) -> None:
        if type(k) is not int or not 0 <= k <= 3:
            raise ValueError("invalid unit number")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            r = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if not r or r["status"] == "released":
                raise ValueError("position requires a reservation")
            values = (position_id, r["instance_id"], r["campaign_id"], k, remote_id, reservation_id)
            old = db.execute("SELECT * FROM owned_positions WHERE id=?", (position_id,)).fetchone()
            if old:
                if tuple(old) != values:
                    raise ValueError("position ownership conflict")
                return
            db.execute("INSERT INTO owned_positions VALUES (?,?,?,?,?,?)", values)
            db.execute("UPDATE reservations SET status='posted' WHERE id=?", (reservation_id,))

    def record_accounting(
        self,
        event_id: str,
        position_id: str,
        kind: str,
        sats: int,
        ts: datetime,
    ) -> None:
        """Signed cash P&L components: fees negative; received funding positive."""
        if type(sats) is not int or kind not in ("realized", "fee", "funding"):
            raise ValueError("invalid accounting event")
        if kind == "fee" and sats > 0:
            raise ValueError("fees must be nonpositive")
        values = (event_id, position_id, kind, sats, timestamp(ts).isoformat())
        payload = encode(values)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT payload FROM accounting_events WHERE id=?", (event_id,)
            ).fetchone()
            if old:
                if old[0] != payload:
                    raise ValueError("accounting identity conflict")
                return
            db.execute("INSERT INTO accounting_events VALUES (?,?,?,?,?,?)", (*values, payload))

    def record_close(self, position_id: str, ts: datetime, reason: str) -> None:
        """Record confirmed trade termination; wallet credit requires separate reconciliation."""
        values = (position_id, timestamp(ts).isoformat(), reason)
        if not reason:
            raise ValueError("close reason required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT * FROM position_closes WHERE position_id=?", (position_id,)
            ).fetchone()
            if old:
                if tuple(old) != values:
                    raise ValueError("close identity conflict")
                return
            db.execute("INSERT INTO position_closes VALUES (?,?,?)", values)

    def parent_admission_reason(
        self,
        instance_id: str,
        *,
        now: datetime,
        max_observation_age: timedelta = timedelta(hours=25),
    ) -> str:
        """Occupancy gate only; passing does not authorize or reserve an order.

        Add-on admission is deliberately outside this parent-only gate.
        """
        with self.connect() as db:
            row = db.execute(
                "SELECT close_ts,payload FROM observations WHERE instance_id=? ORDER BY close_ts DESC LIMIT 1",
                (instance_id,),
            ).fetchone()
        if not row:
            return "missing_campaign_context"
        if not timedelta(0) <= timestamp(now) - timestamp(row["close_ts"]) <= max_observation_age:
            return "stale_campaign_context"
        if json.loads(row["payload"]).get("active_hypothetical_stack"):
            return "existing_hypothetical_campaign"
        return "occupancy_clear_other_checks_required"

    def initialize_machine(
        self,
        instance_id: str,
        *,
        last_candle_ts: datetime,
        state: dict[str, Any],
        activated_at: datetime,
    ) -> None:
        state_json = encode(state)
        digest = hashlib.sha256(state_json.encode()).hexdigest()
        values = (
            instance_id,
            timestamp(last_candle_ts).isoformat(),
            digest,
            state_json,
            timestamp(activated_at).isoformat(),
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            instance = db.execute(
                "SELECT mode FROM instances WHERE id=?", (instance_id,)
            ).fetchone()
            if not instance or instance[0] != "paper":
                raise ValueError("forward shadow requires a registered paper instance")
            old = db.execute(
                "SELECT * FROM machine_state WHERE instance_id=?", (instance_id,)
            ).fetchone()
            if old:
                if tuple(old) != values:
                    raise ValueError("machine initialization conflict")
                return
            db.execute("INSERT INTO machine_state VALUES (?,?,?,?,?)", values)

    def load_machine(self, instance_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM machine_state WHERE instance_id=?", (instance_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "last_candle_ts": row["last_candle_ts"],
            "state": json.loads(row["state_json"]),
            "activated_at": row["activated_at"],
        }

    def advance_machine(
        self,
        instance_id: str,
        *,
        expected_previous_ts: datetime,
        candle_ts: datetime,
        state: dict[str, Any],
        decisions: list[dict[str, Any]],
    ) -> None:
        """Atomically append one completed day and its immutable decisions."""
        previous = timestamp(expected_previous_ts)
        current = timestamp(candle_ts)
        if current != previous + timedelta(days=1):
            raise ValueError("machine steps must advance exactly one day")
        state_json = encode(state)
        digest = hashlib.sha256(state_json.encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM machine_state WHERE instance_id=?", (instance_id,)
            ).fetchone()
            if not row:
                raise ValueError("machine is not initialized")
            stored_ts = timestamp(row["last_candle_ts"])
            if stored_ts == current:
                if row["state_digest"] != digest:
                    raise ValueError("completed machine state changed")
                self._verify_decisions(db, instance_id, current, decisions)
                return
            if stored_ts != previous:
                raise ValueError("stale or gapped machine writer")
            for decision in decisions:
                self._insert_decision(db, instance_id, current, decision)
            db.execute(
                "UPDATE machine_state SET last_candle_ts=?,state_digest=?,state_json=? WHERE instance_id=?",
                (current.isoformat(), digest, state_json, instance_id),
            )

    @staticmethod
    def _decision_values(
        instance_id: str, candle_ts: datetime, decision: dict[str, Any]
    ) -> tuple[Any, ...]:
        required = {"ts", "kind", "reason", "campaign_id", "k", "side", "price", "metadata"}
        if set(decision) != required:
            raise ValueError("invalid decision shape")
        payload = encode(decision)
        identity = hashlib.sha256(
            encode([instance_id, timestamp(candle_ts).isoformat(), payload]).encode()
        ).hexdigest()
        return (
            identity,
            instance_id,
            timestamp(candle_ts).isoformat(),
            timestamp(decision["ts"]).isoformat(),
            str(decision["kind"]),
            str(decision["reason"]),
            decision["campaign_id"],
            decision["k"],
            decision["side"],
            decision["price"],
            payload,
        )

    @classmethod
    def _insert_decision(
        cls, db: sqlite3.Connection, instance_id: str, candle_ts: datetime, decision: dict[str, Any]
    ) -> None:
        values = cls._decision_values(instance_id, candle_ts, decision)
        old = db.execute(
            "SELECT payload FROM strategy_decisions WHERE id=?", (values[0],)
        ).fetchone()
        if old and old[0] != values[-1]:
            raise ValueError("decision identity conflict")
        if not old:
            db.execute("INSERT INTO strategy_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)", values)

    @classmethod
    def _verify_decisions(
        cls,
        db: sqlite3.Connection,
        instance_id: str,
        candle_ts: datetime,
        decisions: list[dict[str, Any]],
    ) -> None:
        expected = {cls._decision_values(instance_id, candle_ts, value)[0] for value in decisions}
        actual = {
            row[0]
            for row in db.execute(
                "SELECT id FROM strategy_decisions WHERE instance_id=? AND candle_ts=?",
                (instance_id, timestamp(candle_ts).isoformat()),
            )
        }
        if actual != expected:
            raise ValueError("completed decision set changed")

    def import_shadow_observation(
        self, instance_id: str, snapshot: dict[str, Any], *, now: datetime
    ) -> None:
        """Persist immutable research observations, seeding occupancy but never cash.

        Requires daily continuity after the seed; missing observations must be
        replayed individually. A pending exit is still occupied. Research
        snapshots do not grant live order permission. The execution coordinator
        must reconcile campaign termination before acting on a newly flat state.
        No policy for funded add-ons to an unowned parent is implemented here.
        """
        now = timestamp(now)
        if (
            snapshot.get("order_capability") is not False
            or snapshot.get("mode") != "shadow_no_orders"
        ):
            raise ValueError("expected order-incapable shadow observation")
        if snapshot.get("strategy") != "structure_parent_addons_raw":
            raise ValueError("unexpected breakout candidate")
        opened = timestamp(snapshot["as_of_close"])
        closed = timestamp(snapshot["next_open"])
        generated = timestamp(snapshot["generated_at"])
        if closed != opened + timedelta(days=1) or not closed <= generated <= now:
            raise ValueError("forming candle or invalid observation timestamp")
        if closed.hour or closed.minute or closed.second or closed.microsecond:
            raise ValueError("expected UTC daily boundary")
        active = snapshot.get("active_hypothetical_stack")
        if active:
            if timestamp(active["entry_ts"]) >= closed or active["side"] not in ("long", "short"):
                raise ValueError("invalid historical campaign")
            if not active.get("parent_id") or not 1 <= active["active_units"] <= 4:
                raise ValueError("invalid historical units")
        # Generated-at is collection metadata; a retry of identical market
        # evidence must not conflict merely because the job ran again later.
        canonical = {k: v for k, v in snapshot.items() if k != "generated_at"}
        payload = encode(canonical)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            instance = db.execute("SELECT * FROM instances WHERE id=?", (instance_id,)).fetchone()
            if (
                not instance
                or instance["mode"] != "paper"
                or instance["rules_hash"] != snapshot["rules_sha256"]
            ):
                raise ValueError("seed requires a matching paper instance")
            old = db.execute(
                "SELECT digest FROM observations WHERE instance_id=? AND close_ts=?",
                (instance_id, closed.isoformat()),
            ).fetchone()
            if old:
                if old[0] != digest:
                    raise ValueError("observation changed; retain original and investigate")
                return
            previous = db.execute(
                "SELECT close_ts FROM observations WHERE instance_id=? ORDER BY close_ts DESC LIMIT 1",
                (instance_id,),
            ).fetchone()
            if previous and closed != timestamp(previous[0]) + timedelta(days=1):
                raise ValueError("observations must advance one completed day at a time")
            db.execute(
                "INSERT INTO observations VALUES (?,?,?,?)",
                (instance_id, closed.isoformat(), digest, payload),
            )
            if not previous:
                db.execute(
                    "INSERT INTO seed_state VALUES (?,?,?,?)",
                    (
                        instance_id,
                        active["parent_id"] if active else None,
                        int(bool(active)),
                        now.isoformat(),
                    ),
                )


def read_overview(path: Path) -> dict[str, Any]:
    """Read-only dashboard projection. Never creates or migrates a database."""
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        if [r[0] for r in db.execute("SELECT version FROM portfolio_metadata")] != [1]:
            raise ValueError("unsupported portfolio schema")
        strategies = [
            dict(row)
            for row in db.execute(
                "SELECT i.id, i.mode, COALESCE(SUM(CASE WHEN a.kind='realized' THEN a.sats ELSE 0 END),0) realized_sats, "
                "COALESCE(SUM(CASE WHEN a.kind='fee' THEN a.sats ELSE 0 END),0) fee_sats, "
                "COALESCE(SUM(CASE WHEN a.kind='funding' THEN a.sats ELSE 0 END),0) funding_sats, "
                "COALESCE(SUM(a.sats),0) net_sats FROM instances i "
                "LEFT JOIN owned_positions p ON p.instance_id=i.id "
                "LEFT JOIN accounting_events a ON a.position_id=p.id GROUP BY i.id ORDER BY i.id"
            )
        ]
        seeds = [
            dict(row)
            for row in db.execute(
                "SELECT s.*, o.close_ts, o.payload FROM seed_state s JOIN observations o "
                "ON o.instance_id=s.instance_id WHERE o.close_ts=(SELECT MAX(x.close_ts) "
                "FROM observations x WHERE x.instance_id=s.instance_id)"
            )
        ]
        for seed in seeds:
            seed["observation"] = json.loads(seed.pop("payload"))
            seed["parent_occupied"] = bool(seed["observation"].get("active_hypothetical_stack"))
        for strategy in strategies:
            closed = list(
                db.execute(
                    "SELECT p.id,COALESCE(SUM(a.sats),0) net FROM owned_positions p "
                    "JOIN position_closes c ON c.position_id=p.id "
                    "LEFT JOIN accounting_events a ON a.position_id=p.id "
                    "WHERE p.instance_id=? GROUP BY p.id",
                    (strategy["id"],),
                )
            )
            strategy["closed_trades"] = len(closed)
            strategy["winning_trades"] = sum(row["net"] > 0 for row in closed)
        runtimes = [
            {
                **dict(row),
                "state": json.loads(row["state_json"]),
            }
            for row in db.execute(
                "SELECT instance_id,last_candle_ts,state_json,activated_at FROM machine_state ORDER BY instance_id"
            )
        ]
        return {
            "strategies": strategies,
            "seeds": seeds,
            "runtimes": runtimes,
            "order_capability": False,
        }
    finally:
        db.close()
