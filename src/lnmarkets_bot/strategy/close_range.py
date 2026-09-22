"""Pure daily state machine for the frozen BTC close-range challenger.

This module has no exchange, persistence, or order-submission dependency.  It
turns completed UTC daily candles and the following daily open into auditable
decisions.  A coordinator must decide whether a paper action becomes a funded
order and must report actual fills/liquidations back separately.
"""

from __future__ import annotations

import hashlib
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any, ClassVar

DAY = timedelta(days=1)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class DailyCandle:
    """A completed UTC candle identified by its opening timestamp."""

    ts: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        ts = _utc(self.ts)
        for field_name in ("open", "high", "low", "close"):
            object.__setattr__(self, field_name, float(getattr(self, field_name)))
        if ts.hour or ts.minute or ts.second or ts.microsecond:
            raise ValueError("daily candle must start at 00:00 UTC")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("prices must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC candle")
        object.__setattr__(self, "ts", ts)


@dataclass(frozen=True)
class BreakoutCandidate:
    signal_ts: datetime
    side: int
    boundary: float
    signal_close: float
    ema20: float
    atr14: float
    average_overlap10: float
    distance_ema_atr: float
    structure_pass: bool


@dataclass
class CampaignUnit:
    k: int
    entry_ts: datetime
    entry_price: float
    origin: str


@dataclass
class CampaignState:
    campaign_id: str
    side: int
    boundary: float
    entry_ts: datetime
    held_days: int
    peak_favorable: float
    origin: str
    lifetime_units: int
    units: list[CampaignUnit] = field(default_factory=list)


@dataclass(frozen=True)
class BreakoutDecision:
    ts: datetime
    kind: str
    reason: str
    campaign_id: str | None
    k: int | None
    side: int | None
    price: float | None
    metadata: dict[str, Any]


class CloseRangeMachine:
    """Frozen structure-parent/raw-add-on close-range rules.

    Historical occupancy is reconstructed from an explicit seed.  It blocks a
    new parent but is never promoted to an owned trade.  Add-ons to historical
    occupancy are evaluated by the ordinary rules and remain historical/paper
    observations; this class deliberately encodes no funded-add-on policy.
    """

    VERSION: ClassVar[int] = 1
    MIN_HISTORY: ClassVar[int] = 120  # Matches the frozen research feature matrix.
    ENTRY_SLIPPAGE: ClassVar[float] = 0.0005
    STRUCTURE_DISTANCE_MIN: ClassVar[float] = 1.5
    OVERLAP_MAX: ClassVar[float] = 0.55
    MAX_UNITS: ClassVar[int] = 4
    MAX_ADDON_DISPLACEMENT: ClassVar[float] = 0.15
    RECOVERY_START_DAY: ClassVar[int] = 85
    RECOVERY_FRACTION: ClassVar[float] = 0.97
    MAX_HOLD_DAYS: ClassVar[int] = 120

    def __init__(self) -> None:
        self.closes: deque[float] = deque(maxlen=256)
        self.candles: deque[DailyCandle] = deque(maxlen=256)
        self.true_ranges: deque[float] = deque(maxlen=64)
        self.ema20: float | None = None
        self.previous_close: float | None = None
        self.last_bar_ts: datetime | None = None
        self.source_digest = hashlib.sha256(b"").hexdigest()
        self.source_count = 0
        self.campaign: CampaignState | None = None
        self.pending_exit: str | None = None
        self.pending_candidate: BreakoutCandidate | None = None

    def warmup(self, candles: list[DailyCandle]) -> None:
        """Load indicators and retain the latest completed candle's signal.

        Warmup never reconstructs a position.  Retaining only the final signal
        lets a separately seeded campaign apply the correct decision at the
        first open after activation.
        """
        if self.last_bar_ts is not None or self.candles:
            raise ValueError("warmup requires a blank machine")
        for candle in candles:
            self._require_next(candle.ts)
            self.pending_candidate = self._candidate(candle)
            self._append_indicators(candle)
            self.last_bar_ts = _utc(candle.ts)

    def seed_campaign(self, value: dict[str, Any]) -> None:
        """Restore unowned campaign context from a completed research snapshot."""
        if self.campaign is not None:
            raise ValueError("campaign already present")
        entry_ts = _utc(datetime.fromisoformat(str(value["entry_ts"])))
        side = 1 if value["side"] == "long" else -1 if value["side"] == "short" else 0
        units = int(value["active_units"])
        if side == 0 or not 1 <= units <= self.MAX_UNITS:
            raise ValueError("invalid seeded campaign")
        entry_price = float(value["entry_price"])
        if entry_price <= 0 or float(value["boundary"]) <= 0:
            raise ValueError("invalid seeded prices")
        self.campaign = CampaignState(
            campaign_id=str(value["parent_id"]),
            side=side,
            boundary=float(value["boundary"]),
            entry_ts=entry_ts,
            held_days=int(value["held_days"]),
            peak_favorable=float(value["peak_favorable_pct"]),
            origin="historical",
            lifetime_units=units,
            # The compact seed knows only the parent fill. Preserve K occupancy
            # separately instead of inventing child timestamps or prices.
            units=[
                CampaignUnit(k=0, entry_ts=entry_ts, entry_price=entry_price, origin="historical")
            ],
        )
        pending = value.get("pending_exit")
        if pending not in (None, "range_close", "recover", "maximum_hold"):
            raise ValueError("invalid seeded pending exit")
        self.pending_exit = pending

    def advance(self, candle: DailyCandle, *, activation_ts: datetime) -> list[BreakoutDecision]:
        """Apply the open, then evaluate this completed candle for the next open."""
        activation_ts = _utc(activation_ts)
        self._require_next(candle.ts)
        decisions = self._apply_open(_utc(candle.ts), candle.open, activation_ts)
        decisions.extend(self._observe_close(candle))
        return decisions

    def complete_and_apply_next_open(
        self,
        candle: DailyCandle,
        *,
        next_open_ts: datetime,
        next_open_price: float,
        activation_ts: datetime,
    ) -> list[BreakoutDecision]:
        """Evaluate a just-completed candle and act at the live next open.

        A live aggregate becomes available only at its right boundary.  This
        method preserves the research order without waiting for another daily
        candle: evaluate day D, then apply its decision at D+1's first
        executable price.
        """
        activation_ts = _utc(activation_ts)
        next_open_ts = _utc(next_open_ts)
        self._require_next(candle.ts)
        if next_open_ts != candle.ts + DAY:
            raise ValueError("next daily open must immediately follow the completed candle")
        decisions = self._observe_close(candle)
        decisions.extend(self._apply_open(next_open_ts, float(next_open_price), activation_ts))
        return decisions

    def _observe_close(self, candle: DailyCandle) -> list[BreakoutDecision]:
        decisions: list[BreakoutDecision] = []

        candidate = self._candidate(candle)
        if self.campaign is not None:
            campaign = self.campaign
            campaign.held_days += 1
            favorable = candle.high if campaign.side == 1 else candle.low
            parent = campaign.units[0]
            excursion = campaign.side * (favorable - parent.entry_price) / parent.entry_price
            campaign.peak_favorable = max(campaign.peak_favorable, excursion)
            progress = campaign.side * (candle.close - parent.entry_price) / parent.entry_price
            if campaign.side * (candle.close - campaign.boundary) <= 0:
                self.pending_exit = "range_close"
            elif (
                campaign.held_days >= self.RECOVERY_START_DAY
                and campaign.peak_favorable > 0
                and progress >= self.RECOVERY_FRACTION * campaign.peak_favorable
            ):
                self.pending_exit = "recover"
            elif campaign.held_days >= self.MAX_HOLD_DAYS:
                self.pending_exit = "maximum_hold"

        self.pending_candidate = candidate
        self._append_indicators(candle)
        self.last_bar_ts = _utc(candle.ts)
        if candidate is not None:
            decisions.append(
                BreakoutDecision(
                    ts=candle.ts + DAY,
                    kind="signal",
                    reason="structure_pass" if candidate.structure_pass else "structure_reject",
                    campaign_id=self.campaign.campaign_id if self.campaign else None,
                    k=None,
                    side=candidate.side,
                    price=None,
                    metadata=self._candidate_metadata(candidate),
                )
            )
        return decisions

    def confirm_owned_fill(self, *, k: int, ts: datetime, price: float) -> None:
        """Replace a modeled decision price with a confirmed live fill."""
        if self.campaign is None:
            raise ValueError("no active campaign")
        unit = next((value for value in self.campaign.units if value.k == k), None)
        if unit is None:
            raise ValueError("campaign unit is absent")
        unit.entry_ts = _utc(ts)
        unit.entry_price = float(price)
        unit.origin = "live"
        if k == 0:
            self.campaign.entry_ts = _utc(ts)
            self.campaign.origin = "live"

    def parent_liquidated(self, ts: datetime, price: float) -> BreakoutDecision:
        """Apply an externally observed parent liquidation to campaign occupancy."""
        if self.campaign is None:
            raise ValueError("no active campaign")
        campaign = self.campaign
        self.campaign = None
        self.pending_exit = None
        return BreakoutDecision(
            ts=_utc(ts),
            kind="campaign_exit",
            reason="parent_liquidation",
            campaign_id=campaign.campaign_id,
            k=0,
            side=campaign.side,
            price=price,
            metadata={"origin": campaign.origin, "owned": campaign.origin != "historical"},
        )

    def child_liquidated(self, *, k: int, ts: datetime, price: float) -> BreakoutDecision:
        """Remove one liquidated add-on without replenishing its lifetime slot."""
        if self.campaign is None or k <= 0:
            raise ValueError("no active child campaign unit")
        unit = next((value for value in self.campaign.units if value.k == k), None)
        if unit is None:
            raise ValueError("campaign child is absent")
        self.campaign.units.remove(unit)
        return BreakoutDecision(
            ts=_utc(ts),
            kind="unit_exit",
            reason="child_liquidation",
            campaign_id=self.campaign.campaign_id,
            k=k,
            side=self.campaign.side,
            price=float(price),
            metadata={"origin": unit.origin, "owned": unit.origin == "live"},
        )

    def persistent_state(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "closes": list(self.closes),
            "candles": [self._candle_dict(value) for value in self.candles],
            "true_ranges": list(self.true_ranges),
            "ema20": self.ema20,
            "previous_close": self.previous_close,
            "last_bar_ts": self.last_bar_ts.isoformat() if self.last_bar_ts else None,
            "source_digest": self.source_digest,
            "source_count": self.source_count,
            "campaign": self._campaign_dict(self.campaign) if self.campaign else None,
            "pending_exit": self.pending_exit,
            "pending_candidate": (
                self._candidate_dict(self.pending_candidate) if self.pending_candidate else None
            ),
        }

    @classmethod
    def restore(cls, state: dict[str, Any]) -> CloseRangeMachine:
        if state.get("version") != cls.VERSION:
            raise ValueError("unsupported close-range state version")
        result = cls()
        result.closes = deque((float(value) for value in state["closes"]), maxlen=256)
        result.candles = deque(
            (DailyCandle(**cls._restore_times(value, "ts")) for value in state["candles"]),
            maxlen=256,
        )
        result.true_ranges = deque((float(value) for value in state["true_ranges"]), maxlen=64)
        result.ema20 = float(state["ema20"]) if state["ema20"] is not None else None
        result.previous_close = (
            float(state["previous_close"]) if state["previous_close"] is not None else None
        )
        result.last_bar_ts = (
            _utc(datetime.fromisoformat(state["last_bar_ts"])) if state["last_bar_ts"] else None
        )
        result.source_digest = str(state["source_digest"])
        result.source_count = int(state["source_count"])
        if state["campaign"]:
            value = state["campaign"]
            result.campaign = CampaignState(
                campaign_id=str(value["campaign_id"]),
                side=int(value["side"]),
                boundary=float(value["boundary"]),
                entry_ts=_utc(datetime.fromisoformat(value["entry_ts"])),
                held_days=int(value["held_days"]),
                peak_favorable=float(value["peak_favorable"]),
                origin=str(value["origin"]),
                lifetime_units=int(value.get("lifetime_units", len(value["units"]))),
                units=[
                    CampaignUnit(
                        k=int(unit["k"]),
                        entry_ts=_utc(datetime.fromisoformat(unit["entry_ts"])),
                        entry_price=float(unit["entry_price"]),
                        origin=str(unit["origin"]),
                    )
                    for unit in value["units"]
                ],
            )
        result.pending_exit = state["pending_exit"]
        if state["pending_candidate"]:
            result.pending_candidate = BreakoutCandidate(
                **cls._restore_times(state["pending_candidate"], "signal_ts")
            )
        result._validate_restored()
        return result

    def _apply_open(
        self, ts: datetime, raw_open: float, activation_ts: datetime
    ) -> list[BreakoutDecision]:
        decisions: list[BreakoutDecision] = []
        if self.campaign is not None and self.pending_exit is not None:
            campaign = self.campaign
            decisions.append(
                BreakoutDecision(
                    ts=ts,
                    kind="historical_exit" if campaign.origin == "historical" else "campaign_exit",
                    reason=self.pending_exit,
                    campaign_id=campaign.campaign_id,
                    k=None,
                    side=campaign.side,
                    price=raw_open,
                    metadata={"origin": campaign.origin, "owned": campaign.origin != "historical"},
                )
            )
            self.campaign = None
            self.pending_exit = None

        candidate = self.pending_candidate
        self.pending_candidate = None
        if candidate is None:
            return decisions
        meta = self._candidate_metadata(candidate)
        if self.campaign is None:
            if not candidate.structure_pass:
                decisions.append(self._reject(ts, "parent_structure", candidate, meta))
                return decisions
            origin = "paper" if ts >= activation_ts else "historical"
            price = raw_open * (1 + candidate.side * self.ENTRY_SLIPPAGE)
            campaign_id = ts.strftime("%Y%m%d") + ("L" if candidate.side == 1 else "S")
            self.campaign = CampaignState(
                campaign_id=campaign_id,
                side=candidate.side,
                boundary=candidate.boundary,
                entry_ts=ts,
                held_days=0,
                peak_favorable=0.0,
                origin=origin,
                lifetime_units=1,
                units=[CampaignUnit(k=0, entry_ts=ts, entry_price=price, origin=origin)],
            )
            decisions.append(
                BreakoutDecision(
                    ts=ts,
                    kind="paper_parent" if origin == "paper" else "historical_parent",
                    reason="structure_parent",
                    campaign_id=campaign_id,
                    k=0,
                    side=candidate.side,
                    price=price,
                    metadata={**meta, "owned": False},
                )
            )
            return decisions

        campaign = self.campaign
        if candidate.side != campaign.side:
            decisions.append(self._reject(ts, "occupied_opposite", candidate, meta))
        elif campaign.lifetime_units >= self.MAX_UNITS:
            decisions.append(self._reject(ts, "addon_cap", candidate, meta))
        else:
            price = raw_open * (1 + campaign.side * self.ENTRY_SLIPPAGE)
            displacement = campaign.side * (price / campaign.units[0].entry_price - 1)
            meta["entry_displacement"] = displacement
            if campaign.side * (raw_open - campaign.boundary) <= 0:
                decisions.append(self._reject(ts, "addon_parent_boundary", candidate, meta))
            elif displacement > self.MAX_ADDON_DISPLACEMENT + 1e-12:
                decisions.append(self._reject(ts, "addon_distance", candidate, meta))
            else:
                origin = "paper" if ts >= activation_ts else "historical"
                k = campaign.lifetime_units
                campaign.lifetime_units += 1
                campaign.units.append(
                    CampaignUnit(k=k, entry_ts=ts, entry_price=price, origin=origin)
                )
                decisions.append(
                    BreakoutDecision(
                        ts=ts,
                        kind="paper_addon" if origin == "paper" else "historical_addon",
                        reason="raw_same_side_addon",
                        campaign_id=campaign.campaign_id,
                        k=k,
                        side=campaign.side,
                        price=price,
                        metadata={**meta, "owned": False},
                    )
                )
        return decisions

    def _candidate(self, candle: DailyCandle) -> BreakoutCandidate | None:
        if len(self.candles) < self.MIN_HISTORY or len(self.true_ranges) < 14 or self.ema20 is None:
            return None
        previous20 = list(self.closes)[-20:]
        upper, lower = max(previous20), min(previous20)
        side = 1 if candle.close > upper else -1 if candle.close < lower else 0
        if side == 0:
            return None
        atr = sum(list(self.true_ranges)[-14:]) / 14
        if atr <= 0:
            return None
        previous10 = list(self.candles)[-10:]
        overlaps = []
        for left, right in pairwise(previous10):
            overlap = max(0.0, min(left.high, right.high) - max(left.low, right.low))
            union = max(left.high, right.high) - min(left.low, right.low)
            overlaps.append(overlap / union if union else 0.0)
        average_overlap = sum(overlaps) / len(overlaps)
        boundary = upper if side == 1 else lower
        distance = side * (candle.close - self.ema20) / atr
        return BreakoutCandidate(
            signal_ts=_utc(candle.ts),
            side=side,
            boundary=boundary,
            signal_close=candle.close,
            ema20=self.ema20,
            atr14=atr,
            average_overlap10=average_overlap,
            distance_ema_atr=distance,
            structure_pass=distance >= self.STRUCTURE_DISTANCE_MIN
            and average_overlap <= self.OVERLAP_MAX,
        )

    def _append_indicators(self, candle: DailyCandle) -> None:
        if self.previous_close is not None:
            self.true_ranges.append(
                max(
                    candle.high - candle.low,
                    abs(candle.high - self.previous_close),
                    abs(candle.low - self.previous_close),
                )
            )
        self.ema20 = (
            candle.close
            if self.ema20 is None
            else ((2 / 21) * candle.close + (19 / 21) * self.ema20)
        )
        self.previous_close = candle.close
        self.closes.append(candle.close)
        self.candles.append(candle)
        encoded = json.dumps(
            [candle.ts.isoformat(), candle.open, candle.high, candle.low, candle.close],
            separators=(",", ":"),
            allow_nan=False,
        )
        self.source_digest = hashlib.sha256(
            bytes.fromhex(self.source_digest) + encoded.encode()
        ).hexdigest()
        self.source_count += 1

    def _require_next(self, ts: datetime) -> None:
        ts = _utc(ts)
        if self.last_bar_ts is not None and ts != self.last_bar_ts + DAY:
            raise ValueError("daily candles must be unique and contiguous")

    def _validate_restored(self) -> None:
        if len(self.closes) != len(self.candles) or len(self.candles) > 256:
            raise ValueError("corrupt indicator history")
        if self.source_count < len(self.candles) or len(self.source_digest) != 64:
            raise ValueError("corrupt source history")
        if self.candles and self.last_bar_ts != self.candles[-1].ts:
            raise ValueError("last bar does not match history")
        if self.campaign:
            if (
                self.campaign.side not in (-1, 1)
                or not 1 <= self.campaign.lifetime_units <= 4
                or not 1 <= len(self.campaign.units) <= self.campaign.lifetime_units
            ):
                raise ValueError("corrupt campaign")
            keys = [unit.k for unit in self.campaign.units]
            if (
                keys[0] != 0
                or keys != sorted(set(keys))
                or keys[-1] >= self.campaign.lifetime_units
            ):
                raise ValueError("corrupt campaign units")

    @staticmethod
    def _reject(
        ts: datetime, reason: str, candidate: BreakoutCandidate, metadata: dict[str, Any]
    ) -> BreakoutDecision:
        return BreakoutDecision(ts, "reject", reason, None, None, candidate.side, None, metadata)

    @staticmethod
    def _candidate_metadata(value: BreakoutCandidate) -> dict[str, Any]:
        return {
            "signal_ts": value.signal_ts.isoformat(),
            "boundary": value.boundary,
            "signal_close": value.signal_close,
            "ema20": value.ema20,
            "atr14": value.atr14,
            "average_overlap10": value.average_overlap10,
            "distance_ema_atr": value.distance_ema_atr,
            "structure_pass": value.structure_pass,
        }

    @staticmethod
    def _candle_dict(value: DailyCandle) -> dict[str, Any]:
        result = asdict(value)
        result["ts"] = value.ts.isoformat()
        return result

    @staticmethod
    def _candidate_dict(value: BreakoutCandidate) -> dict[str, Any]:
        result = asdict(value)
        result["signal_ts"] = value.signal_ts.isoformat()
        return result

    @staticmethod
    def _campaign_dict(value: CampaignState) -> dict[str, Any]:
        return {
            **asdict(value),
            "entry_ts": value.entry_ts.isoformat(),
            "units": [
                {**asdict(unit), "entry_ts": unit.entry_ts.isoformat()} for unit in value.units
            ],
        }

    @staticmethod
    def _restore_times(value: dict[str, Any], field_name: str) -> dict[str, Any]:
        result = dict(value)
        result[field_name] = _utc(datetime.fromisoformat(result[field_name]))
        return result
