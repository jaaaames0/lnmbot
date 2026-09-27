"""Rebuild compact historical occupancy from verified units and settlements.

No funded orders, account balances or P&L records are touched. The monotone
collateral check proves survival without inventing an OHLC path. If an adverse
bar crosses the final (most adverse) threshold, finer event data is required.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import isclose, isfinite

from .close_range import CampaignUnit, CloseRangeMachine


def hydrate_historical(
    machine: CloseRangeMachine, reference: dict, settlements: list[tuple[datetime, float, float]]
) -> None:
    campaign = machine.campaign
    if campaign is None or campaign.origin != "historical":
        return
    if (
        campaign.campaign_id != reference["campaign_id"]
        or not isclose(campaign.boundary, float(reference["boundary"]))
        or campaign.entry_ts != datetime.fromisoformat(reference["entry_ts"]).astimezone(UTC)
    ):
        raise ValueError("historical reference does not match stored campaign")
    if campaign.lifetime_units != len(reference["units"]):
        raise ValueError("historical reference lifetime count differs")
    if machine.last_bar_ts is None:
        raise ValueError("historical source end missing")
    end = machine.last_bar_ts + timedelta(days=1)
    rows = sorted(
        {
            stamp: (rate, fixing)
            for stamp, rate, fixing in settlements
            if campaign.entry_ts < stamp < end
        }.items()
    )
    expected = campaign.entry_ts + timedelta(hours=8)
    for stamp, (rate, fixing) in rows:
        if stamp != expected or fixing <= 0 or not isfinite(fixing) or not isfinite(rate):
            raise ValueError("historical funding source gap or invalid fixing")
        expected += timedelta(hours=8)
    if expected != end:
        raise ValueError("historical funding source incomplete")
    evidence = [c for c in machine.candles if campaign.entry_ts <= c.ts < end]
    if (
        not evidence
        or evidence[0].ts != campaign.entry_ts
        or evidence[-1].ts + timedelta(days=1) != end
    ):
        raise ValueError("historical candle source truncated before campaign")
    units = []
    for raw in reference["units"]:
        entry = datetime.fromisoformat(raw["entry_ts"]).astimezone(UTC)
        price = float(raw["entry_price"])
        margin = 1 / price / 5
        margin += sum(
            min(-campaign.side * rate / fixing, 0.0)
            for stamp, (rate, fixing) in rows
            if entry < stamp
        )
        denominator = margin + 1 / price if campaign.side == 1 else 1 / price - margin
        if denominator <= 0:
            raise ValueError("historical unit exhausted collateral")
        level = (1 + campaign.side * 0.001) / denominator
        relevant = [c for c in evidence if c.ts >= entry]
        if any(c.low <= level if campaign.side == 1 else c.high >= level for c in relevant):
            raise ValueError("historical survival needs finer chronological liquidation replay")
        units.append(CampaignUnit(int(raw["k"]), entry, price, "historical", margin))
    if [u.k for u in units] != list(range(campaign.lifetime_units)):
        raise ValueError("historical reference unit identities invalid")
    campaign.units = units
    machine.last_historical_funding_ts = rows[-1][0]
    machine.last_historical_price_ts = end
    machine.historical_model_complete = True
    machine.historical_funding_available = True
