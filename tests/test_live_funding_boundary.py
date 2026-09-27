"""Funding must include the boundary before modeled price observation."""

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_inclusive_model_boundary_with_exclusive_venue_end(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "runner_boundary", Path(__file__).resolve().parents[1] / "scripts/run_live.py"
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    end = datetime(2026, 9, 27, 8, tzinfo=UTC)
    start = end - timedelta(hours=16)
    queries = []

    async def venue(_self, *, from_ts, to_ts):
        queries.append((from_ts, to_ts))
        # Retain a malformed extra future row to prove it cannot be applied.
        stamps = [end - timedelta(hours=8), end, end + timedelta(hours=8)]
        rows = [
            {"time": ts.isoformat(), "fundingRate": "0.0001", "fixingPrice": "75000"}
            for ts in stamps if ts < to_ts
        ]
        rows.append({"time": (end + timedelta(hours=8)).isoformat(), "fundingRate": "0.0001", "fixingPrice": "75000"})
        return rows

    monkeypatch.setattr(runner.MarketApi, "funding_settlements", venue)
    rows = await runner._historical_funding(None, start, end)
    assert queries == [(start, end + timedelta(seconds=1))]
    assert [row[0] for row in rows] == [end - timedelta(hours=8), end]
    assert all(row[1:] == (0.0001, 75000.0) for row in rows)
