"""Command-line interface.

Subcommands:
    backtest   — historical replay
    paper      — dry-run live (mock data source when no LNM creds)
    run-live   — DISABLED in v0; explicit gate before enabling
    status     — summary of the most recent run
    db-stats   — counts per table

All commands wire config + logging + recorder consistently. The CLI is
intentionally a thin shell over the engines — no business logic here.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy import func, select

from .config import BotConfig, load_config
from .data import BacktestReplay, MockLiveStream
from .logging import configure_logging, get_logger
from .persistence.db import make_engine, init_schema, make_session_factory
from .persistence.models import (
    account_snapshots,
    bars,
    daily_pnl,
    fills,
    orders,
    risk_events,
    runs,
    signals,
)
from .persistence.recorder import Recorder
from .strategy import import_strategy

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


def _init_runtime(env_file: Path | None) -> BotConfig:
    cfg = load_config(env_file=str(env_file) if env_file else None)
    configure_logging(cfg.storage_log_level, cfg.storage_log_path)
    return cfg


def _load_strategy(cfg: BotConfig):
    log = get_logger("cli")
    try:
        strat = import_strategy(cfg.strategy)
    except Exception as exc:  # noqa: BLE001
        log.error("cli.strategy_load_failed", spec=cfg.strategy, error=str(exc))
        raise typer.Exit(code=2) from exc
    return strat


@app.command()
def backtest(
    data: Path = typer.Option(..., help="Parquet file with OHLCV bars"),
    cadence: str = typer.Option("fast", help="Replay speed: instant|fast|realtime"),
    env_file: Path | None = typer.Option(None, "--env-file", help="Path to .env (default: project .env)"),
) -> None:
    """Run a historical backtest."""
    cfg = _init_runtime(env_file)
    strat = _load_strategy(cfg)

    if not data.exists():
        console.print(f"[red]data file not found:[/red] {data}")
        raise typer.Exit(code=2)

    if cadence == "instant":
        replay = BacktestReplay(data, cadence="instant")
    elif cadence == "fast":
        replay = BacktestReplay(data, cadence="fast")
    elif cadence == "realtime":
        replay = BacktestReplay(data, cadence="realtime")
    else:
        console.print(f"[red]cadence must be one of instant|fast|realtime; got {cadence!r}[/red]")
        raise typer.Exit(code=2)

    from .engine.backtest import run_backtest

    run_id = asyncio.run(
        run_backtest(
            cfg=cfg,
            data_source=replay,
            strategy=strat,
            install_signal_handlers=False,
        )
    )
    console.print(f"[green]backtest complete[/green] run_id={run_id} db={cfg.storage_db_path}")


@app.command()
def paper(
    data: Path | None = typer.Option(
        None, help="Parquet file used to drive a MockLiveStream (when LNM creds unavailable)"
    ),
    duration: str = typer.Option("30s", help="Bounded run length, e.g. 30s, 2m"),
    env_file: Path | None = typer.Option(None, "--env-file"),
) -> None:
    """Run a dry-run live paper session. Default data source is the MockLiveStream."""
    cfg = _init_runtime(env_file)
    strat = _load_strategy(cfg)
    duration_seconds = _parse_duration(duration)

    if cfg.has_credentials():
        console.print("[yellow]note:[/yellow] LNM credentials present, but v0 live-stream is not yet wired. Using MockLiveStream.")

    if data is None:
        # Default: look next to cfg.backtest_data_path
        data = cfg.backtest_data_path
    if not data.exists():
        console.print(f"[red]no data file for mock stream:[/red] {data}")
        console.print("hint: backfill from Binance first, e.g.:")
        console.print(
            f"  python scripts/backfill_binance.py --start $(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%SZ') --end $(date -u '+%Y-%m-%dT%H:%M:%SZ') --output {data}"
        )
        raise typer.Exit(code=2)

    stream = MockLiveStream(data, seconds_per_bar=_effective_paper_bar_seconds(duration_seconds), loop_forever=False)
    from .engine.live import run_paper
    run_id = asyncio.run(
        run_paper(
            cfg=cfg,
            data_source=stream,
            strategy=strat,
            duration_seconds=duration_seconds,
            install_signal_handlers=False,
        )
    )
    console.print(f"[green]paper session complete[/green] run_id={run_id} db={cfg.storage_db_path}")


@app.command()
def run_live(
    env_file: Path | None = typer.Option(None, "--env-file"),
) -> None:
    """Run with real orders against LNM. DISABLED in v0.

    Enabled only after explicit user approval. The CLI surface is here so we
    don't have to revisit the dispatch table when we enable it.
    """
    console.print("[red]run-live is disabled in v0.[/red]")
    console.print("Maintenance flag: TRADINGBOT_ENABLE_LIVE must be set to 1 AND the user must explicitly enable.")
    if not os.environ.get("TRADINGBOT_ENABLE_LIVE"):
        raise typer.Exit(code=3)
    console.print("[red]run-live is not yet implemented; abort.[/red]")
    raise typer.Exit(code=3)


@app.command()
def status(
    env_file: Path | None = typer.Option(None, "--env-file"),
    last: int = typer.Option(5, help="Number of recent runs to show"),
) -> None:
    """Show recent runs + a summary of the last one."""
    cfg = _init_runtime(env_file)
    if not cfg.storage_db_path.exists():
        console.print(f"[yellow]no DB at {cfg.storage_db_path}[/yellow]")
        raise typer.Exit(code=0)

    eng = make_engine(cfg.storage_db_path)
    fac = make_session_factory(eng)
    with fac() as s:
        rows = s.execute(
            select(runs).order_by(runs.c.started_at.desc()).limit(last)
        ).fetchall()
        if not rows:
            console.print("[yellow]no runs in DB yet[/yellow]")
            return
        for r in rows:
            console.print(
                f"run {r.id:>4}  {r.mode:<8} {r.strategy_name:<40} "
                f"{r.status:<8} started={r.started_at.isoformat()} ended={r.ended_at.isoformat() if r.ended_at else '—'}"
            )


@app.command()
def db_stats(
    env_file: Path | None = typer.Option(None, "--env-file"),
) -> None:
    """Print row counts per table — sanity check for the persistence layer."""
    cfg = _init_runtime(env_file)
    eng = make_engine(cfg.storage_db_path)
    init_schema(eng)
    fac = make_session_factory(eng)
    tables = [
        ("runs", runs),
        ("bars", bars),
        ("signals", signals),
        ("orders", orders),
        ("fills", fills),
        ("account_snapshots", account_snapshots),
        ("daily_pnl", daily_pnl),
        ("risk_events", risk_events),
    ]
    with fac() as s:
        for name, t in tables:
            n = s.execute(select(func.count()).select_from(t)).scalar()
            console.print(f"{name:<22} {n}")


def _parse_duration(s: str) -> float:
    """Parse '30s', '2m', '1h' to seconds."""
    if not s:
        return 30.0
    unit = s[-1]
    n = float(s[:-1])
    if unit == "s":
        return n
    if unit == "m":
        return n * 60
    if unit == "h":
        return n * 3600
    raise typer.BadParameter(f"duration must end in s|m|h; got {s!r}")


def _effective_paper_bar_seconds(duration_seconds: float) -> float:
    """Aim for ~10-30 bars over the duration; default 1.0s per bar."""
    # Pick a per-bar interval that yields ~10 bars total so the demo is meaningful.
    target_bars = 20
    return max(0.05, duration_seconds / max(1, target_bars))


if __name__ == "__main__":
    app()
