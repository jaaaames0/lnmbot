"""Run lifecycle helpers.

`RunSession` is a context manager that:
  - opens/closes a DB row for the run
  - installs SIGINT/SIGTERM handlers that cleanly set a stop flag
  - yields a `RunHandle` with run_id and a `should_stop()` predicate

This is the only signal-handling code in the bot. It MUST run in the main
thread (Python signal rules).
"""

from __future__ import annotations

import logging
import signal
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..time import now_utc

if TYPE_CHECKING:
    from datetime import datetime

    from ..config import BotConfig
    from ..persistence.recorder import Recorder


_log = logging.getLogger("lnmarkets_bot.lifecycle")


@dataclass
class RunHandle:
    run_id: int
    started_at: datetime
    stop_now: bool = False
    halt_reason: str | None = None

    def should_stop(self) -> bool:
        return self.stop_now


@contextmanager
def run_session(
    recorder: Recorder,
    *,
    cfg: BotConfig,
    mode: str,
    strategy_name: str,
    strategy_params: dict[str, Any] | None = None,
    notes: str | None = None,
    install_signal_handlers: bool = True,
):
    """Context manager wrapping a single backtest/live run.

    Usage:
        with run_session(rec, cfg=cfg, mode='backtest', strategy_name='DoNothing') as run:
            ...
            if run.should_stop(): break
    """
    strategy_params = strategy_params or {}
    run_id = recorder.start_run(
        mode=mode,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        config=cfg.model_dump(mode="json"),
        started_at=now_utc(),
        notes=notes,
    )
    handle = RunHandle(run_id=run_id, started_at=now_utc())

    if install_signal_handlers:
        prev_handlers: dict[int, Any] = {}

        def _handler(signum: int, _frame: Any) -> None:
            signame = signal.Signals(signum).name
            _log.warning("run.signal_received run_id=%d signal=%s", run_id, signame)
            handle.stop_now = True
            handle.halt_reason = f"signal:{signame}"

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                prev_handlers[sig] = signal.signal(sig, _handler)
            except ValueError:
                # Not in main thread.
                _log.warning(
                    "run.cannot_install_signal_handlers run_id=%d signal=%s",
                    run_id,
                    signal.Signals(sig).name,
                )

    status = "done"
    try:
        yield handle
    except BaseException:
        status = "error"
        raise
    finally:
        # Restoration of prior handlers is best-effort; not strictly required
        # since the process is typically the only thing in this thread.
        if install_signal_handlers:
            for sig, prev in prev_handlers.items():
                with suppress(Exception):
                    signal.signal(sig, prev)
        if handle.stop_now:
            status = "halted"
        recorder.end_run(run_id, status=status, ended_at=now_utc())
