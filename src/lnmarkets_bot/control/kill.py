"""Kill switch.

Two trigger surfaces, both checked:
  - env var `TRADINGBOT_HALTED=1`
  - halt file at `TRADINGBOT_HALT_FILE` (or `cfg.halt_file`)

Trips both inside the running process (via setter on a long-running scenario,
or via env var set by the parent process). The `cached_for_seconds` arg
avoids hammering the filesystem on every engine tick.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass

from ..config import BotConfig


@dataclass
class KillSwitch:
    cfg: BotConfig
    _last_check: float = 0.0
    _cached_halted: bool = False
    _cache_ttl: float = 1.0  # seconds

    def is_halted(self) -> bool:
        now = time.monotonic()
        if now - self._last_check < self._cache_ttl:
            return self._cached_halted
        self._last_check = now
        env = os.environ.get("TRADINGBOT_HALTED", "") or self.cfg.halted
        env_halted = env.strip() in ("1", "true", "yes", "on")
        file_halted = False
        if self.cfg.halt_file is not None:
            file_halted = self.cfg.halt_file.exists()
        # Also check the literal env var (env on the process overrides cfg).
        if os.environ.get("TRADINGBOT_HALTED", "").strip() in ("1", "true", "yes", "on"):
            env_halted = True
        self._cached_halted = env_halted or file_halted
        return self._cached_halted

    def reset_cache(self) -> None:
        self._last_check = 0.0
        self._cached_halted = False

    async def wait_if_halted(self, poll_every: float = 0.5) -> None:
        """If halted, sleep until no longer halted. (Not used in v0 but useful for hot-loops.)"""
        while self.is_halted():
            await asyncio.sleep(poll_every)
