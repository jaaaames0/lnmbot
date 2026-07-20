"""Kill switch — env var + halt file + cache TTL."""
from __future__ import annotations

import os
from pathlib import Path

from lnmarkets_bot.config import BotConfig
from lnmarkets_bot.control.kill import KillSwitch


def _cfg_with(halt_file: Path | None) -> BotConfig:
    return BotConfig(
        storage_db_path=Path("/tmp/_unused.sqlite"),
        halt_file=halt_file,
    )


def test_default_is_not_halted() -> None:
    ks = KillSwitch(cfg=_cfg_with(None))
    ks.reset_cache()
    assert not ks.is_halted()


def test_env_var_halts(monkeypatch: object) -> None:
    monkeypatch.setenv("TRADINGBOT_HALTED", "1")  # type: ignore[attr-defined]
    ks = KillSwitch(cfg=_cfg_with(None))
    ks.reset_cache()
    assert ks.is_halted()


def test_halt_file_halts(tmp_path: Path) -> None:
    flag = tmp_path / "HALT"
    flag.write_text("")  # presence is what matters
    ks = KillSwitch(cfg=_cfg_with(flag))
    ks.reset_cache()
    assert ks.is_halted()


def test_cache_ttl_does_not_stale_halt(tmp_path: Path) -> None:
    """Once the TTL elapses, a newly created halt file is picked up."""
    flag = tmp_path / "HALT"
    ks = KillSwitch(cfg=_cfg_with(flag))
    ks._cache_ttl = 0.0
    assert not ks.is_halted()
    flag.write_text("")
    assert ks.is_halted()
