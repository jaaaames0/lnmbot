"""Regression tests for caller-supplied live persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lnmarkets_bot.engine import live
from lnmarkets_bot.strategy import DoNothing

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lnmarkets_bot.config import BotConfig
    from lnmarkets_bot.persistence.recorder import Recorder
    from lnmarkets_bot.strategy import Bar


class _EmptyDataSource:
    async def stream(self) -> AsyncIterator[Bar]:
        if False:
            yield


@pytest.mark.asyncio
async def test_recorder_override_skips_redundant_database_initialization(
    monkeypatch: pytest.MonkeyPatch,
    cfg: BotConfig,
    recorder: Recorder,
) -> None:
    def unexpected_make_engine(*_args: object, **_kwargs: object) -> None:
        pytest.fail("run_paper must not initialize a second engine with recorder_override")

    monkeypatch.setattr(live, "make_engine", unexpected_make_engine)

    run_id = await live.run_paper(
        cfg=cfg,
        data_source=_EmptyDataSource(),
        strategy=DoNothing(),
        install_signal_handlers=False,
        recorder_override=recorder,
    )

    assert run_id > 0
