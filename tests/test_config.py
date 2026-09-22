"""Configuration defaults and network endpoint selection."""

from __future__ import annotations

from sqlalchemy import select

from lnmarkets_bot.config import BotConfig, Network, load_config
from lnmarkets_bot.control.lifecycle import run_session
from lnmarkets_bot.persistence.db import init_schema, make_engine, make_session_factory
from lnmarkets_bot.persistence.models import runs
from lnmarkets_bot.persistence.recorder import Recorder


def test_signet_endpoints_are_the_testnet_defaults():
    cfg = BotConfig(lnm_network=Network.TESTNET)

    assert cfg.effective_base_url() == "https://api.signet.lnmarkets.com/v3"
    assert cfg.effective_ws_url() == "wss://stream.signet.lnmarkets.com/v1"


def test_live_sizing_settings_load_from_environment_file(tmp_path):
    env_file = tmp_path / "sizing.env"
    env_file.write_text(
        "\n".join(
            (
                "SIZING_MODE=equity_fraction",
                "SIZING_LEVERAGE=2",
                "SIZING_TOTAL_MARGIN_FRACTION=0.4",
                'SIZING_TIMEFRAME_WEIGHTS={"1d": 0.6, "4h": 0.4}',
                "RISK_MAX_TOTAL_NOTIONAL_USD=100",
                "STRATEGY_4H_CHOP_REDUCE_ENABLED=true",
                "STRATEGY_CHOP_HIGH_SIZE_MULTIPLIER=0.5",
            )
        )
    )

    cfg = load_config(env_file=env_file)

    assert cfg.sizing_mode == "equity_fraction"
    assert cfg.sizing_leverage == 2.0
    assert cfg.sizing_timeframe_weights == {"1d": 0.6, "4h": 0.4}
    assert cfg.risk_max_total_notional_usd == 100.0
    assert cfg.strategy_4h_chop_reduce_enabled is True
    assert cfg.strategy_chop_high_size_multiplier == 0.5


def test_blank_optional_paths_are_disabled_not_current_directory(tmp_path):
    env_file = tmp_path / "blank-paths.env"
    env_file.write_text("STORAGE_LOG_PATH=\nHALT_FILE=\n")

    cfg = load_config(env_file=env_file)

    assert cfg.storage_log_path is None
    assert cfg.halt_file is None


def test_run_audit_metadata_excludes_api_credentials(tmp_path):
    db_path = tmp_path / "audit.sqlite"
    engine = make_engine(db_path)
    init_schema(engine)
    factory = make_session_factory(engine)
    recorder = Recorder(factory)
    cfg = BotConfig(
        lnm_access_key="key",
        lnm_access_secret="secret",
        lnm_access_passphrase="passphrase",
        storage_db_path=db_path,
    )

    with run_session(
        recorder,
        cfg=cfg,
        mode="live",
        strategy_name="test",
        install_signal_handlers=False,
    ):
        pass

    with factory() as session:
        config = session.execute(select(runs.c.config_json)).scalar_one()
    assert "lnm_access_key" not in config
    assert "lnm_access_secret" not in config
    assert "lnm_access_passphrase" not in config
    assert config["storage_db_path"] == str(db_path)
