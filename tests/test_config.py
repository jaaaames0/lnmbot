"""Configuration defaults and network endpoint selection."""

from __future__ import annotations

from lnmarkets_bot.config import BotConfig, Network, load_config


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
