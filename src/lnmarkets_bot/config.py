"""Configuration loaded from environment / .env.

Flat shape so env-var names match field names 1:1 (after lowercasing).
"""

from __future__ import annotations

from datetime import UTC, datetime  # noqa: F401  (re-exported for sub-modules)
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Network(StrEnum):
    TESTNET = "testnet"
    MAINNET = "mainnet"


_BASE_URLS: dict[Network, str] = {
    Network.TESTNET: "https://api.signet.lnmarkets.com/v3",
    Network.MAINNET: "https://api.lnmarkets.com/v3",
}

_WS_URLS: dict[Network, str] = {
    Network.TESTNET: "wss://stream.signet.lnmarkets.com/v1",
    Network.MAINNET: "wss://stream.lnmarkets.com/v1",
}


class BotConfig(BaseSettings):
    """All config in one place. Environment variables are flat (no nesting).

    For grouping clarity fields are named `lnm_*`, `risk_*`, `storage_*`,
    `backtest_*`. Field names map to env vars with the same spelling (case
    insensitive) — e.g. `lnm_network` ⇄ `LNM_NETWORK`.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- LN Markets connection ---
    lnm_network: Network = Field(default=Network.TESTNET)
    lnm_base_url: str | None = None
    lnm_ws_url: str | None = None
    lnm_access_key: str = ""
    lnm_access_secret: str = ""
    lnm_access_passphrase: str = ""

    # --- Hard risk limits (cannot be bypassed by strategy code) ---
    risk_max_position_usd: float = 1000.0
    risk_max_leverage: float = 10.0
    risk_max_daily_loss_usd: float = 200.0
    risk_max_orders_per_minute: int = 10
    risk_max_total_notional_usd: float | None = None
    risk_max_total_margin_usd: float | None = None

    # --- Storage ---
    storage_db_path: Path = Path("./lnmarkets.sqlite")
    storage_log_path: Path | None = None
    storage_log_level: str = "INFO"

    # --- Backtest defaults ---
    backtest_data_path: Path = Path("./data/cache/btcusdt_perp_1m.parquet")
    backtest_funding_rate_per_8h: float = 0.0001
    backtest_resolution: str = "1m"

    # --- Strategy runtime ---
    strategy: str = "lnmarkets_bot.strategy.do_nothing:DoNothing"
    initial_balance_usd: float = 10_000.0  # backtest/paper starting collateral, USD

    # --- Live sizing policy ---
    sizing_mode: Literal["fixed_notional", "equity_fraction"] = "fixed_notional"
    sizing_fixed_notional_usd: float = 1.0
    sizing_leverage: float = 1.0
    sizing_total_margin_fraction: float = 0.50
    sizing_timeframe_weights: dict[str, float] = Field(
        default_factory=lambda: {"1d": 0.5, "4h": 0.5}
    )
    sizing_equity_haircut: float = 0.95

    # --- Optional strategy regime overlay ---
    # A 4h entry with CHOP above the threshold can request reduced notional.
    # It never alters exits, leverage, or the 1d timeframe.
    strategy_4h_chop_reduce_enabled: bool = False
    strategy_chop_lookback: int = 14
    strategy_chop_high_threshold: float = 61.8
    strategy_chop_high_size_multiplier: float = 0.5

    # --- Kill switch ---
    halted: str = ""  # "1" to halt
    halt_file: Path | None = None

    @field_validator("storage_log_path", "halt_file", mode="before")
    @classmethod
    def blank_optional_paths_are_none(cls, value: object) -> object | None:
        """Treat an empty dotenv assignment as disabled, not ``Path('.')``."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return value

    # --- Derived helpers ---

    def effective_base_url(self) -> str:
        if self.lnm_base_url:
            return self.lnm_base_url
        return _BASE_URLS[self.lnm_network]

    def effective_ws_url(self) -> str:
        if self.lnm_ws_url:
            return self.lnm_ws_url
        return _WS_URLS[self.lnm_network]

    def has_credentials(self) -> bool:
        return bool(self.lnm_access_key and self.lnm_access_secret and self.lnm_access_passphrase)


def load_config(env_file: str | Path | None = ".env") -> BotConfig:
    """Build a `BotConfig`, overlaying `.env` over the process environment."""
    # Pydantic-settings reads .env on its own; we just hand it in via model_config.
    # This wrapper exists to keep the entry point uniform across callers.
    return BotConfig(_env_file=str(env_file)) if env_file else BotConfig()
