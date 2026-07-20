"""Hard risk limits. Read from env at startup; do not mutate."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskLimits:
    """Hard limits. Strategies must not be allowed to set or override these.

    All values loaded from env via BotConfig. Daily-loss stays in USD; the
    guard converts to sats against the engine's current price.
    """

    max_position_usd: float
    max_leverage: float
    max_daily_loss_usd: float
    max_orders_per_minute: int
    max_total_notional_usd: float | None = None
    max_total_margin_usd: float | None = None


def from_config(cfg) -> RiskLimits:
    """Construct from a BotConfig instance."""
    return RiskLimits(
        max_position_usd=cfg.risk_max_position_usd,
        max_leverage=cfg.risk_max_leverage,
        max_daily_loss_usd=cfg.risk_max_daily_loss_usd,
        max_orders_per_minute=cfg.risk_max_orders_per_minute,
        max_total_notional_usd=cfg.risk_max_total_notional_usd,
        max_total_margin_usd=cfg.risk_max_total_margin_usd,
    )
