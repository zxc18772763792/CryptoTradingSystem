"""Exchange adapter interfaces (staging area, not wired into runtime yet)."""

from core.exchange_adapters.base import (
    ExchangeAdapter,
    ExchangeOrderRequest,
    ExchangeOrderSnapshot,
    ExchangePositionSnapshot,
    FundingRatePoint,
    MarketInfo,
)

__all__ = [
    "ExchangeAdapter",
    "ExchangeOrderRequest",
    "ExchangeOrderSnapshot",
    "ExchangePositionSnapshot",
    "FundingRatePoint",
    "MarketInfo",
]

