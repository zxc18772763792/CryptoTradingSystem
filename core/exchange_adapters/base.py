"""Abstract exchange adapter contracts for incremental integration."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class MarketInfo:
    symbol: str
    exchange: str
    market_type: str
    base: str
    quote: str
    active: bool = True
    price_precision: Optional[int] = None
    amount_precision: Optional[int] = None
    min_qty: Optional[float] = None
    min_notional: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FundingRatePoint:
    symbol: str
    timestamp: datetime
    rate: float
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    source: str = "unknown"


@dataclass
class ExchangePositionSnapshot:
    symbol: str
    side: str
    quantity: float
    entry_price: float
    mark_price: Optional[float]
    unrealized_pnl: Optional[float]
    leverage: Optional[float]
    margin_mode: Optional[str] = None
    liquidation_price: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExchangeOrderRequest:
    symbol: str
    side: str
    order_type: str
    amount: float
    price: Optional[float] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExchangeOrderSnapshot:
    order_id: str
    client_order_id: Optional[str]
    symbol: str
    status: str
    side: str
    order_type: str
    amount: float
    filled: float
    remaining: float
    price: Optional[float]
    avg_price: Optional[float]
    fee: Optional[float]
    fee_currency: Optional[str]
    timestamp: Optional[datetime]
    raw: Dict[str, Any] = field(default_factory=dict)


class ExchangeAdapter(ABC):
    exchange: str = "unknown"
    market_type: str = "spot"

    @abstractmethod
    async def initialize(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def fetch_markets(self, reload: bool = False) -> List[MarketInfo]: ...

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]: ...

    @abstractmethod
    async def fetch_balances(self) -> Dict[str, Any]: ...

    @abstractmethod
    async def fetch_positions(self, symbols: Optional[List[str]] = None) -> List[ExchangePositionSnapshot]: ...

    @abstractmethod
    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot: ...

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str, params: Optional[Dict[str, Any]] = None) -> ExchangeOrderSnapshot: ...

    @abstractmethod
    async def fetch_order(self, symbol: str, order_id: str) -> ExchangeOrderSnapshot: ...

    @abstractmethod
    async def fetch_open_orders(self, symbol: Optional[str] = None) -> List[ExchangeOrderSnapshot]: ...

    @abstractmethod
    async def fetch_funding_rate(self, symbol: str) -> Optional[FundingRatePoint]: ...

    @abstractmethod
    async def fetch_funding_history(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[FundingRatePoint]: ...

