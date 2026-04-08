"""
交易所基类模块
定义所有交易所连接器的通用接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any, AsyncGenerator, List
import asyncio
from loguru import logger

from config.exchanges import ExchangeConfig, ExchangeType


class OrderSide(Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """订单类型"""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    STOP_LOSS_LIMIT = "stop_loss_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class OrderStatus(Enum):
    """订单状态"""
    OPEN = "open"
    CLOSED = "closed"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REJECTED = "rejected"


@dataclass
class Ticker:
    """行情数据"""
    symbol: str
    last: float
    bid: float
    ask: float
    high_24h: float
    low_24h: float
    volume_24h: float
    timestamp: datetime
    exchange: str = ""


@dataclass
class Kline:
    """K线数据"""
    symbol: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    trades: int = 0
    exchange: str = ""


@dataclass
class Order:
    """订单数据"""
    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    price: float
    amount: float
    filled: float = 0.0
    remaining: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    fee_currency: str = ""
    status: OrderStatus = OrderStatus.OPEN
    timestamp: Optional[datetime] = None
    exchange: str = ""


@dataclass
class Balance:
    """账户余额"""
    currency: str
    free: float
    used: float
    total: float


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    side: str  # long/short
    amount: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    leverage: float = 1.0
    liquidation_price: Optional[float] = None


class BaseExchange(ABC):
    """交易所基类"""

    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.exchange_type = config.exchange_type
        self.name = config.name
        self._connected = False
        self._client: Any = None

    @abstractmethod
    async def connect(self) -> bool:
        """连接交易所"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass

    @abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        """获取行情数据"""
        pass

    @abstractmethod
    async def get_klines(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Kline]:
        """获取K线数据"""
        pass

    @abstractmethod
    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        """获取订单簿"""
        pass

    @abstractmethod
    async def get_balance(self) -> List[Balance]:
        """获取账户余额"""
        pass

    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Order:
        """创建订单"""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """取消订单"""
        pass

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> Order:
        """获取订单信息"""
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """获取未完成订单"""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """获取持仓信息（合约交易）"""
        pass

    @abstractmethod
    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """获取成交记录"""
        pass

    async def subscribe_kline(
        self,
        symbol: str,
        timeframe: str,
    ) -> AsyncGenerator[Kline, None]:
        """订阅K线数据（WebSocket）"""
        yield  # 占位，子类实现
        raise NotImplementedError("Subclass must implement this method")

    async def subscribe_ticker(
        self,
        symbol: str,
    ) -> AsyncGenerator[Ticker, None]:
        """订阅行情数据（WebSocket）"""
        yield  # 占位，子类实现
        raise NotImplementedError("Subclass must implement this method")

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self._connected

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            await self.get_ticker("BTC/USDT")
            return True
        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            return False

    def _handle_error(self, error: Exception, operation: str) -> None:
        """统一错误处理"""
        logger.error(f"[{self.name}] {operation} failed: {error}")
        raise error
