"""
交易所基类模块
定义所有交易所连接器的通用接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any, AsyncGenerator, Dict, List
import asyncio
import time
from loguru import logger

from config.exchanges import ExchangeConfig, ExchangeType


class ExchangeThrottled(Exception):
    """Raised to fast-fail a market-data read while the connector is in a
    throttle cooldown (exchange returned 429/418/DDoSProtection). Lets callers
    skip the request instead of piling more weight onto a rate-limited account.
    """


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

    _ERROR_LOG_REPEAT_WINDOW_SEC = 300.0

    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.exchange_type = config.exchange_type
        self.name = config.name
        self._connected = False
        self._client: Any = None
        self._throttle_until = 0.0
        self._throttle_failures = 0
        # (operation|message) -> last ERROR-level log time; see _handle_error.
        self._error_log_last_at: Dict[str, float] = {}

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

    async def _ensure_client(self) -> Any:
        client = self._client
        if client is not None and self._connected:
            return client
        await self.connect()
        client = self._client
        if client is None or not self._connected:
            raise RuntimeError(f"[{self.name}] client unavailable")
        return client

    async def health_check(self) -> bool:
        """健康检查 — 使用最轻的 fetch_time 端点而非 24hr ticker.

        前版本调用 ``get_ticker("BTC/USDT")`` 走的是 ``/fapi/v1/ticker/24hr`` —
        每分钟一次的 watchdog 持续给 24hr stats 端点施压, 还消耗 ccxt 内部
        节流配额. fetch_time 在所有主流 CEX 上都是亚毫秒级响应, 也不占
        签名/权重配额.
        """
        client = getattr(self, "_client", None)
        if client is None:
            try:
                client = await self._ensure_client()  # type: ignore[attr-defined]
            except AttributeError:
                # Subclasses without _ensure_client → fall back to old path.
                try:
                    await self.get_ticker("BTC/USDT")
                    return True
                except Exception as e:
                    logger.error(f"Health check failed for {self.name}: {type(e).__name__}: {e}")
                    return False
            except Exception as e:
                logger.error(f"Health check failed for {self.name}: {type(e).__name__}: {e}")
                return False

        try:
            fetch_time = getattr(client, "fetch_time", None)
            if callable(fetch_time):
                # 15s, not 6s: fetch_time goes through the ccxt rate-limit
                # queue, so under REST load the wait reflects queue depth,
                # not connectivity. A tight timeout here made the watchdog
                # declare a busy-but-healthy client dead.
                await asyncio.wait_for(fetch_time(), timeout=15.0)
                return True
            # No fetch_time on this client: legacy ticker path.
            await self.get_ticker("BTC/USDT")
            return True
        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {type(e).__name__}: {e}")
            return False

    @staticmethod
    def _is_transient_connection_error(error: Exception) -> bool:
        transient_names = {
            "NetworkError",
            "RequestTimeout",
            "ExchangeNotAvailable",
            "DDoSProtection",
            "RateLimitExceeded",
        }
        if isinstance(error, (asyncio.TimeoutError, TimeoutError, ConnectionError)):
            return True
        for cls in type(error).mro():
            if cls.__name__ in transient_names:
                return True
        message = str(error or "").lower()
        return any(
            token in message
            for token in (
                "timeout",
                "timed out",
                "connection",
                "network",
                "temporarily unavailable",
                "exchange not available",
                "server disconnected",
            )
        )

    @staticmethod
    def _is_throttle_error(error: Exception) -> bool:
        """True for exchange rate-limit / ban errors (429 / 418 / -1003)."""
        if isinstance(error, ExchangeThrottled):
            return False
        for cls in type(error).mro():
            if cls.__name__ in ("DDoSProtection", "RateLimitExceeded"):
                return True
        message = str(error or "").lower()
        return any(
            token in message
            for token in (
                "too many requests",
                "-1003",
                "request weight",
                "way too much",
                "banned until",
                "ip banned",
                "ratelimit",
            )
        )

    def _throttle_remaining(self) -> float:
        """Seconds left in the current throttle cooldown (0.0 if clear)."""
        return max(0.0, float(getattr(self, "_throttle_until", 0.0)) - time.monotonic())

    def _note_throttle(self) -> None:
        """Enter / extend an exponential throttle cooldown (5s..120s)."""
        failures = int(getattr(self, "_throttle_failures", 0)) + 1
        self._throttle_failures = failures
        backoff = min(120.0, 5.0 * (2 ** min(failures - 1, 5)))
        self._throttle_until = time.monotonic() + backoff

    def _clear_throttle(self) -> None:
        """Reset the cooldown after a successful request."""
        if getattr(self, "_throttle_failures", 0) or getattr(self, "_throttle_until", 0.0):
            self._throttle_failures = 0
            self._throttle_until = 0.0

    def _raise_if_throttled(self, operation: str) -> None:
        """Fast-fail a read while cooling down, so we add no weight to the ban."""
        remaining = self._throttle_remaining()
        if remaining > 0:
            raise ExchangeThrottled(
                f"[{self.name}] {operation} skipped: throttle cooldown ~{remaining:.0f}s remaining"
            )

    def _handle_error(self, error: Exception, operation: str) -> None:
        """统一错误处理"""
        if isinstance(error, ExchangeThrottled):
            # Already a cooldown fast-fail; propagate quietly, don't re-note.
            raise error
        if self._is_throttle_error(error):
            # Throttle/ban: back off instead of reconnecting (reconnect reloads
            # markets = more weight = worse). Do NOT flip _connected here.
            self._note_throttle()
            logger.warning(
                f"[{self.name}] {operation} throttled by exchange; "
                f"backing off ~{self._throttle_remaining():.0f}s"
            )
            raise error
        if self._is_transient_connection_error(error):
            self._connected = False
        # Throttle identical error lines: a permanently-failing call site (e.g.
        # gate "Request IP not in whitelist" on every balances poll) otherwise
        # floods the log with one ERROR per poll around the clock. Same
        # (operation, message) repeats within the window log at DEBUG instead.
        # Pure logging change — the exception always propagates unchanged.
        now = time.monotonic()
        signature = f"{operation}|{str(error)[:120]}"
        last_at = self._error_log_last_at.get(signature, 0.0)
        if (now - last_at) >= self._ERROR_LOG_REPEAT_WINDOW_SEC:
            self._error_log_last_at[signature] = now
            if len(self._error_log_last_at) > 64:
                cutoff = now - self._ERROR_LOG_REPEAT_WINDOW_SEC
                self._error_log_last_at = {
                    key: value
                    for key, value in self._error_log_last_at.items()
                    if value >= cutoff
                }
            logger.error(f"[{self.name}] {operation} failed: {error}")
        else:
            logger.debug(f"[{self.name}] {operation} failed (repeat suppressed): {error}")
        raise error
