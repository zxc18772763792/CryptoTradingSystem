"""
OKX交易所连接器
"""
from datetime import datetime
from typing import Optional, Any, List
import ccxt.async_support as ccxt
from loguru import logger

from config.settings import settings
from config.exchanges import ExchangeConfig
from core.exchanges.base_exchange import (
    BaseExchange,
    Ticker,
    Kline,
    Order,
    Balance,
    Position,
    OrderSide,
    OrderType,
    OrderStatus,
)


class OKXConnector(BaseExchange):
    """OKX交易所连接器"""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config)

    async def connect(self) -> bool:
        """连接OKX"""
        try:
            self._client = ccxt.okx({
                "apiKey": settings.OKX_API_KEY or self.config.api_key,
                "secret": settings.OKX_API_SECRET or self.config.api_secret,
                "password": settings.OKX_PASSPHRASE or self.config.passphrase,
                "enableRateLimit": self.config.enable_rate_limit,
                "rateLimit": self.config.rate_limit,
                "timeout": self.config.timeout,
                "sandbox": self.config.sandbox,
                "defaultType": self.config.default_type,
            })

            if settings.HTTP_PROXY:
                self._client.proxies = {
                    "http": settings.HTTP_PROXY,
                    "https": settings.HTTPS_PROXY or settings.HTTP_PROXY,
                }

            await self._client.load_markets()
            self._connected = True
            logger.info(f"[{self.name}] Connected successfully")
            return True

        except Exception as e:
            try:
                if self._client:
                    await self._client.close()
            except Exception:
                pass
            self._client = None
            self._connected = False
            self._handle_error(e, "connect")
            return False

    async def disconnect(self) -> None:
        """断开连接"""
        if self._client:
            await self._client.close()
        self._connected = False
        logger.info(f"[{self.name}] Disconnected")

    async def get_ticker(self, symbol: str) -> Ticker:
        """获取行情数据"""
        try:
            ticker = await self._client.fetch_ticker(symbol)
            return Ticker(
                symbol=symbol,
                last=float(ticker.get("last", 0)),
                bid=float(ticker.get("bid", 0)),
                ask=float(ticker.get("ask", 0)),
                high_24h=float(ticker.get("high", 0)),
                low_24h=float(ticker.get("low", 0)),
                volume_24h=float(ticker.get("baseVolume", 0)),
                timestamp=datetime.fromtimestamp(ticker.get("timestamp", 0) / 1000),
                exchange=self.name,
            )
        except Exception as e:
            self._handle_error(e, f"get_ticker({symbol})")

    async def get_klines(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Kline]:
        """获取K线数据"""
        try:
            since_ms = int(since.timestamp() * 1000) if since else None
            ohlcv = await self._client.fetch_ohlcv(
                symbol,
                timeframe,
                since=since_ms,
                limit=limit or 300,
            )

            klines = []
            for candle in ohlcv:
                klines.append(Kline(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromtimestamp(candle[0] / 1000),
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5]),
                    exchange=self.name,
                ))

            return klines

        except Exception as e:
            self._handle_error(e, f"get_klines({symbol}, {timeframe})")

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        """获取订单簿"""
        try:
            orderbook = await self._client.fetch_order_book(symbol, limit)
            return {
                "bids": orderbook.get("bids", []),
                "asks": orderbook.get("asks", []),
                "timestamp": datetime.now(),
            }
        except Exception as e:
            self._handle_error(e, f"get_order_book({symbol})")

    async def get_balance(self) -> List[Balance]:
        """获取账户余额"""
        try:
            balance = await self._client.fetch_balance()
            balances = []

            for currency, amounts in balance.items():
                if currency in ["info", "timestamp", "datetime", "free", "used", "total"]:
                    continue

                free = float(amounts.get("free", 0) or 0)
                used = float(amounts.get("used", 0) or 0)
                total = float(amounts.get("total", 0) or 0)

                if total > 0:
                    balances.append(Balance(
                        currency=currency,
                        free=free,
                        used=used,
                        total=total,
                    ))

            return balances

        except Exception as e:
            self._handle_error(e, "get_balance")

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
        try:
            ccxt_order = await self._client.create_order(
                symbol=symbol,
                type=order_type.value,
                side=side.value,
                amount=amount,
                price=price,
                params=params or {},
            )

            return self._parse_order(ccxt_order)

        except Exception as e:
            self._handle_error(e, f"create_order({symbol}, {side.value}, {order_type.value})")

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """取消订单"""
        try:
            await self._client.cancel_order(order_id, symbol)
            logger.info(f"[{self.name}] Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Failed to cancel order {order_id}: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Order:
        """获取订单信息"""
        try:
            ccxt_order = await self._client.fetch_order(order_id, symbol)
            return self._parse_order(ccxt_order)
        except Exception as e:
            self._handle_error(e, f"get_order({order_id})")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """获取未完成订单"""
        try:
            ccxt_orders = await self._client.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in ccxt_orders]
        except Exception as e:
            self._handle_error(e, "get_open_orders")

    async def get_positions(self) -> List[Position]:
        """获取持仓信息"""
        try:
            positions = await self._client.fetch_positions()
            result = []

            for pos in positions:
                if float(pos.get("contracts", 0)) > 0:
                    result.append(Position(
                        symbol=pos.get("symbol", ""),
                        side=pos.get("side", ""),
                        amount=float(pos.get("contracts", 0)),
                        entry_price=float(pos.get("entryPrice", 0)),
                        current_price=float(pos.get("markPrice", 0)),
                        unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                        leverage=float(pos.get("leverage", 1)),
                        liquidation_price=pos.get("liquidationPrice"),
                    ))

            return result

        except Exception as e:
            self._handle_error(e, "get_positions")

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """获取成交记录"""
        try:
            since_ms = int(since.timestamp() * 1000) if since else None
            trades = await self._client.fetch_my_trades(
                symbol,
                since=since_ms,
                limit=limit or 100,
            )
            return trades
        except Exception as e:
            self._handle_error(e, f"get_trades({symbol})")

    def _parse_order(self, ccxt_order: dict) -> Order:
        """解析CCXT订单格式"""
        status_map = {
            "open": OrderStatus.OPEN,
            "closed": OrderStatus.CLOSED,
            "canceled": OrderStatus.CANCELED,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
        }

        return Order(
            id=str(ccxt_order.get("id", "")),
            symbol=ccxt_order.get("symbol", ""),
            side=OrderSide(ccxt_order.get("side", "buy")),
            type=OrderType(ccxt_order.get("type", "limit")),
            price=float(ccxt_order.get("price", 0) or 0),
            amount=float(ccxt_order.get("amount", 0) or 0),
            filled=float(ccxt_order.get("filled", 0) or 0),
            remaining=float(ccxt_order.get("remaining", 0) or 0),
            cost=float(ccxt_order.get("cost", 0) or 0),
            status=status_map.get(ccxt_order.get("status", "open"), OrderStatus.OPEN),
            timestamp=datetime.fromtimestamp(ccxt_order.get("timestamp", 0) / 1000) if ccxt_order.get("timestamp") else None,
            exchange=self.name,
        )
