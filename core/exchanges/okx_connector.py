"""OKX exchange connector."""

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any, List, Optional

import ccxt.async_support as ccxt
from loguru import logger

from config.exchanges import ExchangeConfig
from config.settings import settings
from core.exchanges.base_exchange import (
    Balance,
    BaseExchange,
    Kline,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
)
from core.exchanges.order_parsing import resolve_ccxt_order_fill_price


class OKXConnector(BaseExchange):
    """OKX exchange connector."""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        self._connection_lock = asyncio.Lock()

    def _build_client_config(self) -> dict:
        return {
            "apiKey": self.config.api_key or settings.OKX_API_KEY,
            "secret": self.config.api_secret or settings.OKX_API_SECRET,
            "password": self.config.passphrase or settings.OKX_PASSPHRASE,
            "enableRateLimit": self.config.enable_rate_limit,
            "rateLimit": self.config.rate_limit,
            "timeout": self.config.timeout,
            "sandbox": self.config.sandbox,
            "defaultType": self.config.default_type,
        }

    def _apply_proxy(self, client: Any) -> None:
        proxy_url = str(self.config.proxy or settings.HTTP_PROXY or settings.HTTPS_PROXY or "").strip() or None
        if proxy_url:
            client.proxies = {
                "http": proxy_url,
                "https": settings.HTTPS_PROXY or proxy_url,
            }

    def _format_order_precision(
        self,
        client: Any,
        symbol: str,
        amount: float,
        price: Optional[float],
    ) -> tuple[Any, Optional[Any]]:
        precise_amount: Any = amount
        amount_to_precision = getattr(client, "amount_to_precision", None)
        if callable(amount_to_precision):
            precise_amount = amount_to_precision(symbol, amount)

        precise_price: Optional[Any] = price
        price_to_precision = getattr(client, "price_to_precision", None)
        if price is not None and callable(price_to_precision):
            precise_price = price_to_precision(symbol, price)

        return precise_amount, precise_price

    async def connect(self) -> bool:
        """Connect to OKX."""
        async with self._connection_lock:
            existing_client = self._client
            existing_connected = bool(existing_client is not None and self._connected)
            candidate_client = None
            try:
                candidate_client = ccxt.okx(self._build_client_config())
                self._apply_proxy(candidate_client)
                await candidate_client.load_markets()
                self._client = candidate_client
                self._connected = True
                if existing_client is not None and existing_client is not candidate_client:
                    with contextlib.suppress(Exception):
                        await existing_client.close()
                logger.info(f"[{self.name}] Connected successfully")
                return True

            except BaseException as e:
                if candidate_client is not None and candidate_client is not existing_client:
                    with contextlib.suppress(Exception):
                        await candidate_client.close()
                if existing_connected:
                    self._client = existing_client
                    self._connected = True
                else:
                    self._client = None
                    self._connected = False
                if isinstance(e, asyncio.CancelledError):
                    raise
                self._handle_error(e, "connect")
                return False

    async def disconnect(self) -> None:
        """Disconnect from OKX."""
        async with self._connection_lock:
            client = self._client
            self._client = None
            self._connected = False
            if client:
                with contextlib.suppress(Exception):
                    await client.close()
        logger.info(f"[{self.name}] Disconnected")

    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker data."""
        try:
            client = await self._ensure_client()
            ticker = await client.fetch_ticker(symbol)
            return Ticker(
                symbol=symbol,
                last=float(ticker.get("last", 0)),
                bid=float(ticker.get("bid", 0)),
                ask=float(ticker.get("ask", 0)),
                high_24h=float(ticker.get("high", 0)),
                low_24h=float(ticker.get("low", 0)),
                volume_24h=float(ticker.get("baseVolume", 0)),
                timestamp=datetime.fromtimestamp(ticker.get("timestamp", 0) / 1000, tz=timezone.utc)
                if ticker.get("timestamp")
                else datetime.now(timezone.utc),
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
        """Get OHLCV data."""
        try:
            client = await self._ensure_client()
            since_ms = int(since.timestamp() * 1000) if since else None
            ohlcv = await client.fetch_ohlcv(
                symbol,
                timeframe,
                since=since_ms,
                limit=limit or 300,
            )

            klines = []
            for candle in ohlcv:
                klines.append(
                    Kline(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                        open=float(candle[1]),
                        high=float(candle[2]),
                        low=float(candle[3]),
                        close=float(candle[4]),
                        volume=float(candle[5]),
                        exchange=self.name,
                    )
                )

            return klines

        except Exception as e:
            self._handle_error(e, f"get_klines({symbol}, {timeframe})")

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        """Get order book."""
        try:
            client = await self._ensure_client()
            orderbook = await client.fetch_order_book(symbol, limit)
            return {
                "bids": orderbook.get("bids", []),
                "asks": orderbook.get("asks", []),
                "timestamp": datetime.now(timezone.utc),
            }
        except Exception as e:
            self._handle_error(e, f"get_order_book({symbol})")

    async def get_balance(self) -> List[Balance]:
        """Get account balances."""
        try:
            client = await self._ensure_client()
            balance = await client.fetch_balance()
            balances = []

            for currency, amounts in balance.items():
                if currency in ["info", "timestamp", "datetime", "free", "used", "total"]:
                    continue

                free = float(amounts.get("free", 0) or 0)
                used = float(amounts.get("used", 0) or 0)
                total = float(amounts.get("total", 0) or 0)

                if total > 0:
                    balances.append(
                        Balance(
                            currency=currency,
                            free=free,
                            used=used,
                            total=total,
                        )
                    )

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
        """Create an order."""
        try:
            client = await self._ensure_client()
            amount = self._to_contract_amount(symbol, amount)
            precise_amount, precise_price = self._format_order_precision(client, symbol, amount, price)
            ccxt_order = await client.create_order(
                symbol=symbol,
                type=order_type.value,
                side=side.value,
                amount=precise_amount,
                price=precise_price,
                params=params or {},
            )

            return self._parse_order(ccxt_order)

        except Exception as e:
            self._handle_error(e, f"create_order({symbol}, {side.value}, {order_type.value})")

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an order."""
        try:
            client = await self._ensure_client()
            await client.cancel_order(order_id, symbol)
            logger.info(f"[{self.name}] Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Failed to cancel order {order_id}: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Order:
        """Get order details."""
        try:
            client = await self._ensure_client()
            ccxt_order = await client.fetch_order(order_id, symbol)
            return self._parse_order(ccxt_order)
        except Exception as e:
            self._handle_error(e, f"get_order({order_id})")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders."""
        try:
            client = await self._ensure_client()
            ccxt_orders = await client.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in ccxt_orders]
        except Exception as e:
            self._handle_error(e, "get_open_orders")

    async def get_positions(self) -> List[Position]:
        """Get positions."""
        try:
            client = await self._ensure_client()
            positions = await client.fetch_positions()
            result = []

            for pos in positions:
                if float(pos.get("contracts", 0)) > 0:
                    result.append(
                        Position(
                            symbol=pos.get("symbol", ""),
                            side=pos.get("side", ""),
                            amount=float(pos.get("contracts", 0)) * self._contract_size(str(pos.get("symbol") or ""), position=pos),
                            entry_price=float(pos.get("entryPrice", 0)),
                            current_price=float(pos.get("markPrice", 0)),
                            unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                            leverage=float(pos.get("leverage", 1)),
                            liquidation_price=pos.get("liquidationPrice"),
                        )
                    )

            return result

        except Exception as e:
            self._handle_error(e, "get_positions")

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """Get trade history."""
        try:
            client = await self._ensure_client()
            since_ms = int(since.timestamp() * 1000) if since else None
            trades = await client.fetch_my_trades(
                symbol,
                since=since_ms,
                limit=limit or 100,
            )
            return trades
        except Exception as e:
            self._handle_error(e, f"get_trades({symbol})")

    def _parse_order(self, ccxt_order: dict) -> Order:
        """Parse a CCXT order payload."""
        ccxt_order = self._normalize_ccxt_order_quantities(ccxt_order)
        status_map = {
            "open": OrderStatus.OPEN,
            "closed": OrderStatus.CLOSED,
            "canceled": OrderStatus.CANCELED,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
        }

        fee_info = ccxt_order.get("fee") or {}
        fee_cost = float(fee_info.get("cost", 0) or 0)
        fee_currency = str(fee_info.get("currency", "") or "")

        return Order(
            id=str(ccxt_order.get("id", "")),
            symbol=ccxt_order.get("symbol", ""),
            side=OrderSide(ccxt_order.get("side", "buy")),
            type=OrderType(ccxt_order.get("type", "limit")),
            price=resolve_ccxt_order_fill_price(ccxt_order),
            amount=float(ccxt_order.get("amount", 0) or 0),
            filled=float(ccxt_order.get("filled", 0) or 0),
            remaining=float(ccxt_order.get("remaining", 0) or 0),
            cost=float(ccxt_order.get("cost", 0) or 0),
            fee=fee_cost,
            fee_currency=fee_currency,
            status=status_map.get(ccxt_order.get("status", "open"), OrderStatus.OPEN),
            timestamp=datetime.fromtimestamp(ccxt_order.get("timestamp", 0) / 1000, tz=timezone.utc)
            if ccxt_order.get("timestamp")
            else None,
            exchange=self.name,
        )
