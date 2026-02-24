"""Gate.io exchange connector."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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


class GateConnector(BaseExchange):
    """Connector implementation for Gate.io via CCXT."""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config)

    async def connect(self) -> bool:
        try:
            self._client = ccxt.gate(
                {
                    "apiKey": settings.GATE_API_KEY or self.config.api_key,
                    "secret": settings.GATE_API_SECRET or self.config.api_secret,
                    "enableRateLimit": self.config.enable_rate_limit,
                    "rateLimit": self.config.rate_limit,
                    "timeout": self.config.timeout,
                    "sandbox": self.config.sandbox,
                    "defaultType": self.config.default_type,
                }
            )

            if settings.HTTP_PROXY:
                self._client.proxies = {
                    "http": settings.HTTP_PROXY,
                    "https": settings.HTTPS_PROXY or settings.HTTP_PROXY,
                }

            try:
                self._time_offset = 0
                load_diff = getattr(self._client, "load_time_difference", None)
                if callable(load_diff):
                    diff = await load_diff()
                    if isinstance(diff, (int, float)):
                        self._time_offset = int(diff)
                if self._time_offset == 0:
                    server_time = int(await self._client.fetch_time())
                    local_time = int(time.time() * 1000)
                    # CCXT expects local - server as timeDifference.
                    self._time_offset = int(local_time - server_time)
                self._client.options["adjustForTimeDifference"] = True
                self._client.options["timeDifference"] = int(self._time_offset)
                logger.info(f"[{self.name}] Time synced, offset: {self._time_offset}ms")
            except Exception as e:
                logger.warning(f"[{self.name}] Time sync failed: {e}")
                self._time_offset = 0

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
        if self._client:
            await self._client.close()
        self._connected = False
        logger.info(f"[{self.name}] Disconnected")

    async def get_ticker(self, symbol: str) -> Ticker:
        try:
            ticker = await self._client.fetch_ticker(symbol)
            ts = ticker.get("timestamp")
            timestamp = datetime.fromtimestamp(ts / 1000) if ts else datetime.now()
            return Ticker(
                symbol=symbol,
                last=float(ticker.get("last", 0) or 0),
                bid=float(ticker.get("bid", 0) or 0),
                ask=float(ticker.get("ask", 0) or 0),
                high_24h=float(ticker.get("high", 0) or 0),
                low_24h=float(ticker.get("low", 0) or 0),
                volume_24h=float(ticker.get("baseVolume", 0) or 0),
                timestamp=timestamp,
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
        try:
            since_ms = int(since.timestamp() * 1000) if since else None
            ohlcv = await self._client.fetch_ohlcv(
                symbol,
                timeframe,
                since=since_ms,
                limit=limit or 1000,
            )
            return [
                Kline(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromtimestamp(candle[0] / 1000),
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5]),
                    exchange=self.name,
                )
                for candle in ohlcv
            ]
        except Exception as e:
            self._handle_error(e, f"get_klines({symbol}, {timeframe})")

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
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
        """Fetch and merge balances across major Gate account buckets."""
        try:
            merged: Dict[str, Dict[str, float]] = {}
            seen_snapshots: set[Tuple[Tuple[str, float, float, float], ...]] = set()

            def _merge_amount(currency: str, free: float, used: float, total: float) -> None:
                if total <= 0:
                    return
                ccy = str(currency or "").upper()
                if not ccy:
                    return
                if ccy not in merged:
                    merged[ccy] = {"free": 0.0, "used": 0.0, "total": 0.0}
                merged[ccy]["free"] += float(free or 0.0)
                merged[ccy]["used"] += float(used or 0.0)
                merged[ccy]["total"] += float(total or 0.0)

            def _parse_payload(balance_payload: dict) -> Dict[str, Dict[str, float]]:
                parsed: Dict[str, Dict[str, float]] = {}
                reserved = {"info", "timestamp", "datetime", "free", "used", "total"}

                for currency, amounts in (balance_payload or {}).items():
                    if currency in reserved or not isinstance(amounts, dict):
                        continue
                    free = float(amounts.get("free", 0) or 0)
                    used = float(amounts.get("used", 0) or 0)
                    total = float(amounts.get("total", 0) or 0)
                    if total > 0:
                        parsed[str(currency).upper()] = {"free": free, "used": used, "total": total}

                free_map = balance_payload.get("free") if isinstance(balance_payload.get("free"), dict) else {}
                used_map = balance_payload.get("used") if isinstance(balance_payload.get("used"), dict) else {}
                total_map = balance_payload.get("total") if isinstance(balance_payload.get("total"), dict) else {}
                for currency in set(list(free_map.keys()) + list(used_map.keys()) + list(total_map.keys())):
                    ccy = str(currency).upper()
                    free = float(free_map.get(currency, 0) or 0)
                    used = float(used_map.get(currency, 0) or 0)
                    total = float(total_map.get(currency, free + used) or 0)
                    if total <= 0:
                        continue
                    prev = parsed.get(ccy, {"free": 0.0, "used": 0.0, "total": 0.0})
                    parsed[ccy] = {
                        "free": max(prev["free"], free),
                        "used": max(prev["used"], used),
                        "total": max(prev["total"], total),
                    }
                return parsed

            async def _fetch_by_type(account_type: str) -> Dict[str, Dict[str, float]]:
                try:
                    payload = await self._client.fetch_balance({"type": account_type})
                    parsed = _parse_payload(payload)
                    if parsed:
                        snapshot_key = tuple(
                            sorted((ccy, vals["free"], vals["used"], vals["total"]) for ccy, vals in parsed.items())
                        )
                        if snapshot_key in seen_snapshots:
                            return {}
                        seen_snapshots.add(snapshot_key)
                    return parsed
                except Exception as e:
                    logger.debug(f"[{self.name}] {account_type} balance fetch failed: {e}")
                    return {}

            for account_type in ["spot", "funding", "swap"]:
                parsed = await _fetch_by_type(account_type)
                for ccy, vals in parsed.items():
                    _merge_amount(ccy, vals["free"], vals["used"], vals["total"])

            if not merged:
                fallback_payload = await self._client.fetch_balance()
                parsed = _parse_payload(fallback_payload)
                for ccy, vals in parsed.items():
                    _merge_amount(ccy, vals["free"], vals["used"], vals["total"])

            balances = [
                Balance(
                    currency=ccy,
                    free=round(values["free"], 10),
                    used=round(values["used"], 10),
                    total=round(values["total"], 10),
                )
                for ccy, values in merged.items()
                if values["total"] > 0
            ]
            balances.sort(key=lambda b: b.total, reverse=True)
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
        try:
            await self._client.cancel_order(order_id, symbol)
            logger.info(f"[{self.name}] Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Failed to cancel order {order_id}: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Order:
        try:
            ccxt_order = await self._client.fetch_order(order_id, symbol)
            return self._parse_order(ccxt_order)
        except Exception as e:
            self._handle_error(e, f"get_order({order_id})")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        try:
            ccxt_orders = await self._client.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in ccxt_orders]
        except Exception as e:
            self._handle_error(e, "get_open_orders")

    async def get_positions(self) -> List[Position]:
        try:
            positions = await self._client.fetch_positions()
            result: List[Position] = []
            for pos in positions:
                if float(pos.get("contracts", 0) or 0) <= 0:
                    continue
                result.append(
                    Position(
                        symbol=pos.get("symbol", ""),
                        side=pos.get("side", ""),
                        amount=float(pos.get("contracts", 0) or 0),
                        entry_price=float(pos.get("entryPrice", 0) or 0),
                        current_price=float(pos.get("markPrice", 0) or 0),
                        unrealized_pnl=float(pos.get("unrealizedPnl", 0) or 0),
                        leverage=float(pos.get("leverage", 1) or 1),
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
            timestamp=(
                datetime.fromtimestamp(ccxt_order.get("timestamp", 0) / 1000)
                if ccxt_order.get("timestamp")
                else None
            ),
            exchange=self.name,
        )
