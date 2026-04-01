"""Binance connector."""
import asyncio
import contextlib
import re
import time
from datetime import datetime
from typing import Optional, Any, List, Dict

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

_BALANCE_SPOT_TIMEOUT_SEC = 4.0
_FUNDING_FETCH_TIMEOUT_SEC = 2.8
_FUNDING_CACHE_TTL_SEC = 300.0
_BALANCE_CACHE_TTL_SEC = 90.0

# Symbol mapping: spot symbol -> futures symbol (for low-price tokens)
_FUTURES_SYMBOL_MAP = {
    "PEPE/USDT": "1000PEPE/USDT",
    "SHIB/USDT": "1000SHIB/USDT",
    "LDO/USDT": "1000LDO/USDT",
    "XCN/USDT": "1000XCN/USDT",
    "FLOKI/USDT": "1000FLOKI/USDT",
    "BONK/USDT": "1000BONK/USDT",
    "RATS/USDT": "1000RATS/USDT",
    "SATS/USDT": "1000SATS/USDT",
    "COMBO/USDT": "1000COMBO/USDT",
    "BIGTIME/USDT": "1000BIGTIME/USDT",
    "ORDI/USDT": "1000ORDI/USDT",
    "WLD/USDT": "1000WLD/USDT",
    "ACE/USDT": "1000ACE/USDT",
    "MEME/USDT": "1000MEME/USDT",
    "NFP/USDT": "1000NFP/USDT",
    "AI/USDT": "1000AI/USDT",
    "XAI/USDT": "1000XAI/USDT",
    "MANTA/USDT": "1000MANTA/USDT",
    "JUP/USDT": "1000JUP/USDT",
    "ONDO/USDT": "1000ONDO/USDT",
    "AEVO/USDT": "1000AEVO/USDT",
    "ETHFI/USDT": "1000ETHFI/USDT",
    "ENA/USDT": "1000ENA/USDT",
    "W/USDT": "1000W/USDT",
    "TNSR/USDT": "1000TNSR/USDT",
    "SAGA/USDT": "1000SAGA/USDT",
    "TAO/USDT": "1000TAO/USDT",
    "OMNI/USDT": "1000OMNI/USDT",
    "REZ/USDT": "1000REZ/USDT",
    "BB/USDT": "1000BB/USDT",
    "NOT/USDT": "1000NOT/USDT",
    "IO/USDT": "1000IO/USDT",
    "ZK/USDT": "1000ZK/USDT",
    "LISTA/USDT": "1000LISTA/USDT",
    "ZRO/USDT": "1000ZRO/USDT",
    "BLAST/USDT": "1000BLAST/USDT",
    "RENDER/USDT": "1000RENDER/USDT",
    "TON/USDT": "1000TON/USDT",
}


def _map_futures_symbol(symbol: str, default_type: str) -> str:
    """Map spot symbol to futures symbol for low-price tokens."""
    if not symbol:
        return symbol
    if str(default_type or "").lower() not in ("future", "swap"):
        return symbol
    return _FUTURES_SYMBOL_MAP.get(symbol, symbol)


def _futures_price_divisor(symbol: str, mapped_symbol: str, default_type: str) -> float:
    """Normalize prices for mapped futures symbols like 1000SHIB/USDT."""
    if str(default_type or "").lower() not in ("future", "swap"):
        return 1.0
    src = str(symbol or "").upper()
    dst = str(mapped_symbol or "").upper()
    if not src or not dst or src == dst or "/" not in src or "/" not in dst:
        return 1.0
    src_base, src_quote = src.split("/", 1)
    dst_base, dst_quote = dst.split("/", 1)
    if src_quote != dst_quote:
        return 1.0
    m = re.match(r"^(\d+)([A-Z0-9_]+)$", dst_base)
    if not m:
        return 1.0
    mult = float(m.group(1) or 1.0)
    tail = str(m.group(2) or "")
    if mult <= 1 or tail != src_base:
        return 1.0
    return mult


class BinanceConnector(BaseExchange):
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        self._ws_client: Any = None
        self._funding_cache: Dict[str, Dict[str, float]] = {}
        self._funding_cache_ts: float = 0.0
        self._balance_cache: List[Balance] = []
        self._balance_cache_ts: float = 0.0
        self._connection_lock = asyncio.Lock()

    def _build_client_config(self) -> Dict[str, Any]:
        client_config: Dict[str, Any] = {
            "apiKey": settings.BINANCE_API_KEY or self.config.api_key,
            "secret": settings.BINANCE_API_SECRET or self.config.api_secret,
            "enableRateLimit": self.config.enable_rate_limit,
            "rateLimit": self.config.rate_limit,
            "timeout": self.config.timeout,
            "sandbox": self.config.sandbox,
            "defaultType": self.config.default_type,
            "options": {
                "defaultType": self.config.default_type,
                "recvWindow": 59000,
                "adjustForTimeDifference": True,
            },
        }

        if settings.HTTP_PROXY:
            client_config["aiohttp_proxy"] = settings.HTTP_PROXY
            client_config["proxies"] = {
                "http": settings.HTTP_PROXY,
                "https": settings.HTTPS_PROXY or settings.HTTP_PROXY,
            }
        return client_config

    async def _prepare_client(self, client: Any) -> int:
        time_offset = 0
        try:
            load_diff = getattr(client, "load_time_difference", None)
            if callable(load_diff):
                diff = await load_diff()
                if isinstance(diff, (int, float)):
                    time_offset = int(diff)
            if time_offset == 0:
                server_time = int(await client.fetch_time())
                local_time = int(time.time() * 1000)
                # CCXT expects local - server as timeDifference.
                time_offset = int(local_time - server_time)
            client.options["adjustForTimeDifference"] = True
            client.options["timeDifference"] = int(time_offset)
            logger.info(f"[{self.name}] Time synced, offset: {time_offset}ms")
        except Exception as e:
            logger.warning(f"[{self.name}] Time sync failed: {e}")
            time_offset = 0
        client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        await client.load_markets()
        return time_offset

    async def _ensure_client(self) -> Any:
        client = self._client
        if client is not None and self._connected:
            return client
        await self.connect()
        client = self._client
        if client is None or not self._connected:
            raise RuntimeError(f"[{self.name}] client unavailable")
        return client

    async def connect(self) -> bool:
        async with self._connection_lock:
            existing_client = self._client
            existing_connected = bool(existing_client is not None and self._connected)
            candidate_client = None
            try:
                candidate_client = ccxt.binance(self._build_client_config())
                self._time_offset = await self._prepare_client(candidate_client)
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
        async with self._connection_lock:
            client = self._client
            self._client = None
            self._connected = False
            if client:
                with contextlib.suppress(Exception):
                    await client.close()
        logger.info(f"[{self.name}] Disconnected")

    async def get_ticker(self, symbol: str) -> Ticker:
        try:
            client = await self._ensure_client()
            mapped_symbol = _map_futures_symbol(symbol, self.config.default_type)
            ticker = await client.fetch_ticker(mapped_symbol)
            divisor = _futures_price_divisor(symbol, mapped_symbol, self.config.default_type)
            def _norm_price(v: Any) -> float:
                px = float(v or 0)
                return (px / divisor) if divisor > 1 else px
            ts = ticker.get("timestamp")
            timestamp = datetime.fromtimestamp(ts / 1000) if ts else datetime.now()
            return Ticker(
                symbol=symbol,
                last=_norm_price(ticker.get("last", 0)),
                bid=_norm_price(ticker.get("bid", 0)),
                ask=_norm_price(ticker.get("ask", 0)),
                high_24h=_norm_price(ticker.get("high", 0)),
                low_24h=_norm_price(ticker.get("low", 0)),
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
            client = await self._ensure_client()
            mapped_symbol = _map_futures_symbol(symbol, self.config.default_type)
            divisor = _futures_price_divisor(symbol, mapped_symbol, self.config.default_type)
            since_ms = int(since.timestamp() * 1000) if since else None
            ohlcv = await client.fetch_ohlcv(
                mapped_symbol,
                timeframe,
                since=since_ms,
                limit=limit or 1000,
            )
            return [
                Kline(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromtimestamp(candle[0] / 1000),
                    open=(float(candle[1]) / divisor) if divisor > 1 else float(candle[1]),
                    high=(float(candle[2]) / divisor) if divisor > 1 else float(candle[2]),
                    low=(float(candle[3]) / divisor) if divisor > 1 else float(candle[3]),
                    close=(float(candle[4]) / divisor) if divisor > 1 else float(candle[4]),
                    volume=float(candle[5]),
                    exchange=self.name,
                )
                for candle in ohlcv
            ]
        except Exception as e:
            self._handle_error(e, f"get_klines({symbol}, {timeframe})")

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        try:
            client = await self._ensure_client()
            orderbook = await client.fetch_order_book(symbol, limit)
            return {
                "bids": orderbook.get("bids", []),
                "asks": orderbook.get("asks", []),
                "timestamp": datetime.now(),
            }
        except Exception as e:
            self._handle_error(e, f"get_order_book({symbol})")

    async def get_balance(self) -> List[Balance]:
        def _merge_amount(
            bucket: Dict[str, Dict[str, float]],
            currency: str,
            free: float,
            used: float,
            total: float,
        ) -> None:
            if total <= 0:
                return
            if currency not in bucket:
                bucket[currency] = {"free": 0.0, "used": 0.0, "total": 0.0}
            bucket[currency]["free"] += float(free or 0.0)
            bucket[currency]["used"] += float(used or 0.0)
            bucket[currency]["total"] += float(total or 0.0)

        def _merge_ccxt_balance_payload(
            payload: Any,
            bucket: Dict[str, Dict[str, float]],
            snapshot_bucket: Optional[Dict[str, Dict[str, float]]] = None,
        ) -> None:
            if not isinstance(payload, dict):
                return
            reserved = {"info", "timestamp", "datetime", "free", "used", "total"}
            seen: set[str] = set()

            for currency, amounts in payload.items():
                if currency in reserved or not isinstance(amounts, dict):
                    continue
                ccy = str(currency).upper()
                free = float(amounts.get("free", 0) or 0)
                used = float(amounts.get("used", 0) or 0)
                total = float(amounts.get("total", 0) or 0)
                _merge_amount(bucket, ccy, free, used, total)
                if snapshot_bucket is not None:
                    _merge_amount(snapshot_bucket, ccy, free, used, total)
                seen.add(ccy)

            free_map = payload.get("free") if isinstance(payload.get("free"), dict) else {}
            used_map = payload.get("used") if isinstance(payload.get("used"), dict) else {}
            total_map = payload.get("total") if isinstance(payload.get("total"), dict) else {}
            for currency in set(list(free_map.keys()) + list(used_map.keys()) + list(total_map.keys())):
                ccy = str(currency).upper()
                if ccy in seen or ccy in reserved:
                    continue
                free = float(free_map.get(currency, 0) or 0)
                used = float(used_map.get(currency, 0) or 0)
                total = float(total_map.get(currency, free + used) or 0)
                _merge_amount(bucket, ccy, free, used, total)
                if snapshot_bucket is not None:
                    _merge_amount(snapshot_bucket, ccy, free, used, total)

        try:
            client = await self._ensure_client()
            merged: Dict[str, Dict[str, float]] = {}
            funding_loaded = False
            funding_timed_out = False
            funding_snapshot: Dict[str, Dict[str, float]] = {}
            futures_loaded = False
            default_type = str(getattr(self.config, "default_type", "") or "").lower()

            funding_methods = [
                "sapiPostAssetGetFundingAsset",
                "sapiGetAssetGetFundingAsset",
                "privatePostAssetGetFundingAsset",
                "privateGetAssetGetFundingAsset",
            ]
            funding_fetch = None
            funding_method_name = ""
            for method_name in funding_methods:
                candidate = getattr(client, method_name, None)
                if callable(candidate):
                    funding_fetch = candidate
                    funding_method_name = method_name
                    break

            if callable(funding_fetch):
                try:
                    raw = await asyncio.wait_for(
                        funding_fetch({"needBtcValuation": "false"}),
                        timeout=_FUNDING_FETCH_TIMEOUT_SEC,
                    )
                    if isinstance(raw, dict):
                        funding_assets = (
                            raw.get("data")
                            or raw.get("rows")
                            or raw.get("assets")
                            or raw.get("balances")
                            or []
                        )
                    else:
                        funding_assets = raw or []

                    for item in funding_assets:
                        currency = str(item.get("asset", "")).upper()
                        if not currency:
                            continue
                        free = float(item.get("free", 0) or 0)
                        locked = float(item.get("locked", 0) or 0)
                        freeze = float(item.get("freeze", 0) or 0)
                        withdrawing = float(item.get("withdrawing", 0) or 0)
                        used = locked + freeze + withdrawing
                        total = free + used
                        _merge_amount(merged, currency, free, used, total)
                        _merge_amount(funding_snapshot, currency, free, used, total)
                    if funding_snapshot:
                        self._funding_cache = funding_snapshot
                        self._funding_cache_ts = time.time()
                    funding_loaded = True
                except Exception as e:
                    if isinstance(e, asyncio.TimeoutError):
                        funding_timed_out = True
                    logger.debug(
                        f"[{self.name}] {funding_method_name} funding fetch failed: {e}"
                    )

            if not funding_loaded and not funding_timed_out:
                try:
                    funding_balance = await asyncio.wait_for(
                        client.fetch_balance({"type": "funding"}),
                        timeout=_FUNDING_FETCH_TIMEOUT_SEC,
                    )
                    _merge_ccxt_balance_payload(
                        funding_balance,
                        merged,
                        snapshot_bucket=funding_snapshot,
                    )
                    if funding_snapshot:
                        self._funding_cache = funding_snapshot
                        self._funding_cache_ts = time.time()
                        funding_loaded = True
                except Exception as e:
                    logger.debug(f"[{self.name}] funding balance fallback unavailable: {e}")

            if (
                not funding_loaded
                and self._funding_cache
                and (time.time() - self._funding_cache_ts) <= _FUNDING_CACHE_TTL_SEC
            ):
                for currency, values in self._funding_cache.items():
                    _merge_amount(
                        merged,
                        str(currency).upper(),
                        float(values.get("free", 0) or 0),
                        float(values.get("used", 0) or 0),
                        float(values.get("total", 0) or 0),
                    )

            spot_loaded = False
            try:
                # Important: when defaultType=future/swap, bare fetch_balance() may return
                # derivatives wallet instead of spot wallet. Request spot explicitly to avoid
                # double counting between spot and futures balances.
                if default_type in {"future", "swap"}:
                    spot = await asyncio.wait_for(
                        client.fetch_balance({"type": "spot"}),
                        timeout=_BALANCE_SPOT_TIMEOUT_SEC,
                    )
                else:
                    spot = await asyncio.wait_for(
                        client.fetch_balance(),
                        timeout=_BALANCE_SPOT_TIMEOUT_SEC,
                    )
                _merge_ccxt_balance_payload(spot, merged)
                spot_loaded = True
            except Exception as e:
                logger.debug(f"[{self.name}] spot balance fetch failed: {e}")

            future_types: List[str] = []
            if default_type in {"future", "swap"}:
                # Use default type first, then fallback to the alternate type.
                alt_type = "swap" if default_type == "future" else "future"
                future_types.extend([default_type, alt_type])
            else:
                # Even when the connector default type is spot, Binance assets may still
                # live in the USD-M futures wallet. Include futures explicitly so the
                # account valuation reflects funding + spot + futures as one total.
                future_types.extend(["future", "swap"])

            chosen_futures_snapshot: Dict[str, Dict[str, float]] = {}
            tried_future_types: set[str] = set()
            for account_type in future_types:
                if account_type in tried_future_types:
                    continue
                tried_future_types.add(account_type)
                try:
                    future_balance = await asyncio.wait_for(
                        client.fetch_balance({"type": account_type}),
                        timeout=_BALANCE_SPOT_TIMEOUT_SEC,
                    )
                    current_snapshot: Dict[str, Dict[str, float]] = {}
                    _merge_ccxt_balance_payload(future_balance, current_snapshot)
                    if not current_snapshot:
                        continue
                    chosen_futures_snapshot = current_snapshot
                    futures_loaded = True
                    # Stop at first valid futures payload to avoid future/swap alias double counting.
                    break
                except Exception as e:
                    logger.debug(f"[{self.name}] {account_type} balance fetch failed: {e}")

            if chosen_futures_snapshot:
                for currency, values in chosen_futures_snapshot.items():
                    _merge_amount(
                        merged,
                        str(currency).upper(),
                        float(values.get("free", 0) or 0),
                        float(values.get("used", 0) or 0),
                        float(values.get("total", 0) or 0),
                    )

            if not merged and not funding_loaded and not spot_loaded and not futures_loaded:
                raise RuntimeError("spot/funding/future balance unavailable")

            balances = [
                Balance(
                    currency=currency,
                    free=round(values["free"], 10),
                    used=round(values["used"], 10),
                    total=round(values["total"], 10),
                )
                for currency, values in merged.items()
                if values["total"] > 0
            ]
            balances.sort(key=lambda b: b.total, reverse=True)
            self._balance_cache = balances
            self._balance_cache_ts = time.time()
            return balances
        except Exception as e:
            if self._balance_cache and (time.time() - self._balance_cache_ts) <= _BALANCE_CACHE_TTL_SEC:
                logger.warning(f"[{self.name}] get_balance failed, return cached balances: {e}")
                return self._balance_cache
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
            client = await self._ensure_client()
            ccxt_order = await client.create_order(
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
            client = await self._ensure_client()
            await client.cancel_order(order_id, symbol)
            logger.info(f"[{self.name}] Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Failed to cancel order {order_id}: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Order:
        try:
            client = await self._ensure_client()
            ccxt_order = await client.fetch_order(order_id, symbol)
            return self._parse_order(ccxt_order)
        except Exception as e:
            self._handle_error(e, f"get_order({order_id})")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        try:
            client = await self._ensure_client()
            ccxt_orders = await client.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in ccxt_orders]
        except Exception as e:
            self._handle_error(e, "get_open_orders")

    async def get_positions(self) -> List[Position]:
        try:
            client = await self._ensure_client()
            default_type = str(getattr(self.config, "default_type", "") or "").lower()
            positions: List[dict] = []
            if default_type in ["future", "swap"]:
                positions = await client.fetch_positions()
            else:
                original_default_type = client.options.get("defaultType")
                for account_type in ("future", "swap"):
                    try:
                        client.options["defaultType"] = account_type
                        positions = await client.fetch_positions()
                        if positions:
                            break
                    except Exception as inner:
                        logger.debug(f"[{self.name}] fetch_positions fallback {account_type} failed: {inner}")
                    finally:
                        if original_default_type is None:
                            client.options.pop("defaultType", None)
                        else:
                            client.options["defaultType"] = original_default_type

            result = []
            for pos in positions:
                raw_contracts = float(pos.get("contracts", 0) or 0)
                if abs(raw_contracts) > 0:
                    raw_side = str(pos.get("side", "") or "").strip().lower()
                    if raw_side not in {"long", "short"}:
                        raw_side = "short" if raw_contracts < 0 else "long"
                    result.append(
                        Position(
                            symbol=pos.get("symbol", ""),
                            side=raw_side,
                            amount=abs(raw_contracts),
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
            client = await self._ensure_client()
            since_ms = int(since.timestamp() * 1000) if since else None
            return await client.fetch_my_trades(symbol, since=since_ms, limit=limit or 100)
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
            timestamp=datetime.fromtimestamp(ccxt_order.get("timestamp", 0) / 1000)
            if ccxt_order.get("timestamp")
            else None,
            exchange=self.name,
        )

    async def get_deposit_address(self, currency: str) -> str:
        try:
            client = await self._ensure_client()
            address = await client.fetch_deposit_address(currency)
            return address.get("address", "")
        except Exception as e:
            self._handle_error(e, f"get_deposit_address({currency})")

    async def withdraw(
        self,
        currency: str,
        amount: float,
        address: str,
        params: Optional[dict] = None,
    ) -> dict:
        try:
            client = await self._ensure_client()
            result = await client.withdraw(currency, amount, address, params or {})
            logger.info(f"[{self.name}] Withdrawal initiated: {amount} {currency} to {address}")
            return result
        except Exception as e:
            self._handle_error(e, f"withdraw({currency}, {amount})")
