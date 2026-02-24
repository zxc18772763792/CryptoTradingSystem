"""CCXT-based exchange adapter (read-only methods first, execution methods TODO)."""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import ccxt
from loguru import logger

from core.exchange_adapters.base import (
    ExchangeAdapter,
    ExchangeOrderRequest,
    ExchangeOrderSnapshot,
    ExchangePositionSnapshot,
    FundingRatePoint,
    MarketInfo,
)


def _to_dt_ms(ms: Any) -> Optional[datetime]:
    try:
        if ms is None:
            return None
        v = int(float(ms))
        if v <= 0:
            return None
        if v > 10**12:  # micro/nano guard
            v = v // 1000
        return datetime.utcfromtimestamp(v / 1000.0 if v > 10**10 else v)
    except Exception:
        return None


def _normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if "/" in s:
        return s
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}/USDT"
    return s


class CCXTExchangeAdapter(ExchangeAdapter):
    """Incremental CCXT adapter.

    Current scope:
    - read-only REST methods implemented (markets/ticker/balances/positions/funding)
    - execution methods intentionally left TODO to avoid changing live path
    """

    def __init__(
        self,
        exchange: str = "binance",
        market_type: str = "swap",
        api_key: str = "",
        secret: str = "",
        password: str = "",
        options: Optional[Dict[str, Any]] = None,
    ):
        self.exchange = str(exchange).lower()
        self.market_type = str(market_type).lower()
        self.api_key = api_key
        self.secret = secret
        self.password = password
        self.options = dict(options or {})
        self._client: Any = None
        self._markets_cache: Dict[str, MarketInfo] = {}

    def _build_client(self) -> Any:
        if not hasattr(ccxt, self.exchange):
            raise ValueError(f"ccxt exchange not found: {self.exchange}")
        klass = getattr(ccxt, self.exchange)
        opts = {
            "enableRateLimit": True,
            "timeout": int(self.options.get("timeout", 30000)),
            "apiKey": self.api_key or None,
            "secret": self.secret or None,
            "password": self.password or None,
            "options": {"defaultType": self.market_type, **dict(self.options.get("ccxt_options") or {})},
        }
        proxies = {}
        if self.options.get("http_proxy"):
            proxies["http"] = self.options["http_proxy"]
        if self.options.get("https_proxy"):
            proxies["https"] = self.options["https_proxy"]
        if proxies:
            opts["proxies"] = proxies
        return klass(opts)

    async def _call(self, fn_name: str, *args, **kwargs):
        if self._client is None:
            raise RuntimeError("Adapter not initialized")
        fn = getattr(self._client, fn_name)
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def initialize(self) -> None:
        if self._client is None:
            self._client = self._build_client()
        await self.fetch_markets(reload=True)
        logger.info(f"CCXT adapter initialized exchange={self.exchange} market_type={self.market_type}")

    async def close(self) -> None:
        if self._client is None:
            return
        close_fn = getattr(self._client, "close", None)
        if callable(close_fn):
            try:
                await asyncio.to_thread(close_fn)
            except Exception:
                pass
        self._client = None
        logger.debug("CCXT adapter closed")

    async def fetch_markets(self, reload: bool = False) -> List[MarketInfo]:
        raw = await self._call("load_markets", bool(reload))
        cache: Dict[str, MarketInfo] = {}
        for _, m in (raw or {}).items():
            try:
                market_type = "spot"
                if bool(m.get("swap")):
                    market_type = "swap"
                elif bool(m.get("future")):
                    market_type = "future"
                elif bool(m.get("margin")):
                    market_type = "margin"
                info = MarketInfo(
                    symbol=_normalize_symbol(str(m.get("symbol") or "")),
                    exchange=self.exchange,
                    market_type=market_type,
                    base=str(m.get("base") or ""),
                    quote=str(m.get("quote") or ""),
                    active=bool(m.get("active", True)),
                    price_precision=(m.get("precision") or {}).get("price"),
                    amount_precision=(m.get("precision") or {}).get("amount"),
                    min_qty=((m.get("limits") or {}).get("amount") or {}).get("min"),
                    min_notional=((m.get("limits") or {}).get("cost") or {}).get("min"),
                    extra={"raw_id": m.get("id"), "contract": bool(m.get("contract", False))},
                )
                if info.symbol:
                    cache[info.symbol] = info
            except Exception:
                continue
        self._markets_cache = cache
        return list(cache.values())

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        s = _normalize_symbol(symbol)
        raw = await self._call("fetch_ticker", s)
        return {
            "exchange": self.exchange,
            "market_type": self.market_type,
            "symbol": s,
            "timestamp": _to_dt_ms(raw.get("timestamp")).isoformat() if _to_dt_ms(raw.get("timestamp")) else None,
            "last": raw.get("last"),
            "bid": raw.get("bid"),
            "ask": raw.get("ask"),
            "high": raw.get("high"),
            "low": raw.get("low"),
            "base_volume": raw.get("baseVolume"),
            "quote_volume": raw.get("quoteVolume"),
            "info": raw.get("info", {}),
        }

    async def fetch_balances(self) -> Dict[str, Any]:
        raw = await self._call("fetch_balance")
        balances: List[Dict[str, Any]] = []
        for asset, row in (raw or {}).items():
            if not isinstance(row, dict):
                continue
            if asset in {"info", "free", "used", "total"}:
                continue
            total = row.get("total")
            free = row.get("free")
            used = row.get("used")
            if all(v in (None, 0, 0.0) for v in [total, free, used]):
                continue
            balances.append({"asset": asset, "free": free, "used": used, "total": total})
        return {"exchange": self.exchange, "market_type": self.market_type, "balances": balances, "raw_info": (raw or {}).get("info")}

    async def fetch_positions(self, symbols: Optional[List[str]] = None) -> List[ExchangePositionSnapshot]:
        has_fetch_positions = bool((getattr(self._client, "has", {}) or {}).get("fetchPositions"))
        if not has_fetch_positions:
            return []
        norm_symbols = [_normalize_symbol(s) for s in (symbols or [])] or None
        raw_positions = await self._call("fetch_positions", norm_symbols)
        out: List[ExchangePositionSnapshot] = []
        for p in raw_positions or []:
            try:
                contracts = float(p.get("contracts") or p.get("positionAmt") or p.get("contractsSize") or p.get("size") or 0.0)
                side_raw = str(p.get("side") or "").lower()
                if side_raw not in {"long", "short"}:
                    if contracts > 0:
                        side_raw = "long"
                    elif contracts < 0:
                        side_raw = "short"
                    else:
                        side_raw = "flat"
                qty = abs(contracts)
                out.append(
                    ExchangePositionSnapshot(
                        symbol=_normalize_symbol(str(p.get("symbol") or "")),
                        side=side_raw,
                        quantity=qty,
                        entry_price=float(p.get("entryPrice") or p.get("entry_price") or 0.0),
                        mark_price=(float(p.get("markPrice")) if p.get("markPrice") is not None else None),
                        unrealized_pnl=(float(p.get("unrealizedPnl")) if p.get("unrealizedPnl") is not None else None),
                        leverage=(float(p.get("leverage")) if p.get("leverage") is not None else None),
                        margin_mode=p.get("marginMode"),
                        liquidation_price=(float(p.get("liquidationPrice")) if p.get("liquidationPrice") is not None else None),
                        extra={"raw": p},
                    )
                )
            except Exception:
                continue
        return out

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        raise NotImplementedError("TODO: wire to ccxt create_order (defer until state-machine integration)")

    async def cancel_order(self, symbol: str, order_id: str, params: Optional[Dict[str, Any]] = None) -> ExchangeOrderSnapshot:
        raise NotImplementedError("TODO: wire to ccxt cancel_order (defer until state-machine integration)")

    async def fetch_order(self, symbol: str, order_id: str) -> ExchangeOrderSnapshot:
        raise NotImplementedError("TODO: wire to ccxt fetch_order (defer until state-machine integration)")

    async def fetch_open_orders(self, symbol: Optional[str] = None) -> List[ExchangeOrderSnapshot]:
        has_fetch_open = bool((getattr(self._client, "has", {}) or {}).get("fetchOpenOrders"))
        if not has_fetch_open:
            return []
        sym = _normalize_symbol(symbol) if symbol else None
        raw_orders = await self._call("fetch_open_orders", sym)
        out: List[ExchangeOrderSnapshot] = []
        for o in raw_orders or []:
            try:
                amt = float(o.get("amount") or 0.0)
                filled = float(o.get("filled") or 0.0)
                out.append(
                    ExchangeOrderSnapshot(
                        order_id=str(o.get("id") or ""),
                        client_order_id=o.get("clientOrderId"),
                        symbol=_normalize_symbol(str(o.get("symbol") or sym or "")),
                        status=str(o.get("status") or "unknown"),
                        side=str(o.get("side") or ""),
                        order_type=str(o.get("type") or ""),
                        amount=amt,
                        filled=filled,
                        remaining=float(o.get("remaining") if o.get("remaining") is not None else max(0.0, amt - filled)),
                        price=(float(o.get("price")) if o.get("price") is not None else None),
                        avg_price=(float(o.get("average")) if o.get("average") is not None else None),
                        fee=(float((o.get("fee") or {}).get("cost")) if isinstance(o.get("fee"), dict) and (o.get("fee") or {}).get("cost") is not None else None),
                        fee_currency=((o.get("fee") or {}).get("currency") if isinstance(o.get("fee"), dict) else None),
                        timestamp=_to_dt_ms(o.get("timestamp")),
                        raw=o,
                    )
                )
            except Exception:
                continue
        return out

    async def fetch_funding_rate(self, symbol: str) -> Optional[FundingRatePoint]:
        has_fetch = bool((getattr(self._client, "has", {}) or {}).get("fetchFundingRate"))
        if not has_fetch:
            return None
        s = _normalize_symbol(symbol)
        try:
            raw = await self._call("fetch_funding_rate", s)
        except Exception as e:
            logger.warning(f"fetch_funding_rate failed {s}: {e}")
            return None
        rate = raw.get("fundingRate")
        if rate is None:
            return None
        ts = _to_dt_ms(raw.get("timestamp")) or _to_dt_ms(raw.get("fundingTimestamp"))
        return FundingRatePoint(
            symbol=s,
            timestamp=ts or datetime.utcnow(),
            rate=float(rate),
            mark_price=(float(raw.get("markPrice")) if raw.get("markPrice") is not None else None),
            index_price=(float(raw.get("indexPrice")) if raw.get("indexPrice") is not None else None),
            source=self.exchange,
        )

    async def fetch_funding_history(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[FundingRatePoint]:
        has_hist = bool((getattr(self._client, "has", {}) or {}).get("fetchFundingRateHistory"))
        if not has_hist:
            return []
        s = _normalize_symbol(symbol)
        params: Dict[str, Any] = {}
        since = int(start_time.timestamp() * 1000) if start_time else None
        try:
            rows = await self._call("fetch_funding_rate_history", s, since, int(limit), params)
        except Exception as e:
            logger.warning(f"fetch_funding_rate_history failed {s}: {e}")
            return []
        out: List[FundingRatePoint] = []
        for r in rows or []:
            try:
                ts = _to_dt_ms(r.get("timestamp")) or _to_dt_ms(r.get("fundingTimestamp"))
                if ts is None:
                    continue
                if end_time and ts > end_time:
                    continue
                rate = r.get("fundingRate")
                if rate is None:
                    continue
                out.append(
                    FundingRatePoint(
                        symbol=s,
                        timestamp=ts,
                        rate=float(rate),
                        mark_price=(float(r.get("markPrice")) if r.get("markPrice") is not None else None),
                        index_price=(float(r.get("indexPrice")) if r.get("indexPrice") is not None else None),
                        source=self.exchange,
                    )
                )
            except Exception:
                continue
        return out

