"""Runtime price reads backed by MarketDataHub with REST fallback.

This module is the narrow adapter that strategy/runtime code should use when
it needs a current ticker price. It keeps the price and its freshness/source
metadata together so live code can fail closed instead of silently consuming a
stale or unverifiable number.
"""
from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Any, Callable, Optional

from core.marketdata.hub import (
    MarketDataHub,
    market_data_hub,
    normalize_exchange_name,
    normalize_market_symbol,
)


class PriceUnavailableError(RuntimeError):
    """Raised when a live-safe price read cannot be satisfied."""


@dataclass(frozen=True)
class PriceReadResult:
    exchange: str
    symbol: str
    price: Optional[float]
    bid: Optional[float] = None
    ask: Optional[float] = None
    source: str = "unavailable"
    age_ms: Optional[int] = None
    is_stale: bool = True
    fallback_required: bool = True
    reason: str = "unavailable"
    channel: str = "ticker"
    raw_symbol: Optional[str] = None
    hub_quality: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.price is not None and math.isfinite(self.price) and self.price > 0 and not self.is_stale

    def to_metadata(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "price": self.price,
            "bid": self.bid,
            "ask": self.ask,
            "source": self.source,
            "age_ms": self.age_ms,
            "is_stale": self.is_stale,
            "fallback_required": self.fallback_required,
            "reason": self.reason,
            "channel": self.channel,
            "raw_symbol": self.raw_symbol,
            "hub_quality": self.hub_quality,
        }


def _field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict):
            if name in obj:
                return obj.get(name)
        else:
            value = getattr(obj, name, None)
            if value is not None:
                return value
    return None


def _float_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _positive_or_none(value: Any) -> Optional[float]:
    out = _float_or_none(value)
    if out is None or out <= 0:
        return None
    return out


def _price_from_fields(
    *,
    last: Optional[float],
    bid: Optional[float],
    ask: Optional[float],
    mark: Optional[float] = None,
) -> Optional[float]:
    if last and last > 0:
        return float(last)
    if mark and mark > 0:
        return float(mark)
    if bid and ask and bid > 0 and ask > 0:
        return float((bid + ask) / 2.0)
    if bid and bid > 0:
        return float(bid)
    if ask and ask > 0:
        return float(ask)
    return None


def _ticker_payload(ticker: Any, fallback_symbol: str) -> dict[str, Any]:
    return {
        "last": _field(ticker, "last", "close", "price"),
        "bid": _field(ticker, "bid"),
        "ask": _field(ticker, "ask"),
        "mark": _field(ticker, "mark", "mark_price", "markPrice"),
        "volume": _field(ticker, "volume", "volume_24h", "baseVolume"),
        "timestamp": _field(ticker, "timestamp", "datetime"),
        "raw_symbol": _field(ticker, "symbol") or fallback_symbol,
    }


def _result_from_hub_payload(
    exchange: str,
    symbol: str,
    current: dict[str, Any],
    *,
    reason: str,
) -> PriceReadResult:
    tick = dict(current.get("tick") or {})
    meta = dict(current.get("meta") or {})
    last = _positive_or_none(tick.get("last"))
    bid = _positive_or_none(tick.get("bid"))
    ask = _positive_or_none(tick.get("ask"))
    mark = _positive_or_none(tick.get("mark"))
    price = _price_from_fields(last=last, bid=bid, ask=ask, mark=mark)
    is_stale = bool(meta.get("is_stale", True))
    return PriceReadResult(
        exchange=exchange,
        symbol=symbol,
        price=price,
        bid=bid,
        ask=ask,
        source=str(meta.get("source") or tick.get("source") or "hub"),
        age_ms=int(meta["age_ms"]) if meta.get("age_ms") is not None else None,
        is_stale=is_stale,
        fallback_required=bool(meta.get("fallback_required", is_stale)),
        reason=reason,
        channel=str(tick.get("channel") or "ticker"),
        raw_symbol=str(tick.get("raw_symbol") or symbol),
        hub_quality=str(meta.get("quality") or ""),
    )


def _unavailable(
    exchange: str,
    symbol: str,
    *,
    reason: str,
    fail_closed: bool,
    stale_result: Optional[PriceReadResult] = None,
) -> PriceReadResult:
    if fail_closed:
        raise PriceUnavailableError(f"market_data_unavailable:{exchange}:{symbol}:{reason}")
    if stale_result is not None:
        return PriceReadResult(
            exchange=stale_result.exchange,
            symbol=stale_result.symbol,
            price=stale_result.price,
            bid=stale_result.bid,
            ask=stale_result.ask,
            source=stale_result.source,
            age_ms=stale_result.age_ms,
            is_stale=True,
            fallback_required=True,
            reason=reason,
            channel=stale_result.channel,
            raw_symbol=stale_result.raw_symbol,
            hub_quality=stale_result.hub_quality,
        )
    return PriceReadResult(exchange=exchange, symbol=symbol, price=None, reason=reason)


async def get_realtime_price(
    exchange: Any,
    symbol: Any,
    *,
    hub: MarketDataHub = market_data_hub,
    connector: Any = None,
    connector_resolver: Optional[Callable[[str], Any]] = None,
    max_age_sec: Optional[float] = None,
    allow_rest_fallback: bool = True,
    fail_closed: bool = False,
    rest_timeout_sec: float = 3.0,
) -> PriceReadResult:
    """Read a current price plus source/freshness metadata.

    The function prefers a fresh hub tick. When the hub value is missing, stale,
    or malformed, it can fall back to the provided REST connector and records
    that fallback back into the hub as `rest_fallback`.
    """
    name = normalize_exchange_name(exchange)
    norm_symbol = normalize_market_symbol(symbol)
    if not name or not norm_symbol:
        return _unavailable(name, norm_symbol, reason="invalid_request", fail_closed=fail_closed)

    stale_result: Optional[PriceReadResult] = None
    current = hub.get_tick(name, norm_symbol, max_age_sec=max_age_sec)
    if current is not None:
        result = _result_from_hub_payload(name, norm_symbol, current, reason="hub_fresh")
        if result.ok:
            return result
        stale_result = result
        hub_reason = "hub_stale" if result.is_stale else "hub_invalid_price"
    else:
        hub_reason = "hub_missing"

    if not allow_rest_fallback:
        return _unavailable(
            name,
            norm_symbol,
            reason=hub_reason,
            fail_closed=fail_closed,
            stale_result=stale_result,
        )

    if connector is None and connector_resolver is not None:
        connector = connector_resolver(name)
    if connector is None:
        return _unavailable(
            name,
            norm_symbol,
            reason="connector_missing",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )

    try:
        ticker = await asyncio.wait_for(
            connector.get_ticker(norm_symbol),
            timeout=max(0.1, float(rest_timeout_sec or 3.0)),
        )
    except Exception as exc:
        return _unavailable(
            name,
            norm_symbol,
            reason=f"rest_error:{type(exc).__name__}",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )

    raw_exchange = _field(ticker, "exchange")
    if raw_exchange and normalize_exchange_name(raw_exchange) != name:
        return _unavailable(
            name,
            norm_symbol,
            reason="exchange_mismatch",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )
    raw_symbol = _field(ticker, "symbol")
    if raw_symbol and normalize_market_symbol(raw_symbol) != norm_symbol:
        return _unavailable(
            name,
            norm_symbol,
            reason="symbol_mismatch",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )

    payload = _ticker_payload(ticker, norm_symbol)
    tick = hub.upsert_rest_tick(name, norm_symbol, payload, source="rest_fallback", reason=hub_reason)
    if tick is None:
        return _unavailable(
            name,
            norm_symbol,
            reason="rest_invalid_payload",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )

    current = hub.get_tick(name, norm_symbol, max_age_sec=max_age_sec)
    if current is None:
        return _unavailable(
            name,
            norm_symbol,
            reason="rest_missing_after_write",
            fail_closed=fail_closed,
            stale_result=stale_result,
        )
    result = _result_from_hub_payload(name, norm_symbol, current, reason=f"rest_fallback:{hub_reason}")
    if result.ok:
        return result
    return _unavailable(
        name,
        norm_symbol,
        reason="rest_stale_after_write" if result.is_stale else "rest_invalid_price",
        fail_closed=fail_closed,
        stale_result=result,
    )


async def require_realtime_price(*args: Any, **kwargs: Any) -> PriceReadResult:
    kwargs["fail_closed"] = True
    result = await get_realtime_price(*args, **kwargs)
    if not result.ok:
        raise PriceUnavailableError(
            f"market_data_unavailable:{result.exchange}:{result.symbol}:{result.reason}"
        )
    return result
