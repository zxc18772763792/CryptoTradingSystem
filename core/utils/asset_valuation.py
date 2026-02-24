"""Helpers for converting exchange asset balances to USD-equivalent value."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, Optional, Sequence, Set

from loguru import logger

STABLE_COINS: Set[str] = {"USDT", "USDC", "USD", "BUSD"}
_STABLE_ORDER: Sequence[str] = ("USDT", "USDC", "BUSD", "USD")
_BRIDGE_ASSETS: Sequence[str] = ("BTC", "ETH", "BNB", "OKB")


def _normalize_currency(currency: str) -> str:
    return str(currency or "").strip().upper()


def _extract_market_symbols(connector: Any) -> Optional[Set[str]]:
    try:
        client = getattr(connector, "_client", None)
        markets = getattr(client, "markets", None)
        if isinstance(markets, dict) and markets:
            return {str(k) for k in markets.keys()}
    except Exception:
        return None
    return None


async def _fetch_last_price(
    connector: Any,
    symbol: str,
    timeout_sec: float,
    price_cache: Dict[str, float],
    market_symbols: Optional[Set[str]],
) -> float:
    key = str(symbol or "").strip()
    if not key:
        return 0.0

    if key in price_cache:
        return float(price_cache[key] or 0.0)

    if market_symbols is not None and key not in market_symbols:
        price_cache[key] = 0.0
        return 0.0

    try:
        ticker = await asyncio.wait_for(connector.get_ticker(key), timeout=max(0.1, float(timeout_sec)))
        last = float((ticker.last if ticker else 0.0) or 0.0)
        if last > 0:
            price_cache[key] = last
            return last
    except Exception:
        pass

    price_cache[key] = 0.0
    return 0.0


async def _resolve_bridge_usd_prices(
    connector: Any,
    timeout_sec: float,
    price_cache: Dict[str, float],
    market_symbols: Optional[Set[str]],
    allow_slow_fallback: bool = False,
) -> Dict[str, float]:
    bridge_usd: Dict[str, float] = {coin: 1.0 for coin in STABLE_COINS}

    for bridge in _BRIDGE_ASSETS:
        if bridge in STABLE_COINS:
            bridge_usd[bridge] = 1.0
            continue

        px = 0.0
        # Fast path: direct bridge/stable quote is the common case.
        for stable in _STABLE_ORDER:
            px = await _fetch_last_price(
                connector=connector,
                symbol=f"{bridge}/{stable}",
                timeout_sec=timeout_sec,
                price_cache=price_cache,
                market_symbols=market_symbols,
            )
            if px > 0:
                break
        # Slow fallback: reverse quote.
        if px <= 0 and allow_slow_fallback:
            for stable in _STABLE_ORDER:
                inv = await _fetch_last_price(
                    connector=connector,
                    symbol=f"{stable}/{bridge}",
                    timeout_sec=timeout_sec,
                    price_cache=price_cache,
                    market_symbols=market_symbols,
                )
                if inv > 0:
                    px = 1.0 / inv
                    break
        if px > 0:
            bridge_usd[bridge] = px

    return bridge_usd


async def resolve_currency_usd_price(
    connector: Any,
    currency: str,
    timeout_sec: float = 2.0,
    price_cache: Optional[Dict[str, float]] = None,
    bridge_usd: Optional[Dict[str, float]] = None,
    market_symbols: Optional[Set[str]] = None,
    allow_slow_fallback: bool = False,
) -> float:
    ccy = _normalize_currency(currency)
    if not ccy:
        return 0.0
    if ccy in STABLE_COINS:
        return 1.0

    cache = price_cache if price_cache is not None else {}
    symbols = market_symbols if market_symbols is not None else _extract_market_symbols(connector)
    bridges = bridge_usd
    if bridges is None:
        bridges = await _resolve_bridge_usd_prices(
            connector=connector,
            timeout_sec=timeout_sec,
            price_cache=cache,
            market_symbols=symbols,
            allow_slow_fallback=allow_slow_fallback,
        )

    # Fast path 1: direct stable quote (e.g. ETH/USDT).
    for stable in _STABLE_ORDER:
        direct = await _fetch_last_price(
            connector=connector,
            symbol=f"{ccy}/{stable}",
            timeout_sec=timeout_sec,
            price_cache=cache,
            market_symbols=symbols,
        )
        if direct > 0:
            return direct

    # Fast path 2: bridge quote (e.g. ALT/BTC * BTC/USDT).
    for bridge in _BRIDGE_ASSETS:
        bridge_px = float(bridges.get(bridge, 0.0) or 0.0)
        if bridge_px <= 0 or bridge == ccy:
            continue

        direct = await _fetch_last_price(
            connector=connector,
            symbol=f"{ccy}/{bridge}",
            timeout_sec=timeout_sec,
            price_cache=cache,
            market_symbols=symbols,
        )
        if direct > 0:
            return direct * bridge_px

    if allow_slow_fallback:
        # Slow fallback 1: reverse stable quote.
        for stable in _STABLE_ORDER:
            inverse = await _fetch_last_price(
                connector=connector,
                symbol=f"{stable}/{ccy}",
                timeout_sec=timeout_sec,
                price_cache=cache,
                market_symbols=symbols,
            )
            if inverse > 0:
                return 1.0 / inverse

        # Slow fallback 2: reverse bridge quote.
        for bridge in _BRIDGE_ASSETS:
            bridge_px = float(bridges.get(bridge, 0.0) or 0.0)
            if bridge_px <= 0 or bridge == ccy:
                continue
            inverse = await _fetch_last_price(
                connector=connector,
                symbol=f"{bridge}/{ccy}",
                timeout_sec=timeout_sec,
                price_cache=cache,
                market_symbols=symbols,
            )
            if inverse > 0:
                return bridge_px / inverse

    return 0.0


async def build_currency_usd_quotes(
    connector: Any,
    currencies: Iterable[str],
    timeout_sec: float = 2.0,
    max_parallel: int = 24,
    allow_slow_fallback: bool = False,
) -> Dict[str, float]:
    normalized = []
    seen = set()
    for raw in currencies:
        ccy = _normalize_currency(raw)
        if not ccy or ccy in STABLE_COINS or ccy in seen:
            continue
        normalized.append(ccy)
        seen.add(ccy)

    if not normalized:
        return {}

    market_symbols = _extract_market_symbols(connector)
    price_cache: Dict[str, float] = {}
    bridge_usd = await _resolve_bridge_usd_prices(
        connector=connector,
        timeout_sec=timeout_sec,
        price_cache=price_cache,
        market_symbols=market_symbols,
        allow_slow_fallback=allow_slow_fallback,
    )

    semaphore = asyncio.Semaphore(max(1, int(max_parallel)))
    quotes: Dict[str, float] = {}

    async def _resolve_one(ccy: str) -> None:
        async with semaphore:
            try:
                quotes[ccy] = await resolve_currency_usd_price(
                    connector=connector,
                    currency=ccy,
                    timeout_sec=timeout_sec,
                    price_cache=price_cache,
                    bridge_usd=bridge_usd,
                    market_symbols=market_symbols,
                    allow_slow_fallback=allow_slow_fallback,
                )
            except Exception as e:
                logger.debug(f"Failed to resolve usd quote for {ccy}: {e}")
                quotes[ccy] = 0.0

    await asyncio.gather(*[_resolve_one(ccy) for ccy in normalized], return_exceptions=False)
    return {ccy: float(px or 0.0) for ccy, px in quotes.items() if float(px or 0.0) > 0}
