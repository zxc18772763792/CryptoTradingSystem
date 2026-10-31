"""Keep derivative bars out of the spot kline store.

``data/historical/<exchange>/`` holds spot bars; ``binance_futures/`` holds the
USD-M perpetuals. The shared trading connector follows ``<EXCHANGE>_DEFAULT_TYPE``
and, with ``BINANCE_DEFAULT_TYPE=future``, ccxt resolves ``BTC/USDT`` to the
``BTC/USDT:USDT`` perpetual, so its ``get_klines`` and ``fetch_trades`` return perp
data. Writers that persist connector data must check the venue first.
"""
from __future__ import annotations

from typing import Any

SPOT = "spot"
_DERIVATIVE_TYPES = {"future", "swap", "delivery"}
_ALIASES = {"futures": "future", "perp": "swap", "perpetual": "swap", "linear": "future"}


class KlineVenueError(ValueError):
    """Bars from one market type were about to be stored under another."""


def normalize_market_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return _ALIASES.get(raw, raw)


def connector_market_type(connector: Any) -> str:
    """The market a connector's OHLCV/trade calls resolve to.

    Connectors carry it as ``config.default_type`` (trading connectors) or
    ``default_type`` (the public ccxt client). Objects with neither, such as
    test doubles, are treated as spot, matching ccxt's own default.
    """
    for value in (
        getattr(getattr(connector, "config", None), "default_type", None),
        getattr(connector, "default_type", None),
    ):
        if isinstance(value, str) and value.strip():
            return normalize_market_type(value)
    return SPOT


def exchange_market_type(exchange: str, connector: Any = None) -> str:
    """Market served by ``exchange``'s shared connector, or by its setting when none exists yet."""
    if connector is not None:
        return connector_market_type(connector)
    from config.settings import settings

    configured = getattr(settings, f"{str(exchange or '').strip().upper()}_DEFAULT_TYPE", SPOT)
    return normalize_market_type(configured) or SPOT


def store_market_type(exchange: str) -> str:
    """Market type held by a store namespace (``binance_futures`` holds perps)."""
    return "future" if str(exchange or "").strip().lower().endswith("_futures") else SPOT


def market_data_store(exchange: str, market_type: Any) -> str:
    """Local OHLCV namespace for a feed, including reads used as a fallback."""
    market = normalize_market_type(market_type) or SPOT
    base = str(exchange or "").strip().lower().removesuffix("_futures")
    if market in _DERIVATIVE_TYPES:
        return f"{base}_futures"
    if market in {SPOT, "margin"}:
        return base
    raise KlineVenueError(f"Unsupported kline market type: {market}")


def venue_matches_store(exchange: str, market_type: Any) -> bool:
    market = normalize_market_type(market_type) or SPOT
    if store_market_type(exchange) == SPOT:
        return market == SPOT
    return market in _DERIVATIVE_TYPES


def require_store_venue(exchange: str, symbol: str, timeframe: str, market_type: Any) -> None:
    if not venue_matches_store(exchange, market_type):
        raise KlineVenueError(
            f"Refusing to store {normalize_market_type(market_type)} bars for {symbol} {timeframe} "
            f"in the {store_market_type(exchange)} store '{exchange}'"
        )
