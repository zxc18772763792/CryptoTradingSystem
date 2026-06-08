"""Binance perpetual marketdata WS client skeleton."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from core.marketdata.hub import normalize_market_symbol
from core.marketdata.ws_client import WSClient, WSClientConfig


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _iso_from_ms(value: Any) -> Optional[str]:
    ts = _as_int(value)
    if ts is None or ts <= 0:
        return None
    return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat()


def _price_levels(value: Any) -> list[list[float]]:
    if not isinstance(value, list):
        return []
    out: list[list[float]] = []
    for row in value:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price = _as_float(row[0])
        quantity = _as_float(row[1])
        if price is None or quantity is None:
            continue
        out.append([price, quantity])
    return out


class BinancePerpWSClient(WSClient):
    """Skeleton wrapper for Binance Futures websocket subscriptions."""

    def __init__(self, symbols: Iterable[str] | None = None, base_url: str = "wss://fstream.binance.com/stream"):
        super().__init__(WSClientConfig(url=base_url, name="binance_perp_ws"))
        self.symbols = [str(s).upper().replace("/", "") for s in (symbols or [])]

    async def subscribe_book_ticker(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "book_ticker", "symbols": syms})

    async def subscribe_depth(self, symbols: Iterable[str] | None = None, level: int = 20) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "depth", "symbols": syms, "level": int(level)})

    async def subscribe_agg_trade(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "agg_trade", "symbols": syms})

    async def subscribe_kline(self, symbols: Iterable[str] | None = None, interval: str = "5m") -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "kline", "symbols": syms, "interval": interval})

    async def subscribe_mark_price(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "mark_price", "symbols": syms})

    async def subscribe_funding(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "funding", "symbols": syms})

    @staticmethod
    def normalize_event(message: Dict) -> Dict:
        """Normalize Binance Futures raw WS events into the app's market shape.

        The transport is still a skeleton, but this parser is deliberately
        concrete so accidental callers do not receive opaque Binance payloads.
        """
        raw = dict(message or {})
        data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
        stream = raw.get("stream") if data is not raw else None
        event_name = str(data.get("e") or "").strip()
        kline = data.get("k") if isinstance(data.get("k"), dict) else {}
        raw_symbol = data.get("s") or data.get("ps") or kline.get("s")
        symbol = normalize_market_symbol(raw_symbol)
        timestamp_ms = _as_int(data.get("E") or data.get("T") or kline.get("T") or kline.get("t"))

        out: Dict[str, Any] = {
            "exchange": "binance",
            "type": "unknown",
            "event_type": event_name or None,
            "symbol": symbol,
            "raw_symbol": str(raw_symbol) if raw_symbol else None,
            "timestamp": _iso_from_ms(timestamp_ms),
            "timestamp_ms": timestamp_ms,
            "raw": data,
        }
        if stream:
            out["stream"] = stream

        if event_name == "depthUpdate":
            out.update(
                {
                    "type": "depth",
                    "bids": _price_levels(data.get("b")),
                    "asks": _price_levels(data.get("a")),
                    "first_update_id": _as_int(data.get("U")),
                    "final_update_id": _as_int(data.get("u")),
                    "previous_final_update_id": _as_int(data.get("pu")),
                }
            )
            return out

        if event_name == "bookTicker" or {"b", "a"}.issubset(data.keys()):
            out.update(
                {
                    "type": "book_ticker",
                    "bid": _as_float(data.get("b")),
                    "bid_size": _as_float(data.get("B")),
                    "ask": _as_float(data.get("a")),
                    "ask_size": _as_float(data.get("A")),
                    "sequence": _as_int(data.get("u")),
                }
            )
            return out

        if event_name == "aggTrade":
            price = _as_float(data.get("p"))
            out.update(
                {
                    "type": "agg_trade",
                    "last": price,
                    "price": price,
                    "quantity": _as_float(data.get("q")),
                    "trade_id": _as_int(data.get("a")),
                    "first_trade_id": _as_int(data.get("f")),
                    "last_trade_id": _as_int(data.get("l")),
                    "trade_time_ms": _as_int(data.get("T")),
                    "is_buyer_maker": data.get("m") if isinstance(data.get("m"), bool) else None,
                }
            )
            return out

        if event_name == "kline":
            close = _as_float(kline.get("c"))
            out.update(
                {
                    "type": "kline",
                    "timeframe": kline.get("i"),
                    "open": _as_float(kline.get("o")),
                    "high": _as_float(kline.get("h")),
                    "low": _as_float(kline.get("l")),
                    "close": close,
                    "last": close,
                    "volume": _as_float(kline.get("v")),
                    "quote_volume": _as_float(kline.get("q")),
                    "trade_count": _as_int(kline.get("n")),
                    "kline_start_time": _as_int(kline.get("t")),
                    "kline_close_time": _as_int(kline.get("T")),
                    "is_closed": kline.get("x") if isinstance(kline.get("x"), bool) else None,
                }
            )
            return out

        if event_name == "markPriceUpdate":
            mark = _as_float(data.get("p"))
            out.update(
                {
                    "type": "mark_price",
                    "last": mark,
                    "mark": mark,
                    "index": _as_float(data.get("i")),
                    "estimated_settle_price": _as_float(data.get("P")),
                    "funding_rate": _as_float(data.get("r")),
                    "next_funding_time": _as_int(data.get("T")),
                }
            )
            return out

        return out
