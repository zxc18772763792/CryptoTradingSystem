from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import requests
import websockets

from prediction_markets.polymarket.utils import AsyncTokenBucket, CircuitBreaker, async_retry, parse_ts_any, utc_now


class ClobReader:
    """Read-only Polymarket CLOB reader using WS primary + REST fallback."""

    def __init__(
        self,
        base_url: str = "https://clob.polymarket.com",
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        request_timeout_sec: int = 8,
        burst: int = 20,
        refill_per_sec: float = 5.0,
        max_concurrency: int = 6,
        breaker_errors: int = 8,
        breaker_cooldown_sec: int = 120,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.ws_url = str(ws_url).strip()
        self.request_timeout_sec = max(2, int(request_timeout_sec or 8))
        self.rate_limiter = AsyncTokenBucket(burst=burst, refill_per_sec=refill_per_sec)
        self.breaker = CircuitBreaker(error_threshold=breaker_errors, cooldown_sec=breaker_cooldown_sec)
        self.semaphore = asyncio.Semaphore(max(1, int(max_concurrency or 6)))
        self.headers = {
            "User-Agent": "CryptoTradingSystem/PolymarketClobReader",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.ws_connected = False
        self._last_ws_error: Optional[str] = None

    async def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json_payload: Any = None) -> Any:
        if not self.breaker.allow():
            raise RuntimeError("clob circuit breaker is open")
        await self.rate_limiter.acquire()

        async def _do() -> Any:
            async with self.semaphore:
                def _sync_request() -> Any:
                    resp = requests.request(
                        method.upper(),
                        f"{self.base_url}{path}",
                        params=params,
                        json=json_payload,
                        headers=self.headers,
                        timeout=self.request_timeout_sec,
                    )
                    resp.raise_for_status()
                    return resp.json()

                return await asyncio.to_thread(_sync_request)

        try:
            data = await async_retry(_do, retries=3, base_delay=0.5, max_delay=3.0)
            self.breaker.on_success()
            return data
        except Exception:
            self.breaker.on_error()
            raise

    @staticmethod
    def _normalize_orderbook_side(levels: Iterable[Any], limit: int = 5) -> List[Dict[str, float]]:
        out: List[Dict[str, float]] = []
        for raw in list(levels or [])[:limit]:
            price = 0.0
            size = 0.0
            if isinstance(raw, dict):
                price = float(raw.get("price") or raw.get("p") or 0.0)
                size = float(raw.get("size") or raw.get("s") or raw.get("quantity") or 0.0)
            elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
                price = float(raw[0] or 0.0)
                size = float(raw[1] or 0.0)
            if price <= 0:
                continue
            out.append({"price": price, "size": max(0.0, size)})
        return out

    @classmethod
    def _quote_from_book(cls, market_id: str, token_id: str, outcome: str, book: Dict[str, Any], ts: Optional[Any] = None) -> Dict[str, Any]:
        bids = cls._normalize_orderbook_side(book.get("bids") or book.get("buy") or [])
        asks = cls._normalize_orderbook_side(book.get("asks") or book.get("sell") or [])
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        midpoint = None
        if best_bid is not None and best_ask is not None:
            midpoint = (best_bid + best_ask) / 2.0
        price = midpoint if midpoint is not None else float(book.get("price") or book.get("mid") or 0.0)
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = max(0.0, best_ask - best_bid)
        depth1 = (bids[0]["size"] if bids else 0.0) + (asks[0]["size"] if asks else 0.0)
        depth5 = sum(x["size"] for x in bids[:5]) + sum(x["size"] for x in asks[:5])
        ts_value = parse_ts_any(ts or utc_now())
        return {
            "ts": ts_value,
            "market_id": market_id,
            "token_id": token_id,
            "outcome": str(outcome or "YES").upper(),
            "price": float(price or 0.0),
            "bid": best_bid,
            "ask": best_ask,
            "midpoint": midpoint,
            "spread": spread,
            "depth1": depth1,
            "depth5": depth5,
            "fetched_at": utc_now(),
            "payload": {"book": book, "source": "clob_rest"},
        }

    async def get_price(self, token_id: str) -> Dict[str, Any]:
        return await self._request("GET", "/price", params={"token_id": str(token_id)})

    async def get_prices(self, token_ids: List[str]) -> Any:
        return await self._request("GET", "/prices", params={"token_ids": ",".join(token_ids)})

    async def get_book(self, token_id: str) -> Dict[str, Any]:
        return await self._request("GET", "/book", params={"token_id": str(token_id)})

    async def get_books(self, token_ids: List[str]) -> Any:
        return await self._request("POST", "/books", json_payload={"token_ids": token_ids})

    async def get_midpoint(self, token_id: str) -> Any:
        return await self._request("GET", "/midpoint", params={"token_id": str(token_id)})

    async def get_spread(self, token_id: str) -> Any:
        return await self._request("GET", "/spread", params={"token_id": str(token_id)})

    async def get_prices_history(self, token_id: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None, interval: str = "1m") -> Any:
        params = {"market": str(token_id), "interval": interval}
        if start_ts is not None:
            params["startTs"] = int(start_ts)
        if end_ts is not None:
            params["endTs"] = int(end_ts)
        return await self._request("GET", "/prices-history", params=params)

    async def fetch_quotes_for_subscriptions(self, subscriptions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not subscriptions:
            return []
        token_map = {str(item["token_id"]): item for item in subscriptions if item.get("token_id")}
        token_ids = list(token_map.keys())
        quotes: List[Dict[str, Any]] = []
        try:
            books = await self.get_books(token_ids)
            items = books.get("data") if isinstance(books, dict) else books
            if isinstance(items, dict):
                items = list(items.values())
            for book in list(items or []):
                token_id = str(book.get("asset_id") or book.get("token_id") or book.get("tokenId") or "")
                sub = token_map.get(token_id)
                if not sub:
                    continue
                quotes.append(
                    self._quote_from_book(
                        market_id=str(sub.get("market_id") or ""),
                        token_id=token_id,
                        outcome=str(sub.get("outcome") or "YES"),
                        book=book,
                        ts=book.get("timestamp") or book.get("ts"),
                    )
                )
        except Exception:
            # fall back to per-token REST reads; slower but robust
            for token_id, sub in token_map.items():
                try:
                    book = await self.get_book(token_id)
                    quotes.append(
                        self._quote_from_book(
                            market_id=str(sub.get("market_id") or ""),
                            token_id=token_id,
                            outcome=str(sub.get("outcome") or "YES"),
                            book=book,
                            ts=book.get("timestamp") or book.get("ts"),
                        )
                    )
                except Exception:
                    continue
        return quotes

    async def stream_quotes(
        self,
        subscriptions: List[Dict[str, Any]],
        *,
        on_quote,
        stop_event: asyncio.Event,
    ) -> None:
        token_map = {str(item.get("token_id") or ""): item for item in subscriptions if item.get("token_id")}
        token_ids = [token_id for token_id in token_map.keys() if token_id]
        if not token_ids or not self.ws_url:
            return
        try:
            async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                self.ws_connected = True
                self._last_ws_error = None
                await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))
                while not stop_event.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    payload = json.loads(raw)
                    for quote in self._quotes_from_ws_payload(payload, token_map):
                        await on_quote(quote)
        except Exception as exc:
            self.ws_connected = False
            self._last_ws_error = str(exc)
            raise
        finally:
            self.ws_connected = False

    def _quotes_from_ws_payload(self, payload: Any, token_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            items = payload
        else:
            items = [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            token_id = str(item.get("asset_id") or item.get("token_id") or item.get("tokenId") or "")
            sub = token_map.get(token_id)
            if not sub:
                continue
            bids = self._normalize_orderbook_side(item.get("bids") or item.get("buys") or [])
            asks = self._normalize_orderbook_side(item.get("asks") or item.get("sells") or [])
            best_bid = bids[0]["price"] if bids else None
            best_ask = asks[0]["price"] if asks else None
            midpoint = item.get("midpoint")
            midpoint = float(midpoint) if midpoint is not None else ((best_bid + best_ask) / 2.0 if best_bid is not None and best_ask is not None else None)
            price = item.get("price")
            if price is None:
                price = midpoint if midpoint is not None else float(item.get("last_price") or 0.0)
            spread = item.get("spread")
            if spread is None and best_bid is not None and best_ask is not None:
                spread = max(0.0, best_ask - best_bid)
            messages.append(
                {
                    "ts": parse_ts_any(item.get("timestamp") or item.get("ts") or utc_now()),
                    "market_id": str(sub.get("market_id") or ""),
                    "token_id": token_id,
                    "outcome": str(sub.get("outcome") or "YES").upper(),
                    "price": float(price or 0.0),
                    "bid": best_bid,
                    "ask": best_ask,
                    "midpoint": None if midpoint is None else float(midpoint),
                    "spread": None if spread is None else float(spread),
                    "depth1": (bids[0]["size"] if bids else 0.0) + (asks[0]["size"] if asks else 0.0),
                    "depth5": sum(x["size"] for x in bids[:5]) + sum(x["size"] for x in asks[:5]),
                    "fetched_at": utc_now(),
                    "payload": {"ws": item, "source": "clob_ws"},
                }
            )
        return messages

    def get_runtime_status(self) -> Dict[str, Any]:
        return {
            "ws_connected": bool(self.ws_connected),
            "last_ws_error": self._last_ws_error,
            "breaker_open": not self.breaker.allow(),
            "breaker_error_count": int(self.breaker.error_count or 0),
        }
