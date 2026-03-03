from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncio
import requests

from prediction_markets.polymarket.utils import AsyncTokenBucket, CircuitBreaker, async_retry


class GammaClient:
    """Read-only client for Polymarket Gamma discovery endpoints."""

    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        request_timeout_sec: int = 15,
        burst: int = 10,
        refill_per_sec: float = 3.0,
        breaker_errors: int = 5,
        breaker_cooldown_sec: int = 300,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.request_timeout_sec = max(3, int(request_timeout_sec or 15))
        self.rate_limiter = AsyncTokenBucket(burst=burst, refill_per_sec=refill_per_sec)
        self.breaker = CircuitBreaker(error_threshold=breaker_errors, cooldown_sec=breaker_cooldown_sec)
        self.headers = {
            "User-Agent": "CryptoTradingSystem/PolymarketGammaClient",
            "Accept": "application/json",
        }

    async def _request(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.breaker.allow():
            raise RuntimeError("gamma circuit breaker is open")
        await self.rate_limiter.acquire()

        async def _do() -> Any:
            def _sync_request() -> Any:
                resp = requests.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers=self.headers,
                    timeout=self.request_timeout_sec,
                )
                resp.raise_for_status()
                return resp.json()

            return await asyncio.to_thread(_sync_request)

        try:
            data = await async_retry(_do, retries=3, base_delay=1.0, max_delay=5.0)
            self.breaker.on_success()
            return data
        except Exception:
            self.breaker.on_error()
            raise

    async def list_events(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
        tag_id: Optional[int] = None,
        search: Optional[str] = None,
        order: str = "volume_24hr",
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "active": str(bool(active)).lower(),
            "closed": str(bool(closed)).lower(),
            "limit": max(1, min(int(limit or 100), 500)),
            "offset": max(0, int(offset or 0)),
        }
        if order:
            params["order"] = order
        if tag_id is not None:
            params["tag_id"] = int(tag_id)
        if search:
            params["search"] = str(search)
        try:
            data = await self._request("/events", params=params)
        except requests.HTTPError as exc:
            response = getattr(exc, "response", None)
            if response is not None and int(response.status_code) == 422 and "order" in params:
                params = dict(params)
                params.pop("order", None)
                data = await self._request("/events", params=params)
            else:
                raise
        return list(data or [])

    async def list_markets(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
        slug: Optional[str] = None,
        tag_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "active": str(bool(active)).lower(),
            "closed": str(bool(closed)).lower(),
            "limit": max(1, min(int(limit or 100), 500)),
            "offset": max(0, int(offset or 0)),
        }
        if slug:
            params["slug"] = str(slug)
        if tag_id is not None:
            params["tag_id"] = int(tag_id)
        data = await self._request("/markets", params=params)
        return list(data or [])

    async def list_tags(self, limit: int = 200) -> List[Dict[str, Any]]:
        data = await self._request("/tags", params={"limit": max(1, min(int(limit or 200), 500))})
        return list(data or [])

    async def search_public(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        data = await self._request("/public-search", params={"q": str(query or "").strip(), "limit": max(1, min(int(limit or 50), 100))})
        if isinstance(data, dict):
            if isinstance(data.get("events"), list):
                return list(data.get("events") or [])
            if isinstance(data.get("data"), list):
                return list(data.get("data") or [])
        return list(data or [])
