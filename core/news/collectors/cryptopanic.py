"""CryptoPanic collector for crypto-native aggregated headlines."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import parser as dt_parser


class CryptoPanicCollector:
    """Pull crypto-focused headlines from CryptoPanic API."""

    endpoint = "https://cryptopanic.com/api/v1/posts/"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        self.timeout_sec = int(defaults.get("cryptopanic_timeout_sec") or 30)
        self.max_records = int(defaults.get("cryptopanic_max_records") or 80)
        self.default_filter = str(defaults.get("cryptopanic_filter") or "hot")
        self.default_currencies = str(defaults.get("cryptopanic_currencies") or "BTC,ETH,BNB,SOL,XRP,ADA,DOGE")
        self.token = str(os.getenv("CRYPTOPANIC_TOKEN") or os.getenv("CRYPTOPANIC_API_KEY") or "").strip()

    @staticmethod
    def _parse_ts(value: Any) -> str:
        if not value:
            return datetime.now(timezone.utc).isoformat()
        try:
            dt = dt_parser.parse(str(value).strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.isoformat()
        except Exception:
            return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
        source_obj = raw.get("source") if isinstance(raw.get("source"), dict) else {}
        source_name = str(
            source_obj.get("title")
            or source_obj.get("domain")
            or raw.get("domain")
            or "cryptopanic"
        ).strip() or "cryptopanic"
        title = str(raw.get("title") or "").strip()
        url = str(raw.get("url") or "").strip()
        published = raw.get("published_at") or raw.get("created_at")
        currencies = raw.get("currencies") if isinstance(raw.get("currencies"), list) else []
        currency_codes = [str(x.get("code") or "").upper() for x in currencies if isinstance(x, dict)]

        return {
            "source": source_name,
            "title": title,
            "url": url,
            "content": str(raw.get("slug") or raw.get("kind") or "").strip(),
            "published_at": CryptoPanicCollector._parse_ts(published),
            "lang": "en",
            "payload": {
                "provider": "cryptopanic",
                "currencies": [c for c in currency_codes if c],
                "raw": raw,
            },
        }

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        """Pull latest posts from CryptoPanic."""
        if not self.token:
            raise RuntimeError("CRYPTOPANIC_TOKEN is missing")

        max_records = max(10, min(int(max_records or self.max_records), 200))
        params: Dict[str, Any] = {
            "auth_token": self.token,
            "public": "true",
            "kind": "news",
            "filter": self.default_filter,
            "currencies": self.default_currencies,
        }
        if query:
            params["search"] = str(query)

        response = requests.get(self.endpoint, params=params, timeout=self.timeout_sec)
        response.raise_for_status()
        payload = response.json()
        raw_items = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(raw_items, list):
            return []

        out: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        for raw in raw_items[:max_records]:
            if not isinstance(raw, dict):
                continue
            item = self._normalize_item(raw)
            if not item["title"] or not item["url"]:
                continue
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            out.append(item)
        return out
