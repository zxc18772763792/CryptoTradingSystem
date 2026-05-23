"""NewsAPI collector for development/demo news ingestion."""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import parser as dt_parser


class NewsAPICollector:
    """Pull crypto-related news from NewsAPI."""

    endpoint = "https://newsapi.org/v2/everything"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        self.timeout_sec = int(defaults.get("newsapi_timeout_sec") or 30)
        self.max_records = int(defaults.get("newsapi_max_records") or 80)
        self.language = str(defaults.get("newsapi_language") or "en")
        self.default_query = str(
            defaults.get("newsapi_query")
            or defaults.get("gdelt_query")
            or "(bitcoin OR ethereum OR crypto OR ETF OR SEC OR hack OR liquidation)"
        )
        self.api_key = str(os.getenv("NEWSAPI_KEY") or "").strip()
        self.retry_attempts = max(1, int(defaults.get("newsapi_retry_attempts") or 3))
        self.retry_base_sleep_sec = max(0.0, float(defaults.get("newsapi_retry_base_sleep_sec") or 0.5))

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> Optional[float]:
        value = str(getattr(response, "headers", {}).get("Retry-After") or "").strip()
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except Exception:
            return None

    def _get_with_retries(self, *, params: Dict[str, Any], headers: Dict[str, str]) -> requests.Response:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = requests.get(self.endpoint, params=params, headers=headers, timeout=self.timeout_sec)
                if response.status_code not in {429, 500, 502, 503, 504}:
                    return response
                last_exc = requests.HTTPError(f"transient status {response.status_code}", response=response)
                if attempt >= self.retry_attempts:
                    return response
                retry_after = self._retry_after_seconds(response)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= self.retry_attempts:
                    raise
                retry_after = None
            sleep_sec = retry_after if retry_after is not None else self.retry_base_sleep_sec * attempt
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("NewsAPI request failed without response")

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
        source_name = str(source_obj.get("name") or "newsapi").strip() or "newsapi"
        title = str(raw.get("title") or "").strip()
        url = str(raw.get("url") or "").strip()
        description = str(raw.get("description") or "").strip()
        content = str(raw.get("content") or "").strip()

        return {
            "source": source_name,
            "title": title,
            "url": url,
            "content": (description or content)[:2000],
            "published_at": NewsAPICollector._parse_ts(raw.get("publishedAt")),
            "lang": "en",
            "payload": {
                "provider": "newsapi",
                "raw": raw,
            },
        }

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        """Pull latest news from NewsAPI."""
        if not self.api_key:
            raise RuntimeError("NEWSAPI_KEY is missing")

        max_records = max(10, min(int(max_records or self.max_records), 100))
        since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))
        start = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

        params = {
            "q": str(query or self.default_query),
            "language": self.language,
            "sortBy": "publishedAt",
            "pageSize": str(max_records),
            "from": start.isoformat(),
            "searchIn": "title,description,content",
        }
        headers = {"X-Api-Key": self.api_key}

        response = self._get_with_retries(params=params, headers=headers)
        response.raise_for_status()
        payload = response.json()

        if str(payload.get("status") or "").lower() != "ok":
            msg = payload.get("message") or "unknown newsapi response status"
            raise RuntimeError(f"NewsAPI response status not ok: {msg}")

        raw_items = payload.get("articles")
        if not isinstance(raw_items, list):
            return []

        out: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        for raw in raw_items:
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
