"""OpenNews/6551 collector for aggregated crypto news and AI-rated signals."""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from core.news.collectors.common import BaseNewsCollector, optional_env, parse_datetime_utc


_DEFAULT_BASE_URL = "https://ai.6551.io"
_NEWS_SEARCH_PATH = "/open/news_search"


def _compact_text(*parts: Any, limit: int = 4000) -> str:
    text = " | ".join(str(part or "").strip() for part in parts if str(part or "").strip())
    return " ".join(text.split())[:limit]


def _safe_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    return max(low, min(parsed, high))


def _parse_bool(value: Any, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "on", "y"}


def _parse_csv(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value or "").split(",")
    return [str(item or "").strip() for item in raw_items if str(item or "").strip()]


def _normalize_coin_symbol(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("symbol") or value.get("coin") or value.get("base") or ""
    text = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    if not text or text in {"USD", "USDT", "USDC"}:
        return ""
    if text.endswith("USDT"):
        return text
    return f"{text}USDT"


def _normalize_base_coin(value: Any) -> str:
    text = _normalize_coin_symbol(value)
    if text.endswith("USDT"):
        return text[:-4]
    return text


def _parse_engine_types(value: Any) -> Optional[Dict[str, List[str]]]:
    if isinstance(value, Mapping):
        out: Dict[str, List[str]] = {}
        for engine, categories in value.items():
            engine_name = str(engine or "").strip()
            if not engine_name:
                continue
            out[engine_name] = _parse_csv(categories)
        return out or None

    text = str(value or "").strip()
    if not text:
        return None

    out = {}
    for part in text.split(";"):
        if ":" not in part:
            continue
        engine, categories = part.split(":", 1)
        engine_name = engine.strip()
        if not engine_name:
            continue
        out[engine_name] = _parse_csv(categories)
    return out or None


def _extract_rows(payload: Any) -> List[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if isinstance(data, Mapping):
        for key in ("items", "list", "rows", "records", "results"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, Mapping)]
    for key in ("items", "list", "rows", "records", "results"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, Mapping)]
    return []


class OpenNewsCollector(BaseNewsCollector):
    """Pull AI-rated crypto news from the 6551 OpenNews REST API."""

    provider_name = "opennews"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        base_url = (
            os.getenv("OPENNEWS_API_BASE")
            or defaults.get("opennews_api_base")
            or defaults.get("opennews_base_url")
            or _DEFAULT_BASE_URL
        )
        endpoint = str(defaults.get("opennews_endpoint") or "").strip()
        if endpoint:
            self.endpoint = endpoint
        else:
            self.endpoint = f"{str(base_url).rstrip('/')}{_NEWS_SEARCH_PATH}"
        self.token = optional_env("OPENNEWS_TOKEN")
        self.default_query = str(defaults.get("opennews_query") or "").strip()
        self.default_coins = [
            _normalize_base_coin(item)
            for item in _parse_csv(defaults.get("opennews_coins") or "")
        ]
        self.default_coins = [coin for coin in self.default_coins if coin]
        self.engine_types = _parse_engine_types(defaults.get("opennews_engine_types"))
        self.min_score = _safe_int(defaults.get("opennews_min_score"), 0, 0, 100)
        self.has_coin = _parse_bool(defaults.get("opennews_has_coin"), False)

    @staticmethod
    def _parse_ts(raw: Mapping[str, Any]) -> str:
        for key in ("ts", "published_at", "publishedAt", "createdAt", "created_at", "updatedAt"):
            value = raw.get(key)
            if value:
                try:
                    if isinstance(value, (int, float)) or str(value).strip().isdigit():
                        numeric = float(value)
                        if numeric > 1_000_000_000_000:
                            numeric = numeric / 1000.0
                        return parse_datetime_utc(numeric).isoformat()
                    return parse_datetime_utc(value).isoformat()
                except Exception:
                    continue
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _stable_fallback_url(raw: Mapping[str, Any], title: str, published_at: str) -> str:
        item_id = str(raw.get("id") or raw.get("_id") or "").strip()
        if item_id:
            safe_id = re.sub(r"[^A-Za-z0-9_.-]", "-", item_id)
            return f"{_DEFAULT_BASE_URL}/open/news/{safe_id}"
        seed = re.sub(r"[^a-z0-9]+", "-", f"{title}-{published_at}".lower()).strip("-")
        return f"{_DEFAULT_BASE_URL}/open/news/{seed[:120] or 'item'}"

    @staticmethod
    def _normalize_item(raw: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        title = str(raw.get("text") or raw.get("title") or raw.get("headline") or "").strip()
        if not title:
            return None

        published_at = OpenNewsCollector._parse_ts(raw)
        url = str(raw.get("link") or raw.get("url") or raw.get("sourceUrl") or "").strip()
        if not url:
            url = OpenNewsCollector._stable_fallback_url(raw, title, published_at)

        ai_rating = raw.get("aiRating") if isinstance(raw.get("aiRating"), Mapping) else {}
        score = _safe_int(ai_rating.get("score") if isinstance(ai_rating, Mapping) else None, 0, 0, 100)
        signal = str(ai_rating.get("signal") if isinstance(ai_rating, Mapping) else "").strip().lower()
        en_summary = str(ai_rating.get("enSummary") if isinstance(ai_rating, Mapping) else "").strip()
        zh_summary = str(ai_rating.get("summary") if isinstance(ai_rating, Mapping) else "").strip()
        content = _compact_text(en_summary, zh_summary, raw.get("content"), raw.get("summary"), title)

        raw_coins = raw.get("coins") if isinstance(raw.get("coins"), list) else []
        symbols = [_normalize_coin_symbol(item) for item in raw_coins]
        symbols = [symbol for symbol in symbols if symbol]

        news_type = str(raw.get("newsType") or raw.get("source") or "").strip()
        engine_type = str(raw.get("engineType") or "").strip()

        return {
            "source": "opennews",
            "title": title[:600],
            "url": url,
            "content": content[:4000],
            "published_at": published_at,
            "lang": "en" if en_summary else "zh" if zh_summary else "en",
            "symbols": symbols,
            "payload": {
                "provider": OpenNewsCollector.provider_name,
                "opennews_id": raw.get("id") or raw.get("_id"),
                "opennews_news_type": news_type,
                "opennews_engine_type": engine_type,
                "opennews_ai_score": score,
                "opennews_ai_signal": signal,
                "opennews_ai_grade": ai_rating.get("grade") if isinstance(ai_rating, Mapping) else None,
                "opennews_ai_status": ai_rating.get("status") if isinstance(ai_rating, Mapping) else None,
                "opennews_summary_en": en_summary,
                "opennews_summary_zh": zh_summary,
                "currencies": symbols,
                "symbols": symbols,
                "raw": dict(raw),
            },
        }

    def _build_body(self, *, query: Optional[str], limit: int, page: int = 1) -> Dict[str, Any]:
        body: Dict[str, Any] = {"limit": limit, "page": page}
        effective_query = str(query or self.default_query or "").strip()
        if effective_query:
            body["q"] = effective_query
        if self.default_coins:
            body["coins"] = self.default_coins
        if self.engine_types:
            body["engineTypes"] = self.engine_types
        if self.has_coin:
            body["hasCoin"] = True
        if self.min_score > 0:
            body["score"] = self.min_score
        return body

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        if not self.token:
            raise RuntimeError("OPENNEWS_TOKEN is missing")

        limit = max(5, min(int(max_records or self.max_records), 100))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        body = self._build_body(query=query, limit=limit, page=1)
        response = self._request(
            self.endpoint,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json_body=body,
        )
        payload = response.json()
        rows = _extract_rows(payload)
        if not rows:
            return []

        out: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        for raw in rows:
            item = self._normalize_item(raw)
            if not item:
                continue
            published = parse_datetime_utc(item.get("published_at"))
            if published < since_ts:
                continue
            url = str(item.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            out.append(item)
            if len(out) >= limit:
                break
        return out

    def pull_incremental(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        items = self.pull_latest(query=query, max_records=max_records, since_minutes=since_minutes)
        filtered = self.filter_incremental(items, cursor)
        return filtered, self.build_ts_cursor(items, fallback=cursor)
