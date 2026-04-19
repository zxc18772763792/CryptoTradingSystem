from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from core.data.coinglass_client import CoinglassClient
from core.news.collectors.common import BaseNewsCollector, clamp_int, parse_datetime_utc
from core.news.eventizer.rules import SymbolMapper


_DEFAULT_LANGUAGE = "en"
_DEFAULT_MACRO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def _coinglass_code(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return ""
    return str(payload.get("code") or "").strip()


def _coinglass_message(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return ""
    for key in ("msg", "message", "error"):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    return ""


def _safe_slug(value: Any, limit: int = 72) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    parts = [part for part in text.split("-") if part]
    slug = "-".join(parts)
    if not slug:
        return "item"
    return slug[:limit].rstrip("-") or "item"


def _stable_coinglass_url(kind: str, title: str, published_at: datetime, source_name: str) -> str:
    seed = f"{kind}|{source_name}|{title}|{published_at.astimezone(timezone.utc).isoformat()}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    slug = _safe_slug(title)
    return f"https://www.coinglass.com/news/{kind}/{slug}-{digest}"


def _compact_text(*parts: Any, limit: int = 4000) -> str:
    text = " | ".join(str(part or "").strip() for part in parts if str(part or "").strip())
    text = " ".join(text.split())
    return text[:limit]


def _parse_coinglass_timestamp(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1_000_000_000_000:
            numeric = numeric / 1000.0
        return parse_datetime_utc(numeric)
    return parse_datetime_utc(value or datetime.now(timezone.utc))


class _CoinGlassBaseCollector(BaseNewsCollector):
    provider_name = "coinglass_news"
    endpoint_path = ""
    supports_paging = True
    page_size_default = 40
    macro_fallback_symbols: List[str] = []

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        prefix = self.provider_name.lower()
        self.language = str(
            defaults.get(f"{prefix}_language")
            or defaults.get("coinglass_news_language")
            or _DEFAULT_LANGUAGE
        ).strip() or _DEFAULT_LANGUAGE
        self.page_size = clamp_int(
            defaults.get(f"{prefix}_page_size"),
            self.page_size_default,
            1,
            100,
        )
        mapper = (cfg or {}).get("_symbol_mapper")
        self._mapper = mapper if isinstance(mapper, SymbolMapper) else SymbolMapper({"symbols": {}})

    def _run_async(self, coro: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        raise RuntimeError(f"{self.provider_name} collector must run outside the active event loop")

    async def _request_rows(
        self,
        client: CoinglassClient,
        *,
        page: int,
        per_page: int,
    ) -> List[Dict[str, Any]]:
        params = self._build_params(page=page, per_page=per_page)
        response = await client.request_json(self.endpoint_path, params=params, manual=False)
        payload = response.get("payload")
        code = _coinglass_code(payload)
        if code and code != "0":
            raise RuntimeError(f"{self.provider_name} code={code}: {_coinglass_message(payload) or 'request_failed'}")
        data = payload.get("data") if isinstance(payload, Mapping) else payload
        if isinstance(data, list):
            return [dict(item or {}) for item in data if isinstance(item, Mapping)]
        if isinstance(data, Mapping):
            return [dict(data)]
        return []

    def _build_params(self, *, page: int, per_page: int) -> Dict[str, Any]:
        params: Dict[str, Any] = {"language": self.language}
        if self.supports_paging:
            params["page"] = max(1, int(page))
            params["per_page"] = max(1, int(per_page))
        return params

    def _symbols_from_text(self, title: str, content: str) -> List[str]:
        text = f"{title}\n{content}"
        symbols = self._mapper.extract_symbols_from_text(text, limit=8)
        if symbols:
            return symbols
        return list(self.macro_fallback_symbols or [])

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def _pull_latest_async(
        self,
        *,
        max_records: int,
        since_minutes: int,
    ) -> List[Dict[str, Any]]:
        limit = max(1, int(max_records))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        out: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        max_pages = 1 if not self.supports_paging else max(1, min(8, (limit + self.page_size - 1) // self.page_size + 1))

        async with CoinglassClient(timeout_sec=self.timeout_sec) as client:
            for page in range(1, max_pages + 1):
                rows = await self._request_rows(client, page=page, per_page=min(self.page_size, limit))
                if not rows:
                    break
                page_items: List[Dict[str, Any]] = []
                for raw in rows:
                    item = self._normalize_row(raw)
                    if not item:
                        continue
                    page_items.append(item)
                page_items.sort(
                    key=lambda item: parse_datetime_utc(item.get("published_at")).timestamp(),
                    reverse=True,
                )
                for item in page_items:
                    published_at = parse_datetime_utc(item.get("published_at"))
                    if published_at < since_ts:
                        continue
                    url = str(item.get("url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    out.append(item)
                    if len(out) >= limit:
                        return out
                if self.supports_paging and len(rows) > min(self.page_size, limit):
                    break
                if not self.supports_paging or len(rows) < min(self.page_size, limit):
                    break
        return out

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 200))
        since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))
        return list(self._run_async(self._pull_latest_async(max_records=limit, since_minutes=since_minutes)) or [])

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


class CoinGlassNewsflashCollector(_CoinGlassBaseCollector):
    provider_name = "coinglass_newsflash"
    endpoint_path = "/v4/api/newsflash/list"
    page_size_default = 50

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        title = str(row.get("newsflash_title") or "").strip()
        content = str(row.get("newsflash_content") or "").strip()
        source_name = str(row.get("source_name") or "CoinGlass").strip() or "CoinGlass"
        published_at = _parse_coinglass_timestamp(row.get("newsflash_release_time"))
        headline = title or content
        if not headline:
            return None
        symbols = self._symbols_from_text(headline, content)
        return {
            "source": self.provider_name,
            "title": headline[:600],
            "url": _stable_coinglass_url("newsflash", headline, published_at, source_name),
            "content": content[:4000],
            "published_at": published_at.isoformat(),
            "lang": self.language,
            "symbols": symbols,
            "payload": {
                "provider": self.provider_name,
                "source_name": source_name,
                "source_website_logo": row.get("source_website_logo"),
                "coinglass_endpoint": self.endpoint_path,
                "raw": dict(row or {}),
                "symbols": symbols,
            },
        }


class CoinGlassArticlesCollector(_CoinGlassBaseCollector):
    provider_name = "coinglass_articles"
    endpoint_path = "/v4/api/article/list"
    page_size_default = 30

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        title = str(row.get("article_title") or "").strip()
        content = str(row.get("article_content") or row.get("article_description") or "").strip()
        source_name = str(row.get("source_name") or "CoinGlass").strip() or "CoinGlass"
        published_at = _parse_coinglass_timestamp(row.get("article_release_time"))
        if not title:
            return None
        symbols = self._symbols_from_text(title, content)
        return {
            "source": self.provider_name,
            "title": title[:600],
            "url": _stable_coinglass_url("article", title, published_at, source_name),
            "content": content[:4000],
            "published_at": published_at.isoformat(),
            "lang": self.language,
            "symbols": symbols,
            "payload": {
                "provider": self.provider_name,
                "source_name": source_name,
                "source_website_logo": row.get("source_website_logo"),
                "article_picture": row.get("article_picture"),
                "article_description": row.get("article_description"),
                "coinglass_endpoint": self.endpoint_path,
                "raw": dict(row or {}),
                "symbols": symbols,
            },
        }


class CoinGlassEconomicDataCollector(_CoinGlassBaseCollector):
    provider_name = "coinglass_economic_data"
    endpoint_path = "/v4/api/calendar/economic-data"
    supports_paging = True
    page_size_default = 40
    macro_fallback_symbols = _DEFAULT_MACRO_SYMBOLS

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        name = str(row.get("calendar_name") or "").strip()
        country = str(row.get("country_name") or row.get("country_code") or "Macro").strip() or "Macro"
        if not name:
            return None
        published_at = _parse_coinglass_timestamp(row.get("publish_timestamp"))
        title = f"{country} economic data: {name}"
        content = _compact_text(
            f"Effect: {row.get('data_effect')}",
            f"Published: {row.get('published_value')}",
            f"Forecast: {row.get('forecast_value')}",
            f"Previous: {row.get('previous_value')}",
            f"Revised previous: {row.get('revised_previous_value')}",
            f"Importance level: {row.get('importance_level')}",
        )
        symbols = self._symbols_from_text(title, content)
        return {
            "source": self.provider_name,
            "title": title[:600],
            "url": _stable_coinglass_url("economic-data", title, published_at, country),
            "content": content[:4000],
            "published_at": published_at.isoformat(),
            "lang": self.language,
            "symbols": symbols,
            "payload": {
                "provider": self.provider_name,
                "country_code": row.get("country_code"),
                "country_name": country,
                "importance_level": row.get("importance_level"),
                "coinglass_endpoint": self.endpoint_path,
                "raw": dict(row or {}),
                "symbols": symbols,
            },
        }


class CoinGlassFinancialEventsCollector(_CoinGlassBaseCollector):
    provider_name = "coinglass_financial_events"
    endpoint_path = "/v4/api/calendar/financial-events"
    supports_paging = False
    macro_fallback_symbols = _DEFAULT_MACRO_SYMBOLS

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        name = str(row.get("event_name") or row.get("calendar_name") or "").strip()
        country = str(row.get("country_name") or row.get("country_code") or "Macro").strip() or "Macro"
        if not name:
            return None
        published_at = _parse_coinglass_timestamp(row.get("publish_timestamp") or row.get("event_timestamp"))
        title = f"{country} financial event: {name}"
        content = _compact_text(
            f"Event: {name}",
            f"Country: {country}",
            f"Importance level: {row.get('importance_level')}",
            f"Effect: {row.get('data_effect') or row.get('event_effect')}",
        )
        symbols = self._symbols_from_text(title, content)
        return {
            "source": self.provider_name,
            "title": title[:600],
            "url": _stable_coinglass_url("financial-events", title, published_at, country),
            "content": content[:4000],
            "published_at": published_at.isoformat(),
            "lang": self.language,
            "symbols": symbols,
            "payload": {
                "provider": self.provider_name,
                "country_code": row.get("country_code"),
                "country_name": country,
                "importance_level": row.get("importance_level"),
                "coinglass_endpoint": self.endpoint_path,
                "raw": dict(row or {}),
                "symbols": symbols,
            },
        }


class CoinGlassCentralBankCollector(_CoinGlassBaseCollector):
    provider_name = "coinglass_central_bank"
    endpoint_path = "/v4/api/calendar/central-bank-activities"
    supports_paging = False
    macro_fallback_symbols = _DEFAULT_MACRO_SYMBOLS

    def _normalize_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        name = str(row.get("activity_name") or row.get("calendar_name") or row.get("event_name") or "").strip()
        country = str(row.get("country_name") or row.get("country_code") or "Macro").strip() or "Macro"
        if not name:
            return None
        published_at = _parse_coinglass_timestamp(row.get("publish_timestamp") or row.get("activity_timestamp"))
        title = f"{country} central bank activity: {name}"
        content = _compact_text(
            f"Activity: {name}",
            f"Country: {country}",
            f"Importance level: {row.get('importance_level')}",
            f"Effect: {row.get('data_effect') or row.get('activity_effect')}",
        )
        symbols = self._symbols_from_text(title, content)
        return {
            "source": self.provider_name,
            "title": title[:600],
            "url": _stable_coinglass_url("central-bank", title, published_at, country),
            "content": content[:4000],
            "published_at": published_at.isoformat(),
            "lang": self.language,
            "symbols": symbols,
            "payload": {
                "provider": self.provider_name,
                "country_code": row.get("country_code"),
                "country_name": country,
                "importance_level": row.get("importance_level"),
                "coinglass_endpoint": self.endpoint_path,
                "raw": dict(row or {}),
                "symbols": symbols,
            },
        }
