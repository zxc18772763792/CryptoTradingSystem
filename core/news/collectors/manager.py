"""Multi-source news collector manager with de-duplication and source stats."""
from __future__ import annotations

import hashlib
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil import parser as dt_parser
from loguru import logger

from core.news.collectors.cryptopanic import CryptoPanicCollector
from core.news.collectors.gdelt import GDELTCollector
from core.news.collectors.jin10 import Jin10Collector
from core.news.collectors.newsapi import NewsAPICollector
from core.news.collectors.rss import RSSNewsCollector


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _parse_ts_to_unix(value: Any) -> float:
    if not value:
        return 0.0
    try:
        dt = dt_parser.parse(str(value).strip())
        return dt.timestamp()
    except Exception:
        return 0.0


def _normalize_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
        query_items = []
        for k, v in parse_qsl(parts.query, keep_blank_values=False):
            key = str(k or "").lower()
            if key.startswith("utm_") or key in {"fbclid", "gclid", "spm"}:
                continue
            query_items.append((k, v))
        query = urlencode(query_items, doseq=True)
        normalized = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, query, ""))
        return normalized
    except Exception:
        return text


def _dedupe_key(item: Dict[str, Any]) -> str:
    url_norm = _normalize_url(item.get("url") or "")
    if url_norm:
        return f"url:{url_norm}"
    title = str(item.get("title") or "").strip().lower()
    ts = str(item.get("published_at") or "").strip()
    seed = f"{title}|{ts}"
    return f"title:{hashlib.sha1(seed.encode('utf-8')).hexdigest()}"


@dataclass
class _CollectorSpec:
    name: str
    collector: Any


class MultiSourceNewsCollector:
    """Aggregate from multiple collectors, de-duplicate and return source stats."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or {}
        self.defaults = self.cfg.get("defaults") or {}
        raw_sources = self.defaults.get("news_sources") or ["jin10", "rss", "cryptopanic", "gdelt", "newsapi"]
        self.sources: List[str] = [str(x).strip().lower() for x in raw_sources if str(x).strip()]
        if not self.sources:
            self.sources = ["jin10", "rss", "cryptopanic", "gdelt", "newsapi"]

    def _build_collectors(self) -> Tuple[List[_CollectorSpec], List[str]]:
        specs: List[_CollectorSpec] = []
        errors: List[str] = []

        for name in self.sources:
            if name == "gdelt":
                enabled = _env_bool("NEWS_ENABLE_GDELT", True)
                if not enabled:
                    continue
                specs.append(_CollectorSpec(name="gdelt", collector=GDELTCollector(self.cfg)))
                continue

            if name == "jin10":
                enabled = _env_bool("NEWS_ENABLE_JIN10", True)
                if not enabled:
                    continue
                specs.append(_CollectorSpec(name="jin10", collector=Jin10Collector(self.cfg)))
                continue

            if name == "rss":
                enabled = _env_bool("NEWS_ENABLE_RSS", True)
                if not enabled:
                    continue
                specs.append(_CollectorSpec(name="rss", collector=RSSNewsCollector(self.cfg)))
                continue

            if name == "newsapi":
                enabled = _env_bool("NEWS_ENABLE_NEWSAPI", True)
                if not enabled:
                    continue
                if not str(os.getenv("NEWSAPI_KEY") or "").strip():
                    errors.append("newsapi disabled: NEWSAPI_KEY missing")
                    continue
                specs.append(_CollectorSpec(name="newsapi", collector=NewsAPICollector(self.cfg)))
                continue

            if name == "cryptopanic":
                enabled = _env_bool("NEWS_ENABLE_CRYPTOPANIC", True)
                if not enabled:
                    continue
                token = str(os.getenv("CRYPTOPANIC_TOKEN") or os.getenv("CRYPTOPANIC_API_KEY") or "").strip()
                if not token:
                    errors.append("cryptopanic disabled: CRYPTOPANIC_TOKEN missing")
                    continue
                specs.append(_CollectorSpec(name="cryptopanic", collector=CryptoPanicCollector(self.cfg)))
                continue

            errors.append(f"unsupported collector source: {name}")

        if not specs:
            specs.append(_CollectorSpec(name="jin10", collector=Jin10Collector(self.cfg)))
            specs.append(_CollectorSpec(name="rss", collector=RSSNewsCollector(self.cfg)))
            specs.append(_CollectorSpec(name="gdelt", collector=GDELTCollector(self.cfg)))
            errors.append("all configured sources unavailable; fallback to jin10/rss/gdelt")

        return specs, errors

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> Dict[str, Any]:
        """Pull from enabled sources and merge results."""
        max_records = max(10, min(_safe_int(max_records, 120), 500))
        since_minutes = max(15, min(_safe_int(since_minutes, 240), 24 * 60))

        specs, setup_errors = self._build_collectors()
        source_stats: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = list(setup_errors)
        all_items: List[Dict[str, Any]] = []

        source_count = max(1, len(specs))
        per_source = max(10, min(250, int(math.ceil(max_records / source_count * 1.6))))

        for spec in specs:
            source_stats[spec.name] = {
                "enabled": True,
                "pulled_count": 0,
                "kept_count": 0,
                "errors": [],
            }
            try:
                items = spec.collector.pull_latest(
                    query=query,
                    max_records=per_source,
                    since_minutes=since_minutes,
                )
                source_stats[spec.name]["pulled_count"] = len(items)
            except Exception as exc:
                err_msg = f"{spec.name} pull failed: {exc}"
                logger.warning(err_msg)
                errors.append(err_msg)
                source_stats[spec.name]["errors"].append(str(exc))
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                payload = item.get("payload")
                if not isinstance(payload, dict):
                    payload = {}
                payload["provider"] = spec.name
                item["payload"] = payload
                item["provider"] = spec.name
                if not str(item.get("source") or "").strip():
                    item["source"] = spec.name
                all_items.append(item)

        all_items.sort(key=lambda x: _parse_ts_to_unix(x.get("published_at")), reverse=True)
        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()

        for item in all_items:
            key = _dedupe_key(item)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= max_records:
                break

        for item in deduped:
            provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "unknown")
            if provider not in source_stats:
                source_stats[provider] = {"enabled": True, "pulled_count": 0, "kept_count": 0, "errors": []}
            source_stats[provider]["kept_count"] = int(source_stats[provider].get("kept_count") or 0) + 1

        total_pulled = sum(int(v.get("pulled_count") or 0) for v in source_stats.values())
        return {
            "items": deduped,
            "source_stats": source_stats,
            "pulled_total": total_pulled,
            "kept_total": len(deduped),
            "errors": errors,
        }
