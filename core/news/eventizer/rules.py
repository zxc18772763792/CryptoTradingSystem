"""Rule-based fallback event extractor."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml
from loguru import logger

from core.news.storage.models import EventSchema, parse_any_datetime


DEFAULT_RULES_PATH = Path("config/news_rules.yaml")
DEFAULT_SYMBOLS_PATH = Path("config/symbols.yaml")


class SymbolMapper:
    """Resolve aliases/base symbols to canonical quote symbols."""

    def __init__(self, symbol_cfg: Dict[str, Any]):
        self._canonical_map: Dict[str, str] = {}
        self._alias_to_canonical: Dict[str, str] = {}
        self._aliases_sorted: List[str] = []
        self._build_maps(symbol_cfg)

    def _build_maps(self, symbol_cfg: Dict[str, Any]) -> None:
        symbols = symbol_cfg.get("symbols") if isinstance(symbol_cfg, dict) else {}
        if not isinstance(symbols, dict):
            symbols = {}

        for base_or_symbol, meta in symbols.items():
            item = meta if isinstance(meta, dict) else {}
            canonical = str(item.get("canonical") or "").strip().upper()
            base = str(item.get("base") or base_or_symbol or "").strip().upper()
            if not canonical:
                canonical = f"{base}USDT" if base and not base.endswith("USDT") else base
            if not base:
                base = canonical.replace("USDT", "")

            self._canonical_map[base] = canonical
            self._canonical_map[canonical] = canonical

            aliases = item.get("aliases") or []
            for raw in [base_or_symbol, base, canonical, *aliases]:
                alias = self._normalize_alias(raw)
                if alias:
                    self._alias_to_canonical[alias] = canonical

        self._aliases_sorted = sorted(self._alias_to_canonical.keys(), key=len, reverse=True)

    @staticmethod
    def _normalize_alias(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = text.replace("/", "").replace("-", "").replace("_", "").replace(" ", "")
        return text

    def normalize_symbol(self, symbol: Any) -> str:
        text = self._normalize_alias(symbol)
        if not text:
            return ""

        if text in self._alias_to_canonical:
            return self._alias_to_canonical[text]

        upper = text.upper()
        if upper in self._canonical_map:
            return self._canonical_map[upper]
        if upper.endswith("USDT"):
            base = upper[:-4]
            return self._canonical_map.get(base, upper)

        return self._canonical_map.get(upper, "")

    def extract_symbols_from_text(self, text: str, limit: int = 3) -> List[str]:
        raw = self._normalize_alias(text)
        if not raw:
            return []

        found: List[str] = []
        seen: set[str] = set()
        for alias in self._aliases_sorted:
            if alias and alias in raw:
                symbol = self._alias_to_canonical[alias]
                if symbol not in seen:
                    seen.add(symbol)
                    found.append(symbol)
                    if len(found) >= max(1, int(limit)):
                        break
        return found


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def load_news_rule_config(
    rules_path: Path = DEFAULT_RULES_PATH,
    symbols_path: Path = DEFAULT_SYMBOLS_PATH,
) -> Dict[str, Any]:
    """Load rules and symbol mapping config from YAML."""
    rules_cfg = _load_yaml(rules_path)
    symbols_cfg = _load_yaml(symbols_path)
    mapper = SymbolMapper(symbols_cfg)

    cfg = {
        "rules": rules_cfg.get("rules") or [],
        "defaults": rules_cfg.get("defaults") or {},
        "thresholds": rules_cfg.get("thresholds") or {},
        "llm": rules_cfg.get("llm") or {},
        "symbols": symbols_cfg.get("symbols") or {},
        "_symbol_mapper": mapper,
    }
    return cfg


def _stable_event_id(url: str, symbol: str, reason: str, ts: datetime) -> str:
    seed = f"{url}|{symbol}|{reason}|{ts.isoformat()}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _rule_match(rule: Dict[str, Any], text_lower: str) -> bool:
    keywords_any = [str(k).lower() for k in (rule.get("keywords_any") or []) if str(k).strip()]
    keywords_all = [str(k).lower() for k in (rule.get("keywords_all") or []) if str(k).strip()]
    exclude_keywords = [str(k).lower() for k in (rule.get("exclude_keywords") or []) if str(k).strip()]

    if keywords_any and not any(k in text_lower for k in keywords_any):
        return False
    if keywords_all and not all(k in text_lower for k in keywords_all):
        return False
    if exclude_keywords and any(k in text_lower for k in exclude_keywords):
        return False
    return bool(keywords_any or keywords_all)


def _extract_ts(item: Dict[str, Any]) -> datetime:
    for key in ("published_at", "published", "seendate", "ts"):
        value = item.get(key)
        if value:
            try:
                return parse_any_datetime(value)
            except Exception:
                continue
    return datetime.now(timezone.utc)


def _as_reason(rule: Dict[str, Any]) -> str:
    return str(rule.get("name") or rule.get("id") or "rules_fallback")


def extract_events_rules(news_items: List[Dict[str, Any]], cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Extract structured events from rules config as fallback."""
    if cfg is None:
        cfg = load_news_rule_config()

    mapper: SymbolMapper = cfg.get("_symbol_mapper") or SymbolMapper({"symbols": {}})
    defaults = cfg.get("defaults") or {}
    rules = cfg.get("rules") or []

    default_symbol = mapper.normalize_symbol(defaults.get("default_symbol") or "BTCUSDT") or "BTCUSDT"
    default_half_life = int(defaults.get("default_half_life_min") or 180)

    out: List[Dict[str, Any]] = []
    seen_event_ids: set[str] = set()

    for item in news_items:
        title = str(item.get("title") or "")
        content = str(item.get("content") or item.get("summary") or "")
        source = str(item.get("source") or "gdelt")
        url = str(item.get("url") or "")
        ts = _extract_ts(item)

        text_lower = f"{title}\n{content}".lower()
        inferred_symbols = mapper.extract_symbols_from_text(text_lower)

        for rule in rules:
            if not _rule_match(rule, text_lower):
                continue

            matched_reason = _as_reason(rule)
            rule_symbols = [mapper.normalize_symbol(s) for s in (rule.get("symbols") or [])]
            symbols = [s for s in rule_symbols if s] or inferred_symbols or [default_symbol]
            symbols = symbols[: max(1, int(rule.get("max_symbols") or 3))]

            for symbol in symbols:
                event = {
                    "event_id": _stable_event_id(url=url or title, symbol=symbol, reason=matched_reason, ts=ts),
                    "ts": ts.isoformat(),
                    "symbol": symbol,
                    "event_type": str(rule.get("event_type") or "other").lower(),
                    "sentiment": int(rule.get("sentiment", 0)),
                    "impact_score": float(rule.get("impact_score", defaults.get("default_impact_score", 0.35))),
                    "half_life_min": int(rule.get("half_life_min", default_half_life)),
                    "evidence": {
                        "title": title,
                        "url": url,
                        "source": source,
                        "matched_reason": matched_reason,
                    },
                    "model_source": "rules",
                }

                try:
                    validated = EventSchema.model_validate(event).model_dump(mode="json")
                except Exception as exc:
                    logger.debug(f"rules event validation failed: {exc}")
                    continue

                if validated["event_id"] in seen_event_ids:
                    continue
                seen_event_ids.add(validated["event_id"])
                out.append(validated)

    return out
