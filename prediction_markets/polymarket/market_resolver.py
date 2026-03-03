from __future__ import annotations

import math
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from prediction_markets.polymarket.utils import parse_ts_any, utc_now


@dataclass
class ResolvedCategory:
    name: str
    keywords: List[str]
    tags: List[int]
    max_markets: int
    min_liquidity: float
    max_spread: float
    target_symbols: Dict[str, float]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _normalized_text(*parts: Any) -> str:
    text = " ".join(str(part or "") for part in parts)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _keyword_score(text: str, keywords: Iterable[str]) -> float:
    score = 0.0
    for keyword in keywords:
        token = str(keyword or "").strip().lower()
        if token and token in text:
            score += 1.0 + min(1.5, len(token) / 12.0)
    return score


def _symbol_affinity(question: str, category: str, target_symbols: Dict[str, float]) -> float:
    text = _normalized_text(question)
    if category == "PRICE":
        if "btc" in text or "bitcoin" in text:
            return 1.0
        if "eth" in text or "ethereum" in text:
            return 1.0
        if "sol" in text or "solana" in text:
            return 1.0
        return 0.4 if target_symbols else 0.0
    return max(target_symbols.values()) if target_symbols else 0.0


class MarketResolver:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self.defaults = self.cfg.get("defaults") or {}
        self.categories = {
            name.upper(): ResolvedCategory(
                name=name.upper(),
                keywords=list((item or {}).get("keywords") or []),
                tags=[int(x) for x in ((item or {}).get("tags") or [])],
                max_markets=int((item or {}).get("max_markets") or 12),
                min_liquidity=float((item or {}).get("min_liquidity") or 0.0),
                max_spread=float((item or {}).get("max_spread") or 1.0),
                target_symbols={str(k).upper(): float(v) for k, v in ((item or {}).get("target_symbols") or {}).items()},
            )
            for name, item in (self.cfg.get("categories") or {}).items()
        }

    def expand_event_markets(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        event_id = event.get("id") or event.get("eventId") or event.get("slug")
        event_title = event.get("title") or event.get("question") or event.get("slug") or ""
        event_description = event.get("description") or event.get("body") or ""
        tags = event.get("tags") or []
        markets = event.get("markets") or event.get("children") or []
        for market in markets:
            row = dict(market or {})
            row.setdefault("event_id", str(event_id or ""))
            row.setdefault("event_title", event_title)
            row.setdefault("event_description", event_description)
            row.setdefault("event_tags", tags)
            out.append(row)
        return out

    def normalize_market(self, market: Dict[str, Any], category: str, relevance_score: float, latest_quote: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        outcomes = market.get("outcomes") or market.get("tokens") or market.get("outcomePrices") or []
        token_ids: Dict[str, str] = {}
        outcomes_json: Dict[str, Any] = {}
        if isinstance(outcomes, list):
            for item in outcomes:
                if isinstance(item, dict):
                    outcome_name = str(item.get("outcome") or item.get("name") or item.get("label") or "").upper() or "UNKNOWN"
                    token = str(item.get("asset_id") or item.get("token_id") or item.get("id") or "")
                    if token:
                        token_ids[outcome_name] = token
                    outcomes_json[outcome_name] = item
        if not token_ids:
            outcome_names = [str(x).upper() for x in _json_list(market.get("outcomes"))]
            clob_token_ids = [str(x) for x in _json_list(market.get("clobTokenIds"))]
            outcome_prices = _json_list(market.get("outcomePrices"))
            for idx, outcome_name in enumerate(outcome_names):
                if idx < len(clob_token_ids):
                    token_ids[outcome_name] = clob_token_ids[idx]
                    outcomes_json[outcome_name] = {
                        "outcome": outcome_name,
                        "token_id": clob_token_ids[idx],
                        "price": outcome_prices[idx] if idx < len(outcome_prices) else None,
                    }
        question = str(market.get("question") or market.get("title") or market.get("event_title") or "")
        description = str(market.get("description") or market.get("event_description") or "")
        return {
            "market_id": str(market.get("id") or market.get("market_id") or market.get("conditionId") or market.get("slug") or ""),
            "event_id": market.get("event_id") or market.get("eventId"),
            "slug": market.get("slug"),
            "question": question,
            "description": description,
            "category": category,
            "tags": {"event_tags": market.get("event_tags") or [], "tags": market.get("tags") or []},
            "outcomes": outcomes_json,
            "token_ids": token_ids,
            "end_time": market.get("endDate") or market.get("endTime") or market.get("end_time"),
            "active": bool(market.get("active", True)),
            "closed": bool(market.get("closed", False)),
            "resolved": bool(market.get("resolved", False) or market.get("isResolved", False)),
            "resolution": market.get("resolution") or {},
            "liquidity": _safe_float(market.get("liquidity") or market.get("liquidityClob") or market.get("liquidityNum"), 0.0),
            "volume_24h": _safe_float(market.get("volume24hr") or market.get("volume_24h") or market.get("volume24H"), 0.0),
            "relevance_score": relevance_score,
            "updated_at": market.get("updatedAt") or market.get("updated_at") or utc_now(),
            "payload": {"market": market, "latest_quote": latest_quote or {}},
        }

    def score_market(self, category: ResolvedCategory, market: Dict[str, Any], latest_quote: Optional[Dict[str, Any]] = None) -> float:
        question = market.get("question") or market.get("title") or market.get("event_title") or ""
        description = market.get("description") or market.get("event_description") or ""
        text = _normalized_text(question, description)
        keyword_score = _keyword_score(text, category.keywords)
        liquidity = _safe_float(market.get("liquidity") or market.get("liquidityClob"), 0.0)
        volume = _safe_float(market.get("volume24hr") or market.get("volume_24h"), 0.0)
        liquidity_score = math.log1p(max(0.0, liquidity + volume))
        updated_at = market.get("updatedAt") or market.get("updated_at") or utc_now()
        age_hours = max(0.0, (utc_now() - parse_ts_any(updated_at)).total_seconds() / 3600.0)
        freshness_score = max(0.0, 1.0 - min(age_hours / 72.0, 1.0))
        symbol_score = _symbol_affinity(question, category.name, category.target_symbols)
        spread = _safe_float((latest_quote or {}).get("spread"), 0.0)
        quality_penalty = 0.5 if spread and spread > category.max_spread else 0.0
        return 0.40 * keyword_score + 0.25 * liquidity_score + 0.15 * freshness_score + 0.20 * symbol_score - quality_penalty

    def resolve(
        self,
        *,
        events: List[Dict[str, Any]],
        keyword_search_hits: Dict[str, List[Dict[str, Any]]],
        latest_quotes: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        latest_quote_by_market = {str(item.get("market_id") or ""): item for item in (latest_quotes or []) if item.get("market_id")}
        out_markets: List[Dict[str, Any]] = []
        out_subscriptions: Dict[str, List[Dict[str, Any]]] = {name: [] for name in self.categories.keys()}

        all_markets = []
        for event in events or []:
            all_markets.extend(self.expand_event_markets(event))
        for items in (keyword_search_hits or {}).values():
            all_markets.extend(items or [])

        deduped: Dict[str, Dict[str, Any]] = {}
        for market in all_markets:
            key = str(market.get("id") or market.get("market_id") or market.get("conditionId") or market.get("slug") or "")
            if key and key not in deduped:
                deduped[key] = market

        for category_name, category in self.categories.items():
            scored: List[Tuple[float, Dict[str, Any], Optional[Dict[str, Any]]]] = []
            for market in deduped.values():
                question = _normalized_text(market.get("question") or market.get("title") or market.get("event_title"))
                if category.keywords and not any(str(k).lower() in question for k in category.keywords):
                    if category_name == "PRICE":
                        if not any(k.lower() in _normalized_text(market.get("description"), market.get("event_description")) for k in category.keywords):
                            continue
                    else:
                        continue
                latest_quote = latest_quote_by_market.get(str(market.get("id") or market.get("market_id") or market.get("conditionId") or market.get("slug") or ""))
                spread = _safe_float((latest_quote or {}).get("spread"), 0.0)
                liquidity = _safe_float(market.get("liquidity") or market.get("liquidityClob"), 0.0)
                if liquidity < category.min_liquidity:
                    continue
                if spread and spread > category.max_spread:
                    continue
                score = self.score_market(category, market, latest_quote)
                scored.append((score, market, latest_quote))
            scored.sort(key=lambda x: x[0], reverse=True)
            for score, market, latest_quote in scored[: category.max_markets]:
                normalized = self.normalize_market(market, category_name, score, latest_quote)
                if normalized["market_id"]:
                    out_markets.append(normalized)
                for outcome, token_id in (normalized.get("token_ids") or {}).items():
                    if outcome not in {"YES", "NO"}:
                        continue
                    out_subscriptions[category_name].append(
                        {
                            "market_id": normalized["market_id"],
                            "token_id": token_id,
                            "outcome": outcome,
                            "relevance_score": score,
                            "symbol_weights": category.target_symbols,
                            "min_liquidity": category.min_liquidity,
                            "max_spread": category.max_spread,
                            "enabled": True,
                        }
                    )
        # market list de-dup after category tagging
        market_map = {item["market_id"]: item for item in out_markets if item.get("market_id")}
        return {"markets": list(market_map.values()), "subscriptions": out_subscriptions}
