"""Research workbench API for the advanced research page."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from core.news.storage import db as news_db
from web.api.data import (
    get_factor_library,
    get_fama_like_factors,
    get_multi_assets_overview,
    get_onchain_overview,
    get_research_symbols,
    resolve_onchain_chain_context,
)
from web.api.trading import (
    get_analytics_history_status,
    get_behavior_report,
    get_community_overview,
    get_market_microstructure,
    get_risk_dashboard,
    get_stoploss_policy,
    get_trading_calendar,
)

router = APIRouter()
_UI_TIMEZONE = (
    str(
        os.environ.get("CTS_UI_TIMEZONE")
        or os.environ.get("UI_TIMEZONE")
        or "Asia/Shanghai"
    ).strip()
    or "Asia/Shanghai"
)
try:
    _UI_ZONEINFO = ZoneInfo(_UI_TIMEZONE)
except Exception:
    _UI_ZONEINFO = timezone.utc
_TIMEZONE_BASIS = f"UTC storage, {_UI_TIMEZONE} display"

_VALID_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d"}
_DEFAULT_UNIVERSE = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "TRX/USDT",
    "LINK/USDT",
    "AVAX/USDT",
    "DOT/USDT",
    "POL/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "ETC/USDT",
    "ATOM/USDT",
    "NEAR/USDT",
    "APT/USDT",
    "ARB/USDT",
    "OP/USDT",
    "SUI/USDT",
    "INJ/USDT",
    "RUNE/USDT",
    "AAVE/USDT",
    "MKR/USDT",
    "UNI/USDT",
    "FIL/USDT",
    "HBAR/USDT",
    "ICP/USDT",
    "TON/USDT",
]


def _analytics_status_collectors_to_map(
    payload: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    collectors = list((payload or {}).get("collectors") or [])
    mapped: Dict[str, Any] = {}
    for item in collectors:
        collector = str((item or {}).get("collector") or "").strip()
        if collector:
            mapped[collector] = dict(item or {})
    return mapped


_MODULE_ORDER = ["market_state", "factors", "cross_asset", "onchain", "discipline"]
_MODULE_TIMEOUT_SEC = {
    "market_state": 40.0,
    "factors": 45.0,
    "cross_asset": 16.0,
    "onchain": 30.0,
    "discipline": 5.0,
}
_MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC = 5 * 60
_MARKET_STATE_NEWS_TIMEOUT_SEC = 12.0
_MARKET_STATE_PUBLIC_MARKET_TIMEOUT_SEC = 9.0
# Live microstructure / community fetches. They are the only source while the
# analytics-history collectors are off; a cold pass measured 3.7-4.1 s and 6-7 s
# (2026-09-28), so the old 4 s always fell back. The module budget is 40 s and
# results are served stale-while-revalidate, so only the first cold call waits.
_MARKET_STATE_LIVE_FETCH_TIMEOUT_SEC = 9.0
_ONCHAIN_WARMUP_WAIT_SEC = 14.0  # cold on-chain overview takes ~10 s; the module budget is 30 s
_COINGLASS_PREFERRED_MAX_AGE_SEC = 5 * 60
_MACRO_MARKET_STALE_MAX_AGE_SEC = 3 * 24 * 60 * 60
_MACRO_MONTHLY_STALE_MAX_AGE_SEC = 62 * 24 * 60 * 60
_PUBLIC_MARKET_DATA_CACHE_TTL_SEC = 5 * 60
_PUBLIC_FEAR_GREED_STALE_MAX_AGE_SEC = 6 * 60 * 60
_PUBLIC_MARKET_BREADTH_STALE_MAX_AGE_SEC = 30 * 60
_PUBLIC_MARKET_DATA_CACHE: Dict[str, Dict[str, Any]] = {}
_NEWS_SUMMARY_CACHE_TTL_SEC = 5 * 60
_NEWS_SUMMARY_STALE_MAX_AGE_SEC = 30 * 60
_NEWS_SUMMARY_CACHE: Dict[str, Dict[str, Any]] = {}
_PUBLIC_MARKET_DATA_RUNTIME_FIELDS = {
    "cache_hit",
    "cache_age_sec",
    "stale",
    "stale_reason",
    "source_status",
}
_NEWS_SUMMARY_RUNTIME_FIELDS = {
    "cache_hit",
    "cache_age_sec",
    "stale",
    "stale_reason",
    "source_status",
}


class ResearchProfile(BaseModel):
    exchange: str = "binance"
    primary_symbol: str = "BTC/USDT"
    universe_symbols: List[str] = Field(default_factory=lambda: list(_DEFAULT_UNIVERSE))
    timeframe: str = "5m"
    lookback: int = 1200
    exclude_retired: bool = True
    horizon: str = "short_intraday"


class ResearchWorkbenchRequest(BaseModel):
    profile: ResearchProfile = Field(default_factory=ResearchProfile)

    @model_validator(mode="before")
    @classmethod
    def coerce_profile(cls, value: Any) -> Any:
        if isinstance(value, dict) and "profile" not in value:
            keys = {
                "exchange",
                "primary_symbol",
                "universe_symbols",
                "timeframe",
                "lookback",
                "exclude_retired",
                "horizon",
            }
            if any(key in value for key in keys):
                return {"profile": value}
        return value


class ResearchRecommendationRequest(BaseModel):
    profile: ResearchProfile = Field(default_factory=ResearchProfile)
    overview: Optional[Dict[str, Any]] = None
    modules: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def coerce_profile(cls, value: Any) -> Any:
        if isinstance(value, dict) and "profile" not in value:
            keys = {
                "exchange",
                "primary_symbol",
                "universe_symbols",
                "timeframe",
                "lookback",
                "exclude_retired",
                "horizon",
            }
            if any(key in value for key in keys):
                cloned = dict(value)
                profile = {
                    key: cloned.pop(key) for key in list(cloned.keys()) if key in keys
                }
                cloned["profile"] = profile
                return cloned
        return value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _utc_iso(value: Any) -> Optional[str]:
    dt = _coerce_utc_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _local_iso(value: Any) -> Optional[str]:
    dt = _coerce_utc_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(_UI_ZONEINFO).isoformat()


def _symbol_to_news_key(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "BTC"
    main = raw.split(":")[0]
    if "/" in main:
        return main.split("/")[0]
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if main.endswith(suffix):
            return main[: -len(suffix)] or main
    return main


def _symbol_to_news_keys(symbol: str) -> List[str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ["BTCUSDT", "BTC"]
    main = raw.split(":")[0]
    keys: List[str] = []

    compact = main.replace("/", "")
    if compact:
        keys.append(compact)

    asset_key = _symbol_to_news_key(main)
    if asset_key and asset_key not in keys:
        keys.append(asset_key)

    if main and "/" not in main and main not in keys:
        keys.append(main)
    return keys


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "BTC/USDT"
    if ":" in raw:
        raw = raw.split(":")[0]
    if "/" not in raw:
        raw = f"{raw}/USDT"
    return raw


def _normalize_profile(profile: ResearchProfile) -> ResearchProfile:
    exchange = str(profile.exchange or "binance").strip().lower() or "binance"
    timeframe = str(profile.timeframe or "5m").strip().lower()
    if timeframe not in _VALID_TIMEFRAMES:
        timeframe = "5m"
    lookback = max(120, min(int(profile.lookback or 1200), 5000))
    primary_symbol = _normalize_symbol(profile.primary_symbol or "BTC/USDT")

    universe: List[str] = []
    seen = set()
    for symbol in [primary_symbol, *(profile.universe_symbols or [])]:
        normalized = _normalize_symbol(symbol)
        if normalized in seen:
            continue
        seen.add(normalized)
        universe.append(normalized)
    if not universe:
        universe = list(_DEFAULT_UNIVERSE)
    if primary_symbol not in universe:
        universe.insert(0, primary_symbol)

    return ResearchProfile(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe[:30],
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=bool(profile.exclude_retired),
        horizon=str(profile.horizon or "short_intraday"),
    )


def _build_profile_from_query(
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> ResearchProfile:
    symbols = [
        item.strip() for item in str(universe_symbols or "").split(",") if item.strip()
    ]
    return _normalize_profile(
        ResearchProfile(
            exchange=exchange,
            primary_symbol=primary_symbol,
            universe_symbols=symbols or list(_DEFAULT_UNIVERSE),
            timeframe=timeframe,
            lookback=lookback,
            exclude_retired=exclude_retired,
            horizon=horizon,
        )
    )


def _profile_symbol_window(profile: ResearchProfile, limit: int) -> List[str]:
    universe = list(profile.universe_symbols or [])
    if not universe:
        universe = [profile.primary_symbol]
    return universe[: max(2, int(limit))]


def _status_from_flags(
    ok: bool = True, degraded: bool = False
) -> Literal["ok", "degraded", "error"]:
    if not ok:
        return "error"
    return "degraded" if degraded else "ok"


def _module_result(
    module: str,
    *,
    status: Literal["ok", "degraded", "error"],
    source_labels: List[str],
    summary: Dict[str, Any],
    payload: Dict[str, Any],
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "module": module,
        "status": status,
        "freshness_sec": 0,
        "source_labels": source_labels,
        "warnings": list(warnings or []),
        "summary": summary,
        "payload": payload,
        "generated_at": _now_iso(),
    }


def _extract_module_payload(module_or_wrapper: Any) -> Dict[str, Any]:
    if not isinstance(module_or_wrapper, dict):
        return {}
    payload = module_or_wrapper.get("payload")
    return payload if isinstance(payload, dict) else module_or_wrapper


async def _wait_or_none(coro: Any, timeout_sec: float) -> Any:
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec)
    except Exception:
        return None


def _consume_background_task_result(task: asyncio.Task) -> None:
    with contextlib.suppress(asyncio.CancelledError, Exception):
        task.result()


async def _wait_or_none_keep_running(coro: Any, timeout_sec: float) -> Any:
    task = asyncio.create_task(coro)
    task.add_done_callback(_consume_background_task_result)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_sec)
    except Exception:
        return None


def _public_market_cache_entry(
    cache_key: str,
    *,
    max_age_sec: Optional[float] = _PUBLIC_MARKET_DATA_CACHE_TTL_SEC,
) -> tuple[Optional[Dict[str, Any]], Optional[float]]:
    cached = _PUBLIC_MARKET_DATA_CACHE.get(str(cache_key or "").strip())
    if not isinstance(cached, dict):
        return None, None
    age_sec = max(0.0, time.time() - float(cached.get("ts") or 0.0))
    if max_age_sec is not None and age_sec > float(max_age_sec):
        return None, age_sec
    payload = cached.get("payload")
    if not isinstance(payload, dict):
        return None, age_sec
    return dict(payload), age_sec


def _cached_public_market_data(cache_key: str) -> Dict[str, Any]:
    payload, _ = _public_market_cache_entry(
        cache_key, max_age_sec=_PUBLIC_MARKET_DATA_CACHE_TTL_SEC
    )
    return dict(payload or {})


def _status_error_text(error: Any, limit: int = 240) -> str:
    text = str(error or "").strip()
    if not text:
        return ""
    return text[:limit]


def _strip_public_market_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(payload or {}).items()
        if key not in _PUBLIC_MARKET_DATA_RUNTIME_FIELDS
    }


def _with_public_market_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    out = dict(_strip_public_market_runtime_fields(payload))
    out["cache_hit"] = bool(cache_hit)
    out["cache_age_sec"] = (
        round(float(cache_age_sec or 0.0), 3) if cache_age_sec is not None else None
    )
    out["stale"] = bool(stale)
    out["source_status"] = str(
        source_status
        or ("cache_stale" if stale else ("cache_fresh" if cache_hit else "live"))
    )
    if stale_reason:
        out["stale_reason"] = str(stale_reason)
    return out


def _public_market_unavailable(source: str, error: Any) -> Dict[str, Any]:
    message = _status_error_text(error)
    return {
        "available": False,
        "source": str(source or "").strip() or None,
        "error": message or None,
        "cache_hit": False,
        "cache_age_sec": None,
        "stale": False,
        "source_status": "unavailable",
    }


def _store_public_market_data(
    cache_key: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    snapshot = _strip_public_market_runtime_fields(dict(payload or {}))
    _PUBLIC_MARKET_DATA_CACHE[str(cache_key or "").strip()] = {
        "ts": time.time(),
        "payload": dict(snapshot),
    }
    return dict(snapshot)


def _news_summary_cache_key(symbol: str, hours: int) -> str:
    return f"{_symbol_to_news_key(symbol)}|{max(1, min(int(hours or 24), 168))}"


def _news_summary_cache_entry(
    cache_key: str,
    *,
    max_age_sec: Optional[float] = _NEWS_SUMMARY_CACHE_TTL_SEC,
) -> tuple[Optional[Dict[str, Any]], Optional[float]]:
    cached = _NEWS_SUMMARY_CACHE.get(str(cache_key or "").strip())
    if not isinstance(cached, dict):
        return None, None
    age_sec = max(0.0, time.time() - float(cached.get("ts") or 0.0))
    if max_age_sec is not None and age_sec > float(max_age_sec):
        return None, age_sec
    payload = cached.get("payload")
    if not isinstance(payload, dict):
        return None, age_sec
    return dict(payload), age_sec


def _strip_news_summary_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(payload or {}).items()
        if key not in _NEWS_SUMMARY_RUNTIME_FIELDS
    }


def _store_news_summary(cache_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = _strip_news_summary_runtime_fields(dict(payload or {}))
    _NEWS_SUMMARY_CACHE[str(cache_key or "").strip()] = {
        "ts": time.time(),
        "payload": dict(snapshot),
    }
    return dict(snapshot)


def _with_news_summary_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    out = dict(_strip_news_summary_runtime_fields(payload))
    out["cache_hit"] = bool(cache_hit)
    out["cache_age_sec"] = (
        round(float(cache_age_sec or 0.0), 3) if cache_age_sec is not None else None
    )
    out["stale"] = bool(stale)
    out["source_status"] = str(
        source_status
        or ("cache_stale" if stale else ("cache_fresh" if cache_hit else "live"))
    )
    if stale_reason:
        out["stale_reason"] = _status_error_text(stale_reason)
    return out


def _news_summary_timeout_fallback(
    symbol: str,
    hours: int,
    *,
    reason: str = "overview_timeout",
) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    cache_key = _news_summary_cache_key(symbol, hours)
    stale_cached, stale_age = _news_summary_cache_entry(
        cache_key,
        max_age_sec=_NEWS_SUMMARY_STALE_MAX_AGE_SEC,
    )
    if stale_cached is not None:
        return _with_news_summary_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            stale_reason=reason,
            source_status="cache_stale",
        )
    now = _now_iso()
    return {
        "symbol": _symbol_to_news_key(symbol),
        "query_symbols": _symbol_to_news_keys(symbol),
        "hours": int(hours),
        "scope": "pending",
        "events_count": 0,
        "raw_count": 0,
        "feed_count": 0,
        "active_provider_count": 0,
        "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
        "by_type": {},
        "source_states": [],
        "llm_queue": {},
        "timestamp": now,
        "generated_at_utc": now,
        "generated_at_local": _local_iso(now),
        "ui_timezone": _UI_TIMEZONE,
        "timezone_basis": _TIMEZONE_BASIS,
        "source_errors": [],
        "cache_hit": False,
        "cache_age_sec": None,
        "stale": True,
        "stale_reason": reason,
        "source_status": "pending_timeout",
    }


def _public_market_timeout_fallback(
    cache_key: str,
    *,
    source: str,
    stale_max_age_sec: float,
    reason: str = "overview_timeout",
) -> Dict[str, Any]:
    stale_cached, stale_age = _public_market_cache_entry(
        cache_key,
        max_age_sec=stale_max_age_sec,
    )
    if stale_cached is not None:
        return _with_public_market_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            stale_reason=reason,
            source_status="cache_stale",
        )
    return {
        "available": None,
        "source": source,
        "error": None,
        "cache_hit": False,
        "cache_age_sec": None,
        "stale": True,
        "stale_reason": reason,
        "source_status": "pending_timeout",
    }


def _source_pending_timeout(payload: Optional[Dict[str, Any]]) -> bool:
    return str((payload or {}).get("source_status") or "").strip().lower() == "pending_timeout"


async def _load_public_fear_greed_snapshot() -> Dict[str, Any]:
    cached, cached_age = _public_market_cache_entry(
        "fear_greed", max_age_sec=_PUBLIC_MARKET_DATA_CACHE_TTL_SEC
    )
    if cached:
        return _with_public_market_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )
    stale_cached, stale_age = _public_market_cache_entry(
        "fear_greed",
        max_age_sec=_PUBLIC_FEAR_GREED_STALE_MAX_AGE_SEC,
    )

    try:
        from core.data.sentiment.fear_greed_collector import (
            FearGreedCollector,
        )  # noqa: PLC0415
    except Exception as exc:
        if stale_cached:
            return _with_public_market_runtime_fields(
                stale_cached,
                cache_hit=True,
                cache_age_sec=stale_age,
                stale=True,
                stale_reason=exc,
                source_status="cache_stale",
            )
        return _public_market_unavailable("alternative.me", exc)

    try:
        async with FearGreedCollector(timeout=8) as collector:
            current = await collector.fetch_current()
    except Exception as exc:
        if stale_cached:
            return _with_public_market_runtime_fields(
                stale_cached,
                cache_hit=True,
                cache_age_sec=stale_age,
                stale=True,
                stale_reason=exc,
                source_status="cache_stale",
            )
        return _public_market_unavailable("alternative.me", exc)

    if not current:
        if stale_cached:
            return _with_public_market_runtime_fields(
                stale_cached,
                cache_hit=True,
                cache_age_sec=stale_age,
                stale=True,
                stale_reason="empty_response",
                source_status="cache_stale",
            )
        return _public_market_unavailable("alternative.me", "empty_response")

    payload = {
        "available": True,
        "value": int(getattr(current, "value", 0)),
        "classification": str(getattr(current, "classification", "") or ""),
        "signal": str(getattr(current, "signal", "") or ""),
        "signal_strength": round(
            float(getattr(current, "signal_strength", 0.0) or 0.0), 4
        ),
        "timestamp": (
            getattr(current, "timestamp", None).isoformat()
            if getattr(current, "timestamp", None)
            else None
        ),
        "time_until_update": getattr(current, "time_until_update", None),
        "source": "alternative.me",
    }
    return _with_public_market_runtime_fields(
        _store_public_market_data("fear_greed", payload),
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


async def _load_public_market_breadth_snapshot() -> Dict[str, Any]:
    cached, cached_age = _public_market_cache_entry(
        "global_market_breadth", max_age_sec=_PUBLIC_MARKET_DATA_CACHE_TTL_SEC
    )
    if cached:
        return _with_public_market_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )
    stale_cached, stale_age = _public_market_cache_entry(
        "global_market_breadth",
        max_age_sec=_PUBLIC_MARKET_BREADTH_STALE_MAX_AGE_SEC,
    )

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            headers=headers,
            follow_redirects=True,
            trust_env=True,
        ) as client:
            response = await client.get("https://api.coingecko.com/api/v3/global")
            response.raise_for_status()
            payload = dict((response.json() or {}).get("data") or {})
    except Exception as exc:
        if stale_cached:
            return _with_public_market_runtime_fields(
                stale_cached,
                cache_hit=True,
                cache_age_sec=stale_age,
                stale=True,
                stale_reason=exc,
                source_status="cache_stale",
            )
        return _public_market_unavailable("coingecko_global", exc)

    total_market_cap = dict(payload.get("total_market_cap") or {})
    total_volume = dict(payload.get("total_volume") or {})
    market_cap_pct = dict(payload.get("market_cap_percentage") or {})
    market_breadth = {
        "available": True,
        "source": "coingecko_global",
        "active_cryptocurrencies": int(payload.get("active_cryptocurrencies") or 0),
        "markets": int(payload.get("markets") or 0),
        "total_market_cap_usd": _coerce_finite_float(total_market_cap.get("usd")),
        "total_volume_usd": _coerce_finite_float(total_volume.get("usd")),
        "market_cap_change_pct_24h": _coerce_finite_float(
            payload.get("market_cap_change_percentage_24h_usd")
        ),
        "volume_change_pct_24h": _coerce_finite_float(
            payload.get("volume_change_percentage_24h_usd")
        ),
        "btc_dominance_pct": _coerce_finite_float(market_cap_pct.get("btc")),
        "eth_dominance_pct": _coerce_finite_float(market_cap_pct.get("eth")),
        "updated_at": payload.get("updated_at"),
    }
    return _with_public_market_runtime_fields(
        _store_public_market_data("global_market_breadth", market_breadth),
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


async def _load_preferred_coinglass_overview(
    symbol: str,
    *,
    max_age_sec: float = _COINGLASS_PREFERRED_MAX_AGE_SEC,
) -> Dict[str, Any]:
    try:
        from core.data.coinglass_feature_builder import (
            build_coinglass_overview_payload,
        )  # noqa: PLC0415
    except Exception:
        return {}

    try:
        overview = dict(
            await build_coinglass_overview_payload(
                symbol=symbol, refresh=False, manual=False
            )
            or {}
        )
    except Exception:
        return {}

    return overview


def _merge_nested_payload(primary: Any, fallback: Any) -> Any:
    if isinstance(primary, dict) and isinstance(fallback, dict):
        merged = dict(fallback)
        for key, value in primary.items():
            merged[key] = _merge_nested_payload(value, merged.get(key))
        return merged
    if primary is None:
        return fallback
    return primary


async def _build_news_summary(symbol: str, hours: int = 24) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    cache_key = _news_summary_cache_key(symbol, hours)
    cached, cached_age = _news_summary_cache_entry(
        cache_key, max_age_sec=_NEWS_SUMMARY_CACHE_TTL_SEC
    )
    if cached and _news_summary_sample_count(cached) > 0:
        return _with_news_summary_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )
    stale_cached, stale_age = _news_summary_cache_entry(
        cache_key, max_age_sec=_NEWS_SUMMARY_STALE_MAX_AGE_SEC
    )
    if stale_cached is not None and _news_summary_sample_count(stale_cached) <= 0:
        stale_cached = None

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    symbol_key = _symbol_to_news_key(symbol)
    symbol_keys = _symbol_to_news_keys(symbol)
    db_timeout = 8.0
    event_tasks = [
        asyncio.wait_for(
            news_db.list_events(symbol=news_symbol, since=since, limit=300),
            timeout=db_timeout,
        )
        for news_symbol in symbol_keys
    ]
    raw_task = asyncio.wait_for(
        news_db.list_news_raw(since=since, limit=400), timeout=db_timeout
    )
    states_task = asyncio.wait_for(news_db.list_source_states(), timeout=db_timeout)
    queue_task = asyncio.wait_for(news_db.get_llm_queue_stats(), timeout=db_timeout)
    (
        event_results,
        raw_rows_raw,
        source_states_raw,
        llm_queue_raw,
    ) = await asyncio.gather(
        asyncio.gather(*event_tasks, return_exceptions=True),
        raw_task,
        states_task,
        queue_task,
        return_exceptions=True,
    )

    source_errors: List[str] = []
    events: List[Dict[str, Any]] = []
    if not isinstance(event_results, Exception):
        seen_event_ids: set[str] = set()
        for batch in list(event_results or []):
            if isinstance(batch, Exception):
                source_errors.append(_status_error_text(batch))
                continue
            for event in list(batch or []):
                event_id = str((event or {}).get("event_id") or "").strip()
                dedupe_key = event_id or str(event)
                if dedupe_key in seen_event_ids:
                    continue
                seen_event_ids.add(dedupe_key)
                events.append(dict(event or {}))
    else:
        source_errors.append(_status_error_text(event_results))
    if isinstance(raw_rows_raw, Exception):
        source_errors.append(_status_error_text(raw_rows_raw))
    raw_rows = [] if isinstance(raw_rows_raw, Exception) else list(raw_rows_raw or [])
    if isinstance(source_states_raw, Exception):
        source_errors.append(_status_error_text(source_states_raw))
    source_states = (
        []
        if isinstance(source_states_raw, Exception)
        else list(source_states_raw or [])
    )
    if isinstance(llm_queue_raw, Exception):
        source_errors.append(_status_error_text(llm_queue_raw))
    llm_queue = (
        {} if isinstance(llm_queue_raw, Exception) else dict(llm_queue_raw or {})
    )

    sentiment = {"positive": 0, "neutral": 0, "negative": 0}
    by_type: Dict[str, int] = {}

    def consume(rows: List[Dict[str, Any]]) -> None:
        for event in rows:
            score = int(event.get("sentiment") or 0)
            if score > 0:
                sentiment["positive"] += 1
            elif score < 0:
                sentiment["negative"] += 1
            else:
                sentiment["neutral"] += 1
            event_type = str(event.get("event_type") or "other")
            by_type[event_type] = by_type.get(event_type, 0) + 1

    scope = "symbol"
    consume(events)
    if not events:
        scope = "global_fallback"
        try:
            events = await asyncio.wait_for(
                news_db.list_events(symbol=None, since=since, limit=300), timeout=5.0
            )
        except Exception as exc:
            source_errors.append(_status_error_text(exc))
            events = []
        sentiment = {"positive": 0, "neutral": 0, "negative": 0}
        by_type = {}
        consume(events)

    recent_flow_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=min(4, max(1, int(hours or 24) // 6 or 1))
    )
    feed_count = 0
    active_providers: set[str] = set()
    for row in raw_rows or []:
        provider = str(
            row.get("provider") or ((row.get("payload") or {}).get("provider")) or ""
        ).strip()
        if provider:
            active_providers.add(provider)
        published_raw = (
            row.get("published_at") or row.get("timestamp") or row.get("created_at")
        )
        published_at: Optional[datetime] = None
        if isinstance(published_raw, datetime):
            published_at = (
                published_raw
                if published_raw.tzinfo
                else published_raw.replace(tzinfo=timezone.utc)
            )
        else:
            text = str(published_raw or "").strip()
            if text:
                try:
                    published_at = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)
                except Exception:
                    published_at = None
        if published_at and published_at >= recent_flow_cutoff:
            feed_count += 1
    if feed_count <= 0 and raw_rows:
        feed_count = max(1, min(len(raw_rows), len(active_providers) or 0))

    generated_at = _now_iso()
    payload = {
        "symbol": symbol_key,
        "query_symbols": symbol_keys,
        "hours": int(hours),
        "scope": scope,
        "events_count": int(len(events or [])),
        "raw_count": int(len(raw_rows or [])),
        "feed_count": int(feed_count),
        "active_provider_count": int(len(active_providers)),
        "sentiment": sentiment,
        "by_type": dict(
            sorted(by_type.items(), key=lambda item: item[1], reverse=True)[:8]
        ),
        "source_states": source_states,
        "llm_queue": llm_queue,
        "timestamp": generated_at,
        "generated_at_utc": generated_at,
        "generated_at_local": _local_iso(generated_at),
        "window_since_utc": _utc_iso(since),
        "window_since_local": _local_iso(since),
        "ui_timezone": _UI_TIMEZONE,
        "timezone_basis": _TIMEZONE_BASIS,
        "source_errors": [item for item in source_errors if item][:6],
    }
    if (
        _news_summary_sample_count(payload) <= 0
        and stale_cached is not None
    ):
        return _with_news_summary_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            stale_reason=" | ".join(payload["source_errors"][:4]),
            source_status="cache_stale",
        )
    if _news_summary_sample_count(payload) <= 0:
        return _with_news_summary_runtime_fields(
            payload,
            cache_hit=False,
            cache_age_sec=0.0,
            stale=False,
            source_status="empty",
        )
    return _with_news_summary_runtime_fields(
        _store_news_summary(cache_key, payload),
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live_partial" if payload["source_errors"] else "live",
    )


def _extract_long_short_ratio(payload: Dict[str, Any]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    row = payload.get("long_short_ratio") or {}
    ratio = float(
        row.get("long_short_ratio") or row.get("ratio") or row.get("ls_ratio") or 0.0
    )
    return ratio if ratio > 0 else None


def _microstructure_wall_bias(payload: Dict[str, Any]) -> float:
    if not isinstance(payload, dict):
        return 0.0
    rows = list(payload.get("large_orders") or [])
    bid_notional = 0.0
    ask_notional = 0.0
    for row in rows[:20]:
        if not isinstance(row, dict):
            continue
        side = str(row.get("side") or "").strip().lower()
        notional = float(row.get("notional") or 0.0)
        if notional <= 0:
            continue
        if side == "bid":
            bid_notional += notional
        elif side == "ask":
            ask_notional += notional
    total = bid_notional + ask_notional
    return round(((bid_notional - ask_notional) / total) if total > 0 else 0.0, 6)


def _build_microstructure_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    orderbook = dict((payload or {}).get("orderbook") or {})
    aggressor = dict((payload or {}).get("aggressor_flow") or {})
    long_short = dict((payload or {}).get("long_short_ratio") or {})
    iceberg = dict((payload or {}).get("iceberg_detection") or {})
    large_orders = list((payload or {}).get("large_orders") or [])

    has_depth_rows = bool(orderbook.get("bid_depth") or orderbook.get("ask_depth"))
    has_mid_price = float(orderbook.get("mid_price") or 0.0) > 0
    has_flow = (
        bool(aggressor.get("count"))
        or abs(float(aggressor.get("imbalance") or 0.0)) > 0
    )
    long_short_ratio = _extract_long_short_ratio(payload)
    wall_bias = _microstructure_wall_bias(payload)
    iceberg_count = int(float(iceberg.get("candidate_count") or 0.0))
    actionable_signal = bool(
        has_mid_price
        or has_depth_rows
        or has_flow
        or bool(large_orders)
        or bool(long_short_ratio and long_short_ratio > 0)
        or iceberg_count > 0
        or bool((payload.get("funding_rate") or {}).get("available"))
        or bool((payload.get("spot_futures_basis") or {}).get("available"))
    )

    return {
        "has_actionable_signal": actionable_signal,
        "has_orderbook_depth": has_depth_rows,
        "has_mid_price": has_mid_price,
        "has_aggressor_flow": has_flow,
        "large_order_count": len(large_orders),
        "iceberg_candidates": iceberg_count,
        "long_short_ratio_available": bool(long_short.get("available"))
        or bool(long_short_ratio and long_short_ratio > 0),
        "long_short_ratio": round(long_short_ratio, 6) if long_short_ratio else None,
        "wall_bias": wall_bias,
    }


def _build_derivatives_shadow_summary(
    history_status: Dict[str, Any],
    onchain: Dict[str, Any],
) -> Dict[str, Any]:
    derivatives = dict((history_status or {}).get("derivatives") or {})
    details = dict(derivatives.get("details") or {})
    quota_headroom = dict(details.get("quota_headroom") or {})
    snapshot = dict(details.get("snapshot") or {})
    snapshot_payload = dict(snapshot.get("payload") or {})
    active_datasets = list(details.get("active_datasets") or [])
    freshness_raw = details.get("freshness_sec")
    try:
        freshness_sec = float(freshness_raw) if freshness_raw is not None else None
    except Exception:
        freshness_sec = None
    funding_multi = dict((onchain or {}).get("funding_rate_multi_source") or {})
    funding_count = int(funding_multi.get("count") or 0)
    funding_mean = _coerce_finite_float(snapshot_payload.get("funding_mean"))
    funding_mean_rate_pct = (funding_mean * 100.0) if funding_mean is not None else None
    if funding_mean_rate_pct is None and funding_count > 0:
        funding_mean_rate_pct = float(funding_multi.get("mean_rate_pct") or 0.0)
    derivatives_labels = [
        str(item).strip()
        for item in list(snapshot_payload.get("derivatives_labels") or [])
        if str(item).strip()
    ]

    return {
        "available": bool(derivatives.get("available")),
        "status": str(derivatives.get("status") or "missing"),
        "key_configured": bool(details.get("key_configured")),
        "provider": str(details.get("provider") or "coinglass"),
        "freshness_sec": freshness_sec,
        "degraded_reason": details.get("degraded_reason"),
        "active_datasets": active_datasets,
        "dataset_count": len(active_datasets),
        "quota_headroom": quota_headroom,
        "snapshot_at": snapshot.get("timestamp"),
        "history_ready": bool(snapshot_payload.get("history_ready")),
        "history_exchange": snapshot_payload.get("history_exchange"),
        "history_interval": snapshot_payload.get("history_interval"),
        "funding_rate": _coerce_finite_float(snapshot.get("funding_rate")),
        "funding_mean": funding_mean,
        "funding_mean_rate_pct": funding_mean_rate_pct,
        "funding_zscore": _coerce_finite_float(snapshot_payload.get("funding_zscore")),
        "funding_reversion_speed": _coerce_finite_float(
            snapshot_payload.get("funding_reversion_speed")
        ),
        "long_short_ratio": _coerce_finite_float(snapshot.get("long_short_ratio")),
        "long_short_ratio_change_24h": _coerce_finite_float(
            snapshot_payload.get("long_short_ratio_change_24h")
        ),
        "liquidation_burst_score": _coerce_finite_float(
            snapshot_payload.get("liquidation_burst_score")
        ),
        "derivatives_heat_score": _coerce_finite_float(
            snapshot_payload.get("derivatives_heat_score")
        ),
        "crowding_score": _coerce_finite_float(snapshot.get("crowding_score")),
        "squeeze_score": _coerce_finite_float(snapshot.get("squeeze_score")),
        "distribution_score": _coerce_finite_float(snapshot.get("distribution_score")),
        "basis_pct": _coerce_finite_float(snapshot.get("basis_pct")),
        "taker_buy_sell_imbalance": _coerce_finite_float(
            snapshot.get("taker_buy_sell_imbalance")
        ),
        "crowded_long": bool(snapshot_payload.get("crowded_long")),
        "crowded_short": bool(snapshot_payload.get("crowded_short")),
        "squeeze_building": bool(snapshot_payload.get("squeeze_building")),
        "flush_risk": bool(snapshot_payload.get("flush_risk")),
        "basis_dislocation": bool(snapshot_payload.get("basis_dislocation")),
        "flow_divergence": bool(snapshot_payload.get("flow_divergence")),
        "order_flow_confirmed": bool(snapshot_payload.get("order_flow_confirmed")),
        "derivatives_labels": derivatives_labels,
    }


def _build_derivatives_summary_from_overview(
    overview: Dict[str, Any]
) -> Dict[str, Any]:
    overview = dict(overview or {})
    snapshot = dict(overview.get("snapshot") or {})
    snapshot_payload = dict(snapshot.get("payload") or {})
    active_datasets = list(
        overview.get("active_datasets") or snapshot_payload.get("active_datasets") or []
    )
    freshness_sec = _coerce_finite_float(overview.get("freshness_sec"))
    degraded_reason = str(overview.get("degraded_reason") or "").strip() or None
    status_rows = list(overview.get("status") or [])
    funding_mean = _coerce_finite_float(snapshot_payload.get("funding_mean"))
    derivatives_labels = [
        str(item).strip()
        for item in list(snapshot_payload.get("derivatives_labels") or [])
        if str(item).strip()
    ]

    status = "missing"
    if bool(overview.get("available")) and not degraded_reason:
        status = "ok"
    elif (
        degraded_reason
        or active_datasets
        or bool(overview.get("key_configured"))
        or status_rows
    ):
        status = "degraded"

    return {
        "available": bool(overview.get("available")),
        "status": status,
        "key_configured": bool(overview.get("key_configured")),
        "provider": "coinglass",
        "freshness_sec": freshness_sec,
        "degraded_reason": degraded_reason,
        "active_datasets": active_datasets,
        "dataset_count": len(active_datasets),
        "quota_headroom": dict(overview.get("quota_headroom") or {}),
        "snapshot_at": snapshot.get("timestamp"),
        "history_ready": bool(snapshot_payload.get("history_ready")),
        "history_exchange": snapshot_payload.get("history_exchange"),
        "history_interval": snapshot_payload.get("history_interval"),
        "funding_rate": _coerce_finite_float(snapshot.get("funding_rate")),
        "funding_mean": funding_mean,
        "funding_mean_rate_pct": (funding_mean * 100.0)
        if funding_mean is not None
        else None,
        "funding_zscore": _coerce_finite_float(snapshot_payload.get("funding_zscore")),
        "funding_reversion_speed": _coerce_finite_float(
            snapshot_payload.get("funding_reversion_speed")
        ),
        "long_short_ratio": _coerce_finite_float(snapshot.get("long_short_ratio")),
        "long_short_ratio_change_24h": _coerce_finite_float(
            snapshot_payload.get("long_short_ratio_change_24h")
        ),
        "liquidation_burst_score": _coerce_finite_float(
            snapshot_payload.get("liquidation_burst_score")
        ),
        "derivatives_heat_score": _coerce_finite_float(
            snapshot_payload.get("derivatives_heat_score")
        ),
        "crowding_score": _coerce_finite_float(snapshot.get("crowding_score")),
        "squeeze_score": _coerce_finite_float(snapshot.get("squeeze_score")),
        "distribution_score": _coerce_finite_float(snapshot.get("distribution_score")),
        "basis_pct": _coerce_finite_float(snapshot.get("basis_pct")),
        "taker_buy_sell_imbalance": _coerce_finite_float(
            snapshot.get("taker_buy_sell_imbalance")
        ),
        "crowded_long": bool(snapshot_payload.get("crowded_long")),
        "crowded_short": bool(snapshot_payload.get("crowded_short")),
        "squeeze_building": bool(snapshot_payload.get("squeeze_building")),
        "flush_risk": bool(snapshot_payload.get("flush_risk")),
        "basis_dislocation": bool(snapshot_payload.get("basis_dislocation")),
        "flow_divergence": bool(snapshot_payload.get("flow_divergence")),
        "order_flow_confirmed": bool(snapshot_payload.get("order_flow_confirmed")),
        "derivatives_labels": derivatives_labels,
    }


_DERIVATIVES_SIGNAL_FIELDS = (
    "funding_rate",
    "funding_mean_rate_pct",
    "funding_zscore",
    "long_short_ratio",
    "basis_pct",
    "taker_buy_sell_imbalance",
    "crowding_score",
    "squeeze_score",
    "distribution_score",
)


def _derivatives_summary_metric_count(summary: Optional[Dict[str, Any]]) -> int:
    data = dict(summary or {})
    return sum(
        1
        for field in _DERIVATIVES_SIGNAL_FIELDS
        if _coerce_finite_float(data.get(field)) is not None
    )


def _pick_preferred_derivatives_summary(*summaries: Any) -> Dict[str, Any]:
    best: Dict[str, Any] = {}
    best_score: Optional[tuple[Any, ...]] = None
    for index, item in enumerate(summaries):
        summary = dict(item or {})
        if not summary:
            continue
        freshness_sec = _coerce_finite_float(summary.get("freshness_sec"))
        score = (
            1 if bool(summary.get("available")) else 0,
            1 if str(summary.get("status") or "").strip().lower() == "ok" else 0,
            1 if bool(summary.get("history_ready")) else 0,
            int(summary.get("dataset_count") or 0),
            _derivatives_summary_metric_count(summary),
            -(freshness_sec if freshness_sec is not None else 1.0e12),
            -index,
        )
        if best_score is None or score > best_score:
            best = summary
            best_score = score
    return best


def _build_derivatives_source_summary(
    summary: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    payload = dict(summary or {})
    source = str(payload.get("provider") or "coinglass").strip() or "coinglass"
    status = str(payload.get("status") or "missing").strip().lower() or "missing"
    freshness_sec = _coerce_finite_float(payload.get("freshness_sec"))
    stale = freshness_sec is not None and freshness_sec > float(
        _COINGLASS_PREFERRED_MAX_AGE_SEC
    )
    degraded_reason = str(payload.get("degraded_reason") or "").strip() or None
    available = bool(payload.get("available"))
    dataset_count = int(payload.get("dataset_count") or 0)
    history_ready = bool(payload.get("history_ready"))

    source_status = "missing"
    note = None
    if available and status == "ok" and not stale and not degraded_reason:
        source_status = "live"
    elif available and stale:
        source_status = "cache_stale"
        note = "CoinGlass derivatives snapshot exceeds the preferred freshness window."
    elif (
        available
        or degraded_reason
        or bool(payload.get("key_configured"))
        or dataset_count > 0
    ):
        source_status = "degraded"
        if degraded_reason:
            note = f"CoinGlass derivatives degraded: {degraded_reason}"
        elif stale:
            note = (
                "CoinGlass derivatives snapshot exceeds the preferred freshness window."
            )
        elif not history_ready:
            note = "CoinGlass derivatives history context is incomplete."

    return {
        "source": source,
        "available": available,
        "source_status": source_status,
        "status": status,
        "stale": stale,
        "cache_age_sec": freshness_sec,
        "dataset_count": dataset_count,
        "history_ready": history_ready,
        "snapshot_at": payload.get("snapshot_at"),
        "quota_headroom": dict(payload.get("quota_headroom") or {}),
        "degraded_reason": degraded_reason,
        "note": note,
    }


def _news_summary_sample_count(summary: Optional[Dict[str, Any]]) -> int:
    data = dict(summary or {})
    return (
        int(data.get("events_count") or 0)
        + int(data.get("feed_count") or 0)
        + int(data.get("raw_count") or 0)
    )


def _news_summary_has_usable_samples(summary: Optional[Dict[str, Any]]) -> bool:
    return _news_summary_sample_count(summary) > 0


def _pick_preferred_news_summary(*summaries: Any) -> Dict[str, Any]:
    best: Dict[str, Any] = {}
    best_score: Optional[tuple[Any, ...]] = None
    for index, item in enumerate(summaries):
        summary = dict(item or {})
        if not summary:
            continue
        score = (
            1 if int(summary.get("events_count") or 0) > 0 else 0,
            1 if str(summary.get("scope") or "").strip().lower() == "symbol" else 0,
            _news_summary_sample_count(summary),
            -index,
        )
        if best_score is None or score > best_score:
            best = summary
            best_score = score
    return best


def _apply_derivatives_shadow_to_microstructure(
    microstructure: Dict[str, Any],
    derivatives_summary: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(microstructure or {})
    derivatives_summary = dict(derivatives_summary or {})
    snapshot_at = derivatives_summary.get("snapshot_at")

    long_short_ratio = _coerce_finite_float(derivatives_summary.get("long_short_ratio"))
    if long_short_ratio is not None and long_short_ratio > 0:
        merged["long_short_ratio"] = {
            "available": True,
            "source": "coinglass_cache",
            "symbol": merged.get("symbol"),
            "long_short_ratio": round(long_short_ratio, 6),
            "timestamp": snapshot_at,
        }

    funding_rate = _coerce_finite_float(derivatives_summary.get("funding_rate"))
    if funding_rate is not None:
        existing_funding = dict(merged.get("funding_rate") or {})
        merged["funding_rate"] = {
            **existing_funding,
            "available": True,
            "source": "coinglass_cache",
            "funding_rate": funding_rate,
            "timestamp": snapshot_at,
        }

    basis_pct = _coerce_finite_float(derivatives_summary.get("basis_pct"))
    if basis_pct is not None:
        existing_basis = dict(merged.get("spot_futures_basis") or {})
        merged["spot_futures_basis"] = {
            **existing_basis,
            "available": True,
            "source": "coinglass_cache",
            "basis_pct": basis_pct,
            "timestamp": snapshot_at,
        }

    taker_imbalance = _coerce_finite_float(
        derivatives_summary.get("taker_buy_sell_imbalance")
    )
    current_flow = dict(merged.get("aggressor_flow") or {})
    current_flow_count = int(current_flow.get("count") or 0)
    current_flow_imbalance = _coerce_finite_float(current_flow.get("imbalance")) or 0.0
    has_live_flow = bool(current_flow.get("available")) and (
        current_flow_count > 0 or abs(current_flow_imbalance) > 1e-9
    )
    if taker_imbalance is not None and not has_live_flow:
        merged["aggressor_flow"] = {
            **current_flow,
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "count": current_flow_count,
            "buy_volume": current_flow.get("buy_volume"),
            "sell_volume": current_flow.get("sell_volume"),
            "imbalance": taker_imbalance,
            "timestamp": snapshot_at,
        }

    merged["derivatives_context"] = {
        "provider": str(derivatives_summary.get("provider") or "coinglass"),
        "available": bool(derivatives_summary.get("available")),
        "status": str(derivatives_summary.get("status") or "missing"),
        "key_configured": bool(derivatives_summary.get("key_configured")),
        "freshness_sec": derivatives_summary.get("freshness_sec"),
        "degraded_reason": derivatives_summary.get("degraded_reason"),
        "dataset_count": int(derivatives_summary.get("dataset_count") or 0),
        "active_datasets": list(derivatives_summary.get("active_datasets") or []),
        "snapshot_at": snapshot_at,
    }
    return merged


def _build_market_regime(
    analytics: Dict[str, Any],
    microstructure: Dict[str, Any],
    news: Dict[str, Any],
    derivatives_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    risk_module = dict(
        (analytics.get("modules") or {}).get("risk_dashboard", {}).get("data") or {}
    )
    micro_module = dict(
        (analytics.get("modules") or {}).get("microstructure", {}).get("data") or {}
    )
    merged_micro = dict(
        _merge_nested_payload(microstructure or {}, micro_module or {}) or {}
    )
    micro_summary = _build_microstructure_summary(merged_micro)
    derivatives_summary = dict(derivatives_summary or {})

    risk_level = str(risk_module.get("risk_level") or "unknown")
    spread_bps = float((merged_micro.get("orderbook") or {}).get("spread_bps") or 0.0)
    imbalance = float(
        (merged_micro.get("aggressor_flow") or {}).get("imbalance") or 0.0
    )
    long_short_ratio = float(micro_summary.get("long_short_ratio") or 1.0)
    wall_bias = float(micro_summary.get("wall_bias") or 0.0)

    sentiment = dict(news.get("sentiment") or {})
    total_news = sum(
        int(sentiment.get(key) or 0) for key in ("positive", "neutral", "negative")
    )
    news_bias = (
        ((sentiment.get("positive", 0) - sentiment.get("negative", 0)) / total_news)
        if total_news
        else 0.0
    )

    micro_signal = (
        imbalance * 0.55
        + max(-0.6, min(0.6, long_short_ratio - 1.0)) * 0.30
        + wall_bias * 0.25
    )
    derivatives_ready = bool(derivatives_summary.get("available"))
    derivatives_history_ready = bool(derivatives_summary.get("history_ready"))
    derivatives_freshness_sec = _coerce_finite_float(
        derivatives_summary.get("freshness_sec")
    )
    derivatives_confidence_bonus = 0.0
    if derivatives_ready:
        derivatives_confidence_bonus += 0.08
    if derivatives_history_ready:
        derivatives_confidence_bonus += 0.04
    if derivatives_freshness_sec is not None and derivatives_freshness_sec > float(
        _COINGLASS_PREFERRED_MAX_AGE_SEC
    ):
        derivatives_confidence_bonus -= 0.05
    confidence = min(
        0.95,
        max(
            0.2,
            0.3
            + min(0.25, total_news / 220.0)
            + (0.15 if micro_summary.get("has_actionable_signal") else 0.0)
            + (0.08 if micro_summary.get("long_short_ratio_available") else 0.0)
            + derivatives_confidence_bonus,
        ),
    )

    from core.market_state.classifier import classify_market_regime

    classified = classify_market_regime(
        risk_level=risk_level,
        spread_bps=spread_bps,
        imbalance=imbalance,
        long_short_ratio=long_short_ratio,
        wall_bias=wall_bias,
        news_bias=news_bias,
        total_news=total_news,
        derivatives_ready=derivatives_ready,
        derivatives_history_ready=derivatives_history_ready,
        derivatives_freshness_sec=derivatives_freshness_sec,
    )
    classified["confidence"] = round(confidence, 4)
    classified["wall_bias"] = round(wall_bias, 6)
    return classified


def _has_positive_number(value: Any) -> bool:
    try:
        return float(value) > 0
    except Exception:
        return False


def _snapshot_age_sec(payload: Dict[str, Any]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    ts = _coerce_utc_datetime(payload.get("timestamp"))
    if ts is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _snapshot_is_recent(payload: Dict[str, Any], max_age_sec: float) -> bool:
    age_sec = _snapshot_age_sec(payload)
    return age_sec is not None and age_sec <= float(max_age_sec)


def _analytics_module_entry(
    task_name: str, data: Dict[str, Any], *, ok: bool, error: Optional[str] = None
) -> Dict[str, Any]:
    payload = dict(data or {})
    if error:
        payload.setdefault("error", error)
    return {
        "task": task_name,
        "ok": bool(ok),
        "latency_ms": 0.0,
        "data": payload,
    }


def _microstructure_has_signal(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    summary = _build_microstructure_summary(payload)
    return bool(summary.get("has_actionable_signal"))


def _community_has_signal(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(
        [
            bool(payload.get("announcements")),
            bool((payload.get("whale_transfers") or {}).get("count")),
            bool((payload.get("flow_proxy") or {}).get("count")),
            bool((payload.get("security_alerts") or {}).get("events")),
        ]
    )


def _map_calendar_rows(rows: Any) -> List[Dict[str, Any]]:
    def _importance_rank(value: Any) -> int:
        text = str(value or "").strip().lower()
        if text == "critical":
            return 4
        if text == "high":
            return 3
        if text == "medium":
            return 2
        if text == "low":
            return 1
        return 0

    def _source_label(source: Any) -> str:
        mapping = {
            "coinglass_economic_data": "CoinGlass宏观",
            "coinglass_central_bank": "CoinGlass央行",
            "coinglass_unlock_list": "CoinGlass解锁",
            "internal_estimate": "内部估算",
        }
        key = str(source or "").strip().lower()
        return mapping.get(key, str(source or "").strip() or "未标记")

    def _source_rank(source: Any) -> int:
        text = str(source or "").strip().lower()
        if text.startswith("coinglass_"):
            return 0
        if text == "internal_estimate":
            return 2
        return 1

    def _category_rank(category: Any) -> int:
        text = str(category or "").strip().lower()
        return {
            "economic": 0,
            "central_bank": 1,
            "unlock": 2,
            "expiry": 3,
        }.get(text, 4)

    normalized: List[Dict[str, Any]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        source = row.get("source") or row.get("provider")
        normalized.append(
            {
                "title": row.get("title")
                or row.get("name")
                or row.get("event")
                or "事件",
                "timestamp": row.get("timestamp")
                or row.get("time_utc")
                or row.get("start_time")
                or row.get("time"),
                "importance": row.get("importance") or "medium",
                "category": row.get("category") or "event",
                "note": row.get("note"),
                "source": source,
                "source_label": _source_label(source),
                "official_source": str(source or "")
                .strip()
                .lower()
                .startswith("coinglass_"),
                "estimated_source": str(source or "").strip().lower()
                == "internal_estimate",
            }
        )

    normalized.sort(
        key=lambda row: (
            -_importance_rank(row.get("importance")),
            _source_rank(row.get("source")),
            _category_rank(row.get("category")),
            str(row.get("timestamp") or ""),
        )
    )

    category_caps = {"economic": 4, "central_bank": 2, "unlock": 3, "expiry": 1}
    selected: List[Dict[str, Any]] = []
    selected_keys = set()
    category_counts: Dict[str, int] = {}

    for row in normalized:
        category = str(row.get("category") or "event").strip().lower()
        cap = category_caps.get(category, 2)
        if int(category_counts.get(category) or 0) >= cap:
            continue
        key = (
            category,
            str(row.get("timestamp") or ""),
            str(row.get("title") or "").strip().lower(),
        )
        if key in selected_keys:
            continue
        selected_keys.add(key)
        category_counts[category] = int(category_counts.get(category) or 0) + 1
        selected.append(row)
        if len(selected) >= 8:
            break

    if len(selected) < 8:
        for row in normalized:
            key = (
                str(row.get("category") or "event").strip().lower(),
                str(row.get("timestamp") or ""),
                str(row.get("title") or "").strip().lower(),
            )
            if key in selected_keys:
                continue
            selected_keys.add(key)
            selected.append(row)
            if len(selected) >= 8:
                break

    selected.sort(
        key=lambda row: (
            str(row.get("timestamp") or ""),
            -_importance_rank(row.get("importance")),
        )
    )
    return selected


def _build_calendar_source_summary(
    calendar_data: Dict[str, Any], calendar_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    payload = dict(calendar_data or {})
    source_details = dict(payload.get("source_details") or {})
    explicit_source_metadata = bool(payload.get("source") or source_details)
    if not explicit_source_metadata:
        explicit_source_metadata = any(
            bool((item or {}).get("source")) for item in list(calendar_rows or [])
        )
    official_mapping = {
        "economic": "coinglass_economic_data",
        "central_bank": "coinglass_central_bank",
        "unlocks": "coinglass_unlock_list",
    }
    official_sources = [
        label
        for key, label in official_mapping.items()
        if bool((source_details.get(key) or {}).get("available"))
    ]
    if not official_sources:
        official_sources = sorted(
            {
                str(item.get("source") or "").strip()
                for item in list(calendar_rows or [])
                if str(item.get("source") or "").strip().startswith("coinglass_")
            }
        )
    internal_details = dict(source_details.get("internal_estimate") or {})
    estimated_count = int(internal_details.get("count") or 0)
    if estimated_count <= 0:
        estimated_count = len(
            [
                item
                for item in list(calendar_rows or [])
                if bool(item.get("estimated_source"))
            ]
        )
    official_count = 0
    for key in official_mapping:
        official_count += int((source_details.get(key) or {}).get("count") or 0)
    if official_count <= 0:
        official_count = len(
            [
                item
                for item in list(calendar_rows or [])
                if bool(item.get("official_source"))
            ]
        )
    return {
        "source": str(payload.get("source") or "").strip() or None,
        "explicit_source_metadata": explicit_source_metadata,
        "official_sources": official_sources,
        "official_available": bool(official_sources),
        "official_count": official_count,
        "estimated_count": estimated_count,
        "fallback_used": bool(internal_details.get("used")) or estimated_count > 0,
        "note": str(payload.get("note") or "").strip() or None,
        "stale": bool(payload.get("stale")),
        "cache_age_sec": payload.get("cache_age_sec"),
        "source_status": str(payload.get("source_status") or "").strip() or None,
        "stale_reason": str(payload.get("stale_reason") or "").strip() or None,
    }


def _coerce_finite_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:
        return None
    return parsed


def _macro_has_signal(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(_coerce_finite_float(value) is not None for value in payload.values())


def _build_macro_region_summary(
    snapshot: Dict[str, Any],
    *,
    name: str,
    scalar_fields: List[tuple[str, str]],
    ppi_key: Optional[str] = None,
    cpi_key: Optional[str] = None,
    scissors_key: Optional[str] = None,
    m1_key: Optional[str] = None,
    m2_key: Optional[str] = None,
    liquidity_key: Optional[str] = None,
    fed_key: Optional[str] = None,
) -> Dict[str, Any]:
    values = {key: _coerce_finite_float(snapshot.get(key)) for key, _ in scalar_fields}
    parts: List[str] = []
    active_series = [key for key, value in values.items() if value is not None]

    if fed_key:
        fed_rate = _coerce_finite_float(snapshot.get(fed_key))
        if fed_rate is not None:
            parts.append(f"FF {fed_rate:.2f}%")
            if fed_key not in active_series:
                active_series.append(fed_key)
    else:
        fed_rate = None

    cpi_yoy = _coerce_finite_float(snapshot.get(cpi_key)) if cpi_key else None
    ppi_yoy = _coerce_finite_float(snapshot.get(ppi_key)) if ppi_key else None
    m1_yoy = _coerce_finite_float(snapshot.get(m1_key)) if m1_key else None
    m2_yoy = _coerce_finite_float(snapshot.get(m2_key)) if m2_key else None
    scissors_spread = (
        _coerce_finite_float(snapshot.get(scissors_key)) if scissors_key else None
    )
    liquidity_spread = (
        _coerce_finite_float(snapshot.get(liquidity_key)) if liquidity_key else None
    )

    for key, label in scalar_fields:
        value = values.get(key)
        if value is None:
            continue
        if key in {cpi_key, ppi_key, m1_key, m2_key}:
            parts.append(f"{label} {value:.2f}%")
        elif key in {scissors_key, liquidity_key}:
            parts.append(f"{label} {value:+.2f}pp")

    headline = f"{name}: " + " | ".join(parts[:4]) if parts else f"{name}: unavailable"
    return {
        "name": name,
        "available_series": active_series,
        "available_count": len(active_series),
        "headline": headline,
        "fed_rate": round(fed_rate, 4) if fed_rate is not None else None,
        "cpi_yoy": round(cpi_yoy, 4) if cpi_yoy is not None else None,
        "ppi_yoy": round(ppi_yoy, 4) if ppi_yoy is not None else None,
        "m1_yoy": round(m1_yoy, 4) if m1_yoy is not None else None,
        "m2_yoy": round(m2_yoy, 4) if m2_yoy is not None else None,
        "scissors_spread_pp": round(scissors_spread, 4)
        if scissors_spread is not None
        else None,
        "liquidity_scissors_spread_pp": round(liquidity_spread, 4)
        if liquidity_spread is not None
        else None,
    }


def _build_macro_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = dict(payload or {})
    active_series = [
        name
        for name, value in snapshot.items()
        if _coerce_finite_float(value) is not None
    ]
    market_parts: List[str] = []
    vix = _coerce_finite_float(snapshot.get("vix"))
    dxy = _coerce_finite_float(snapshot.get("dxy"))
    tnx = _coerce_finite_float(snapshot.get("tnx_10y"))
    if vix is not None:
        market_parts.append(f"VIX {vix:.1f}")
    if dxy is not None:
        market_parts.append(f"DXY {dxy:.1f}")
    if tnx is not None:
        market_parts.append(f"UST10Y {tnx:.2f}%")

    market_headline = (
        "Cross-market: " + " | ".join(market_parts)
        if market_parts
        else "Cross-market: unavailable"
    )
    us_summary = _build_macro_region_summary(
        snapshot,
        name="US",
        scalar_fields=[
            ("cpi_yoy", "CPI"),
            ("ppi_yoy", "PPI"),
            ("ppi_cpi_gap", "PPI-CPI"),
            ("m1_yoy", "M1"),
            ("m2_yoy", "M2"),
            ("m1_m2_gap", "M1-M2"),
        ],
        ppi_key="ppi_yoy",
        cpi_key="cpi_yoy",
        scissors_key="ppi_cpi_gap",
        m1_key="m1_yoy",
        m2_key="m2_yoy",
        liquidity_key="m1_m2_gap",
        fed_key="fed_rate",
    )
    china_summary = _build_macro_region_summary(
        snapshot,
        name="China",
        scalar_fields=[
            ("cn_cpi_yoy", "CPI"),
            ("cn_ppi_yoy", "PPI"),
            ("cn_ppi_cpi_gap", "PPI-CPI"),
            ("cn_m1_yoy", "M1"),
            ("cn_m2_yoy", "M2"),
            ("cn_m1_m2_gap", "M1-M2"),
        ],
        ppi_key="cn_ppi_yoy",
        cpi_key="cn_cpi_yoy",
        scissors_key="cn_ppi_cpi_gap",
        m1_key="cn_m1_yoy",
        m2_key="cn_m2_yoy",
        liquidity_key="cn_m1_m2_gap",
    )

    headline_parts = []
    if market_parts:
        headline_parts.append(market_headline)
    if us_summary["available_count"]:
        headline_parts.append(us_summary["headline"])
    if china_summary["available_count"]:
        headline_parts.append(china_summary["headline"])

    return {
        "available_series": active_series,
        "available_count": len(active_series),
        "headline": " || ".join(headline_parts)
        if headline_parts
        else "宏观快照不可用",
        "cross_market_headline": market_headline,
        "us_headline": us_summary["headline"],
        "china_headline": china_summary["headline"],
        "scissors_spread_pp": us_summary["scissors_spread_pp"],
        "liquidity_scissors_spread_pp": us_summary["liquidity_scissors_spread_pp"],
        "china_scissors_spread_pp": china_summary["scissors_spread_pp"],
        "china_liquidity_scissors_spread_pp": china_summary[
            "liquidity_scissors_spread_pp"
        ],
        "fed_rate": us_summary["fed_rate"],
        "cpi_yoy": us_summary["cpi_yoy"],
        "ppi_yoy": us_summary["ppi_yoy"],
        "m1_yoy": us_summary["m1_yoy"],
        "m2_yoy": us_summary["m2_yoy"],
        "cn_cpi_yoy": china_summary["cpi_yoy"],
        "cn_ppi_yoy": china_summary["ppi_yoy"],
        "cn_m1_yoy": china_summary["m1_yoy"],
        "cn_m2_yoy": china_summary["m2_yoy"],
        "regions": {
            "market": {
                "headline": market_headline,
                "available_series": [
                    key
                    for key in ("vix", "dxy", "tnx_10y")
                    if _coerce_finite_float(snapshot.get(key)) is not None
                ],
                "available_count": len(
                    [
                        key
                        for key in ("vix", "dxy", "tnx_10y")
                        if _coerce_finite_float(snapshot.get(key)) is not None
                    ]
                ),
                "vix": round(vix, 4) if vix is not None else None,
                "dxy": round(dxy, 4) if dxy is not None else None,
                "tnx_10y": round(tnx, 4) if tnx is not None else None,
            },
            "us": us_summary,
            "china": china_summary,
        },
    }


def _build_macro_source_summary(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    snapshot = dict(payload or {})
    meta = dict(snapshot.get("_meta") or {})
    groups_meta = dict(meta.get("groups") or {})
    group_specs = {
        "market": {
            "keys": ("vix", "dxy", "tnx_10y"),
            "provider": "yfinance+yahoo_chart",
            "max_age_sec": _MACRO_MARKET_STALE_MAX_AGE_SEC,
        },
        "us": {
            "keys": (
                "fed_rate",
                "cpi_yoy",
                "ppi_yoy",
                "ppi_cpi_gap",
                "m1_yoy",
                "m2_yoy",
                "m1_m2_gap",
            ),
            "provider": "fred",
            "max_age_sec": _MACRO_MONTHLY_STALE_MAX_AGE_SEC,
        },
        "china": {
            "keys": (
                "cn_cpi_yoy",
                "cn_ppi_yoy",
                "cn_ppi_cpi_gap",
                "cn_m1_yoy",
                "cn_m2_yoy",
                "cn_m1_m2_gap",
            ),
            "provider": "stats.gov.cn+pbc.gov.cn",
            "max_age_sec": _MACRO_MONTHLY_STALE_MAX_AGE_SEC,
        },
    }

    groups: Dict[str, Any] = {}
    stale_groups: List[str] = []
    partial_groups: List[str] = []
    missing_groups: List[str] = []

    for group_name, spec in group_specs.items():
        group_payload = dict(groups_meta.get(group_name) or {})
        keys = tuple(spec["keys"])
        available_count = int(group_payload.get("available_count") or 0)
        if available_count <= 0:
            available_count = sum(
                1 for key in keys if _coerce_finite_float(snapshot.get(key)) is not None
            )
        total_series = int(group_payload.get("total_series") or 0) or len(keys)
        latest_timestamp = (
            str(group_payload.get("latest_timestamp") or "").strip() or None
        )
        latest_dt = _coerce_utc_datetime(latest_timestamp)
        age_sec = None
        if latest_dt is not None:
            age_sec = max(0.0, (datetime.now(timezone.utc) - latest_dt).total_seconds())
        stale = bool(
            available_count > 0
            and age_sec is not None
            and age_sec > float(spec["max_age_sec"])
        )
        if available_count <= 0:
            source_status = "missing"
            missing_groups.append(group_name)
        elif stale:
            source_status = "cache_stale"
            stale_groups.append(group_name)
        elif available_count < total_series:
            source_status = "partial"
            partial_groups.append(group_name)
        else:
            source_status = "cache_fresh"
        groups[group_name] = {
            "provider": str(group_payload.get("provider") or spec["provider"]),
            "available_count": available_count,
            "total_series": total_series,
            "available_series": list(
                group_payload.get("available_series")
                or [
                    key
                    for key in keys
                    if _coerce_finite_float(snapshot.get(key)) is not None
                ]
            ),
            "latest_timestamp": latest_timestamp,
            "cache_age_sec": age_sec,
            "stale": stale,
            "source_status": source_status,
        }

    explicit_source = str(meta.get("source") or "").strip()
    if explicit_source:
        source = explicit_source
    else:
        providers = [
            str(item.get("provider") or "").strip()
            for item in groups.values()
            if int(item.get("available_count") or 0) > 0
        ]
        source = "+".join([part for part in providers if part]) or None

    latest_timestamp = str(meta.get("latest_timestamp") or "").strip() or None
    latest_dt = _coerce_utc_datetime(latest_timestamp)
    cache_age_sec = (
        max(0.0, (datetime.now(timezone.utc) - latest_dt).total_seconds())
        if latest_dt is not None
        else None
    )
    available_series = sum(
        int(item.get("available_count") or 0) for item in groups.values()
    )

    source_status = "missing"
    note = None
    if available_series > 0:
        if stale_groups:
            source_status = (
                "cache_stale"
                if len(stale_groups)
                == len(
                    [
                        name
                        for name, item in groups.items()
                        if int(item.get("available_count") or 0) > 0
                    ]
                )
                else "partial"
            )
            note = "宏观缓存包含偏旧分组，请检查每日刷新任务和上游发布窗口。"
        elif partial_groups or missing_groups:
            source_status = "partial"
            note = (
                "宏观快照在跨市场/美国/中国分组上仅部分填充。"
            )
        else:
            source_status = "cache_fresh"

    return {
        "source": source,
        "source_status": source_status,
        "available": available_series > 0,
        "available_series": available_series,
        "groups": groups,
        "stale": bool(stale_groups),
        "stale_groups": stale_groups,
        "partial_groups": partial_groups,
        "missing_groups": missing_groups,
        "cache_age_sec": cache_age_sec,
        "latest_timestamp": latest_timestamp,
        "note": note,
    }


async def _load_macro_snapshot_payload() -> Dict[str, Any]:
    def _sync() -> Dict[str, Any]:
        try:
            from core.data.macro_collector import (  # noqa: PLC0415
                load_macro_snapshot,
                load_macro_snapshot_metadata,
            )

            snapshot = dict(load_macro_snapshot() or {})
            metadata = dict(load_macro_snapshot_metadata(snapshot) or {})
            if metadata:
                snapshot["_meta"] = metadata
                if metadata.get("latest_timestamp"):
                    snapshot["timestamp"] = metadata.get("latest_timestamp")
                if metadata.get("source"):
                    snapshot["source"] = metadata.get("source")
            return snapshot
        except Exception:
            return {}

    return await asyncio.to_thread(_sync)


async def _load_latest_snapshot(
    model: Any, exchange: str, symbol: str
) -> Optional[Any]:
    async with async_session_maker() as session:
        preferred_stmt = (
            select(model)
            .where(
                model.exchange == exchange,
                model.symbol == symbol,
                model.capture_status.in_(["ok", "degraded"]),
            )
            .order_by(model.timestamp.desc())
            .limit(1)
        )
        row = (await session.execute(preferred_stmt)).scalars().first()
        if row is not None:
            return row
        fallback_stmt = (
            select(model)
            .where(model.exchange == exchange, model.symbol == symbol)
            .order_by(model.timestamp.desc())
            .limit(1)
        )
        return (await session.execute(fallback_stmt)).scalars().first()


async def _load_latest_microstructure_snapshot(
    exchange: str, symbol: str
) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsMicrostructureSnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "available": row.capture_status != "failed" and row.mid_price > 0,
                "source_error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "orderbook": {
                    "mid_price": row.mid_price,
                    "spread_bps": row.spread_bps,
                },
                "aggressor_flow": {
                    "imbalance": row.order_flow_imbalance,
                },
                "funding_rate": {
                    "available": row.funding_rate is not None,
                    "funding_rate": row.funding_rate,
                },
                "spot_futures_basis": {
                    "available": row.basis_pct is not None,
                    "basis_pct": row.basis_pct,
                },
            },
        )
        or {}
    )


async def _load_latest_community_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsCommunitySnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "source_error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "flow_proxy": {
                    "imbalance": row.flow_imbalance,
                    "buy_ratio": row.buy_ratio,
                    "sell_ratio": row.sell_ratio,
                },
                "announcements": list(payload.get("announcements") or [])[:10],
                "security_alerts": payload.get("security_alerts") or {},
                "twitter_watchlist": list(payload.get("twitter_watchlist") or [])[:10],
            },
        )
        or {}
    )


async def _load_latest_whale_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsWhaleSnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "available": row.capture_status != "failed",
                "error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "count": int(row.whale_count or 0),
                "threshold_btc": payload.get("threshold_btc"),
                "btc_price": payload.get("btc_price"),
                "transactions": list(payload.get("transactions") or [])[:10],
            },
        )
        or {}
    )


def _compact_factor_library(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    if (
        not data.get("factors")
        and not data.get("latest")
        and not data.get("asset_scores")
        and not data.get("points")
    ):
        return {}
    return {
        "exchange": data.get("exchange"),
        "timeframe": data.get("timeframe"),
        "lookback_effective": data.get("lookback_effective"),
        "symbols_used": list(data.get("symbols_used") or [])[:12],
        "retired_filter": data.get("retired_filter") or {},
        "points": int(data.get("points") or 0),
        "factors": list(data.get("factors") or []),
        "universe_size": int(data.get("universe_size") or 0),
        "universe_quality": data.get("universe_quality") or "unknown",
        "warnings": list(data.get("warnings") or []),
        "diagnostics": dict(data.get("diagnostics") or {}),
        "window_config": dict(data.get("window_config") or {}),
        "latest": dict(data.get("latest") or {}),
        "mean_24": dict(data.get("mean_24") or {}),
        "std_24": dict(data.get("std_24") or {}),
        "correlation": dict(data.get("correlation") or {}),
        "series": [
            row for row in list(data.get("series") or [])[:120] if isinstance(row, dict)
        ],
        "asset_scores": [
            row
            for row in list(data.get("asset_scores") or [])[:12]
            if isinstance(row, dict)
        ],
    }


def _compact_fama(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    series = [row for row in list(data.get("series") or []) if isinstance(row, dict)]
    points = int(data.get("points") or 0)
    # The async Fama endpoint returns a placeholder with a zero-filled
    # ``latest`` mapping while its background task is warming up.  Treating
    # that mapping as real data makes the workbench render every factor as 0
    # and, because the pending metadata used to be discarded here, prevents
    # the client from recognizing that the result is only a placeholder.
    if points <= 0 and not series:
        return {}
    return {
        "exchange": data.get("exchange"),
        "timeframe": data.get("timeframe"),
        "symbols_used": list(data.get("symbols_used") or [])[:12],
        "points": points,
        "universe_size": int(data.get("universe_size") or 0),
        "universe_quality": data.get("universe_quality") or "unknown",
        "latest": dict(data.get("latest") or {}),
        "mean_24": dict(data.get("mean_24") or {}),
        "std_24": dict(data.get("std_24") or {}),
        "series": series[:120],
        "warnings": list(data.get("warnings") or []),
        "served_mode": data.get("served_mode") or "unknown",
    }


_FAMA_STYLE_FACTOR_IDS = ("MKT", "SMB", "HML", "MOM", "RMW", "CMA", "VOL")


def _fama_from_factor_library(data: Dict[str, Any]) -> Dict[str, Any]:
    """Build the workbench Fama view from the canonical factor-library run."""
    if not isinstance(data, dict) or int(data.get("points") or 0) <= 0:
        return {}

    latest_raw = dict(data.get("latest") or {})
    latest = {
        factor_id: latest_raw[factor_id]
        for factor_id in _FAMA_STYLE_FACTOR_IDS
        if factor_id in latest_raw
    }
    series: List[Dict[str, Any]] = []
    for row in list(data.get("series") or []):
        if not isinstance(row, dict):
            continue
        compact_row = {"timestamp": row.get("timestamp")}
        compact_row.update(
            {
                factor_id: row[factor_id]
                for factor_id in _FAMA_STYLE_FACTOR_IDS
                if factor_id in row
            }
        )
        series.append(compact_row)
    if not latest and not series:
        return {}

    def _style_snapshot(name: str) -> Dict[str, Any]:
        raw = dict(data.get(name) or {})
        return {
            factor_id: raw[factor_id]
            for factor_id in _FAMA_STYLE_FACTOR_IDS
            if factor_id in raw
        }

    return {
        "exchange": data.get("exchange"),
        "timeframe": data.get("timeframe"),
        "symbols_used": list(data.get("symbols_used") or [])[:12],
        "points": int(data.get("points") or 0),
        "universe_size": int(data.get("universe_size") or 0),
        "universe_quality": data.get("universe_quality") or "unknown",
        "latest": latest,
        "mean_24": _style_snapshot("mean_24"),
        "std_24": _style_snapshot("std_24"),
        "series": series[:120],
        "warnings": ["Fama 专用快照仍在后台计算，当前复用因子库中的同口径风格因子。"],
        "served_mode": "factor_library_fallback",
    }


def _fallback_factor_library_from_fama(
    profile: ResearchProfile,
    fama: Dict[str, Any],
    cross_asset: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(fama, dict) or not fama:
        return {}

    series_rows = [
        row for row in list(fama.get("series") or []) if isinstance(row, dict)
    ]
    factor_names = [
        str(name) for name in list((fama.get("latest") or {}).keys()) if str(name)
    ]
    correlation: Dict[str, Dict[str, float]] = {}

    if series_rows and factor_names:
        frame = pd.DataFrame(series_rows)
        if "timestamp" in frame.columns:
            frame = frame.drop(columns=["timestamp"])
        usable_cols = [col for col in factor_names if col in frame.columns]
        if usable_cols:
            corr_df = (
                frame[usable_cols]
                .apply(pd.to_numeric, errors="coerce")
                .corr()
                .round(4)
                .fillna(0.0)
            )
            correlation = corr_df.to_dict()

    asset_scores: List[Dict[str, Any]] = []
    for row in list((cross_asset or {}).get("assets") or [])[:12]:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        ret_pct = float(row.get("return_pct") or 0.0)
        vol_pct = abs(float(row.get("volatility_pct") or 0.0))
        score = round(ret_pct / 100.0, 6)
        low_vol = round(max(0.0, 1.0 - min(vol_pct / 100.0, 1.0)), 6)
        asset_scores.append(
            {
                "symbol": symbol,
                "score": score,
                "momentum": score,
                "value": 0.0,
                "quality": low_vol,
                "low_vol": low_vol,
                "liquidity": 0.0,
                "low_beta": 0.0,
                "size": 0.0,
            }
        )

    return {
        "exchange": profile.exchange,
        "timeframe": profile.timeframe,
        "lookback_effective": min(360, profile.lookback),
        "symbols_used": list(fama.get("symbols_used") or [])[:12],
        "retired_filter": dict((cross_asset or {}).get("retired_filter") or {}),
        "points": int(fama.get("points") or 0),
        "factors": factor_names,
        "universe_size": int(fama.get("universe_size") or len(asset_scores)),
        "universe_quality": fama.get("universe_quality") or "low",
        "warnings": [
            "因子库降级到 Fama 快照兜底，以便工坊快速响应。"
        ],
        "latest": dict(fama.get("latest") or {}),
        "mean_24": dict(fama.get("mean_24") or {}),
        "std_24": dict(fama.get("std_24") or {}),
        "correlation": correlation,
        "series": series_rows[:120],
        "asset_scores": asset_scores,
    }


async def _build_market_state_module(profile: ResearchProfile) -> Dict[str, Any]:
    risk_task = _wait_or_none_keep_running(
        get_risk_dashboard(lookback=min(720, profile.lookback)),
        4.0,
    )
    news_task = _wait_or_none_keep_running(
        _build_news_summary(profile.primary_symbol, hours=24),
        _MARKET_STATE_NEWS_TIMEOUT_SEC,
    )
    calendar_task = _wait_or_none_keep_running(get_trading_calendar(days=7), 4.0)
    macro_task = _wait_or_none_keep_running(_load_macro_snapshot_payload(), 2.0)
    history_micro_task = _wait_or_none_keep_running(
        _load_latest_microstructure_snapshot(profile.exchange, profile.primary_symbol),
        4.0,
    )
    history_community_task = _wait_or_none_keep_running(
        _load_latest_community_snapshot(profile.exchange, profile.primary_symbol), 4.0
    )
    history_whale_task = _wait_or_none_keep_running(
        _load_latest_whale_snapshot(profile.exchange, profile.primary_symbol), 4.0
    )
    derivatives_overview_task = _wait_or_none_keep_running(
        _load_preferred_coinglass_overview(profile.primary_symbol),
        8.0,
    )
    fear_greed_task = _wait_or_none_keep_running(
        _load_public_fear_greed_snapshot(),
        _MARKET_STATE_PUBLIC_MARKET_TIMEOUT_SEC,
    )
    market_breadth_task = _wait_or_none_keep_running(
        _load_public_market_breadth_snapshot(),
        _MARKET_STATE_PUBLIC_MARKET_TIMEOUT_SEC,
    )
    history_status_task = _wait_or_none_keep_running(
        get_analytics_history_status(
            exchange=profile.exchange, symbol=profile.primary_symbol
        ),
        6.0,
    )
    (
        risk_dashboard,
        news,
        calendar_data,
        macro_snapshot,
        history_micro,
        history_community,
        history_whale,
        derivatives_overview,
        fear_greed,
        market_breadth,
        history_status,
    ) = await asyncio.gather(
        risk_task,
        news_task,
        calendar_task,
        macro_task,
        history_micro_task,
        history_community_task,
        history_whale_task,
        derivatives_overview_task,
        fear_greed_task,
        market_breadth_task,
        history_status_task,
    )
    risk_dashboard = dict(risk_dashboard or {})
    news = (
        dict(news)
        if isinstance(news, dict)
        else _news_summary_timeout_fallback(profile.primary_symbol, 24)
    )
    calendar_data = dict(calendar_data or {})
    macro_snapshot = dict(macro_snapshot or {})
    history_micro = dict(history_micro or {})
    history_community = dict(history_community or {})
    history_whale = dict(history_whale or {})
    derivatives_overview = dict(derivatives_overview or {})
    fear_greed = (
        dict(fear_greed)
        if isinstance(fear_greed, dict)
        else _public_market_timeout_fallback(
            "fear_greed",
            source="alternative.me",
            stale_max_age_sec=_PUBLIC_FEAR_GREED_STALE_MAX_AGE_SEC,
        )
    )
    market_breadth = (
        dict(market_breadth)
        if isinstance(market_breadth, dict)
        else _public_market_timeout_fallback(
            "global_market_breadth",
            source="coingecko_global",
            stale_max_age_sec=_PUBLIC_MARKET_BREADTH_STALE_MAX_AGE_SEC,
        )
    )
    history_status = _analytics_status_collectors_to_map(dict(history_status or {}))
    derivatives_summary = _pick_preferred_derivatives_summary(
        _build_derivatives_summary_from_overview(derivatives_overview),
        _build_derivatives_shadow_summary(history_status, {}),
    )

    prefer_history_micro = _snapshot_is_recent(
        history_micro, _MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC
    ) and _microstructure_has_signal(history_micro)
    prefer_history_community = _snapshot_is_recent(
        history_community, _MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC
    ) and _community_has_signal(history_community)

    live_micro_task: Optional[Any] = None
    live_community_task: Optional[Any] = None
    if not prefer_history_micro:
        live_micro_task = _wait_or_none_keep_running(
            get_market_microstructure(
                exchange=profile.exchange,
                symbol=profile.primary_symbol,
                depth_limit=20,
            ),
            _MARKET_STATE_LIVE_FETCH_TIMEOUT_SEC,
        )
    if not prefer_history_community:
        live_community_task = _wait_or_none_keep_running(
            get_community_overview(
                symbol=profile.primary_symbol,
                exchange=profile.exchange,
            ),
            _MARKET_STATE_LIVE_FETCH_TIMEOUT_SEC,
        )
    live_micro = (
        dict((await live_micro_task) or {}) if live_micro_task is not None else {}
    )
    live_community = (
        dict((await live_community_task) or {})
        if live_community_task is not None
        else {}
    )

    micro = dict(
        history_micro
        if prefer_history_micro
        else (_merge_nested_payload(live_micro, history_micro) or {})
    )
    community = dict(
        history_community
        if prefer_history_community
        else (_merge_nested_payload(live_community, history_community) or {})
    )
    micro = _apply_derivatives_shadow_to_microstructure(micro, derivatives_summary)
    used_history_micro = prefer_history_micro or (
        not prefer_history_micro
        and _microstructure_has_signal(history_micro)
        and not _microstructure_has_signal(live_micro)
    )
    used_history_community = prefer_history_community or (
        not prefer_history_community
        and _community_has_signal(history_community)
        and not _community_has_signal(live_community)
    )
    used_history_whale = False

    if not isinstance(community.get("whale_transfers"), dict) or "count" not in (
        community.get("whale_transfers") or {}
    ):
        if history_whale:
            community["whale_transfers"] = {
                "available": bool(history_whale.get("available", True)),
                "count": int(history_whale.get("count") or 0),
                "threshold_btc": history_whale.get("threshold_btc"),
                "btc_price": history_whale.get("btc_price"),
                "transactions": list(history_whale.get("transactions") or [])[:10],
            }
            used_history_whale = True

    calendar_rows = _map_calendar_rows(calendar_data.get("events") or [])
    calendar_source_summary = _build_calendar_source_summary(
        calendar_data, calendar_rows
    )
    macro_summary = _build_macro_summary(macro_snapshot)
    macro_source_summary = _build_macro_source_summary(macro_snapshot)
    derivatives_source_summary = _build_derivatives_source_summary(derivatives_summary)
    analytics_modules = {
        "risk_dashboard": _analytics_module_entry(
            "risk_dashboard",
            risk_dashboard,
            ok=bool(risk_dashboard),
            error=None if risk_dashboard else "risk dashboard unavailable",
        ),
        "calendar": _analytics_module_entry(
            "calendar",
            calendar_data,
            ok=bool(calendar_rows),
            error=None if calendar_rows else "calendar unavailable",
        ),
        "microstructure": _analytics_module_entry(
            "microstructure",
            micro,
            ok=_microstructure_has_signal(micro),
            error=None
            if _microstructure_has_signal(micro)
            else "microstructure unavailable",
        ),
        "community": _analytics_module_entry(
            "community",
            community,
            ok=_community_has_signal(community),
            error=None if _community_has_signal(community) else "community unavailable",
        ),
        "macro": _analytics_module_entry(
            "macro",
            {"snapshot": macro_snapshot, "summary": macro_summary},
            ok=_macro_has_signal(macro_snapshot),
            error=None
            if _macro_has_signal(macro_snapshot)
            else "macro snapshot unavailable",
        ),
    }
    analytics = {
        "timestamp": _now_iso(),
        "all_ok": all(
            bool((item or {}).get("ok")) for item in analytics_modules.values()
        ),
        "ok_count": len(
            [
                item
                for item in analytics_modules.values()
                if bool((item or {}).get("ok"))
            ]
        ),
        "total": len(analytics_modules),
        "modules": analytics_modules,
    }

    micro_summary = _build_microstructure_summary(micro)
    regime = _build_market_regime(analytics, micro, news, derivatives_summary)
    degraded = False
    warnings: List[str] = []

    if not risk_dashboard:
        degraded = True
        warnings.append("风险看板超时，市场状态判定使用了部分输入。")
    elif bool(risk_dashboard.get("stale")):
        warnings.append("风险看板实时刷新挂起，正在使用最近一次快照。")
    if str(news.get("scope")) == "global_fallback":
        degraded = True
        warnings.append("当前标的新闻样本稀疏，已切换到全市场新闻兜底。")
    if news.get("stale") and not _source_pending_timeout(news):
        warnings.append("新闻摘要实时刷新失败，正在使用最近一次快照。")
    if _source_pending_timeout(news):
        degraded = True
        warnings.append("新闻摘要仍在刷新中，事件覆盖稍后才会更新。")
    elif (
        int(news.get("events_count") or 0)
        + int(news.get("feed_count") or 0)
        + int(news.get("raw_count") or 0)
        <= 0
    ):
        degraded = True
        warnings.append("新闻摘要无可用样本，事件覆盖可能过旧。")
    if used_history_micro and not prefer_history_micro:
        degraded = True
        warnings.append("实时微观结构超时，正在使用最近一次快照兜底。")
    elif bool(micro.get("stale")):
        warnings.append("微观结构实时刷新挂起，正在使用最近一次快照。")
    if (used_history_community and not prefer_history_community) or used_history_whale:
        degraded = True
        warnings.append("实时社区/巨鲸流超时，正在使用最近一次快照兜底。")
    elif bool(community.get("stale")):
        warnings.append("社区摘要实时刷新挂起，正在使用最近一次快照。")
    if bool(((community.get("security_alerts") or {}).get("stale"))):
        warnings.append("安全警报实时刷新失败，正在使用最近一次 SlowMist 缓存。")
    if not calendar_rows:
        degraded = True
        warnings.append("交易日历不可用，已使用 watchlist 兜底。")
    elif bool(calendar_source_summary.get("stale")) and calendar_rows:
        warnings.append("交易日历实时刷新失败，正在使用最近一次 CoinGlass 缓存。")
    elif bool(
        calendar_source_summary.get("explicit_source_metadata")
    ) and not calendar_source_summary.get("official_available"):
        degraded = True
        warnings.append("交易日历目前只能依赖内部估算，官方 CoinGlass 事件暂不可用。")
    elif (
        bool(calendar_source_summary.get("fallback_used"))
        and int(calendar_source_summary.get("estimated_count") or 0) > 0
        and int(calendar_source_summary.get("official_count") or 0) > 0
        and int(calendar_source_summary.get("estimated_count") or 0)
        >= int(calendar_source_summary.get("official_count") or 0)
    ):
        warnings.append("交易日历当前混用官方 CoinGlass 事件和内部估算补充。")
    if not micro_summary.get("has_actionable_signal"):
        degraded = True
        warnings.append("微观结构无可用信号，盘口/主动流解读受限。")
    elif not micro_summary.get("has_orderbook_depth"):
        warnings.append("盘口深度样本稀疏，墙/流动性诊断可能不完整。")
    if not micro_summary.get("long_short_ratio_available"):
        warnings.append("当前快照缺多空比，拥挤度诊断不完整。")
    has_derivatives_context = (
        bool(derivatives_summary.get("key_configured"))
        or bool(derivatives_summary.get("available"))
        or bool(derivatives_summary.get("degraded_reason"))
        or int(derivatives_summary.get("dataset_count") or 0) > 0
    )
    if has_derivatives_context:
        if not derivatives_summary.get("available"):
            degraded = True
            warnings.append("CoinGlass 衍生品快照不可用，研究正在使用交易所/公开兜底数据。")
        elif _coerce_finite_float(
            derivatives_summary.get("freshness_sec")
        ) is not None and float(
            derivatives_summary.get("freshness_sec") or 0.0
        ) > float(
            _COINGLASS_PREFERRED_MAX_AGE_SEC
        ):
            degraded = True
            warnings.append("CoinGlass 衍生品快照偏旧，拥挤度/资金费率上下文可能滞后。")
        elif derivatives_summary.get("degraded_reason"):
            degraded = True
            warnings.append(
                f"CoinGlass 衍生品快照降级：{derivatives_summary.get('degraded_reason')}。"
            )
    if not _macro_has_signal(macro_snapshot):
        warnings.append("宏观快照不可用，市场状态判断只能依靠微观结构和新闻。")
    elif bool(macro_source_summary.get("stale")):
        macro_cache_age = _coerce_finite_float(macro_source_summary.get("cache_age_sec"))
        if (
            macro_cache_age is not None
            and macro_cache_age > float(_MACRO_MONTHLY_STALE_MAX_AGE_SEC)
        ):
            degraded = True
        warnings.append("宏观缓存偏旧，跨市场或地区数据释放上下文可能滞后。")
    elif str(macro_source_summary.get("source_status") or "") == "partial":
        warnings.append("宏观快照仅部分填充，跨市场/美国/中国分组中有缺失。")
    elif macro_summary.get("scissors_spread_pp") is None:
        warnings.append("宏观快照缺 PPI-CPI 剪刀差，请刷新宏观缓存补全。")
    elif macro_summary.get("china_scissors_spread_pp") is None:
        warnings.append("中国宏观快照缺 PPI-CPI 剪刀差，中国侧解读不完整。")
    fear_greed_value = _coerce_finite_float(fear_greed.get("value"))
    if _source_pending_timeout(fear_greed):
        warnings.append("Fear & Greed 刷新仍在进行，情绪上下文稍后才会更新。")
    elif fear_greed.get("available") is False or "available" not in fear_greed:
        warnings.append("Fear & Greed 指数不可用，群体情绪上下文不完整。")
    elif fear_greed.get("stale"):
        warnings.append("Fear & Greed 实时刷新失败，正在使用最近一次快照。")
    elif fear_greed_value is not None and fear_greed_value <= 25:
        warnings.append("Fear & Greed 处于极度恐惧区，恐慌引发的反转风险上升。")
    elif fear_greed_value is not None and fear_greed_value >= 75:
        warnings.append("Fear & Greed 处于极度贪婪区，拥挤风险升高。")
    market_cap_change_pct_24h = _coerce_finite_float(
        market_breadth.get("market_cap_change_pct_24h")
    )
    if _source_pending_timeout(market_breadth):
        warnings.append("全市场广度刷新仍在进行，跨市场盘口上下文稍后才会更新。")
    elif market_breadth.get("available") is False or "available" not in market_breadth:
        warnings.append("全市场广度数据不可用，跨市场盘口上下文不完整。")
    elif market_breadth.get("stale"):
        warnings.append("全市场广度实时刷新失败，正在使用最近一次快照。")
    elif market_cap_change_pct_24h is not None and market_cap_change_pct_24h <= -3.0:
        warnings.append("加密总市值 24h 急跌，beta 风险仍高。")
    elif market_cap_change_pct_24h is not None and market_cap_change_pct_24h >= 3.0:
        warnings.append("加密总市值 24h 快速扩张，beta 跟随改善。")
    if regime.get("risk_level") == "high":
        warnings.append("当前风险等级高，请降低置信度并收紧风险预算。")
    return _module_result(
        "market_state",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=[
            "trading.analytics.risk_dashboard",
            "trading.analytics.calendar",
            "trading.analytics.microstructure",
            "trading.analytics.community",
            "news.storage.summary",
            "analytics.history.snapshots",
            "data.macro.snapshot",
            "data.coinglass.derivatives",
            "alternative.me.fng",
            "coingecko.global",
        ],
        warnings=warnings,
        summary={
            "headline": f"{regime['regime']} | {profile.primary_symbol} | {profile.timeframe}",
            "market_regime": regime["regime"],
            "direction_bias": regime["bias"],
            "confidence": regime["confidence"],
            "risk_level": regime["risk_level"],
            "macro_focus": macro_summary["headline"],
            "calendar_source": calendar_source_summary.get("source"),
            "calendar_official_count": int(
                calendar_source_summary.get("official_count") or 0
            ),
            "calendar_estimated_count": int(
                calendar_source_summary.get("estimated_count") or 0
            ),
            "derivatives_status": str(derivatives_summary.get("status") or "missing"),
            "derivatives_source_status": derivatives_source_summary.get(
                "source_status"
            ),
            "derivatives_freshness_sec": derivatives_summary.get("freshness_sec"),
            "macro_source_status": macro_source_summary.get("source_status"),
            "macro_cache_age_sec": macro_source_summary.get("cache_age_sec"),
            "fear_greed_value": int(fear_greed_value)
            if fear_greed_value is not None
            else None,
            "market_cap_change_pct_24h": market_cap_change_pct_24h,
        },
        payload={
            "analytics_overview": analytics,
            "sentiment_dashboard": {
                "exchange": profile.exchange,
                "symbol": profile.primary_symbol,
                "timestamp": _now_iso(),
                "microstructure": micro,
                "microstructure_summary": micro_summary,
                "community": community,
                "news": news,
                "macro": macro_snapshot,
                "macro_source_summary": macro_source_summary,
                "macro_regions": dict(macro_summary.get("regions") or {}),
                "derivatives": derivatives_summary,
                "derivatives_source_summary": derivatives_source_summary,
                "fear_greed": fear_greed,
                "global_market_breadth": market_breadth,
                "calendar_source_summary": calendar_source_summary,
            },
            "calendar_watchlist": calendar_rows,
            "calendar_source_summary": calendar_source_summary,
            "regime": regime,
            "microstructure_summary": micro_summary,
            "derivatives_summary": derivatives_summary,
            "derivatives_source_summary": derivatives_source_summary,
            "macro_snapshot": macro_snapshot,
            "macro_summary": macro_summary,
            "macro_source_summary": macro_source_summary,
            "macro_regions": dict(macro_summary.get("regions") or {}),
            "fear_greed": fear_greed,
            "global_market_breadth": market_breadth,
        },
    )


async def _build_factors_module(profile: ResearchProfile) -> Dict[str, Any]:
    symbols = ",".join(_profile_symbol_window(profile, 30))
    factor_task = _wait_or_none(
        get_factor_library(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(900, profile.lookback),
            quantile=0.3,
            series_limit=240,
            exclude_retired=profile.exclude_retired,
        ),
        14.0,
    )
    fama_task = _wait_or_none(
        get_fama_like_factors(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(360, profile.lookback),
            exclude_retired=profile.exclude_retired,
        ),
        12.0,
    )
    cross_task = _wait_or_none(
        get_multi_assets_overview(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(360, profile.lookback),
            exclude_retired=profile.exclude_retired,
        ),
        12.0,
    )
    factor_raw, fama_raw, cross_asset_raw = await asyncio.gather(
        factor_task, fama_task, cross_task
    )
    factor_library = _compact_factor_library(factor_raw or {})
    fama = _compact_fama(fama_raw or {})
    fama_from_factor_library = False
    if not fama and factor_library:
        fama = _fama_from_factor_library(factor_library)
        fama_from_factor_library = bool(fama)
    cross_asset = dict(cross_asset_raw or {})

    fallback_library = (
        _fallback_factor_library_from_fama(profile, fama, cross_asset) if fama else {}
    )
    if not factor_library and fallback_library:
        factor_library = fallback_library
    elif factor_library:
        if not factor_library.get("asset_scores") and fallback_library.get(
            "asset_scores"
        ):
            factor_library["asset_scores"] = fallback_library["asset_scores"]
        if not factor_library.get("correlation") and fallback_library.get(
            "correlation"
        ):
            factor_library["correlation"] = fallback_library["correlation"]

    warnings: List[str] = list(factor_library.get("warnings") or [])
    if fama_from_factor_library:
        warnings.extend(list(fama.get("warnings") or []))
    if not factor_library:
        warnings.append("因子库超时，正在返回简化兜底摘要。")
    if not fama:
        warnings.append("本次运行无可用的 Fama 风格因子。")
    if not cross_asset:
        warnings.append("多币种快照超时，资产排序可能不完整。")
    warnings.append(
        "研究工坊因子模块为速度优化，并不替代完整的因子研究任务。"
    )
    latest_fama = dict(fama.get("latest") or {})
    top_symbols = [
        str(item.get("symbol") or "")
        for item in list(factor_library.get("asset_scores") or [])[:3]
        if item.get("symbol")
    ]
    degraded = (
        not factor_library
        or not fama
        or fama_from_factor_library
        or str(factor_library.get("universe_quality") or "") == "low"
        or int(factor_library.get("universe_size") or 0) < 4
    )

    return _module_result(
        "factors",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["data.factors.library", "data.factors.fama"],
        warnings=warnings[:8],
        summary={
            "headline": "因子与风格",
            "top_symbols": top_symbols,
            "universe_size": int(factor_library.get("universe_size") or 0),
            "factor_count": len(factor_library.get("factors") or []),
            "mkt": float(latest_fama.get("MKT") or 0.0),
            "mom": float(latest_fama.get("MOM") or 0.0),
        },
        payload={"factor_library": factor_library, "fama": fama},
    )


async def _build_cross_asset_module(profile: ResearchProfile) -> Dict[str, Any]:
    data = (
        await _wait_or_none(
            get_multi_assets_overview(
                exchange=profile.exchange,
                symbols=",".join(_profile_symbol_window(profile, 10)),
                timeframe=profile.timeframe,
                lookback=min(720, profile.lookback),
                exclude_retired=profile.exclude_retired,
            ),
            12.0,
        )
        or {}
    )
    assets = list(data.get("assets") or [])
    leader = assets[0] if assets else {}
    degraded = int(data.get("count") or 0) < 3
    warnings = (
        ["可用币种少于 3 个，多币种轮动结论可能噪声较大。"]
        if degraded
        else []
    )

    return _module_result(
        "cross_asset",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["data.multi_assets.overview"],
        warnings=warnings,
        summary={
            "headline": "多币种轮动",
            "asset_count": int(data.get("count") or 0),
            "leader_symbol": str(leader.get("symbol") or "-"),
            "leader_return_pct": float(leader.get("return_pct") or 0.0),
        },
        payload={"cross_asset": data},
    )


async def _onchain_overview_for_workbench(profile: ResearchProfile) -> Dict[str, Any]:
    """On-chain overview, waiting briefly through a cold cache's warm-up.

    get_onchain_overview never blocks: on a cold cache it returns a "warming"
    placeholder (served_mode=bootstrap) and fills the cache in the background
    (~10 s). The workbench used to cache that placeholder as its module result
    for up to 10 minutes, so the first view after a restart or an idle spell
    always showed no funding / no Fear & Greed (2026-09-28).
    """
    kwargs = dict(exchange=profile.exchange, symbol=profile.primary_symbol, whale_threshold_btc=10.0, chain="auto")
    result = dict(await get_onchain_overview(refresh=True, **kwargs) or {})
    deadline = time.monotonic() + _ONCHAIN_WARMUP_WAIT_SEC
    while str(result.get("served_mode") or "") == "bootstrap" and time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        result = dict(await get_onchain_overview(refresh=False, **kwargs) or {})
    return result


async def _build_onchain_module(profile: ResearchProfile) -> Dict[str, Any]:
    chain_context = resolve_onchain_chain_context(profile.primary_symbol, "auto")
    onchain_task = _wait_or_none(_onchain_overview_for_workbench(profile), _ONCHAIN_WARMUP_WAIT_SEC + 4.0)
    community_snapshot_task = _wait_or_none(
        _load_latest_community_snapshot(profile.exchange, profile.primary_symbol), 4.0
    )
    whale_snapshot_task = _wait_or_none(
        _load_latest_whale_snapshot(profile.exchange, profile.primary_symbol), 4.0
    )
    news_task = _wait_or_none(
        _build_news_summary(profile.primary_symbol, hours=72), 8.0
    )
    history_task = _wait_or_none(
        get_analytics_history_status(
            exchange=profile.exchange, symbol=profile.primary_symbol
        ),
        6.0,
    )
    (
        onchain,
        community_snapshot,
        whale_snapshot,
        news,
        history_status,
    ) = await asyncio.gather(
        onchain_task,
        community_snapshot_task,
        whale_snapshot_task,
        news_task,
        history_task,
    )

    onchain = dict(onchain or {})
    community_snapshot = dict(community_snapshot or {})
    community = community_snapshot
    whale_snapshot = dict(whale_snapshot or {})
    news = dict(news or {})
    history_status = _analytics_status_collectors_to_map(dict(history_status or {}))

    funding_multi = dict(onchain.get("funding_rate_multi_source") or {})
    fear_greed = dict(onchain.get("fear_greed_index") or {})
    derivatives_summary = _build_derivatives_shadow_summary(history_status, onchain)
    derivatives_source_summary = _build_derivatives_source_summary(derivatives_summary)
    funding_count = int(funding_multi.get("count") or 0)
    fear_greed_available = bool(fear_greed.get("available"))

    degraded = (
        bool(onchain.get("degraded"))
        or not onchain
        or str(news.get("scope")) == "global_fallback"
        or (funding_count <= 0 and not fear_greed_available)
    )
    warnings: List[str] = []
    if not onchain:
        warnings.append("链上概览超时，正在返回兜底摘要。")
    elif onchain.get("degraded"):
        warnings.append("链上数据包含代理/缓存数据，置信度下降。")
    if news.get("stale"):
        warnings.append("新闻摘要实时刷新失败，正在使用最近一次快照。")
    if funding_count <= 0:
        warnings.append("当前无可用的多交易所资金费率。")
    if not fear_greed_available:
        warnings.append("当前 Fear & Greed 指数不可用。")
    if not derivatives_summary.get("available"):
        warnings.append("CoinGlass 衍生品影子不可用，拥挤度上下文受限。")
    elif (
        derivatives_summary.get("freshness_sec") is not None
        and float(derivatives_summary.get("freshness_sec") or 0.0) > 1800
    ):
        warnings.append("CoinGlass 衍生品影子偏旧，资金费率/拥挤度上下文可能滞后。")
    elif derivatives_summary.get("degraded_reason"):
        warnings.append(
            f"CoinGlass 衍生品影子降级：{derivatives_summary.get('degraded_reason')}。"
        )
    if str(news.get("scope")) == "global_fallback":
        warnings.append("当前标的的外生新闻稀疏，正在使用全市场兜底。")

    return _module_result(
        "onchain",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=[
            "data.onchain.overview",
            "trading.analytics.community",
            "news.storage.summary",
            "analytics.history",
        ],
        warnings=warnings[:8],
        summary={
            "headline": "链上与外生",
            "whale_count": int(
                (onchain.get("whale_activity") or {}).get("count")
                or (community.get("whale_transfers") or {}).get("count")
                or whale_snapshot.get("count")
                or 0
            ),
            "news_events": int(news.get("events_count") or 0),
            "tvl_chain": str(
                (onchain.get("defi_tvl") or {}).get("chain")
                or (onchain.get("chain_context") or {}).get("display_name")
                or chain_context.get("display_name")
                or "Auto"
            ),
            "served_mode": str(onchain.get("served_mode") or "live"),
            "funding_sources": funding_count,
            "funding_mean_rate_pct": float(funding_multi.get("mean_rate_pct") or 0.0)
            if funding_count > 0
            else None,
            "fear_greed_value": int(fear_greed.get("value") or 0)
            if fear_greed_available
            else None,
            "fear_greed_classification": str(fear_greed.get("classification") or "")
            if fear_greed_available
            else None,
            "derivatives_status": str(derivatives_summary.get("status") or "missing"),
            "derivatives_source_status": derivatives_source_summary.get(
                "source_status"
            ),
            "derivatives_freshness_sec": derivatives_summary.get("freshness_sec"),
            "derivatives_dataset_count": int(
                derivatives_summary.get("dataset_count") or 0
            ),
        },
        payload={
            "onchain": onchain,
            "community": community,
            "whale_snapshot": whale_snapshot,
            "news_summary": news,
            "analytics_history_status": history_status,
            "derivatives_summary": derivatives_summary,
            "derivatives_source_summary": derivatives_source_summary,
        },
    )


async def _build_discipline_module(_: ResearchProfile) -> Dict[str, Any]:
    behavior_task = _wait_or_none(get_behavior_report(days=7), 4.0)
    stoploss_task = _wait_or_none(get_stoploss_policy(), 4.0)
    behavior, stoploss = await asyncio.gather(behavior_task, stoploss_task)
    behavior = dict(behavior or {})
    stoploss = dict(stoploss or {})
    suggestions = list(stoploss.get("position_suggestions") or [])
    impulsive_ratio = float(behavior.get("impulsive_ratio") or 0.0)
    overtrade = bool(behavior.get("overtrading_warning"))
    degraded = int(behavior.get("entries") or 0) == 0

    warnings: List[str] = []
    if degraded:
        warnings.append("近期无行为记录，纪律模块仅展示通用建议。")
    if overtrade:
        warnings.append("检测到过度交易风险。")
    if impulsive_ratio >= 0.3:
        warnings.append("冲动交易比例偏高。")

    return _module_result(
        "discipline",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=[
            "trading.analytics.behavior.report",
            "trading.analytics.stoploss.policy",
        ],
        warnings=warnings,
        summary={
            "headline": "纪律与风控",
            "entries": int(behavior.get("entries") or 0),
            "impulsive_ratio": round(impulsive_ratio, 4),
            "overtrading_warning": overtrade,
            "position_suggestions": len(suggestions),
        },
        payload={"behavior_report": behavior, "stoploss_policy": stoploss},
    )


async def _build_module(module_name: str, profile: ResearchProfile) -> Dict[str, Any]:
    if module_name == "market_state":
        return await _build_market_state_module(profile)
    if module_name == "factors":
        return await _build_factors_module(profile)
    if module_name == "cross_asset":
        return await _build_cross_asset_module(profile)
    if module_name == "onchain":
        return await _build_onchain_module(profile)
    if module_name == "discipline":
        return await _build_discipline_module(profile)
    raise HTTPException(
        status_code=404, detail=f"Unknown research module: {module_name}"
    )


# Workbench modules fan out to a dozen slow upstreams (each module burns its
# own timeout when a source is down), so an uncached overview costs 15-20s per
# page load. Serve a recent result instantly and refresh behind it.
_WORKBENCH_MODULE_FRESH_SEC = 60.0
_WORKBENCH_MODULE_STALE_MAX_SEC = 600.0
_WORKBENCH_MODULE_CACHE_MAX_ENTRIES = 64
_WORKBENCH_MODULE_CACHE: Dict[str, Dict[str, Any]] = {}
_WORKBENCH_MODULE_REFRESH_TASKS: Dict[str, "asyncio.Task[Dict[str, Any]]"] = {}


def _clear_workbench_module_cache() -> Dict[str, int]:
    cleared = len(_WORKBENCH_MODULE_CACHE)
    _WORKBENCH_MODULE_CACHE.clear()
    _WORKBENCH_MODULE_REFRESH_TASKS.clear()
    return {"entries": cleared}


def _workbench_module_cache_key(module_name: str, profile: ResearchProfile) -> str:
    return f"{module_name}|{profile.model_dump_json()}"


def _with_workbench_cache_meta(result: Dict[str, Any], *, age_sec: float, served_mode: str) -> Dict[str, Any]:
    payload = dict(result)
    payload["cache"] = {
        "hit": served_mode != "live_compute",
        "age_sec": round(max(0.0, age_sec), 3),
        "fresh_sec": _WORKBENCH_MODULE_FRESH_SEC,
        "served_mode": served_mode,
    }
    return payload


async def _build_and_cache_module(module_name: str, profile: ResearchProfile, cache_key: str) -> Dict[str, Any]:
    result = await _capture_module_build_uncached(module_name, profile)
    if str((result or {}).get("status") or "") in {"ok", "degraded"}:
        _WORKBENCH_MODULE_CACHE[cache_key] = {"stored_at": time.time(), "result": result}
        if len(_WORKBENCH_MODULE_CACHE) > _WORKBENCH_MODULE_CACHE_MAX_ENTRIES:
            oldest = min(_WORKBENCH_MODULE_CACHE, key=lambda k: _WORKBENCH_MODULE_CACHE[k]["stored_at"])
            _WORKBENCH_MODULE_CACHE.pop(oldest, None)
    return result


def _ensure_workbench_module_refresh(module_name: str, profile: ResearchProfile, cache_key: str) -> "asyncio.Task[Dict[str, Any]]":
    task = _WORKBENCH_MODULE_REFRESH_TASKS.get(cache_key)
    if task is None or task.done():
        task = asyncio.create_task(_build_and_cache_module(module_name, profile, cache_key))

        def _consume(finished: "asyncio.Task[Dict[str, Any]]", key: str = cache_key) -> None:
            if not finished.cancelled():
                finished.exception()
            if _WORKBENCH_MODULE_REFRESH_TASKS.get(key) is finished:
                _WORKBENCH_MODULE_REFRESH_TASKS.pop(key, None)

        task.add_done_callback(_consume)
        _WORKBENCH_MODULE_REFRESH_TASKS[cache_key] = task
    return task


async def _capture_module_build(
    module_name: str, profile: ResearchProfile
) -> Dict[str, Any]:
    if module_name not in _MODULE_ORDER:
        # Let the uncached path raise its 404 without creating cache entries.
        return await _capture_module_build_uncached(module_name, profile)
    cache_key = _workbench_module_cache_key(module_name, profile)
    entry = _WORKBENCH_MODULE_CACHE.get(cache_key)
    if entry:
        age_sec = time.time() - float(entry.get("stored_at") or 0.0)
        if age_sec <= _WORKBENCH_MODULE_FRESH_SEC:
            return _with_workbench_cache_meta(entry["result"], age_sec=age_sec, served_mode="cache_hit")
        if age_sec <= _WORKBENCH_MODULE_STALE_MAX_SEC:
            _ensure_workbench_module_refresh(module_name, profile, cache_key)
            return _with_workbench_cache_meta(entry["result"], age_sec=age_sec, served_mode="stale_refresh")
    task = _ensure_workbench_module_refresh(module_name, profile, cache_key)
    result = await asyncio.shield(task)
    return _with_workbench_cache_meta(result, age_sec=0.0, served_mode="live_compute")


async def _capture_module_build_uncached(
    module_name: str, profile: ResearchProfile
) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(
            _build_module(module_name, profile),
            timeout=_MODULE_TIMEOUT_SEC.get(module_name, 12.0),
        )
    except asyncio.TimeoutError:
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[f"{module_name} timed out and was skipped this round."],
            summary={"headline": f"{module_name} timeout", "error": "timeout"},
            payload={"error": "timeout"},
        )
    except HTTPException as exc:
        if exc.status_code == 404 and "Unknown research module" in str(exc.detail):
            raise
        error_text = str(exc.detail or "module failed")
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[error_text],
            summary={"headline": f"{module_name} failed", "error": error_text},
            payload={"error": error_text},
        )
    except Exception as exc:
        error_text = str(exc)
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[error_text],
            summary={"headline": f"{module_name} failed", "error": error_text},
            payload={"error": error_text},
        )


def _build_recommendations(
    profile: ResearchProfile,
    modules: Dict[str, Any],
    overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    market_payload = _extract_module_payload(modules.get("market_state"))
    cross_payload = _extract_module_payload(modules.get("cross_asset"))
    onchain_payload = _extract_module_payload(modules.get("onchain"))
    discipline_payload = _extract_module_payload(modules.get("discipline"))

    regime = dict(market_payload.get("regime") or {})
    cross_asset = dict(cross_payload.get("cross_asset") or {})
    onchain = dict(onchain_payload.get("onchain") or {})
    sentiment_dashboard = dict(market_payload.get("sentiment_dashboard") or {})
    market_derivatives_summary = dict(
        market_payload.get("derivatives_summary")
        or sentiment_dashboard.get("derivatives")
        or {}
    )
    derivatives_summary = _pick_preferred_derivatives_summary(
        onchain_payload.get("derivatives_summary") or {},
        market_derivatives_summary,
    )
    news_summary = _pick_preferred_news_summary(
        onchain_payload.get("news_summary") or {},
        sentiment_dashboard.get("news") or {},
    )
    behavior = dict(discipline_payload.get("behavior_report") or {})

    direction_bias = str(regime.get("bias") or "neutral")
    if direction_bias == "bullish":
        preferred = ["趋势跟随", "动量突破", "回踩入场"]
    elif direction_bias == "bearish":
        preferred = ["防守型均值回归", "反弹做空", "事件驱动快进快出"]
    elif direction_bias == "defensive":
        preferred = ["轻仓观察", "防守对冲", "回撤控制"]
    else:
        preferred = ["均值回归", "区间交易", "轻仓试探"]

    avoid: List[str] = []
    next_actions: List[str] = []
    jump_targets: List[Dict[str, Any]] = []

    if bool(onchain.get("degraded")):
        avoid.append("链上数据降级，请勿单独作为入场触发条件。")
    if not bool(derivatives_summary.get("available")):
        avoid.append("衍生品快照缺失，不要依赖资金费率/拥挤度做单独确认。")
    elif (
        _coerce_finite_float(derivatives_summary.get("freshness_sec")) is not None
        and float(derivatives_summary.get("freshness_sec") or 0.0) > 1800
    ):
        avoid.append("衍生品快照偏旧，执行前先确认最新资金费率与拥挤度。")
    if not _news_summary_has_usable_samples(news_summary):
        avoid.append("当前标的新闻样本不足，避免只靠事件驱动做决策。")
    if bool(behavior.get("overtrading_warning")):
        avoid.append("过度交易预警已触发，降低试单频率。")
    if float(behavior.get("impulsive_ratio") or 0.0) >= 0.3:
        avoid.append("执行纪律偏弱，避免同时追逐多个标的。")

    if direction_bias == "bullish":
        next_actions.append("在 5m/15m 上确认趋势延续后再加仓。")
    elif direction_bias == "bearish":
        next_actions.append("优先考虑防守型设置，控制下行风险敞口。")
    elif direction_bias == "defensive":
        next_actions.append("减小仓位、等点差与拥挤度回到正常区间再考虑入场。")
    else:
        next_actions.append("先确认震荡/回归型条件，再扩大覆盖范围。")

    if int(cross_asset.get("count") or 0) < 3:
        next_actions.append("可观察币种不足 3 个，先扩展币种覆盖再下轮动结论。")
    if bool(derivatives_summary.get("available")):
        next_actions.append("用衍生品快照确认资金费率和拥挤度后再执行。")

    headline = str(
        (overview or {}).get("market_regime")
        or regime.get("regime")
        or "研究推荐"
    )
    if profile.primary_symbol:
        jump_targets.append(
            {
                "label": f"回测 {profile.primary_symbol}",
                "target": "backtest",
                "params": {
                    "exchange": profile.exchange,
                    "symbol": profile.primary_symbol,
                    "timeframe": profile.timeframe,
                },
            }
        )

    return {
        "direction_bias": direction_bias,
        "preferred_strategy_families": preferred,
        "avoid_conditions": avoid,
        "next_actions": next_actions,
        "backtest_jump_targets": jump_targets,
        "headline": headline,
        "generated_at": _now_iso(),
    }


def _build_structured_recommendations(
    profile: ResearchProfile,
    modules: Dict[str, Any],
    overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _derive_research_timeframes(base_timeframe: str) -> List[str]:
        presets = {
            "1m": ["1m", "5m", "15m"],
            "5m": ["5m", "15m", "1h"],
            "15m": ["5m", "15m", "1h", "4h"],
            "1h": ["15m", "1h", "4h"],
            "4h": ["1h", "4h", "1d"],
            "1d": ["4h", "1d"],
        }
        selected = presets.get(str(base_timeframe or "5m").lower(), ["5m", "15m", "1h"])
        normalized: List[str] = []
        for timeframe in selected:
            tf = str(timeframe or "").lower()
            if tf in _VALID_TIMEFRAMES and tf not in normalized:
                normalized.append(tf)
        return normalized or ["5m", "15m", "1h"]

    def _pick_backtest_strategy(bias: str, title: str) -> Dict[str, str]:
        headline_text = str(title or "")
        headline_lower = headline_text.lower()
        if (
            "breakout" in headline_lower
            or "break" in headline_lower
            or "突破" in headline_text
        ):
            return {"strategy_type": "DonchianBreakoutStrategy", "label": "突破"}
        if bias == "bullish":
            return {"strategy_type": "TrendFollowingStrategy", "label": "趋势"}
        if bias == "bearish":
            return {"strategy_type": "MeanReversionStrategy", "label": "均值回归"}
        return {"strategy_type": "MeanReversionStrategy", "label": "均值回归"}

    def _map_planner_regime(bias: str, title: str) -> str:
        headline_text = str(title or "")
        headline_lower = headline_text.lower()
        if (
            "news" in headline_lower
            or "event" in headline_lower
            or "事件" in headline_text
            or "新闻" in headline_text
        ):
            return "news_event"
        if (
            "breakout" in headline_lower
            or "break" in headline_lower
            or "突破" in headline_text
        ):
            return "breakout"
        if bias == "bullish":
            return "trend_up"
        if bias == "bearish":
            return "trend_down"
        if "mean" in headline_lower or "range" in headline_lower:
            return "mean_reversion"
        return "mixed"

    base = _build_recommendations(profile, modules, overview)
    market_payload = _extract_module_payload(modules.get("market_state"))
    factors_payload = _extract_module_payload(modules.get("factors"))
    cross_payload = _extract_module_payload(modules.get("cross_asset"))
    onchain_payload = _extract_module_payload(modules.get("onchain"))

    regime = dict(market_payload.get("regime") or {})
    factor_library = dict(factors_payload.get("factor_library") or {})
    cross_asset = dict(cross_payload.get("cross_asset") or {})
    onchain = dict(onchain_payload.get("onchain") or {})
    sentiment_dashboard = dict(market_payload.get("sentiment_dashboard") or {})
    macro_snapshot = dict(
        market_payload.get("macro_snapshot") or sentiment_dashboard.get("macro") or {}
    )
    market_derivatives_summary = dict(
        market_payload.get("derivatives_summary")
        or sentiment_dashboard.get("derivatives")
        or {}
    )
    derivatives_summary = _pick_preferred_derivatives_summary(
        onchain_payload.get("derivatives_summary") or {},
        market_derivatives_summary,
    )
    news_summary = _pick_preferred_news_summary(
        onchain_payload.get("news_summary") or {},
        sentiment_dashboard.get("news") or {},
    )

    direction_bias = str(base.get("direction_bias") or "neutral")
    preferred = list(base.get("preferred_strategy_families") or [])
    avoid = list(base.get("avoid_conditions") or [])
    next_actions = list(base.get("next_actions") or [])
    jump_targets = list(base.get("backtest_jump_targets") or [])

    asset_scores = list(factor_library.get("asset_scores") or [])
    factor_focus = [
        {
            "symbol": str(item.get("symbol") or "").strip(),
            "score": round(float(item.get("score") or 0.0), 4),
            "momentum": round(float(item.get("momentum") or 0.0), 4),
            "quality": round(float(item.get("quality") or 0.0), 4),
        }
        for item in asset_scores[:3]
        if str(item.get("symbol") or "").strip()
    ]
    top_symbols = [item["symbol"] for item in factor_focus]
    focus_symbols = top_symbols or [profile.primary_symbol]

    headline = str(
        (overview or {}).get("market_regime")
        or regime.get("regime")
        or base.get("headline")
        or "研究推荐"
    )
    planner_regime = _map_planner_regime(direction_bias, headline)
    research_timeframes = _derive_research_timeframes(profile.timeframe)
    backtest_strategy = _pick_backtest_strategy(direction_bias, headline)

    factor_source_meta = {
        "served_mode": str(factor_library.get("served_mode") or "unknown"),
        "cached": bool(factor_library.get("cached")),
        "cache_age_sec": round(float(factor_library.get("cache_age_sec") or 0.0), 3),
        "universe_size": int(factor_library.get("universe_size") or 0),
        "symbols_used": int(len(factor_library.get("symbols_used") or [])),
        "generated_at": str(modules.get("factors", {}).get("generated_at") or ""),
    }

    thesis_points: List[str] = []
    if factor_focus:
        thesis_points.append(
            "因子焦点："
            + " / ".join(
                f"{item['symbol']}({item['score']:.2f})" for item in factor_focus
            )
        )
    cross_leader = str(
        cross_asset.get("leader_symbol")
        or (
            (cross_asset.get("assets") or [{}])[0].get("symbol")
            if isinstance(cross_asset.get("assets"), list)
            else ""
        )
        or ""
    )
    if cross_leader:
        thesis_points.append(f"横截面领涨：{cross_leader}。")
    whale_count = int((onchain.get("whale_activity") or {}).get("count") or 0)
    if whale_count > 0:
        thesis_points.append(f"链上巨鲸活跃（{whale_count} 笔）。")
    derivatives_freshness = _coerce_finite_float(
        derivatives_summary.get("freshness_sec")
    )
    derivatives_labels = list(derivatives_summary.get("derivatives_labels") or [])
    funding_zscore = _coerce_finite_float(derivatives_summary.get("funding_zscore"))
    long_short_ratio_change = _coerce_finite_float(
        derivatives_summary.get("long_short_ratio_change_24h")
    )
    if bool(derivatives_summary.get("available")):
        dataset_count = int(derivatives_summary.get("dataset_count") or 0)
        provider = str(derivatives_summary.get("provider") or "coinglass")
        thesis_points.append(
            f"衍生品快照：{provider} / {str(derivatives_summary.get('status') or 'ok')} / {dataset_count} 个数据集。"
        )
        if derivatives_freshness is not None:
            thesis_points.append(
                f"衍生品新鲜度：{derivatives_freshness:.0f} 秒。"
            )
        funding_mean_rate_pct = _coerce_finite_float(
            derivatives_summary.get("funding_mean_rate_pct")
        )
        if funding_mean_rate_pct is not None:
            thesis_points.append(
                f"资金费率均值：{funding_mean_rate_pct:+.2f}%。"
            )
        if funding_zscore is not None:
            thesis_points.insert(
                min(len(thesis_points), 1),
                f"资金费率 z-score：{funding_zscore:+.2f}。",
            )
        if derivatives_summary.get("squeeze_building"):
            thesis_points.insert(
                min(len(thesis_points), 2),
                "衍生品空头挤压风险正在累积。",
            )
        if derivatives_summary.get("order_flow_confirmed"):
            thesis_points.insert(
                min(len(thesis_points), 2),
                "主动流正在确认当前衍生品持仓方向。",
            )
        if long_short_ratio_change is not None:
            thesis_points.append(
                f"多空比 24h 变化：{long_short_ratio_change:+.2f}。"
            )
    macro_gap = _coerce_finite_float(macro_snapshot.get("ppi_cpi_gap"))
    if macro_gap is not None:
        thesis_points.append(f"宏观剪刀差（PPI-CPI）：{macro_gap:+.2f}pp。")
    liquidity_gap = _coerce_finite_float(macro_snapshot.get("m1_m2_gap"))
    if liquidity_gap is not None:
        thesis_points.append(
            f"流动性剪刀差（M1-M2）：{liquidity_gap:+.2f}pp。"
        )
    if int(news_summary.get("events_count") or 0) > 0:
        thesis_points.append(
            f"过去 24 小时新闻事件：{int(news_summary.get('events_count') or 0)} 条。"
        )
    if not thesis_points:
        thesis_points.append(
            "当前结论基于轻量模块摘要构建，建议补齐数据后再下定论。"
        )

    if not bool(derivatives_summary.get("available")):
        avoid.append("衍生品快照缺失，资金费率/拥挤度确认不完整。")
    elif derivatives_freshness is not None and derivatives_freshness > 1800:
        avoid.append("衍生品快照偏旧，资金费率/拥挤度确认可能滞后。")
    elif not bool(derivatives_summary.get("history_ready")):
        avoid.append("衍生品历史序列不完整，z-score 与回归信号置信度偏低。")
    if derivatives_summary.get("crowded_long"):
        avoid.append("多头拥挤，追涨开仓的轧空风险升高。")
    if derivatives_summary.get("basis_dislocation"):
        avoid.append("基差/资金费率偏离度上升，杠杆与执行时点应保守。")
    if derivatives_summary.get("flow_divergence"):
        avoid.append("主动流与持仓方向背离，信号确认质量下降。")

    if derivatives_summary.get("squeeze_building") or derivatives_summary.get(
        "order_flow_confirmed"
    ):
        next_actions.append("跟踪主动流和持仓量是否持续确认挤压设置。")
    if derivatives_summary.get("crowded_long"):
        next_actions.append("等拥挤度或资金费率回落后再考虑加仓动量多单。")
    if derivatives_summary.get("basis_dislocation"):
        next_actions.append("执行前先复核基差和资金费率偏离，避免被动成交价差过大。")

    ai_goal = (
        f"围绕 {' / '.join(focus_symbols)} 在 {headline} 环境下，"
        f"优先验证 {' / '.join(preferred[:2] or ['核心思路'])}，"
        "并明确触发条件、失效条件与仓位约束。"
    )
    ai_brief = {
        "headline": headline,
        "goal": ai_goal,
        "planner_regime": planner_regime,
        "market_regime": headline,
        "direction_bias": direction_bias,
        "symbols": focus_symbols,
        "timeframes": research_timeframes,
        "preferred_strategy_families": preferred,
        "thesis": thesis_points[:4],
        "risk_notes": (
            avoid
            or [
                "未发现明显额外风险，但仍需做好回测与成交质量验证。"
            ]
        )[:4],
        "next_steps": next_actions[:4],
        "factor_focus": factor_focus,
        "derivatives_context": {
            "available": bool(derivatives_summary.get("available")),
            "status": str(derivatives_summary.get("status") or "missing"),
            "provider": str(derivatives_summary.get("provider") or "coinglass"),
            "freshness_sec": derivatives_freshness,
            "dataset_count": int(derivatives_summary.get("dataset_count") or 0),
            "history_ready": bool(derivatives_summary.get("history_ready")),
            "history_interval": derivatives_summary.get("history_interval"),
            "funding_zscore": funding_zscore,
            "derivatives_labels": derivatives_labels,
        },
    }
    ai_brief["prompt_context"] = "\n".join(
        [
            f"研究任务：{ai_goal}",
            f"市场状态：{headline} / {direction_bias}",
            f"关注标的：{' / '.join(ai_brief['symbols'])}",
            f"观察周期：{' / '.join(ai_brief['timeframes'])}",
            f"优先策略：{' / '.join(preferred)}",
            f"衍生品快照：{ai_brief['derivatives_context']['status']} / {ai_brief['derivatives_context']['provider']} / {ai_brief['derivatives_context']['dataset_count']} 个数据集",
            f"研究观察：{'；'.join(ai_brief['thesis'])}",
            f"风险提示：{'；'.join(ai_brief['risk_notes'])}",
            f"下一步：{'；'.join(ai_brief['next_steps'])}",
        ]
    )

    action_items: List[Dict[str, Any]] = [
        {
            "id": "prefill_ai_research",
            "kind": "ai_prefill",
            "label": "填入 AI 研究器",
            "description": "把市场状态、币种、周期和风险约束写入 AI 研究页面。",
            "tone": "primary",
            "params": {
                "goal": ai_brief["prompt_context"],
                "regime": planner_regime,
                "symbols": focus_symbols,
                "timeframes": research_timeframes,
                "brief": ai_brief,
            },
        }
    ]

    if focus_symbols:
        backtest_params = {
            "exchange": profile.exchange,
            "symbol": focus_symbols[0],
            "symbols": focus_symbols,
            "timeframe": profile.timeframe,
            "strategy_type": backtest_strategy["strategy_type"],
        }
        action_items.append(
            {
                "id": "open_backtest_focus_symbol",
                "kind": "backtest",
                "label": f"回测 {focus_symbols[0]} {backtest_strategy['label']}策略",
                "description": f"跳转到回测页并预填 {focus_symbols[0]} / {profile.timeframe}。",
                "tone": "positive",
                "params": backtest_params,
            }
        )
        jump_targets.append(
            {
                "label": f"回测 {focus_symbols[0]} {backtest_strategy['label']}策略",
                "target": "backtest",
                "params": backtest_params,
            }
        )

    if not top_symbols:
        action_items.append(
            {
                "id": "refresh_factor_module",
                "kind": "module",
                "label": "刷新因子风格",
                "description": "当前还没有清晰的优先币种，先补齐因子排序。",
                "tone": "neutral",
                "module": "factors",
            }
        )
    if int(cross_asset.get("count") or 0) < 3:
        action_items.append(
            {
                "id": "refresh_cross_asset_module",
                "kind": "module",
                "label": "补齐多币种覆盖",
                "description": "当前横截面线索偏少，先刷新多币种轮动面板。",
                "tone": "neutral",
                "module": "cross_asset",
            }
        )
    if bool(onchain.get("degraded")) or not _news_summary_has_usable_samples(
        news_summary
    ):
        action_items.append(
            {
                "id": "refresh_onchain_module",
                "kind": "module",
                "label": "刷新链上数据",
                "description": "链上/新闻样本不足，先刷新外生数据模块。",
                "tone": "warn",
                "module": "onchain",
            }
        )

    insight_cards: List[Dict[str, Any]] = []
    if factor_focus:
        insight_cards.append(
            {
                "title": "因子观察",
                "tone": "neutral",
                "body": " / ".join(
                    f"{item['symbol']} 评分 {item['score']:.2f}"
                    + (
                        f" | 动量 {item['momentum']:.2f}"
                        if item["momentum"]
                        else ""
                    )
                    for item in factor_focus
                ),
            }
        )
    insight_cards.extend(
        {"title": "下一步", "tone": "positive", "body": text}
        for text in next_actions[:4]
    )
    insight_cards.extend(
        {"title": "风险提示", "tone": "warn", "body": text} for text in avoid[:4]
    )
    insight_cards.extend(
        {"title": "研究观察", "tone": "neutral", "body": text} for text in thesis_points[:4]
    )

    return {
        "direction_bias": direction_bias,
        "preferred_strategy_families": preferred,
        "avoid_conditions": avoid,
        "next_actions": next_actions,
        "backtest_jump_targets": jump_targets,
        "action_items": action_items[:4],
        "insight_cards": insight_cards[:8],
        "ai_brief": ai_brief,
        "factor_focus": factor_focus,
        "source_meta": factor_source_meta,
        "focus_symbols": focus_symbols,
        "headline": headline,
        "generated_at": _now_iso(),
    }


async def _get_research_workbench_context(exchange: str = "binance") -> Dict[str, Any]:
    symbols = await get_research_symbols(exchange=exchange)
    analytics_history_status = _analytics_status_collectors_to_map(
        await get_analytics_history_status(exchange=exchange, symbol="BTC/USDT")
    )
    available_symbols = list(symbols.get("symbols") or [])
    default_symbols = available_symbols[:30] or list(_DEFAULT_UNIVERSE)
    profile = _normalize_profile(
        ResearchProfile(
            exchange=exchange,
            primary_symbol=default_symbols[0] if default_symbols else "BTC/USDT",
            universe_symbols=default_symbols,
            timeframe="5m",
            lookback=1200,
            exclude_retired=True,
            horizon="short_intraday",
        )
    )
    return {
        "profile": profile.model_dump(),
        "available_symbols": available_symbols,
        "defaults": {"overview_days": 3, "calendar_days": 7, "news_hours": 24},
        "available_modules": list(_MODULE_ORDER),
        "data_status": {
            "news_events_available": True,
            "analytics_history_collectors": analytics_history_status,
        },
        "generated_at": _now_iso(),
    }


async def _run_research_workbench_overview(
    payload: ResearchWorkbenchRequest,
) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    module_names = list(_MODULE_ORDER)
    module_tasks = [_capture_module_build(name, profile) for name in module_names]
    module_results = await asyncio.gather(*module_tasks, return_exceptions=True)

    modules: Dict[str, Any] = {}
    for name, result in zip(module_names, module_results):
        if isinstance(result, Exception):
            modules[name] = _module_result(
                name,
                status="error",
                source_labels=[f"research.workbench.{name}"],
                warnings=[str(result)],
                summary={"headline": f"{name} failed", "error": str(result)},
                payload={"error": str(result)},
            )
            continue
        modules[name] = dict(result or {})
    ok_count = len(
        [module for module in modules.values() if module.get("status") == "ok"]
    )
    degraded_count = len(
        [module for module in modules.values() if module.get("status") == "degraded"]
    )
    warnings: List[str] = []
    for module in modules.values():
        warnings.extend(module.get("warnings") or [])
    regime = dict(
        _extract_module_payload(modules.get("market_state")).get("regime") or {}
    )
    return {
        "profile": profile.model_dump(),
        "market_regime": regime.get("regime") or "pending_confirmation",
        "direction_bias": regime.get("bias") or "neutral",
        "confidence": float(regime.get("confidence") or 0.0),
        "coverage": {
            "ok_count": ok_count,
            "degraded_count": degraded_count,
            "total": len(modules),
        },
        "warnings": warnings[:12],
        "modules": modules,
        "generated_at": _now_iso(),
    }


async def _run_research_workbench_module(
    module_name: str, payload: ResearchWorkbenchRequest
) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    return await _capture_module_build(module_name, profile)


async def _get_research_workbench_recommendations(
    payload: ResearchRecommendationRequest,
) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    modules = dict(payload.modules or {})
    overview = dict(payload.overview or {})
    return _build_structured_recommendations(profile, modules, overview)


@router.get("/workbench/context")
async def get_research_workbench_context(exchange: str = "binance") -> Dict[str, Any]:
    return await _get_research_workbench_context(exchange)


@router.get("/market-state")
async def get_market_state_snapshot(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
) -> Dict[str, Any]:
    profile = ResearchProfile(exchange=exchange, primary_symbol=_normalize_symbol(symbol))
    module = await _capture_module_build("market_state", profile)
    payload = _extract_module_payload(module)
    regime = dict(payload.get("regime") or {})
    return {
        "exchange": exchange,
        "symbol": _normalize_symbol(symbol),
        "market_state": regime,
        "data_manifest": regime.get("data_manifest") or [],
        "module": module,
        "generated_at": _now_iso(),
    }


@router.post("/workbench/overview")
async def run_research_workbench_overview(
    payload: ResearchWorkbenchRequest,
) -> Dict[str, Any]:
    return await _run_research_workbench_overview(payload)


@router.get("/workbench/overview")
async def run_research_workbench_overview_query(
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> Dict[str, Any]:
    profile = _build_profile_from_query(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe_symbols,
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=exclude_retired,
        horizon=horizon,
    )
    return await _run_research_workbench_overview(
        ResearchWorkbenchRequest(profile=profile)
    )


@router.post("/workbench/modules/{module_name}")
async def run_research_workbench_module(
    module_name: str, payload: ResearchWorkbenchRequest
) -> Dict[str, Any]:
    return await _run_research_workbench_module(module_name, payload)


@router.get("/workbench/modules/{module_name}")
async def run_research_workbench_module_query(
    module_name: str,
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> Dict[str, Any]:
    profile = _build_profile_from_query(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe_symbols,
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=exclude_retired,
        horizon=horizon,
    )
    return await _run_research_workbench_module(
        module_name, ResearchWorkbenchRequest(profile=profile)
    )


@router.post("/workbench/recommendations")
async def get_research_workbench_recommendations(
    payload: ResearchRecommendationRequest,
) -> Dict[str, Any]:
    return await _get_research_workbench_recommendations(payload)


@router.get("/workbench/regime-calendar")
async def get_regime_calendar(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    days: int = 7,
) -> Dict[str, Any]:
    """Return a daily market-regime timeline for the past N days.

    Uses stored microstructure + community snapshots to reconstruct the
    intraday regime label for each calendar day.
    """
    from sqlalchemy import select as _sel

    from config.database import AnalyticsMicrostructureSnapshot
    from config.database import async_session_maker as _asm

    days = max(1, min(int(days), 30))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    sym_key = _normalize_symbol(symbol)

    try:
        async with _asm() as session:
            micro_stmt = (
                _sel(AnalyticsMicrostructureSnapshot)
                .where(
                    AnalyticsMicrostructureSnapshot.exchange == exchange,
                    AnalyticsMicrostructureSnapshot.symbol == sym_key,
                    AnalyticsMicrostructureSnapshot.timestamp >= since,
                    AnalyticsMicrostructureSnapshot.capture_status.in_(
                        ["ok", "degraded"]
                    ),
                )
                .order_by(AnalyticsMicrostructureSnapshot.timestamp.asc())
            )
            micro_rows = (await session.execute(micro_stmt)).scalars().all()
    except Exception as exc:
        return {"calendar": [], "error": str(exc), "generated_at": _now_iso()}

    # Group by calendar date (UTC)
    from collections import defaultdict

    daily: Dict[str, list] = defaultdict(list)
    for row in micro_rows:
        ts = row.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        date_str = ts.strftime("%Y-%m-%d")
        daily[date_str].append(row)

    # "ok" snapshots are trusted fully; "degraded" ones still carry signal but
    # are noisier, so they contribute at reduced weight to the daily averages.
    _DEGRADED_WEIGHT = 0.5

    def _weighted_avg(pairs: list) -> "float | None":
        # pairs: list of (value, weight); ignores None values.
        num = 0.0
        den = 0.0
        for value, weight in pairs:
            if value is None or weight <= 0:
                continue
            num += float(value) * weight
            den += weight
        return (num / den) if den > 0 else None

    calendar = []
    for date_str in sorted(daily.keys()):
        rows = daily[date_str]

        def _row_weight(r) -> float:
            return _DEGRADED_WEIGHT if str(getattr(r, "capture_status", "") or "") == "degraded" else 1.0

        ok_count = sum(1 for r in rows if str(getattr(r, "capture_status", "") or "") == "ok")
        degraded_count = len(rows) - ok_count

        # Quality-weighted daily aggregates.
        avg_imbalance = _weighted_avg([(r.order_flow_imbalance, _row_weight(r)) for r in rows]) or 0.0
        avg_funding = _weighted_avg([(r.funding_rate, _row_weight(r)) for r in rows])
        avg_basis = _weighted_avg([(r.basis_pct, _row_weight(r)) for r in rows])
        avg_spread = _weighted_avg([(r.spread_bps, _row_weight(r)) for r in rows]) or 0.0

        # A day reconstructed solely from degraded snapshots is lower-trust.
        data_quality = "degraded" if ok_count == 0 and degraded_count > 0 else "ok"
        from core.market_state.classifier import classify_daily_regime

        daily_regime = classify_daily_regime(
            avg_imbalance=avg_imbalance,
            avg_spread_bps=avg_spread,
            data_quality=data_quality,
        )
        regime = str(daily_regime.get("regime") or "low_info_range")
        bias = str(daily_regime.get("bias") or "neutral")

        calendar.append(
            {
                "date": date_str,
                "regime": regime,
                "bias": bias,
                "uncertainty": daily_regime.get("uncertainty"),
                "risk_posture": daily_regime.get("risk_posture"),
                "classification_margin": daily_regime.get("classification_margin"),
                "hysteresis_state": daily_regime.get("hysteresis_state"),
                "avg_imbalance": round(avg_imbalance, 4),
                "avg_funding": round(avg_funding, 6)
                if avg_funding is not None
                else None,
                "avg_basis": round(avg_basis, 4) if avg_basis is not None else None,
                "avg_spread_bps": round(avg_spread, 2),
                "snapshot_count": len(rows),
                "ok_count": ok_count,
                "degraded_count": degraded_count,
                "data_quality": data_quality,
            }
        )

    return {
        "symbol": sym_key,
        "exchange": exchange,
        "days": days,
        "calendar": calendar,
        "generated_at": _now_iso(),
    }
