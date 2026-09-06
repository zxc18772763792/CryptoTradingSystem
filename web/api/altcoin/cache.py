"""Scan-result cache primitives (keying, single-flight payload assembly, TTL).

Pure cache machinery lifted out of the monolith. The single-flight *refresh*
task machinery stays in the package __init__ because it calls the scan-compute
hot path; these primitives have no dependency on it. Re-exported from __init__
to preserve the altcoin_api.<name> surface.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any, Dict, Mapping, Optional, Sequence

from .helpers import (
    _clone_payload,
    _hash_universe,
    _normalize_exchange,
    _normalize_mode,
    _normalize_symbols,
    _normalize_timeframe,
    _normalize_universe_scope,
    _normalize_view,
    _safe_float,
)
from .state import (
    _ALTCOIN_SCAN_CACHE,
    _ALTCOIN_SCAN_LOCKS,
    _ALTCOIN_SCAN_LOCKS_GUARD,
    _ALTCOIN_SCAN_REFRESH_TASKS,
)


def build_altcoin_notification_config_key(
    *,
    exchange: str,
    timeframe: str,
    universe_symbols: Sequence[str],
    exclude_retired: bool = True,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
) -> str:
    payload = {
        "exchange": _normalize_exchange(exchange),
        "timeframe": _normalize_timeframe(timeframe),
        "universe_symbols": _normalize_symbols(universe_symbols),
        "exclude_retired": bool(exclude_retired),
        "mode": _normalize_mode(mode),
        "view": _normalize_view(view) if view else "",
        "universe_scope": _normalize_universe_scope(universe_scope),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_key(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
) -> str:
    universe_hash = _hash_universe(symbols)
    return (
        f"{_normalize_exchange(exchange)}"
        f"|{_normalize_timeframe(timeframe)}"
        f"|{universe_hash}"
        f"|{bool(exclude_retired)}"
        f"|{_normalize_mode(mode)}"
        f"|{_normalize_view(view) if view else 'none'}"
        f"|{_normalize_universe_scope(universe_scope)}"
    )


def _cache_lock(cache_key: str) -> asyncio.Lock:
    with _ALTCOIN_SCAN_LOCKS_GUARD:
        lock = _ALTCOIN_SCAN_LOCKS.get(cache_key)
        if lock is None:
            lock = asyncio.Lock()
            _ALTCOIN_SCAN_LOCKS[cache_key] = lock
        return lock


def _cache_age_sec(cached_entry: Optional[Mapping[str, Any]], now_ts: Optional[float] = None) -> float:
    if not cached_entry:
        return 0.0
    current_ts = float(now_ts if now_ts is not None else time.time())
    return max(0.0, current_ts - float(cached_entry.get("stored_at", 0.0)))


def _build_cached_scan_payload(
    *,
    cached_entry: Mapping[str, Any],
    cache_key: str,
    ttl: float,
    now_ts: Optional[float] = None,
    stale: bool = False,
    refreshing: bool = False,
    served_mode: str = "cache_hit",
    warnings: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    payload = _clone_payload(cached_entry.get("payload") or {})
    merged_warnings = [
        str(item)
        for item in list(payload.get("warnings") or []) + list(warnings or [])
        if str(item or "").strip()
    ]
    payload["warnings"] = list(dict.fromkeys(merged_warnings))
    payload["cache"] = {
        "cache_key": cache_key,
        "hit": True,
        "age_sec": round(_cache_age_sec(cached_entry, now_ts), 3),
        "ttl_sec": ttl,
        "stale": bool(stale),
        "refreshing": bool(refreshing),
        "served_mode": str(served_mode or "cache_hit"),
    }
    return payload


def _finalize_scan_payload(
    *,
    payload: Mapping[str, Any],
    cache_key: str,
    ttl: float,
    mode: str,
    view: str,
    timeframe: str,
    pre_warnings: Sequence[str],
    cache_hit: bool,
    age_sec: float = 0.0,
    stale: bool = False,
    refreshing: bool = False,
    served_mode: str = "live_compute",
) -> Dict[str, Any]:
    final_payload = _clone_payload(payload)
    final_payload["mode"] = _normalize_mode(mode)
    final_payload["view"] = (_normalize_view(view) if view else "") or _normalize_timeframe(timeframe)
    merged_warnings = [
        str(item)
        for item in list(pre_warnings or []) + list(final_payload.get("warnings") or [])
        if str(item or "").strip()
    ]
    final_payload["warnings"] = list(dict.fromkeys(merged_warnings))
    final_payload["cache"] = {
        "cache_key": cache_key,
        "hit": bool(cache_hit),
        "age_sec": round(float(age_sec or 0.0), 3),
        "ttl_sec": ttl,
        "stale": bool(stale),
        "refreshing": bool(refreshing),
        "served_mode": str(served_mode or ("cache_hit" if cache_hit else "live_compute")),
    }
    return final_payload


def _should_cache_scan_payload(payload: Mapping[str, Any]) -> bool:
    rows = list(payload.get("rows") or [])
    if not rows:
        return False
    for row in rows:
        freshness = dict((row or {}).get("freshness") or {})
        data_quality = dict((row or {}).get("data_quality") or {})
        if bool(freshness.get("using_market_snapshot") or data_quality.get("using_market_snapshot")):
            return True
        market_freshness = _safe_float(data_quality.get("market_data_freshness"), 0.0)
        if market_freshness >= 0.45:
            return True
    return False


def _clear_altcoin_scan_cache() -> None:
    # Do NOT cancel in-flight refresh tasks: requests awaiting them via
    # asyncio.shield would surface CancelledError as user-visible 500s (shield
    # only protects against caller cancellation, not inner-task cancellation).
    # Detaching them from the registry is enough — they finish, write a stale
    # cache entry keyed by the old universe hash, and eviction reclaims it.
    _ALTCOIN_SCAN_REFRESH_TASKS.clear()
    _ALTCOIN_SCAN_CACHE.clear()


_ALTCOIN_SCAN_CACHE_MAX_ENTRIES = 48


def _evict_altcoin_scan_cache() -> None:
    # Cache keys include the universe hash, so key churn (watchlist edits,
    # symbol-set tweaks) grows the dict unboundedly on a long-lived process.
    if len(_ALTCOIN_SCAN_CACHE) <= _ALTCOIN_SCAN_CACHE_MAX_ENTRIES:
        return
    oldest_first = sorted(
        _ALTCOIN_SCAN_CACHE.items(), key=lambda kv: float((kv[1] or {}).get("stored_at", 0.0))
    )
    for key, _ in oldest_first[: len(_ALTCOIN_SCAN_CACHE) - _ALTCOIN_SCAN_CACHE_MAX_ENTRIES]:
        _ALTCOIN_SCAN_CACHE.pop(key, None)
    _ALTCOIN_SCAN_CACHE.clear()
