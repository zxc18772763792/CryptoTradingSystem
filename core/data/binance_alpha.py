"""Binance Alpha public market-data adapter.

The Alpha API is a discovery surface for on-chain tokens.  It is deliberately
kept separate from the normal Binance spot/futures symbol universe because an
Alpha token's public symbol is not necessarily a tradable CEX symbol; the
stable identifier for market-data calls is ``alphaId`` (for example,
``ALPHA_175``).

This module provides three small, research-friendly primitives:

* a short-lived in-memory/disk cache for the official Token List;
* conversion of Alpha token metadata into the radar's market-snapshot shape;
* append-only local snapshots for later offline research.

No trading endpoint is used here and no Alpha token is treated as a buy
signal merely because it appears in the list.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import httpx

from config.settings import settings
from core.utils.shared_ssl import get_shared_ssl_context


ALPHA_BASE_URL = "https://www.binance.com"
ALPHA_TOKEN_LIST_PATH = "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
ALPHA_TOKEN_LIST_URL = f"{ALPHA_BASE_URL}{ALPHA_TOKEN_LIST_PATH}"
ALPHA_SOURCE_NAME = "binance_alpha"
ALPHA_CACHE_TTL_SEC = 300.0
ALPHA_FORCE_REFRESH_DEDUP_SEC = 15.0

_CACHE: Dict[str, Any] = {"payload": None, "stored_at": 0.0}
_CACHE_LOCK = threading.RLock()
_PERSIST_LOCK = threading.RLock()
_INFLIGHT: Optional[asyncio.Task] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: Optional[datetime] = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _alpha_root() -> Path:
    return Path(settings.BASE_DIR) / "data" / "research" / "binance_alpha"


def alpha_catalog_path() -> Path:
    return _alpha_root() / "token_catalog.json"


def alpha_snapshot_history_path() -> Path:
    return _alpha_root() / "token_snapshots.jsonl"


def _alpha_enabled() -> bool:
    return bool(getattr(settings, "BINANCE_ALPHA_ENABLED", True))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if math.isfinite(number) else float(default)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return int(default)
    return number


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _history_max_bytes() -> int:
    return max(
        16,
        int(getattr(settings, "BINANCE_ALPHA_HISTORY_MAX_MB", 256) or 256),
    ) * 1024 * 1024


def _extract_token_list(payload: Any) -> List[Dict[str, Any]]:
    """Extract tokens from either the official response or our disk envelope."""
    if isinstance(payload, dict):
        data = payload.get("tokens")
        if not isinstance(data, list) or not data:
            data = payload.get("data")
    else:
        data = payload
    if not isinstance(data, list):
        return []
    tokens: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, Mapping):
            continue
        alpha_id = _clean_text(item.get("alphaId"))
        if not alpha_id:
            continue
        tokens.append(dict(item))
    return tokens


def _load_disk_payload() -> Optional[Dict[str, Any]]:
    path = alpha_catalog_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict) or not _extract_token_list(payload):
        return None
    return payload


def _payload_age_sec(payload: Mapping[str, Any]) -> Optional[float]:
    updated_at = _clean_text(payload.get("updated_at"))
    if not updated_at:
        return None
    try:
        parsed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (_utc_now() - parsed.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def _persist_payload(payload: Mapping[str, Any]) -> None:
    root = _alpha_root()
    root.mkdir(parents=True, exist_ok=True)
    catalog = root / "token_catalog.json"
    history = root / "token_snapshots.jsonl"
    catalog_text = json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n"
    history_line = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")) + "\n"
    with _PERSIST_LOCK:
        temp_catalog = catalog.with_name(f"{catalog.name}.{os.getpid()}.tmp")
        temp_catalog.write_text(catalog_text, encoding="utf-8")
        os.replace(temp_catalog, catalog)

        max_history_bytes = _history_max_bytes()
        if history.exists() and history.stat().st_size + len(history_line.encode("utf-8")) > max_history_bytes:
            archive = history.with_name(f"{history.name}.1")
            if archive.exists():
                archive.unlink()
            os.replace(history, archive)
        # The current file plus one previous segment bounds raw replay storage
        # while retaining exact upstream envelopes for debugging and research.
        with history.open("a", encoding="utf-8") as handle:
            handle.write(history_line)


async def _fetch_payload() -> Dict[str, Any]:
    timeout_sec = float(getattr(settings, "BINANCE_ALPHA_TIMEOUT_SEC", 10.0) or 10.0)
    async with httpx.AsyncClient(
        timeout=max(2.0, min(timeout_sec, 30.0)),
        follow_redirects=True,
        verify=get_shared_ssl_context(),
        headers={"User-Agent": "crypto-trading-system/altcoin-radar"},
    ) as client:
        response = await client.get(ALPHA_TOKEN_LIST_URL)
        response.raise_for_status()
        raw = response.json()
    tokens = _extract_token_list(raw)
    if not tokens:
        raise RuntimeError("Binance Alpha token list returned no valid tokens")
    updated_at = _utc_iso()
    return {
        "source": ALPHA_SOURCE_NAME,
        "endpoint": ALPHA_TOKEN_LIST_URL,
        "updated_at": updated_at,
        "count": len(tokens),
        "tokens": tokens,
        "stale": False,
        "warning": "",
    }


async def load_alpha_token_catalog(*, refresh: bool = False) -> Dict[str, Any]:
    """Load Alpha Token List with safe stale-disk fallback.

    The request is shared between concurrent dashboard calls, and a stale
    on-disk catalog remains usable when Binance is unavailable.  The returned
    object is always a dictionary so callers can degrade to an empty list.
    """
    global _INFLIGHT
    if not _alpha_enabled():
        return {
            "source": ALPHA_SOURCE_NAME,
            "tokens": [],
            "count": 0,
            "stale": False,
            "warning": "Binance Alpha integration disabled by configuration",
        }

    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get("payload")
        stored_at = float(_CACHE.get("stored_at") or 0.0)
        cache_age = now - stored_at
        cache_is_fresh = cached and not bool(cached.get("stale"))
        if cached and (
            (not refresh and cache_age <= ALPHA_CACHE_TTL_SEC)
            or (refresh and cache_is_fresh and cache_age <= ALPHA_FORCE_REFRESH_DEDUP_SEC)
        ):
            return dict(cached)

    disk_payload = _load_disk_payload()
    disk_age = _payload_age_sec(disk_payload or {}) if disk_payload else None
    if disk_payload and not refresh and (disk_age is None or disk_age <= ALPHA_CACHE_TTL_SEC):
        with _CACHE_LOCK:
            _CACHE.update({"payload": dict(disk_payload), "stored_at": now})
        return dict(disk_payload)

    with _CACHE_LOCK:
        task = _INFLIGHT
        if task is None or task.done():
            task = asyncio.create_task(_fetch_payload())
            _INFLIGHT = task
    try:
        payload = await asyncio.shield(task)
        try:
            _persist_payload(payload)
        except Exception as persist_exc:
            # A read-only/temporarily full data volume must not discard a
            # valid live catalog; surface the issue while keeping the payload
            # usable for the current scan.
            payload = dict(payload)
            payload["warning"] = (
                "Binance Alpha catalog fetched but local persistence failed: "
                f"{type(persist_exc).__name__}"
            )
        with _CACHE_LOCK:
            _CACHE.update({"payload": dict(payload), "stored_at": time.time()})
        return dict(payload)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if disk_payload:
            fallback = dict(disk_payload)
            fallback["stale"] = True
            fallback["warning"] = f"Binance Alpha refresh failed; using local catalog: {type(exc).__name__}"
            with _CACHE_LOCK:
                _CACHE.update({"payload": fallback, "stored_at": time.time()})
            return fallback
        return {
            "source": ALPHA_SOURCE_NAME,
            "tokens": [],
            "count": 0,
            "stale": True,
            "warning": f"Binance Alpha catalog unavailable: {type(exc).__name__}",
        }
    finally:
        with _CACHE_LOCK:
            if _INFLIGHT is task and task.done():
                _INFLIGHT = None


def alpha_pair_from_id(alpha_id: Any) -> str:
    """Return the radar's stable synthetic pair for an Alpha identifier."""
    text = _clean_text(alpha_id).upper()
    text = re.sub(r"[^A-Z0-9]", "", text)
    return f"{text}/USDT" if text else ""


def alpha_trade_symbol_from_id(alpha_id: Any) -> str:
    """Return the official Alpha market-data symbol for an Alpha identifier.

    The radar uses a separator-free synthetic pair (``ALPHA175/USDT``), while
    Binance Alpha REST/WS endpoints expect the identifier's underscore to be
    preserved (``ALPHA_175USDT``).  Keeping this conversion explicit prevents
    accidental calls to the normal Binance spot API with a synthetic pair.
    """
    text = _clean_text(alpha_id).upper()
    text = re.sub(r"[^A-Z0-9_]", "", text)
    if not text:
        return ""
    return text if text.endswith("USDT") else f"{text}USDT"


def alpha_symbols(tokens: Iterable[Mapping[str, Any]]) -> List[str]:
    """Return stable, de-duplicated radar pairs for active Alpha tokens."""
    out: List[str] = []
    seen = set()
    for item in tokens:
        if not isinstance(item, Mapping) or _as_bool(item.get("offline")):
            continue
        pair = alpha_pair_from_id(item.get("alphaId"))
        if pair and pair not in seen:
            seen.add(pair)
            out.append(pair)
    return out


def build_alpha_market_snapshots(
    tokens: Iterable[Mapping[str, Any]],
    *,
    timestamp: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Map raw Alpha token metadata to the radar market-snapshot contract."""
    captured_at = timestamp or _utc_iso()
    snapshots: Dict[str, Dict[str, Any]] = {}
    for item in tokens:
        if not isinstance(item, Mapping) or _as_bool(item.get("offline")):
            continue
        alpha_id = _clean_text(item.get("alphaId"))
        pair = alpha_pair_from_id(alpha_id)
        if not pair:
            continue
        price = _as_float(item.get("price"), 0.0)
        if price <= 0:
            continue
        percent_24h = _as_float(item.get("percentChange24h"), 0.0)
        quote_volume = _as_float(item.get("volume24h"), 0.0)
        market_cap = _as_float(item.get("marketCap"), 0.0)
        liquidity = _as_float(item.get("liquidity"), 0.0)
        total_supply = _as_float(item.get("totalSupply"), 0.0)
        circulating_supply = _as_float(item.get("circulatingSupply"), 0.0)
        snapshots[pair] = {
            "symbol": pair,
            "base_symbol": pair.split("/", 1)[0],
            "raw_symbol": alpha_id,
            "current_price": price,
            "last_price": price,
            "price_change_percent_24h": percent_24h,
            "quote_volume_24h": quote_volume,
            "market_cap_usd": market_cap,
            "timestamp": captured_at,
            "source_name": ALPHA_SOURCE_NAME,
            "capture_status": "ok",
            "source_error": None,
            # These fields are intentionally metadata, not a bullish score.
            "alpha_context": {
                "alpha_id": alpha_id,
                "display_symbol": _clean_text(item.get("symbol")),
                "name": _clean_text(item.get("name")),
                "chain_id": _clean_text(item.get("chainId")),
                "chain_name": _clean_text(item.get("chainName")),
                "contract_address": _clean_text(item.get("contractAddress")),
                "icon_url": _clean_text(item.get("iconUrl")),
                "liquidity_usd": liquidity,
                "fdv_usd": _as_float(item.get("fdv"), 0.0),
                "holders": _as_int(item.get("holders"), 0),
                "total_supply": total_supply,
                "circulating_supply": circulating_supply,
                "circulating_ratio": (
                    circulating_supply / total_supply if total_supply > 0 else None
                ),
                "hot_tag": _as_bool(item.get("hotTag")),
                "listing_time": item.get("listingTime"),
                "count_24h": _as_int(item.get("count24h"), 0),
                "alpha_directory_score": _as_int(item.get("score"), 0),
                "percent_change_24h": percent_24h,
                "volume_24h_usd": quote_volume,
                "market_cap_usd": market_cap,
            },
        }
    return snapshots


def alpha_catalog_meta(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return safe display metadata without exposing the raw token list twice."""
    data = dict(payload or {})
    tokens = _extract_token_list(data)
    return {
        "source": data.get("source") or ALPHA_SOURCE_NAME,
        "count": len(tokens),
        "active_count": len(alpha_symbols(tokens)),
        "updated_at": data.get("updated_at"),
        "stale": bool(data.get("stale")),
        "warning": _clean_text(data.get("warning")),
        "catalog_path": str(alpha_catalog_path()),
        "history_path": str(alpha_snapshot_history_path()),
    }
