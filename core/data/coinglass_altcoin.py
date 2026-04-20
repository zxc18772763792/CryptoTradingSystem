from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import httpx
from loguru import logger

from core.data.coinglass_client import CoinglassClient, coinglass_enabled
from core.data.coinglass_registry import normalize_coinglass_symbol


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CACHE_DIR = _PROJECT_ROOT / "data" / "cache" / "coinglass_altcoin"
_MARKET_CACHE_TTL_SEC = 120.0
_UNIVERSE_CACHE_TTL_SEC = 6 * 3600.0
_UNIVERSE_REFRESH_TIMEOUT_SEC = 20.0
_TAG_CACHE_TTL_SEC = 7 * 86400.0
_DEFAULT_MARKET_PER_PAGE = 200
_DEFAULT_MARKET_MAX_PAGES = 4
_DEFAULT_TAG_SAMPLE_LIMIT = 240
_RESEARCH_MAJOR_SYMBOL_LIMIT = 10
_MAJOR_MARKET_CAP_EXCLUSION_USD = 80_000_000_000.0
_CURRENCY_PAGE_URL = "https://www.coinglass.com/currencies/{symbol}"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)

_MAJOR_BASES = {
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "TRX",
    "TON",
}

_NON_ALT_BASES = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "USDE",
    "USDS",
    "PYUSD",
    "USDP",
    "DAI",
    "PAXG",
    "XAUT",
    "WBTC",
    "WETH",
    "WEETH",
    "STETH",
    "WSTETH",
    "COIN",
}

_MARKET_CACHE: Dict[str, Dict[str, Any]] = {}
_UNIVERSE_CACHE: Dict[str, Dict[str, Any]] = {}
_TAG_CACHE_MEM: Optional[Dict[str, Any]] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: Optional[datetime] = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.isoformat()


def _clip_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:  # NaN guard
        return None
    return float(parsed)


def _normalize_exchange(exchange: str) -> str:
    text = str(exchange or "binance").strip().lower()
    if not text:
        return "binance"
    aliases = {
        "binanceusdm": "binance",
        "binance-futures": "binance",
        "gateio": "gate",
        "gate.io": "gate",
    }
    return aliases.get(text, text)


def _coinglass_exchange(exchange: str) -> str:
    normalized = _normalize_exchange(exchange)
    mapping = {
        "binance": "Binance",
        "gate": "Gate",
    }
    return mapping.get(normalized, normalized.capitalize())


def _symbol_base(symbol: str) -> str:
    return normalize_coinglass_symbol(symbol)


def _pair_symbol(symbol: str) -> str:
    base = _symbol_base(symbol)
    return f"{base}/USDT" if base else ""


def is_alt_candidate_symbol(symbol: str, market_cap_usd: Optional[float] = None) -> bool:
    base = _symbol_base(symbol)
    if not base:
        return False
    if re.match(r"^\d", base):
        return False
    if base in _MAJOR_BASES:
        return False
    if base in _NON_ALT_BASES:
        return False
    market_cap = _clip_float(market_cap_usd)
    if market_cap is not None and market_cap >= _MAJOR_MARKET_CAP_EXCLUSION_USD:
        return False
    return True


def is_research_universe_symbol(symbol: str, market_cap_usd: Optional[float] = None) -> bool:
    base = _symbol_base(symbol)
    if not base:
        return False
    if re.match(r"^\d", base):
        return False
    if base in _NON_ALT_BASES:
        return False
    market_cap = _clip_float(market_cap_usd)
    if market_cap is not None and market_cap <= 0:
        return False
    return True


def _major_market_cap_symbols(
    market_ranked: Sequence[Mapping[str, Any]],
    *,
    limit: int = _RESEARCH_MAJOR_SYMBOL_LIMIT,
) -> List[str]:
    selected: List[str] = []
    seen: set[str] = set()
    for item in market_ranked:
        symbol = str((item or {}).get("symbol") or "").strip()
        if not symbol or symbol in seen:
            continue
        if not is_research_universe_symbol(
            str((item or {}).get("base_symbol") or symbol),
            _clip_float((item or {}).get("market_cap_usd")),
        ):
            continue
        selected.append(symbol)
        seen.add(symbol)
        if len(selected) >= max(1, int(limit or _RESEARCH_MAJOR_SYMBOL_LIMIT)):
            break
    return selected


def _market_cache_key(exchange: str) -> str:
    return _normalize_exchange(exchange)


def _universe_cache_key(exchange: str) -> str:
    return _normalize_exchange(exchange)


def _ensure_cache_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _tag_cache_path() -> Path:
    _ensure_cache_dir()
    return _CACHE_DIR / "currency_tags.json"


def _universe_cache_path(exchange: str) -> Path:
    _ensure_cache_dir()
    return _CACHE_DIR / f"altcoin_universe_{_normalize_exchange(exchange)}.json"


def _load_universe_cache_payload(exchange: str) -> Optional[Dict[str, Any]]:
    path = _universe_cache_path(exchange)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return dict(payload) if isinstance(payload, dict) else None


def _universe_cache_age_sec(payload: Mapping[str, Any]) -> Optional[float]:
    updated_at = payload.get("updated_at")
    if not updated_at:
        return None
    try:
        return max(0.0, (_utc_now() - datetime.fromisoformat(str(updated_at))).total_seconds())
    except Exception:
        return None


def _build_universe_stale_fallback(
    payload: Mapping[str, Any],
    *,
    exchange: str,
    reason: str,
) -> Dict[str, Any]:
    fallback = dict(payload or {})
    warnings = [
        str(item).strip()
        for item in list(fallback.get("warnings") or [])
        if str(item).strip()
    ]
    warning = f"coinglass universe refresh fallback for {exchange}: {reason}"
    if warning not in warnings:
        warnings.append(warning)
    fallback["warnings"] = warnings
    fallback["stale_fallback"] = True
    fallback["exchange"] = exchange
    fallback.setdefault("source", "coinglass_altcoin_universe")
    return fallback


def _load_tag_cache() -> Dict[str, Any]:
    global _TAG_CACHE_MEM
    if _TAG_CACHE_MEM is not None:
        return _TAG_CACHE_MEM
    path = _tag_cache_path()
    if not path.exists():
        _TAG_CACHE_MEM = {"symbols": {}}
        return _TAG_CACHE_MEM
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {"symbols": {}}
    if not isinstance(payload, dict):
        payload = {"symbols": {}}
    payload.setdefault("symbols", {})
    _TAG_CACHE_MEM = payload
    return payload


def _save_tag_cache(payload: Mapping[str, Any]) -> None:
    global _TAG_CACHE_MEM
    data = dict(payload or {})
    data.setdefault("symbols", {})
    data["updated_at"] = _utc_iso()
    path = _tag_cache_path()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _TAG_CACHE_MEM = data


def _cached_tags(symbol: str, *, refresh: bool) -> Optional[List[str]]:
    if refresh:
        return None
    cache = _load_tag_cache().get("symbols") or {}
    entry = cache.get(_symbol_base(symbol))
    if not isinstance(entry, dict):
        return None
    updated_at = entry.get("updated_at")
    if updated_at:
        try:
            age_sec = max(0.0, (_utc_now() - datetime.fromisoformat(str(updated_at))).total_seconds())
            if age_sec > _TAG_CACHE_TTL_SEC:
                return None
        except Exception:
            return None
    tags = entry.get("tags")
    if not isinstance(tags, list):
        return None
    return [str(item).strip().lower() for item in tags if str(item).strip()]


def _remember_tags(symbol: str, tags: Sequence[str]) -> None:
    cache = _load_tag_cache()
    symbols = cache.setdefault("symbols", {})
    symbols[_symbol_base(symbol)] = {
        "updated_at": _utc_iso(),
        "tags": [str(item).strip().lower() for item in tags if str(item).strip()],
    }
    _save_tag_cache(cache)


def _extract_tags_from_currency_html(symbol: str, html: str) -> List[str]:
    base = re.escape(_symbol_base(symbol))
    patterns = [
        rf'"symbol":"{base}".{{0,6000}}?"tags":(\[[^\]]*\])',
        rf'"tags":(\[[^\]]*\]).{{0,6000}}?"symbol":"{base}"',
        r'"tags":(\[[^\]]*\])',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.S)
        if not match:
            continue
        raw = match.group(1)
        try:
            tags = json.loads(raw)
        except Exception:
            continue
        if isinstance(tags, list):
            return [str(item).strip().lower() for item in tags if str(item).strip()]
    return []


async def _fetch_currency_tags_uncached(symbol: str, client: httpx.AsyncClient) -> List[str]:
    base = _symbol_base(symbol)
    if not base:
        return []
    url = _CURRENCY_PAGE_URL.format(symbol=base)
    response = await client.get(url)
    response.raise_for_status()
    return _extract_tags_from_currency_html(base, response.text)


async def load_currency_tags(symbols: Iterable[str], *, refresh: bool = False) -> Dict[str, List[str]]:
    requested = []
    seen = set()
    for symbol in symbols:
        base = _symbol_base(symbol)
        if not base or base in seen:
            continue
        seen.add(base)
        requested.append(base)

    results: Dict[str, List[str]] = {}
    missing: List[str] = []
    for symbol in requested:
        cached = _cached_tags(symbol, refresh=refresh)
        if cached is not None:
            results[symbol] = cached
        else:
            missing.append(symbol)

    if not missing:
        return results

    semaphore = asyncio.Semaphore(8)

    async def _worker(symbol: str) -> None:
        async with semaphore:
            try:
                tags = await _fetch_currency_tags_uncached(symbol, client)
            except Exception as exc:
                logger.debug(f"coinglass_altcoin: tag fetch failed for {symbol}: {exc}")
                tags = []
            _remember_tags(symbol, tags)
            results[symbol] = tags

    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers={"user-agent": _USER_AGENT},
    ) as client:
        await asyncio.gather(*(_worker(symbol) for symbol in missing))
    return results


def _build_market_snapshot(row: Mapping[str, Any], exchange: str, *, timestamp: str, latency_ms: int) -> Dict[str, Any]:
    base_symbol = _symbol_base(str(row.get("symbol") or ""))
    pair_symbol = _pair_symbol(base_symbol)
    snapshot = {str(key): value for key, value in dict(row or {}).items()}
    snapshot.update(
        {
            "symbol": pair_symbol,
            "raw_symbol": str(row.get("symbol") or ""),
            "base_symbol": base_symbol,
            "exchange": _normalize_exchange(exchange),
            "timestamp": timestamp,
            "source_name": "coinglass_coins_markets",
            "capture_status": "ok",
            "source_error": None,
            "latency_ms": latency_ms,
        }
    )
    return snapshot


async def load_coinglass_market_snapshots(
    exchange: str,
    *,
    symbols: Optional[Sequence[str]] = None,
    refresh: bool = False,
    manual: bool = False,
    per_page: int = _DEFAULT_MARKET_PER_PAGE,
    max_pages: int = _DEFAULT_MARKET_MAX_PAGES,
) -> Dict[str, Dict[str, Any]]:
    normalized_exchange = _normalize_exchange(exchange)
    cache_key = _market_cache_key(normalized_exchange)
    requested_symbols = {_pair_symbol(symbol) for symbol in (symbols or []) if _pair_symbol(symbol)}
    cached = _MARKET_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and not refresh and (now_ts - float(cached.get("stored_at", 0.0))) <= _MARKET_CACHE_TTL_SEC:
        rows = dict(cached.get("rows") or {})
        if requested_symbols:
            return {symbol: row for symbol, row in rows.items() if symbol in requested_symbols}
        return rows

    if not coinglass_enabled():
        return {}

    rows: Dict[str, Dict[str, Any]] = {}
    exchange_name = _coinglass_exchange(normalized_exchange)
    async with CoinglassClient() as client:
        for page in range(1, max(1, int(max_pages or 1)) + 1):
            response = await client._request_json(
                "/api/futures/coins-markets",
                params={
                    "exchange_list": exchange_name,
                    "per_page": str(max(20, int(per_page or _DEFAULT_MARKET_PER_PAGE))),
                    "page": str(page),
                },
                manual=manual,
            )
            payload = dict(response.get("payload") or {})
            items = payload.get("data") or []
            captured_at = _utc_iso()
            latency_ms = int(response.get("latency_ms") or 0)
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                snapshot = _build_market_snapshot(item, normalized_exchange, timestamp=captured_at, latency_ms=latency_ms)
                if snapshot["symbol"]:
                    rows[snapshot["symbol"]] = snapshot
            if requested_symbols and requested_symbols.issubset(rows.keys()):
                break
            if len(items) < max(20, int(per_page or _DEFAULT_MARKET_PER_PAGE)):
                break

    _MARKET_CACHE[cache_key] = {"stored_at": now_ts, "rows": rows}
    if requested_symbols:
        return {symbol: row for symbol, row in rows.items() if symbol in requested_symbols}
    return rows


async def load_exchange_onboard_map(
    exchange: str,
    *,
    refresh: bool = False,
    manual: bool = False,
) -> Dict[str, Dict[str, Any]]:
    normalized_exchange = _normalize_exchange(exchange)
    cache_key = f"onboard:{normalized_exchange}"
    cached = _MARKET_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and not refresh and (now_ts - float(cached.get("stored_at", 0.0))) <= _UNIVERSE_CACHE_TTL_SEC:
        return dict(cached.get("rows") or {})

    if not coinglass_enabled():
        return {}

    exchange_name = _coinglass_exchange(normalized_exchange)
    async with CoinglassClient() as client:
        response = await client._request_json(
            "/api/futures/supported-exchange-pairs",
            params={"exchange": exchange_name},
            manual=manual,
        )
    payload = dict(response.get("payload") or {})
    data = payload.get("data") or {}
    items = data.get(exchange_name) if isinstance(data, Mapping) else []
    onboard_map: Dict[str, Dict[str, Any]] = {}
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        base = _symbol_base(str(item.get("base_asset") or item.get("symbol") or ""))
        onboard_date = item.get("onboard_date")
        if not base or onboard_date in (None, ""):
            continue
        numeric = _clip_float(onboard_date)
        if numeric is None:
            continue
        if numeric > 1_000_000_000_000:
            numeric = numeric / 1000.0
        current = onboard_map.get(base)
        if current is None or numeric < float(current.get("onboard_ts") or numeric):
            onboard_map[base] = {
                "symbol": _pair_symbol(base),
                "base_symbol": base,
                "onboard_ts": float(numeric),
                "onboard_date": datetime.fromtimestamp(float(numeric), tz=timezone.utc).isoformat(),
            }

    _MARKET_CACHE[cache_key] = {"stored_at": now_ts, "rows": onboard_map}
    return onboard_map


def build_derivatives_snapshot_from_market_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    def _value(name: str) -> Optional[float]:
        return _clip_float(snapshot.get(name))

    def _imbalance(window: str) -> float:
        long_value = _value(f"long_volume_usd_{window}") or 0.0
        short_value = _value(f"short_volume_usd_{window}") or 0.0
        total = long_value + short_value
        if total <= 0:
            return 0.0
        return (long_value - short_value) / total

    def _clamp01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    funding_rate = _value("avg_funding_rate_by_oi")
    long_short_ratio = _value("long_short_ratio_4h")
    oi_change_1h = _value("open_interest_change_percent_1h")
    oi_change_24h = _value("open_interest_change_percent_24h")
    volume_4h = (_value("long_volume_usd_4h") or 0.0) + (_value("short_volume_usd_4h") or 0.0)
    long_liq_24h = _value("long_liquidation_usd_24h") or 0.0
    short_liq_24h = _value("short_liquidation_usd_24h") or 0.0
    liq_total_24h = long_liq_24h + short_liq_24h
    imbalance_4h = _imbalance("4h")

    crowding_score = _clamp01(
        min(abs(funding_rate or 0.0) / 0.0035, 1.0) * 0.40
        + min(max((long_short_ratio or 1.0) - 1.0, 0.0) / 0.30, 1.0) * 0.30
        + min(abs(oi_change_24h or 0.0) / 25.0, 1.0) * 0.30
    )
    squeeze_score = _clamp01(
        min(max(oi_change_1h or 0.0, 0.0) / 10.0, 1.0) * 0.35
        + min(max(imbalance_4h, 0.0), 1.0) * 0.35
        + min((short_liq_24h / max(liq_total_24h, 1.0)), 1.0) * 0.30
    )
    distribution_score = _clamp01(
        min((long_liq_24h / max(liq_total_24h, 1.0)), 1.0) * 0.45
        + min(max(funding_rate or 0.0, 0.0) / 0.0035, 1.0) * 0.25
        + min(max((long_short_ratio or 1.0) - 1.0, 0.0) / 0.35, 1.0) * 0.30
    )

    return {
        "exchange": "aggregate",
        "symbol": _pair_symbol(str(snapshot.get("base_symbol") or snapshot.get("symbol") or "")),
        "timestamp": snapshot.get("timestamp"),
        "capture_status": "ok",
        "source_error": None,
        "source_name": "coinglass_coins_markets",
        "latency_ms": snapshot.get("latency_ms"),
        "payload": {
            "market_cap_usd": _value("market_cap_usd"),
            "current_price": _value("current_price"),
            "source": "coins_markets",
        },
        "oi_usd": _value("open_interest_usd"),
        "oi_change_1h": oi_change_1h,
        "oi_change_4h": _value("open_interest_change_percent_4h"),
        "oi_change_24h": oi_change_24h,
        "funding_rate": funding_rate,
        "long_short_ratio": long_short_ratio,
        "basis_pct": (_value("oi_vol_ratio_change_percent_4h") or 0.0) / 100.0,
        "taker_buy_sell_imbalance": imbalance_4h,
        "crowding_score": crowding_score,
        "squeeze_score": squeeze_score,
        "distribution_score": distribution_score,
        "orderbook_imbalance_score": _clamp01(0.5 + imbalance_4h * 0.5),
        "depth_thinness_score": _clamp01(1.0 - min(volume_4h / 250_000_000.0, 1.0)),
    }


async def build_exchange_altcoin_universe(
    exchange: str,
    *,
    refresh: bool = False,
    manual: bool = False,
) -> Dict[str, Any]:
    normalized_exchange = _normalize_exchange(exchange)
    cache_key = _universe_cache_key(normalized_exchange)
    cached = _UNIVERSE_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and not refresh and (now_ts - float(cached.get("stored_at", 0.0))) <= _UNIVERSE_CACHE_TTL_SEC:
        return dict(cached.get("payload") or {})

    cache_path = _universe_cache_path(normalized_exchange)
    stale_payload = _load_universe_cache_payload(normalized_exchange) if not refresh else None
    if stale_payload:
        age_sec = _universe_cache_age_sec(stale_payload)
        if age_sec is not None and age_sec <= _UNIVERSE_CACHE_TTL_SEC:
            _UNIVERSE_CACHE[cache_key] = {"stored_at": now_ts, "payload": stale_payload}
            return stale_payload

    try:
        market_rows, onboard_map = await asyncio.wait_for(
            asyncio.gather(
                load_coinglass_market_snapshots(normalized_exchange, refresh=refresh, manual=manual),
                load_exchange_onboard_map(normalized_exchange, refresh=refresh, manual=manual),
            ),
            timeout=_UNIVERSE_REFRESH_TIMEOUT_SEC,
        )
    except Exception as exc:
        if stale_payload and not refresh:
            fallback_payload = _build_universe_stale_fallback(
                stale_payload,
                exchange=normalized_exchange,
                reason=str(exc),
            )
            _UNIVERSE_CACHE[cache_key] = {"stored_at": now_ts, "payload": fallback_payload}
            return fallback_payload
        raise
    if not market_rows:
        if stale_payload and not refresh:
            fallback_payload = _build_universe_stale_fallback(
                stale_payload,
                exchange=normalized_exchange,
                reason="empty market rows",
            )
            _UNIVERSE_CACHE[cache_key] = {"stored_at": now_ts, "payload": fallback_payload}
            return fallback_payload
        return {
            "exchange": normalized_exchange,
            "symbols": [],
            "count": 0,
            "source": "coinglass_altcoin_universe",
            "board_count": 0,
            "boards": [],
        }

    market_ranked = sorted(
        market_rows.values(),
        key=lambda item: (_clip_float(item.get("market_cap_usd")) or 0.0, item.get("symbol") or ""),
        reverse=True,
    )
    onboard_ranked = sorted(
        onboard_map.values(),
        key=lambda item: (float(item.get("onboard_ts") or float("inf")), item.get("symbol") or ""),
    )

    tag_sample_bases = {
        str(item.get("base_symbol") or "")
        for item in market_ranked[:_DEFAULT_TAG_SAMPLE_LIMIT]
    }
    tag_sample_bases.update(
        str(item.get("base_symbol") or "")
        for item in onboard_ranked[:_DEFAULT_TAG_SAMPLE_LIMIT]
    )
    tag_sample_bases = {base for base in tag_sample_bases if base}
    tags_map = await load_currency_tags(tag_sample_bases, refresh=refresh)

    board_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for snapshot in market_ranked:
        base = str(snapshot.get("base_symbol") or "")
        tags = tags_map.get(base) or []
        for tag in tags:
            board_map[tag].append(snapshot)

    selected_reasons: Dict[str, List[str]] = defaultdict(list)
    selected_market_caps: Dict[str, float] = {}
    selected_onboard_ts: Dict[str, float] = {}
    boards_payload: List[Dict[str, Any]] = []

    for board, items in sorted(board_map.items()):
        eligible = [
            item
            for item in items
            if is_alt_candidate_symbol(
                str(item.get("base_symbol") or ""),
                _clip_float(item.get("market_cap_usd")),
            )
        ]
        if not eligible:
            continue

        top_market = sorted(
            eligible,
            key=lambda item: (_clip_float(item.get("market_cap_usd")) or 0.0, item.get("symbol") or ""),
            reverse=True,
        )[:2]
        top_onboard = sorted(
            [
                item
                for item in eligible
                if str(item.get("base_symbol") or "") in onboard_map
            ],
            key=lambda item: (
                float(onboard_map.get(str(item.get("base_symbol") or ""), {}).get("onboard_ts") or float("inf")),
                item.get("symbol") or "",
            ),
        )[:2]

        board_symbols: List[str] = []
        for item in top_market:
            symbol = str(item.get("symbol") or "")
            if not symbol:
                continue
            board_symbols.append(symbol)
            selected_reasons[symbol].append(f"{board}:market_cap_top2")
            selected_market_caps[symbol] = _clip_float(item.get("market_cap_usd")) or 0.0
            selected_onboard_ts[symbol] = float(
                onboard_map.get(str(item.get("base_symbol") or ""), {}).get("onboard_ts") or float("inf")
            )
        for item in top_onboard:
            symbol = str(item.get("symbol") or "")
            if not symbol:
                continue
            if symbol not in board_symbols:
                board_symbols.append(symbol)
            selected_reasons[symbol].append(f"{board}:onboard_top2")
            selected_market_caps[symbol] = _clip_float(item.get("market_cap_usd")) or 0.0
            selected_onboard_ts[symbol] = float(
                onboard_map.get(str(item.get("base_symbol") or ""), {}).get("onboard_ts") or float("inf")
            )

        boards_payload.append(
            {
                "board": board,
                "market_cap_top2": [str(item.get("symbol") or "") for item in top_market if str(item.get("symbol") or "")],
                "onboard_top2": [str(item.get("symbol") or "") for item in top_onboard if str(item.get("symbol") or "")],
            }
        )

    final_symbols = sorted(
        selected_reasons.keys(),
        key=lambda symbol: (
            -len(selected_reasons[symbol]),
            -(selected_market_caps.get(symbol) or 0.0),
            selected_onboard_ts.get(symbol) or float("inf"),
            symbol,
        ),
    )

    if not final_symbols:
        fallback = [
            str(item.get("symbol") or "")
            for item in market_ranked
            if is_alt_candidate_symbol(
                str(item.get("base_symbol") or ""),
                _clip_float(item.get("market_cap_usd")),
            )
        ]
        final_symbols = fallback[:60]

    payload = {
        "exchange": normalized_exchange,
        "symbols": final_symbols,
        "count": len(final_symbols),
        "source": "coinglass_altcoin_universe",
        "major_market_cap_symbols": _major_market_cap_symbols(market_ranked),
        "board_count": len(boards_payload),
        "boards": boards_payload,
        "selection_reasons": {symbol: reasons for symbol, reasons in selected_reasons.items()},
        "excluded_major_symbols": sorted(
            [
                str(item.get("symbol") or "")
                for item in market_ranked
                if not is_alt_candidate_symbol(
                    str(item.get("base_symbol") or ""),
                    _clip_float(item.get("market_cap_usd")),
                )
            ]
        ),
        "updated_at": _utc_iso(),
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _UNIVERSE_CACHE[cache_key] = {"stored_at": now_ts, "payload": payload}
    return payload
