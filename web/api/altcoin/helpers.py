"""Pure, side-effect-free helpers for the altcoin radar API.

Normalisers, hashing, snapshot serialisers, market-data freshness checks and the
Binance public-ticker builders. None of these are monkeypatched by the test
suite, so callers may import them by name; they are re-exported from the package
__init__ to preserve the altcoin_api.<name> surface.
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from fastapi import HTTPException

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsDerivativesSnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
)
from core.data.alpha_market_data import is_alpha_symbol as is_collected_alpha_symbol
from core.data.coinglass_registry import normalize_coinglass_symbol
from core.research.altcoin_radar import TIMEFRAME_SECONDS, VALID_TIMEFRAMES
from core.research.altcoin_radar_universe import normalize_altcoin_pair

from .constants import (
    ALLOWED_MODES,
    ALLOWED_SORTS,
    ALLOWED_UNIVERSE_SCOPES,
    ALLOWED_VIEWS,
    DEFAULT_EXCHANGE,
    DEFAULT_SORT,
    DEFAULT_TIMEFRAME,
    TTL_BY_TIMEFRAME,
    TTL_BY_VIEW,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clone_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(payload or {}))


def _normalize_symbols(symbols: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for symbol in symbols:
        text = normalize_altcoin_pair(symbol)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _is_alpha_symbol(symbol: str) -> bool:
    """Return whether *symbol* is one of our stable Alpha directory pairs."""
    return is_collected_alpha_symbol(normalize_altcoin_pair(symbol))


def _parse_symbols_param(symbols: Optional[str]) -> List[str]:
    if not symbols:
        return []
    parts = [part.strip() for part in str(symbols).replace(";", ",").split(",")]
    return _normalize_symbols(parts)


def _normalize_exchange(exchange: str) -> str:
    text = str(exchange or DEFAULT_EXCHANGE).strip().lower()
    return text or DEFAULT_EXCHANGE


def _normalize_timeframe(timeframe: str) -> str:
    tf = str(timeframe or DEFAULT_TIMEFRAME).strip().lower()
    if tf not in VALID_TIMEFRAMES:
        return DEFAULT_TIMEFRAME
    return tf


def _normalize_sort(sort_by: str) -> str:
    text = str(sort_by or DEFAULT_SORT).strip().lower()
    if text not in ALLOWED_SORTS:
        return DEFAULT_SORT
    return text


def _normalize_mode(mode: str) -> str:
    text = str(mode or "combined").strip().lower()
    return text if text in ALLOWED_MODES else "combined"


def _normalize_view(view: str) -> str:
    text = str(view or "4h").strip().lower()
    return text if text in ALLOWED_VIEWS else "4h"


def _normalize_universe_scope(scope: str) -> str:
    text = str(scope or "research").strip().lower()
    return text if text in ALLOWED_UNIVERSE_SCOPES else "research"


def _cache_ttl(timeframe: str, view: str = "") -> float:
    if view and view in TTL_BY_VIEW:
        return TTL_BY_VIEW[view]
    return float(TTL_BY_TIMEFRAME.get(_normalize_timeframe(timeframe), TTL_BY_TIMEFRAME[DEFAULT_TIMEFRAME]))


def _resolve_requested_timeframe(*, timeframe: str, view: str) -> str:
    normalized_view = _normalize_view(view) if view else ""
    if normalized_view:
        return _normalize_timeframe(normalized_view)
    return _normalize_timeframe(timeframe)


def _hash_universe(symbols: Sequence[str]) -> str:
    normalized = _normalize_symbols(symbols)
    raw = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _serialize_micro_snapshot(row: AnalyticsMicrostructureSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "available": row.capture_status != "failed" and float(row.mid_price or 0.0) > 0.0,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "orderbook": {
            "mid_price": float(row.mid_price or 0.0),
            "spread_bps": float(row.spread_bps or 0.0),
        },
        "aggressor_flow": {
            "imbalance": float(row.order_flow_imbalance or 0.0),
            "buy_ratio": float(row.buy_ratio or 0.0),
            "sell_ratio": float(row.sell_ratio or 0.0),
        },
        "funding_rate": {
            "available": row.funding_rate is not None,
            "funding_rate": row.funding_rate,
        },
        "spot_futures_basis": {
            "available": row.basis_pct is not None,
            "basis_pct": row.basis_pct,
        },
    }


def _serialize_community_snapshot(row: AnalyticsCommunitySnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "flow_proxy": {
            "imbalance": float(row.flow_imbalance or 0.0),
            "buy_ratio": float(row.buy_ratio or 0.0),
            "sell_ratio": float(row.sell_ratio or 0.0),
        },
        "announcements": list(payload.get("announcements") or []),
        "security_alerts": payload.get("security_alerts") or {},
        "twitter_watchlist": list(payload.get("twitter_watchlist") or []),
    }


def _serialize_whale_snapshot(row: AnalyticsWhaleSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "available": row.capture_status != "failed",
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "count": int(row.whale_count or 0),
        "threshold_btc": payload.get("threshold_btc"),
        "btc_price": payload.get("btc_price"),
        "transactions": list(payload.get("transactions") or []),
    }


def _serialize_derivatives_snapshot(row: AnalyticsDerivativesSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "oi_usd": row.oi_usd,
        "oi_change_1h": row.oi_change_1h,
        "oi_change_4h": row.oi_change_4h,
        "oi_change_24h": row.oi_change_24h,
        "funding_rate": row.funding_rate,
        "long_short_ratio": row.long_short_ratio,
        "basis_pct": row.basis_pct,
        "taker_buy_sell_imbalance": row.taker_buy_sell_imbalance,
        "crowding_score": row.crowding_score,
        "squeeze_score": row.squeeze_score,
        "distribution_score": row.distribution_score,
        "orderbook_imbalance_score": row.orderbook_imbalance_score,
        "depth_thinness_score": row.depth_thinness_score,
    }


def _snapshot_age_seconds(value: Any) -> Optional[float]:
    return _age_seconds_at(value, now=_utcnow())


def _age_seconds_at(value: Any, *, now: datetime) -> Optional[float]:
    if not value:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return max(0.0, (now - ts.to_pydatetime()).total_seconds())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


def _market_data_stale_cutoff_seconds(timeframe: str) -> float:
    normalized = _normalize_timeframe(timeframe)
    return float(TIMEFRAME_SECONDS.get(normalized, TIMEFRAME_SECONDS[DEFAULT_TIMEFRAME]) * 4.0)


def _market_frame_age_seconds(frame: Any, *, now: datetime) -> Optional[float]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None
    try:
        index = frame.sort_index().index
        if len(index) == 0:
            return None
        return _age_seconds_at(index[-1], now=now)
    except Exception:
        return None


def _is_fresh_market_snapshot(snapshot: Optional[Mapping[str, Any]], *, timeframe: str, now: datetime) -> bool:
    if not snapshot:
        return False
    status = str(snapshot.get("capture_status") or "ok").strip().lower()
    if status in {"failed", "error", "unavailable"}:
        return False
    if _safe_float(snapshot.get("current_price") or snapshot.get("last_price"), 0.0) <= 0:
        return False
    age_sec = _age_seconds_at(snapshot.get("timestamp"), now=now)
    if age_sec is None:
        return False
    return age_sec <= _market_data_stale_cutoff_seconds(timeframe)


def _filter_fresh_market_frames(
    *,
    frames: Mapping[str, Any],
    market_snapshots: Mapping[str, Mapping[str, Any]],
    timeframe: str,
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    if not frames:
        return {}, [], []

    now = _utcnow()
    stale_cutoff_sec = _market_data_stale_cutoff_seconds(timeframe)
    snapshots_by_symbol = {
        str(symbol or "").strip().upper(): snapshot
        for symbol, snapshot in dict(market_snapshots or {}).items()
        if str(symbol or "").strip()
    }
    kept: Dict[str, Any] = {}
    dropped_with_snapshot: List[str] = []
    dropped_without_snapshot: List[str] = []
    for symbol, frame in dict(frames or {}).items():
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            continue
        frame_age_sec = _market_frame_age_seconds(frame, now=now)
        if frame_age_sec is not None and frame_age_sec > stale_cutoff_sec:
            if _is_fresh_market_snapshot(snapshots_by_symbol.get(normalized_symbol), timeframe=timeframe, now=now):
                dropped_with_snapshot.append(normalized_symbol)
            else:
                dropped_without_snapshot.append(normalized_symbol)
            continue
        kept[normalized_symbol] = frame
    return kept, _normalize_symbols(dropped_with_snapshot), _normalize_symbols(dropped_without_snapshot)


def _pair_symbol_from_base(base: str) -> str:
    text = str(base or "").strip().upper()
    return f"{text}/USDT" if text else ""


def _binance_public_symbol_key(symbol: str) -> str:
    base = normalize_coinglass_symbol(symbol)
    return f"{base}USDT" if base else ""


def _match_binance_public_ticker(
    ticker_map: Mapping[str, Mapping[str, Any]],
    symbol: str,
) -> Tuple[Optional[Mapping[str, Any]], float, str]:
    direct_key = _binance_public_symbol_key(symbol)
    if not direct_key:
        return None, 1.0, ""
    direct = ticker_map.get(direct_key)
    if direct:
        return direct, 1.0, direct_key

    candidates: List[Tuple[float, str, Mapping[str, Any]]] = []
    for key, ticker in ticker_map.items():
        key_text = str(key or "").strip().upper()
        if not key_text.endswith(direct_key):
            continue
        prefix = key_text[: -len(direct_key)]
        if not prefix.isdigit():
            continue
        multiplier = _safe_float(prefix, 1.0)
        if multiplier <= 1:
            continue
        candidates.append((multiplier, key_text, ticker))
    if not candidates:
        return None, 1.0, ""
    multiplier, matched_key, ticker = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
    return ticker, multiplier, matched_key


def _binance_timestamp_from_ticker(ticker: Mapping[str, Any]) -> str:
    for key in ("closeTime", "time", "openTime"):
        raw = ticker.get(key)
        if raw in (None, ""):
            continue
        try:
            numeric = float(raw)
        except Exception:
            continue
        if numeric > 1_000_000_000_000:
            numeric = numeric / 1000.0
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
    return _utcnow().isoformat()


def _build_binance_public_market_snapshot(
    *,
    requested_symbol: str,
    ticker: Mapping[str, Any],
    divisor: float,
    matched_symbol: str,
    source_name: str,
) -> Dict[str, Any]:
    base = normalize_coinglass_symbol(requested_symbol)
    symbol = _pair_symbol_from_base(base)
    price_divisor = max(1.0, float(divisor or 1.0))

    def _price(*keys: str) -> float:
        for key in keys:
            value = ticker.get(key)
            if value not in (None, ""):
                return _safe_float(value, 0.0) / price_divisor
        return 0.0

    last_price = _price("lastPrice", "last")
    high_price = _price("highPrice", "high")
    low_price = _price("lowPrice", "low")
    bid_price = _price("bidPrice", "bid")
    ask_price = _price("askPrice", "ask")
    quote_volume = _safe_float(
        ticker.get("quoteVolume")
        or ticker.get("quote_volume")
        or ticker.get("turnover")
        or ticker.get("turnover_usd"),
        0.0,
    )
    spread_bps = 0.0
    mid = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else 0.0
    if mid > 0 and ask_price >= bid_price:
        spread_bps = ((ask_price - bid_price) / mid) * 10_000.0

    return {
        "symbol": symbol,
        "raw_symbol": matched_symbol or str(ticker.get("symbol") or ""),
        "base_symbol": base,
        "exchange": "binance",
        "timestamp": _binance_timestamp_from_ticker(ticker),
        "source_name": source_name,
        "capture_status": "ok",
        "source_error": None,
        "latency_ms": 0,
        "current_price": last_price,
        "high_price_24h": high_price,
        "low_price_24h": low_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "spread_bps": spread_bps,
        "quote_volume_24h": quote_volume,
        "base_volume_24h": _safe_float(ticker.get("volume") or ticker.get("baseVolume"), 0.0),
        "price_change_percent_24h": _safe_float(ticker.get("priceChangePercent"), 0.0),
        "price_change_abs_24h": _safe_float(ticker.get("priceChange"), 0.0) / price_divisor,
    }


def _should_overlay_coinglass_market_snapshot(snapshot: Optional[Mapping[str, Any]]) -> bool:
    if not snapshot:
        return True
    capture_status = str(snapshot.get("capture_status") or "").strip().lower()
    if capture_status and capture_status not in {"ok", "success"}:
        return True
    age_sec = _snapshot_age_seconds(snapshot.get("timestamp"))
    if age_sec is None:
        return True
    return age_sec > 1800.0


def _needs_detail_chain_fallback(row: Optional[Mapping[str, Any]]) -> bool:
    percentiles = (((row or {}).get("metrics") or {}).get("percentiles") or {})
    return any(
        percentiles.get(key) is None
        for key in ("community_flow", "announcements", "funding_basis", "whale_context")
    )


# --- alert preset lookups (pure; shared by the scan pipeline and alert routes) ---
def _preset_definition(preset: str) -> Tuple[str, str, float, str]:
    text = str(preset or "").strip()
    if text == "点火预警":
        return "altcoin_ignition_cross_up", "ignition", 0.60, "anomaly"
    if text == "跃升预警":
        return "altcoin_rank_jump_top_n", "rank_jump", 0.30, "accumulation"
    if text == "拥挤预警":
        return "altcoin_crowding_risk_spike", "crowding", 0.65, "control"
    if text == "叙事预警":
        return "altcoin_narrative_heat_spike", "narrative", 0.55, "narrative"
    if text == "异动预警":
        return "altcoin_score_above", "anomaly", 0.72, "legacy_anomaly"
    if text == "吸筹预警":
        return "altcoin_score_above", "accumulation", 0.68, "legacy_accumulation"
    if text == "高控盘预警":
        return "altcoin_score_above", "control", 0.70, "legacy_control"
    raise HTTPException(status_code=400, detail="unsupported preset")


def _preset_label_from_rule(rule_type: str, score_key: str) -> str:
    normalized_type = str(rule_type or "").strip()
    normalized_key = str(score_key or "").strip().lower()
    if normalized_type == "altcoin_ignition_cross_up" or normalized_key == "ignition":
        return "点火预警"
    if normalized_type == "altcoin_rank_jump_top_n" or normalized_key == "rank_jump":
        return "跃升预警"
    if normalized_type == "altcoin_crowding_risk_spike" or normalized_key == "crowding":
        return "拥挤预警"
    if normalized_type == "altcoin_narrative_heat_spike" or normalized_key == "narrative":
        return "叙事预警"
    if normalized_key == "anomaly":
        return "异动预警"
    if normalized_key == "accumulation":
        return "吸筹预警"
    if normalized_key == "control":
        return "高控盘预警"
    return "山寨预警"


def _alert_kind_from_rule(rule_type: str, score_key: str) -> str:
    _, _, _, kind = _preset_definition(_preset_label_from_rule(rule_type, score_key))
    return kind
