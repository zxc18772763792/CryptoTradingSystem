"""News signal engine (read-only, outputs signal JSON only)."""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.ai.risk_gate import RiskGate
from core.news.eventizer.rules import SymbolMapper
from core.news.storage import db as news_db
from core.news.storage.models import SignalSchema, parse_any_datetime
from prediction_markets.polymarket import db as pm_db


MODEL_VERSION = "glm5_event_rules_v1"


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _build_risk(signal: str, market_features: Dict[str, Any]) -> Dict[str, Any]:
    atr = abs(_safe_float((market_features or {}).get("atr"), 0.015))
    atr_pct = atr if atr <= 1.0 else atr / 100.0
    base_stop = _clamp(max(0.008, atr_pct * 1.4), 0.008, 0.20)
    take = _clamp(base_stop * 2.0, 0.01, 0.40)

    if signal == "LONG":
        return {
            "stop_loss": -round(base_stop, 6),
            "take_profit": round(take, 6),
            "invalid_if": "price drops below stop_loss level or spread expands above threshold",
        }
    if signal == "SHORT":
        return {
            "stop_loss": round(base_stop, 6),
            "take_profit": -round(take, 6),
            "invalid_if": "price rises above stop_loss level or spread expands above threshold",
        }
    return {"stop_loss": 0.0, "take_profit": 0.0, "invalid_if": "no active directional view"}


def _confidence_from_events(events: List[Dict[str, Any]], weighted_contribs: List[Tuple[Dict[str, Any], float]]) -> float:
    n = len(events)
    if n == 0:
        return 0.0

    sources = [str((e.get("evidence") or {}).get("source") or "unknown") for e in events]
    source_diversity = _clamp(len(set(sources)) / max(1.0, min(4.0, float(n))))

    sentiments = [int(e.get("sentiment", 0)) for e in events]
    sentiment_groups = {
        -1: sentiments.count(-1),
        0: sentiments.count(0),
        1: sentiments.count(1),
    }
    dominant_sentiment_ratio = max(sentiment_groups.values()) / max(1, n)
    multi_source_consistency = _clamp(source_diversity * dominant_sentiment_ratio)

    type_counts: Dict[str, int] = {}
    for e in events:
        t = str(e.get("event_type") or "other")
        type_counts[t] = type_counts.get(t, 0) + 1
    type_repeat = max(type_counts.values()) / max(1, n)

    abs_contribs = sorted([abs(c) for _, c in weighted_contribs], reverse=True)
    total_abs = sum(abs_contribs)
    impact_concentration = 0.0 if total_abs <= 0 else sum(abs_contribs[:3]) / total_abs

    conf = 0.38 * multi_source_consistency + 0.27 * type_repeat + 0.35 * impact_concentration
    return round(_clamp(conf), 6)


def _choose_horizon(weighted_half_life: float) -> str:
    if weighted_half_life <= 120:
        return "15m"
    if weighted_half_life <= 480:
        return "1h"
    return "4h"


async def generate_signal(
    symbol: str,
    market_features: Dict[str, Any],
    since_minutes: int,
    cfg: Dict[str, Any],
    risk_gate: Optional[RiskGate] = None,
) -> Dict[str, Any]:
    """Generate deterministic signal JSON from stored events + market filters."""
    mapper: SymbolMapper = cfg.get("_symbol_mapper") or SymbolMapper({"symbols": cfg.get("symbols") or {}})
    symbol_norm = mapper.normalize_symbol(symbol) or str(symbol or "").upper()
    since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))

    events = await news_db.get_recent_events(symbol=symbol_norm, since_minutes=since_minutes)
    now = datetime.now(timezone.utc)

    threshold = float(((cfg.get("thresholds") or {}).get("alpha_threshold")) or 0.08)
    pm_enable = str(os.getenv("PM_FEATURES_ENABLE") or "").strip().lower() in {"1", "true", "yes", "on", "y"}
    pm_cfg = cfg.get("polymarket") or {}
    pm_alpha = float(os.getenv("PM_SIGNAL_ALPHA") or pm_cfg.get("alpha_weight") or 0.35)
    pm_beta = float(os.getenv("PM_RISK_BETA") or pm_cfg.get("global_risk_penalty") or 0.40)

    alpha = 0.0
    weighted_half_numer = 0.0
    weighted_half_denom = 0.0
    weighted_contribs: List[Tuple[Dict[str, Any], float]] = []

    for event in events:
        ts = parse_any_datetime(event.get("ts"))
        age_min = max(0.0, (now - ts).total_seconds() / 60.0)
        half_life = max(1.0, _safe_float(event.get("half_life_min"), 180.0))
        impact = _safe_float(event.get("impact_score"), 0.0)
        sentiment = int(event.get("sentiment", 0))

        decay = math.exp(-age_min / half_life)
        contrib = impact * sentiment * decay
        alpha += contrib

        abs_contrib = abs(contrib)
        weighted_half_numer += half_life * abs_contrib
        weighted_half_denom += abs_contrib
        weighted_contribs.append((event, contrib))

    merged_features = dict(market_features or {})
    pm_explain: List[str] = []
    if pm_enable:
        try:
            pm_snapshot = await pm_db.get_features_asof(symbol=symbol_norm, ts=now, timeframe="1m")
            if pm_snapshot:
                for key, value in pm_snapshot.items():
                    merged_features.setdefault(key, value)
                pm_price_signal = _safe_float(pm_snapshot.get("pm_price_signal"), 0.0)
                pm_global_risk = _safe_float(pm_snapshot.get("pm_global_risk"), 0.0)
                alpha = alpha + pm_alpha * pm_price_signal - pm_beta * pm_global_risk
                pm_explain.append(
                    f"pm boost: alpha += {pm_alpha:.2f}*{pm_price_signal:.4f} - {pm_beta:.2f}*{pm_global_risk:.4f}"
                )
            else:
                pm_explain.append("pm features unavailable -> fallback to news-only alpha")
        except Exception as exc:
            pm_explain.append(f"pm features failed: {exc}")

    desired_signal = "FLAT"
    if alpha > threshold:
        desired_signal = "LONG"
    elif alpha < -threshold:
        desired_signal = "SHORT"

    gate = risk_gate or RiskGate(cfg)
    final_signal, gate_reasons = gate.evaluate(
        symbol=symbol_norm,
        proposed_signal=desired_signal,
        market_features=merged_features,
        now=now,
    )

    weighted_half_life = weighted_half_numer / weighted_half_denom if weighted_half_denom > 0 else 240.0
    horizon = _choose_horizon(weighted_half_life)

    strength = _clamp(abs(alpha) / max(threshold * 3.0, 1e-9))
    confidence = _confidence_from_events(events, weighted_contribs)

    weighted_contribs.sort(key=lambda x: abs(x[1]), reverse=True)
    used_events = [str(e.get("event_id")) for e, _ in weighted_contribs[:12] if e.get("event_id")]

    explain: List[str] = [
        f"news_alpha={alpha:.6f} threshold={threshold:.6f}",
        f"events={len(events)} lookback={since_minutes}m weighted_half_life={weighted_half_life:.1f}m",
    ]
    explain.extend(pm_explain)

    if desired_signal != final_signal:
        explain.append(f"risk gate override: {desired_signal} -> {final_signal}")
    explain.extend([f"gate: {msg}" for msg in gate_reasons])

    signal = {
        "ts": now.isoformat(),
        "symbol": symbol_norm,
        "horizon": horizon,
        "signal": final_signal,
        "strength": round(strength, 6),
        "confidence": round(confidence, 6),
        "risk": _build_risk(final_signal, merged_features or {}),
        "explain": explain,
        "used_events": used_events,
        "model_version": MODEL_VERSION,
    }

    return SignalSchema.model_validate(signal).model_dump(mode="json")
