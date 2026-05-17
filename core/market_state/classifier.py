"""Market regime classifier shared by workbench, planner, and runtime views."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.market_state.hysteresis import (
    DEFAULT_SPREAD_ENTER_BPS,
    DEFAULT_TREND_BAND,
    classify_margin,
    spread_risk_posture,
)
from core.market_state.schema import DataManifestEntry, MarketStateSnapshot


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value or 0.0)))


def classify_market_regime(
    *,
    symbol: str = "BTC/USDT",
    exchange: str = "binance",
    risk_level: str = "unknown",
    spread_bps: float = 0.0,
    imbalance: float = 0.0,
    long_short_ratio: float = 1.0,
    wall_bias: float = 0.0,
    news_bias: float = 0.0,
    total_news: int = 0,
    derivatives_ready: bool = False,
    derivatives_history_ready: bool = False,
    derivatives_freshness_sec: Optional[float] = None,
    data_manifest: Optional[List[DataManifestEntry]] = None,
) -> Dict[str, Any]:
    micro_signal = (
        float(imbalance or 0.0) * 0.55
        + _clip(float(long_short_ratio or 1.0) - 1.0, -0.6, 0.6) * 0.30
        + float(wall_bias or 0.0) * 0.25
    )
    joint_signal = micro_signal + (float(news_bias or 0.0) * 0.25)
    signal_state, margin = classify_margin(
        joint_signal,
        enter_threshold=DEFAULT_TREND_BAND.enter_threshold,
        uncertain_low=DEFAULT_TREND_BAND.uncertain_low,
    )
    risk_posture = spread_risk_posture(spread_bps)

    derivatives_confidence_bonus = 0.0
    if derivatives_ready:
        derivatives_confidence_bonus += 0.08
    if derivatives_history_ready:
        derivatives_confidence_bonus += 0.04
    if derivatives_freshness_sec is not None and derivatives_freshness_sec > 1800:
        derivatives_confidence_bonus -= 0.05
    confidence = min(
        0.95,
        max(
            0.2,
            0.3
            + min(0.25, int(total_news or 0) / 220.0)
            + (0.15 if abs(micro_signal) >= DEFAULT_TREND_BAND.uncertain_low else 0.0)
            + (0.08 if float(long_short_ratio or 0.0) > 0 else 0.0)
            + derivatives_confidence_bonus,
        ),
    )

    conflicts: List[str] = []
    if risk_posture in {"defensive", "halt_new_entries"} and signal_state == "confirmed":
        conflicts.append("directional_signal_with_wide_spread")

    risk_text = str(risk_level or "unknown").lower()
    if risk_text == "high" or risk_posture == "halt_new_entries":
        regime = "high_risk_chop"
        bias = "defensive"
        uncertainty = "confirmed" if risk_posture == "halt_new_entries" or float(spread_bps or 0.0) >= DEFAULT_SPREAD_ENTER_BPS else "borderline"
    elif signal_state == "confirmed" and joint_signal > 0:
        regime = "trend_bullish"
        bias = "bullish"
        uncertainty = "confirmed"
    elif signal_state == "confirmed" and joint_signal < 0:
        regime = "trend_bearish"
        bias = "bearish"
        uncertainty = "confirmed"
    elif signal_state == "borderline":
        regime = "event_driven_mixed"
        bias = "neutral"
        uncertainty = "borderline"
    elif abs(joint_signal) <= 0.05 and int(total_news or 0) < 5:
        regime = "low_info_range"
        bias = "neutral"
        uncertainty = "low_signal"
    else:
        regime = "event_driven_mixed"
        bias = "neutral"
        uncertainty = "uncertain"

    snapshot = MarketStateSnapshot(
        symbol=symbol,
        exchange=exchange,
        as_of=datetime.now(timezone.utc),
        regime=regime,
        bias=bias,
        confidence=round(confidence, 4),
        uncertainty=uncertainty,
        risk_posture=risk_posture if str(risk_level or "").lower() != "high" else "halt_new_entries",
        component_votes={
            "micro_signal": round(micro_signal, 6),
            "news_bias": round(float(news_bias or 0.0), 6),
            "joint_signal": round(joint_signal, 6),
            "spread_bps": round(float(spread_bps or 0.0), 4),
            "risk_level": risk_level,
            "derivatives_ready": bool(derivatives_ready),
        },
        conflicts=conflicts,
        data_manifest=list(data_manifest or []),
        metadata={
            "classification_margin": margin,
            "hysteresis_state": {
                "trend_enter": DEFAULT_TREND_BAND.enter_threshold,
                "trend_exit": DEFAULT_TREND_BAND.exit_threshold,
                "trend_uncertain_band": [DEFAULT_TREND_BAND.uncertain_low, DEFAULT_TREND_BAND.uncertain_high],
                "spread_enter_bps": DEFAULT_SPREAD_ENTER_BPS,
            },
        },
    )
    out = snapshot.to_dict()
    out.update(
        {
            "risk_level": risk_level,
            "spread_bps": round(float(spread_bps or 0.0), 4),
            "imbalance": round(float(imbalance or 0.0), 4),
            "news_bias": round(float(news_bias or 0.0), 4),
            "long_short_ratio": round(float(long_short_ratio or 0.0), 6) if float(long_short_ratio or 0.0) > 0 else None,
            "classification_margin": margin,
            "hysteresis_state": out["metadata"]["hysteresis_state"],
        }
    )
    return out


def classify_daily_regime(*, avg_imbalance: float, avg_spread_bps: float, data_quality: str = "ok") -> Dict[str, Any]:
    result = classify_market_regime(
        spread_bps=avg_spread_bps,
        imbalance=avg_imbalance,
        long_short_ratio=1.0,
        wall_bias=0.0,
        news_bias=0.0,
        total_news=0,
    )
    if data_quality == "degraded" and result.get("uncertainty") == "confirmed":
        result["uncertainty"] = "borderline"
    return result


def manifest_entry(
    *,
    source: str,
    scope: str = "symbol",
    as_of: Any = None,
    age_seconds: Optional[float] = None,
    ttl_seconds: Optional[float] = None,
    degraded_reason: str = "",
) -> DataManifestEntry:
    freshness = "unknown"
    if age_seconds is not None and ttl_seconds is not None:
        freshness = "fresh" if float(age_seconds) <= float(ttl_seconds) else "stale"
    elif degraded_reason:
        freshness = "degraded"
    return DataManifestEntry(
        source=source,
        scope=scope,
        as_of=str(as_of) if as_of is not None else None,
        age_seconds=age_seconds,
        ttl_seconds=ttl_seconds,
        freshness=freshness,
        degraded_reason=degraded_reason,
    )

