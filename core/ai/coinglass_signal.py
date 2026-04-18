from __future__ import annotations

from typing import Any, Dict, Tuple

from config.settings import settings
from core.data.coinglass_feature_builder import (
    build_coinglass_runtime_context,
    load_latest_derivatives_snapshot,
)


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


async def build_coinglass_signal(symbol: str) -> Tuple[str, float, Dict[str, Any]]:
    if not bool(getattr(settings, "COINGLASS_ENABLED", False) and getattr(settings, "COINGLASS_INCLUDE_AI", True)):
        return "FLAT", 0.0, {"available": False, "reason": "coinglass_ai_disabled"}

    snapshot = await load_latest_derivatives_snapshot(symbol)
    if not snapshot:
        return "FLAT", 0.0, {"available": False, "reason": "coinglass_snapshot_unavailable"}

    crowding_score = _as_float(snapshot.get("crowding_score"))
    squeeze_score = _as_float(snapshot.get("squeeze_score"))
    distribution_score = _as_float(snapshot.get("distribution_score"))
    imbalance = _as_float(snapshot.get("taker_buy_sell_imbalance"))
    funding_rate = _as_float(snapshot.get("funding_rate"))

    direction = "FLAT"
    confidence = 0.0
    if squeeze_score >= 0.62 and imbalance > 0:
        direction = "LONG"
        confidence = min(1.0, squeeze_score * 0.65 + max(imbalance, 0.0) * 0.35)
    elif (crowding_score >= 0.72 and funding_rate > 0) or distribution_score >= 0.70:
        direction = "SHORT"
        confidence = min(1.0, max(crowding_score, distribution_score))

    risk_flags = []
    if crowding_score >= 0.70:
        risk_flags.append("crowding_hot")
    if distribution_score >= 0.65:
        risk_flags.append("distribution_risk")
    if squeeze_score >= 0.70:
        risk_flags.append("squeeze_active")

    context = build_coinglass_runtime_context(snapshot)
    return direction, confidence, {
        "available": True,
        "snapshot": snapshot,
        "context": context,
        "regime": context.get("market_regime"),
        "crowding_score": crowding_score,
        "squeeze_score": squeeze_score,
        "risk_flags": risk_flags,
        "explain": f"crowding={crowding_score:.3f}, squeeze={squeeze_score:.3f}, distribution={distribution_score:.3f}",
    }
