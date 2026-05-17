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

    context = build_coinglass_runtime_context(snapshot)
    crowding_score = _as_float(snapshot.get("crowding_score"))
    squeeze_score = _as_float(snapshot.get("squeeze_score"))
    distribution_score = _as_float(snapshot.get("distribution_score"))
    imbalance = _as_float(snapshot.get("taker_buy_sell_imbalance"))
    funding_rate = _as_float(snapshot.get("funding_rate"))
    funding_zscore = _as_float(context.get("funding_zscore"))
    liquidation_burst_score = _as_float(context.get("liquidation_burst_score"))
    long_short_ratio_change = _as_float(context.get("long_short_ratio_change_24h"))
    heatmap_pressure_score = _as_float(context.get("heatmap_pressure_score"))
    liquidity_void_score = _as_float(context.get("liquidity_void_score"))
    orderbook_agg_imbalance = _as_float(context.get("orderbook_agg_imbalance"))

    history_ready = bool(context.get("history_ready"))
    crowded_long = bool(context.get("crowded_long"))
    crowded_short = bool(context.get("crowded_short"))
    squeeze_building = bool(context.get("squeeze_building"))
    flush_risk = bool(context.get("flush_risk"))
    basis_dislocation = bool(context.get("basis_dislocation"))
    flow_divergence = bool(context.get("flow_divergence"))
    order_flow_confirmed = bool(context.get("order_flow_confirmed"))

    direction = "FLAT"
    confidence = 0.0
    if squeeze_building or ((squeeze_score >= 0.62 or crowded_short) and imbalance > 0 and order_flow_confirmed):
        direction = "LONG"
        confidence = min(
            1.0,
            max(squeeze_score, 0.62) * 0.50
            + max(imbalance, 0.0) * 0.20
            + min(liquidation_burst_score, 1.0) * 0.15
            + (0.15 if order_flow_confirmed else 0.0),
        )
    elif crowded_long or flush_risk or ((crowding_score >= 0.72 and funding_rate > 0) or distribution_score >= 0.70):
        direction = "SHORT"
        confidence = min(
            1.0,
            max(crowding_score, distribution_score) * 0.70
            + min(abs(long_short_ratio_change), 1.0) * 0.15
            + (0.15 if crowded_long or flush_risk else 0.0),
        )

    if confidence > 0 and basis_dislocation:
        confidence *= 0.85
    if confidence > 0 and flow_divergence:
        confidence *= 0.80
    if confidence > 0 and liquidity_void_score >= 0.70:
        confidence *= 0.90
    if confidence > 0 and not history_ready:
        confidence *= 0.90

    risk_flags = []
    if crowding_score >= 0.70:
        risk_flags.append("crowding_hot")
    if distribution_score >= 0.65:
        risk_flags.append("distribution_risk")
    if squeeze_score >= 0.70:
        risk_flags.append("squeeze_active")
    if crowded_long:
        risk_flags.append("crowded_long")
    if flush_risk:
        risk_flags.append("flush_risk")
    if basis_dislocation:
        risk_flags.append("basis_dislocation")
    if flow_divergence:
        risk_flags.append("flow_divergence")
    if liquidation_burst_score >= 0.70:
        risk_flags.append("liquidation_burst")
    if heatmap_pressure_score >= 0.70:
        risk_flags.append("liquidity_heatmap_hot")
    if liquidity_void_score >= 0.70:
        risk_flags.append("liquidity_void")
    if not history_ready:
        risk_flags.append("history_incomplete")

    context_flags = []
    if crowded_short:
        context_flags.append("crowded_short")
    if squeeze_building:
        context_flags.append("squeeze_building")
    if order_flow_confirmed:
        context_flags.append("order_flow_confirmed")
    if abs(orderbook_agg_imbalance) >= 0.20:
        context_flags.append("orderbook_agg_bid_bias" if orderbook_agg_imbalance > 0 else "orderbook_agg_ask_bias")

    explain_parts = [
        f"crowding={crowding_score:.3f}",
        f"squeeze={squeeze_score:.3f}",
        f"distribution={distribution_score:.3f}",
    ]
    if heatmap_pressure_score > 0:
        explain_parts.append(f"heatmap={heatmap_pressure_score:.3f}")
    if history_ready:
        explain_parts.append(f"funding_z={funding_zscore:.2f}")
    if context.get("derivatives_labels"):
        explain_parts.append(f"labels={','.join(str(item) for item in context.get('derivatives_labels') or [])}")
    return direction, confidence, {
        "available": True,
        "snapshot": snapshot,
        "context": context,
        "regime": context.get("market_regime"),
        "crowding_score": crowding_score,
        "squeeze_score": squeeze_score,
        "risk_flags": risk_flags,
        "context_flags": context_flags,
        "explain": ", ".join(explain_parts),
    }
