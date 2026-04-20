"""Perp-specific ignition / continuation / crowding scoring for the altcoin radar.

These are pure functions that take pre-computed float values extracted from
derivatives snapshots and market-data raw_components.  They are called from
altcoin_radar.build_altcoin_rows() after the existing component pipeline.

Score meanings
--------------
ignition_score       : 0-1, high = coin just lit up (15m/1h view)
continuation_score   : 0-1, high = move in progress, not yet crowded
crowding_late_score  : 0-1, high = late-stage / over-crowded (risk flag)
"""
from __future__ import annotations

import math
from typing import Any, Dict


# ── helpers ────────────────────────────────────────────────────────────────

def _f(value: Any, default: float = 0.0) -> float:
    """Safe float conversion with finite check."""
    try:
        v = float(value)
    except Exception:
        return float(default)
    return float(default) if not math.isfinite(v) else v


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


# ── ignition_score ─────────────────────────────────────────────────────────

def compute_ignition_score(
    *,
    oi_zscore_1h: float,
    volume_zscore_1h: float,
    short_liq_share: float,
    breakout_from_compression: float,
    exchange_breadth: float,
) -> float:
    """Perp ignition score for ORDI-type coins.

    Formula (from plan section 8.2):
        0.30 * oi_zscore_1h
      + 0.25 * volume_zscore_1h
      + 0.20 * short_liq_share
      + 0.15 * breakout_from_compression
      + 0.10 * exchange_breadth

    All inputs are expected to be in [0, 1] (already normalised by caller).
    """
    score = (
        0.30 * _clamp(_f(oi_zscore_1h))
        + 0.25 * _clamp(_f(volume_zscore_1h))
        + 0.20 * _clamp(_f(short_liq_share))
        + 0.15 * _clamp(_f(breakout_from_compression))
        + 0.10 * _clamp(_f(exchange_breadth))
    )
    return _clamp(score)


def _derive_ignition_inputs(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    derivatives: Dict[str, Any],
) -> Dict[str, float]:
    """Extract ignition inputs from already-computed pipeline data.

    Maps from existing altcoin_radar.py fields:
    - oi_zscore_1h       ← pct["derivatives_heat"] (best proxy for OI momentum)
    - volume_zscore_1h   ← pct["volume_burst"] (already percentile-ranked)
    - short_liq_share    ← metrics_raw["liquidation_burst_score"] normalised
    - breakout_from_compression ← pct["impulse_after_compression"]
    - exchange_breadth   ← fixed 1.0 when derivatives present, else 0
    """
    oi_zscore_1h = _clamp(_f(pct.get("derivatives_heat")))
    volume_zscore_1h = _clamp(_f(pct.get("volume_burst")))

    liq_burst = _clamp(_f(metrics_raw.get("liquidation_burst_score")))
    short_liq_share = liq_burst

    breakout_from_compression = _clamp(_f(pct.get("impulse_after_compression")))

    exchange_breadth = 1.0 if derivatives else 0.0

    return {
        "oi_zscore_1h": oi_zscore_1h,
        "volume_zscore_1h": volume_zscore_1h,
        "short_liq_share": short_liq_share,
        "breakout_from_compression": breakout_from_compression,
        "exchange_breadth": exchange_breadth,
    }


# ── continuation_score ─────────────────────────────────────────────────────

def compute_continuation_score(
    *,
    oi_change_4h: float,
    price_change_4h: float,
    taker_buy_sell_imbalance: float,
    flow_confirmation: float,
    funding_not_overcrowded: float,
) -> float:
    """Move is in progress but not yet over-crowded.

    All inputs expected in [0, 1].
    """
    score = (
        0.25 * _clamp(_f(oi_change_4h))
        + 0.20 * _clamp(_f(price_change_4h))
        + 0.20 * _clamp(_f(taker_buy_sell_imbalance))
        + 0.20 * _clamp(_f(flow_confirmation))
        + 0.15 * _clamp(_f(funding_not_overcrowded))
    )
    return _clamp(score)


def _derive_continuation_inputs(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    derivatives: Dict[str, Any],
) -> Dict[str, float]:
    # oi_change_4h: normalise raw value to [0,1]  (10% change → ~0.5)
    raw_oi_4h = _f(derivatives.get("oi_change_4h") or metrics_raw.get("oi_change_4h"), 0.0)
    oi_change_4h = _clamp(raw_oi_4h / 0.20) if raw_oi_4h > 0 else 0.0

    # price_change_4h from pct["return_shock"] (already captures recent return)
    price_change_4h = _clamp(_f(pct.get("return_shock")))

    # taker imbalance: raw +/- normalised
    raw_taker = _f(derivatives.get("taker_buy_sell_imbalance") or
                   metrics_raw.get("taker_buy_sell_imbalance"), 0.0)
    taker_buy_sell_imbalance = _clamp((raw_taker + 1.0) / 2.0)  # map [-1,1] → [0,1]

    flow_confirmation = _clamp(_f(pct.get("flow_confirmation")))

    # funding_not_overcrowded: low funding (not extreme) is good for continuation
    funding_abs = abs(_f(metrics_raw.get("funding_rate"), 0.0))
    # 0 funding → 1.0 (perfect), 0.1% → 0.0 (extreme)
    funding_not_overcrowded = _clamp(1.0 - funding_abs / 0.001) if funding_abs > 0 else 1.0

    return {
        "oi_change_4h": oi_change_4h,
        "price_change_4h": price_change_4h,
        "taker_buy_sell_imbalance": taker_buy_sell_imbalance,
        "flow_confirmation": flow_confirmation,
        "funding_not_overcrowded": funding_not_overcrowded,
    }


# ── crowding_late_score ────────────────────────────────────────────────────

def compute_crowding_late_score(
    *,
    funding_extreme: float,
    crowding_risk: float,
    long_short_ratio_extreme: float,
    distribution_score: float,
    liq_after_spike: float,
) -> float:
    """Late-stage / over-crowded risk.  High score = avoid chasing.

    All inputs expected in [0, 1].
    """
    score = (
        0.30 * _clamp(_f(funding_extreme))
        + 0.25 * _clamp(_f(crowding_risk))
        + 0.20 * _clamp(_f(long_short_ratio_extreme))
        + 0.15 * _clamp(_f(distribution_score))
        + 0.10 * _clamp(_f(liq_after_spike))
    )
    return _clamp(score)


def _derive_crowding_inputs(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    derivatives: Dict[str, Any],
) -> Dict[str, float]:
    # funding_extreme: map funding rate to a 0-1 extremeness measure
    fr = abs(_f(metrics_raw.get("funding_rate"), 0.0))
    # > 0.03% per 8h is notable; > 0.1% is extreme
    funding_extreme = _clamp(fr / 0.001)

    crowding_risk = _clamp(_f(pct.get("crowding_risk")))

    # long_short_ratio_extreme: values far from 1.0 are crowded
    ls_ratio = _f(derivatives.get("long_short_ratio") or metrics_raw.get("long_short_ratio"), 1.0)
    deviation = abs(ls_ratio - 1.0) / 1.0  # 2.0 ratio → 1.0 score
    long_short_ratio_extreme = _clamp(deviation)

    distribution_score = _clamp(_f(metrics_raw.get("distribution_score")))

    # liq_after_spike: proxy using liquidity_trap + liquidity_thinness
    liq_after_spike = _clamp(_f(pct.get("liquidity_trap")))

    return {
        "funding_extreme": funding_extreme,
        "crowding_risk": crowding_risk,
        "long_short_ratio_extreme": long_short_ratio_extreme,
        "distribution_score": distribution_score,
        "liq_after_spike": liq_after_spike,
    }


# ── public entry point ─────────────────────────────────────────────────────

def compute_perp_scores(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    derivatives: Dict[str, Any],
) -> Dict[str, float]:
    """Compute all three perp scores from existing pipeline data.

    Returns:
        {
            "ignition_score":       float in [0, 1],
            "continuation_score":   float in [0, 1],
            "crowding_late_score":  float in [0, 1],
        }
    """
    ign_inputs = _derive_ignition_inputs(metrics_raw, pct, derivatives)
    ign = compute_ignition_score(**ign_inputs)

    cont_inputs = _derive_continuation_inputs(metrics_raw, pct, derivatives)
    cont = compute_continuation_score(**cont_inputs)

    crowd_inputs = _derive_crowding_inputs(metrics_raw, pct, derivatives)
    crowd = compute_crowding_late_score(**crowd_inputs)

    return {
        "ignition_score": round(ign, 4),
        "continuation_score": round(cont, 4),
        "crowding_late_score": round(crowd, 4),
    }


# ── signal source classification ───────────────────────────────────────────

def classify_signal_source(
    ignition_score: float,
    continuation_score: float,
    crowding_late_score: float,
    derivatives_present: bool,
) -> str:
    """Map perp scores to a signal_source label.

    Returns one of:
        "perp_ignition"       — coin just lit up via derivatives
        "perp_continuation"   — move in progress
        "crowded_late_stage"  — late / over-crowded risk
        ""                    — no clear perp signal
    """
    ig = _f(ignition_score)
    co = _f(continuation_score)
    cr = _f(crowding_late_score)

    if not derivatives_present:
        return ""

    if cr >= 0.65:
        return "crowded_late_stage"
    if ig >= 0.60:
        return "perp_ignition"
    if co >= 0.55 and ig < 0.50:
        return "perp_continuation"
    return ""
