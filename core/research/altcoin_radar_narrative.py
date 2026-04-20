"""Narrative Radar scoring functions for the altcoin radar.

Handles '币安人生型' pattern coins driven by narrative, community heat,
and sector rotation — distinct from OI/liquidation-driven Perp coins.

Score meanings
--------------
narrative_heat_score  : 0-1, high = active narrative / board heat present
meme_rotation_score   : 0-1, high = rotation conditions met (sector + watchlist)

Both scores are derived from already-computed pipeline fields so no extra
DB or API calls are needed.  sector and in_watchlist must be supplied by
the caller (derived from altcoin_radar_universe helpers).
"""
from __future__ import annotations

import math
from typing import Any, Dict

# ── sector heat constants ──────────────────────────────────────────────────
# High-heat narrative sectors (meme coins, ordinals, AI tokens)
_HIGH_HEAT_SECTORS = frozenset({"meme", "ordinals", "ai"})
# Medium-heat sectors (gaming, DeFi, L1, L2 ecosystem plays)
_MEDIUM_HEAT_SECTORS = frozenset({"gaming", "defi", "l1", "l2", "infra"})


# ── helpers ────────────────────────────────────────────────────────────────

def _f(value: Any, default: float = 0.0) -> float:
    """Safe float with finite check."""
    try:
        v = float(value)
    except Exception:
        return float(default)
    return float(default) if not math.isfinite(v) else v


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


# ── narrative_heat_score ───────────────────────────────────────────────────

def compute_narrative_heat_score(
    *,
    board_heat: float,
    news_burst: float,
    community_flow: float,
    tx_count_acceleration: float,
    active_address_acceleration: float,
) -> float:
    """Narrative heat score for '币安人生型' coins.

    Formula:
        0.30 * board_heat
      + 0.25 * news_burst
      + 0.20 * community_flow
      + 0.15 * tx_count_acceleration
      + 0.10 * active_address_acceleration

    All inputs expected in [0, 1].  Weights sum to 1.0, so max score = 1.0.
    """
    score = (
        0.30 * _clamp(_f(board_heat))
        + 0.25 * _clamp(_f(news_burst))
        + 0.20 * _clamp(_f(community_flow))
        + 0.15 * _clamp(_f(tx_count_acceleration))
        + 0.10 * _clamp(_f(active_address_acceleration))
    )
    return _clamp(score)


# ── meme_rotation_score ────────────────────────────────────────────────────

def compute_meme_rotation_score(
    *,
    sector_breadth: float,
    watchlist_hit: float,
    turnover_spike: float,
    market_cap_elasticity: float,
    short_term_dominance: float,
) -> float:
    """Meme / rotation score: sector breadth × watchlist × volume activity.

    Formula:
        0.30 * sector_breadth
      + 0.25 * watchlist_hit
      + 0.20 * turnover_spike
      + 0.15 * market_cap_elasticity
      + 0.10 * short_term_dominance

    All inputs in [0, 1].  Weights sum to 1.0.
    """
    score = (
        0.30 * _clamp(_f(sector_breadth))
        + 0.25 * _clamp(_f(watchlist_hit))
        + 0.20 * _clamp(_f(turnover_spike))
        + 0.15 * _clamp(_f(market_cap_elasticity))
        + 0.10 * _clamp(_f(short_term_dominance))
    )
    return _clamp(score)


# ── derive helpers ─────────────────────────────────────────────────────────

def _derive_narrative_inputs(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    sector: str,
    in_watchlist: bool,
) -> Dict[str, float]:
    """Map already-computed pipeline data to narrative heat inputs.

    board_heat       ← sector membership + watchlist boost
    news_burst       ← pct["announcements"] (announcement percentile)
    community_flow   ← pct["community_flow"]
    tx_count_accel   ← pct["volume_burst"] (on-chain activity proxy)
    active_addr_accel← pct["whale_context"] (whale tx ≈ large address activity)
    """
    sec = str(sector or "").strip().lower()

    if sec in _HIGH_HEAT_SECTORS:
        board_heat_base = 0.80
    elif sec in _MEDIUM_HEAT_SECTORS:
        board_heat_base = 0.45
    elif sec:
        board_heat_base = 0.20
    else:
        board_heat_base = 0.0

    # Watchlist membership adds a fixed +0.20 boost (capped at 1.0)
    board_heat = _clamp(board_heat_base + (0.20 if in_watchlist else 0.0))

    news_burst = _clamp(_f(pct.get("announcements")))
    community_flow_val = _clamp(_f(pct.get("community_flow")))
    tx_count_acceleration = _clamp(_f(pct.get("volume_burst")))
    active_address_acceleration = _clamp(_f(pct.get("whale_context")))

    return {
        "board_heat": board_heat,
        "news_burst": news_burst,
        "community_flow": community_flow_val,
        "tx_count_acceleration": tx_count_acceleration,
        "active_address_acceleration": active_address_acceleration,
    }


def _derive_meme_rotation_inputs(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    sector: str,
    in_watchlist: bool,
) -> Dict[str, float]:
    """Map pipeline data to meme rotation inputs.

    sector_breadth       ← sector enum (meme/ordinals/ai → high)
    watchlist_hit        ← 1.0 if in hardcoded watchlist, else 0.0
    turnover_spike       ← pct["volume_burst"]
    market_cap_elasticity← cap factor × return shock
    short_term_dominance ← pct["return_shock"]
    """
    sec = str(sector or "").strip().lower()

    if sec in _HIGH_HEAT_SECTORS:
        sector_breadth = 0.90
    elif sec in _MEDIUM_HEAT_SECTORS:
        sector_breadth = 0.50
    elif sec:
        sector_breadth = 0.20
    else:
        sector_breadth = 0.0

    watchlist_hit = 1.0 if in_watchlist else 0.0
    turnover_spike = _clamp(_f(pct.get("volume_burst")))

    # market_cap_elasticity: small cap + high return = high elasticity
    market_cap_usd = _f(metrics_raw.get("market_cap_usd"), 0.0)
    return_shock = _clamp(_f(pct.get("return_shock")))
    if market_cap_usd > 0:
        # $5B cap normalises to 0; $0 cap → 1.0
        cap_factor = _clamp(1.0 - market_cap_usd / 5_000_000_000.0)
        market_cap_elasticity = _clamp(cap_factor * 0.65 + return_shock * 0.35)
    else:
        market_cap_elasticity = return_shock * 0.40  # unknown cap = conservative

    short_term_dominance = _clamp(_f(pct.get("return_shock")))

    return {
        "sector_breadth": sector_breadth,
        "watchlist_hit": watchlist_hit,
        "turnover_spike": turnover_spike,
        "market_cap_elasticity": market_cap_elasticity,
        "short_term_dominance": short_term_dominance,
    }


# ── public entry point ─────────────────────────────────────────────────────

def compute_narrative_scores(
    metrics_raw: Dict[str, Any],
    pct: Dict[str, Any],
    sector: str = "",
    in_watchlist: bool = False,
) -> Dict[str, float]:
    """Compute both narrative scores from pipeline data.

    Parameters
    ----------
    metrics_raw  : per-symbol raw metrics from altcoin_radar.build_altcoin_rows
    pct          : per-symbol percentile map (same loop)
    sector       : sector label from altcoin_radar_universe.get_sector(symbol)
    in_watchlist : True if symbol is in ALTCOIN_WATCHLIST

    Returns
    -------
    {
        "narrative_heat_score": float in [0, 1],
        "meme_rotation_score":  float in [0, 1],
    }
    """
    nh_inputs = _derive_narrative_inputs(metrics_raw, pct, sector, in_watchlist)
    nh = compute_narrative_heat_score(**nh_inputs)

    mr_inputs = _derive_meme_rotation_inputs(metrics_raw, pct, sector, in_watchlist)
    mr = compute_meme_rotation_score(**mr_inputs)

    return {
        "narrative_heat_score": round(nh, 4),
        "meme_rotation_score": round(mr, 4),
    }


# ── narrative signal source classification ─────────────────────────────────

def classify_narrative_source(
    narrative_heat_score: float,
    meme_rotation_score: float,
) -> str:
    """Classify narrative signal source.

    Returns
    -------
    "narrative_ignition"     — strong board heat + rotation conditions
    "narrative_confirmation" — moderate heat with some rotation support
    ""                       — below thresholds, no clear narrative signal
    """
    nh = _f(narrative_heat_score)
    mr = _f(meme_rotation_score)

    # Strong heat + decent rotation = ignition
    if nh >= 0.55:
        return "narrative_ignition"

    # Moderate heat + rotation support = confirmation
    if nh >= 0.40 and mr >= 0.35:
        return "narrative_confirmation"

    return ""
