"""Unit tests for core/research/altcoin_radar_perp.py — Phase 1."""
import pytest

from core.research.altcoin_radar_perp import (
    classify_signal_source,
    compute_crowding_late_score,
    compute_ignition_score,
    compute_perp_scores,
    _derive_ignition_inputs,
    _derive_continuation_inputs,
    _derive_crowding_inputs,
)


# ── ignition_score ─────────────────────────────────────────────────────────

def test_ignition_score_high_oi_and_volume():
    """OI spike + volume burst → high ignition score."""
    score = compute_ignition_score(
        oi_zscore_1h=0.9,
        volume_zscore_1h=0.85,
        short_liq_share=0.7,
        breakout_from_compression=0.8,
        exchange_breadth=1.0,
    )
    assert score >= 0.8, f"Expected high ignition, got {score}"


def test_ignition_score_low_all():
    """All inputs near zero → near-zero ignition score."""
    score = compute_ignition_score(
        oi_zscore_1h=0.05,
        volume_zscore_1h=0.05,
        short_liq_share=0.0,
        breakout_from_compression=0.0,
        exchange_breadth=0.0,
    )
    assert score < 0.1, f"Expected low ignition, got {score}"


def test_ignition_score_clamped():
    """Score must be in [0, 1] even with out-of-range inputs."""
    score = compute_ignition_score(
        oi_zscore_1h=5.0,
        volume_zscore_1h=5.0,
        short_liq_share=5.0,
        breakout_from_compression=5.0,
        exchange_breadth=5.0,
    )
    assert 0.0 <= score <= 1.0


def test_ignition_weights_sum_to_one():
    """Weights 0.30+0.25+0.20+0.15+0.10 = 1.0 → max score from uniform 1.0 inputs = 1.0."""
    score = compute_ignition_score(
        oi_zscore_1h=1.0,
        volume_zscore_1h=1.0,
        short_liq_share=1.0,
        breakout_from_compression=1.0,
        exchange_breadth=1.0,
    )
    assert abs(score - 1.0) < 1e-6


# ── crowding_late_score ────────────────────────────────────────────────────

def test_crowding_score_extreme_funding():
    """Extreme funding + high crowding → high crowding_late_score."""
    score = compute_crowding_late_score(
        funding_extreme=0.95,
        crowding_risk=0.90,
        long_short_ratio_extreme=0.85,
        distribution_score=0.80,
        liq_after_spike=0.70,
    )
    assert score >= 0.80


def test_crowding_score_normal():
    """Normal market → low crowding_late_score."""
    score = compute_crowding_late_score(
        funding_extreme=0.05,
        crowding_risk=0.10,
        long_short_ratio_extreme=0.05,
        distribution_score=0.10,
        liq_after_spike=0.05,
    )
    assert score < 0.15


# ── compute_perp_scores ────────────────────────────────────────────────────

def test_compute_perp_scores_returns_all_keys():
    """compute_perp_scores always returns all three score keys."""
    metrics_raw = {
        "liquidation_burst_score": 0.7,
        "funding_rate": 0.0003,
        "long_short_ratio": 1.8,
        "distribution_score": 0.3,
        "taker_buy_sell_imbalance": 0.2,
        "oi_change_4h": 0.05,
    }
    pct = {
        "derivatives_heat": 0.8,
        "volume_burst": 0.7,
        "impulse_after_compression": 0.6,
        "return_shock": 0.7,
        "flow_confirmation": 0.5,
        "crowding_risk": 0.3,
        "liquidity_trap": 0.2,
    }
    derivatives = {"funding_rate": 0.0003, "long_short_ratio": 1.8, "taker_buy_sell_imbalance": 0.2}

    result = compute_perp_scores(metrics_raw, pct, derivatives)
    assert "ignition_score" in result
    assert "continuation_score" in result
    assert "crowding_late_score" in result
    for v in result.values():
        assert 0.0 <= v <= 1.0


def test_compute_perp_scores_no_derivatives():
    """When no derivatives data, all perp scores should be reduced."""
    metrics_raw = {}
    pct = {k: 0.0 for k in ["derivatives_heat", "volume_burst", "impulse_after_compression",
                              "return_shock", "flow_confirmation", "crowding_risk", "liquidity_trap"]}
    result = compute_perp_scores(metrics_raw, pct, {})
    assert result["ignition_score"] == 0.0   # exchange_breadth = 0.0 → contribution absent


# ── classify_signal_source ─────────────────────────────────────────────────

def test_classify_signal_source_perp_ignition():
    src = classify_signal_source(
        ignition_score=0.75,
        continuation_score=0.40,
        crowding_late_score=0.30,
        derivatives_present=True,
    )
    assert src == "perp_ignition"


def test_classify_signal_source_crowded():
    src = classify_signal_source(
        ignition_score=0.50,
        continuation_score=0.60,
        crowding_late_score=0.80,
        derivatives_present=True,
    )
    assert src == "crowded_late_stage"


def test_classify_signal_source_no_derivatives():
    src = classify_signal_source(
        ignition_score=0.90,
        continuation_score=0.90,
        crowding_late_score=0.10,
        derivatives_present=False,
    )
    assert src == ""


def test_classify_signal_source_continuation():
    src = classify_signal_source(
        ignition_score=0.35,
        continuation_score=0.65,
        crowding_late_score=0.20,
        derivatives_present=True,
    )
    assert src == "perp_continuation"


def test_classify_signal_source_empty_below_thresholds():
    src = classify_signal_source(
        ignition_score=0.30,
        continuation_score=0.40,
        crowding_late_score=0.20,
        derivatives_present=True,
    )
    assert src == ""
