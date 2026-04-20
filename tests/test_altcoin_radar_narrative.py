"""Unit tests for core/research/altcoin_radar_narrative.py — Phase 2."""
import pytest

from core.research.altcoin_radar_narrative import (
    _HIGH_HEAT_SECTORS,
    _MEDIUM_HEAT_SECTORS,
    classify_narrative_source,
    compute_meme_rotation_score,
    compute_narrative_heat_score,
    compute_narrative_scores,
    _derive_meme_rotation_inputs,
    _derive_narrative_inputs,
)


# ── narrative_heat_score ───────────────────────────────────────────────────

def test_narrative_heat_score_high_all():
    """All inputs near 1.0 → near-maximum narrative heat score."""
    score = compute_narrative_heat_score(
        board_heat=0.9,
        news_burst=0.85,
        community_flow=0.80,
        tx_count_acceleration=0.75,
        active_address_acceleration=0.70,
    )
    assert score >= 0.80, f"Expected high narrative heat, got {score}"


def test_narrative_heat_score_low_all():
    """All inputs near zero → near-zero narrative heat score."""
    score = compute_narrative_heat_score(
        board_heat=0.05,
        news_burst=0.05,
        community_flow=0.0,
        tx_count_acceleration=0.0,
        active_address_acceleration=0.0,
    )
    assert score < 0.10, f"Expected low narrative heat, got {score}"


def test_narrative_heat_score_clamped():
    """Score is in [0, 1] even with out-of-range inputs."""
    score = compute_narrative_heat_score(
        board_heat=5.0,
        news_burst=5.0,
        community_flow=5.0,
        tx_count_acceleration=5.0,
        active_address_acceleration=5.0,
    )
    assert 0.0 <= score <= 1.0


def test_narrative_heat_weights_sum_to_one():
    """Weights 0.30+0.25+0.20+0.15+0.10=1.0 → uniform 1.0 inputs → score=1.0."""
    score = compute_narrative_heat_score(
        board_heat=1.0,
        news_burst=1.0,
        community_flow=1.0,
        tx_count_acceleration=1.0,
        active_address_acceleration=1.0,
    )
    assert abs(score - 1.0) < 1e-6


# ── meme_rotation_score ────────────────────────────────────────────────────

def test_meme_rotation_score_high_watchlist():
    """Watchlist member in high-heat sector + volume spike → high score."""
    score = compute_meme_rotation_score(
        sector_breadth=0.90,
        watchlist_hit=1.0,
        turnover_spike=0.85,
        market_cap_elasticity=0.70,
        short_term_dominance=0.75,
    )
    assert score >= 0.75, f"Expected high meme rotation, got {score}"


def test_meme_rotation_score_low_all():
    """All inputs near zero → near-zero score."""
    score = compute_meme_rotation_score(
        sector_breadth=0.0,
        watchlist_hit=0.0,
        turnover_spike=0.02,
        market_cap_elasticity=0.0,
        short_term_dominance=0.0,
    )
    assert score < 0.05, f"Expected low rotation, got {score}"


def test_meme_rotation_weights_sum_to_one():
    """Weights 0.30+0.25+0.20+0.15+0.10=1.0 → uniform 1.0 inputs → score=1.0."""
    score = compute_meme_rotation_score(
        sector_breadth=1.0,
        watchlist_hit=1.0,
        turnover_spike=1.0,
        market_cap_elasticity=1.0,
        short_term_dominance=1.0,
    )
    assert abs(score - 1.0) < 1e-6


# ── _derive_narrative_inputs ───────────────────────────────────────────────

def test_derive_narrative_inputs_high_heat_sector():
    """Meme sector in watchlist → board_heat near 1.0."""
    metrics_raw = {}
    pct = {"announcements": 0.8, "community_flow": 0.7, "volume_burst": 0.6, "whale_context": 0.5}
    result = _derive_narrative_inputs(metrics_raw, pct, sector="meme", in_watchlist=True)
    assert result["board_heat"] == 1.0  # 0.80 + 0.20 = 1.0, clamped
    assert result["news_burst"] == pytest.approx(0.8, abs=1e-4)
    assert result["community_flow"] == pytest.approx(0.7, abs=1e-4)


def test_derive_narrative_inputs_no_sector():
    """No sector + not in watchlist → board_heat = 0.0."""
    result = _derive_narrative_inputs({}, {}, sector="", in_watchlist=False)
    assert result["board_heat"] == 0.0


def test_derive_narrative_inputs_medium_sector_no_watchlist():
    """L2 sector, not in watchlist → board_heat = 0.45."""
    result = _derive_narrative_inputs({}, {}, sector="l2", in_watchlist=False)
    assert result["board_heat"] == pytest.approx(0.45, abs=1e-4)


def test_derive_meme_rotation_inputs_high_heat():
    """Meme sector in watchlist → sector_breadth=0.90, watchlist_hit=1.0."""
    metrics_raw = {"market_cap_usd": 1e8}  # $100M cap
    pct = {"volume_burst": 0.8, "return_shock": 0.7}
    result = _derive_meme_rotation_inputs(metrics_raw, pct, sector="meme", in_watchlist=True)
    assert result["sector_breadth"] == pytest.approx(0.90, abs=1e-4)
    assert result["watchlist_hit"] == 1.0
    assert result["turnover_spike"] == pytest.approx(0.8, abs=1e-4)


# ── compute_narrative_scores ───────────────────────────────────────────────

def test_compute_narrative_scores_returns_all_keys():
    """compute_narrative_scores always returns both score keys."""
    pct = {k: 0.5 for k in ["announcements", "community_flow", "volume_burst",
                              "whale_context", "return_shock"]}
    result = compute_narrative_scores({}, pct, sector="meme", in_watchlist=True)
    assert "narrative_heat_score" in result
    assert "meme_rotation_score" in result
    for v in result.values():
        assert 0.0 <= v <= 1.0


def test_compute_narrative_scores_watchlist_boosts_heat():
    """In-watchlist meme coin with moderate activity → narrative_heat > 0.4."""
    pct = {k: 0.4 for k in ["announcements", "community_flow", "volume_burst",
                              "whale_context", "return_shock"]}
    result = compute_narrative_scores({}, pct, sector="meme", in_watchlist=True)
    # board_heat = 1.0 (meme 0.80 + watchlist 0.20), other inputs 0.4
    # score = 0.30*1.0 + 0.25*0.4 + 0.20*0.4 + 0.15*0.4 + 0.10*0.4 = 0.30 + 0.28 = 0.58
    assert result["narrative_heat_score"] >= 0.40


def test_compute_narrative_scores_no_sector_low():
    """No sector, not in watchlist, all pct zero → near-zero scores."""
    result = compute_narrative_scores({}, {}, sector="", in_watchlist=False)
    assert result["narrative_heat_score"] < 0.10
    assert result["meme_rotation_score"] < 0.05


# ── classify_narrative_source ──────────────────────────────────────────────

def test_classify_narrative_source_ignition():
    """High narrative heat → narrative_ignition."""
    src = classify_narrative_source(
        narrative_heat_score=0.70,
        meme_rotation_score=0.60,
    )
    assert src == "narrative_ignition"


def test_classify_narrative_source_ignition_at_threshold():
    """narrative_heat == 0.55 → narrative_ignition (at threshold)."""
    src = classify_narrative_source(
        narrative_heat_score=0.55,
        meme_rotation_score=0.20,
    )
    assert src == "narrative_ignition"


def test_classify_narrative_source_confirmation():
    """Moderate heat + decent rotation → narrative_confirmation."""
    src = classify_narrative_source(
        narrative_heat_score=0.45,
        meme_rotation_score=0.40,
    )
    assert src == "narrative_confirmation"


def test_classify_narrative_source_below_thresholds():
    """All below thresholds → empty string."""
    src = classify_narrative_source(
        narrative_heat_score=0.30,
        meme_rotation_score=0.20,
    )
    assert src == ""


def test_classify_narrative_source_moderate_heat_low_rotation():
    """Moderate heat but low rotation → empty (no confirmation)."""
    src = classify_narrative_source(
        narrative_heat_score=0.45,
        meme_rotation_score=0.20,
    )
    assert src == ""


# ── event integration ──────────────────────────────────────────────────────

def test_record_narrative_heat_spike_fires():
    """narrative_heat_score >= 0.55 → event recorded."""
    from core.research.altcoin_radar_events import (
        EVENT_NARRATIVE_HEAT_SPIKE,
        _events,
        _lock,
        record_narrative_heat_spike,
    )
    with _lock:
        _events.clear()
    event = record_narrative_heat_spike("PEPE/USDT", 0.70)
    assert event is not None
    assert event["event_type"] == EVENT_NARRATIVE_HEAT_SPIKE
    assert event["symbol"] == "PEPE/USDT"
    assert event["narrative_heat_score"] == pytest.approx(0.70, abs=1e-3)


def test_record_narrative_heat_spike_no_fire():
    """Below threshold → no event."""
    from core.research.altcoin_radar_events import record_narrative_heat_spike
    event = record_narrative_heat_spike("PEPE/USDT", 0.40)
    assert event is None


def test_high_heat_sectors_constant():
    """Verify expected sectors are in _HIGH_HEAT_SECTORS."""
    assert "meme" in _HIGH_HEAT_SECTORS
    assert "ordinals" in _HIGH_HEAT_SECTORS
    assert "ai" in _HIGH_HEAT_SECTORS
    assert "l2" not in _HIGH_HEAT_SECTORS
