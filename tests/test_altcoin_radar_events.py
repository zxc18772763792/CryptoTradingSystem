"""Unit tests for core/research/altcoin_radar_events.py — Phase 1."""
import time

import pytest

from core.research.altcoin_radar_events import (
    EVENT_CROWDING_SPIKE,
    EVENT_IGNITION_CROSS_UP,
    EVENT_RANK_JUMP,
    _events,
    _rank_history,
    bulk_update_ranks,
    compute_rank_jump_score,
    get_all_recent_events,
    get_recent_symbol_events,
    get_rank_history,
    prune_old_events,
    record_crowding_spike,
    record_ignition_cross_up,
    record_rank_jump_event,
    update_rank_snapshot,
)


def _clear():
    """Clear in-memory state for test isolation."""
    _rank_history.clear()
    _events.clear()


# ── rank history ───────────────────────────────────────────────────────────

def test_update_and_retrieve_rank_history():
    _clear()
    update_rank_snapshot("BTC/USDT", rank=5)
    history = get_rank_history("BTC/USDT")
    assert len(history) == 1
    assert history[0]["rank"] == 5


def test_rank_history_caps_at_max():
    _clear()
    from core.research.altcoin_radar_events import MAX_HISTORY_PER_SYMBOL
    for i in range(MAX_HISTORY_PER_SYMBOL + 5):
        update_rank_snapshot("ETH/USDT", rank=i + 1)
    history = get_rank_history("ETH/USDT")
    assert len(history) <= MAX_HISTORY_PER_SYMBOL


def test_rank_history_symbol_normalized():
    _clear()
    update_rank_snapshot("sol/usdt", rank=3)
    history = get_rank_history("SOL/USDT")
    assert len(history) == 1


# ── rank_jump_score ────────────────────────────────────────────────────────

def test_rank_jump_score_no_history():
    _clear()
    score = compute_rank_jump_score("XRP/USDT", current_rank=2)
    assert score == 0.0


def test_rank_jump_score_no_old_entries():
    """All history is recent → no baseline → score=0."""
    _clear()
    update_rank_snapshot("LINK/USDT", rank=10)
    score = compute_rank_jump_score("LINK/USDT", current_rank=2)
    assert score == 0.0  # no entry older than lookback_sec


def test_rank_jump_score_with_jump():
    """Insert an old entry (rank=20), current rank=2 → big jump → nonzero score."""
    _clear()
    sym = "AVAX/USDT"
    # Manually insert an old snapshot by patching ts
    from core.research.altcoin_radar_events import _rank_history, _lock
    old_ts = time.time() - 1000  # definitely older than default 900s lookback
    with _lock:
        _rank_history[sym] = [{"ts": old_ts, "rank": 25}]

    score = compute_rank_jump_score(sym, current_rank=2, top_n=10, lookback_sec=900)
    assert score > 0.0, f"Expected non-zero jump score, got {score}"


def test_rank_jump_score_small_jump_no_reward():
    """Jump of 1 position is below min_jump=3 → score=0."""
    _clear()
    sym = "DOT/USDT"
    from core.research.altcoin_radar_events import _rank_history, _lock
    old_ts = time.time() - 1000
    with _lock:
        _rank_history[sym] = [{"ts": old_ts, "rank": 5}]
    score = compute_rank_jump_score(sym, current_rank=4, min_jump=3)
    assert score == 0.0


# ── event recording ────────────────────────────────────────────────────────

def test_record_ignition_cross_up_fires():
    _clear()
    event = record_ignition_cross_up("ORDI/USDT", ignition_score=0.70, prev_ignition_score=0.55)
    assert event is not None
    assert event["event_type"] == EVENT_IGNITION_CROSS_UP
    assert event["symbol"] == "ORDI/USDT"


def test_record_ignition_cross_up_no_fire_if_already_above():
    _clear()
    event = record_ignition_cross_up("ORDI/USDT", ignition_score=0.70, prev_ignition_score=0.65, threshold=0.60)
    assert event is None  # prev already above threshold


def test_record_ignition_cross_up_no_fire_if_below():
    _clear()
    event = record_ignition_cross_up("BNB/USDT", ignition_score=0.50, prev_ignition_score=0.40)
    assert event is None  # both below threshold


def test_record_rank_jump_fires_in_top_n():
    _clear()
    event = record_rank_jump_event("SUI/USDT", current_rank=5, prev_rank=15, top_n=10)
    assert event is not None
    assert event["event_type"] == EVENT_RANK_JUMP
    assert event["rank_jump"] == 10


def test_record_rank_jump_no_fire_outside_top_n():
    _clear()
    event = record_rank_jump_event("SUI/USDT", current_rank=20, prev_rank=28, top_n=10)
    assert event is None


def test_record_crowding_spike_fires():
    _clear()
    event = record_crowding_spike("BTC/USDT", crowding_late_score=0.80)
    assert event is not None
    assert event["event_type"] == EVENT_CROWDING_SPIKE


def test_record_crowding_spike_no_fire_below_threshold():
    _clear()
    event = record_crowding_spike("BTC/USDT", crowding_late_score=0.40)
    assert event is None


# ── event retrieval ────────────────────────────────────────────────────────

def test_get_recent_symbol_events():
    _clear()
    record_ignition_cross_up("PEPE/USDT", 0.70, 0.50)
    record_ignition_cross_up("ORDI/USDT", 0.75, 0.50)
    events = get_recent_symbol_events("PEPE/USDT")
    assert all(e["symbol"] == "PEPE/USDT" for e in events)


def test_get_all_recent_events_limit():
    _clear()
    for i in range(10):
        record_crowding_spike(f"COIN{i}/USDT", crowding_late_score=0.70)
    events = get_all_recent_events(limit=5)
    assert len(events) <= 5


def test_prune_removes_old_events():
    _clear()
    # Force an old event by inserting directly
    from core.research.altcoin_radar_events import _events, _lock
    old_ts = time.time() - 10000
    with _lock:
        _events.append({"event_type": "test", "symbol": "OLD/USDT", "ts": old_ts})
    record_crowding_spike("NEW/USDT", crowding_late_score=0.70)
    removed = prune_old_events(max_age_sec=5000)
    assert removed >= 1


# ── bulk_update_ranks ──────────────────────────────────────────────────────

def test_bulk_update_ranks_updates_multiple():
    _clear()
    rows = [
        {"symbol": "A/USDT", "rank": 1, "ignition_score": 0.8, "layout_score": 0.7, "alert_score": 0.5, "crowding_late_score": 0.1},
        {"symbol": "B/USDT", "rank": 2, "ignition_score": 0.4, "layout_score": 0.5, "alert_score": 0.4, "crowding_late_score": 0.2},
    ]
    bulk_update_ranks(rows)
    assert len(get_rank_history("A/USDT")) == 1
    assert len(get_rank_history("B/USDT")) == 1
