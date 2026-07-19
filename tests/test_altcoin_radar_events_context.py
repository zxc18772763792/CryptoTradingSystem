"""Context isolation tests for radar rank/ignition history (phantom-alert fix)."""
from __future__ import annotations

import core.research.altcoin_radar_events as events


def _reset_state():
    with events._lock:  # noqa: SLF001 - test-only reset of module cache
        events._rank_history.clear()
        events._events.clear()


def test_rank_history_is_isolated_per_context():
    _reset_state()
    events.update_rank_snapshot("AAA/USDT", 3, {"ignition_score": 0.9}, context="15m")
    assert events.get_rank_history("AAA/USDT", context="15m")
    assert events.get_rank_history("AAA/USDT", context="4h") == []
    assert events.get_rank_history("AAA/USDT") == []


def test_rank_jump_score_does_not_leak_across_contexts():
    _reset_state()
    stale_ts = events._now_ts() - 1200.0
    with events._lock:  # noqa: SLF001 - inject an aged snapshot directly
        events._rank_history[events._history_key("BBB/USDT", "15m")] = [
            {"ts": stale_ts, "rank": 25, "ignition_score": 0.1}
        ]
    same_context = events.compute_rank_jump_score("BBB/USDT", 2, context="15m")
    other_context = events.compute_rank_jump_score("BBB/USDT", 2, context="4h")
    assert same_context > 0.0
    assert other_context == 0.0


def test_bulk_update_ranks_threads_context():
    _reset_state()
    rows = [{"symbol": "CCC/USDT", "rank": 1, "ignition_score": 0.5}]
    events.bulk_update_ranks(rows, context="4h")
    assert events.get_rank_history("CCC/USDT", context="4h")
    assert events.get_rank_history("CCC/USDT", context="15m") == []
