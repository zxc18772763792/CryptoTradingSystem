from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.ai.autonomous_learning import build_blocked_symbol_side_map, build_learning_memory


def _iso(hours_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def test_build_learning_memory_raises_guards_after_losses_and_outages():
    journal_rows = [
        {
            "timestamp": _iso(2),
            "latency_ms": 92000,
            "rejection_reason": "model_error:codex_http_503",
            "decision": {"action": "hold", "reason": "model_error:codex_http_503"},
            "context": {
                "price": 8.41,
                "market_structure": {"available": True},
                "research_context": {"available": False},
            },
            "execution": {"submitted": False},
        },
        {
            "timestamp": _iso(1),
            "latency_ms": 98000,
            "rejection_reason": "no_price",
            "decision": {"action": "hold", "reason": "no_price"},
            "context": {
                "price": 0.0,
                "market_structure": {"available": False},
                "research_context": {"available": False},
            },
            "execution": {"submitted": False},
        },
        {
            "timestamp": _iso(0.5),
            "latency_ms": 78000,
            "decision": {"action": "sell", "reason": "fresh_short"},
            "context": {
                "price": 8.427,
                "market_structure": {"available": True},
                "research_context": {"available": False},
            },
            "execution": {"submitted": True},
        },
    ]

    live_review = {
        "items": [
            {
                "timestamp": _iso(6),
                "action": "open_or_add",
                "symbol": "LINK/USDT",
                "side": "sell",
                "signal": {"signal_type": "sell"},
                "pnl": 0.0,
            },
            {
                "timestamp": _iso(4),
                "action": "close",
                "symbol": "LINK/USDT",
                "side": "buy",
                "signal": {"signal_type": "close_short"},
                "pnl": -1.42,
            },
        ]
    }

    positions = [
        {
            "symbol": "LINK/USDT",
            "side": "short",
            "strategy": "AI_AutonomousAgent",
            "unrealized_pnl": -3.07,
            "unrealized_pnl_pct": -0.0036,
            "updated_at": _iso(0.1),
        }
    ]

    memory = build_learning_memory(
        journal_rows=journal_rows,
        live_review=live_review,
        positions=positions,
        base_min_confidence=0.58,
    )

    adaptive = memory["adaptive_risk"]
    assert adaptive["effective_min_confidence"] > 0.58
    assert adaptive["same_direction_max_exposure_ratio"] < 0.5
    assert adaptive["entry_size_scale"] < 1.0
    assert adaptive["force_close_on_data_outage_losing_position"] is True
    assert adaptive["require_research_for_new_entries"] is False
    assert memory["summary"]["recent_model_issue_count"] == 1
    assert memory["summary"]["recent_no_price_count"] == 1
    assert memory["summary"]["recent_researchless_entry_count"] == 0
    assert memory["summary"]["current_open_losing_count"] == 1
    blocked = build_blocked_symbol_side_map(memory, base_min_confidence=0.58)
    assert ("LINK/USDT", "short") in blocked


def test_build_learning_memory_blocks_fresh_entries_during_recent_loss_streak():
    live_review = {
        "items": [
            {
                "timestamp": _iso(9),
                "action": "open_or_add",
                "symbol": "BTC/USDT",
                "side": "buy",
                "signal": {"signal_type": "buy"},
                "pnl": 0.0,
            },
            {
                "timestamp": _iso(8),
                "action": "close",
                "symbol": "BTC/USDT",
                "side": "sell",
                "signal": {"signal_type": "close_long"},
                "pnl": -0.8,
            },
            {
                "timestamp": _iso(6),
                "action": "open_or_add",
                "symbol": "ETH/USDT",
                "side": "sell",
                "signal": {"signal_type": "sell"},
                "pnl": 0.0,
            },
            {
                "timestamp": _iso(5),
                "action": "close",
                "symbol": "ETH/USDT",
                "side": "buy",
                "signal": {"signal_type": "close_short"},
                "pnl": -1.1,
            },
            {
                "timestamp": _iso(3),
                "action": "open_or_add",
                "symbol": "SOL/USDT",
                "side": "buy",
                "signal": {"signal_type": "buy"},
                "pnl": 0.0,
            },
            {
                "timestamp": _iso(2),
                "action": "close",
                "symbol": "SOL/USDT",
                "side": "sell",
                "signal": {"signal_type": "close_long"},
                "pnl": -0.6,
            },
        ]
    }

    memory = build_learning_memory(
        journal_rows=[],
        live_review=live_review,
        positions=[],
        base_min_confidence=0.58,
    )

    assert memory["summary"]["recent_close_loss_streak_count"] == 3
    assert memory["adaptive_risk"]["avoid_new_entries_during_loss_streak"] is True
    assert "block fresh entries during active loss streak" in memory["guardrails"]
