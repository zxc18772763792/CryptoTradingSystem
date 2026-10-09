from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import core.ai.autonomous_agent as module
from core.utils.openai_responses import should_failover_openai_response

CFG = {"strategy_name": "AI_AutonomousAgent", "reentry_cooldown_sec": 4 * 3600, "chase_max_move_24h": 0.04}


def _closed(symbol, minutes_ago, strategy="AI_AutonomousAgent"):
    return SimpleNamespace(symbol=symbol, strategy=strategy, metadata={},
                           updated_at=datetime.now(timezone.utc) - timedelta(minutes=minutes_ago))


def _history(monkeypatch, rows):
    monkeypatch.setattr(module.position_manager, "get_closed_positions", lambda limit=None, scope=None: list(rows))


@pytest.fixture
def agent(tmp_path, monkeypatch):
    monkeypatch.setenv("AI_AGENT_CONFIG_PATH", str(tmp_path / "agent.json"))
    _history(monkeypatch, [])
    monkeypatch.setattr(module, "_exchange_notice", lambda symbol: None)
    return module.AutonomousTradingAgent(cache_root=tmp_path)


def test_reentry_cooldown_blocks_the_same_coin_until_it_expires(agent, monkeypatch):
    _history(monkeypatch, [_closed("FIL/USDT", 300), _closed("FIL/USDT", 2), _closed("ETH/USDT", 1, strategy="Grid")])

    reason = agent._entry_filter_reason(cfg=CFG, symbol="FIL/USDT", side="long", r_24h=0.0)
    assert reason == "reentry_cooldown(FIL/USDT:238m)"  # latest close wins; either side is blocked
    assert agent._entry_filter_reason(cfg=CFG, symbol="FIL/USDT", side="short", r_24h=0.0) == reason
    # another strategy's close does not block the agent
    assert agent._entry_filter_reason(cfg=CFG, symbol="ETH/USDT", side="long", r_24h=0.0) is None

    _history(monkeypatch, [_closed("FIL/USDT", 241)])
    assert agent._entry_filter_reason(cfg=CFG, symbol="FIL/USDT", side="long", r_24h=0.0) is None


def test_chase_guard_is_side_adjusted(agent):
    assert agent._entry_filter_reason(cfg=CFG, symbol="SOL/USDT", side="long", r_24h=0.05) == "chase_guard(SOL/USDT:long:24h+5.0%>4%)"
    assert agent._entry_filter_reason(cfg=CFG, symbol="SOL/USDT", side="short", r_24h=-0.06) == "chase_guard(SOL/USDT:short:24h+6.0%>4%)"
    # fading a move is not chasing it, and moves inside the limit pass
    assert agent._entry_filter_reason(cfg=CFG, symbol="SOL/USDT", side="short", r_24h=0.05) is None
    assert agent._entry_filter_reason(cfg=CFG, symbol="SOL/USDT", side="long", r_24h=0.039) is None


def test_filters_are_off_without_configuration(agent, monkeypatch):
    _history(monkeypatch, [_closed("FIL/USDT", 1)])
    assert agent._entry_filter_reason(cfg={"strategy_name": "AI_AutonomousAgent"}, symbol="FIL/USDT", side="long", r_24h=0.5) is None


def test_runtime_config_carries_the_registered_defaults(agent):
    cfg = agent.get_runtime_config()
    assert cfg["reentry_cooldown_sec"] == 4 * 3600
    assert cfg["chase_max_move_24h"] == pytest.approx(0.04)
    assert "AI_AUTONOMOUS_AGENT_REENTRY_COOLDOWN_SEC" in module._AGENT_PERSISTABLE_KEYS
    assert "AI_AUTONOMOUS_AGENT_CHASE_MAX_MOVE_24H" in module._AGENT_PERSISTABLE_KEYS


def test_fresh_entry_guard_filters_only_fresh_entries(agent, monkeypatch):
    _history(monkeypatch, [_closed("FIL/USDT", 5)])
    ctx = {"symbol": "FIL/USDT", "returns": {"r_24h": 0.0}, "position": {}, "account_risk": {}}
    assert agent._fresh_entry_guard_reason(cfg=dict(CFG), context_payload=ctx, side="long").startswith("reentry_cooldown(FIL/USDT:")
    ctx["position"] = {"side": "long"}  # managing an open position is not a fresh entry
    assert agent._fresh_entry_guard_reason(cfg=dict(CFG), context_payload=ctx, side="long") is None


def test_fast_path_holds_without_a_model_call_when_the_entry_would_chase(agent):
    ctx = {"symbol": "SOL/USDT", "returns": {"r_24h": 0.08}, "position": {}, "account_risk": {},
           "aggregated_signal": {"direction": "LONG", "confidence": 0.9}}
    hold = agent._maybe_build_fast_path_hold(cfg=dict(CFG, min_confidence=0.58, mode="shadow"), context_payload=ctx)
    assert hold["action"] == "hold"
    assert hold["reason"] == "chase_guard(SOL/USDT:long:24h+8.0%>4%)"


def test_symbol_ranking_steers_away_from_filtered_candidates(agent, monkeypatch):
    _history(monkeypatch, [_closed("FIL/USDT", 5)])
    row = {"symbol": "FIL/USDT", "direction": "LONG", "score": 0.9, "tradable_now": True,
           "has_position": False, "r_24h": 0.0, "summary": "LONG 0.900"}
    adjusted = agent._apply_learning_score_adjustments(row=row, cfg=dict(CFG))
    assert adjusted["tradable_now"] is False
    assert adjusted["score"] == pytest.approx(0.55)
    assert "reentry_cooldown(FIL/USDT:" in adjusted["summary"]

    held = agent._apply_learning_score_adjustments(row=dict(row, has_position=True), cfg=dict(CFG))
    assert held["tradable_now"] is True and held["score"] == pytest.approx(0.9)


def test_endpoint_that_rejects_no_reasoning_gets_low_from_the_next_call(monkeypatch):
    monkeypatch.setattr(module, "_REASONING_EFFORT_OVERRIDES", {})
    base = {"model": "x", "messages": [], "reasoning_effort": "none"}
    plain = {"chat_options": {}}
    assert module._chat_payload_for_target(base, plain, "https://b", "gpt-5.6-sol")["reasoning_effort"] == "none"

    body = '{"error":{"message":"gpt-6.1-sol does not support reasoning effort \\"none\\"; use low, medium, high, xhigh or max"}}'
    module._note_reasoning_effort_rejection("https://b", "gpt-5.6-sol", 400, body, base)
    assert module._chat_payload_for_target(base, plain, "https://b", "gpt-5.6-sol")["reasoning_effort"] == "low"

    # other endpoints, unrelated 400s and target chat options are untouched
    module._note_reasoning_effort_rejection("https://a", "deepseek", 400, '{"error":"bad json"}', base)
    deepseek = module._chat_payload_for_target(base, {"chat_options": {"thinking": {"type": "disabled"}}}, "https://a", "deepseek")
    assert deepseek["reasoning_effort"] == "none" and deepseek["thinking"] == {"type": "disabled"}


def test_billing_errors_fail_over_but_ordinary_bad_requests_do_not():
    assert should_failover_openai_response(400, '{"error":{"message":"credit insufficient balance: balance=0 required=884"}}')
    assert should_failover_openai_response(402, "")
    assert should_failover_openai_response(503, "")
    assert not should_failover_openai_response(400, '{"error":{"message":"does not support reasoning effort"}}')
    assert not should_failover_openai_response(404, "not found")
