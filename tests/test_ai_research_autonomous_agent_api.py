from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError


def test_runtime_config_contains_ai_autonomous_agent(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.live_decision_router, "get_runtime_config", lambda: {"enabled": False})
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"enabled": True, "mode": "shadow", "provider": "glm"},
    )

    result = asyncio.run(ai_module.get_ai_runtime_config(request))
    assert "ai_autonomous_agent" in result
    assert result["ai_autonomous_agent"]["provider"] == "glm"


def test_update_autonomous_agent_runtime_config_endpoint(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    async def _fake_update(**kwargs):
        return {
            "enabled": bool(kwargs.get("enabled")),
            "mode": str(kwargs.get("mode") or "shadow"),
            "provider": str(kwargs.get("provider") or "glm"),
        }

    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "update_runtime_config",
        _fake_update,
    )

    payload = ai_module.AIAutonomousAgentConfigUpdateRequest(
        enabled=True,
        mode="execute",
        provider="codex",
        symbol_mode="auto",
        universe_symbols=["BTC/USDT", "ETH/USDT"],
        selection_top_n=8,
    )
    result = asyncio.run(ai_module.update_ai_autonomous_agent_runtime_config(request, payload))
    assert result["updated"] is True
    assert result["config"]["enabled"] is True
    assert result["config"]["mode"] == "execute"
    assert result["config"]["provider"] == "codex"


def test_update_autonomous_agent_runtime_config_payload_rejects_non_one_leverage():
    from web.api import ai_agent as ai_module

    with pytest.raises(ValidationError):
        ai_module.AIAutonomousAgentConfigUpdateRequest(default_leverage=2.0)


def test_update_autonomous_agent_runtime_config_payload_rejects_auto_start():
    from web.api import ai_agent as ai_module

    with pytest.raises(ValidationError):
        ai_module.AIAutonomousAgentConfigUpdateRequest(auto_start=True)


def test_autonomous_agent_start_and_run_once_endpoints(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    update_mock = AsyncMock(return_value={"enabled": True})
    start_mock = AsyncMock(return_value={"running": True})
    run_once_mock = AsyncMock(return_value={"decision": {"action": "hold"}})
    status_mock = lambda: {"running": True}

    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "update_runtime_config", update_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "start", start_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "trigger_run_once", run_once_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "get_status", status_mock)
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"enabled": True},
    )

    start_result = asyncio.run(
        ai_module.start_ai_autonomous_agent(
            request,
            ai_module.AIAutonomousAgentStartRequest(enable=True),
        )
    )
    assert start_result["started"] is True
    assert update_mock.await_count == 1
    assert start_mock.await_count == 1

    once_result = asyncio.run(
        ai_module.run_ai_autonomous_agent_once(
            request,
            ai_module.AIAutonomousAgentRunOnceRequest(force=True),
        )
    )
    assert once_result["decision"]["action"] == "hold"
    assert run_once_mock.await_count == 1


def test_autonomous_agent_symbol_ranking_endpoint(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    scan_mock = AsyncMock(
        return_value={
            "generated_at": "2026-01-01T00:00:00+00:00",
            "symbol_mode": "auto",
            "configured_symbol": "BTC/USDT",
            "selected_symbol": "ETH/USDT",
            "selection_reason": "top_ranked_tradable_symbol",
            "candidate_count": 2,
            "top_n": 10,
            "top_candidates": [
                {"rank": 1, "symbol": "ETH/USDT", "score": 0.88},
                {"rank": 2, "symbol": "BTC/USDT", "score": 0.51},
            ],
        }
    )
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_symbol_scan_preview",
        scan_mock,
    )

    result = asyncio.run(ai_module.get_ai_autonomous_agent_symbol_ranking(request, limit=10, refresh=True))
    assert result["selected_symbol"] == "ETH/USDT"
    assert scan_mock.await_count == 1


def test_autonomous_agent_status_endpoint_does_not_require_ai_research_runtime(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    def _unexpected_ensure(app):
        raise AssertionError("autonomous-agent status should not require ai research runtime")

    monkeypatch.setattr(ai_module.ai_research_module, "ensure_ai_research_runtime_state", _unexpected_ensure)
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"symbol_mode": "manual"},
    )
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "get_status", lambda: {"running": True})

    result = asyncio.run(ai_module.get_ai_autonomous_agent_status(request))
    assert result["status"]["running"] is True


def test_autonomous_agent_live_signals_endpoint_does_not_require_ai_research_runtime(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    def _unexpected_ensure(app):
        raise AssertionError("autonomous-agent live signals should not require ai research runtime")

    payload = {
        "sections": [],
        "candidate_items": [],
        "watchlist_items": [],
        "items": [],
        "candidate_count": 0,
        "watchlist_count": 0,
        "count": 0,
        "ml_model_loaded": False,
        "ts": "2026-04-02T00:00:00+00:00",
    }

    monkeypatch.setattr(ai_module.ai_research_module, "ensure_ai_research_runtime_state", _unexpected_ensure)
    monkeypatch.setattr(
        ai_module.ai_research_module,
        "_build_autonomous_watchlist_live_signals_payload",
        AsyncMock(return_value=payload),
    )

    result = asyncio.run(ai_module.get_autonomous_agent_live_signals(request))
    assert result["watchlist_count"] == 0
    assert result["count"] == 0


def test_autonomous_agent_review_endpoint_includes_learning_memory(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(
        ai_module.ai_research_module,
        "_build_autonomous_agent_review",
        lambda limit=12: {"summary": {"submitted_count": 0}, "insights": [], "items": []},
    )
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_learning_memory",
        lambda force=False: {
            "adaptive_risk": {"effective_min_confidence": 0.66},
            "lessons": ["近期样本偏弱，抬高开仓门槛。"],
        },
    )

    result = asyncio.run(ai_module.get_ai_autonomous_agent_review(request, limit=8))
    assert result["summary"]["submitted_count"] == 0
    assert result["learning_memory"]["adaptive_risk"]["effective_min_confidence"] == 0.66


def test_live_signals_gracefully_degrades_when_symbol_scan_times_out(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    monkeypatch.setattr(ai_module, "_build_live_signal_watchlist_symbols", lambda runtime_cfg, selection: [])
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "get_runtime_config", lambda: {"exchange": "binance"})
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_symbol_scan",
        AsyncMock(return_value={"selected_symbol": "BTC/USDT"}),
    )

    async def fake_wait_for(awaitable, timeout):
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise asyncio.TimeoutError("scan timed out")

    monkeypatch.setattr(ai_module.asyncio, "wait_for", fake_wait_for)

    result = asyncio.run(ai_module.get_live_signals(request, symbol="BTC/USDT"))

    assert result["count"] == 0
    assert result["watchlist_count"] == 0
