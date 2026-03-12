from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock


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
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)

    async def _fake_update(**kwargs):
        return {
            "enabled": bool(kwargs.get("enabled")),
            "mode": str(kwargs.get("mode") or "shadow"),
            "provider": str(kwargs.get("provider") or "glm"),
        }

    monkeypatch.setattr(ai_module.autonomous_trading_agent, "update_runtime_config", _fake_update)

    payload = ai_module.AIAutonomousAgentConfigUpdateRequest(
        enabled=True,
        mode="execute",
        provider="codex",
    )
    result = asyncio.run(ai_module.update_ai_autonomous_agent_runtime_config(request, payload))
    assert result["updated"] is True
    assert result["config"]["enabled"] is True
    assert result["config"]["mode"] == "execute"
    assert result["config"]["provider"] == "codex"


def test_autonomous_agent_start_and_run_once_endpoints(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)

    update_mock = AsyncMock(return_value={"enabled": True})
    start_mock = AsyncMock(return_value={"running": True})
    run_once_mock = AsyncMock(return_value={"decision": {"action": "hold"}})
    status_mock = lambda: {"running": True}

    monkeypatch.setattr(ai_module.autonomous_trading_agent, "update_runtime_config", update_mock)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "start", start_mock)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "run_once", run_once_mock)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "get_status", status_mock)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "get_runtime_config", lambda: {"enabled": True})

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
    assert once_result["result"]["decision"]["action"] == "hold"
    assert run_once_mock.await_count == 1
