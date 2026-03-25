from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from config.settings import settings


class _FakeResponse:
    def __init__(self, payload, status: int = 200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)


class _FakeSession:
    def __init__(self, *, capture: dict, payload: dict, status: int = 200, **kwargs):
        self._capture = capture
        self._payload = payload
        self._status = status
        self._capture["session_kwargs"] = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, url, *, headers=None, json=None):
        self._capture["url"] = url
        self._capture["headers"] = headers
        self._capture["json"] = json
        return _FakeResponse(self._payload, status=self._status)


def test_live_decision_router_codex_uses_responses_api(monkeypatch, tmp_path):
    import core.ai.live_decision_router as module

    monkeypatch.setattr(module, "_OVERLAY_PATH", tmp_path / "ai_runtime_config.json")
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)

    capture = {}
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"action":"block","reason":"risk","confidence":0.93}',
                    }
                ],
            }
        ]
    }
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSession(capture=capture, payload=response_payload, **kwargs),
    )

    router = module.LiveAIDecisionRouter()
    result = asyncio.run(
        router._call_provider(
            provider="codex",
            model="gpt-5.4",
            timeout_ms=5000,
            max_tokens=180,
            temperature=0.0,
            system_prompt="sys",
            user_prompt="usr",
        )
    )

    assert result["action"] == "block"
    assert capture["url"] == "https://example.test/v1/responses"
    assert capture["json"]["text"]["format"]["type"] == "json_object"
    assert capture["json"]["max_output_tokens"] == 180


def test_autonomous_agent_codex_uses_responses_api(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)

    capture = {}
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": (
                            '{"action":"buy","confidence":0.81,"strength":0.7,'
                            '"leverage":3,"stop_loss_pct":0.02,"take_profit_pct":0.04,'
                            '"reason":"trend"}'
                        ),
                    }
                ],
            }
        ]
    }
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSession(capture=capture, payload=response_payload, **kwargs),
    )

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")
    result = asyncio.run(
        agent._call_provider(
            provider="codex",
            model="gpt-5.4",
            timeout_ms=8000,
            max_tokens=256,
            temperature=0.1,
            system_prompt="sys",
            user_prompt="usr",
        )
    )

    assert result["action"] == "buy"
    assert capture["url"] == "https://example.test/v1/responses"
    assert capture["json"]["text"]["format"]["type"] == "json_object"
    assert capture["json"]["max_output_tokens"] == 256


def test_async_glm_client_openai_branch_normalizes_responses(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    client = module.AsyncGLMClient({})
    request_mock = AsyncMock(
        return_value=(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": '{"summary":"测试","sentiment":"positive"}'}],
                    }
                ]
            },
            "none",
        )
    )
    monkeypatch.setattr(client, "_request", request_mock)

    response, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert response["choices"][0]["message"]["content"] == '{"summary":"测试","sentiment":"positive"}'
    assert request_mock.await_args.args[1] == "https://example.test/v1/responses"
