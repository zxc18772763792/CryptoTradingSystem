from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from config.settings import settings


@pytest.fixture(autouse=True)
def _isolate_failover_file(monkeypatch, tmp_path):
    import core.utils.openai_responses as helpers
    monkeypatch.delenv("OPENAI_FAILOVER_STATE_PATH", raising=False)
    monkeypatch.setattr(helpers, "_openai_failover_state_path", lambda: Path(
        os.getenv("OPENAI_FAILOVER_STATE_PATH") or tmp_path / "isolated_failover.json"
    ))


@pytest.mark.parametrize("consumer", ["agent", "research"])
@pytest.mark.parametrize("primary_status", [200, 503, "invalid_json"])
def test_dedicated_ai_models_use_direct_chat_and_gpt_backup(monkeypatch, tmp_path, consumer, primary_status):
    import core.ai.autonomous_agent as agent_module
    import core.ai.research_context_generator as research_module
    import core.utils.openai_responses as helpers

    monkeypatch.setenv("AI_AGENT_CONFIG_PATH", str(tmp_path / "agent.json"))
    for name, value in {
        "AI_MODEL_BASE_URL": "https://primary.test/v1",
        "AI_MODEL_API_KEY": "primary-key",
        "AI_MODEL_BACKUP_BASE_URL": "https://backup.test/v1",
        "AI_MODEL_BACKUP_API_KEY": "backup-key",
        "AI_MODEL_BACKUP_MODEL": "gpt-5.6-sol",
        "AI_MODEL_FORCE_CHAT_COMPLETIONS": True,
        "AI_AUTONOMOUS_AGENT_MODEL": "deepseek-v4.1-flash-特价",
        "AI_RESEARCH_MODEL": "deepseek-v4.1-flash-特价",
        "AI_RESEARCH_BACKUP_MODEL": "gpt-5.6-sol",
    }.items():
        monkeypatch.setattr(settings, name, value)
    helpers.reset_openai_target_preferences()
    content = '{"action":"hold","reason":"test"}'
    responses = [_FakeResponse({"choices": [{"message": {"content": content}}]})]
    if primary_status != 200:
        responses.insert(0, _FakeResponse({"choices": [{"message": {"content": "invalid JSON"}}]})
                         if primary_status == "invalid_json" else _FakeResponse({"error": "unavailable"}, status=primary_status))
    capture = {}
    monkeypatch.setattr(agent_module.aiohttp, "ClientSession", lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs))
    if consumer == "agent":
        agent = agent_module.AutonomousTradingAgent(cache_root=tmp_path)
        result = asyncio.run(agent._call_provider(provider="codex", model=settings.AI_AUTONOMOUS_AGENT_MODEL, timeout_ms=5000, max_tokens=420, temperature=0.15, system_prompt="JSON only", user_prompt="test"))
        assert agent._provider_base_url("codex") == "https://primary.test/v1"
    else:
        result = asyncio.run(research_module._call_openai_responses_json("test", timeout=5))
    assert result["action"] == "hold"
    assert capture["session_kwargs"]["trust_env"] is False
    requests = capture["requests"]
    assert requests[0]["url"] == "https://primary.test/v1/chat/completions"
    assert requests[0]["json"]["model"] == "deepseek-v4.1-flash-特价"
    assert requests[0]["json"]["thinking"] == {"type": "disabled"}
    assert requests[0]["headers"]["Authorization"] == "Bearer primary-key"
    if primary_status != 200:
        assert requests[1]["url"] == "https://backup.test/v1/chat/completions"
        assert requests[1]["json"]["model"] == "gpt-5.6-sol"
        assert "thinking" not in requests[1]["json"]
        assert requests[1]["headers"]["Authorization"] == "Bearer backup-key"
    else:
        assert len(requests) == 1


def test_agent_survives_exhausted_primary_and_backup_rejecting_no_reasoning(monkeypatch, tmp_path):
    """2026-10-07: primary relay out of credit (HTTP 400) and the GPT backup rejecting reasoning_effort=none
    left the agent without a single model decision for 34h."""
    import core.ai.autonomous_agent as agent_module
    import core.utils.openai_responses as helpers

    monkeypatch.setenv("AI_AGENT_CONFIG_PATH", str(tmp_path / "agent.json"))
    for name, value in {
        "AI_MODEL_BASE_URL": "https://primary.test/v1",
        "AI_MODEL_API_KEY": "primary-key",
        "AI_MODEL_BACKUP_BASE_URL": "https://backup.test/v1",
        "AI_MODEL_BACKUP_API_KEY": "backup-key",
        "AI_MODEL_BACKUP_MODEL": "gpt-5.6-sol",
        "AI_MODEL_FORCE_CHAT_COMPLETIONS": True,
        "AI_AUTONOMOUS_AGENT_MODEL": "deepseek-v4.1-flash-特价",
    }.items():
        monkeypatch.setattr(settings, name, value)
    helpers.reset_openai_target_preferences()
    monkeypatch.setattr(agent_module, "_REASONING_EFFORT_OVERRIDES", {})
    credit = {"error": {"message": "credit insufficient balance: balance=0 required=884", "type": "api_error"}}
    effort = {"error": {"message": 'gpt-6.1-sol does not support reasoning effort "none"; use low, medium, high, xhigh or max',
                        "type": "invalid_request_error"}}
    ok = {"choices": [{"message": {"content": '{"action":"hold","reason":"test"}'}}]}
    routes = {
        "https://primary.test/v1/chat/completions": [_FakeResponse(credit, status=400), _FakeResponse(credit, status=400)],
        "https://backup.test/v1/chat/completions": [_FakeResponse(effort, status=400), _FakeResponse(ok)],
    }
    requests = []

    class _RoutedSession:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def post(self, url, *, headers=None, json=None, timeout=None):
            requests.append({"url": url, "json": json})
            return routes[url].pop(0)

    monkeypatch.setattr(agent_module.aiohttp, "ClientSession", lambda **kwargs: _RoutedSession(**kwargs))
    agent = agent_module.AutonomousTradingAgent(cache_root=tmp_path)

    def call():
        return asyncio.run(agent._call_provider(provider="codex", model=settings.AI_AUTONOMOUS_AGENT_MODEL, timeout_ms=5000,
                                                max_tokens=420, temperature=0.15, system_prompt="JSON only", user_prompt="test"))

    with pytest.raises(RuntimeError, match="reasoning effort"):
        call()  # the out-of-credit primary now fails over; the backup's first rejection still fails this call
    assert [r["url"] for r in requests] == ["https://primary.test/v1/chat/completions", "https://backup.test/v1/chat/completions"]
    assert requests[1]["json"]["reasoning_effort"] == "none"

    assert call()["action"] == "hold"
    backup_calls = [r for r in requests if r["url"].startswith("https://backup.test")]
    assert backup_calls[-1]["json"]["reasoning_effort"] == "low"
    assert agent._last_answered_model == {"model": "gpt-5.6-sol", "backup": True}
    assert all(r["json"]["reasoning_effort"] == "none" for r in requests if r["url"].startswith("https://primary.test"))


class _FakeResponse:
    def __init__(self, payload, status: int = 200, *, text_payload: str | None = None, headers: dict | None = None):
        self._payload = payload
        self.status = status
        self._text_payload = text_payload
        self.headers = headers or {"content-type": "application/json; charset=utf-8"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        if self._text_payload is not None:
            return self._text_payload
        if isinstance(self._payload, (dict, list)):
            return json.dumps(self._payload, ensure_ascii=False)
        return str(self._payload)


class _FakeSession:
    def __init__(self, *, capture: dict, payload: dict, status: int = 200, text_payload: str | None = None, headers: dict | None = None, **kwargs):
        self._capture = capture
        self._payload = payload
        self._status = status
        self._text_payload = text_payload
        self._headers = headers
        self._capture["session_kwargs"] = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, url, *, headers=None, json=None, timeout=None):
        self._capture["url"] = url
        self._capture["headers"] = headers
        self._capture["json"] = json
        self._capture["timeout"] = timeout
        return _FakeResponse(self._payload, status=self._status, text_payload=self._text_payload, headers=self._headers)


class _FakeSequenceSession:
    def __init__(self, *, capture: dict, responses: list[_FakeResponse], **kwargs):
        self._capture = capture
        self._responses = list(responses)
        self._capture["session_kwargs"] = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def request(self, method, url, *, headers=None, json=None, timeout=None):
        self._capture.setdefault("urls", []).append(url)
        self._capture.setdefault("requests", []).append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("unexpected extra request")
        return self._responses.pop(0)

    def post(self, url, *, headers=None, json=None, timeout=None):
        return self.request("POST", url, headers=headers, json=json, timeout=timeout)


@pytest.mark.parametrize("compatibility_fallback", [False, True])
@pytest.mark.parametrize("bad_content", ['{"hypothesis":"Go long BTC now"}', 'not valid JSON'])
def test_full_research_generation_retries_invalid_primary(monkeypatch, compatibility_fallback, bad_content):
    import core.ai.research_context_generator as module
    for name, value in {"OPENAI_API_KEY": "test-key", "OPENAI_BASE_URL": "https://primary.test/v1",
                        "OPENAI_BACKUP_BASE_URL": "https://backup.test/v1", "OPENAI_BACKUP_API_KEY": "test-backup"}.items():
        monkeypatch.setattr(settings, name, value)
    def response(content):
        return _FakeResponse({"choices": [{"message": {"content": content}}]})
    responses = [response(bad_content), response(json.dumps({"hypothesis": "Compare long-term trends",
        "proposed_strategy_changes": [{"program": {"execution_mode": "stateful_long"}}]}))]
    if compatibility_fallback:
        responses.insert(0, _FakeResponse({"error": "unknown endpoint /responses"}, status=404))
    capture = {}
    monkeypatch.setattr(module.aiohttp, "ClientSession", lambda **kw: _FakeSequenceSession(capture=capture, responses=responses, **kw))
    result = asyncio.run(module.generate_research_context({}, timeout=10, raise_on_error=True))
    assert result["hypothesis"] == "Compare long-term trends"
    assert capture["requests"][-1]["url"].startswith("https://backup.test/")
    assert len(capture["requests"]) == (3 if compatibility_fallback else 2)
    assert module._last_generation_error.get() is None


def test_full_research_rejects_invalid_schema_on_both_targets(monkeypatch):
    import core.ai.research_context_generator as module
    for name, value in {"OPENAI_API_KEY": "test-key", "OPENAI_BASE_URL": "https://primary.test/v1",
                        "OPENAI_BACKUP_BASE_URL": "https://backup.test/v1", "OPENAI_BACKUP_API_KEY": "test-backup"}.items():
        monkeypatch.setattr(settings, name, value)
    capture = {}
    responses = [_FakeResponse({"choices": [{"message": {"content": '{"hypothesis":"Go long BTC now"}'}}]}) for _ in range(2)]
    monkeypatch.setattr(module.aiohttp, "ClientSession", lambda **kw: _FakeSequenceSession(capture=capture, responses=responses, **kw))
    with pytest.raises(module.ResearchGenerationError) as raised:
        asyncio.run(module.generate_research_context({}, timeout=10, raise_on_error=True))
    assert raised.value.code == "invalid_model_schema"
    assert "BTC" not in str(raised.value)
    assert len(capture["requests"]) == 2


@pytest.mark.parametrize("backup_times_out", [False, True])
def test_agent_reserves_backup_timeout_budget(monkeypatch, tmp_path, backup_times_out):
    import core.ai.autonomous_agent as module
    from types import SimpleNamespace
    clock = [100.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    targets = [{"base_url": f"https://{name}.test/v1", "api_key": "test", "model": name,
                "force_chat_completions": True} for name in ("primary", "backup")]
    monkeypatch.setattr(module, "prioritize_openai_targets", lambda targets, **kw: targets)
    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")
    monkeypatch.setattr(agent, "_provider_endpoint_targets", lambda provider: targets)
    capture = {}
    class TimedOutResponse(_FakeResponse):
        async def __aenter__(self):
            clock[0] += capture["requests"][-1]["timeout"].total
            raise asyncio.TimeoutError()
    responses = [TimedOutResponse({}), TimedOutResponse({}) if backup_times_out else
        _FakeResponse({"choices": [{"message": {"content": '{"action":"hold"}'}}]})]
    monkeypatch.setattr(module.aiohttp, "ClientSession", lambda **kw: _FakeSequenceSession(capture=capture, responses=responses, **kw))
    call = agent._call_provider(provider="codex", model="primary", timeout_ms=40000, max_tokens=400,
                                temperature=0.1, system_prompt="JSON", user_prompt="test")
    if backup_times_out:
        with pytest.raises(asyncio.TimeoutError, match="codex_request_timeout"):
            asyncio.run(call)
    else:
        assert asyncio.run(call)["action"] == "hold"
    assert [r["timeout"].total for r in capture["requests"]] == pytest.approx([20, 20])
    assert clock[0] <= 140


class _SyncResponse:
    def __init__(self, payload, status_code: int = 200, *, text_payload: str | None = None, headers: dict | None = None):
        self._payload = payload
        self.status_code = status_code
        self.text = text_payload if text_payload is not None else (
            json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else str(payload)
        )
        self.headers = headers or {"content-type": "application/json; charset=utf-8"}

    def json(self):
        return self._payload


class _SyncSSELikeResponse(_SyncResponse):
    def json(self):
        raise ValueError("not json")


@pytest.fixture(autouse=True)
def _reset_openai_target_state(monkeypatch):
    import core.utils.openai_responses as response_helpers
    import core.news.eventizer.async_glm_client as news_async_module
    import core.news.eventizer.llm_glm5 as news_sync_module

    for name in (
        "NEWS_LLM_API_KEY",
        "NEWS_LLM_BASE_URL",
        "NEWS_LLM_MODEL",
        "NEWS_LLM_BACKUP_API_KEY",
        "NEWS_LLM_BACKUP_BASE_URL",
        "NEWS_LLM_BACKUP_MODEL",
        "NEWS_LLM_PROVIDER",
        "NEWS_LLM_FORCE_CHAT_COMPLETIONS",
    ):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.setattr(settings, name, False if name == "NEWS_LLM_FORCE_CHAT_COMPLETIONS" else "", raising=False)

    response_helpers.reset_openai_target_preferences()
    news_async_module.AsyncGLMClient._summary_cache.clear()
    news_sync_module._SUMMARY_CACHE.clear()
    yield
    response_helpers.reset_openai_target_preferences()
    news_async_module.AsyncGLMClient._summary_cache.clear()
    news_sync_module._SUMMARY_CACHE.clear()


def test_openai_responses_parser_supports_event_stream_payload():
    from core.utils.openai_responses import extract_response_text, parse_responses_body

    event_stream = (
        'event: response.created\n'
        'data: {"response":{"id":"resp_1","status":"in_progress"}}\n\n'
        'event: response.output_text.delta\n'
        'data: {"delta":"{\\"summary\\":\\"BTC利好\\""}\n\n'
        'event: response.output_text.delta\n'
        'data: {"delta":",\\"sentiment\\":\\"positive\\"}"}\n\n'
        'event: response.completed\n'
        'data: {"response":{"output":[{"type":"message","content":[{"type":"output_text","text":"{\\"summary\\":\\"BTC利好\\",\\"sentiment\\":\\"positive\\"}"}]}]}}\n\n'
    )

    parsed = parse_responses_body(event_stream, content_type="text/event-stream")

    assert parsed["output"][0]["content"][0]["text"] == '{"summary":"BTC利好","sentiment":"positive"}'
    assert extract_response_text(parsed) == '{"summary":"BTC利好","sentiment":"positive"}'


def test_openai_requests_reader_falls_back_to_event_stream():
    from core.utils.openai_responses import extract_response_text, read_requests_responses_json

    event_stream = (
        'event: response.output_text.delta\n'
        'data: {"delta":"{\\"summary\\":\\"快讯\\"}"}\n\n'
    )
    response = _SyncSSELikeResponse(
        {},
        text_payload=event_stream,
        headers={"content-type": "text/event-stream"},
    )

    parsed = read_requests_responses_json(response)

    assert extract_response_text(parsed) == '{"summary":"快讯"}'


def test_build_responses_payload_moves_system_messages_to_instructions():
    from core.utils.openai_responses import build_responses_payload

    payload = build_responses_payload(
        model="gpt-5.4",
        messages=[
            {"role": "system", "content": "system guidance"},
            {"role": "developer", "content": "developer policy"},
            {"role": "user", "content": "user asks for help"},
        ],
        max_output_tokens=120,
        text_format="json_object",
    )

    assert payload["instructions"] == "system guidance\n\ndeveloper policy"
    assert payload["input"] == [
        {
            "role": "user",
            "content": [{"type": "input_text", "text": "user asks for help"}],
        }
    ]
    assert payload["max_output_tokens"] == 120
    assert payload["text"]["format"]["type"] == "json_object"


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
    assert "temperature" not in capture["json"]


def test_live_decision_router_codex_falls_back_to_chat_completions(monkeypatch, tmp_path):
    import core.ai.live_decision_router as module

    monkeypatch.setattr(module, "_OVERLAY_PATH", tmp_path / "ai_runtime_config.json")
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "not found"}}, status=404),
        _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"action":"block","reason":"chat_fallback","confidence":0.81}',
                            "role": "assistant",
                        }
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
    )

    router = module.LiveAIDecisionRouter()
    result = asyncio.run(
        router._call_provider(
            provider="codex",
            model="gpt-5.5",
            timeout_ms=5000,
            max_tokens=180,
            temperature=0.0,
            system_prompt="sys",
            user_prompt="usr",
        )
    )

    assert result["reason"] == "chat_fallback"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]
    assert capture["requests"][1]["json"]["response_format"]["type"] == "json_object"


def test_live_decision_router_codex_fails_over_to_anthropic_style_backup(monkeypatch, tmp_path):
    import core.ai.live_decision_router as module

    monkeypatch.setattr(module, "_OVERLAY_PATH", tmp_path / "ai_runtime_config.json")
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://anthropic-proxy.test/anthropic/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "anthropic-key", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "claude-compatible-model", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
        _FakeResponse(
            {
                "content": [
                    {
                        "type": "text",
                        "text": '{"action":"block","reason":"anthropic_backup","confidence":0.77}',
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
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

    assert result["reason"] == "anthropic_backup"
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://anthropic-proxy.test/anthropic/v1/messages",
    ]
    assert capture["requests"][1]["headers"]["x-api-key"] == "anthropic-key"
    assert "api-key" not in capture["requests"][1]["headers"]
    assert capture["requests"][1]["json"]["model"] == "claude-compatible-model"


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
    assert "text" not in capture["json"]
    assert capture["json"]["max_output_tokens"] == 256
    assert "temperature" not in capture["json"]


def test_autonomous_agent_codex_falls_back_to_chat_completions(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "not found"}}, status=404),
        _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"action":"buy","confidence":0.74,"strength":0.61,'
                                '"leverage":1,"stop_loss_pct":0.02,"take_profit_pct":0.04,'
                                '"reason":"chat_fallback"}'
                            ),
                            "role": "assistant",
                        }
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
    )

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")
    result = asyncio.run(
        agent._call_provider(
            provider="codex",
            model="gpt-5.5",
            timeout_ms=8000,
            max_tokens=256,
            temperature=0.1,
            system_prompt="sys",
            user_prompt="usr",
        )
    )

    assert result["reason"] == "chat_fallback"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]
    assert capture["requests"][1]["json"]["response_format"]["type"] == "json_object"


def test_autonomous_agent_codex_fails_over_to_backup_relay(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)

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
                            '"leverage":1,"stop_loss_pct":0.02,"take_profit_pct":0.04,'
                            '"reason":"backup_relay"}'
                        ),
                    }
                ],
            }
        ]
    }
    responses = [
        _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
        _FakeResponse(response_payload, status=200),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
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

    assert result["reason"] == "backup_relay"
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]


def test_autonomous_agent_codex_sticks_to_backup_until_next_day(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("OPENAI_FAILOVER_TZ", "Asia/Shanghai")
    response_helpers.reset_openai_target_preferences()

    day_one = datetime(2026, 4, 6, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    day_two = datetime(2026, 4, 7, 0, 1, tzinfo=ZoneInfo("Asia/Shanghai"))

    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": (
                            '{"action":"buy","confidence":0.81,"strength":0.7,'
                            '"leverage":1,"stop_loss_pct":0.02,"take_profit_pct":0.04,'
                            '"reason":"sticky_backup"}'
                        ),
                    }
                ],
            }
        ]
    }
    session_plans = [
        [
            _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
    ]
    captures: list[dict] = []

    def _session_factory(**kwargs):
        capture = {}
        captures.append(capture)
        responses = session_plans[len(captures) - 1]
        return _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs)

    monkeypatch.setattr(module.aiohttp, "ClientSession", _session_factory)
    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")

    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_one)
    first = asyncio.run(
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
    second = asyncio.run(
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
    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_two)
    third = asyncio.run(
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

    assert first["reason"] == "sticky_backup"
    assert second["reason"] == "sticky_backup"
    assert third["reason"] == "sticky_backup"
    assert captures[0]["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]
    assert captures[1]["urls"] == [
        "https://backup.test/v1/responses",
    ]
    assert captures[2]["urls"] == [
        "https://primary.test/v1/responses",
    ]


def test_autonomous_agent_codex_retries_responses_token_param_variant(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)

    capture = {}
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": (
                            '{"action":"buy","confidence":0.78,"strength":0.66,'
                            '"leverage":1,"stop_loss_pct":0.02,"take_profit_pct":0.04,'
                            '"reason":"token_param_variant_ok"}'
                        ),
                    }
                ],
            }
        ]
    }
    responses = [
        _FakeResponse({"detail": "Unsupported parameter: max_output_tokens"}, status=400),
        _FakeResponse(response_payload, status=200),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
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

    assert result["reason"] == "token_param_variant_ok"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/responses",
    ]
    assert capture["requests"][0]["json"]["max_output_tokens"] == 256
    assert "max_output_tokens" not in capture["requests"][1]["json"]
    assert capture["requests"][1]["json"]["max_completion_tokens"] == 256


def test_research_context_generator_accepts_completed_stream(monkeypatch):
    import core.ai.research_context_generator as module

    monkeypatch.setattr(settings, 'OPENAI_API_KEY', 'test-key', raising=False)
    monkeypatch.setattr(settings, 'OPENAI_BASE_URL', 'https://stream.test/v1', raising=False)
    monkeypatch.setattr(settings, 'OPENAI_BACKUP_BASE_URL', '', raising=False)
    response = {'status': 'completed', 'model': 'test-model', 'output': [{'type': 'message',
        'content': [{'type': 'output_text', 'text': '{"hypothesis":"streamed hypothesis"}'}]}]}
    stream = 'event: response.created\ndata: {"response":{"status":"in_progress","output":[]}}\n\n'
    stream += 'event: response.completed\ndata: ' + json.dumps({'response': response}) + '\n\n'
    capture = {}
    monkeypatch.setattr(module.aiohttp, 'ClientSession', lambda **kw: _FakeSession(
        capture=capture, payload=None, text_payload=stream, headers={'content-type':'text/event-stream'}, **kw))
    result = asyncio.run(module._call_openai_responses_json('prompt', timeout=10))
    assert capture['json']['stream'] is True
    assert capture['json']['reasoning'] == {'effort': 'low'}
    assert result['hypothesis'] == 'streamed hypothesis'
    assert result['_generation']['response_model'] == 'test-model'


def test_research_context_generator_fails_over_to_backup_relay(monkeypatch):
    import core.ai.research_context_generator as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)

    capture = {}
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"hypothesis":"backup relay ok"}',
                    }
                ],
            }
        ]
    }
    responses = [
        _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
        _FakeResponse(response_payload, status=200),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
    )

    result = asyncio.run(module._call_openai_responses_json("prompt", timeout=10))

    assert result["hypothesis"] == "backup relay ok"
    assert result["_generation"]["requested_model"] == capture["requests"][-1]["json"]["model"]
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]


def test_research_context_generator_sticks_to_backup_until_next_day(monkeypatch, tmp_path):
    import core.ai.research_context_generator as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("OPENAI_FAILOVER_TZ", "Asia/Shanghai")
    response_helpers.reset_openai_target_preferences()

    day_one = datetime(2026, 4, 6, 11, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    day_two = datetime(2026, 4, 7, 0, 2, tzinfo=ZoneInfo("Asia/Shanghai"))
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"hypothesis":"sticky backup ok"}',
                    }
                ],
            }
        ]
    }
    session_plans = [
        [
            _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
    ]
    captures: list[dict] = []

    def _session_factory(**kwargs):
        capture = {}
        captures.append(capture)
        responses = session_plans[len(captures) - 1]
        return _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs)

    monkeypatch.setattr(module.aiohttp, "ClientSession", _session_factory)

    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_one)
    first = asyncio.run(module._call_openai_responses_json("prompt", timeout=10))
    second = asyncio.run(module._call_openai_responses_json("prompt", timeout=10))
    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_two)
    third = asyncio.run(module._call_openai_responses_json("prompt", timeout=10))

    for result in (first, second, third):
        assert result["hypothesis"] == "sticky backup ok"
        assert result["_generation"]["requested_model"]
    assert captures[0]["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]
    assert captures[1]["urls"] == [
        "https://backup.test/v1/responses",
    ]
    assert captures[2]["urls"] == [
        "https://primary.test/v1/responses",
    ]


def test_research_context_generator_falls_back_to_chat_completions(monkeypatch):
    import core.ai.research_context_generator as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.5", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "not found"}}, status=404),
        _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"hypothesis":"chat fallback ok"}',
                            "role": "assistant",
                        }
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
    )

    result = asyncio.run(module._call_openai_responses_json("prompt", timeout=10))

    assert result["hypothesis"] == "chat fallback ok"
    assert result["_generation"]["requested_model"] == capture["requests"][-1]["json"]["model"]
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]


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


def test_async_glm_client_openai_fails_over_to_backup_relay(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
        _FakeResponse(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": '{"summary":"切到备用站","sentiment":"positive"}'}],
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs),
    )

    client = module.AsyncGLMClient({})
    response, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert response["choices"][0]["message"]["content"] == '{"summary":"切到备用站","sentiment":"positive"}'
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]


def test_async_glm_client_aiohttp_uses_proxy_environment_by_default(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.delenv("NEWS_LLM_AIOHTTP_TRUST_ENV", raising=False)
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {}
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(
            capture=capture,
            responses=[
                _FakeResponse(
                    {
                        "output": [
                            {
                                "type": "message",
                                "content": [
                                    {"type": "output_text", "text": '{"summary":"ok","sentiment":"neutral"}'}
                                ],
                            }
                        ]
                    },
                    status=200,
                )
            ],
            **kwargs,
        ),
    )

    client = module.AsyncGLMClient({})
    _, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert capture["session_kwargs"]["trust_env"] is True


def test_async_glm_client_aiohttp_trust_env_can_be_disabled(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setenv("NEWS_LLM_AIOHTTP_TRUST_ENV", "0")
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {}
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(
            capture=capture,
            responses=[
                _FakeResponse(
                    {
                        "output": [
                            {
                                "type": "message",
                                "content": [
                                    {"type": "output_text", "text": '{"summary":"ok","sentiment":"neutral"}'}
                                ],
                            }
                        ]
                    },
                    status=200,
                )
            ],
            **kwargs,
        ),
    )

    client = module.AsyncGLMClient({})
    _, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert capture["session_kwargs"]["trust_env"] is False


def test_async_glm_client_openai_sticks_to_backup_until_next_day(monkeypatch, tmp_path):
    import core.news.eventizer.async_glm_client as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("OPENAI_FAILOVER_TZ", "Asia/Shanghai")
    response_helpers.reset_openai_target_preferences()

    day_one = datetime(2026, 4, 6, 12, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    day_two = datetime(2026, 4, 7, 0, 3, tzinfo=ZoneInfo("Asia/Shanghai"))
    response_payload = {
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": '{"summary":"sticky","sentiment":"positive"}'}],
            }
        ]
    }
    session_plans = [
        [
            _FakeResponse({"error": {"message": "Service temporarily unavailable"}}, status=503),
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
        [
            _FakeResponse(response_payload, status=200),
        ],
    ]
    captures: list[dict] = []

    def _session_factory(**kwargs):
        capture = {}
        captures.append(capture)
        responses = session_plans[len(captures) - 1]
        return _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs)

    monkeypatch.setattr(module.aiohttp, "ClientSession", _session_factory)
    client = module.AsyncGLMClient({})

    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_one)
    first, first_error = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )
    second, second_error = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )
    monkeypatch.setattr(response_helpers, "_openai_failover_now", lambda: day_two)
    third, third_error = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert first_error == "none"
    assert second_error == "none"
    assert third_error == "none"
    assert first["choices"][0]["message"]["content"] == '{"summary":"sticky","sentiment":"positive"}'
    assert second["choices"][0]["message"]["content"] == '{"summary":"sticky","sentiment":"positive"}'
    assert third["choices"][0]["message"]["content"] == '{"summary":"sticky","sentiment":"positive"}'
    assert captures[0]["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]
    assert captures[1]["urls"] == [
        "https://backup.test/v1/responses",
    ]
    assert captures[2]["urls"] == [
        "https://primary.test/v1/responses",
    ]


def test_autonomous_agent_codex_skips_responses_after_chat_fallback(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.delenv("OPENAI_FAILOVER_STATE_PATH", raising=False)
    response_helpers.reset_openai_target_preferences(scope="ai_autonomous_agent")

    session_plans = [
        [
            _FakeResponse({"error": {"message": "responses not found"}}, status=404),
            _FakeResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"action":"hold","confidence":0.6,"strength":0.2,'
                                    '"leverage":1,"stop_loss_pct":0.01,"take_profit_pct":0.02,'
                                    '"reason":"chat_fallback"}'
                                ),
                                "role": "assistant",
                            }
                        }
                    ]
                },
                status=200,
            ),
        ],
        [
            _FakeResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"action":"hold","confidence":0.61,"strength":0.22,'
                                    '"leverage":1,"stop_loss_pct":0.01,"take_profit_pct":0.02,'
                                    '"reason":"chat_sticky"}'
                                ),
                                "role": "assistant",
                            }
                        }
                    ]
                },
                status=200,
            ),
        ],
    ]
    captures: list[dict] = []

    def _session_factory(**kwargs):
        capture = {}
        captures.append(capture)
        responses = session_plans[len(captures) - 1]
        return _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs)

    monkeypatch.setattr(module.aiohttp, "ClientSession", _session_factory)
    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")

    first = asyncio.run(
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
    second = asyncio.run(
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

    assert first["reason"] == "chat_fallback"
    assert second["reason"] == "chat_sticky"
    assert captures[0]["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]
    assert captures[1]["urls"] == [
        "https://example.test/v1/chat/completions",
    ]


def test_news_worker_marks_rules_fallback_with_saved_events_as_done(monkeypatch):
    import core.news.service.worker as module

    finish_mock = AsyncMock()
    summarize_mock = AsyncMock()
    monkeypatch.setattr(
        module,
        "extract_events_async_with_meta",
        AsyncMock(
            return_value=(
                [
                    {
                        "event_id": "evt-1",
                        "symbol": "BTC/USDT",
                        "evidence": {"url": "https://example.test/news/1"},
                    }
                ],
                False,
                "other",
            )
        ),
    )
    monkeypatch.setattr(module.news_db, "save_events", AsyncMock(return_value={"events_count": 1}))
    monkeypatch.setattr(module.news_db, "finish_llm_tasks", finish_mock)
    monkeypatch.setattr(module, "_persist_llm_title_summaries", summarize_mock)

    events, llm_used, error_type, events_count = asyncio.run(
        module._execute_llm_batch(
            [{"id": 1, "url": "https://example.test/news/1", "source": "jin10"}],
            {"llm": {}},
            [1],
            url_to_raw_id={"https://example.test/news/1": 1},
        )
    )

    assert llm_used is False
    assert error_type == "other"
    assert events_count == 1
    assert events[0]["raw_news_id"] == 1
    assert finish_mock.await_args.kwargs["success"] is True
    assert finish_mock.await_args.kwargs["error"] is None
    summarize_mock.assert_not_awaited()


def test_news_worker_persists_llm_summary_even_when_no_events(monkeypatch):
    import core.news.service.worker as module

    raw_updates = []
    event_updates = []
    finish_mock = AsyncMock()

    monkeypatch.setattr(
        module,
        "extract_events_async_with_meta",
        AsyncMock(return_value=([], True, "none")),
    )
    monkeypatch.setattr(module.news_db, "save_events", AsyncMock(return_value={"events_count": 0, "inserted": []}))
    monkeypatch.setattr(module.news_db, "finish_llm_tasks", finish_mock)
    monkeypatch.setattr(
        module,
        "summarize_batch_async",
        AsyncMock(return_value=[{"summary": "比特币 ETF 获批", "sentiment": "positive", "source": "openai_responses"}]),
    )

    async def fake_save_news_raw_summaries(rows):
        raw_updates.extend(rows)
        return {"updated_count": len(rows), "skipped_count": 0}

    async def fake_save_news_event_summaries(rows):
        event_updates.extend(rows)
        return {"updated_count": len(rows), "skipped_count": 0}

    monkeypatch.setattr(module.news_db, "save_news_raw_summaries", fake_save_news_raw_summaries)
    monkeypatch.setattr(module.news_db, "save_news_event_summaries", fake_save_news_event_summaries)

    events, llm_used, error_type, events_count = asyncio.run(
        module._execute_llm_batch(
            [
                {
                    "id": 1,
                    "title": "Bitcoin ETF approved by SEC",
                    "url": "https://example.test/news/1",
                    "source": "jin10",
                }
            ],
            {"llm": {"summarize_batch_size": 2, "summarize_timeout_sec": 10}},
            [1],
            url_to_raw_id={"https://example.test/news/1": 1},
        )
    )

    assert events == []
    assert llm_used is True
    assert error_type == "none"
    assert events_count == 0
    assert raw_updates == [
        {
            "raw_news_id": 1,
            "summary_title": "比特币 ETF 获批",
            "summary_sentiment": "positive",
            "summary_source": "openai_responses",
        }
    ]
    assert event_updates == []
    assert finish_mock.await_args.kwargs["success"] is True


def test_async_glm_client_openai_falls_back_to_chat_completions(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.5", raising=False)

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "not found"}}, status=404),
        _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"切到 chat","sentiment":"positive"}',
                            "role": "assistant",
                        }
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=list(responses), **kwargs),
    )

    client = module.AsyncGLMClient({})
    response, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert response["choices"][0]["message"]["content"] == '{"summary":"切到 chat","sentiment":"positive"}'
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]


def test_news_llm_defaults_use_deepseek_flash(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    assert sync_module._llm_provider({}) == "openai"
    assert async_module._llm_provider({}) == "openai"
    assert sync_module._llm_model({}) == "deepseek-v4-flash"
    assert async_module._llm_model({}) == "deepseek-v4-flash"


def test_news_legacy_glm_provider_is_normalized_to_openai(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)

    legacy_cfg = {
        "llm": {
            "provider": "glm",
            "model": "GLM-4.5-Air",
            "base_url": "https://open.bigmodel.cn/api/coding/paas/v4",
        }
    }

    assert sync_module._llm_provider(legacy_cfg) == "openai"
    assert async_module._llm_provider(legacy_cfg) == "openai"
    assert sync_module._llm_model(legacy_cfg) == "deepseek-v4-flash"
    assert async_module._llm_model(legacy_cfg) == "deepseek-v4-flash"
    assert sync_module._llm_base_url(legacy_cfg) == "https://kuaipao.ai"
    assert async_module._llm_base_url(legacy_cfg) == "https://kuaipao.ai"


def test_news_runtime_openai_settings_override_yaml_config(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://runtime-primary.test/v1", raising=False)
    monkeypatch.setattr(
        settings,
        "OPENAI_BACKUP_BASE_URL",
        "https://runtime-backup-a.test/v1,https://runtime-backup-b.test/v1",
        raising=False,
    )
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "gpt-5.4-mini", raising=False)

    cfg = {
        "llm": {
            "provider": "openai",
            "model": "GLM-4.5-Air",
            "backup_model": "glm-4.5",
            "base_url": "https://open.bigmodel.cn/api/coding/paas/v4",
            "backup_base_url": "https://legacy-backup.test/v1",
        }
    }

    sync_targets = sync_module._openai_endpoint_targets(cfg)
    async_targets = async_module._openai_endpoint_targets(cfg)

    assert sync_module._llm_base_url(cfg) == "https://runtime-primary.test/v1"
    assert async_module._llm_base_url(cfg) == "https://runtime-primary.test/v1"
    assert sync_module._llm_model(cfg) == "gpt-5.4"
    assert async_module._llm_model(cfg) == "gpt-5.4"
    assert [target["base_url"] for target in sync_targets[:4]] == [
        "https://runtime-primary.test/v1",
        "https://runtime-backup-a.test/v1",
        "https://runtime-backup-b.test/v1",
        "https://legacy-backup.test/v1",
    ]
    assert [target["base_url"] for target in async_targets[:4]] == [
        "https://runtime-primary.test/v1",
        "https://runtime-backup-a.test/v1",
        "https://runtime-backup-b.test/v1",
        "https://legacy-backup.test/v1",
    ]


def test_news_deepseek_yaml_config_ignores_generic_openai_runtime_settings(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-openai", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://old-openai.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://anthropic-proxy.test/anthropic/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.5", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "claude-compatible-model", raising=False)

    cfg = {
        "llm": {
            "provider": "openai",
            "model": "deepseek-v4-flash",
            "base_url": "https://kuaipao.ai",
            "backup_base_url": "",
            "force_chat_completions": True,
        }
    }

    sync_targets = sync_module._openai_endpoint_targets(cfg)
    async_targets = async_module._openai_endpoint_targets(cfg)

    assert sync_module._llm_base_url(cfg) == "https://kuaipao.ai"
    assert async_module._llm_base_url(cfg) == "https://kuaipao.ai"
    assert sync_module._llm_model(cfg) == "deepseek-v4-flash"
    assert async_module._llm_model(cfg) == "deepseek-v4-flash"
    assert [target["base_url"] for target in sync_targets] == ["https://kuaipao.ai"]
    assert [target["base_url"] for target in async_targets] == ["https://kuaipao.ai"]
    assert [target["model"] for target in sync_targets] == ["deepseek-v4-flash"]
    assert [target["model"] for target in async_targets] == ["deepseek-v4-flash"]


def test_news_llm_runtime_backup_settings_allow_distinct_local_fallback(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "primary-news-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://primary-news.test/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "local-gemma-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "http://192.168.1.24:8010/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://generic-backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "generic-backup-key", raising=False)

    cfg = {"llm": {"provider": "openai", "force_chat_completions": True}}

    sync_targets = sync_module._openai_endpoint_targets(cfg)
    async_targets = async_module._openai_endpoint_targets(cfg)

    for targets in (sync_targets, async_targets):
        assert [target["base_url"] for target in targets] == [
            "https://primary-news.test/v1",
            "http://192.168.1.24:8010/v1",
        ]
        assert [target["api_key"] for target in targets] == ["primary-news-key", "local-gemma-key"]
        assert [target["model"] for target in targets] == ["deepseek-v4-flash", "gemma4-local"]


def test_news_llm_runtime_backup_settings_support_three_step_translation_chain(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nvidia-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-4-31b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "local-gemma-key,previous-ds-key", raising=False)
    monkeypatch.setattr(
        settings,
        "NEWS_LLM_BACKUP_BASE_URL",
        "http://192.168.1.24:8010/v1,https://kuaipao.ai",
        raising=False,
    )
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local,deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://generic-backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "generic-backup-key", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "generic-backup-model", raising=False)

    cfg = {"llm": {"provider": "openai", "force_chat_completions": True}}

    sync_targets = sync_module._openai_endpoint_targets(cfg)
    async_targets = async_module._openai_endpoint_targets(cfg)

    for targets in (sync_targets, async_targets):
        assert [target["base_url"] for target in targets] == [
            "https://integrate.api.nvidia.com/v1",
            "http://192.168.1.24:8010/v1",
            "https://kuaipao.ai",
        ]
        assert [target["api_key"] for target in targets] == [
            "nvidia-key",
            "local-gemma-key",
            "previous-ds-key",
        ]
        assert [target["model"] for target in targets] == [
            "google/gemma-4-31b-it",
            "gemma4-local",
            "deepseek-v4-flash",
        ]


def test_news_sync_failover_uses_longer_timeout_for_local_gemma(monkeypatch, tmp_path):
    import core.news.eventizer.llm_glm5 as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("NEWS_LLM_LOCAL_TARGET_TIMEOUT_SEC", "120")
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nvidia-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-4-31b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "local-gemma-key,previous-ds-key", raising=False)
    monkeypatch.setattr(
        settings,
        "NEWS_LLM_BACKUP_BASE_URL",
        "http://192.168.1.24:8010/v1,https://kuaipao.ai",
        raising=False,
    )
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local,deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)
    response_helpers.reset_openai_target_preferences()

    captured: list[dict] = []

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        captured.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        if "integrate.api.nvidia.com" in url:
            return _SyncResponse({"error": {"message": "temporarily unavailable"}}, status_code=503)
        return _SyncResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"GM ok","sentiment":"neutral"}',
                            "role": "assistant",
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_llm(
        "BTC trades flat",
        {
            "llm": {
                "provider": "openai",
                "force_chat_completions": True,
                "summarize_timeout_sec": 12,
            }
        },
        max_length=60,
    )

    assert result["source"] == "gm_summary:gemma4-local"
    assert [item["url"] for item in captured] == [
        "https://integrate.api.nvidia.com/v1/chat/completions",
        "http://192.168.1.24:8010/v1/chat/completions",
    ]
    assert captured[0]["timeout"] == 12
    assert captured[1]["timeout"] == 120


def test_news_sync_failover_wall_clock_budget_is_not_multiplied_by_targets(monkeypatch, tmp_path):
    import core.news.eventizer.llm_glm5 as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("NEWS_LLM_LOCAL_TARGET_TIMEOUT_SEC", "120")
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "primary-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "primary-model", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "local-key,backup-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "http://192.168.1.24:8010/v1,https://backup.test/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local,backup-model", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)
    response_helpers.reset_openai_target_preferences()

    monotonic_values = [0.0, 0.0, 130.0]

    def _fake_monotonic():
        if monotonic_values:
            return monotonic_values.pop(0)
        return 130.0

    monkeypatch.setattr(module.time, "monotonic", _fake_monotonic)
    captured_urls: list[str] = []

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        captured_urls.append(url)
        if len(captured_urls) > 1:
            raise AssertionError("failover should stop before a second target after wall-clock budget is exhausted")
        return _SyncResponse({"error": {"message": "temporarily unavailable"}}, status_code=503)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    with pytest.raises(RuntimeError, match="wall-clock budget exhausted"):
        module._openai_post_with_failover(
            cfg={"llm": {"provider": "openai"}},
            payload={"model": "primary-model", "input": "hello"},
            timeout_sec=12,
            log_prefix="test",
        )

    assert captured_urls == ["https://primary.test/v1/responses"]


def test_news_sync_extract_json_block_finds_fenced_json_after_prose():
    import core.news.eventizer.llm_glm5 as module

    parsed = module._extract_json_block(
        'Here is the result:\n```json\n{"items":[{"symbol":"BTC","sentiment":"positive"}]}\n```\nDone.'
    )

    assert parsed == {"items": [{"symbol": "BTC", "sentiment": "positive"}]}


def test_news_sync_summary_empty_choices_falls_back(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "sk-news", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://kuaipao.ai", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_FORCE_CHAT_COMPLETIONS", True, raising=False)

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        return _SyncResponse({"choices": []})

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_llm(
        "BTC trades flat",
        {"llm": {"provider": "openai", "force_chat_completions": True}},
        max_length=60,
    )

    assert result["summary"]
    assert result["source"] == "fallback_rule"


def test_async_glm_client_failover_uses_longer_timeout_for_local_gemma(monkeypatch, tmp_path):
    import core.news.eventizer.async_glm_client as module
    import core.utils.openai_responses as response_helpers

    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("NEWS_LLM_LOCAL_TARGET_TIMEOUT_SEC", "120")
    monkeypatch.setenv("NEWS_LLM_LOCAL_CONNECT_TIMEOUT_SEC", "10")
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nvidia-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-4-31b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "local-gemma-key,previous-ds-key", raising=False)
    monkeypatch.setattr(
        settings,
        "NEWS_LLM_BACKUP_BASE_URL",
        "http://192.168.1.24:8010/v1,https://kuaipao.ai",
        raising=False,
    )
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local,deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)
    response_helpers.reset_openai_target_preferences()

    capture = {}
    responses = [
        _FakeResponse({"error": {"message": "temporarily unavailable"}}, status=503),
        _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"GM ok","sentiment":"neutral"}',
                            "role": "assistant",
                        }
                    }
                ]
            },
            status=200,
        ),
    ]
    monkeypatch.setattr(
        module.aiohttp,
        "ClientSession",
        lambda **kwargs: _FakeSequenceSession(capture=capture, responses=responses, **kwargs),
    )

    client = module.AsyncGLMClient(
        {
            "llm": {
                "provider": "openai",
                "force_chat_completions": True,
                "timeout_sec": 16,
                "connect_timeout_sec": 6,
            }
        }
    )
    response, error_type = asyncio.run(
        client.chat_completions(
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=64,
        )
    )

    assert error_type == "none"
    assert response["choices"][0]["message"]["content"] == '{"summary":"GM ok","sentiment":"neutral"}'
    assert capture["urls"] == [
        "https://integrate.api.nvidia.com/v1/chat/completions",
        "http://192.168.1.24:8010/v1/chat/completions",
    ]
    assert capture["requests"][0]["timeout"].total == 16
    assert capture["requests"][1]["timeout"].total == 120
    assert capture["requests"][1]["timeout"].connect == 10


def test_news_sync_summary_uses_openai_mini_source(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {}

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["url"] = url
        capture["headers"] = headers
        capture["json"] = json
        capture["timeout"] = timeout
        return _SyncResponse(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": '{"summary":"ETF approval positive","sentiment":"positive"}'}],
                    }
                ]
            }
        )

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("BTC ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "ETF approval positive"
    assert result["sentiment"] == "positive"
    assert result["source"] == "openai_responses:deepseek-v4-flash"
    assert capture["url"] == "https://example.test/v1/responses"
    assert capture["json"]["model"] == "deepseek-v4-flash"


def test_news_sync_extract_json_block_reads_fenced_json_after_prose():
    import core.news.eventizer.llm_glm5 as module

    parsed = module._extract_json_block(
        'Here is the result:\n```json\n{"items":[{"idx":0,"summary":"ok","sentiment":"neutral"}]}\n```'
    )

    assert parsed == {"items": [{"idx": 0, "summary": "ok", "sentiment": "neutral"}]}


def test_news_sync_summary_empty_choices_falls_back_safely(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_openai_post_with_failover", lambda **_kwargs: {"choices": []})

    result = module.summarize_title_glm5("BTC trades flat", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["source"] == "fallback_rule"
    assert result["sentiment"] in {"positive", "negative", "neutral"}


def test_news_sync_batch_summary_bad_first_choice_falls_back(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_openai_post_with_failover", lambda **_kwargs: {"choices": [None]})

    result = module.batch_summarize_titles(["BTC trades flat"], {"llm": {"provider": "openai"}}, max_length=60)

    assert result[0]["source"] == "fallback_rule"


def test_news_sync_summary_uses_news_deepseek_chat_override(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-old-openai", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://old-openai.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "sk-news", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://kuaipao.ai", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_FORCE_CHAT_COMPLETIONS", True, raising=False)

    capture = {}

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["url"] = url
        capture["headers"] = headers
        capture["json"] = json
        return _SyncResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"DeepSeek 摘要","sentiment":"neutral"}',
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("BTC trades flat", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "DeepSeek 摘要"
    assert result["sentiment"] == "neutral"
    assert result["source"] == "ds_summary:deepseek-v4-flash"
    assert capture["url"] == "https://kuaipao.ai/v1/chat/completions"
    assert capture["json"]["model"] == "deepseek-v4-flash"
    assert capture["headers"]["Authorization"] == "Bearer sk-news"


def test_news_sync_summary_source_includes_response_model(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "sk-news", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://kuaipao.ai", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_FORCE_CHAT_COMPLETIONS", True, raising=False)

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        return _SyncResponse(
            {
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"DS 摘要","sentiment":"neutral"}',
                        }
                    }
                ],
            }
        )

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("BTC trades flat", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["source"] == "ds_summary:deepseek-v4-flash"


def test_news_sync_summary_source_labels_nim_gm_and_ds(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "NEWS_LLM_FORCE_CHAT_COMPLETIONS", True, raising=False)

    cases = [
        (
            {
                "NEWS_LLM_API_KEY": "nim-key",
                "NEWS_LLM_BASE_URL": "https://integrate.api.nvidia.com/v1",
                "NEWS_LLM_MODEL": "google/gemma-4-31b-it",
            },
            "nim_summary:google-gemma-4-31b-it",
        ),
        (
            {
                "NEWS_LLM_API_KEY": "gm-key",
                "NEWS_LLM_BASE_URL": "http://192.168.1.24:8010/v1",
                "NEWS_LLM_MODEL": "gemma4-local",
            },
            "gm_summary:gemma4-local",
        ),
        (
            {
                "NEWS_LLM_API_KEY": "ds-key",
                "NEWS_LLM_BASE_URL": "https://kuaipao.ai",
                "NEWS_LLM_MODEL": "deepseek-v4-flash",
            },
            "ds_summary:deepseek-v4-flash",
        ),
        (
            {
                "NEWS_LLM_API_KEY": "qwen-key",
                "NEWS_LLM_BASE_URL": "https://kuaipao.ai",
                "NEWS_LLM_MODEL": "qwen3-vl-flash",
            },
            "qwen_summary:qwen3-vl-flash",
        ),
    ]

    for env_patch, expected_source in cases:
        module._SUMMARY_CACHE.clear()
        for name, value in env_patch.items():
            monkeypatch.setattr(settings, name, value, raising=False)

        def _fake_post(url, *, headers=None, json=None, timeout=None):
            return _SyncResponse(
                {
                    "model": str(env_patch["NEWS_LLM_MODEL"]),
                    "choices": [
                        {
                            "message": {
                                "content": '{"summary":"摘要","sentiment":"neutral"}',
                            }
                        }
                    ],
                }
            )

        monkeypatch.setattr(module.requests, "post", _fake_post)
        result = module.summarize_title_glm5("BTC trades flat", {"llm": {"provider": "openai"}}, max_length=60)
        assert result["source"] == expected_source


def test_news_sync_batch_summary_accepts_nim_result_wrapper(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nim-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-3n-e4b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_FORCE_CHAT_COMPLETIONS", True, raising=False)

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        return _SyncResponse(
            {
                "model": "google/gemma-3n-e4b-it",
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"output":[{"idx":0,"summary":"NIM 摘要",'
                                '"sentiment":"neutral"}]}'
                            ),
                        }
                    }
                ],
            }
        )

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.batch_summarize_titles(
        ["BTC trades flat"],
        {"llm": {"provider": "openai", "force_chat_completions": True, "summarize_batch_size": 1}},
        max_length=60,
    )

    assert result == [
        {
            "summary": "NIM 摘要",
            "sentiment": "neutral",
            "source": "nim_summary:google-gemma-3n-e4b-it",
        }
    ]


def test_news_sync_summary_falls_back_to_chat_completions(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.5", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": []}
    responses = iter(
        [
            _SyncResponse({"error": {"message": "not found"}}, status_code=404),
            _SyncResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"summary":"chat 摘要","sentiment":"positive"}',
                                "role": "assistant",
                            }
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("Bitcoin ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "chat 摘要"
    assert result["sentiment"] == "positive"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]


def test_news_sync_summary_retries_responses_token_param_variant(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": [], "payloads": []}
    responses = iter(
        [
            _SyncResponse({"detail": "Unsupported parameter: max_output_tokens"}, status_code=400),
            _SyncResponse(
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [{"type": "output_text", "text": '{"summary":"token variant ok","sentiment":"positive"}'}],
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        capture["payloads"].append(dict(json or {}))
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("Bitcoin ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "token variant ok"
    assert result["sentiment"] == "positive"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/responses",
    ]
    assert capture["payloads"][0]["max_output_tokens"] == 110
    assert "max_output_tokens" not in capture["payloads"][1]
    assert capture["payloads"][1]["max_completion_tokens"] == 110


def test_news_sync_summary_uses_chat_when_responses_body_is_empty(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": []}
    responses = iter(
        [
            _SyncResponse({"id": "resp_1", "status": "completed", "output": []}, status_code=200),
            _SyncResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"summary":"empty body recovered","sentiment":"positive"}',
                                "role": "assistant",
                            }
                        }
                    ]
                },
                status_code=200,
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("Bitcoin ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "empty body recovered"
    assert result["sentiment"] == "positive"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]


def test_news_batch_summary_prefers_new_llm_cap_key(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(
        module,
        "_call_llm_batch_summarize",
        lambda titles, cfg, max_length=60: [
            {"summary": f"llm:{title}", "sentiment": "neutral", "source": "openai_responses"}
            for title in titles
        ],
    )

    result = module.batch_summarize_titles(
        ["headline-a", "headline-b"],
        {"llm": {"summarize_batch_size": 2, "summarize_max_llm_items": 1}},
        max_length=60,
    )

    assert result[0]["source"] == "openai_responses"
    assert result[0]["summary"] == "llm:headline-a"
    assert result[1]["source"] == "fallback_rule"


def test_news_batch_summary_keeps_legacy_glm_cap_key_compatible(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(
        module,
        "_call_llm_batch_summarize",
        lambda titles, cfg, max_length=60: [
            {"summary": f"llm:{title}", "sentiment": "neutral", "source": "openai_responses"}
            for title in titles
        ],
    )

    result = module.batch_summarize_titles(
        ["headline-a", "headline-b"],
        {"llm": {"summarize_batch_size": 2, "summarize_max_glm_items": 1}},
        max_length=60,
    )

    assert result[0]["source"] == "openai_responses"
    assert result[1]["source"] == "fallback_rule"


def test_news_sync_summary_fails_over_to_backup_relay(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": []}
    responses = iter(
        [
            _SyncResponse({"error": {"message": "temporary unavailable"}}, status_code=503),
            _SyncResponse(
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [{"type": "output_text", "text": '{"summary":"备用站摘要","sentiment":"positive"}'}],
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("ETH staking inflow rises", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "备用站摘要"
    assert result["sentiment"] == "positive"
    assert result["source"] == "openai_responses:gpt-5.5"
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
    ]


def test_news_extract_events_fails_over_to_backup_relay(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://primary.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "https://backup.test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": []}
    responses = iter(
        [
            _SyncResponse({"error": {"message": "temporary unavailable"}}, status_code=503),
            _SyncResponse(
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": json.dumps(
                                        {
                                            "events": [
                                                {
                                                    "event_id": "evt-1",
                                                    "ts": "2026-03-29T00:00:00Z",
                                                    "symbol": "BTCUSDT",
                                                    "event_type": "etf",
                                                    "sentiment": 1,
                                                    "impact_score": 0.88,
                                                    "half_life_min": 180,
                                                    "evidence": {
                                                        "title": "Bitcoin ETF approved",
                                                        "url": "https://example.test/news/1",
                                                        "source": "jin10",
                                                        "matched_reason": "approval",
                                                    },
                                                }
                                            ]
                                        },
                                        ensure_ascii=False,
                                    ),
                                }
                            ],
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    events = module.extract_events_glm5(
        [
            {
                "title": "Bitcoin ETF approved",
                "content": "ETF approval boosts sentiment",
                "url": "https://example.test/news/1",
                "source": "jin10",
                "published_at": "2026-03-29T00:00:00Z",
            }
        ],
        {
            "llm": {"provider": "openai", "batch_size": 1},
            "symbols": {"BTCUSDT": {"canonical": "BTCUSDT", "aliases": ["BTC", "BTCUSDT"]}},
        },
    )

    assert len(events) == 1
    assert events[0]["symbol"] == "BTCUSDT"
    assert events[0]["event_type"] == "etf"


def test_news_extract_events_falls_back_to_chat_completions(monkeypatch):
    import core.news.eventizer.llm_glm5 as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.5", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    capture = {"urls": []}
    responses = iter(
        [
            _SyncResponse({"error": {"message": "not found"}}, status_code=404),
            _SyncResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "events": [
                                            {
                                                "event_id": "evt-chat-1",
                                                "ts": "2026-03-29T00:00:00Z",
                                                "symbol": "BTCUSDT",
                                                "event_type": "etf",
                                                "sentiment": 1,
                                                "impact_score": 0.9,
                                                "half_life_min": 180,
                                                "evidence": {
                                                    "title": "Bitcoin ETF approved",
                                                    "url": "https://example.test/news/1",
                                                    "source": "jin10",
                                                    "matched_reason": "approval",
                                                },
                                            }
                                        ]
                                    },
                                    ensure_ascii=False,
                                ),
                                "role": "assistant",
                            }
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        capture["urls"].append(url)
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    events = module.extract_events_glm5(
        [
            {
                "title": "Bitcoin ETF approved",
                "content": "ETF approval boosts sentiment",
                "url": "https://example.test/news/1",
                "source": "jin10",
                "published_at": "2026-03-29T00:00:00Z",
            }
        ],
        {
            "llm": {"provider": "openai", "batch_size": 1},
            "symbols": {"BTCUSDT": {"canonical": "BTCUSDT", "aliases": ["BTC", "BTCUSDT"]}},
        },
    )

    assert len(events) == 1
    assert events[0]["symbol"] == "BTCUSDT"
    assert capture["urls"] == [
        "https://example.test/v1/responses",
        "https://example.test/v1/chat/completions",
    ]


def test_async_glm_client_summarize_batch_marks_openai_source(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BASE_URL", "https://example.test/v1", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    client = module.AsyncGLMClient({"llm": {"provider": "openai"}})
    request_mock = AsyncMock(
        return_value=(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "output_text",
                                "text": '{"items":[{"idx":0,"summary":"headline summary","sentiment":"neutral"}]}',
                            }
                        ],
                    }
                ]
            },
            "none",
        )
    )
    monkeypatch.setattr(client, "_request", request_mock)

    result = asyncio.run(client.summarize_batch(["BTC trades flat"], max_length=60))

    assert result[0]["summary"] == "headline summary"
    assert result[0]["sentiment"] == "neutral"
    assert result[0]["source"] == "openai_responses"
    assert "temperature" not in request_mock.await_args.args[2]
    assert "temperature" not in request_mock.await_args.kwargs["chat_fallback_payload"]


def test_async_glm_client_summarize_batch_labels_nim_target(monkeypatch):
    import core.news.eventizer.async_glm_client as module

    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nim-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-4-31b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    client = module.AsyncGLMClient({"llm": {"provider": "openai"}})
    request_mock = AsyncMock(
        return_value=(
            {
                "model": "google/gemma-4-31b-it",
                "_cts_openai_target": {
                    "base_url": "https://integrate.api.nvidia.com/v1",
                    "model": "google/gemma-4-31b-it",
                    "index": 0,
                },
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"items":[{"idx":0,"summary":"headline summary",'
                                '"sentiment":"neutral"}]}'
                            ),
                        }
                    }
                ],
            },
            "none",
        )
    )
    monkeypatch.setattr(client, "_request", request_mock)

    result = asyncio.run(client.summarize_batch(["BTC trades flat"], max_length=60))

    assert result[0]["source"] == "nim_summary:google-gemma-4-31b-it"


def test_news_feed_summarize_cfg_uses_llm_defaults_when_env_absent(monkeypatch):
    import web.api.news as module

    monkeypatch.delenv("NEWS_API_SUMMARY_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NEWS_API_SUMMARIZE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    effective = module._feed_summarize_cfg(
        {
            "llm": {
                "provider": "openai",
                "summarize_batch_size": 6,
                "summarize_timeout_sec": 60,
            }
        },
        limit=12,
    )

    assert effective["llm"]["summarize_batch_size"] == 6
    assert effective["llm"]["summarize_timeout_sec"] == 45


def test_news_feed_summarize_cfg_caps_batch_and_extends_timeout_for_local_gemma(monkeypatch):
    import web.api.news as module

    monkeypatch.setenv("NEWS_API_SUMMARY_BATCH_SIZE", "6")
    monkeypatch.setenv("NEWS_API_SUMMARIZE_TIMEOUT_SEC", "45")
    monkeypatch.delenv("NEWS_LLM_LOCAL_SUMMARY_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NEWS_LLM_LOCAL_SUMMARIZE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "http://192.168.1.24:8010/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    effective = module._feed_summarize_cfg(
        {
            "llm": {
                "provider": "openai",
                "summarize_batch_size": 6,
                "summarize_timeout_sec": 45,
            }
        },
        limit=12,
    )

    assert effective["llm"]["summarize_batch_size"] == 1
    assert effective["llm"]["summarize_timeout_sec"] == 120


def test_news_llm_runtime_snapshot_reports_nim_translation_chain(monkeypatch):
    import web.api.news as module

    monkeypatch.setattr(settings, "NEWS_LLM_PROVIDER", "openai", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_API_KEY", "nvidia-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BASE_URL", "https://integrate.api.nvidia.com/v1", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_MODEL", "google/gemma-4-31b-it", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_API_KEY", "kuaipao-key", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_BASE_URL", "https://kuaipao.ai,https://kuaipao.ai", raising=False)
    monkeypatch.setattr(settings, "NEWS_LLM_BACKUP_MODEL", "qwen3-vl-flash,deepseek-v4-flash", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    payload = module._news_llm_runtime_snapshot({"llm": {"provider": "openai", "force_chat_completions": True}})

    assert payload["enabled"] is True
    assert [target["role"] for target in payload["targets"]] == ["primary", "backup", "backup"]
    assert [target["summary_label"] for target in payload["targets"]] == [
        "nim_summary",
        "qwen_summary",
        "ds_summary",
    ]
    assert [target["model"] for target in payload["targets"]] == [
        "google/gemma-4-31b-it",
        "qwen3-vl-flash",
        "deepseek-v4-flash",
    ]
    assert all(target["api_key_configured"] for target in payload["targets"])
    assert all("api_key" not in target for target in payload["targets"])


def test_failed_unstructured_with_llm_summary_is_treated_as_repaired():
    import web.api.news as module

    status = module._derive_unstructured_processing_status(
        {"payload": {"summary_source": "openai_responses"}},
        "failed",
        35,
    )

    assert status == "summarized_no_event"


def test_apply_display_summaries_preserves_persisted_event_translation():
    import web.api.news as module

    rows = module._apply_display_summaries(
        [
            {
                "has_event": True,
                "title": "Bitcoin ETF approved by SEC",
                "sentiment": 1,
                "summary_title": "比特币 ETF 获批",
                "summary_sentiment": "positive",
                "summary_source": "openai_responses",
            }
        ]
    )

    assert rows[0]["summary_title"] == "比特币 ETF 获批"
    assert rows[0]["summary_sentiment"] == "positive"
    assert rows[0]["summary_source"] == "openai_responses"


def test_build_latest_feed_summarize_persists_event_translation(monkeypatch):
    import web.api.news as module

    raw_row = {
        "id": 1,
        "source": "jin10",
        "title": "Bitcoin ETF approved by SEC",
        "url": "https://example.test/news/1",
        "content": "ETF approval lifts crypto market sentiment.",
        "published_at": "2026-03-26T00:00:00Z",
        "payload": {"provider": "jin10", "importance_score": 80},
    }
    event_row = {
        "id": 11,
        "event_id": "evt-1",
        "ts": "2026-03-26T00:00:00Z",
        "symbol": "BTCUSDT",
        "event_type": "etf",
        "sentiment": 1,
        "impact_score": 0.88,
        "model_source": "llm",
        "raw_news_id": 1,
        "evidence": {
            "title": "Bitcoin ETF approved by SEC",
            "url": "https://example.test/news/1",
            "source": "jin10",
        },
        "payload": {"provider": "jin10"},
    }
    saved = {"raw": None, "event": None}

    async def fake_list_news_raw(*, since=None, limit=0):
        return [raw_row]

    async def fake_list_events(*, symbol=None, since=None, limit=0):
        return [event_row]

    async def fake_list_llm_task_status(raw_ids):
        return {1: "done"}

    async def fake_save_news_raw_summaries(rows):
        saved["raw"] = list(rows)
        return {"updated_count": len(rows), "skipped_count": 0}

    async def fake_save_news_event_summaries(rows):
        saved["event"] = list(rows)
        return {"updated_count": len(rows), "skipped_count": 0}

    monkeypatch.setattr(module.news_db, "list_news_raw", fake_list_news_raw)
    monkeypatch.setattr(module.news_db, "list_events", fake_list_events)
    monkeypatch.setattr(module.news_db, "list_llm_task_status", fake_list_llm_task_status)
    monkeypatch.setattr(module.news_db, "save_news_raw_summaries", fake_save_news_raw_summaries)
    monkeypatch.setattr(module.news_db, "save_news_event_summaries", fake_save_news_event_summaries)
    monkeypatch.setattr(
        module,
        "batch_summarize_titles",
        lambda titles, cfg, max_length=60: [
            {"summary": "比特币 ETF 获批，市场偏利好", "sentiment": "positive", "source": "openai_responses"}
            for _ in titles
        ],
    )

    result = asyncio.run(
        module.build_latest_feed(
            cfg={
                "llm": {
                    "provider": "openai",
                    "model": "gpt-5.1-codex-mini",
                    "summarize_limit": 4,
                    "summarize_batch_size": 2,
                    "summarize_timeout_sec": 10,
                },
                "symbols": {
                    "BTCUSDT": {"canonical": "BTCUSDT", "aliases": ["BTC", "BTCUSDT"]},
                },
            },
            symbol=None,
            hours=24,
            limit=10,
            summarize=True,
        )
    )

    assert result["items"][0]["has_event"] is True
    assert result["items"][0]["summary_title"] == "比特币 ETF 获批，市场偏利好"
    assert result["items"][0]["summary_source"] == "openai_responses"
    assert saved["event"] == [
        {
            "event_id": "evt-1",
            "summary_title": "比特币 ETF 获批，市场偏利好",
            "summary_sentiment": "positive",
            "summary_source": "openai_responses",
        }
    ]
    assert saved["raw"] == [
        {
            "raw_news_id": 1,
            "summary_title": "比特币 ETF 获批，市场偏利好",
            "summary_sentiment": "positive",
            "summary_source": "openai_responses",
        }
    ]


def test_auto_summary_repair_invalidates_feed_cache_after_success(monkeypatch):
    import web.api.news as module

    repair_mock = AsyncMock(
        return_value={
            "updated_raw_count": 2,
            "updated_event_count": 1,
            "skipped_non_llm": 0,
            "errors": [],
        }
    )
    invalidate_calls = []

    monkeypatch.setattr(module, "_SUMMARY_REPAIR_AUTO_LAST_AT", None)
    monkeypatch.setattr(module, "_SUMMARY_REPAIR_AUTO_TASK", None)
    monkeypatch.setattr(module, "_news_llm_enabled", lambda: True)
    monkeypatch.setattr(module, "repair_recent_news_summaries", repair_mock)
    monkeypatch.setattr(module, "_invalidate_news_caches", lambda clear_feed=False: invalidate_calls.append(clear_feed))

    async def _run():
        result = await module._maybe_schedule_background_summary_repair(
            {},
            items=[
                {"summary_source": "rule_fallback"},
                {"summary_source": "api_timeout_fallback"},
                {"summary_source": "rule_fallback"},
            ],
            hours=24,
            trigger="test_case",
        )
        task = module._SUMMARY_REPAIR_AUTO_TASK
        assert task is not None
        await task
        return result

    result = asyncio.run(_run())

    assert result["queued"] is True
    assert repair_mock.await_count == 1
    assert invalidate_calls == [True]
    assert module._SUMMARY_REPAIR_AUTO_TASK is None


def test_news_auto_requeue_skips_when_queue_busy(monkeypatch):
    import web.api.news as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_FAILED_REQUEUE_LAST_AT", None)

    queue_mock = AsyncMock(
        return_value={
            "pending_total": 1,
            "counts": {"pending": 1, "running": 0, "retry": 0, "failed": 3},
        }
    )
    requeue_mock = AsyncMock(return_value={"requeued_count": 2})

    monkeypatch.setattr(module.news_db, "get_llm_queue_stats", queue_mock)
    monkeypatch.setattr(module.news_db, "auto_requeue_failed_llm_tasks", requeue_mock)

    result = asyncio.run(module.auto_requeue_failed_llm_tasks({}, limit=2, cooldown_sec=0))

    assert result["enabled"] is True
    assert result["reason"] == "queue_busy"
    assert result["requeued_count"] == 0
    assert requeue_mock.await_count == 0


def test_news_auto_requeue_calls_db_when_queue_idle(monkeypatch):
    import web.api.news as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_FAILED_REQUEUE_LAST_AT", None)

    queue_mock = AsyncMock(
        return_value={
            "pending_total": 0,
            "counts": {"pending": 0, "running": 0, "retry": 0, "failed": 4},
        }
    )
    requeue_mock = AsyncMock(
        return_value={
            "scanned_count": 8,
            "candidate_count": 3,
            "requeued_count": 2,
            "raw_news_ids_sample": [101, 102],
            "skipped_summary_repaired_count": 1,
            "skipped_existing_event_count": 0,
        }
    )

    monkeypatch.setattr(module.news_db, "get_llm_queue_stats", queue_mock)
    monkeypatch.setattr(module.news_db, "auto_requeue_failed_llm_tasks", requeue_mock)

    result = asyncio.run(module.auto_requeue_failed_llm_tasks({}, limit=3, hours=48, cooldown_sec=0))

    assert result["enabled"] is True
    assert result["reason"] == "requeued"
    assert result["requeued_count"] == 2
    assert requeue_mock.await_count == 1
    assert requeue_mock.await_args.kwargs["limit"] == 3

    since = requeue_mock.await_args.kwargs["since"]
    delta_hours = (module._now_utc() - since).total_seconds() / 3600
    assert 47 <= delta_hours <= 49
    assert module._FAILED_REQUEUE_LAST_AT is not None


def test_news_auto_requeue_reports_repaired_when_failed_items_are_auto_closed(monkeypatch):
    import web.api.news as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_FAILED_REQUEUE_LAST_AT", None)

    queue_mock = AsyncMock(
        return_value={
            "pending_total": 0,
            "counts": {"pending": 0, "running": 0, "retry": 0, "failed": 3},
        }
    )
    requeue_mock = AsyncMock(
        return_value={
            "scanned_count": 3,
            "candidate_count": 0,
            "requeued_count": 0,
            "raw_news_ids_sample": [],
            "skipped_summary_repaired_count": 2,
            "skipped_existing_event_count": 1,
            "closed_summary_repaired_count": 2,
            "closed_existing_event_count": 1,
        }
    )

    monkeypatch.setattr(module.news_db, "get_llm_queue_stats", queue_mock)
    monkeypatch.setattr(module.news_db, "auto_requeue_failed_llm_tasks", requeue_mock)

    result = asyncio.run(module.auto_requeue_failed_llm_tasks({}, limit=3, hours=48, cooldown_sec=0))

    assert result["enabled"] is True
    assert result["reason"] == "repaired"
    assert result["requeued_count"] == 0
    assert result["closed_summary_repaired_count"] == 2
    assert result["closed_existing_event_count"] == 1


def test_news_auto_requeue_scales_limit_for_large_failed_backlog(monkeypatch):
    import web.api.news as module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module, "_FAILED_REQUEUE_LAST_AT", None)

    queue_mock = AsyncMock(
        return_value={
            "pending_total": 0,
            "counts": {"pending": 0, "running": 0, "retry": 0, "failed": 500},
        }
    )
    requeue_mock = AsyncMock(return_value={"requeued_count": 8})

    monkeypatch.setattr(module.news_db, "get_llm_queue_stats", queue_mock)
    monkeypatch.setattr(module.news_db, "auto_requeue_failed_llm_tasks", requeue_mock)

    result = asyncio.run(module.auto_requeue_failed_llm_tasks({}, hours=48))

    assert result["backlog_tier"] == "large"
    assert result["effective_limit"] == 8
    assert result["effective_cooldown_sec"] == 30
    assert requeue_mock.await_args.kwargs["limit"] == 8


def test_build_latest_feed_does_not_cross_match_query_only_news_urls(monkeypatch):
    import web.api.news as module

    raw_row = {
        "id": 36745,
        "source": "jin10",
        "title": "【富国基金宣布降费】金十数据3月26日讯，富国基金将下调港股通互联网ETF费率。",
        "url": "https://www.jin10.com/flash_newest.jsp?id=20260326111556691800",
        "content": "港股通互联网ETF富国管理费率和托管费率同步下调。",
        "published_at": "2026-03-26T03:15:56Z",
        "payload": {
            "provider": "jin10",
            "importance_score": 64,
            "summary_title": "富国基金自3月27日起大幅降港股通互联网ETF费率",
            "summary_sentiment": "positive",
            "summary_source": "openai_responses",
        },
    }
    unrelated_event = {
        "id": 2238,
        "event_id": "n2-macro-1",
        "ts": "2026-03-26T02:25:05Z",
        "symbol": "BTCUSDT",
        "event_type": "macro",
        "sentiment": -1,
        "impact_score": 0.45,
        "model_source": "llm",
        "raw_news_id": 36660,
        "evidence": {
            "title": "【伊朗战争余波持续，韩国央行发出金融稳定风险警告】金十数据3月26日讯",
            "url": "https://www.jin10.com/flash_newest.jsp?id=20260326102505351800",
            "source": "jin10",
        },
        "payload": {
            "provider": "jin10",
            "summary_title": "伊朗战争余波下，韩国央行警告金融稳定风险",
            "summary_sentiment": "negative",
            "summary_source": "openai_responses",
        },
    }

    async def fake_list_news_raw(*, since=None, limit=0):
        return [raw_row]

    async def fake_list_events(*, symbol=None, since=None, limit=0):
        return [unrelated_event]

    async def fake_list_llm_task_status(raw_ids):
        return {36745: "done"}

    monkeypatch.setattr(module.news_db, "list_news_raw", fake_list_news_raw)
    monkeypatch.setattr(module.news_db, "list_events", fake_list_events)
    monkeypatch.setattr(module.news_db, "list_llm_task_status", fake_list_llm_task_status)

    result = asyncio.run(
        module.build_latest_feed(
            cfg={"symbols": {}, "llm": {"provider": "openai"}},
            symbol=None,
            hours=24,
            limit=10,
            summarize=False,
        )
    )

    raw_item = next(item for item in result["items"] if int(item.get("raw_news_id") or 0) == 36745)

    assert raw_item["has_event"] is False
    assert raw_item["event_id"] == ""
    assert raw_item["summary_title"] == "富国基金自3月27日起大幅降港股通互联网ETF费率"
    assert raw_item["processing_status"] == "done_no_event"


def test_clean_news_text_repairs_utf8_mojibake():
    from core.news.text_normalizer import clean_news_text

    text = "ãéæè¯å¸ï¼æ²¹ä»·æ³¢å¨ä¸è¶³ä»¥ä¿ä½¿ç¾èå¨æ¿è¿åºå¯¹ã"

    assert clean_news_text(text) == "【道明证券：油价波动不足以促使美联储激进应对】"


def test_clean_news_text_preserves_normal_non_ascii_text():
    from core.news.text_normalizer import clean_news_text

    text = "Česká advokátní komora podala žalobu na advokáta"

    assert clean_news_text(text) == text
