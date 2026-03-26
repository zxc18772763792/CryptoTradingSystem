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


class _SyncResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload


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


def test_news_llm_defaults_use_openai_codex_mini(monkeypatch):
    import core.news.eventizer.async_glm_client as async_module
    import core.news.eventizer.llm_glm5 as sync_module

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    assert sync_module._llm_provider({}) == "openai"
    assert async_module._llm_provider({}) == "openai"
    assert sync_module._llm_model({}) == "gpt-5.1-codex-mini"
    assert async_module._llm_model({}) == "gpt-5.1-codex-mini"


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
    assert result["source"] == "openai_responses"
    assert capture["url"] == "https://example.test/v1/responses"
    assert capture["json"]["model"] == "gpt-5.1-codex-mini"


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


def test_news_feed_summarize_cfg_uses_llm_defaults_when_env_absent(monkeypatch):
    import web.api.news as module

    monkeypatch.delenv("NEWS_API_SUMMARY_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NEWS_API_SUMMARIZE_TIMEOUT_SEC", raising=False)

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
    assert effective["llm"]["summarize_timeout_sec"] == 20


def test_clean_news_text_repairs_utf8_mojibake():
    from core.news.text_normalizer import clean_news_text

    text = "ãéæè¯å¸ï¼æ²¹ä»·æ³¢å¨ä¸è¶³ä»¥ä¿ä½¿ç¾èå¨æ¿è¿åºå¯¹ã"

    assert clean_news_text(text) == "【道明证券：油价波动不足以促使美联储激进应对】"


def test_clean_news_text_preserves_normal_non_ascii_text():
    from core.news.text_normalizer import clean_news_text

    text = "Česká advokátní komora podala žalobu na advokáta"

    assert clean_news_text(text) == text
