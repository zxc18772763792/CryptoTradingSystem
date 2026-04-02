from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

from config.settings import settings


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

    def post(self, url, *, headers=None, json=None):
        self._capture["url"] = url
        self._capture["headers"] = headers
        self._capture["json"] = json
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

    def request(self, method, url, *, headers=None, json=None):
        self._capture.setdefault("urls", []).append(url)
        self._capture.setdefault("requests", []).append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "json": json,
            }
        )
        if not self._responses:
            raise AssertionError("unexpected extra request")
        return self._responses.pop(0)

    def post(self, url, *, headers=None, json=None):
        return self.request("POST", url, headers=headers, json=json)


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

    assert result == {"hypothesis": "backup relay ok"}
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
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
    assert sync_module._llm_model(legacy_cfg) == "gpt-5.1-codex-mini"
    assert async_module._llm_model(legacy_cfg) == "gpt-5.1-codex-mini"
    assert sync_module._llm_base_url(legacy_cfg) == "https://vpsairobot.com/v1"
    assert async_module._llm_base_url(legacy_cfg) == "https://vpsairobot.com/v1"


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
    assert result["source"] == "openai_responses"
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
    assert capture["urls"] == [
        "https://primary.test/v1/responses",
        "https://backup.test/v1/responses",
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
