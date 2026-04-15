import json
from datetime import datetime
from zoneinfo import ZoneInfo

import core.utils.openai_responses as openai_responses
from core.utils.openai_responses import build_target_headers, openai_endpoint_targets


def test_openai_endpoint_targets_support_multiple_backup_api_keys():
    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://secondary.test/v1,https://tertiary.test/v1",
        primary_api_key="primary-key",
        backup_api_key="secondary-key,tertiary-key",
    )

    assert [target["base_url"] for target in targets] == [
        "https://primary.test/v1",
        "https://secondary.test/v1",
        "https://tertiary.test/v1",
    ]
    assert [target["api_key"] for target in targets] == [
        "primary-key",
        "secondary-key",
        "tertiary-key",
    ]


def test_openai_endpoint_targets_support_per_source_models():
    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://secondary.test/v1,https://tertiary.test/v1",
        primary_api_key="primary-key",
        backup_api_key="secondary-key,tertiary-key",
        primary_model="gpt-5.4",
        backup_model="gpt-5.4,mimo-v2-flash",
    )

    assert [target["model"] for target in targets] == [
        "gpt-5.4",
        "gpt-5.4",
        "mimo-v2-flash",
    ]


def test_openai_endpoint_targets_detect_anthropic_style_backup():
    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://secondary.test/v1,https://api.xiaomimimo.com/anthropic/v1",
        primary_api_key="primary-key",
        backup_api_key="secondary-key,mimo-key",
        primary_model="gpt-5.4",
        backup_model="gpt-5.4,mimo-v2-flash",
    )

    assert [target["transport"] for target in targets] == [
        "openai",
        "openai",
        "anthropic",
    ]
    assert build_target_headers(targets[2])["api-key"] == "mimo-key"


class _SyncResponse:
    def __init__(self, payload, *, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {"content-type": "application/json"}
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self):
        return self._payload


def test_news_failover_uses_per_source_models(monkeypatch, tmp_path):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("OPENAI_BASE_URL", "https://primary.test/v1")
    monkeypatch.setenv("OPENAI_BACKUP_BASE_URL", "https://secondary.test/v1,https://tertiary.test/v1")
    monkeypatch.setenv("OPENAI_API_KEY", "primary-key")
    monkeypatch.setenv("OPENAI_BACKUP_API_KEY", "secondary-key,tertiary-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5.4")
    monkeypatch.setenv("OPENAI_BACKUP_MODEL", "gpt-5.4,mimo-v2-flash")
    monkeypatch.setenv("ZHIPU_API_KEY", "")
    openai_responses.reset_openai_target_preferences(scope="news")

    calls = []
    responses = iter(
        [
            _SyncResponse({"error": {"message": "primary failed"}}, status_code=500),
            _SyncResponse({"error": {"message": "secondary failed"}}, status_code=500),
            _SyncResponse(
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": '{"summary":"ETF approval positive","sentiment":"positive"}',
                                }
                            ],
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers, "json": dict(json or {}), "timeout": timeout})
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("BTC ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "ETF approval positive"
    assert result["sentiment"] == "positive"
    assert result["source"] == "openai_responses"
    assert [call["url"] for call in calls] == [
        "https://primary.test/v1/responses",
        "https://secondary.test/v1/responses",
        "https://tertiary.test/v1/responses",
    ]
    assert [call["json"]["model"] for call in calls] == [
        "gpt-5.4",
        "gpt-5.4",
        "mimo-v2-flash",
    ]


def test_news_failover_supports_anthropic_style_backup(monkeypatch, tmp_path):
    import core.news.eventizer.llm_glm5 as module

    module._SUMMARY_CACHE.clear()
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "openai_failover_state.json"))
    monkeypatch.setenv("OPENAI_BASE_URL", "https://primary.test/v1")
    monkeypatch.setenv("OPENAI_BACKUP_BASE_URL", "https://api.xiaomimimo.com/anthropic/v1")
    monkeypatch.setenv("OPENAI_API_KEY", "primary-key")
    monkeypatch.setenv("OPENAI_BACKUP_API_KEY", "mimo-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5.4")
    monkeypatch.setenv("OPENAI_BACKUP_MODEL", "mimo-v2-flash")
    monkeypatch.setenv("ZHIPU_API_KEY", "")
    openai_responses.reset_openai_target_preferences(scope="news")

    calls = []
    responses = iter(
        [
            _SyncResponse({"error": {"message": "primary failed"}}, status_code=503),
            _SyncResponse(
                {
                    "content": [
                        {
                            "type": "text",
                            "text": '{"summary":"MiMo anthropic backup","sentiment":"positive"}',
                        }
                    ]
                }
            ),
        ]
    )

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers, "json": dict(json or {}), "timeout": timeout})
        return next(responses)

    monkeypatch.setattr(module.requests, "post", _fake_post)

    result = module.summarize_title_glm5("BTC ETF approved", {"llm": {"provider": "openai"}}, max_length=60)

    assert result["summary"] == "MiMo anthropic backup"
    assert result["sentiment"] == "positive"
    assert result["source"] == "openai_responses"
    assert [call["url"] for call in calls] == [
        "https://primary.test/v1/responses",
        "https://api.xiaomimimo.com/anthropic/v1/messages",
    ]
    assert calls[1]["json"]["model"] == "mimo-v2-flash"
    assert calls[1]["headers"]["api-key"] == "mimo-key"


def test_scoped_openai_failover_sticks_to_backup_until_next_day(monkeypatch, tmp_path):
    state_path = tmp_path / "openai_failover_state.json"
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(state_path))
    monkeypatch.setenv("OPENAI_FAILOVER_TZ", "Asia/Shanghai")
    openai_responses.reset_openai_target_preferences()

    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://backup-a.test/v1,https://backup-b.test/v1",
        primary_api_key="primary-key",
        backup_api_key="backup-a-key,backup-b-key",
    )

    day_one = datetime(2026, 4, 6, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    day_two = datetime(2026, 4, 7, 0, 1, tzinfo=ZoneInfo("Asia/Shanghai"))

    monkeypatch.setattr(openai_responses, "_openai_failover_now", lambda: day_one)
    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://primary.test/v1",
        "https://backup-a.test/v1",
        "https://backup-b.test/v1",
    ]

    openai_responses.remember_openai_target_failure(targets, "https://primary.test/v1", scope="news")
    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://backup-a.test/v1",
        "https://backup-b.test/v1",
    ]

    openai_responses.remember_openai_target_failure(targets, "https://backup-a.test/v1", scope="news")
    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://backup-b.test/v1",
        "https://backup-a.test/v1",
    ]

    monkeypatch.setattr(openai_responses, "_openai_failover_now", lambda: day_two)
    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://primary.test/v1",
        "https://backup-a.test/v1",
        "https://backup-b.test/v1",
    ]


def test_scoped_openai_failover_uses_in_memory_state_without_env(monkeypatch):
    monkeypatch.delenv("OPENAI_FAILOVER_STATE_PATH", raising=False)
    monkeypatch.setattr(
        openai_responses,
        "_openai_failover_now",
        lambda: datetime(2026, 4, 6, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    openai_responses.reset_openai_target_preferences()

    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://backup.test/v1",
        primary_api_key="primary-key",
        backup_api_key="backup-key",
    )

    openai_responses.remember_openai_target_failure(targets, "https://primary.test/v1", scope="news")

    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://backup.test/v1",
    ]


def test_scoped_openai_chat_preference_resets_next_day(monkeypatch):
    monkeypatch.delenv("OPENAI_FAILOVER_STATE_PATH", raising=False)
    day_one = datetime(2026, 4, 6, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    day_two = datetime(2026, 4, 7, 0, 1, tzinfo=ZoneInfo("Asia/Shanghai"))
    openai_responses.reset_openai_target_preferences()

    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://backup.test/v1",
        primary_api_key="primary-key",
        backup_api_key="backup-key",
    )

    monkeypatch.setattr(openai_responses, "_openai_failover_now", lambda: day_one)
    assert openai_responses.should_prefer_openai_target_chat_completions(
        targets,
        "https://primary.test/v1",
        scope="news",
    ) is False

    openai_responses.remember_openai_target_chat_preference(
        targets,
        "https://primary.test/v1",
        scope="news",
    )
    assert openai_responses.should_prefer_openai_target_chat_completions(
        targets,
        "https://primary.test/v1",
        scope="news",
    ) is True

    openai_responses.clear_openai_target_chat_preference(
        targets,
        "https://primary.test/v1",
        scope="news",
    )
    assert openai_responses.should_prefer_openai_target_chat_completions(
        targets,
        "https://primary.test/v1",
        scope="news",
    ) is False

    openai_responses.remember_openai_target_chat_preference(
        targets,
        "https://primary.test/v1",
        scope="news",
    )
    monkeypatch.setattr(openai_responses, "_openai_failover_now", lambda: day_two)
    assert openai_responses.should_prefer_openai_target_chat_completions(
        targets,
        "https://primary.test/v1",
        scope="news",
    ) is False


def test_scoped_openai_failover_is_isolated_per_scope(monkeypatch, tmp_path):
    state_path = tmp_path / "openai_failover_state.json"
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(state_path))
    monkeypatch.setenv("OPENAI_FAILOVER_TZ", "Asia/Shanghai")
    monkeypatch.setattr(
        openai_responses,
        "_openai_failover_now",
        lambda: datetime(2026, 4, 6, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    openai_responses.reset_openai_target_preferences()

    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://backup.test/v1",
        primary_api_key="primary-key",
        backup_api_key="backup-key",
    )

    openai_responses.remember_openai_target_failure(targets, "https://primary.test/v1", scope="news")

    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="news")] == [
        "https://backup.test/v1",
    ]
    assert [item["base_url"] for item in openai_responses.prioritize_openai_targets(targets, scope="ai_research")] == [
        "https://primary.test/v1",
        "https://backup.test/v1",
    ]
