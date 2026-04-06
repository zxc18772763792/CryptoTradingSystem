from datetime import datetime
from zoneinfo import ZoneInfo

import core.utils.openai_responses as openai_responses
from core.utils.openai_responses import openai_endpoint_targets


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
