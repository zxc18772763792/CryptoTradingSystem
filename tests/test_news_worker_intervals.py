from core.news.service import worker as worker_module


def test_coinglass_low_budget_mode_slows_news_sources(monkeypatch):
    monkeypatch.setenv("COINGLASS_RATE_LIMIT_PER_MIN", "10")
    monkeypatch.delenv("NEWS_COINGLASS_LOW_BUDGET_MODE", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_ARTICLES", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_NEWSFLASH", raising=False)

    assert worker_module._source_interval("coinglass_articles") == 300
    assert worker_module._source_interval("coinglass_newsflash") == 600


def test_explicit_interval_override_wins_under_low_budget(monkeypatch):
    monkeypatch.setenv("COINGLASS_RATE_LIMIT_PER_MIN", "10")
    monkeypatch.setenv("NEWS_INTERVAL_COINGLASS_ARTICLES", "120")

    assert worker_module._source_interval("coinglass_articles") == 120


def test_default_coinglass_budget_keeps_news_near_realtime(monkeypatch):
    monkeypatch.setenv("COINGLASS_RATE_LIMIT_PER_MIN", "30")
    monkeypatch.delenv("NEWS_COINGLASS_LOW_BUDGET_MODE", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_ARTICLES", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_NEWSFLASH", raising=False)

    assert worker_module._source_interval("coinglass_newsflash") == 30
    assert worker_module._source_interval("coinglass_articles") == 180


def test_opennews_default_interval_is_near_realtime(monkeypatch):
    monkeypatch.delenv("NEWS_INTERVAL_OPENNEWS", raising=False)

    assert worker_module._source_interval("opennews") == 45


def test_worker_cfg_caps_batch_to_one_for_local_gemma_backup(monkeypatch):
    monkeypatch.setenv("NEWS_LLM_WORKER_BATCH_SIZE", "8")
    monkeypatch.delenv("NEWS_LLM_LOCAL_BACKUP_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    effective = worker_module._worker_cfg(
        {
            "llm": {
                "provider": "openai",
                "backup_base_url": "http://192.168.1.24:8010/v1",
                "backup_model": "gemma4-local",
            }
        },
        limit=8,
    )

    assert worker_module._has_local_gemma_backup(effective) is True
    assert effective["llm"]["batch_size"] == 1


def test_worker_cfg_allows_explicit_local_gemma_batch_cap(monkeypatch):
    monkeypatch.setenv("NEWS_LLM_WORKER_BATCH_SIZE", "8")
    monkeypatch.setenv("NEWS_LLM_LOCAL_BACKUP_BATCH_SIZE", "2")
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    effective = worker_module._worker_cfg(
        {
            "llm": {
                "provider": "openai",
                "backup_base_url": "http://192.168.1.24:8010/v1",
                "backup_model": "gemma4-local",
            }
        },
        limit=8,
    )

    assert effective["llm"]["batch_size"] == 2


def test_worker_cfg_keeps_batch_size_without_local_gemma_backup(monkeypatch):
    monkeypatch.setenv("NEWS_LLM_WORKER_BATCH_SIZE", "8")
    monkeypatch.delenv("NEWS_LLM_LOCAL_BACKUP_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_MODEL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    effective = worker_module._worker_cfg({"llm": {"provider": "openai"}}, limit=8)

    assert worker_module._has_local_gemma_backup(effective) is False
    assert effective["llm"]["batch_size"] == 8


def test_worker_cfg_preserves_yaml_timeout_when_env_absent(monkeypatch):
    monkeypatch.delenv("NEWS_LLM_WORKER_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("NEWS_LLM_WORKER_CONNECT_TIMEOUT_SEC", raising=False)

    effective = worker_module._worker_cfg(
        {"llm": {"provider": "openai", "timeout_sec": 90, "connect_timeout_sec": 12}},
        limit=8,
    )

    assert effective["llm"]["timeout_sec"] == 90
    assert effective["llm"]["connect_timeout_sec"] == 12


def test_worker_cfg_explicit_env_timeout_overrides_yaml(monkeypatch):
    monkeypatch.setenv("NEWS_LLM_WORKER_TIMEOUT_SEC", "30")
    monkeypatch.setenv("NEWS_LLM_WORKER_CONNECT_TIMEOUT_SEC", "4")

    effective = worker_module._worker_cfg(
        {"llm": {"provider": "openai", "timeout_sec": 90, "connect_timeout_sec": 12}},
        limit=8,
    )

    assert effective["llm"]["timeout_sec"] == 30
    assert effective["llm"]["connect_timeout_sec"] == 4


def test_has_local_gemma_backup_reads_settings_values(monkeypatch):
    monkeypatch.delenv("NEWS_LLM_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("NEWS_LLM_BACKUP_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_BACKUP_MODEL", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_BASE_URL", "http://192.168.1.24:8010/v1", raising=False)
    monkeypatch.setattr(worker_module.settings, "NEWS_LLM_BACKUP_MODEL", "gemma4-local", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_BASE_URL", "", raising=False)
    monkeypatch.setattr(worker_module.settings, "OPENAI_BACKUP_MODEL", "", raising=False)

    assert worker_module._has_local_gemma_backup({"llm": {"provider": "openai"}}) is True
