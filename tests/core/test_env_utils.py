from __future__ import annotations

from types import SimpleNamespace

from config.env_utils import env_bool, env_int, llm_api_enabled, sync_settings_to_environ


def test_env_bool_preserves_default_and_truthy_values():
    environ: dict[str, str] = {}

    assert env_bool("FEATURE_FLAG", True, environ=environ) is True
    assert env_bool("FEATURE_FLAG", False, environ=environ) is False

    environ["FEATURE_FLAG"] = "YeS"
    assert env_bool("FEATURE_FLAG", False, environ=environ) is True

    environ["FEATURE_FLAG"] = "off"
    assert env_bool("FEATURE_FLAG", True, environ=environ) is False


def test_env_int_falls_back_for_blank_and_invalid_values():
    environ = {"WORKERS": "", "TIMEOUT": "abc", "PORT": "18080"}

    assert env_int("WORKERS", 4, environ=environ) == 4
    assert env_int("TIMEOUT", 15, environ=environ) == 15
    assert env_int("PORT", 8000, environ=environ) == 18080


def test_sync_settings_to_environ_skips_empty_values():
    settings_obj = SimpleNamespace(
        OPENAI_API_KEY="  sk-primary  ",
        OPENAI_BACKUP_API_KEY="",
        OPENAI_MODEL=None,
        ZHIPU_MODEL="glm-4.5-air",
    )
    environ: dict[str, str] = {"EXISTING": "1"}

    synced = sync_settings_to_environ(
        settings_obj,
        ("OPENAI_API_KEY", "OPENAI_BACKUP_API_KEY", "OPENAI_MODEL", "ZHIPU_MODEL"),
        environ=environ,
    )

    assert synced == {
        "OPENAI_API_KEY": "sk-primary",
        "ZHIPU_MODEL": "glm-4.5-air",
    }
    assert environ["OPENAI_API_KEY"] == "sk-primary"
    assert environ["ZHIPU_MODEL"] == "glm-4.5-air"
    assert "OPENAI_BACKUP_API_KEY" not in environ
    assert "OPENAI_MODEL" not in environ


def test_llm_api_enabled_checks_environment_and_settings():
    settings_obj = SimpleNamespace(OPENAI_API_KEY="", OPENAI_BACKUP_API_KEY="")

    assert llm_api_enabled(settings_obj, environ={}) is False
    assert llm_api_enabled(settings_obj, environ={"OPENAI_BACKUP_API_KEY": "backup-key"}) is True

    settings_obj = SimpleNamespace(OPENAI_API_KEY="primary-key", OPENAI_BACKUP_API_KEY="")
    assert llm_api_enabled(settings_obj, environ={}) is True
