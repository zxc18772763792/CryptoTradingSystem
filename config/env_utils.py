"""Shared helpers for reading environment-backed runtime flags consistently."""
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any


_TRUTHY_VALUES = {"1", "true", "yes", "on", "y"}


def _resolve_environ(environ: Mapping[str, str] | None = None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def env_bool(
    name: str,
    default: bool = False,
    *,
    environ: Mapping[str, str] | None = None,
) -> bool:
    raw = str(_resolve_environ(environ).get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in _TRUTHY_VALUES


def env_int(
    name: str,
    default: int,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    try:
        return int(_resolve_environ(environ).get(name) or default)
    except Exception:
        return int(default)


def env_float(
    name: str,
    default: float,
    *,
    environ: Mapping[str, str] | None = None,
) -> float:
    try:
        return float(_resolve_environ(environ).get(name) or default)
    except Exception:
        return float(default)


def has_configured_value(*values: Any) -> bool:
    return any(str(value or "").strip() for value in values)


def llm_api_enabled(
    settings_obj: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> bool:
    env = _resolve_environ(environ)
    return has_configured_value(
        env.get("NEWS_LLM_API_KEY"),
        getattr(settings_obj, "NEWS_LLM_API_KEY", ""),
        env.get("OPENAI_API_KEY"),
        getattr(settings_obj, "OPENAI_API_KEY", ""),
        env.get("OPENAI_BACKUP_API_KEY"),
        getattr(settings_obj, "OPENAI_BACKUP_API_KEY", ""),
    )


def sync_settings_to_environ(
    settings_obj: Any,
    field_names: Iterable[str],
    *,
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    target = os.environ if environ is None else environ
    synced: dict[str, str] = {}
    for field_name in field_names:
        value = getattr(settings_obj, field_name, None)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        target[field_name] = text
        synced[field_name] = text
    return synced
