from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace

from fastapi import FastAPI

from core.utils.proxy_env import ensure_proxy_env


def _clear(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)


def test_proxy_from_settings_is_exported_when_the_environment_has_none(monkeypatch):
    _clear(monkeypatch)
    applied = ensure_proxy_env(SimpleNamespace(HTTP_PROXY="http://p:1", HTTPS_PROXY="http://p:1"))
    assert applied == ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"]
    assert os.environ["HTTPS_PROXY"] == "http://p:1" and "127.0.0.1" in os.environ["NO_PROXY"]


def test_an_inherited_proxy_environment_wins(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("https_proxy", "http://inherited:2")
    applied = ensure_proxy_env(SimpleNamespace(HTTP_PROXY=None, HTTPS_PROXY="http://p:1"))
    assert applied == [] and os.environ["https_proxy"] == "http://inherited:2"  # untouched


def test_only_the_whale_history_collector_runs_by_default(monkeypatch):
    import web.main as web_main

    monkeypatch.setattr(web_main, "_ANALYTICS_HISTORY_SELECTED", {"whales"})
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "analytics_history_whales" in factories
    assert "analytics_history_microstructure" not in factories
    assert "analytics_history_community" not in factories


def test_history_runs_wait_for_a_quiet_loop(monkeypatch):
    import web.main as web_main
    from core.monitoring import loop_stall_watchdog

    monkeypatch.setattr(loop_stall_watchdog, "recent_stalls", lambda: [{"at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}])
    assert web_main._web_loop_recently_stalled() is True
    monkeypatch.setattr(loop_stall_watchdog, "recent_stalls", lambda: [{"at": "2026-01-01 00:00:00"}])
    assert web_main._web_loop_recently_stalled() is False
    monkeypatch.setattr(loop_stall_watchdog, "recent_stalls", lambda: [])
    assert web_main._web_loop_recently_stalled() is False
