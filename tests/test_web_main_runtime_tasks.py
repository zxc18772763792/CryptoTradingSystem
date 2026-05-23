from __future__ import annotations

import asyncio

from fastapi import FastAPI
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from web import main as web_main
from web.startup_mode import StartupModeDecision


def test_news_llm_task_runs_as_internal_fallback(monkeypatch):
    monkeypatch.setattr(web_main, "_NEWS_LLM_BACKGROUND_ENABLED", True)
    monkeypatch.setattr(web_main, "_NEWS_LLM_EXTERNAL_ONLY", False)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "news_llm" in factories


def test_news_llm_task_can_be_forced_external_only(monkeypatch):
    monkeypatch.setattr(web_main, "_NEWS_LLM_BACKGROUND_ENABLED", True)
    monkeypatch.setattr(web_main, "_NEWS_LLM_EXTERNAL_ONLY", True)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "news_llm" not in factories


def test_optional_external_data_workers_are_disabled_by_default(monkeypatch):
    monkeypatch.setattr(web_main, "_PUBLIC_MACRO_WORKERS_ENABLED", False)
    monkeypatch.setattr(web_main, "_PREMIUM_EXTERNAL_WORKERS_ENABLED", False)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "coinglass" in factories
    assert "google_trends" not in factories
    assert "macro_cache" not in factories
    assert "glassnode" not in factories
    assert "cryptoquant" not in factories
    assert "nansen" not in factories
    assert "kaiko" not in factories


def test_optional_external_data_workers_can_be_enabled(monkeypatch):
    monkeypatch.setattr(web_main, "_PUBLIC_MACRO_WORKERS_ENABLED", True)
    monkeypatch.setattr(web_main, "_PREMIUM_EXTERNAL_WORKERS_ENABLED", True)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "google_trends" in factories
    assert "macro_cache" in factories
    assert "glassnode" in factories
    assert "cryptoquant" in factories
    assert "nansen" in factories
    assert "kaiko" in factories


def test_cors_does_not_allow_wildcard_with_credentials():
    cors = next(
        middleware
        for middleware in web_main.app.user_middleware
        if isinstance(middleware, Middleware) and middleware.cls is CORSMiddleware
    )

    assert cors.kwargs["allow_credentials"] is True
    assert cors.kwargs["allow_origins"] != ["*"]
    assert "*" not in cors.kwargs["allow_origins"]


def test_websocket_rejects_non_loopback_without_credentials(monkeypatch):
    class _Client:
        host = "203.0.113.10"

    class _WebSocket:
        client = _Client()
        cookies = {}
        headers = {}

    monkeypatch.setattr(web_main, "_ws_client_ip", lambda websocket: "203.0.113.10")

    assert web_main._ws_is_authorized(_WebSocket()) is False


def test_websocket_allows_loopback_without_credentials(monkeypatch):
    class _Client:
        host = "127.0.0.1"

    class _WebSocket:
        client = _Client()
        cookies = {}
        headers = {}

    monkeypatch.setattr(web_main, "_ws_client_ip", lambda websocket: "127.0.0.1")

    assert web_main._ws_is_authorized(_WebSocket()) is True


def test_livez_and_health_are_lightweight_liveness_aliases():
    assert asyncio.run(web_main.livez_check())["status"] == "alive"
    assert asyncio.run(web_main.health_check())["status"] == "healthy"


def test_guarded_startup_syncs_main_account_back_to_paper(monkeypatch):
    calls = []

    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode",
        lambda account_id, mode: calls.append((account_id, mode)) or True,
    )

    updated = web_main._sync_guarded_startup_account_mode(
        StartupModeDecision(
            configured_mode="paper",
            persisted_mode="live",
            effective_mode="paper",
            source="guarded_configured",
            blocked_persisted_live_restore=True,
        )
    )

    assert updated is True
    assert calls == [("main", "paper")]


def test_guarded_startup_does_not_sync_when_live_is_allowed(monkeypatch):
    calls = []

    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode",
        lambda account_id, mode: calls.append((account_id, mode)) or True,
    )

    updated = web_main._sync_guarded_startup_account_mode(
        StartupModeDecision(
            configured_mode="paper",
            persisted_mode="live",
            effective_mode="live",
            source="persisted",
            blocked_persisted_live_restore=False,
        )
    )

    assert updated is False
    assert calls == []
