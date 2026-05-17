from __future__ import annotations

from fastapi import FastAPI

from web import main as web_main


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
