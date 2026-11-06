from __future__ import annotations

import asyncio
import importlib
import threading
from types import SimpleNamespace

from core.exchanges.exchange_manager import ExchangeManager


def test_failed_intended_connection_is_counted_but_unconfigured_exchange_is_not(monkeypatch):
    manager = ExchangeManager()
    async def create(name, config, **kwargs):
        return SimpleNamespace(is_connected=True) if name == "gate" else None
    monkeypatch.setattr(manager, "_create_connector", create)
    asyncio.run(manager.initialize(["gate", "binance"]))
    assert manager.get_intended_exchange_names() == ["binance", "gate"]
    assert manager.get_connected_exchanges() == ["gate"]
    assert "okx" not in manager.get_intended_exchange_names()
    assert manager.get_intended_exchange_names("another-account") == []


def test_initial_windows_news_status_scans_once_without_blocking_event_loop(monkeypatch):
    news = importlib.import_module("web.api.news")
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_PAYLOAD", {})
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_AT", 0.0)
    monkeypatch.setattr(news, "_NEWS_PROCESS_SCAN_TASK", None)
    monkeypatch.setattr(news.sys, "platform", "win32")
    calls = []
    release = threading.Event()
    released_by_event_loop = []
    def scan():
        calls.append(1)
        released_by_event_loop.append(release.wait(timeout=1.0))
        return {"worker_running": True, "llm_running": True, "worker_pids": [100],
                "llm_pids": [200], "detector": "test", "error": None}
    monkeypatch.setattr(news, "_scan_external_news_processes", scan)
    async def run():
        ticks = []
        async def heartbeat():
            for _ in range(4):
                await asyncio.sleep(0.01)
                ticks.append(1)
                release.set()
        await asyncio.gather(news._prime_news_process_snapshot(), news._prime_news_process_snapshot(), heartbeat())
        assert len(ticks) == 4
        assert len(calls) == 1
        assert released_by_event_loop == [True]
        snapshot = news._external_news_process_snapshot()
        assert snapshot["worker_running"] and snapshot["llm_running"]
        assert snapshot["worker_pids"] == [100]
        assert snapshot["detector"] != "deferred"
    asyncio.run(run())


def test_expired_windows_news_snapshot_does_not_scan_on_http_thread(monkeypatch):
    news = importlib.import_module("web.api.news")
    cached = {"worker_running": True, "llm_running": True, "worker_pids": [100],
              "llm_pids": [200], "detector": "powershell:Get-CimInstance", "error": None}
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_PAYLOAD", cached)
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_AT", 0.0)
    monkeypatch.setattr(news, "_NEWS_PROCESS_SCAN_TASK", None)
    monkeypatch.setattr(news.sys, "platform", "win32")
    def scan():
        raise AssertionError("expired cache must not spawn PowerShell on the caller")
    monkeypatch.setattr(news, "_scan_external_news_processes", scan)
    snapshot = news._external_news_process_snapshot()
    assert snapshot["worker_running"] and snapshot["llm_running"]
    assert snapshot["stale_cache"] is True


def test_windows_news_scan_timeout_reports_detector_and_uses_hidden_bounded_process(monkeypatch):
    news = importlib.import_module("web.api.news")
    monkeypatch.setattr(news.sys, "platform", "win32")
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        raise news.subprocess.TimeoutExpired(command, kwargs["timeout"])
    monkeypatch.setattr(news.subprocess, "run", run)
    snapshot = news._scan_external_news_processes()
    assert snapshot["detector"] == "powershell:Get-CimInstance"
    assert snapshot["error"] == "process scan timed out"
    assert calls[0][1]["timeout"] == 10
    assert calls[0][1]["creationflags"] == getattr(news.subprocess, "CREATE_NO_WINDOW", 0)
    assert "-Filter" in calls[0][0][-1]


def test_expired_windows_runtime_snapshot_refreshes_itself_off_loop(monkeypatch):
    news = importlib.import_module("web.api.news")
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_PAYLOAD", {
        "worker_running": True, "llm_running": False, "worker_pids": [100], "llm_pids": [],
        "detector": "test", "error": None})
    monkeypatch.setattr(news, "_NEWS_PROCESS_CACHE_AT", 0.0)
    monkeypatch.setattr(news, "_NEWS_PROCESS_SCAN_TASK", None)
    monkeypatch.setattr(news.sys, "platform", "win32")
    calls = []
    def scan():
        calls.append(threading.get_ident())
        return {"worker_running": True, "llm_running": True, "worker_pids": [100],
                "llm_pids": [200], "detector": "test", "error": None}
    monkeypatch.setattr(news, "_scan_external_news_processes", scan)
    async def run():
        loop_thread = threading.get_ident()
        initial = news._external_news_process_snapshot()
        assert initial["worker_pids"] == [100]
        task = news._NEWS_PROCESS_SCAN_TASK
        assert task is not None
        news._external_news_process_snapshot()
        assert news._NEWS_PROCESS_SCAN_TASK is task
        await task
        assert len(calls) == 1 and calls[0] != loop_thread
        assert news._external_news_process_snapshot()["llm_pids"] == [200]
    asyncio.run(run())
