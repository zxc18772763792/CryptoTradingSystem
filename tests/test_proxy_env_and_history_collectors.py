from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace

from fastapi import FastAPI

from core.utils.proxy_env import bypasses_proxy, ensure_proxy_env


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


def test_configured_direct_hosts_join_an_inherited_no_proxy(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("HTTPS_PROXY", "http://inherited:2")
    monkeypatch.setenv("NO_PROXY", "corp.internal")
    applied = ensure_proxy_env(SimpleNamespace(HTTP_PROXY=None, HTTPS_PROXY=None, PROXY_BYPASS_HOSTS="kuaipao.ai, api.nvidia.com"))
    assert applied == ["NO_PROXY"]
    assert os.environ["NO_PROXY"].split(",") == ["corp.internal", "localhost", "127.0.0.1", "::1", "kuaipao.ai", "api.nvidia.com"]
    assert ensure_proxy_env(SimpleNamespace(HTTP_PROXY=None, HTTPS_PROXY=None, PROXY_BYPASS_HOSTS="kuaipao.ai")) == []  # idempotent


def test_bypass_matches_domain_suffixes_like_the_http_clients(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NO_PROXY", "localhost,api.nvidia.com,gateio.ws,coinglass.site")
    assert bypasses_proxy("https://integrate.api.nvidia.com/v1/chat/completions")
    assert bypasses_proxy("https://api.gateio.ws/api/v4") and bypasses_proxy("vip2.coinglass.site")
    assert not bypasses_proxy("https://api.binance.com/api/v3/ping")
    assert not bypasses_proxy("https://notnvidia.com/")
    _clear(monkeypatch)
    assert not bypasses_proxy("https://api.gateio.ws")


def test_gate_connects_direct_when_no_proxy_lists_it(monkeypatch):
    import asyncio

    from config.exchanges import ExchangeConfig, ExchangeType
    from core.exchanges import gate_connector

    created = []

    class _FakeGate:
        def __init__(self, cfg):
            self.proxies = None
            self.options = {}
            created.append(self)

        async def load_time_difference(self):
            return 5

        async def load_markets(self):
            return {}

    monkeypatch.setattr(gate_connector.ccxt, "gate", _FakeGate)
    monkeypatch.setattr(gate_connector.settings, "HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setattr(gate_connector.settings, "HTTPS_PROXY", "http://127.0.0.1:7890")
    _clear(monkeypatch)
    monkeypatch.setenv("NO_PROXY", "gateio.ws")

    def connect(proxy=None):
        config = ExchangeConfig(name="gate", exchange_type=list(ExchangeType)[0], proxy=proxy)
        assert asyncio.run(gate_connector.GateConnector(config).connect())
        return created[-1].proxies

    assert connect() is None  # global proxy skipped
    assert connect("http://explicit:1")["https"] == "http://127.0.0.1:7890"  # an explicit per-exchange proxy still applies
    monkeypatch.setenv("NO_PROXY", "localhost")
    assert connect()["http"] == "http://127.0.0.1:7890"


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
