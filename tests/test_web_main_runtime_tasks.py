from __future__ import annotations

import asyncio

from fastapi import FastAPI
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from core.ops.service import auth as ops_auth_module
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


def test_market_ws_feed_factory_gated_by_flag(monkeypatch):
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", False)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "market_ws_feed" not in factories

    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "market_ws_feed" in factories
    assert factories["market_ws_feed"]["restart_on_failure"] is True


async def test_market_ws_feed_waits_for_late_exchange_connections(monkeypatch):
    from core.marketdata import ccxt_pro_feed

    captured: dict[str, object] = {}

    class _FakeFeed:
        def __init__(self, *, exchanges, **kwargs):
            captured["exchanges"] = exchanges

        async def run(self, stop_event):
            captured["ran"] = True
            stop_event.set()

        def is_healthy(self):
            return True

    calls = {"count": 0}

    def _connected_exchanges():
        calls["count"] += 1
        return [] if calls["count"] == 1 else ["binance"]

    monkeypatch.setattr(ccxt_pro_feed, "CCXT_PRO_AVAILABLE", True)
    monkeypatch.setattr(ccxt_pro_feed, "CcxtProMarketFeed", _FakeFeed)
    monkeypatch.setattr(web_main.settings, "MARKET_WS_EXCHANGES", "")
    monkeypatch.setattr(
        web_main,
        "_MARKET_WS_EXCHANGE_DISCOVERY_INTERVAL_SEC",
        0.01,
    )
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", _connected_exchanges)

    await asyncio.wait_for(web_main._market_ws_feed_worker(asyncio.Event()), timeout=1.0)

    assert captured["exchanges"] == ["binance"]
    assert captured["ran"] is True
    assert web_main._market_ws_feed is None


def test_runtime_pusher_skips_rest_when_ws_healthy(monkeypatch):
    """When the WS feed reports healthy, the REST market-tick fan-out is skipped."""

    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = {"n": 0}

    async def _fake_emit_market_ticks():
        emit_calls["n"] += 1

    async def _fake_emit_runtime_snapshot():
        return None

    monkeypatch.setattr(web_main, "_market_ws_feed", _HealthyFeed())
    monkeypatch.setattr(web_main, "_emit_market_ticks", _fake_emit_market_ticks)
    monkeypatch.setattr(web_main, "_emit_runtime_snapshot", _fake_emit_runtime_snapshot)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)

    async def _run():
        stop = asyncio.Event()
        task = asyncio.create_task(web_main._runtime_pusher(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())
    assert emit_calls["n"] == 0, "REST ticks should be skipped while WS is healthy"


def test_runtime_pusher_uses_rest_when_ws_unhealthy(monkeypatch):
    """When the WS feed is down/absent, REST market ticks are used as fallback."""

    class _DeadFeed:
        def is_healthy(self):
            return False

    emit_calls = {"n": 0}

    async def _fake_emit_market_ticks():
        emit_calls["n"] += 1

    async def _fake_emit_runtime_snapshot():
        return None

    monkeypatch.setattr(web_main, "_market_ws_feed", _DeadFeed())
    monkeypatch.setattr(web_main, "_emit_market_ticks", _fake_emit_market_ticks)
    monkeypatch.setattr(web_main, "_emit_runtime_snapshot", _fake_emit_runtime_snapshot)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)

    async def _run():
        stop = asyncio.Event()
        task = asyncio.create_task(web_main._runtime_pusher(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())
    assert emit_calls["n"] >= 1, "REST ticks should run as fallback when WS is down"


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


def test_websocket_rejects_forged_cookie_from_non_loopback(monkeypatch):
    class _Client:
        host = "203.0.113.10"

    class _WebSocket:
        client = _Client()
        cookies = {"cts_local_ui_session": "forged"}
        headers = {}

    monkeypatch.setattr(web_main, "_ws_client_ip", lambda websocket: "203.0.113.10")
    monkeypatch.setattr(web_main, "_has_valid_local_ui_session", lambda websocket: False)

    assert web_main._ws_is_authorized(_WebSocket()) is False


def test_websocket_allows_valid_ops_token_from_non_loopback(monkeypatch):
    class _Client:
        host = "203.0.113.10"

    class _WebSocket:
        client = _Client()
        cookies = {}
        headers = {"x-ops-token": "test-token"}

    monkeypatch.setattr(web_main, "_ws_client_ip", lambda websocket: "203.0.113.10")
    monkeypatch.setattr(web_main, "_has_valid_local_ui_session", lambda websocket: False)
    monkeypatch.setattr(ops_auth_module, "get_ops_token", lambda required=False: "test-token")

    assert web_main._ws_is_authorized(_WebSocket()) is True


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


def test_startup_syncs_main_account_to_effective_live_mode(monkeypatch):
    mode_calls = []
    auto_calls = []

    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode",
        lambda account_id, mode: mode_calls.append((account_id, mode)) or True,
    )
    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode_for_auto_strategy_accounts",
        lambda mode: auto_calls.append(mode) or 0,
    )

    result = web_main._sync_startup_account_modes(
        StartupModeDecision(
            configured_mode="live",
            persisted_mode="paper",
            effective_mode="live",
            source="configured",
            blocked_persisted_live_restore=False,
        )
    )

    assert result["main_updated"] is True
    assert result["auto_strategy_accounts_updated"] == 0
    assert mode_calls == [("main", "live")]
    assert auto_calls == []


def test_startup_syncs_auto_strategy_accounts_to_paper(monkeypatch):
    mode_calls = []
    auto_calls = []

    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode",
        lambda account_id, mode: mode_calls.append((account_id, mode)) or True,
    )
    monkeypatch.setattr(
        web_main.account_manager,
        "set_mode_for_auto_strategy_accounts",
        lambda mode: auto_calls.append(mode) or 3,
    )

    result = web_main._sync_startup_account_modes(
        StartupModeDecision(
            configured_mode="paper",
            persisted_mode="live",
            effective_mode="paper",
            source="guarded_configured",
            blocked_persisted_live_restore=True,
        )
    )

    assert result["main_updated"] is True
    assert result["auto_strategy_accounts_updated"] == 3
    assert mode_calls == [("main", "paper")]
    assert auto_calls == ["paper"]
