from __future__ import annotations

import asyncio
from types import SimpleNamespace

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
    monkeypatch.setattr(web_main, "_COINGLASS_WORKER_ENABLED", True)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "coinglass" in factories
    assert "google_trends" not in factories
    assert "macro_cache" not in factories
    assert "glassnode" not in factories
    assert "cryptoquant" not in factories
    assert "nansen" not in factories
    assert "kaiko" not in factories


def test_coinglass_worker_factory_can_be_disabled(monkeypatch):
    monkeypatch.setattr(web_main, "_COINGLASS_WORKER_ENABLED", False)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "coinglass" not in factories


def test_optional_external_data_workers_can_be_enabled(monkeypatch):
    monkeypatch.setattr(web_main, "_COINGLASS_WORKER_ENABLED", True)
    monkeypatch.setattr(web_main, "_PUBLIC_MACRO_WORKERS_ENABLED", True)
    monkeypatch.setattr(web_main, "_PREMIUM_EXTERNAL_WORKERS_ENABLED", True)

    factories = web_main._build_runtime_task_factories(FastAPI())

    assert "coinglass" in factories
    assert "google_trends" in factories
    assert "macro_cache" in factories
    assert "glassnode" in factories
    assert "cryptoquant" in factories
    assert "nansen" in factories
    assert "kaiko" in factories


def test_exchange_watchdog_factory_can_be_disabled(monkeypatch):
    monkeypatch.setattr(web_main, "_EXCHANGE_WATCHDOG_ENABLED", True)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "exchange_watchdog" in factories

    monkeypatch.setattr(web_main, "_EXCHANGE_WATCHDOG_ENABLED", False)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "exchange_watchdog" not in factories


def test_market_ws_feed_factory_gated_by_flag(monkeypatch):
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", False)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "market_ws_feed" not in factories

    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "market_ws_feed" in factories
    assert factories["market_ws_feed"]["restart_on_failure"] is True

    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", True)
    factories = web_main._build_runtime_task_factories(FastAPI())
    assert "market_ws_feed" not in factories


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


def test_runtime_pusher_keeps_rest_in_shadow_mode_even_when_ws_healthy(monkeypatch):
    """Shadow mode records WS ticks but keeps REST as the UI/runtime source."""

    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        return None

    web_main.market_data_hub.clear()
    web_main.market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50000.0})
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
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
    assert emit_calls, "REST ticks should continue in shadow mode"
    assert emit_calls[-1]["hub_source"] == "rest_snapshot"
    assert emit_calls[-1]["fallback_reason"] == "periodic_rest_snapshot"
    web_main.market_data_hub.clear()


def test_runtime_pusher_runs_shadow_rest_reconcile_without_ui_subscribers(monkeypatch):
    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        raise AssertionError("runtime snapshot should not fan out without subscribers")

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_WS_REST_RECONCILE_SEC", 0.01)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 1000.0)
    monkeypatch.setattr(web_main, "_market_ws_feed", None)
    monkeypatch.setattr(web_main, "_emit_market_ticks", _fake_emit_market_ticks)
    monkeypatch.setattr(web_main, "_emit_runtime_snapshot", _fake_emit_runtime_snapshot)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: False)

    async def _run():
        stop = asyncio.Event()
        task = asyncio.create_task(web_main._runtime_pusher(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())
    assert emit_calls, "shadow REST reconciliation should run without UI subscribers"
    assert emit_calls[-1]["hub_source"] == "rest_snapshot"
    assert emit_calls[-1]["fallback_reason"] == "shadow_rest_reconcile"
    assert emit_calls[-1]["publish"] is False
    assert emit_calls[-1]["require_subscribers"] is False
    web_main.market_data_hub.clear()


def test_runtime_pusher_skips_rest_when_ui_primary_ws_and_hub_are_healthy(monkeypatch):
    """UI-primary mode can suppress REST only when feed and hub freshness agree."""

    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = {"n": 0}

    async def _fake_emit_market_ticks():
        emit_calls["n"] += 1

    async def _fake_emit_runtime_snapshot():
        return None

    web_main.market_data_hub.clear()
    web_main.market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50000.0})
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
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
    assert emit_calls["n"] == 0, "REST ticks should be skipped only in ui_primary with fresh hub data"
    web_main.market_data_hub.clear()


def test_runtime_pusher_uses_rest_when_ws_unhealthy(monkeypatch):
    """When the WS feed is down/absent, REST market ticks are used as fallback."""

    class _DeadFeed:
        def is_healthy(self):
            return False

    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        return None

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
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
    assert emit_calls, "REST ticks should run as fallback when WS is down"
    assert emit_calls[-1]["hub_source"] == "rest_fallback"
    assert emit_calls[-1]["fallback_reason"] == "ws_unhealthy"


def test_runtime_pusher_uses_rest_fallback_when_ui_primary_hub_is_stale(monkeypatch):
    """A healthy feed alone is not enough; stale hub data must trigger REST fallback."""

    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        return None

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
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
    assert emit_calls, "stale/missing WS hub data should trigger REST fallback"
    assert emit_calls[-1]["hub_source"] == "rest_fallback"
    assert emit_calls[-1]["fallback_reason"] == "ws_stale"


def test_symbols_requiring_ws_fallback_uses_ws_source_not_latest_rest_snapshot(monkeypatch):
    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main.settings, "MARKET_WS_EXCHANGES", "binance")
    web_main.market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50000.0})
    web_main.market_data_hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 50010.0},
        source="rest_snapshot",
    )

    missing = web_main._symbols_requiring_ws_fallback(["BTC/USDT", "ETH/USDT"])

    assert missing == ["ETH/USDT"]
    web_main.market_data_hub.clear()


def test_runtime_pusher_falls_back_only_missing_ws_symbols_in_ui_primary(monkeypatch):
    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        return None

    web_main.market_data_hub.clear()
    web_main.market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50000.0})
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
    monkeypatch.setattr(web_main.settings, "MARKET_WS_EXCHANGES", "binance")
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT", "ETH/USDT"])
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
    assert emit_calls, "missing WS symbols should trigger a targeted REST fallback"
    assert emit_calls[-1]["hub_source"] == "rest_fallback"
    assert emit_calls[-1]["fallback_reason"] == "ws_stale"
    assert emit_calls[-1]["symbols"] == ["ETH/USDT"]
    web_main.market_data_hub.clear()


def test_publish_market_ticks_writes_hub_without_ui_fanout_in_shadow(monkeypatch):
    published = []

    async def _fake_publish(event, payload=None):
        published.append((event, payload))

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _fake_publish)

    asyncio.run(
        web_main._publish_market_ticks(
            {"binance": {"BTC/USDT:USDT": {"last": 50000.0, "bid": 49999.0, "ask": 50001.0}}}
        )
    )

    assert published == []
    current = web_main.market_data_hub.get_tick("binance", "BTCUSDT")
    assert current is not None
    assert current["tick"]["source"] == "ws"
    assert current["tick"]["last"] == 50000.0
    web_main.market_data_hub.clear()


def test_publish_market_ticks_fans_out_normalized_payload_in_ui_primary(monkeypatch):
    published = []

    async def _fake_publish(event, payload=None):
        published.append((event, payload))

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _fake_publish)

    asyncio.run(
        web_main._publish_market_ticks(
            {"binance": {"BTC/USDT:USDT": {"last": 50000.0, "bid": 49999.0, "ask": 50001.0}}}
        )
    )

    assert len(published) == 1
    assert published[0][0] == "market_tick"
    assert published[0][1]["binance"]["BTC/USDT"]["source"] == "ws"
    assert published[0][1]["binance"]["BTC/USDT"]["last"] == 50000.0
    web_main.market_data_hub.clear()


def test_publish_market_ticks_keeps_hub_write_when_event_bus_publish_fails(monkeypatch):
    async def _failing_publish(event, payload=None):
        raise RuntimeError("event bus down")

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _failing_publish)

    asyncio.run(
        web_main._publish_market_ticks(
            {"binance": {"BTC/USDT": {"last": 50000.0, "bid": 49999.0, "ask": 50001.0}}}
        )
    )

    current = web_main.market_data_hub.get_tick("binance", "BTC/USDT")
    assert current is not None
    assert current["tick"]["source"] == "ws"
    assert current["tick"]["last"] == 50000.0
    web_main.market_data_hub.clear()


def test_emit_market_ticks_keeps_hub_write_when_event_bus_publish_fails(monkeypatch):
    class _Connector:
        async def get_ticker(self, symbol):
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    async def _failing_publish(event, payload=None):
        raise RuntimeError("event bus down")

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _failing_publish)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: _Connector())

    asyncio.run(web_main._emit_market_ticks(hub_source="rest_fallback", fallback_reason="test"))

    snapshot = web_main.market_data_hub.snapshot(include_symbols=True)
    assert snapshot["rest_fallback_count"] == 1
    assert snapshot["fallback_reasons"] == {"test": 1}
    assert snapshot["symbols"]["binance"]["BTC/USDT"]["source"] == "rest_fallback"
    web_main.market_data_hub.clear()


def test_market_ws_status_snapshot_exposes_force_rest_and_hub_metrics(monkeypatch):
    web_main.market_data_hub.clear()
    web_main.market_data_hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 50000.0, "bid": 49999.0, "ask": 50001.0},
        reason="test",
    )
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", True)
    monkeypatch.setattr(web_main, "_market_ws_feed", None)

    payload = web_main._market_ws_status_snapshot(include_symbols=True)

    assert payload["enabled"] is False
    assert payload["configured_enabled"] is True
    assert payload["force_rest"] is True
    assert payload["fail_closed_for_live"] is True
    assert payload["feed_present"] is False
    assert payload["rest_fallback_count"] == 1
    assert payload["symbols"]["binance"]["BTC/USDT"]["source"] == "rest_fallback"
    web_main.market_data_hub.clear()


def test_market_ws_status_snapshot_exposes_feed_diagnostics(monkeypatch):
    class _DiagnosticFeed:
        def is_healthy(self):
            return False

        def healthy_exchanges(self, **_kwargs):
            return []

        def status_snapshot(self):
            return {
                "watch_attempt_count": 5,
                "watch_timeout_count": 2,
                "watch_error_count": 1,
                "watch_empty_count": 3,
                "last_error": "RuntimeError: socket dropped",
                "exchanges": {
                    "binance": {
                        "last_symbols": ["BTC/USDT"],
                        "watch_timeout_count": 2,
                    }
                },
            }

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_market_ws_feed", _DiagnosticFeed())

    payload = web_main._market_ws_status_snapshot(include_symbols=True)

    assert payload["enabled"] is True
    assert payload["feed_present"] is True
    assert payload["feed_healthy"] is False
    assert payload["feed_watch_attempt_count"] == 5
    assert payload["feed_watch_timeout_count"] == 2
    assert payload["feed_watch_error_count"] == 1
    assert payload["feed_watch_empty_count"] == 3
    assert payload["feed_last_error"] == "RuntimeError: socket dropped"
    assert payload["feed_status"]["exchanges"]["binance"]["last_symbols"] == ["BTC/USDT"]
    web_main.market_data_hub.clear()


def test_market_data_status_route_includes_symbol_details(monkeypatch):
    web_main.market_data_hub.clear()
    web_main.market_data_hub.upsert_ws_tick(
        "binance",
        "BTC/USDT:USDT",
        {"last": 50000.0, "bid": 49999.0, "ask": 50001.0},
    )
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)

    payload = asyncio.run(web_main.get_market_data_status())

    assert payload["enabled"] is True
    assert payload["mode"] == "shadow"
    assert payload["symbols"]["binance"]["BTC/USDT"]["source"] == "ws"
    web_main.market_data_hub.clear()


def test_emit_market_ticks_records_rest_snapshot_without_fallback_count(monkeypatch):
    published = []

    class _Connector:
        async def get_ticker(self, symbol):
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    async def _fake_publish(event, payload=None):
        published.append((event, payload))

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: True)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _fake_publish)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: _Connector())

    asyncio.run(web_main._emit_market_ticks(hub_source="rest_snapshot"))

    snapshot = web_main.market_data_hub.snapshot()
    assert snapshot["rest_snapshot_count"] == 1
    assert snapshot["rest_fallback_count"] == 0
    assert published[0][0] == "market_tick"
    assert published[0][1]["binance"]["BTC/USDT"]["last"] == 50000.0
    web_main.market_data_hub.clear()


def test_emit_market_ticks_honors_target_symbol_subset(monkeypatch):
    calls = []

    class _Connector:
        async def get_ticker(self, symbol):
            calls.append(symbol)
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: False)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT", "ETH/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: _Connector())

    asyncio.run(
        web_main._emit_market_ticks(
            hub_source="rest_fallback",
            fallback_reason="ws_stale",
            publish=False,
            require_subscribers=False,
            symbols=["ETH/USDT"],
        )
    )

    snapshot = web_main.market_data_hub.snapshot(include_symbols=True)
    assert calls == ["ETH/USDT"]
    assert snapshot["rest_fallback_count"] == 1
    assert "BTC/USDT" not in snapshot["symbols"]["binance"]
    assert snapshot["symbols"]["binance"]["ETH/USDT"]["source"] == "rest_fallback"
    web_main.market_data_hub.clear()


def test_emit_market_ticks_can_reconcile_without_subscribers(monkeypatch):
    published = []

    class _Connector:
        async def get_ticker(self, symbol):
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    async def _fake_publish(event, payload=None):
        published.append((event, payload))

    web_main.market_data_hub.clear()
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: False)
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", _fake_publish)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: _Connector())

    asyncio.run(
        web_main._emit_market_ticks(
            hub_source="rest_snapshot",
            fallback_reason="shadow_rest_reconcile",
            publish=False,
            require_subscribers=False,
        )
    )

    snapshot = web_main.market_data_hub.snapshot(include_symbols=True)
    assert snapshot["rest_snapshot_count"] == 1
    assert snapshot["rest_fallback_count"] == 0
    assert snapshot["symbols"]["binance"]["BTC/USDT"]["source"] == "rest_snapshot"
    assert published == []
    web_main.market_data_hub.clear()


def test_emit_market_ticks_reconnects_disconnected_exchange_for_shadow_reconcile(monkeypatch):
    class _DisconnectedConnector:
        is_connected = False

    class _ConnectedConnector:
        is_connected = True

        async def get_ticker(self, symbol):
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    reconnects = []
    disconnected = _DisconnectedConnector()
    connected = _ConnectedConnector()

    async def _ensure_exchange(name):
        reconnects.append(name)
        return connected

    web_main.market_data_hub.clear()
    web_main._market_tick_reconnect_last_attempt.clear()
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: False)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: [])
    monkeypatch.setattr(web_main.exchange_manager, "get_all_exchanges", lambda: {"binance": disconnected})
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: disconnected)
    monkeypatch.setattr(web_main.exchange_manager, "ensure_exchange", _ensure_exchange)

    asyncio.run(
        web_main._emit_market_ticks(
            hub_source="rest_snapshot",
            fallback_reason="shadow_rest_reconcile",
            publish=False,
            require_subscribers=False,
        )
    )

    snapshot = web_main.market_data_hub.snapshot()
    assert reconnects == ["binance"]
    assert snapshot["rest_snapshot_count"] == 1
    web_main.market_data_hub.clear()
    web_main._market_tick_reconnect_last_attempt.clear()


def test_emit_market_ticks_reconnects_missing_configured_exchange_for_shadow_reconcile(monkeypatch):
    class _ConnectedConnector:
        is_connected = True

        async def get_ticker(self, symbol):
            return SimpleNamespace(
                last=50000.0,
                bid=49999.0,
                ask=50001.0,
                timestamp=None,
            )

    reconnects = []
    connected = _ConnectedConnector()

    async def _ensure_exchange(name):
        reconnects.append(name)
        return connected

    web_main.market_data_hub.clear()
    web_main._market_tick_reconnect_last_attempt.clear()
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    monkeypatch.setattr(web_main.settings, "MARKET_WS_EXCHANGES", "binance")
    monkeypatch.setattr(web_main.event_bus, "has_subscribers", lambda: False)
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: ["BTC/USDT"])
    monkeypatch.setattr(web_main.exchange_manager, "get_connected_exchanges", lambda: [])
    monkeypatch.setattr(web_main.exchange_manager, "get_all_exchanges", lambda: {})
    monkeypatch.setattr(web_main.exchange_manager, "get_exchange", lambda name: None)
    monkeypatch.setattr(web_main.exchange_manager, "ensure_exchange", _ensure_exchange)

    asyncio.run(
        web_main._emit_market_ticks(
            hub_source="rest_snapshot",
            fallback_reason="shadow_rest_reconcile",
            publish=False,
            require_subscribers=False,
        )
    )

    snapshot = web_main.market_data_hub.snapshot()
    assert reconnects == ["binance"]
    assert snapshot["rest_snapshot_count"] == 1
    web_main.market_data_hub.clear()
    web_main._market_tick_reconnect_last_attempt.clear()


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
