from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

from fastapi import FastAPI
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from core.ops.service import auth as ops_auth_module
from web import main as web_main
from web.startup_mode import StartupModeDecision


def test_model_env_fields_include_news_llm_backup_chain():
    assert "NEWS_LLM_BACKUP_API_KEY" in web_main._MODEL_ENV_FIELDS
    assert "NEWS_LLM_BACKUP_BASE_URL" in web_main._MODEL_ENV_FIELDS
    assert "NEWS_LLM_BACKUP_MODEL" in web_main._MODEL_ENV_FIELDS


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


def test_observe_ws_quality_guard_forces_rest_when_degraded(monkeypatch):
    """When enabled + in a primary mode, a degraded guard returns force-REST=True."""
    from core.marketdata.ws_quality_guard import WsQualityGuard

    g = WsQualityGuard(enabled=True, window_sec=60, min_samples=1, degrade_unhealthy_fraction=0.5)
    monkeypatch.setattr(web_main, "_market_ws_quality_guard", g)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    bad = {"feed_healthy": False, "ws_hub_healthy": False, "ws_stale_symbol_count": 2, "last_tick_age_ms": 99999.0}
    monkeypatch.setattr(web_main, "_market_ws_status_snapshot", lambda **k: bad)
    assert web_main._observe_ws_quality_guard() is True
    assert g.state == "degraded"


def test_observe_ws_quality_guard_noop_when_disabled_or_shadow(monkeypatch):
    """Guard is a no-op when disabled, or when not in ui_primary/strategy_primary."""
    from core.marketdata.ws_quality_guard import WsQualityGuard

    # disabled (None) -> False
    monkeypatch.setattr(web_main, "_market_ws_quality_guard", None)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    assert web_main._observe_ws_quality_guard() is False

    # enabled but shadow mode -> never observed, returns False
    g = WsQualityGuard(enabled=True, window_sec=60, min_samples=1, degrade_unhealthy_fraction=0.5)
    monkeypatch.setattr(web_main, "_market_ws_quality_guard", g)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "shadow")
    bad = {"feed_healthy": False, "ws_hub_healthy": False, "ws_stale_symbol_count": 2, "last_tick_age_ms": 99999.0}
    monkeypatch.setattr(web_main, "_market_ws_status_snapshot", lambda **k: bad)
    assert web_main._observe_ws_quality_guard() is False
    assert g.state == "ws"


def test_runtime_pusher_quality_guard_forces_full_rest_fallback(monkeypatch):
    """A degraded guard must fetch the full watch list even when current WS ticks are fresh."""

    class _HealthyFeed:
        def is_healthy(self):
            return True

    emit_calls = []

    async def _fake_emit_market_ticks(**kwargs):
        emit_calls.append(kwargs)

    async def _fake_emit_runtime_snapshot():
        return None

    watch_symbols = ["BTC/USDT", "ETH/USDT"]
    web_main.market_data_hub.clear()
    for symbol in watch_symbols:
        web_main.market_data_hub.upsert_ws_tick("binance", symbol, {"last": 50000.0})

    monkeypatch.setattr(web_main.settings, "MARKET_WS_EXCHANGES", "binance")
    monkeypatch.setattr(web_main, "_MARKET_WS_ENABLED", True)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "ui_primary")
    monkeypatch.setattr(web_main, "_MARKET_WS_FORCE_REST", False)
    monkeypatch.setattr(web_main, "_MARKET_TICK_INTERVAL_SEC", 0.0)
    monkeypatch.setattr(web_main, "_market_ws_feed", _HealthyFeed())
    monkeypatch.setattr(web_main, "_collect_watch_symbols", lambda: list(watch_symbols))
    monkeypatch.setattr(web_main, "_observe_ws_quality_guard", lambda: True)
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
    assert emit_calls, "quality guard force-REST should not be a no-op"
    assert emit_calls[-1]["hub_source"] == "rest_fallback"
    assert emit_calls[-1]["fallback_reason"] == "ws_quality_guard"
    assert emit_calls[-1]["symbols"] == watch_symbols
    web_main.market_data_hub.clear()


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


def test_publish_market_mark_prices_writes_auxiliary_hub_channel():
    web_main.market_data_hub.clear()

    asyncio.run(
        web_main._publish_market_mark_prices(
            {
                "binance": {
                    "BTC/USDT:USDT": {
                        "mark": 50010.0,
                        "index": 50000.0,
                        "funding_rate": 0.0001,
                        "next_funding_time": 1_700_000_000_000,
                    }
                }
            }
        )
    )

    payload = web_main._market_ws_status_snapshot(include_symbols=True)
    assert payload["symbol_count"] == 0
    assert payload["auxiliary_symbol_count"] == 1
    assert payload["channel_counts"] == {"mark_price": 1}
    mark_status = payload["symbols"]["binance"]["BTC/USDT#mark_price"]
    assert mark_status["channel"] == "mark_price"
    assert mark_status["mark"] == 50010.0
    assert mark_status["index"] == 50000.0
    assert mark_status["funding_rate"] == 0.0001
    assert mark_status["next_funding_time"] == 1_700_000_000_000
    assert mark_status["source"] == "ws"
    web_main.market_data_hub.clear()


def test_market_ws_feed_worker_passes_mark_price_callback_when_enabled(monkeypatch):
    captured = {}

    class _FakeFeed:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def run(self, stop_event):
            stop_event.set()

    fake_module = SimpleNamespace(CcxtProMarketFeed=_FakeFeed, CCXT_PRO_AVAILABLE=True)
    monkeypatch.setitem(sys.modules, "core.marketdata.ccxt_pro_feed", fake_module)
    monkeypatch.setattr(web_main, "_MARKET_WS_MARK_PRICE_ENABLED", True)
    monkeypatch.setattr(web_main, "_configured_market_ws_exchange_names", lambda: ["binance"])
    monkeypatch.setattr(web_main, "_MARKET_WS_EXCHANGE_DISCOVERY_INTERVAL_SEC", 0.01)

    stop = asyncio.Event()
    asyncio.run(web_main._market_ws_feed_worker(stop))

    assert captured["watch_mark_prices"] is True
    assert captured["on_mark"] is web_main._publish_market_mark_prices
    assert captured["on_tick"] is web_main._publish_market_ticks
    assert captured["exchanges"] == ["binance"]
    assert web_main._market_ws_feed is None


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


def test_observe_ws_quality_guard_propagates_hub_ws_trust(monkeypatch):
    """Guard degrade/recover must flip the hub's WS-trust flag so the
    runtime price provider also stops trusting fresh WS ticks."""
    from core.marketdata.ws_quality_guard import WsQualityGuard
    from core.marketdata.hub import market_data_hub

    g = WsQualityGuard(
        enabled=True, window_sec=60, min_samples=1,
        degrade_unhealthy_fraction=0.5, recover_healthy_sec=0.0,
    )
    monkeypatch.setattr(web_main, "_market_ws_quality_guard", g)
    monkeypatch.setattr(web_main, "_MARKET_WS_MODE", "strategy_primary")
    try:
        bad = {"feed_healthy": False, "ws_hub_healthy": False, "ws_stale_symbol_count": 2, "last_tick_age_ms": 99999.0}
        monkeypatch.setattr(web_main, "_market_ws_status_snapshot", lambda **k: bad)
        assert web_main._observe_ws_quality_guard() is True
        assert market_data_hub.ws_trusted is False

        good = {"feed_healthy": True, "ws_hub_healthy": True, "ws_stale_symbol_count": 0, "last_tick_age_ms": 100.0}
        monkeypatch.setattr(web_main, "_market_ws_status_snapshot", lambda **k: good)
        assert web_main._observe_ws_quality_guard() is False
        assert market_data_hub.ws_trusted is True
    finally:
        market_data_hub.set_ws_trust(True)


def test_enforce_primary_mode_guard_downgrades_unguarded_primary():
    """ui_primary / strategy_primary without the quality guard must fail
    closed to shadow (the guard is the only auto-degrade protection)."""
    for mode in ("ui_primary", "strategy_primary"):
        effective, reason = web_main._enforce_primary_mode_guard(mode, guard_active=False)
        assert effective == "shadow"
        assert reason and mode in reason


def test_enforce_primary_mode_guard_keeps_guarded_or_non_primary_modes():
    for mode in ("ui_primary", "strategy_primary"):
        effective, reason = web_main._enforce_primary_mode_guard(mode, guard_active=True)
        assert effective == mode
        assert reason is None
    for mode in ("off", "shadow"):
        for guard_active in (False, True):
            effective, reason = web_main._enforce_primary_mode_guard(mode, guard_active)
            assert effective == mode
            assert reason is None


def test_exchange_watchdog_requires_consecutive_failures_and_cooldown(monkeypatch):
    """A persistently-unhealthy exchange must not be reconnect-churned every
    cycle: 3 consecutive failures arm the first reconnect, then the per-
    exchange cooldown blocks further attempts (old behaviour: one per cycle,
    killing in-flight REST requests each time)."""
    calls = {"health": 0, "reconnect": 0}
    stop_event = asyncio.Event()

    async def fake_health_check():
        calls["health"] += 1
        if calls["health"] >= 10:
            stop_event.set()
        return {"binance": False}

    async def fake_reconnect(name, **kwargs):
        calls["reconnect"] += 1
        return True

    monkeypatch.setattr(web_main.exchange_manager, "health_check", fake_health_check)
    monkeypatch.setattr(web_main.exchange_manager, "reconnect_exchange", fake_reconnect)

    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    async def run():
        task = asyncio.create_task(web_main._exchange_watchdog_worker(stop_event))
        await asyncio.wait_for(stop_event.wait(), timeout=5)
        try:
            await asyncio.wait_for(task, timeout=1)
        except (asyncio.TimeoutError, TimeoutError):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(run())
    assert calls["health"] >= 10
    # Cycle 3 fires the one allowed reconnect; the success resets the failure
    # count and the 300s cooldown (loop time barely advances under the fake
    # sleep) blocks every later attempt.
    assert calls["reconnect"] == 1
