from __future__ import annotations

import asyncio
import time

from web.api import trading as trading_api


async def _sleep_forever() -> None:
    await asyncio.sleep(60)


def test_clear_trading_api_runtime_caches_clears_all_cache_families():
    async def _run() -> None:
        trading_api._clear_trading_api_runtime_caches()

        risk_task = asyncio.create_task(_sleep_forever())
        micro_task = asyncio.create_task(_sleep_forever())
        community_task = asyncio.create_task(_sleep_forever())
        rule_price_task = asyncio.create_task(_sleep_forever())

        trading_api._BALANCE_SNAPSHOT_CACHE["acct"] = {"distribution": {"USDT": 120.0}}
        trading_api._MICROSTRUCTURE_SNAPSHOT_CACHE["binance|BTC/USDT"] = {"payload": {"ok": True}}
        trading_api._COMMUNITY_OVERVIEW_CACHE["binance|BTC/USDT"] = {"payload": {"ok": True}}
        trading_api._RISK_DASHBOARD_CACHE["30"] = {"payload": {"ok": True}}
        trading_api._SECURITY_ALERTS_CACHE["BTC"] = {"events": []}
        trading_api._TRADING_CALENDAR_CACHE["macro"] = {"events": []}
        trading_api._ANALYTICS_HISTORY_HEALTH_CACHE["binance|BTC/USDT|24"] = {"payload": {"ok": True}}
        trading_api._ANALYTICS_HISTORY_STATUS_CACHE["binance|BTC/USDT"] = {"payload": {"ok": True}}
        trading_api._ANALYTICS_HISTORY_STATUS_LAST["binance|BTC/USDT"] = {"collector": "microstructure"}
        trading_api._RULE_PRICE_CACHE["ts"] = time.time()
        trading_api._RULE_PRICE_CACHE["prices"] = {"BTC": 68000.0}
        trading_api._LIVE_POSITION_SNAPSHOT_CACHE["ts"] = time.time()
        trading_api._LIVE_POSITION_SNAPSHOT_CACHE["data"] = {"items": [1]}
        trading_api._LIVE_POSITION_DETAILS_CACHE["ts"] = time.time()
        trading_api._LIVE_POSITION_DETAILS_CACHE["positions"] = [{"symbol": "BTC/USDT"}]
        trading_api._LIVE_POSITION_DETAILS_CACHE["diagnostics"] = {"ok": True}
        trading_api._LIVE_ORDER_DETAILS_CACHE["ts"] = time.time()
        trading_api._LIVE_ORDER_DETAILS_CACHE["orders"] = [{"id": "ord-1"}]
        trading_api._RISK_DASHBOARD_REFRESH_TASKS["30"] = risk_task
        trading_api._MICROSTRUCTURE_REFRESH_TASKS["binance|BTC/USDT"] = micro_task
        trading_api._COMMUNITY_REFRESH_TASKS["binance|BTC/USDT"] = community_task
        trading_api._RULE_PRICE_IN_FLIGHT = rule_price_task

        result = trading_api._clear_trading_api_runtime_caches()
        await asyncio.sleep(0)

        assert result["balance_entries_cleared"] == 1
        assert result["microstructure_entries_cleared"] == 1
        assert result["community_overview_entries_cleared"] == 1
        assert result["risk_dashboard_entries_cleared"] == 1
        assert result["security_alert_entries_cleared"] == 1
        assert result["trading_calendar_entries_cleared"] == 1
        assert result["analytics_history_health_entries_cleared"] == 1
        assert result["analytics_history_status_entries_cleared"] == 1
        assert result["analytics_history_status_last_entries_cleared"] == 1
        assert result["rule_price_entries_cleared"] == 1
        assert result["risk_dashboard_refresh_tasks_cancelled"] == 1
        assert result["microstructure_refresh_tasks_cancelled"] == 1
        assert result["community_refresh_tasks_cancelled"] == 1
        assert result["rule_price_in_flight_cancelled"] is True
        assert risk_task.cancelled() is True
        assert micro_task.cancelled() is True
        assert community_task.cancelled() is True
        assert rule_price_task.cancelled() is True

        inspect = trading_api._inspect_trading_api_runtime_caches()
        assert inspect["balance_snapshot_entries"] == 0
        assert inspect["microstructure_snapshot_entries"] == 0
        assert inspect["community_overview_entries"] == 0
        assert inspect["risk_dashboard_entries"] == 0
        assert inspect["security_alert_entries"] == 0
        assert inspect["trading_calendar_entries"] == 0
        assert inspect["analytics_history_health_entries"] == 0
        assert inspect["analytics_history_status_entries"] == 0
        assert inspect["analytics_history_status_last_entries"] == 0
        assert inspect["rule_price_entries"] == 0
        assert inspect["risk_dashboard_refresh_tasks"] == 0
        assert inspect["microstructure_refresh_tasks"] == 0
        assert inspect["community_refresh_tasks"] == 0
        assert inspect["rule_price_in_flight"] is False
        assert inspect["rule_price_cache_age_sec"] is None

    asyncio.run(_run())


def test_cache_runtime_helpers_strip_runtime_fields_and_preserve_payload_note():
    payload = {
        "value": 42,
        "note": "existing note",
        "cache_hit": True,
        "cache_age_sec": 12.3,
        "stale": True,
        "stale_reason": "old",
        "source_status": "cache_stale",
    }

    stripped = trading_api._strip_microstructure_runtime_fields(payload)
    enriched = trading_api._with_microstructure_runtime_fields(
        payload,
        cache_hit=True,
        cache_age_sec=18.7654,
        stale=True,
        source_status="",
        stale_reason="background_refresh_scheduled",
    )

    assert stripped == {"value": 42, "note": "existing note"}
    assert enriched["value"] == 42
    assert enriched["cache_hit"] is True
    assert enriched["cache_age_sec"] == 18.765
    assert enriched["stale"] is True
    assert enriched["source_status"] == "cache_stale"
    assert enriched["stale_reason"] == "background_refresh_scheduled"
    assert "existing note" in enriched["note"]
    assert "Microstructure live refresh pending" in enriched["note"]


def test_cache_runtime_helpers_drop_stale_reason_when_not_stale():
    enriched = trading_api._with_community_runtime_fields(
        {"value": 1},
        cache_hit=False,
        cache_age_sec=None,
        stale=False,
        source_status="",
        stale_reason=None,
    )

    assert enriched["source_status"] == "live"
    assert enriched["cache_hit"] is False
    assert enriched["cache_age_sec"] is None
    assert enriched["stale"] is False
    assert "stale_reason" not in enriched
