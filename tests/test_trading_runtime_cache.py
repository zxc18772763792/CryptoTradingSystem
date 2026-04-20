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
