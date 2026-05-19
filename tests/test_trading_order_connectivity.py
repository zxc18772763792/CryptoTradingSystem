from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

from web.api import trading as trading_api


def test_live_open_orders_reports_fast_path_failure_without_connector_hang(monkeypatch):
    trading_api._LIVE_ORDER_DETAILS_CACHE["ts"] = 0.0
    trading_api._LIVE_ORDER_DETAILS_CACHE["orders"] = []
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_open_orders_fast",
        AsyncMock(side_effect=RuntimeError("network unavailable")),
    )

    payload = asyncio.run(
        trading_api.get_orders(include_history=False, exchange="binance", limit=20)
    )

    assert payload["orders"] == []
    assert payload["count"] == 0
    assert payload["source"] == "binance_futures_rest"
    assert payload["cache_fallback"]["used"] is False
    assert "network unavailable" in payload["cache_fallback"]["reason"]


def test_live_open_orders_uses_recent_cache_when_fast_path_fails(monkeypatch):
    trading_api._LIVE_ORDER_DETAILS_CACHE["ts"] = time.time()
    trading_api._LIVE_ORDER_DETAILS_CACHE["orders"] = [{"id": "cached-open"}]
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_open_orders_fast",
        AsyncMock(side_effect=TimeoutError()),
    )

    payload = asyncio.run(
        trading_api.get_orders(include_history=False, exchange="binance", limit=20)
    )

    assert payload["orders"] == [{"id": "cached-open"}]
    assert payload["source"] == "cache"
    assert payload["cache_fallback"]["used"] is True
    assert payload["cache_fallback"]["reason"] == "TimeoutError"


def test_live_open_orders_cache_ttl_tolerates_brief_exchange_outage():
    assert trading_api._LIVE_ORDER_DETAILS_CACHE_TTL_SEC >= 60.0


def test_conditional_orders_include_exchange_trigger_orders(monkeypatch):
    trading_api._LIVE_CONDITIONAL_ORDER_CACHE["ts"] = 0.0
    trading_api._LIVE_CONDITIONAL_ORDER_CACHE["orders"] = []
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(trading_api.execution_engine, "list_conditional_orders", lambda: [])
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_open_orders_fast",
        AsyncMock(
            return_value=[
                {
                    "id": "limit-1",
                    "exchange": "binance",
                    "symbol": "XRP/USDT:USDT",
                    "side": "sell",
                    "type": "limit",
                    "price": 1.4,
                    "amount": 10.0,
                    "status": "open",
                },
                {
                    "id": "stop-1",
                    "exchange": "binance",
                    "symbol": "XRP/USDT:USDT",
                    "side": "sell",
                    "type": "stop_market",
                    "price": 0.0,
                    "amount": 10.0,
                    "status": "open",
                    "trigger_price": 1.34,
                    "stop_loss": 1.34,
                    "reduce_only": True,
                },
                {
                    "id": "tp-1",
                    "exchange": "binance",
                    "symbol": "XRP/USDT:USDT",
                    "side": "sell",
                    "type": "take_profit_market",
                    "price": 0.0,
                    "amount": 10.0,
                    "status": "open",
                    "trigger_price": 1.4,
                    "take_profit": 1.4,
                    "reduce_only": True,
                },
            ]
        ),
    )

    payload = asyncio.run(trading_api.get_conditional_orders())

    assert payload["source"] == "local_plus_binance_futures_rest"
    assert payload["local_count"] == 0
    assert payload["exchange_count"] == 2
    assert {row["conditional_id"] for row in payload["orders"]} == {"stop-1", "tp-1"}
    assert {row["source"] for row in payload["orders"]} == {"exchange"}


def test_conditional_orders_fall_back_to_local_when_exchange_unavailable(monkeypatch):
    local = [
        {
            "conditional_id": "local-cond",
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "side": "buy",
            "trigger_price": 100.0,
            "amount": 1.0,
        }
    ]
    trading_api._LIVE_CONDITIONAL_ORDER_CACHE["ts"] = 0.0
    trading_api._LIVE_CONDITIONAL_ORDER_CACHE["orders"] = []
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(trading_api.execution_engine, "list_conditional_orders", lambda: local)
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_open_orders_fast",
        AsyncMock(side_effect=RuntimeError("exchange down")),
    )

    payload = asyncio.run(trading_api.get_conditional_orders())

    assert payload["orders"] == local
    assert payload["source"] == "local_execution_engine"
    assert payload["local_count"] == 1
    assert payload["exchange_count"] == 0
    assert payload["cache_fallback"]["reason"] == "exchange down"
