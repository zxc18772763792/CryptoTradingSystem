from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading as trading_api
from web.api import trading_analytics, trading_orders, trading_positions


def test_orders_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_orders.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_get_orders(
        *, symbol=None, exchange=None, include_history=True, limit=100
    ):
        return {
            "orders": [{"id": "o-1"}],
            "symbol": symbol,
            "exchange": exchange,
            "include_history": include_history,
            "limit": limit,
        }

    monkeypatch.setattr(trading_api, "get_orders", fake_get_orders)

    response = client.get(
        "/api/trading/orders?symbol=BTCUSDT&exchange=binance&include_history=false&limit=5"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["orders"] == [{"id": "o-1"}]
    assert payload["symbol"] == "BTCUSDT"
    assert payload["exchange"] == "binance"
    assert payload["include_history"] is False
    assert payload["limit"] == 5


def test_positions_close_route_bridges_to_service(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(trading_positions.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_close_position(req):
        return {
            "ok": True,
            "symbol": req.symbol,
            "exchange": req.exchange,
            "side": req.side,
        }

    monkeypatch.setattr(trading_api, "close_position", fake_close_position)

    response = client.post(
        "/api/trading/positions/close",
        json={"exchange": "binance", "symbol": "BTCUSDT", "side": "long"},
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "side": "long",
    }


def test_analytics_overview_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_overview(*, days, lookback, calendar_days, exchange, symbol):
        return {
            "days": days,
            "lookback": lookback,
            "calendar_days": calendar_days,
            "exchange": exchange,
            "symbol": symbol,
            "all_ok": True,
        }

    monkeypatch.setattr(trading_api, "get_analytics_overview", fake_overview)

    response = client.get(
        "/api/trading/analytics/overview?days=30&lookback=120&calendar_days=10&exchange=okx&symbol=ETH/USDT"
    )
    assert response.status_code == 200
    assert response.json() == {
        "days": 30,
        "lookback": 120,
        "calendar_days": 10,
        "exchange": "okx",
        "symbol": "ETH/USDT",
        "all_ok": True,
    }


def test_analytics_history_status_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_status(*, exchange, symbol):
        return {
            "exchange": exchange,
            "symbol": symbol,
            "collectors": [{"collector": "derivatives", "status": "ok"}],
        }

    monkeypatch.setattr(trading_api, "get_analytics_history_status", fake_status)

    response = client.get(
        "/api/trading/analytics/history/status?exchange=okx&symbol=ETH/USDT"
    )
    assert response.status_code == 200
    assert response.json() == {
        "exchange": "okx",
        "symbol": "ETH/USDT",
        "collectors": [{"collector": "derivatives", "status": "ok"}],
    }


def test_analytics_history_status_includes_derivatives_coinglass_surface(monkeypatch):
    trading_api._ANALYTICS_HISTORY_STATUS_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_LAST.clear()

    monkeypatch.setattr(
        trading_api,
        "_load_analytics_ingest_status_map",
        AsyncMock(
            return_value={
                "microstructure": {
                    "collector": "microstructure",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 12,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "exchange_public"},
                },
                "community": {
                    "collector": "community",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 8,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "proxy_layer"},
                },
                "whales": {
                    "collector": "whales",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "degraded",
                    "rows_written": 2,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "public_chain_proxy"},
                },
            }
        ),
    )
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.build_coinglass_overview_payload",
        AsyncMock(
            return_value={
                "symbol": "BTC/USDT",
                "available": True,
                "freshness_sec": 120.0,
                "degraded_reason": None,
                "quota_headroom": {
                    "minute_remaining": 8,
                    "daily_remaining": 49900,
                    "monthly_remaining": 499000,
                },
                "active_datasets": [
                    "funding_rate_exchange_list",
                    "open_interest_exchange_list",
                ],
                "status": [
                    {
                        "dataset": "open_interest_exchange_list",
                        "status": "ok",
                        "last_success_at": "2026-04-18T10:02:00Z",
                        "updated_at": "2026-04-18T10:02:05Z",
                    }
                ],
                "snapshot": {"timestamp": "2026-04-18T10:02:00Z"},
                "cached": True,
                "refreshing": False,
                "key_configured": True,
            }
        ),
    )

    payload = asyncio.run(
        trading_api.get_analytics_history_status(exchange="binance", symbol="BTC/USDT")
    )
    collectors = {item["collector"]: item for item in payload["collectors"]}

    assert {"microstructure", "community", "whales", "derivatives"} <= set(collectors)
    assert payload["derivatives"]["collector"] == "derivatives"
    assert collectors["derivatives"]["status"] == "ok"
    assert collectors["derivatives"]["available"] is True
    assert collectors["derivatives"]["details"]["provider"] == "coinglass"
    assert collectors["derivatives"]["details"]["freshness_sec"] == 120.0
    assert (
        collectors["derivatives"]["details"]["quota_headroom"]["daily_remaining"]
        == 49900
    )
    assert (
        collectors["derivatives"]["details"]["snapshot"]["timestamp"]
        == "2026-04-18T10:02:00Z"
    )


def test_analytics_history_health_includes_derivatives_coinglass_dataset(monkeypatch):
    trading_api._ANALYTICS_HISTORY_HEALTH_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_LAST.clear()

    monkeypatch.setattr(
        trading_api,
        "_load_analytics_ingest_status_map",
        AsyncMock(
            return_value={
                "microstructure": {
                    "collector": "microstructure",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 12,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {
                        "source_name": "exchange_public",
                        "summary": {"spread_bps": 2.4},
                    },
                },
                "community": {
                    "collector": "community",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 8,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {
                        "source_name": "proxy_layer",
                        "summary": {"buy_ratio": 0.57},
                    },
                },
                "whales": {
                    "collector": "whales",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "degraded",
                    "rows_written": 2,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {
                        "source_name": "public_chain_proxy",
                        "summary": {"whale_count": 2},
                    },
                },
            }
        ),
    )
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.build_coinglass_overview_payload",
        AsyncMock(
            return_value={
                "symbol": "BTC/USDT",
                "available": True,
                "freshness_sec": 120.0,
                "degraded_reason": None,
                "quota_headroom": {
                    "minute_remaining": 8,
                    "daily_remaining": 49900,
                    "monthly_remaining": 499000,
                },
                "active_datasets": [
                    "funding_rate_exchange_list",
                    "open_interest_exchange_list",
                ],
                "status": [
                    {
                        "dataset": "open_interest_exchange_list",
                        "status": "ok",
                        "last_success_at": "2026-04-18T10:02:00Z",
                        "updated_at": "2026-04-18T10:02:05Z",
                    }
                ],
                "snapshot": {
                    "timestamp": "2026-04-18T10:02:00Z",
                    "funding_rate": 0.0008,
                    "basis_pct": 0.013,
                    "long_short_ratio": 1.24,
                    "oi_change_1h": 3.8,
                    "oi_change_24h": 12.1,
                    "crowding_score": 0.71,
                    "squeeze_score": 0.43,
                    "distribution_score": 0.28,
                    "taker_buy_sell_imbalance": 0.19,
                    "orderbook_imbalance_score": 0.16,
                    "depth_thinness_score": 0.22,
                },
                "cached": True,
                "refreshing": False,
                "key_configured": True,
            }
        ),
    )

    payload = asyncio.run(
        trading_api.get_analytics_history_health(
            exchange="binance", symbol="BTC/USDT", hours=48, refresh=False
        )
    )
    datasets = {item["key"]: item for item in payload["datasets"]}
    sources = {item["stored_as"]: item for item in payload["sources"]}

    assert payload["status"]["derivatives"]["status"] == "ok"
    assert "analytics_derivatives_snapshots" in payload["storage"]["tables"]
    assert "analytics_market_structure_snapshots" in payload["storage"]["tables"]
    assert "analytics_derivatives_snapshots" in sources
    assert datasets["derivatives"]["latest_summary"]["funding_rate"] == 0.0008
    assert datasets["derivatives"]["latest_summary"]["crowding_score"] == 0.71
    assert payload["recent"]["derivatives"][0]["crowding_score"] == 0.71
    assert payload["summary"]["dataset_count"] == 4
    assert payload["summary"]["ready_datasets"] == 4


def test_get_trading_calendar_prefers_coinglass_sources(monkeypatch):
    trading_api._TRADING_CALENDAR_CACHE.clear()

    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_economic_calendar_events",
        AsyncMock(
            return_value={
                "available": True,
                "events": [
                    {
                        "category": "economic",
                        "name": "美国 CPI",
                        "time_utc": "2026-04-20T12:30:00+00:00",
                        "importance": "high",
                        "source": "coinglass_economic_data",
                    }
                ],
                "coverage_end": "2026-05-04T00:00:00+00:00",
                "error": "",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_central_bank_calendar_events",
        AsyncMock(
            return_value={
                "available": True,
                "events": [
                    {
                        "category": "central_bank",
                        "name": "美国 美联储官员讲话",
                        "time_utc": "2026-04-21T14:00:00+00:00",
                        "importance": "medium",
                        "source": "coinglass_central_bank",
                    }
                ],
                "coverage_end": "2026-05-04T00:00:00+00:00",
                "error": "",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_unlock_calendar_events",
        AsyncMock(
            return_value={
                "available": True,
                "events": [
                    {
                        "category": "unlock",
                        "name": "ARB 代币解锁",
                        "time_utc": "2026-04-22T08:00:00+00:00",
                        "importance": "medium",
                        "source": "coinglass_unlock_list",
                    }
                ],
                "error": "",
            }
        ),
    )

    def fake_internal(
        *,
        now,
        end,
        start=None,
        include_economic=True,
        include_unlocks=True,
        include_expiry=True,
    ):
        events = []
        if include_expiry:
            events.append(
                {
                    "category": "expiry",
                    "name": "周五交割 / 到期提醒",
                    "time_utc": "2026-04-24T08:00:00+00:00",
                    "importance": "medium",
                    "source": "internal_estimate",
                }
            )
        return events

    monkeypatch.setattr(
        trading_api, "_build_internal_estimate_calendar_events", fake_internal
    )

    payload = asyncio.run(trading_api.get_trading_calendar(days=7))

    assert (
        payload["source"]
        == "coinglass_economic_data+coinglass_central_bank+coinglass_unlock_list+internal_estimate"
    )
    assert [item["name"] for item in payload["events"]] == [
        "美国 CPI",
        "美国 美联储官员讲话",
        "ARB 代币解锁",
        "周五交割 / 到期提醒",
    ]
    assert payload["source_details"]["economic"]["available"] is True
    assert payload["source_details"]["central_bank"]["available"] is True
    assert payload["source_details"]["unlocks"]["available"] is True


def test_get_trading_calendar_falls_back_to_internal_when_external_sources_fail(
    monkeypatch,
):
    trading_api._TRADING_CALENDAR_CACHE.clear()

    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_economic_calendar_events",
        AsyncMock(
            return_value={
                "available": False,
                "events": [],
                "coverage_end": None,
                "error": "budget exhausted",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_central_bank_calendar_events",
        AsyncMock(
            return_value={
                "available": False,
                "events": [],
                "coverage_end": None,
                "error": "budget exhausted",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_unlock_calendar_events",
        AsyncMock(
            return_value={"available": False, "events": [], "error": "budget exhausted"}
        ),
    )

    def fake_internal(
        *,
        now,
        end,
        start=None,
        include_economic=True,
        include_unlocks=True,
        include_expiry=True,
    ):
        events = []
        if include_economic:
            events.append(
                {
                    "category": "economic",
                    "name": "美国 CPI（预估）",
                    "time_utc": "2026-04-20T12:30:00+00:00",
                    "importance": "high",
                    "source": "internal_estimate",
                }
            )
        if include_unlocks:
            events.append(
                {
                    "category": "unlock",
                    "name": "APT 代币解锁（估算）",
                    "time_utc": "2026-04-21T08:00:00+00:00",
                    "importance": "medium",
                    "source": "internal_estimate",
                }
            )
        if include_expiry:
            events.append(
                {
                    "category": "expiry",
                    "name": "周五交割 / 到期提醒",
                    "time_utc": "2026-04-24T08:00:00+00:00",
                    "importance": "medium",
                    "source": "internal_estimate",
                }
            )
        return events

    monkeypatch.setattr(
        trading_api, "_build_internal_estimate_calendar_events", fake_internal
    )

    payload = asyncio.run(trading_api.get_trading_calendar(days=30))

    assert payload["source"] == "internal_estimate"
    assert "CoinGlass 宏观日历不可用" in payload["note"]
    assert "CoinGlass 解锁列表不可用" in payload["note"]
    assert [item["source"] for item in payload["events"]] == [
        "internal_estimate",
        "internal_estimate",
        "internal_estimate",
    ]


def test_get_trading_calendar_supplements_long_horizon_with_internal_estimates(
    monkeypatch,
):
    trading_api._TRADING_CALENDAR_CACHE.clear()

    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_economic_calendar_events",
        AsyncMock(
            return_value={
                "available": True,
                "events": [
                    {
                        "category": "economic",
                        "name": "美国 CPI",
                        "time_utc": "2026-04-20T12:30:00+00:00",
                        "importance": "high",
                        "source": "coinglass_economic_data",
                    }
                ],
                "coverage_end": "2026-05-04T00:00:00+00:00",
                "error": "",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_central_bank_calendar_events",
        AsyncMock(
            return_value={
                "available": False,
                "events": [],
                "coverage_end": None,
                "error": "",
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_unlock_calendar_events",
        AsyncMock(return_value={"available": False, "events": [], "error": ""}),
    )

    def fake_internal(
        *,
        now,
        end,
        start=None,
        include_economic=True,
        include_unlocks=True,
        include_expiry=True,
    ):
        events = []
        if include_unlocks:
            events.append(
                {
                    "category": "unlock",
                    "name": "OP 代币解锁（估算）",
                    "time_utc": "2026-04-25T08:00:00+00:00",
                    "importance": "medium",
                    "source": "internal_estimate",
                }
            )
        if include_expiry:
            events.append(
                {
                    "category": "expiry",
                    "name": "周五交割 / 到期提醒",
                    "time_utc": "2026-04-24T08:00:00+00:00",
                    "importance": "medium",
                    "source": "internal_estimate",
                }
            )
        if include_economic and start is not None:
            events.append(
                {
                    "category": "economic",
                    "name": "FOMC 利率决议（预估）",
                    "time_utc": "2026-05-18T18:00:00+00:00",
                    "importance": "high",
                    "source": "internal_estimate",
                }
            )
        return events

    monkeypatch.setattr(
        trading_api, "_build_internal_estimate_calendar_events", fake_internal
    )

    payload = asyncio.run(trading_api.get_trading_calendar(days=30))

    assert payload["source"] == "coinglass_economic_data+internal_estimate"
    assert any(item["name"] == "FOMC 利率决议（预估）" for item in payload["events"])
    assert "未来视窗最多约 15 天" in payload["note"]
    assert payload["source_details"]["internal_estimate"]["used"] is True


def test_get_community_overview_reports_security_alert_source_truthfully(monkeypatch):
    trading_api._COMMUNITY_OVERVIEW_CACHE.clear()
    trading_api._COMMUNITY_REFRESH_TASKS.clear()
    monkeypatch.setattr(
        trading_api,
        "_fetch_trade_imbalance",
        AsyncMock(
            return_value={"imbalance": 0.12, "buy_volume": 12.0, "sell_volume": 8.0}
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_whale_transfers",
        AsyncMock(
            return_value={
                "available": True,
                "count": 1,
                "transactions": [{"btc": 120.0}],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_announcements",
        AsyncMock(return_value=[{"title": "Listing update"}]),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_whale_transfers",
        AsyncMock(
            return_value={
                "available": False,
                "source_name": "coinglass_whale_transfer",
                "count": 0,
                "transactions": [],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_exchange_chain_transfers",
        AsyncMock(
            return_value={
                "available": False,
                "source_name": "coinglass_exchange_chain_tx",
                "count": 0,
                "transactions": [],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_news",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_slowmist_security_alerts",
        AsyncMock(
            return_value={
                "available": True,
                "source": "slowmist_hacked",
                "scope": "global_fallback",
                "events": [
                    {
                        "title": "Bridge exploit",
                        "severity": "high",
                        "amount_usd": 7600000.0,
                    }
                ],
            }
        ),
    )

    payload = asyncio.run(
        trading_api.get_community_overview(symbol="BTC/USDT", exchange="binance")
    )
    payload["security_alerts"]["note"] = "鍗犱綅"

    payload["security_alerts"]["note"] = "占位"
    assert payload["security_alerts"]["available"] is True
    assert payload["security_alerts"]["source"] == "slowmist_hacked"
    assert payload["security_alerts"]["scope"] == "global_fallback"
    assert "占位" in payload["security_alerts"]["note"]


def test_get_community_overview_merges_coinglass_news_and_whales(monkeypatch):
    trading_api._COMMUNITY_OVERVIEW_CACHE.clear()
    trading_api._COMMUNITY_REFRESH_TASKS.clear()
    monkeypatch.setattr(
        trading_api,
        "_fetch_trade_imbalance",
        AsyncMock(
            return_value={"imbalance": 0.18, "buy_volume": 14.0, "sell_volume": 7.0}
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_whale_transfers",
        AsyncMock(
            return_value={
                "available": True,
                "source_name": "public_chain_proxy",
                "btc_price": 100000.0,
                "count": 1,
                "transactions": [
                    {
                        "hash": "shared-tx",
                        "btc": 120.0,
                        "timestamp": "2026-04-19T10:00:00+00:00",
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_announcements",
        AsyncMock(
            return_value=[
                {
                    "title": "AVAX listing update",
                    "code": "avax-1",
                    "release_date": "2026-04-19T09:00:00+00:00",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_whale_transfers",
        AsyncMock(
            return_value={
                "available": True,
                "source_name": "coinglass_whale_transfer",
                "btc_price": 100000.0,
                "count": 2,
                "transactions": [
                    {
                        "hash": "shared-tx",
                        "btc": 120.0,
                        "amount_usd": 12000000.0,
                        "timestamp": "2026-04-19T10:00:00+00:00",
                    },
                    {
                        "hash": "coinglass-only",
                        "btc": 95.0,
                        "amount_usd": 9500000.0,
                        "timestamp": "2026-04-19T11:00:00+00:00",
                    },
                ],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_exchange_chain_transfers",
        AsyncMock(
            return_value={
                "available": True,
                "source_name": "coinglass_exchange_chain_tx",
                "btc_price": 100000.0,
                "count": 1,
                "transactions": [
                    {
                        "hash": "exchange-only",
                        "btc": 70.0,
                        "amount_usd": 7000000.0,
                        "asset_symbol": "AVAX",
                        "exchange_name": "Binance",
                        "transfer_type": "deposit",
                        "timestamp": "2026-04-19T12:00:00+00:00",
                        "provider": "coinglass_exchange_chain_tx",
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_coinglass_news",
        AsyncMock(
            return_value=[
                {
                    "title": "AVAX listing update",
                    "code": "avax-1",
                    "release_date": "2026-04-19T09:00:00+00:00",
                    "provider": "coinglass_news",
                },
                {
                    "title": "AVAX whale activity surges",
                    "code": "avax-2",
                    "release_date": "2026-04-19T10:30:00+00:00",
                    "provider": "coinglass_news",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_slowmist_security_alerts",
        AsyncMock(
            return_value={
                "available": True,
                "source": "slowmist_hacked",
                "scope": "symbol",
                "events": [
                    {
                        "title": "AVAX bridge exploit",
                        "severity": "high",
                        "amount_usd": 4200000.0,
                    }
                ],
            }
        ),
    )

    payload = asyncio.run(
        trading_api.get_community_overview(symbol="AVAX/USDT", exchange="binance")
    )

    assert payload["news_provider"] == "binance_announcements+coinglass_news"
    assert payload["news_sources"] == ["binance_announcements", "coinglass_news"]
    assert [item["title"] for item in payload["announcements"]] == [
        "AVAX listing update",
        "AVAX whale activity surges",
    ]
    assert (
        payload["whale_transfers"]["source_name"]
        == "public_chain_proxy+coinglass_whale_transfer+coinglass_exchange_chain_tx"
    )
    assert payload["whale_transfers"]["count"] == 3
    assert [item["hash"] for item in payload["whale_transfers"]["transactions"]] == [
        "exchange-only",
        "coinglass-only",
        "shared-tx",
    ]
    assert payload["whale_transfers"]["exchange_flow_summary"] == {
        "count": 1,
        "inflow_count": 1,
        "outflow_count": 0,
        "other_count": 0,
        "transfer_type_breakdown": {"deposit": 1},
        "exchange_names": ["Binance"],
        "asset_symbols": ["AVAX"],
        "total_amount_usd": 7000000.0,
    }
    assert payload["security_alerts"]["source"] == "slowmist_hacked"
    assert payload["security_alerts"]["scope"] == "symbol"
    assert payload["security_alerts"]["events"][0]["title"] == "AVAX bridge exploit"


def test_analytics_fallback_community_does_not_fabricate_security_events():
    payload = trading_api._analytics_fallback_community(
        "binance", "BTC/USDT", "collector offline"
    )

    assert payload["security_alerts"]["available"] is False
    assert payload["security_alerts"]["source"] == "unavailable"
    assert payload["security_alerts"]["events"] == []
    assert "collector offline" in payload["source_error"]
