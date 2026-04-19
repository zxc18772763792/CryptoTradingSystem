from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock


def _recent_snapshot_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _history_micro_snapshot() -> dict:
    return {
        "timestamp": _recent_snapshot_ts(),
        "orderbook": {"mid_price": 100000.0, "spread_bps": 2.5},
        "aggressor_flow": {"count": 8, "imbalance": 0.18},
        "large_orders": [
            {"side": "bid", "notional": 5000000.0},
            {"side": "ask", "notional": 3000000.0},
        ],
        "iceberg_detection": {"candidate_count": 2},
        "long_short_ratio": {"available": True, "long_short_ratio": 1.22},
        "funding_rate": {"available": True, "funding_rate": 0.0001},
        "spot_futures_basis": {"available": True, "basis_pct": 0.12},
    }


def _history_community_snapshot() -> dict:
    return {
        "timestamp": _recent_snapshot_ts(),
        "announcements": [{"title": "Listing update"}],
        "whale_transfers": {"count": 2},
        "flow_proxy": {"count": 6, "imbalance": 0.11},
        "security_alerts": {"events": []},
    }


def _news_summary(events_count: int = 2) -> dict:
    return {
        "events_count": events_count,
        "feed_count": 0,
        "raw_count": 0,
        "scope": "symbol",
        "sentiment": {"positive": events_count, "neutral": 0, "negative": 0},
    }


def test_market_state_prefers_recent_history_snapshots(monkeypatch):
    from web.api import research as module

    monkeypatch.setattr(module, "_load_preferred_coinglass_overview", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_analytics_history_status", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "CPI", "time_utc": _recent_snapshot_ts(), "importance": "high"}]}),
    )
    monkeypatch.setattr(module, "_build_news_summary", AsyncMock(return_value=_news_summary(3)))
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=_history_micro_snapshot()))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 2, "transactions": []}))

    live_micro_mock = AsyncMock(side_effect=AssertionError("live microstructure should be skipped when history is fresh"))
    live_community_mock = AsyncMock(side_effect=AssertionError("live community should be skipped when history is fresh"))
    monkeypatch.setattr(module, "get_market_microstructure", live_micro_mock)
    monkeypatch.setattr(module, "get_community_overview", live_community_mock)

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert result["payload"]["analytics_overview"]["modules"]["risk_dashboard"]["ok"] is True
    assert result["payload"]["sentiment_dashboard"]["microstructure"]["orderbook"]["spread_bps"] == 2.5
    assert result["payload"]["microstructure_summary"]["long_short_ratio"] == 1.22
    assert result["payload"]["microstructure_summary"]["iceberg_candidates"] == 2
    assert result["payload"]["calendar_watchlist"][0]["title"] == "CPI"
    assert live_micro_mock.await_count == 0
    assert live_community_mock.await_count == 0


def test_market_state_does_not_retry_empty_news_summary(monkeypatch):
    from web.api import research as module

    monkeypatch.setattr(module, "_load_preferred_coinglass_overview", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_analytics_history_status", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "Unlock", "time_utc": _recent_snapshot_ts(), "importance": "medium"}]}),
    )
    news_mock = AsyncMock(return_value=_news_summary(0))
    monkeypatch.setattr(module, "_build_news_summary", news_mock)
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=_history_micro_snapshot()))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 1, "transactions": []}))
    monkeypatch.setattr(module, "get_market_microstructure", AsyncMock(side_effect=AssertionError("live microstructure should be skipped")))
    monkeypatch.setattr(module, "get_community_overview", AsyncMock(side_effect=AssertionError("live community should be skipped")))

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "degraded"
    assert news_mock.await_count == 1
    assert any("News summary returned no usable samples" in warning for warning in result["warnings"])


def test_market_state_keeps_ok_when_spread_zero_but_depth_exists(monkeypatch):
    from web.api import research as module

    monkeypatch.setattr(module, "_load_preferred_coinglass_overview", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_analytics_history_status", AsyncMock(return_value={}))
    micro_snapshot = _history_micro_snapshot()
    micro_snapshot["orderbook"] = {
        "mid_price": 100000.0,
        "spread_bps": 0.0,
        "bid_depth": [{"price": 99990.0, "qty": 12.0}],
        "ask_depth": [{"price": 100010.0, "qty": 11.0}],
    }

    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "CPI", "time_utc": _recent_snapshot_ts(), "importance": "high"}]}),
    )
    monkeypatch.setattr(module, "_build_news_summary", AsyncMock(return_value=_news_summary(3)))
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=micro_snapshot))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 2, "transactions": []}))
    monkeypatch.setattr(module, "get_market_microstructure", AsyncMock(side_effect=AssertionError("live microstructure should be skipped")))
    monkeypatch.setattr(module, "get_community_overview", AsyncMock(side_effect=AssertionError("live community should be skipped")))

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert result["payload"]["sentiment_dashboard"]["microstructure"]["orderbook"]["mid_price"] == 100000.0


def test_market_state_prefers_coinglass_derivatives_overlay(monkeypatch):
    from web.api import research as module

    micro_snapshot = _history_micro_snapshot()
    micro_snapshot["long_short_ratio"] = {"available": True, "long_short_ratio": 0.91}
    micro_snapshot["funding_rate"] = {"available": False}
    micro_snapshot["spot_futures_basis"] = {"available": False}
    micro_snapshot["aggressor_flow"] = {"count": 0, "imbalance": 0.0}

    monkeypatch.setattr(
        module,
        "_load_preferred_coinglass_overview",
        AsyncMock(
            return_value={
                "available": True,
                "key_configured": True,
                "freshness_sec": 42.0,
                "degraded_reason": None,
                "active_datasets": [
                    "funding_rate_exchange_list",
                    "global_long_short_account_ratio_history",
                ],
                "snapshot": {
                    "timestamp": _recent_snapshot_ts(),
                    "funding_rate": 0.00045,
                    "basis_pct": 0.18,
                    "long_short_ratio": 1.37,
                    "taker_buy_sell_imbalance": 0.22,
                    "payload": {"history_ready": True},
                },
            }
        ),
    )
    monkeypatch.setattr(module, "get_analytics_history_status", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "CPI", "time_utc": _recent_snapshot_ts(), "importance": "high"}]}),
    )
    monkeypatch.setattr(module, "_build_news_summary", AsyncMock(return_value=_news_summary(2)))
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=micro_snapshot))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 2, "transactions": []}))
    monkeypatch.setattr(module, "get_market_microstructure", AsyncMock(side_effect=AssertionError("live microstructure should be skipped")))
    monkeypatch.setattr(module, "get_community_overview", AsyncMock(side_effect=AssertionError("live community should be skipped")))

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert result["summary"]["derivatives_status"] == "ok"
    assert result["payload"]["derivatives_summary"]["available"] is True
    assert result["payload"]["microstructure_summary"]["long_short_ratio"] == 1.37
    assert (
        result["payload"]["sentiment_dashboard"]["microstructure"]["funding_rate"]["source"]
        == "coinglass_cache"
    )
    assert (
        result["payload"]["sentiment_dashboard"]["microstructure"]["spot_futures_basis"]["basis_pct"]
        == 0.18
    )


def test_market_state_falls_back_to_history_derivatives_shadow_when_overview_is_missing(monkeypatch):
    from web.api import research as module

    micro_snapshot = _history_micro_snapshot()
    micro_snapshot["long_short_ratio"] = {"available": False}
    micro_snapshot["funding_rate"] = {"available": False}
    micro_snapshot["spot_futures_basis"] = {"available": False}
    micro_snapshot["aggressor_flow"] = {"count": 0, "imbalance": 0.0}

    monkeypatch.setattr(
        module,
        "_load_preferred_coinglass_overview",
        AsyncMock(
            return_value={
                "available": False,
                "key_configured": True,
                "freshness_sec": None,
                "degraded_reason": "coinglass_cache_empty",
                "active_datasets": [],
                "snapshot": {},
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "get_analytics_history_status",
        AsyncMock(
            return_value={
                "collectors": [
                    {
                        "collector": "derivatives",
                        "status": "ok",
                        "available": True,
                        "details": {
                            "provider": "coinglass",
                            "freshness_sec": 120.0,
                            "active_datasets": [
                                "funding_rate_exchange_list",
                                "open_interest_exchange_list",
                            ],
                            "snapshot": {
                                "timestamp": _recent_snapshot_ts(),
                                "funding_rate": 0.00042,
                                "long_short_ratio": 1.31,
                                "basis_pct": 0.11,
                                "taker_buy_sell_imbalance": 0.19,
                                "payload": {
                                    "history_ready": True,
                                    "history_exchange": "Binance",
                                    "history_interval": "h1",
                                    "funding_mean": 0.0007,
                                    "funding_zscore": 1.4,
                                },
                            },
                        },
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "CPI", "time_utc": _recent_snapshot_ts(), "importance": "high"}]}),
    )
    monkeypatch.setattr(module, "_build_news_summary", AsyncMock(return_value=_news_summary(2)))
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=micro_snapshot))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 2, "transactions": []}))
    monkeypatch.setattr(module, "get_market_microstructure", AsyncMock(side_effect=AssertionError("live microstructure should be skipped")))
    monkeypatch.setattr(module, "get_community_overview", AsyncMock(side_effect=AssertionError("live community should be skipped")))

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert result["summary"]["derivatives_status"] == "ok"
    assert result["payload"]["derivatives_summary"]["available"] is True
    assert result["payload"]["microstructure_summary"]["long_short_ratio"] == 1.31
    assert (
        result["payload"]["sentiment_dashboard"]["microstructure"]["funding_rate"]["funding_rate"]
        == 0.00042
    )
    assert (
        result["payload"]["sentiment_dashboard"]["microstructure"]["spot_futures_basis"]["basis_pct"]
        == 0.11
    )
