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
    assert result["payload"]["calendar_watchlist"][0]["title"] == "CPI"
    assert live_micro_mock.await_count == 0
    assert live_community_mock.await_count == 0


def test_market_state_does_not_retry_empty_news_summary(monkeypatch):
    from web.api import research as module

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
