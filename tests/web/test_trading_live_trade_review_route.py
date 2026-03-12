from __future__ import annotations

import asyncio


def test_live_trade_review_route_passes_args(monkeypatch):
    from web.api import trading_analytics as analytics_api

    async def fake_review(*, hours: int, limit: int, strategy: str | None):
        return {
            "hours": hours,
            "limit": limit,
            "strategy": strategy,
            "count": 0,
            "items": [],
        }

    monkeypatch.setattr(analytics_api.trading_api, "get_live_trade_review", fake_review)
    payload = asyncio.run(
        analytics_api.get_live_trade_review(
            hours=24,
            limit=88,
            strategy="alpha",
        )
    )
    assert payload["hours"] == 24
    assert payload["limit"] == 88
    assert payload["strategy"] == "alpha"
