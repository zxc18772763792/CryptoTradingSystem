from __future__ import annotations

import asyncio

from prediction_markets.polymarket import worker


def test_run_worker_once_orchestrates(monkeypatch):
    async def fake_refresh_markets(cfg, categories=None):
        return {"markets": {"upserted": 2}, "subscriptions": {"PRICE": {"enabled": 2}}}

    async def fake_refresh_quotes(cfg, categories=None):
        return {"quotes_inserted": 4, "alerts": {"inserted": 1}, "subscriptions": 2}

    async def fake_list_states():
        return [{"source": "gamma_events", "error_count": 0}]

    async def fake_get_status():
        return {"markets_count": 2, "subscriptions_count": 2, "quotes_last_minute": 4}

    monkeypatch.setattr(worker, "refresh_markets_once", fake_refresh_markets)
    monkeypatch.setattr(worker, "refresh_quotes_once", fake_refresh_quotes)
    monkeypatch.setattr(worker, "list_source_states", fake_list_states)
    monkeypatch.setattr(worker, "get_pm_status", fake_get_status)

    result = asyncio.run(worker.run_worker_once({"defaults": {}}, refresh_markets=True, refresh_quotes=True, categories=["PRICE"]))

    assert result["markets"]["markets"]["upserted"] == 2
    assert result["quotes"]["quotes_inserted"] == 4
    assert result["status"]["markets_count"] == 2
