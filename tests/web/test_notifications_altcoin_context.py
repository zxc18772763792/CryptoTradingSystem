from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import notifications as notifications_api


def test_notifications_evaluate_includes_prefetched_altcoin_context(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(notifications_api.router, prefix="/api/notifications")
    client = TestClient(app)

    async def fake_load_prices(exchange, symbols):
        return {"BTC/USDT": 65000.0}

    async def fake_list_rules():
        return [
            {
                "id": "r1",
                "enabled": True,
                "rule_type": "altcoin_score_above",
                "params": {
                    "config_key": "cfg-1",
                    "exchange": "binance",
                    "timeframe": "4h",
                    "symbol": "AAA/USDT",
                    "universe_symbols": ["AAA/USDT", "BBB/USDT"],
                },
            }
        ]

    async def fake_build_altcoin_notification_context(rules):
        return {"scans": {"cfg-1": {"rows": [{"symbol": "AAA/USDT"}], "sort_indexes": {"layout": {}}}}}

    monkeypatch.setattr(notifications_api, "_load_prices", fake_load_prices)
    monkeypatch.setattr(notifications_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(notifications_api, "build_altcoin_notification_context", fake_build_altcoin_notification_context)

    captured = {}

    async def fake_evaluate_rules(context):
        captured["context"] = context
        return {"triggered_count": 0, "triggered": []}

    monkeypatch.setattr(notifications_api.notification_manager, "evaluate_rules", fake_evaluate_rules)

    response = client.post(
        "/api/notifications/evaluate",
        json={"symbols": ["BTC/USDT"], "exchange": "binance", "total_usd": 1000},
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "altcoin" in payload["context"]
    assert payload["context"]["altcoin"]["scans"]["cfg-1"]["rows"][0]["symbol"] == "AAA/USDT"
    assert captured["context"]["altcoin"]["scans"]["cfg-1"]["rows"][0]["symbol"] == "AAA/USDT"
