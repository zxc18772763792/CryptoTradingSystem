import os

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.ops.service.api import create_router


def test_ops_polymarket_status_route(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(create_router())

    async def fake_status(app):
        return {"markets_count": 1, "subscriptions_count": 2}

    from core.ops.service import api as ops_api

    monkeypatch.setattr(ops_api, "_build_polymarket_status", fake_status)

    client = TestClient(app)
    resp = client.get("/ops/polymarket/status", headers={"X-OPS-TOKEN": "test-token"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["data"]["markets_count"] == 1
