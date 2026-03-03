from __future__ import annotations

from core.ops.service import api as ops_api


def test_ops_status_returns_partial_errors(client, ops_headers, monkeypatch):
    def boom_sync():
        raise RuntimeError("sync boom")

    async def boom_async(*args, **kwargs):
        raise RuntimeError("async boom")

    monkeypatch.setattr(ops_api, "_build_status_execution", boom_sync)
    monkeypatch.setattr(ops_api, "_build_risk_status", boom_sync)
    monkeypatch.setattr(ops_api, "_build_exchange_status", boom_async)
    monkeypatch.setattr(ops_api, "_build_news_status", boom_async)

    response = client.get("/ops/status", headers=ops_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert "error" in payload["data"]["execution_engine"]
    assert "error" in payload["data"]["risk_manager"]
    assert "error" in payload["data"]["exchange_manager"]
    assert "error" in payload["data"]["news"]
