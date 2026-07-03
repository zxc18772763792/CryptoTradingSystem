from __future__ import annotations

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from core.news.service import api as news_api


def test_news_service_health_uses_lifespan_state(monkeypatch):
    async def fake_init():
        return None

    async def fake_close():
        return None

    monkeypatch.setattr(news_api.news_db, "init_news_db", fake_init)
    monkeypatch.setattr(news_api.news_db, "close_news_db", fake_close)

    app = news_api.create_app()
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "news_signal"


def test_news_service_non_health_routes_require_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")

    app = news_api.create_app()
    missing = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path == "/health":
            continue
        if not route.dependencies:
            missing.append(f"{sorted(route.methods or set())} {route.path}")

    assert missing == []


def test_news_service_worker_status_requires_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")

    async def fake_init():
        return None

    async def fake_close():
        return None

    async def fake_list_source_states():
        return []

    async def fake_get_llm_queue_stats():
        return {"pending": 0}

    monkeypatch.setattr(news_api.news_db, "init_news_db", fake_init)
    monkeypatch.setattr(news_api.news_db, "close_news_db", fake_close)
    monkeypatch.setattr(news_api.news_db, "list_source_states", fake_list_source_states)
    monkeypatch.setattr(news_api.news_db, "get_llm_queue_stats", fake_get_llm_queue_stats)

    app = news_api.create_app()
    with TestClient(app) as client:
        response = client.get("/worker/status")
        assert response.status_code == 401

        response = client.get(
            "/worker/status",
            headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
        )
        assert response.status_code == 200
        assert response.json()["llm_queue"] == {"pending": 0}
