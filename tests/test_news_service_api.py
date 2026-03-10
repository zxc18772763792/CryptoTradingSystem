from __future__ import annotations

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
