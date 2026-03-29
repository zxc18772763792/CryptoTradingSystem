from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import news as news_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(news_api.router, prefix="/api/news")
    app.state.news_cfg = news_api.load_news_cfg()
    return app


def test_raw_coverage_exposes_archive_contract(monkeypatch):
    async def fake_coverage():
        return {
            "total_count": 321,
            "history_span_days": 12.5,
            "earliest_published_at": "2026-03-01T00:00:00+00:00",
            "latest_published_at": "2026-03-30T00:00:00+00:00",
            "earliest_fetched_at": "2026-03-01T00:05:00+00:00",
            "latest_fetched_at": "2026-03-30T00:05:00+00:00",
            "count_24h": 12,
            "count_7d": 88,
            "count_30d": 321,
            "active_sources_7d": 6,
            "top_sources": [],
            "recent_daily_counts": [],
            "sampled_provider_counts": {"rss": 120},
            "recent_ingest_mode_counts": {"incremental": 90, "history_backfill": 10},
            "sample_size": 100,
        }

    monkeypatch.setattr(news_api.news_db, "summarize_news_raw_coverage", fake_coverage)

    with TestClient(_build_app()) as client:
        response = client.get("/api/news/raw/coverage")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_count"] == 321
    assert payload["archive_contract"]["stores_all_pulled_raw_news"] is True
    assert payload["archive_contract"]["guarantees_full_upstream_history"] is False


def test_raw_history_rejects_invalid_since():
    with TestClient(_build_app()) as client:
        response = client.get("/api/news/raw/history?since=not-a-date")

    assert response.status_code == 400
    assert "invalid since value" in response.json()["detail"]


def test_ingest_backfill_history_updates_last_pull(monkeypatch):
    async def fake_backfill(_cfg, payload):
        return {
            "mode": "history_backfill",
            "lookback_hours": int(payload.hours),
            "raw_inserted_count": 17,
            "coverage": {"total_count": 88},
            "errors": [],
        }

    monkeypatch.setattr(news_api, "backfill_and_store_news_history", fake_backfill)

    app = _build_app()
    with TestClient(app) as client:
        response = client.post("/api/news/ingest/backfill_history", json={"hours": 72, "max_records": 180})

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "history_backfill"
    assert payload["raw_inserted_count"] == 17
    assert payload["source"] == "manual_history_backfill"
    assert app.state.news_last_pull["coverage"]["total_count"] == 88
