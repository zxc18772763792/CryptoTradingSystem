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
    assert "原始新闻" in payload["archive_contract"]["note"]


def test_raw_history_rejects_invalid_since():
    with TestClient(_build_app()) as client:
        response = client.get("/api/news/raw/history?since=not-a-date")

    assert response.status_code == 400
    assert "invalid since value" in response.json()["detail"]


def test_summary_uses_exact_window_counts(monkeypatch):
    async def fake_list_events(symbol=None, since=None, limit=0):
        del symbol, since, limit
        return [
            {"id": 1, "event_id": "evt-1", "symbol": "BTCUSDT", "event_type": "etf", "sentiment": 1, "ts": "2026-04-04T01:00:00+00:00"},
            {"id": 2, "event_id": "evt-2", "symbol": "ETHUSDT", "event_type": "macro", "sentiment": -1, "ts": "2026-04-04T02:00:00+00:00"},
        ]

    async def fake_list_news_raw(since=None, limit=0):
        del since, limit
        return [
            {"id": 11, "source": "jin10", "title": "row-1", "published_at": "2026-04-04T02:00:00+00:00", "payload": {}},
            {"id": 12, "source": "rss", "title": "row-2", "published_at": "2026-04-04T01:30:00+00:00", "payload": {}},
        ]

    async def fake_source_states():
        return []

    async def fake_llm_queue():
        return {}

    async def fake_build_latest_feed(cfg=None, symbol=None, hours=24, limit=60, summarize=False):
        del cfg, symbol, hours, limit, summarize
        return {
            "count": 5,
            "feed_stats": {"total": 5, "structured": 2, "unstructured": 3, "unstructured_breakdown": {}, "sentiment": {"positive": 2, "neutral": 2, "negative": 1}},
            "source_stats": {"by_provider": {"rss": 3}, "by_source": {"jin10": 2, "rss": 3}},
            "items": [],
        }

    async def fake_count_events(symbol=None, since=None):
        del symbol, since
        return 9

    async def fake_latest_event(symbol=None, since=None):
        del symbol, since
        return "2026-04-04T03:00:00+00:00"

    async def fake_count_raw(since=None):
        del since
        return 3456

    async def fake_latest_raw(since=None):
        del since
        return "2026-04-04T04:00:00+00:00"

    monkeypatch.setattr(news_api.news_db, "list_events", fake_list_events)
    monkeypatch.setattr(news_api.news_db, "list_news_raw", fake_list_news_raw)
    monkeypatch.setattr(news_api.news_db, "list_source_states", fake_source_states)
    monkeypatch.setattr(news_api.news_db, "get_llm_queue_stats", fake_llm_queue)
    monkeypatch.setattr(news_api, "build_latest_feed", fake_build_latest_feed)
    monkeypatch.setattr(news_api.news_db, "count_events", fake_count_events)
    monkeypatch.setattr(news_api.news_db, "latest_event_timestamp", fake_latest_event)
    monkeypatch.setattr(news_api.news_db, "count_news_raw", fake_count_raw)
    monkeypatch.setattr(news_api.news_db, "latest_news_raw_timestamp", fake_latest_raw)

    with TestClient(_build_app()) as client:
        response = client.get("/api/news/summary?hours=24&feed_limit=80")

    assert response.status_code == 200
    payload = response.json()
    assert payload["raw_count"] == 3456
    assert payload["events_count"] == 9
    assert payload["latest_raw_at"] == "2026-04-04T04:00:00+00:00"
    assert payload["latest_event_at"] == "2026-04-04T03:00:00+00:00"


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


def test_start_news_engine_launches_missing_workers(monkeypatch):
    snapshots = [
        {
            "worker_running": False,
            "llm_running": False,
            "worker_pids": [],
            "llm_pids": [],
            "detector": "test",
            "error": None,
        },
        {
            "worker_running": True,
            "llm_running": True,
            "worker_pids": [41001],
            "llm_pids": [41002],
            "detector": "test",
            "error": None,
        },
    ]
    launched_modules = []

    def fake_scan():
        if len(snapshots) > 1:
            return dict(snapshots.pop(0))
        return dict(snapshots[0])

    def fake_spawn(module_name: str):
        launched_modules.append(module_name)
        pid = 41001 if module_name.endswith("worker") and not module_name.endswith("llm_worker") else 41002
        return {"module": module_name, "command": ["python", "-m", module_name], "pid": pid}

    monkeypatch.setattr(news_api, "_scan_external_news_processes", fake_scan)
    monkeypatch.setattr(news_api, "_spawn_detached_news_process", fake_spawn)
    monkeypatch.setattr(news_api, "_news_llm_enabled", lambda: True)
    news_api._invalidate_news_process_cache()

    with TestClient(_build_app()) as client:
        response = client.post("/api/news/engine/start")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "started"
    assert launched_modules == ["core.news.service.worker", "core.news.service.llm_worker"]
    assert [item["role"] for item in payload["started"]] == ["worker", "llm_worker"]
    assert payload["background_pull_running"] is True
    assert payload["background_llm_running"] is True
