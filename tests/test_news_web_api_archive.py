from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

from web.api import news as news_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(news_api.router, prefix="/api/news")
    app.state.news_cfg = news_api.load_news_cfg()
    return app


def _ops_headers() -> dict[str, str]:
    return {"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"}


def test_health_returns_runtime_snapshot_while_db_refresh_runs(monkeypatch):
    news_api._NEWS_HEALTH_REFRESH_TASK = None
    news_api._NEWS_RESPONSE_CACHE.setdefault("health", {}).clear()
    news_api._NEWS_PROCESS_CACHE_PAYLOAD = {}
    news_api._NEWS_PROCESS_CACHE_AT = 0.0

    async def slow_db_snapshot(timeout_sec):
        del timeout_sec
        raise AssertionError("health should schedule db refresh, not await it")

    monkeypatch.setattr(news_api, "_collect_news_db_snapshot", slow_db_snapshot)

    with TestClient(_build_app()) as client:
        response = client.get("/api/news/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "warming_up"
    assert "db snapshot refresh scheduled" in payload["fallback_reason"]


def test_external_news_process_snapshot_defers_first_windows_scan(monkeypatch):
    monkeypatch.setattr(news_api.sys, "platform", "win32")
    news_api._NEWS_PROCESS_CACHE_PAYLOAD = {}
    news_api._NEWS_PROCESS_CACHE_AT = 0.0
    monkeypatch.setattr(
        news_api,
        "_scan_external_news_processes",
        lambda: (_ for _ in ()).throw(AssertionError("scan should be deferred")),
    )

    payload = news_api._external_news_process_snapshot()

    assert payload["detector"] == "deferred"
    assert payload["worker_running"] is False


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
    windows = {}

    async def fake_list_events(symbol=None, since=None, until=None, limit=0):
        del symbol, limit
        windows["list_events"] = (since, until)
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

    async def fake_count_events(symbol=None, since=None, until=None):
        del symbol
        windows["count_events"] = (since, until)
        return 9

    async def fake_latest_event(symbol=None, since=None, until=None):
        del symbol
        windows["latest_event"] = (since, until)
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
    assert windows["list_events"][0] is not None
    assert windows["list_events"][1] is not None
    assert windows["count_events"][1] == windows["list_events"][1]
    assert windows["latest_event"][1] == windows["list_events"][1]


def test_build_source_summary_keeps_coinglass_state_without_recent_rows():
    now = datetime.now(timezone.utc)
    source_states = [
        {
            "source": "coinglass_articles",
            "updated_at": (now - timedelta(hours=2)).isoformat(),
            "last_success_at": (now - timedelta(hours=2)).isoformat(),
            "paused_until": None,
            "last_error": None,
            "error_count": 0,
            "success_count": 5,
            "failure_count": 0,
        }
    ]

    summary = news_api._build_source_summary([], source_states, hours=24)

    assert "coinglass_articles" in summary
    slot = summary["coinglass_articles"]
    assert slot["inserted_count"] == 0
    assert slot["recent_window_status"] == "empty_recent_window"
    assert slot["freshness_status"] == "fresh"
    assert slot["alert_status"] == "notice"


def test_build_source_summary_marks_paused_coinglass_source_critical():
    now = datetime.now(timezone.utc)
    source_states = [
        {
            "source": "coinglass_newsflash",
            "updated_at": (now - timedelta(minutes=20)).isoformat(),
            "last_success_at": (now - timedelta(minutes=20)).isoformat(),
            "paused_until": (now + timedelta(minutes=8)).isoformat(),
            "last_error": "minute_budget_exhausted",
            "error_count": 1,
            "success_count": 4,
            "failure_count": 1,
        }
    ]

    summary = news_api._build_source_summary([], source_states, hours=24)

    slot = summary["coinglass_newsflash"]
    assert slot["freshness_status"] == "paused"
    assert slot["alert_status"] == "critical"
    assert slot["alert_reason"] == "paused"


def test_latest_triggers_auto_summary_repair_for_fallback_feed(monkeypatch):
    async def fake_build_latest_feed(cfg=None, symbol=None, hours=24, limit=60, summarize=False):
        del cfg, symbol, hours, limit, summarize
        return {
            "count": 3,
            "symbol": None,
            "hours": 24,
            "since": "2026-04-04T00:00:00+00:00",
            "items": [
                {"id": "raw-1", "title": "row-1", "summary_title": "row-1", "summary_source": "rule_fallback"},
                {"id": "raw-2", "title": "row-2", "summary_title": "row-2", "summary_source": "api_timeout_fallback"},
                {"id": "raw-3", "title": "row-3", "summary_title": "row-3", "summary_source": "rule_fallback"},
            ],
            "feed_stats": {"total": 3, "structured": 0, "unstructured": 3, "unstructured_breakdown": {}, "sentiment": {"positive": 0, "neutral": 3, "negative": 0}},
            "source_stats": {"by_provider": {}, "by_source": {}},
        }

    auto_pull_mock = AsyncMock(return_value=False)
    repair_mock = AsyncMock(return_value={"queued": True})

    news_api._NEWS_RESPONSE_CACHE.setdefault("latest", {}).clear()
    monkeypatch.setattr(news_api, "build_latest_feed", fake_build_latest_feed)
    monkeypatch.setattr(news_api, "_auto_pull_if_stale", auto_pull_mock)
    monkeypatch.setattr(news_api, "_maybe_schedule_background_summary_repair", repair_mock)

    with TestClient(_build_app()) as client:
        response = client.get("/api/news/latest?hours=24&limit=10&summarize=false")

    assert response.status_code == 200
    assert repair_mock.await_count == 1
    assert repair_mock.await_args.kwargs["trigger"] == "latest_fast"
    assert len(repair_mock.await_args.kwargs["items"]) == 3


def test_latest_cached_payload_still_triggers_auto_summary_repair(monkeypatch):
    async def fail_build_latest_feed(*args, **kwargs):
        raise AssertionError("cached latest payload should avoid rebuilding feed")

    repair_mock = AsyncMock(return_value={"queued": True})
    cache_key = news_api._cache_key(None, 24, 10, "fast")
    news_api._NEWS_RESPONSE_CACHE.setdefault("latest", {}).clear()
    news_api._cache_set(
        "latest",
        cache_key,
        {
            "count": 2,
            "symbol": None,
            "hours": 24,
            "since": "2026-04-04T00:00:00+00:00",
            "items": [
                {"id": "raw-1", "title": "row-1", "summary_title": "row-1", "summary_source": "rule_fallback"},
                {"id": "raw-2", "title": "row-2", "summary_title": "row-2", "summary_source": "rule_fallback"},
            ],
            "feed_stats": {"total": 2, "structured": 0, "unstructured": 2, "unstructured_breakdown": {}, "sentiment": {"positive": 0, "neutral": 2, "negative": 0}},
            "source_stats": {"by_provider": {}, "by_source": {}},
        },
    )
    monkeypatch.setattr(news_api, "build_latest_feed", fail_build_latest_feed)
    monkeypatch.setattr(news_api, "_maybe_schedule_background_summary_repair", repair_mock)

    with TestClient(_build_app()) as client:
        response = client.get("/api/news/latest?hours=24&limit=10&summarize=false")

    assert response.status_code == 200
    assert response.json()["_cache"]["hit"] is True
    assert repair_mock.await_count == 1
    assert repair_mock.await_args.kwargs["trigger"] == "latest_cache"


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
    monkeypatch.setenv("OPS_TOKEN", "test-token")

    app = _build_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/news/ingest/backfill_history",
            json={"hours": 72, "max_records": 180},
            headers=_ops_headers(),
        )

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
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    news_api._invalidate_news_process_cache()

    with TestClient(_build_app()) as client:
        response = client.post("/api/news/engine/start", headers=_ops_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "started"
    assert launched_modules == ["core.news.service.worker", "core.news.service.llm_worker"]
    assert [item["role"] for item in payload["started"]] == ["worker", "llm_worker"]
    assert payload["background_pull_running"] is True
    assert payload["background_llm_running"] is True
