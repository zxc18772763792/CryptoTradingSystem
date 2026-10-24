"""Telegram public posts use the durable news pipeline without account access."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.news.collectors import manager as manager_module
from core.news.eventizer.rules import load_news_rule_config
from core.news.service import worker
from core.news.storage import db as news_db
from core.news.storage.models import NewsBase, NewsLLMTask, NewsRaw


SOURCE = "telegram_alastar"
ROOT = Path(__file__).resolve().parents[1]


def _config():
    return load_news_rule_config(ROOT / "config/news_rules.yaml", ROOT / "config/symbols.yaml")


async def _temporary_news_db(tmp_path, monkeypatch):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'telegram_news.db').as_posix()}",
        poolclass=NullPool,
    )
    monkeypatch.setattr(news_db, "news_engine", engine)
    monkeypatch.setattr(
        news_db, "NewsSessionLocal", async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    )
    async with engine.begin() as conn:
        await conn.run_sync(NewsBase.metadata.create_all)
    return engine


def test_default_configuration_enables_alastar_collection(monkeypatch):
    monkeypatch.setenv("NEWS_ENABLE_TELEGRAM_ALASTAR", "true")
    monkeypatch.delenv("NEWS_INTERVAL_TELEGRAM_ALASTAR", raising=False)
    cfg = _config()
    manager = manager_module.MultiSourceNewsCollector(cfg)
    specs, errors = manager._build_collectors(source_names=[SOURCE])
    try:
        assert SOURCE in manager.sources
        assert cfg["defaults"]["telegram_alastar_channels"] == ["alastartrades", "cryptoquant_alert"]
        assert cfg["defaults"]["telegram_archive_only_channels"] == ["cryptoquant_alert"]  # alert feed: archive, no news item
        assert [spec.name for spec in specs] == [SOURCE]
        assert errors == []
        assert worker._source_interval(SOURCE) == 60
        assert news_db.SOURCE_IMPORTANCE[SOURCE] < news_db.SOURCE_IMPORTANCE["binance_announcements"]
    finally:
        manager._close_collectors(specs)


def test_disabled_single_source_does_not_pull_unrelated_fallbacks(monkeypatch):
    monkeypatch.setenv("NEWS_ENABLE_TELEGRAM_ALASTAR", "false")
    manager = manager_module.MultiSourceNewsCollector(_config())

    def unexpected_fallback(*_args, **_kwargs):
        raise AssertionError("A disabled Telegram-only pull must not query unrelated sources")

    for name in ("Jin10Collector", "RSSNewsCollector", "GDELTCollector"):
        monkeypatch.setattr(manager_module, name, unexpected_fallback)

    result = manager.pull_latest(source_names=[SOURCE])
    assert result["items"] == []
    assert result["source_stats"] == {}
    assert any("telegram_alastar disabled" in error for error in result["errors"])


def test_failed_public_channel_pull_records_failure_without_advancing_cursor(tmp_path, monkeypatch):
    class FailedCollector:
        closed = False

        def __init__(self, _cfg):
            pass

        def pull_incremental(self, **_kwargs):
            raise RuntimeError("public channel unavailable")

        def close(self):
            type(self).closed = True

    async def run():
        engine = await _temporary_news_db(tmp_path, monkeypatch)
        try:
            await news_db.set_source_state(SOURCE, cursor_value="1770000000")
            manager = manager_module.MultiSourceNewsCollector(_config())
            result = await manager.pull_latest_incremental(source_names=[SOURCE])
            state = await news_db.get_source_state(SOURCE)
            assert result["items"] == []
            assert SOURCE not in result["source_cursors"]
            assert "public channel unavailable" in result["errors"][0]
            assert state["cursor_value"] == "1770000000"
            assert state["failure_count"] == 1
            assert state["error_count"] == 1
            assert state["last_success_at"] is None
            assert FailedCollector.closed
        finally:
            await engine.dispose()

    monkeypatch.setenv("NEWS_ENABLE_TELEGRAM_ALASTAR", "true")
    monkeypatch.setattr(manager_module, "TelegramPublicChannelCollector", FailedCollector)
    asyncio.run(run())


def test_worker_persists_publication_time_and_queues_each_post_once(tmp_path, monkeypatch):
    published = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0)
    message = {
        "source": SOURCE,
        "title": "Alastar: Watching this market closely",
        "content": "Personal market commentary with an explicitly identified author.",
        "url": "https://t.me/alastartrades/12345",
        "published_at": published.isoformat(),
        "payload": {
            "telegram_channel": "alastartrades",
            "telegram_message_id": 12345,
            "source_kind": "third_party_commentary",
        },
    }

    class PublicCollector:
        def __init__(self, _cfg):
            pass

        def pull_incremental(self, **_kwargs):
            return [dict(message, payload=dict(message["payload"]))], str(published.timestamp())

        def close(self):
            pass

    async def do_not_run_external_llm(_inserted):
        return None

    async def run():
        engine = await _temporary_news_db(tmp_path, monkeypatch)
        try:
            first = await worker.pull_source_once(_config(), SOURCE)
            second = await worker.pull_source_once(_config(), SOURCE)
            await asyncio.sleep(0)  # Let the stubbed insertion callback finish.

            assert first["inserted_count"] == 1
            assert first["queued_count"] == 1
            assert second["inserted_count"] == 0
            assert second["queued_count"] == 0
            async with news_db.news_session_scope() as session:
                rows = (await session.execute(select(NewsRaw))).scalars().all()
                tasks = (await session.execute(select(NewsLLMTask))).scalars().all()
            assert len(rows) == len(tasks) == 1
            row = rows[0]
            assert row.source == SOURCE
            assert row.published_at.replace(tzinfo=timezone.utc) == published
            assert row.fetched_at.replace(tzinfo=timezone.utc) > published
            assert row.payload["provider"] == SOURCE
            assert row.payload["source_kind"] == "third_party_commentary"
            assert row.payload["latency_sec"] >= 3600
            assert tasks[0].raw_news_id == row.id
            assert tasks[0].status == "pending"
            assert tasks[0].priority == 35
            state = await news_db.get_source_state(SOURCE)
            assert float(state["cursor_value"]) == published.timestamp()
        finally:
            await engine.dispose()

    monkeypatch.setenv("NEWS_ENABLE_TELEGRAM_ALASTAR", "true")
    monkeypatch.setenv("NEWS_LLM_MIN_IMPORTANCE", "35")
    monkeypatch.setattr(manager_module, "TelegramPublicChannelCollector", PublicCollector)
    monkeypatch.setattr(worker, "on_news_inserted", do_not_run_external_llm)
    asyncio.run(run())


@pytest.mark.parametrize("cursor", [None, "1770000000"])
@pytest.mark.parametrize("failed", [False, True])
def test_worker_quiet_poll_health_distinguishes_success_from_failure(tmp_path, monkeypatch, cursor, failed):
    class QuietCollector:
        def __init__(self, _cfg):
            pass

        def pull_latest(self, **_kwargs):
            if failed:
                raise RuntimeError("public preview unavailable")
            return []

        def close(self):
            pass

    async def run():
        engine = await _temporary_news_db(tmp_path, monkeypatch)
        try:
            await news_db.set_source_state(SOURCE, cursor_value=cursor)
            started = datetime.now(timezone.utc)
            result = await worker.pull_source_once(_config(), SOURCE)
            state = await news_db.get_source_state(SOURCE)
            assert result["inserted_count"] == result["queued_count"] == 0
            # A successful check has no publication to acknowledge; in
            # particular, it must not move the cursor forward to wall-clock time.
            assert state["cursor_value"] == cursor
            if failed:
                assert state["last_success_at"] is None
                assert state["success_count"] == 0
                assert state["failure_count"] == state["error_count"] == 1
                assert any("public preview unavailable" in error for error in result["errors"])
            else:
                assert datetime.fromisoformat(state["last_success_at"]) >= started
                assert state["success_count"] == 1
                assert state["failure_count"] == state["error_count"] == 0
                assert result["errors"] == []
        finally:
            await engine.dispose()

    monkeypatch.setenv("NEWS_ENABLE_TELEGRAM_ALASTAR", "true")
    monkeypatch.setattr(manager_module, "TelegramPublicChannelCollector", QuietCollector)
    asyncio.run(run())


def test_bootstrap_snapshot_archives_original_dates_without_queuing_old_events(tmp_path, monkeypatch):
    from scripts import sync_alastar_telegram as snapshot

    published = (datetime.now(timezone.utc) - timedelta(days=7)).replace(microsecond=0)
    content = "Alastar's original public commentary from last week."
    old_cursor = "1770000000"

    class SnapshotCollector:
        closed = False
        window_minutes = None

        def __init__(self, _cfg):
            pass

        def pull_latest(self, *, max_records, since_minutes):
            type(self).window_minutes = since_minutes
            return [{
                "source": SOURCE,
                "title": content,
                "content": content,
                "url": "https://t.me/alastartrades/23456",
                "published_at": published.isoformat(),
                "payload": {"source_kind": "third_party_commentary"},
            }]

        def close(self):
            type(self).closed = True

    async def no_database_lifecycle_side_effects():
        return None

    async def unexpected_queue(*_args, **_kwargs):
        raise AssertionError("Historical snapshot must not enqueue old trading events")

    async def run():
        engine = await _temporary_news_db(tmp_path, monkeypatch)
        try:
            await news_db.set_source_state(SOURCE, cursor_value=old_cursor)
            result = await snapshot.sync_snapshot(days=30)
            assert result["captured_count"] == result["inserted_count"] == 1
            assert result["historical_tasks_queued"] == 0
            assert result["full_history"] is False
            assert result["latest_published_at"] == result["earliest_published_at"] == published.isoformat()
            assert SnapshotCollector.closed
            assert SnapshotCollector.window_minutes == 30 * 1440
            async with news_db.news_session_scope() as session:
                rows = (await session.execute(select(NewsRaw))).scalars().all()
                tasks = (await session.execute(select(NewsLLMTask))).scalars().all()
            assert len(rows) == 1
            assert tasks == []
            row = rows[0]
            assert row.content == row.title == content
            assert row.published_at.replace(tzinfo=timezone.utc) == published
            assert row.payload["ingest"]["mode"] == "telegram_public_snapshot"
            assert row.payload["ingest"]["purpose"] == "historical_archive"
            state = await news_db.get_source_state(SOURCE)
            assert state["last_success_at"] is not None
            assert state["cursor_value"] == old_cursor
        finally:
            await engine.dispose()

    monkeypatch.setattr(snapshot, "TelegramPublicChannelCollector", SnapshotCollector)
    monkeypatch.setattr(snapshot, "ensure_proxy_env", lambda _settings: None)
    monkeypatch.setattr(news_db, "init_news_db", no_database_lifecycle_side_effects)
    monkeypatch.setattr(news_db, "close_news_db", no_database_lifecycle_side_effects)
    monkeypatch.setattr(news_db, "enqueue_llm_tasks", unexpected_queue)
    asyncio.run(run())
