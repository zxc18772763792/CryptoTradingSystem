from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.news.storage import db as news_db
from core.news.storage.models import NewsBase, NewsLLMTask, NewsRaw


async def _with_temp_news_db(tmp_path: Path, monkeypatch):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'news_tasks.db').as_posix()}",
        poolclass=NullPool,
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(news_db, "NewsSessionLocal", session_factory)
    async with engine.begin() as conn:
        await conn.run_sync(NewsBase.metadata.create_all)
    return engine


async def _dispose_temp_news_db(engine) -> None:
    await engine.dispose()
    await asyncio.sleep(0)


def test_enqueue_llm_tasks_dedupes_active_tasks_and_requeues_due_retry(tmp_path, monkeypatch):
    async def _run():
        engine = await _with_temp_news_db(tmp_path, monkeypatch)
        now = datetime.now(timezone.utc)

        try:
            async with news_db.news_session_scope() as session:
                session.add_all(
                    [
                        NewsLLMTask(
                            raw_news_id=1,
                            source="rss",
                            status="running",
                            priority=80,
                            started_at=now,
                            updated_at=now,
                        ),
                        NewsLLMTask(
                            raw_news_id=2,
                            source="rss",
                            status="retry",
                            priority=30,
                            next_retry_at=now - timedelta(seconds=1),
                            updated_at=now - timedelta(seconds=10),
                        ),
                        NewsLLMTask(
                            raw_news_id=3,
                            source="rss",
                            status="done",
                            priority=90,
                            finished_at=now,
                            updated_at=now,
                        ),
                    ]
                )

            result = await news_db.enqueue_llm_tasks(
                [
                    {"id": 1, "source": "rss", "payload": {"importance_score": 90}},
                    {"id": 2, "source": "rss", "payload": {"importance_score": 95}},
                    {"id": 2, "source": "rss", "payload": {"importance_score": 95}},
                    {"id": 3, "source": "rss", "payload": {"importance_score": 90}},
                    {"id": 4, "source": "rss", "payload": {"importance_score": 100}},
                ],
                min_importance=35,
            )

            assert result == {"queued_count": 1, "requeued_count": 1, "skipped_count": 3}

            async with news_db.news_session_scope() as session:
                rows = (
                    await session.execute(select(NewsLLMTask).order_by(NewsLLMTask.raw_news_id.asc()))
                ).scalars().all()
                by_raw_id = {row.raw_news_id: row for row in rows}

            assert set(by_raw_id) == {1, 2, 3, 4}
            assert by_raw_id[1].status == "running"
            assert by_raw_id[2].status == "retry"
            assert by_raw_id[2].priority == 95
            assert by_raw_id[3].status == "done"
            assert by_raw_id[4].status == "pending"
        finally:
            await _dispose_temp_news_db(engine)

    asyncio.run(_run())


def test_claim_llm_tasks_handles_due_retry_datetime_session_sync(tmp_path, monkeypatch):
    async def _run():
        engine = await _with_temp_news_db(tmp_path, monkeypatch)
        now = datetime.now(timezone.utc)

        try:
            async with news_db.news_session_scope() as session:
                session.add(
                    NewsRaw(
                        id=10,
                        source="rss",
                        title="BTC ETF flow update",
                        url="https://example.test/news/10",
                        content="ETF flows increased.",
                        published_at=now - timedelta(minutes=5),
                        fetched_at=now - timedelta(minutes=4),
                        lang="en",
                        content_hash="claim-llm-task-10",
                        symbols={},
                        payload={"importance_score": 80},
                    )
                )
                session.add(
                    NewsLLMTask(
                        raw_news_id=10,
                        source="rss",
                        status="retry",
                        priority=80,
                        attempt_count=1,
                        next_retry_at=now - timedelta(seconds=1),
                        updated_at=now - timedelta(seconds=10),
                    )
                )

            tasks = await news_db.claim_llm_tasks(limit=1)

            assert len(tasks) == 1
            assert tasks[0]["id"] == 10
            assert tasks[0]["llm_task"]["status"] == "running"
            assert tasks[0]["llm_task"]["attempt_count"] == 2

            async with news_db.news_session_scope() as session:
                row = (
                    await session.execute(select(NewsLLMTask).where(NewsLLMTask.raw_news_id == 10))
                ).scalar_one()

            assert row.status == "running"
            assert row.attempt_count == 2
            assert row.started_at is not None
        finally:
            await _dispose_temp_news_db(engine)

    asyncio.run(_run())


def test_save_news_raw_dedupes_tracking_param_url_variants(tmp_path, monkeypatch):
    async def _run():
        engine = await _with_temp_news_db(tmp_path, monkeypatch)
        now = datetime.now(timezone.utc)

        try:
            result = await news_db.save_news_raw(
                [
                    {
                        "source": "rss",
                        "title": "ETF flows accelerate",
                        "url": "https://example.test/story?utm_source=x&ref=home",
                        "content": "ETF demand remains elevated.",
                        "published_at": now - timedelta(minutes=2),
                    },
                    {
                        "source": "rss",
                        "title": "ETF flows accelerate",
                        "url": "https://example.test/story?ref=home&utm_campaign=y",
                        "content": "ETF demand remains elevated.",
                        "published_at": now - timedelta(minutes=1),
                    },
                ]
            )

            assert result["pulled_count"] == 2
            assert result["deduped_count"] == 1
            assert len(result["inserted"]) == 1

            async with news_db.news_session_scope() as session:
                rows = (await session.execute(select(NewsRaw).order_by(NewsRaw.id.asc()))).scalars().all()

            assert len(rows) == 1
            assert rows[0].url == "https://example.test/story?utm_source=x&ref=home"
        finally:
            await _dispose_temp_news_db(engine)

    asyncio.run(_run())
