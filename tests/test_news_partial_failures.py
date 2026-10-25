"""A source that delivers from some accounts and fails on others must say so in its state, without tripping the breaker."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.news.collectors import manager as manager_module
from core.news.eventizer.rules import load_news_rule_config
from core.news.service import worker
from core.news.storage import db as news_db
from core.news.storage.models import NewsBase


ROOT = Path(__file__).resolve().parents[1]
PUBLISHED = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0)


def _config():
    cfg = load_news_rule_config(ROOT / "config/news_rules.yaml", ROOT / "config/symbols.yaml")
    cfg["defaults"] = {**cfg["defaults"], "x_accounts_handles": ["alpha", "beta"], "x_accounts_archive_only_handles": []}
    return cfg


async def _temporary_news_db(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'partial.db').as_posix()}", poolclass=NullPool)
    monkeypatch.setattr(news_db, "news_engine", engine)
    monkeypatch.setattr(news_db, "NewsSessionLocal", async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False))
    async with engine.begin() as conn:
        await conn.run_sync(NewsBase.metadata.create_all)
    return engine


class FlakyX:
    """Delivers a post from `alpha`; the first pull also fails on `beta`, the second does not."""

    pulls = 0

    def __init__(self, _cfg):
        self.last_account_errors = {}

    def pull_incremental(self, **_kwargs):
        type(self).pulls += 1
        self.last_account_errors = {"beta": "RuntimeError: X request failed: status=599"} if type(self).pulls == 1 else {}
        post = {"source": "x_accounts", "title": f"alpha post {type(self).pulls}", "content": "gm", "url": f"https://x.com/alpha/status/{type(self).pulls}",
                "published_at": (PUBLISHED + timedelta(minutes=type(self).pulls)).isoformat(),
                "payload": {"handle": "alpha", "source_kind": "third_party_commentary"}}
        return [post], str((PUBLISHED + timedelta(minutes=type(self).pulls)).timestamp())

    def close(self):
        pass


def test_partial_failure_is_recorded_without_counting_as_a_source_failure_and_clears_when_healthy(tmp_path, monkeypatch):
    async def run():
        engine = await _temporary_news_db(tmp_path, monkeypatch)
        try:
            first = await worker.pull_source_once(_config(), "x_accounts")
            assert first["inserted_count"] == 1 and first["errors"] == []
            assert first["partial_errors"] == ["beta: RuntimeError: X request failed: status=599"]
            state = await news_db.get_source_state("x_accounts")
            assert state["last_error"].startswith("partial: beta:")
            assert state["error_count"] == 0 and state["failure_count"] == 0 and not state["paused_until"]
            assert state["success_count"] == 1                                   # the pull itself succeeded
            second = await worker.pull_source_once(_config(), "x_accounts")
            assert second["partial_errors"] == []
            assert (await news_db.get_source_state("x_accounts"))["last_error"] in (None, "")   # healthy again
        finally:
            await engine.dispose()

    FlakyX.pulls = 0
    monkeypatch.setenv("NEWS_ENABLE_X_ACCOUNTS", "true")
    monkeypatch.setenv("X_AUTH_TOKEN", "f00dfeedcafe0123456789abcdef0123456789ab")
    monkeypatch.setattr(manager_module, "XAccountsCollector", FlakyX)

    async def no_llm(_inserted):
        return None

    monkeypatch.setattr(worker, "on_news_inserted", no_llm)
    asyncio.run(run())


def test_telegram_channel_failures_are_surfaced_the_same_way():
    class Telegramish:
        last_channel_errors = {"cryptoquant_alert": "RuntimeError: preview unavailable"}

    class Quiet:
        pass

    assert manager_module._partial_failures(Telegramish()) == ["cryptoquant_alert: RuntimeError: preview unavailable"]
    assert manager_module._partial_failures(Quiet()) == []
    assert manager_module._partial_failures(type("X", (), {"last_account_errors": {}})()) == []
