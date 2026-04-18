from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from core.news.storage import db as news_db


def test_backoff_locks_rebind_to_current_event_loop():
    news_db._global_rate_limit_backoff = None
    news_db._global_rate_limit_lock = None
    news_db._global_rate_limit_lock_loop = None
    news_db._provider_rate_limit_backoff.clear()
    news_db._provider_rate_limit_lock = None
    news_db._provider_rate_limit_lock_loop = None

    until = datetime.now(timezone.utc) + timedelta(minutes=5)

    asyncio.run(news_db.set_global_backoff(until))
    first_global_loop = news_db._global_rate_limit_lock_loop
    asyncio.run(news_db.set_provider_backoff("glm", until))
    first_provider_loop = news_db._provider_rate_limit_lock_loop

    assert first_global_loop is not None
    assert first_provider_loop is not None

    assert asyncio.run(news_db.get_global_backoff()) == until
    assert asyncio.run(news_db.get_provider_backoff("glm")) == until

    assert news_db._global_rate_limit_lock_loop is not first_global_loop
    assert news_db._provider_rate_limit_lock_loop is not first_provider_loop
