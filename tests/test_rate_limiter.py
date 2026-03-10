"""Test rate limiter functionality without pytest-asyncio."""

from __future__ import annotations

import asyncio
import os

import pytest

from core.news.eventizer import rate_limiter as rate_limiter_module
from core.news.eventizer.rate_limiter import RateLimiter


def _reset_rate_limiter(*, rate_per_minute: int = 60, burst: int = 10) -> RateLimiter:
    RateLimiter._instance = None
    RateLimiter._initialized = False
    limiter = RateLimiter(rate_per_minute=rate_per_minute, burst=burst)
    rate_limiter_module.rate_limiter = limiter
    return limiter


@pytest.fixture(autouse=True)
def reset_rate_limiter_state():
    old_rpm = os.environ.get("GLM_RATE_LIMIT_RPM")
    old_burst = os.environ.get("GLM_RATE_LIMIT_BURST")
    _reset_rate_limiter()
    yield
    if old_rpm is None:
        os.environ.pop("GLM_RATE_LIMIT_RPM", None)
    else:
        os.environ["GLM_RATE_LIMIT_RPM"] = old_rpm
    if old_burst is None:
        os.environ.pop("GLM_RATE_LIMIT_BURST", None)
    else:
        os.environ["GLM_RATE_LIMIT_BURST"] = old_burst
    _reset_rate_limiter()


def test_rate_limiter_singleton():
    limiter1 = _reset_rate_limiter()
    limiter2 = RateLimiter.get_instance()
    assert limiter1 is limiter2


def test_acquire_token():
    async def _run():
        limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
        for _ in range(10):
            assert await limiter.acquire() is True
        assert await limiter.acquire() is False

    asyncio.run(_run())


def test_wait_for_token():
    async def _run():
        limiter = _reset_rate_limiter(rate_per_minute=600, burst=1)
        assert await limiter.acquire() is True
        assert await limiter.acquire() is False

        start = asyncio.get_running_loop().time()
        result = await limiter.wait_for_token(timeout=2.0)
        elapsed = asyncio.get_running_loop().time() - start

        assert result is True
        assert 0.05 <= elapsed <= 2.0

    asyncio.run(_run())


def test_backoff_on_429():
    limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 0

    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 1.0

    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 2.0


def test_backoff_respects_max_limit():
    limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
    for _ in range(10):
        limiter.on_rate_limit()
    assert limiter.get_backoff_time() <= 60.0


def test_reset_backoff():
    limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 0

    limiter.reset_backoff()
    assert limiter.get_backoff_time() == 0
    assert limiter.backoff_attempts == 0


def test_wait_respects_backoff():
    async def _run():
        limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
        limiter.on_rate_limit()

        start = asyncio.get_running_loop().time()
        result = await limiter.wait_for_token(timeout=0.5)
        elapsed = asyncio.get_running_loop().time() - start

        assert result is False
        assert elapsed < 1.0

    asyncio.run(_run())


def test_retry_after_from_429():
    limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
    limiter.on_rate_limit(retry_after=5)
    backoff = limiter.get_backoff_time()
    assert 4.5 <= backoff <= 5.5


def test_rate_limit_convenience():
    async def _run():
        _reset_rate_limiter(rate_per_minute=600, burst=2)
        assert await rate_limiter_module.rate_limit(timeout=0.1) is True
        assert await rate_limiter_module.rate_limit(timeout=0.1) is True

        start = asyncio.get_running_loop().time()
        result = await rate_limiter_module.rate_limit(timeout=2.0)
        elapsed = asyncio.get_running_loop().time() - start

        assert result is True
        assert elapsed > 0.01

    asyncio.run(_run())


def test_update_rate():
    limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
    limiter.update_rate(rate_per_minute=120, burst=20)
    assert limiter.rate_per_minute == 120
    assert limiter.burst == 20


def test_get_tokens():
    async def _run():
        limiter = _reset_rate_limiter(rate_per_minute=60, burst=10)
        assert limiter.get_tokens() == 10.0
        await limiter.acquire()
        assert limiter.get_tokens() < 10.0

    asyncio.run(_run())


def test_environment_variables():
    os.environ["GLM_RATE_LIMIT_RPM"] = "120"
    os.environ["GLM_RATE_LIMIT_BURST"] = "20"

    limiter = _reset_rate_limiter()
    assert limiter.rate_per_minute == 120
    assert limiter.burst == 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
