"""Test rate limiter functionality."""
import asyncio

import pytest

from core.news.eventizer.rate_limiter import RateLimiter, rate_limit


async def test_rate_limiter_singleton():
    """Test that RateLimiter implements singleton pattern."""
    limiter1 = RateLimiter()
    limiter2 = RateLimiter.get_instance()
    assert limiter1 is limiter2


async def test_acquire_token():
    """Test acquiring tokens from the bucket."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)
    # Should be able to acquire burst number of tokens
    for _ in range(10):
        assert await limiter.acquire() is True
    # Next acquire should fail
    assert await limiter.acquire() is False


async def test_wait_for_token():
    """Test waiting for a token to become available."""
    limiter = RateLimiter(rate_per_minute=600, burst=1)
    # Acquire the burst token
    assert await limiter.acquire() is True
    assert await limiter.acquire() is False

    # Wait for token with short timeout should eventually succeed
    start = asyncio.get_event_loop().time()
    result = await limiter.wait_for_token(timeout=2.0)
    elapsed = asyncio.get_event_loop().time() - start

    assert result is True
    assert 0.05 <= elapsed <= 2.0


async def test_backoff_on_429():
    """Test exponential backoff when receiving 429 error."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)

    # Trigger backoff
    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 0

    # Second backoff should be longer
    limiter.on_rate_limit()
    backoff_time = limiter.get_backoff_time()
    assert backoff_time > 1.0  # 2^1 = 2 seconds

    # Third backoff
    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 2.0  # 2^2 = 4 seconds


async def test_backoff_respects_max_limit():
    """Test that backoff respects the 60 second maximum."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)

    # Trigger many backoffs
    for _ in range(10):
        limiter.on_rate_limit()

    # Should be capped at 60 seconds
    assert limiter.get_backoff_time() <= 60.0


async def test_reset_backoff():
    """Test that backoff can be reset."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)
    limiter.on_rate_limit()
    assert limiter.get_backoff_time() > 0

    limiter.reset_backoff()
    assert limiter.get_backoff_time() == 0
    assert limiter.backoff_attempts == 0


async def test_wait_respects_backoff():
    """Test that wait_for_token respects backoff state."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)

    # Trigger backoff for 2 seconds
    limiter.on_rate_limit()

    # Attempt to acquire with short timeout should fail due to backoff
    start = asyncio.get_event_loop().time()
    result = await limiter.wait_for_token(timeout=0.5)
    elapsed = asyncio.get_event_loop().time() - start

    assert result is False
    assert elapsed < 1.0


async def test_retry_after_from_429():
    """Test using retry_after from 429 response."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)

    # API suggests retry after 5 seconds
    limiter.on_rate_limit(retry_after=5)
    backoff = limiter.get_backoff_time()

    # Should be approximately 5 seconds
    assert 4.5 <= backoff <= 5.5


async def test_rate_limit_convenience():
    """Test the rate_limit convenience function."""
    limiter = RateLimiter(rate_per_minute=600, burst=2)

    # First two should succeed immediately
    assert await rate_limit(timeout=0.1) is True
    assert await rate_limit(timeout=0.1) is True

    # Third should wait but succeed within 2 seconds
    start = asyncio.get_event_loop().time()
    result = await rate_limit(timeout=2.0)
    elapsed = asyncio.get_event_loop().time() - start

    assert result is True
    assert elapsed > 0.01


async def test_update_rate():
    """Test updating rate limit parameters."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)

    # Update to higher rate
    limiter.update_rate(rate_per_minute=120, burst=20)
    assert limiter.rate_per_minute == 120
    assert limiter.burst == 20


async def test_get_tokens():
    """Test getting current token count."""
    limiter = RateLimiter(rate_per_minute=60, burst=10)
    tokens = limiter.get_tokens()
    assert tokens == 10.0

    # Acquire a token
    await limiter.acquire()
    tokens = limiter.get_tokens()
    assert tokens < 10.0


async def test_environment_variables():
    """Test that environment variables are respected."""
    # This test requires the RateLimiter to be re-initialized
    # In practice, the singleton pattern means the first call wins
    import os

    os.environ["GLM_RATE_LIMIT_RPM"] = "120"
    os.environ["GLM_RATE_LIMIT_BURST"] = "20"

    # Create new instance (in production, singleton applies)
    limiter = RateLimiter()
    assert limiter.rate_per_minute == 120
    assert limiter.burst == 20

    # Cleanup
    del os.environ["GLM_RATE_LIMIT_RPM"]
    del os.environ["GLM_RATE_LIMIT_BURST"]


if __name__ == "__main__":
    # Quick manual test
    print("Testing RateLimiter...")
    asyncio.run(test_rate_limiter_singleton())
    print("  Singleton pattern: OK")

    asyncio.run(test_acquire_token())
    print("  Token acquisition: OK")

    asyncio.run(test_backoff_on_429())
    print("  Backoff on 429: OK")

    print("\nAll tests passed!")
