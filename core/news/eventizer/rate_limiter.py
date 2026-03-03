"""Rate limiter using token bucket algorithm with exponential backoff for 429 errors."""
from __future__ import annotations

import asyncio
import os
import threading
import time
from typing import Optional

from loguru import logger


class RateLimiter:
    """Thread-safe rate limiter with token bucket algorithm and exponential backoff.

    This class implements a singleton pattern for rate limiting API calls.
    It uses a token bucket algorithm for proactive rate limiting and
    exponential backoff when receiving 429 (rate limit) errors.

    Environment variables:
        GLM_RATE_LIMIT_RPM: Requests per minute (default: 60)
        GLM_RATE_LIMIT_BURST: Maximum burst tokens (default: 10)
    """

    _instance: Optional["RateLimiter"] = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """Singleton pattern - return existing instance or create new one."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        rate_per_minute: int = 60,
        burst: int = 10,
    ):
        """Initialize rate limiter with token bucket parameters.

        Args:
            rate_per_minute: Maximum requests per minute (reads from GLM_RATE_LIMIT_RPM env)
            burst: Maximum burst capacity (reads from GLM_RATE_LIMIT_BURST env)
        """
        # Use environment variables if not provided
        if not self._initialized:
            self._rate_per_minute = int(
                os.getenv("GLM_RATE_LIMIT_RPM", rate_per_minute)
            )
            self._burst = int(os.getenv("GLM_RATE_LIMIT_BURST", burst))

            # Token bucket state
            self._tokens = float(self._burst)
            self._last_update = time.time()
            self._rate_per_second = self._rate_per_minute / 60.0

            # Exponential backoff state for 429 errors
            self._backoff_attempts = 0
            self._backoff_until = 0.0
            self._max_backoff = 60.0  # Maximum backoff time in seconds

            # Async lock for thread-safe token operations
            self._token_lock = asyncio.Lock()

            RateLimiter._initialized = True
            logger.info(
                f"RateLimiter initialized: {self._rate_per_minute} req/min, "
                f"burst={self._burst}"
            )

    @property
    def rate_per_minute(self) -> int:
        """Get current rate limit per minute."""
        return self._rate_per_minute

    @property
    def burst(self) -> int:
        """Get current burst capacity."""
        return self._burst

    @property
    def backoff_attempts(self) -> int:
        """Get current backoff attempt count."""
        return self._backoff_attempts

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time since last update."""
        now = time.time()
        elapsed = now - self._last_update

        if elapsed > 0:
            # Calculate new tokens to add
            new_tokens = elapsed * self._rate_per_second
            self._tokens = min(self._burst, self._tokens + new_tokens)
            self._last_update = now

    async def acquire(self) -> bool:
        """Try to acquire a token without waiting.

        Returns:
            True if a token was acquired, False otherwise
        """
        async with self._token_lock:
            self._refill_tokens()

            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False

    async def wait_for_token(self, timeout: Optional[float] = None) -> bool:
        """Wait until a token is available or timeout is reached.

        Args:
            timeout: Maximum time to wait in seconds. None = wait indefinitely.

        Returns:
            True if a token was acquired, False if timeout was reached
        """
        start_time = time.time()

        while True:
            # Check if we need to back off due to 429
            now = time.time()
            if now < self._backoff_until:
                remaining = self._backoff_until - now
                if timeout is not None and remaining > (timeout - (now - start_time)):
                    return False
                logger.debug(f"Rate limiter backing off for {remaining:.2f}s")
                await asyncio.sleep(min(remaining, 1.0))
                continue

            # Try to acquire token
            if await self.acquire():
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False
                # Wait time until next token is available
                time_to_next = (1 - self._tokens) / self._rate_per_second
                wait_time = min(time_to_next, timeout - elapsed)
            else:
                # Wait time until next token is available
                time_to_next = (1 - self._tokens) / self._rate_per_second
                wait_time = time_to_next

            await asyncio.sleep(max(0.1, wait_time))

    def on_rate_limit(self, retry_after: Optional[int] = None) -> None:
        """Handle a 429 (rate limit) error by setting exponential backoff.

        Args:
            retry_after: Suggested retry time in seconds from the API response.
                         If None, use exponential backoff.
        """
        self._backoff_attempts += 1

        # Calculate backoff time
        if retry_after is not None:
            # Use server-specified retry time
            backoff = float(retry_after)
        else:
            # Use exponential backoff: 1s -> 2s -> 4s -> 8s -> ...
            backoff = min(2 ** (self._backoff_attempts - 1), self._max_backoff)

        self._backoff_until = time.time() + backoff

        logger.warning(
            f"Rate limit hit (429). Backing off for {backoff:.1f}s "
            f"(attempt {self._backoff_attempts})"
        )

    def get_backoff_time(self) -> float:
        """Get the remaining backoff time in seconds.

        Returns:
            Remaining backoff time in seconds, 0 if not backing off
        """
        now = time.time()
        remaining = self._backoff_until - now
        return max(0.0, remaining)

    def reset_backoff(self) -> None:
        """Reset the backoff state (call after a successful request)."""
        if self._backoff_attempts > 0:
            logger.debug(f"Resetting backoff after {self._backoff_attempts} attempts")
        self._backoff_attempts = 0
        self._backoff_until = 0.0

    def get_tokens(self) -> float:
        """Get current token count (for monitoring).

        Returns:
            Current number of available tokens
        """
        self._refill_tokens()
        return self._tokens

    def update_rate(self, rate_per_minute: int, burst: Optional[int] = None) -> None:
        """Update rate limit parameters.

        Args:
            rate_per_minute: New requests per minute limit
            burst: New burst capacity (optional, keeps current if None)
        """
        self._rate_per_minute = rate_per_minute
        if burst is not None:
            self._burst = burst
        self._rate_per_second = rate_per_minute / 60.0
        self._tokens = min(self._burst, self._tokens)
        logger.info(
            f"RateLimiter updated: {self._rate_per_minute} req/min, "
            f"burst={self._burst}"
        )

    @classmethod
    def get_instance(cls) -> "RateLimiter":
        """Get the singleton instance.

        Returns:
            The shared RateLimiter instance
        """
        if cls._instance is None:
            cls()
        return cls._instance


# Global instance for convenience
rate_limiter = RateLimiter.get_instance()


async def rate_limit(timeout: Optional[float] = None) -> bool:
    """Convenience function to wait for a token.

    Args:
        timeout: Maximum time to wait in seconds

    Returns:
        True if a token was acquired, False if timeout
    """
    return await rate_limiter.wait_for_token(timeout)
