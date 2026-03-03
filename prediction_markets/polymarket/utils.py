from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional, TypeVar

from dateutil import parser as dt_parser

T = TypeVar("T")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts_any(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            dt = datetime.fromtimestamp(float(text), tz=timezone.utc)
        else:
            dt = dt_parser.isoparse(text)
    else:
        raise ValueError(f"unsupported ts value: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class AsyncTokenBucket:
    def __init__(self, burst: int, refill_per_sec: float):
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.refill_per_sec = max(0.1, float(refill_per_sec))
        self.updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        need = max(0.0, float(cost))
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.updated_at)
                self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
                self.updated_at = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                missing = need - self.tokens
            await asyncio.sleep(max(0.01, missing / self.refill_per_sec))


@dataclass
class CircuitBreaker:
    error_threshold: int = 8
    cooldown_sec: int = 120
    error_count: int = 0
    paused_until: Optional[datetime] = None

    def allow(self) -> bool:
        if self.paused_until and self.paused_until > utc_now():
            return False
        if self.paused_until and self.paused_until <= utc_now():
            self.paused_until = None
            self.error_count = 0
        return True

    def on_success(self) -> None:
        self.error_count = 0
        self.paused_until = None

    def on_error(self) -> None:
        self.error_count += 1
        if self.error_count >= self.error_threshold:
            self.paused_until = utc_now().replace(microsecond=0) + timedelta(seconds=self.cooldown_sec)


async def async_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 5.0,
    jitter: float = 0.2,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    last_exc: Optional[BaseException] = None
    for attempt in range(max(1, retries)):
        try:
            return await fn()
        except retry_exceptions as exc:
            last_exc = exc
            if attempt >= retries - 1:
                break
            delay = min(max_delay, base_delay * (2 ** attempt))
            if jitter > 0:
                delay += random.uniform(0, jitter)
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc
