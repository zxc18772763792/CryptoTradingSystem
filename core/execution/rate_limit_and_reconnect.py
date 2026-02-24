"""Shared rate-limit and reconnect policy helpers for REST/WS execution paths.

This module is intentionally standalone so it can be adopted incrementally without
changing the current live execution flow.
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


class RateLimitExceeded(RuntimeError):
    """Raised when a bucket cannot satisfy a request within the caller policy."""

    def __init__(self, key: str, retry_after_ms: int, message: str = ""):
        self.key = str(key)
        self.retry_after_ms = max(0, int(retry_after_ms))
        super().__init__(message or f"Rate limit exceeded for {self.key}; retry after {self.retry_after_ms} ms")


@dataclass
class BucketState:
    tokens: float
    capacity: float
    refill_per_sec: float
    last_ts: float = field(default_factory=time.time)
    failures: int = 0
    penalty_until_ts: float = 0.0
    success_count: int = 0
    deny_count: int = 0

    def refill(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        elapsed = max(0.0, ts - self.last_ts)
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
            self.last_ts = ts

    def retry_after_seconds(self, cost: float = 1.0, now: Optional[float] = None) -> float:
        ts = float(now if now is not None else time.time())
        self.refill(ts)
        penalty_wait = max(0.0, self.penalty_until_ts - ts)
        deficit = max(0.0, float(cost) - self.tokens)
        token_wait = deficit / self.refill_per_sec if self.refill_per_sec > 0 else 60.0
        return max(penalty_wait, token_wait)


class RateLimitAndReconnectPolicy:
    """Token buckets + backoff + exchange protection cooldowns.

    Targets:
    - REST weights (IP budget)
    - order rate (per-minute / 10-second)
    - WS control messages
    - quant-rules / reduce-only cooldown mode
    """

    def __init__(self) -> None:
        self._buckets: Dict[str, BucketState] = {}
        self._mode = "normal"  # normal | reduce_only
        self._mode_reason = ""
        self._mode_until_ts = 0.0
        self._mode_change_ts = time.time()
        self._global_failures = 0

    def configure_bucket(self, key: str, capacity: float, refill_per_sec: float, initial_tokens: Optional[float] = None) -> None:
        cap = max(0.001, float(capacity))
        refill = max(0.0, float(refill_per_sec))
        init = cap if initial_tokens is None else max(0.0, min(cap, float(initial_tokens)))
        self._buckets[str(key)] = BucketState(tokens=init, capacity=cap, refill_per_sec=refill)

    def configure_binance_futures_defaults(self) -> None:
        """Reasonable defaults for a single bot/account process.

        Notes:
        - conservative values; you can tune per account / exchange policy.
        - `order_10s` is modeled as token bucket (window approximation).
        """

        self.configure_bucket("rest_weight", capacity=2400, refill_per_sec=2400 / 60.0)
        self.configure_bucket("order_1m", capacity=1200, refill_per_sec=1200 / 60.0)
        self.configure_bucket("order_10s", capacity=300, refill_per_sec=300 / 10.0)
        self.configure_bucket("ws_ctrl", capacity=8, refill_per_sec=8.0)  # reserve headroom below exchange max

    def _bucket(self, key: str) -> Optional[BucketState]:
        return self._buckets.get(str(key))

    def can_open_positions(self, now: Optional[float] = None) -> bool:
        ts = float(now if now is not None else time.time())
        if self._mode == "reduce_only" and ts < self._mode_until_ts:
            return False
        if self._mode == "reduce_only" and ts >= self._mode_until_ts:
            self._mode = "normal"
            self._mode_reason = ""
            self._mode_change_ts = ts
        return True

    def is_reduce_only_mode(self, now: Optional[float] = None) -> bool:
        return not self.can_open_positions(now=now)

    def set_reduce_only_cooldown(self, cooldown_seconds: float, reason: str = "quant_rules") -> None:
        now = time.time()
        self._mode = "reduce_only"
        self._mode_reason = str(reason or "quant_rules")
        self._mode_until_ts = max(self._mode_until_ts, now + max(0.0, float(cooldown_seconds)))
        self._mode_change_ts = now

    def clear_reduce_only_mode(self) -> None:
        self._mode = "normal"
        self._mode_reason = ""
        self._mode_until_ts = 0.0
        self._mode_change_ts = time.time()

    def penalize(self, key: str, retry_after_ms: int = 0, *, failures_inc: int = 1) -> None:
        b = self._bucket(key)
        if b is None:
            return
        now = time.time()
        b.failures += max(0, int(failures_inc))
        if retry_after_ms and retry_after_ms > 0:
            b.penalty_until_ts = max(b.penalty_until_ts, now + retry_after_ms / 1000.0)
        b.deny_count += 1
        self._global_failures += max(0, int(failures_inc))

    def record_success(self, key: str) -> None:
        b = self._bucket(key)
        if b is None:
            return
        b.failures = 0
        b.success_count += 1

    def record_failure(self, key: str) -> None:
        b = self._bucket(key)
        if b is None:
            return
        b.failures += 1
        self._global_failures += 1

    def can_acquire(self, key: str, cost: float = 1.0) -> bool:
        b = self._bucket(key)
        if b is None:
            return True
        now = time.time()
        b.refill(now)
        if now < b.penalty_until_ts:
            return False
        return b.tokens >= float(cost)

    def acquire(self, key: str, cost: float = 1.0, *, wait: bool = False, timeout_ms: Optional[int] = None) -> bool:
        """Acquire a bucket.

        - `wait=False`: returns bool.
        - `wait=True`: blocks until success or raises `RateLimitExceeded` on timeout.
        """
        b = self._bucket(key)
        if b is None:
            return True

        start = time.time()
        cost = max(0.0, float(cost))

        while True:
            now = time.time()
            b.refill(now)
            if now >= b.penalty_until_ts and b.tokens >= cost:
                b.tokens -= cost
                return True

            if not wait:
                return False

            retry_after_s = max(0.0, b.retry_after_seconds(cost=cost, now=now))
            if timeout_ms is not None:
                elapsed_ms = (time.time() - start) * 1000.0
                if elapsed_ms + retry_after_s * 1000.0 > float(timeout_ms):
                    raise RateLimitExceeded(str(key), int(retry_after_s * 1000))
            time.sleep(min(max(retry_after_s, 0.001), 1.0))

    async def acquire_async(self, key: str, cost: float = 1.0, *, timeout_ms: Optional[int] = None) -> bool:
        b = self._bucket(key)
        if b is None:
            return True
        start = time.time()
        cost = max(0.0, float(cost))

        while True:
            now = time.time()
            b.refill(now)
            if now >= b.penalty_until_ts and b.tokens >= cost:
                b.tokens -= cost
                return True

            retry_after_s = max(0.0, b.retry_after_seconds(cost=cost, now=now))
            if timeout_ms is not None:
                elapsed_ms = (time.time() - start) * 1000.0
                if elapsed_ms + retry_after_s * 1000.0 > float(timeout_ms):
                    raise RateLimitExceeded(str(key), int(retry_after_s * 1000))
            await asyncio.sleep(min(max(retry_after_s, 0.001), 1.0))

    def next_retry_delay(self, key: str, base: float = 1.0, cap: float = 30.0) -> float:
        b = self._bucket(key)
        failures = b.failures if b is not None else self._global_failures
        backoff = min(float(cap), float(base) * (2 ** min(int(failures), 8)))
        jitter = random.uniform(0.0, min(1.0, backoff * 0.2))
        penalty = 0.0
        if b is not None:
            penalty = max(0.0, b.penalty_until_ts - time.time())
        return max(backoff + jitter, penalty)

    def retry_after_ms(self, key: str, cost: float = 1.0) -> int:
        b = self._bucket(key)
        if b is None:
            return 0
        return int(max(0.0, b.retry_after_seconds(cost=cost)) * 1000)

    def stats(self) -> Dict[str, Any]:
        now = time.time()
        buckets = {}
        for key, b in self._buckets.items():
            b.refill(now)
            buckets[key] = {
                **asdict(b),
                "tokens": round(float(b.tokens), 4),
                "penalty_ms_left": max(0, int((b.penalty_until_ts - now) * 1000)),
                "retry_after_ms_cost1": int(max(0.0, b.retry_after_seconds(cost=1.0, now=now)) * 1000),
            }
        return {
            "mode": self._mode if self.can_open_positions(now=now) else "reduce_only",
            "mode_reason": self._mode_reason,
            "mode_ms_left": max(0, int((self._mode_until_ts - now) * 1000)),
            "mode_changed_at": self._mode_change_ts,
            "global_failures": self._global_failures,
            "buckets": buckets,
        }

