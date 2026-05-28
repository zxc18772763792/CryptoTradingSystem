from __future__ import annotations

import asyncio

import pytest

from core.utils.asyncio_compat import LoopBoundAsyncLock


def test_loop_bound_async_lock_serializes_on_running_loop():
    async def _run() -> None:
        lock = LoopBoundAsyncLock()
        order: list[str] = []

        async def worker(name: str) -> None:
            async with lock:
                order.append(f"{name}-enter")
                await asyncio.sleep(0)
                order.append(f"{name}-exit")

        await asyncio.gather(worker("a"), worker("b"))

        assert order in (
            ["a-enter", "a-exit", "b-enter", "b-exit"],
            ["b-enter", "b-exit", "a-enter", "a-exit"],
        )

    asyncio.run(_run())


def test_loop_bound_async_lock_rejects_sync_context():
    lock = LoopBoundAsyncLock()

    with pytest.raises(RuntimeError, match="running event loop"):
        lock._lock_for_current_loop()


def test_loop_bound_async_lock_creates_independent_locks_per_event_loop():
    lock = LoopBoundAsyncLock()
    # Hold references to the actual lock objects rather than comparing id(): a
    # freed object's id() can be reused by the next allocation, which made the
    # earlier id()-based assertion fail deterministically on CPython.
    captured: list[asyncio.Lock] = []

    async def _acquire_once() -> None:
        async with lock:
            captured.append(lock._lock_for_current_loop())

    asyncio.run(_acquire_once())
    asyncio.run(_acquire_once())

    assert captured[0] is not captured[1]
