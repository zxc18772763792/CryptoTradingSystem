from __future__ import annotations

import asyncio
import threading
from weakref import WeakKeyDictionary


class LoopBoundAsyncLock:
    """Async lock that is created lazily for the currently running event loop."""

    def __init__(self) -> None:
        self._locks: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]" = WeakKeyDictionary()
        self._guard = threading.Lock()

    def _lock_for_current_loop(self) -> asyncio.Lock:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError("LoopBoundAsyncLock requires a running event loop") from exc

        with self._guard:
            lock = self._locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[loop] = lock
            return lock

    async def __aenter__(self) -> "LoopBoundAsyncLock":
        await self._lock_for_current_loop().acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._lock_for_current_loop().release()

    def locked(self) -> bool:
        with self._guard:
            return any(lock.locked() for lock in self._locks.values())
