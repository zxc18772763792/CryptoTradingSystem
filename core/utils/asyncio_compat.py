from __future__ import annotations

import asyncio
from typing import Dict, Optional


class LoopBoundAsyncLock:
    """Async lock that is created lazily for the currently running event loop."""

    def __init__(self) -> None:
        self._locks: Dict[int, asyncio.Lock] = {}

    def _current_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    def _lock_for_current_loop(self) -> asyncio.Lock:
        loop = self._current_loop()
        if loop is None:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        key = id(loop)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def __aenter__(self) -> "LoopBoundAsyncLock":
        await self._lock_for_current_loop().acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._lock_for_current_loop().release()

    def locked(self) -> bool:
        return any(lock.locked() for lock in self._locks.values())
