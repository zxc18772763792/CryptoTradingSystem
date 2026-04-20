from __future__ import annotations

import asyncio
from typing import Iterable, Optional, Tuple

from config.database import close_db, init_db
from core.data import data_storage
from core.exchanges.exchange_manager import exchange_manager
from core.news.storage import db as news_db


class RuntimeBootstrap:
    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None
        self._db_ready = False
        self._news_ready = False
        self._shared_exchange_names: Tuple[str, ...] = ()

    def _get_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._lock is None or self._lock_loop is not loop:
            self._lock = asyncio.Lock()
            self._lock_loop = loop
        return self._lock

    @staticmethod
    def _normalize_exchange_names(exchange_names: Optional[Iterable[str]]) -> Tuple[str, ...]:
        if not exchange_names:
            return ()
        names = [str(name or "").strip().lower() for name in exchange_names if str(name or "").strip()]
        return tuple(dict.fromkeys(names))

    async def initialize_shared_runtime(
        self,
        *,
        include_news: bool = True,
        initialize_exchanges: bool = True,
        exchange_names: Optional[Iterable[str]] = None,
    ) -> None:
        async with self._get_lock():
            if not self._db_ready:
                await init_db()
                self._db_ready = True
                data_storage.mark_db_initialized()

            await data_storage.initialize(ensure_db=False)

            if include_news and not self._news_ready:
                await news_db.init_news_db()
                self._news_ready = True

            requested = self._normalize_exchange_names(exchange_names)
            if initialize_exchanges and (not self._shared_exchange_names or requested != self._shared_exchange_names):
                await exchange_manager.initialize(list(requested) if requested else None)
                self._shared_exchange_names = requested

    async def shutdown_shared_runtime(
        self,
        *,
        include_news: bool = True,
        close_exchanges: bool = True,
        close_database: bool = True,
    ) -> None:
        async with self._get_lock():
            if close_exchanges:
                await exchange_manager.close_all()
                self._shared_exchange_names = ()

            await data_storage.close()

            if include_news and self._news_ready:
                await news_db.close_news_db()
                self._news_ready = False

            if close_database:
                await close_db()


runtime_bootstrap = RuntimeBootstrap()
