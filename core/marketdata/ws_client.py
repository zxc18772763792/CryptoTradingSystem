"""Generic WebSocket client skeleton with reconnect/subscribe recovery."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from loguru import logger


MessageHandler = Callable[[Dict[str, Any]], Awaitable[None]]


@dataclass
class WSClientConfig:
    url: str
    ping_interval_sec: int = 20
    ping_timeout_sec: int = 10
    reconnect_min_sec: float = 1.0
    reconnect_max_sec: float = 30.0
    max_queue: int = 10000
    name: str = "ws_client"


class WSClient:
    """Reusable WS loop skeleton.

    TODO:
    - implement websockets/aiohttp transport
    - heartbeat
    - subscription replay after reconnect
    """

    def __init__(self, config: WSClientConfig):
        self.config = config
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._subscriptions: List[Dict[str, Any]] = []
        self._connected = False
        self._stop_event = asyncio.Event()

    def register_handler(self, channel: str, handler: MessageHandler) -> None:
        self._handlers.setdefault(channel, []).append(handler)

    async def connect(self) -> None:
        logger.debug(f"{self.config.name}: connect() TODO url={self.config.url}")
        self._connected = True

    async def disconnect(self) -> None:
        self._stop_event.set()
        self._connected = False

    async def subscribe(self, payload: Dict[str, Any]) -> None:
        self._subscriptions.append(dict(payload))
        if self._connected:
            logger.debug(f"{self.config.name}: subscribe TODO payload={payload}")

    async def run_forever(self) -> None:
        logger.info(f"{self.config.name}: run_forever() skeleton started")
        while not self._stop_event.is_set():
            await asyncio.sleep(1)

    async def _dispatch(self, channel: str, message: Dict[str, Any]) -> None:
        for handler in self._handlers.get(channel, []):
            try:
                await handler(message)
            except Exception as e:
                logger.warning(f"{self.config.name}: handler error channel={channel}: {e}")

