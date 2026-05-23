"""Generic WebSocket client skeleton with reconnect/subscribe recovery.

EXPERIMENTAL — DO NOT USE IN LIVE TRADING
=========================================

This module is a *skeleton* placeholder. It does **not** establish a real
WebSocket connection, **does not** send/receive frames, and **does not**
guarantee message delivery, ordering, or heartbeat. Methods raise
``NotImplementedError`` when invoked to prevent silent integration in live
code paths.

If you need a working Binance perpetual WebSocket client, use
``core.marketdata.binance_perp_ws_client`` instead.
"""
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
    """Reusable WS loop skeleton (EXPERIMENTAL).

    All transport methods raise ``NotImplementedError`` until a concrete
    transport is wired up. Subclasses (or a future implementation here) must
    override:

    - :meth:`connect`
    - :meth:`disconnect`
    - :meth:`subscribe`
    - :meth:`run_forever`

    Until then, callers must not depend on this class in live trading.
    """

    def __init__(self, config: WSClientConfig):
        self.config = config
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._subscriptions: List[Dict[str, Any]] = []
        self._connected = False
        self._stop_event = asyncio.Event()
        logger.warning(
            f"{self.config.name}: WSClient is a SKELETON — not connected. "
            f"Use binance_perp_ws_client or a concrete subclass for live use."
        )

    def register_handler(self, channel: str, handler: MessageHandler) -> None:
        self._handlers.setdefault(channel, []).append(handler)

    async def connect(self) -> None:
        raise NotImplementedError(
            f"{self.config.name}: WSClient.connect() is a skeleton and not implemented; "
            f"do not use in live. Provide a concrete subclass or use "
            f"core.marketdata.binance_perp_ws_client."
        )

    async def disconnect(self) -> None:
        # Allow callers to flip the flag for cooperative shutdown of any
        # background loop they may have started themselves, but the skeleton
        # itself has no transport to close.
        self._stop_event.set()
        self._connected = False
        raise NotImplementedError(
            f"{self.config.name}: WSClient.disconnect() is a skeleton and not implemented; "
            f"do not use in live."
        )

    async def subscribe(self, payload: Dict[str, Any]) -> None:
        # Keep subscription replay buffer so future implementations can
        # restore state, but reject the actual subscribe call.
        self._subscriptions.append(dict(payload))
        raise NotImplementedError(
            f"{self.config.name}: WSClient.subscribe() is a skeleton and not implemented; "
            f"do not use in live."
        )

    async def run_forever(self) -> None:
        raise NotImplementedError(
            f"{self.config.name}: WSClient.run_forever() is a skeleton and not implemented; "
            f"do not use in live."
        )

    async def _dispatch(self, channel: str, message: Dict[str, Any]) -> None:
        for handler in self._handlers.get(channel, []):
            try:
                await handler(message)
            except Exception as e:
                logger.warning(f"{self.config.name}: handler error channel={channel}: {e}")
