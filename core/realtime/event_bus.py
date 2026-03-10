"""In-process realtime event bus for WebSocket fanout."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class RealtimeEvent:
    event: str
    payload: Dict[str, Any]
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }


class RealtimeEventBus:
    def __init__(self) -> None:
        self._subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def subscribe(self, maxsize: int = 200) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        async with self._lock:
            self._subscribers.append(q)
        return q

    async def unsubscribe(self, queue: asyncio.Queue) -> None:
        async with self._lock:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    async def publish(self, event: str, payload: Dict[str, Any] | None = None) -> None:
        envelope = RealtimeEvent(
            event=event,
            payload=payload or {},
            timestamp=datetime.now(timezone.utc).isoformat(),
        ).to_dict()

        async with self._lock:
            subscribers = list(self._subscribers)

        stale: List[asyncio.Queue] = []
        for q in subscribers:
            try:
                if q.full():
                    _ = q.get_nowait()
                q.put_nowait(envelope)
            except Exception:
                stale.append(q)

        if stale:
            async with self._lock:
                for q in stale:
                    if q in self._subscribers:
                        self._subscribers.remove(q)

    async def publish_nowait_safe(self, event: str, payload: Dict[str, Any] | None = None) -> None:
        """Best-effort publish that never raises to caller."""
        try:
            await self.publish(event=event, payload=payload)
        except Exception:
            return


event_bus = RealtimeEventBus()

