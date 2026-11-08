"""One-week risk reading for the radar: volatility decile per coin + how often that decile touched a level.

Built from ~650 local parquet files, so it runs in a worker thread (blocking the event loop stalls every module)
and is cached; the store only gains a new daily bar once a day. See core/research/altcoin_risk_meter.py.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

from fastapi import APIRouter

from core.research.altcoin_risk_meter import build_risk_meter

from .helpers import _utcnow

router = APIRouter()

_RISK_TTL_SEC = 6 * 3600.0
_RISK_RETRY_SEC = 300.0
_risk_cache: Dict[str, Any] = {"payload": None, "built_at": 0.0}
_risk_lock = asyncio.Lock()


@router.get("/radar/risk-meter")
async def get_risk_meter():
    async with _risk_lock:
        payload = _risk_cache["payload"]
        age = time.monotonic() - float(_risk_cache["built_at"])
        keep_for = _RISK_TTL_SEC if payload and payload.get("available") else _RISK_RETRY_SEC
        if payload is None or age >= keep_for:
            try:
                payload = await asyncio.to_thread(build_risk_meter)
            except Exception as exc:  # noqa: BLE001
                payload = {"available": False, "reason": f"{type(exc).__name__}: {str(exc)[:120]}"}
            _risk_cache.update(payload=payload, built_at=time.monotonic())
    return {**payload, "ts": _utcnow().isoformat()}
