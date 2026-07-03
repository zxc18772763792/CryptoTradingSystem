"""Process-wide shared TLS context for outbound httpx clients.

``httpx.AsyncClient(...)`` runs ``ssl.create_default_context()`` synchronously
in its constructor (CA-bundle read + parse from disk). Constructing a client
per request therefore does blocking disk I/O on the event loop — normally a
few tens of ms, but on 2026-07-02 a saturated disk stretched it to minutes and
froze the whole live service (py-spy: MainThread parked in
``ssl.py:create_default_context`` under a data-API fetcher, /health dead while
the port stayed bound).

Usage:
* ``await warm_shared_ssl_context()`` once at app startup (off-loop).
* ``httpx.AsyncClient(verify=get_shared_ssl_context(), ...)`` everywhere else —
  after the warmup this is a cached-object read, no I/O.

The first ``get_shared_ssl_context()`` call without a warmup still blocks once
(same cost as today's per-call behavior, paid a single time per process).
"""
from __future__ import annotations

import asyncio
import ssl
import threading
from typing import Optional

_SHARED_CONTEXT: Optional[ssl.SSLContext] = None
_LOCK = threading.Lock()


def get_shared_ssl_context() -> ssl.SSLContext:
    """Return the cached default TLS context, creating it on first use."""
    global _SHARED_CONTEXT
    if _SHARED_CONTEXT is None:
        with _LOCK:
            if _SHARED_CONTEXT is None:
                _SHARED_CONTEXT = ssl.create_default_context()
    return _SHARED_CONTEXT


async def warm_shared_ssl_context() -> ssl.SSLContext:
    """Create the shared context in a worker thread (startup warm-up)."""
    return await asyncio.to_thread(get_shared_ssl_context)
