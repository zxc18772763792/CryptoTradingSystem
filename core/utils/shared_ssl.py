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


# Optional modules that httpx/httpcore try to import on hot paths. httpcore's
# ``current_async_library()`` does ``import sniffio`` inside a try/except on
# every connection/lock operation; when it is not installed each attempt walks
# sys.path on disk, on the event loop (seen as multi-second loop stalls in the
# research workbench under disk contention).
_OPTIONAL_HOT_PATH_MODULES = ("sniffio",)


def negative_cache_missing_optional_modules(names=_OPTIONAL_HOT_PATH_MODULES) -> list:
    """Make ``import <name>`` fail fast for optional modules that are absent.

    A ``None`` entry in ``sys.modules`` makes the import system raise
    ImportError immediately — the same outcome as today, minus the path scan.
    Installed modules are left untouched. Returns the names that were cached.
    """
    import importlib.util
    import sys

    cached = []
    for name in names:
        if name in sys.modules:
            continue
        try:
            missing = importlib.util.find_spec(name) is None
        except (ImportError, ValueError):
            missing = False
        if missing:
            sys.modules[name] = None
            cached.append(name)
    return cached
