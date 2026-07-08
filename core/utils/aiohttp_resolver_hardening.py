"""Force aiohttp onto the OS (getaddrinfo) DNS resolver.

Root cause (diagnosed live 2026-07-08): when ``aiodns`` is installed, aiohttp
defaults to ``AsyncResolver`` (c-ares). On this host the system's primary DNS
server is a broken link-local entry (``fe80::1``); c-ares queries it and fails
with ``Could not contact DNS servers``, while the OS resolver (``getaddrinfo``,
the path ``curl``/``nslookup`` use) falls back to a working server and resolves
fine. The result: EVERY outbound aiohttp call silently starved — CoinGlass
derivatives/chain data went stale for weeks, news collectors failed, the
operating-mode source-health probe timed out, and the altcoin radar showed
degraded/stale rows even though the venues were reachable.

``ThreadedResolver`` runs ``loop.getaddrinfo`` in a thread, so it inherits the
same working OS resolution path as curl. This installs it as aiohttp's default
process-wide, before any connector/session is created.
"""
from __future__ import annotations

from loguru import logger

_INSTALLED = False


def install_aiohttp_threaded_resolver() -> bool:
    """Swap aiohttp's DefaultResolver to ThreadedResolver. Returns True if active.

    Idempotent and best-effort: a failure here must never block startup, and if
    aiohttp already defaults to the threaded resolver this is a no-op success.
    """
    global _INSTALLED
    if _INSTALLED:
        return True
    try:
        import aiohttp.connector as _connector
        import aiohttp.resolver as _resolver

        threaded = _resolver.ThreadedResolver
        # ``TCPConnector`` reads DefaultResolver from the connector module
        # namespace; patch both it and the resolver module for good measure.
        current = getattr(_connector, "DefaultResolver", None)
        if current is threaded:
            _INSTALLED = True
            return True
        _resolver.DefaultResolver = threaded
        _connector.DefaultResolver = threaded
        _INSTALLED = True
        logger.info(
            "aiohttp resolver hardening: DefaultResolver {} -> ThreadedResolver "
            "(outbound DNS now uses the OS getaddrinfo path, avoiding a broken "
            "c-ares/link-local resolver)",
            getattr(current, "__name__", current),
        )
        return True
    except Exception as exc:  # pragma: no cover - hardening must never break boot
        logger.warning("aiohttp resolver hardening failed to install: {}", exc)
        return False
