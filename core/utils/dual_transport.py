"""GET over whichever transport reaches a host from this machine: environment proxy or direct.

No single proxy setting works for every upstream here (see the per-host notes in
web/api/data.py::_defillama_get_json). Measured 2026-09-28 from the service host:

    hacked.slowmist.io   direct 1.5 s | proxy 12.6 s (the 6 s budget always failed)
    api.alternative.me   direct 1.6 s | proxy: first connect often refused
    api.coingecko.com    proxy  0.6 s | direct unavailable

``get`` tries the transport that last worked for the host (or the host's measured
default), then the other one within the same overall budget, and remembers the
winner. ``trust_env=True`` means "use the proxy from the process environment";
without one configured both attempts are simply direct.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlsplit

import httpx

from core.utils.shared_ssl import get_shared_ssl_context

PREFER_DIRECT_DEFAULT: Dict[str, bool] = {
    "hacked.slowmist.io": True,
    "api.alternative.me": True,
}
_prefer_direct: Dict[str, bool] = {}
MIN_ATTEMPT_SEC = 0.5


def preferred_direct(host: str) -> bool:
    host = host.lower()
    return _prefer_direct.get(host, PREFER_DIRECT_DEFAULT.get(host, False))


async def get(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout_sec: float = 8.0,
    follow_redirects: bool = True,
) -> httpx.Response:
    """GET ``url`` (raise_for_status applied); raises the last error if both transports fail."""
    host = (urlsplit(url).hostname or "").lower()
    first = preferred_direct(host)
    deadline = time.monotonic() + max(timeout_sec, MIN_ATTEMPT_SEC)
    last_error: Optional[BaseException] = None
    for attempt, direct in enumerate((first, not first)):
        remaining = deadline - time.monotonic()
        if remaining < MIN_ATTEMPT_SEC:
            break
        # Leave room for the fallback: the first attempt gets ~60% of the budget.
        attempt_timeout = remaining if attempt else max(MIN_ATTEMPT_SEC, remaining * 0.6)
        try:
            async with httpx.AsyncClient(
                timeout=attempt_timeout,
                headers=dict(headers or {}),
                follow_redirects=follow_redirects,
                trust_env=not direct,
                verify=get_shared_ssl_context(),
            ) as client:
                resp = await (client.get(url, params=dict(params)) if params else client.get(url))
                resp.raise_for_status()
            _prefer_direct[host] = direct
            return resp
        except Exception as exc:  # noqa: BLE001 - try the other transport
            last_error = exc
    raise last_error if last_error is not None else TimeoutError(f"no time left to fetch {host}")
