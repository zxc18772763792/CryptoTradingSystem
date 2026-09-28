"""Make the process's proxy environment deterministic.

HTTP clients created with ``trust_env=True`` (httpx, aiohttp, requests) read the
proxy from ``os.environ``. The long-running processes are started by the
task-hosted supervisor, so they inherit whatever proxy variables the supervisor
had when *it* started. On 2026-09-28 the Windows user environment no longer held
any, while the supervisor (started 2026-09-16) still passed its old ones on: a
supervisor restart would have silently switched every trust_env client to direct
connections and broken the upstreams that need the proxy (CoinGecko).

``ensure_proxy_env`` exports the proxy configured in ``.env`` (settings) when the
process has none, so behavior no longer depends on how the process was launched.
An explicitly set environment always wins. Hosts that are faster direct use
core/utils/dual_transport, which does not depend on this choice.
"""
from __future__ import annotations

import os
from typing import Any, List

_LOCAL = "localhost,127.0.0.1,::1"


def ensure_proxy_env(settings_obj: Any) -> List[str]:
    """Export HTTP(S)_PROXY from settings when absent; returns the variable names set."""
    applied: List[str] = []
    for name in ("HTTP_PROXY", "HTTPS_PROXY"):
        value = str(getattr(settings_obj, name, None) or "").strip()
        if value and not (os.environ.get(name) or os.environ.get(name.lower())):
            os.environ[name] = value
            applied.append(name)
    if applied and not (os.environ.get("NO_PROXY") or os.environ.get("no_proxy")):
        os.environ["NO_PROXY"] = _LOCAL  # never proxy the local web service or Redis
        applied.append("NO_PROXY")
    return applied
