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

``settings.PROXY_BYPASS_HOSTS`` lists upstreams that work without the (metered) proxy;
it is merged into ``NO_PROXY`` so every trust_env client goes direct for them, and
``bypasses_proxy`` lets clients that pass an explicit proxy (ccxt) do the same.
"""
from __future__ import annotations

import os
from typing import Any, List
from urllib.parse import urlsplit
from urllib.request import proxy_bypass_environment

_LOCAL = "localhost,127.0.0.1,::1"


def _host_list(*values: Any) -> List[str]:
    hosts: List[str] = []
    seen = set()
    for value in values:
        for item in str(value or "").replace(";", ",").split(","):
            host = item.strip()
            if host and host.lower() not in seen:
                seen.add(host.lower())
                hosts.append(host)
    return hosts


def ensure_proxy_env(settings_obj: Any) -> List[str]:
    """Export HTTP(S)_PROXY from settings when absent and merge PROXY_BYPASS_HOSTS into NO_PROXY.

    Returns the variable names set.
    """
    applied: List[str] = []
    for name in ("HTTP_PROXY", "HTTPS_PROXY"):
        value = str(getattr(settings_obj, name, None) or "").strip()
        if value and not (os.environ.get(name) or os.environ.get(name.lower())):
            os.environ[name] = value
            applied.append(name)
    configured = str(getattr(settings_obj, "PROXY_BYPASS_HOSTS", None) or "").strip()
    if applied or configured:
        current = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
        # never proxy the local web service or Redis; keep whatever was inherited
        merged = ",".join(_host_list(current, _LOCAL, configured))
        if merged != current:
            for key in ("NO_PROXY", "no_proxy"):
                if key == "NO_PROXY" or key in os.environ:
                    os.environ[key] = merged
            applied.append("NO_PROXY")
    return applied


def bypasses_proxy(url: str) -> bool:
    """True when NO_PROXY covers the URL's host (domain-suffix match, as the HTTP clients apply it)."""
    text = str(url or "").strip()
    host = urlsplit(text).hostname if "://" in text else text
    no_proxy = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    return bool(host and no_proxy) and bool(proxy_bypass_environment(host, {"no": no_proxy}))
