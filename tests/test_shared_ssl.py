"""Shared TLS context: cached, thread-warmable, and actually an SSLContext."""
from __future__ import annotations

import asyncio
import ssl

import core.utils.shared_ssl as shared_ssl


def test_get_shared_ssl_context_is_cached_singleton(monkeypatch):
    monkeypatch.setattr(shared_ssl, "_SHARED_CONTEXT", None)
    calls = {"n": 0}
    real_create = ssl.create_default_context

    def counting_create(*args, **kwargs):
        calls["n"] += 1
        return real_create(*args, **kwargs)

    monkeypatch.setattr(shared_ssl.ssl, "create_default_context", counting_create)
    a = shared_ssl.get_shared_ssl_context()
    b = shared_ssl.get_shared_ssl_context()
    assert a is b
    assert isinstance(a, ssl.SSLContext)
    assert calls["n"] == 1, "context must be created exactly once per process"


def test_warm_shared_ssl_context_returns_same_instance(monkeypatch):
    monkeypatch.setattr(shared_ssl, "_SHARED_CONTEXT", None)
    warmed = asyncio.run(shared_ssl.warm_shared_ssl_context())
    assert warmed is shared_ssl.get_shared_ssl_context()
