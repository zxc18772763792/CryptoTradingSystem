from __future__ import annotations

import asyncio
import time
import weakref

import pytest

from core.trading import binance_rest


def test_apply_httpx_proxy_kw_uses_proxy_keyword(monkeypatch):
    class _AsyncClient:
        def __init__(self, *, proxy=None, timeout=None):
            pass

    monkeypatch.setattr(binance_rest.httpx, "AsyncClient", _AsyncClient)

    kwargs = {"timeout": 5.0}
    binance_rest._apply_httpx_proxy_kw(kwargs, "http://127.0.0.1:7890")

    assert kwargs == {"timeout": 5.0, "proxy": "http://127.0.0.1:7890"}


def test_apply_httpx_proxy_kw_uses_legacy_proxies_keyword(monkeypatch):
    class _AsyncClient:
        def __init__(self, *, proxies=None, timeout=None):
            pass

    monkeypatch.setattr(binance_rest.httpx, "AsyncClient", _AsyncClient)

    kwargs = {"timeout": 5.0}
    binance_rest._apply_httpx_proxy_kw(kwargs, "http://127.0.0.1:7890")

    assert kwargs == {"timeout": 5.0, "proxies": "http://127.0.0.1:7890"}


def test_apply_httpx_proxy_kw_falls_back_to_env_proxy(monkeypatch):
    class _AsyncClient:
        def __init__(self, *, timeout=None, trust_env=True):
            pass

    monkeypatch.setattr(binance_rest.httpx, "AsyncClient", _AsyncClient)

    kwargs = {"timeout": 5.0}
    binance_rest._apply_httpx_proxy_kw(kwargs, "http://127.0.0.1:7890")

    assert kwargs == {"timeout": 5.0, "trust_env": True}


@pytest.mark.asyncio
async def test_binance_signed_request_serializes_time_offset_refresh(monkeypatch):
    calls: list[str] = []

    class _Response:
        status_code = 200
        text = ""
        request = object()

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class _AsyncClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url, params=None):
            calls.append(url)
            if url.endswith("/time"):
                await asyncio.sleep(0.01)
                return _Response({"serverTime": int(time.time() * 1000) + 1234})
            return _Response({"ok": True})

    monkeypatch.setattr(binance_rest.httpx, "AsyncClient", _AsyncClient)
    monkeypatch.setattr(
        binance_rest,
        "_binance_credentials",
        lambda account_id=None: {"api_key": "key", "api_secret": "secret", "proxy": ""},
    )
    monkeypatch.setattr(binance_rest, "_BINANCE_TIME_OFFSET_LOCKS", weakref.WeakKeyDictionary())
    monkeypatch.setitem(binance_rest._BINANCE_TIME_OFFSET_MS, "api", 0)
    monkeypatch.setitem(binance_rest._BINANCE_TIME_OFFSET_MS, "fapi", 0)
    monkeypatch.setitem(binance_rest._BINANCE_TIME_OFFSET_MS, "ts", 0.0)

    results = await asyncio.gather(
        *[
            binance_rest.binance_signed_request("GET", "/api/v3/account")
            for _ in range(3)
        ]
    )

    assert results == [{"ok": True}, {"ok": True}, {"ok": True}]
    assert calls.count("https://api.binance.com/api/v3/time") == 1
    assert calls.count("https://fapi.binance.com/fapi/v1/time") == 1
    assert calls.count("https://api.binance.com/api/v3/account") == 3


def test_binance_time_offset_refresh_lock_is_scoped_per_event_loop(monkeypatch):
    monkeypatch.setattr(binance_rest, "_BINANCE_TIME_OFFSET_LOCKS", weakref.WeakKeyDictionary())

    async def contend_for_lock():
        lock = binance_rest._get_binance_time_offset_lock()
        entered = asyncio.Event()

        async def holder():
            async with lock:
                entered.set()
                await asyncio.sleep(0.01)

        async def waiter():
            await entered.wait()
            async with lock:
                return None

        await asyncio.gather(holder(), waiter())
        return lock

    first = asyncio.run(contend_for_lock())
    second = asyncio.run(contend_for_lock())

    assert first is not second
