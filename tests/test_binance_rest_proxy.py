from __future__ import annotations

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
