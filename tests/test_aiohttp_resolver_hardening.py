"""Tests for the aiohttp DNS resolver hardening.

Live incident (2026-07-08): aiodns made aiohttp default to AsyncResolver
(c-ares), which queried a broken link-local DNS server (fe80::1) and failed
every outbound call, starving CoinGlass/news/source-health for weeks. The fix
forces aiohttp's DefaultResolver to ThreadedResolver (OS getaddrinfo) at
startup. These tests pin that the install swaps both module namespaces and is
idempotent, without touching the network.
"""
from __future__ import annotations

import aiohttp.connector as _connector
import aiohttp.resolver as _resolver

import core.utils.aiohttp_resolver_hardening as hardening
from core.utils.aiohttp_resolver_hardening import install_aiohttp_threaded_resolver


def _reset_state(connector_default, resolver_default):
    hardening._INSTALLED = False
    _connector.DefaultResolver = connector_default
    _resolver.DefaultResolver = resolver_default


def test_install_swaps_both_namespaces_to_threaded_resolver():
    orig_conn = _connector.DefaultResolver
    orig_res = _resolver.DefaultResolver
    try:
        # Simulate the broken default: pretend the c-ares resolver is active.
        class _FakeAsyncResolver:
            pass

        hardening._INSTALLED = False
        _connector.DefaultResolver = _FakeAsyncResolver
        _resolver.DefaultResolver = _FakeAsyncResolver

        assert install_aiohttp_threaded_resolver() is True
        assert _connector.DefaultResolver is _resolver.ThreadedResolver
        assert _resolver.DefaultResolver is _resolver.ThreadedResolver
    finally:
        _reset_state(orig_conn, orig_res)


def test_install_is_idempotent_and_noops_when_already_threaded():
    orig_conn = _connector.DefaultResolver
    orig_res = _resolver.DefaultResolver
    try:
        hardening._INSTALLED = False
        _connector.DefaultResolver = _resolver.ThreadedResolver
        _resolver.DefaultResolver = _resolver.ThreadedResolver

        # Already threaded -> no-op success, still the threaded resolver.
        assert install_aiohttp_threaded_resolver() is True
        assert _connector.DefaultResolver is _resolver.ThreadedResolver

        # A second call short-circuits on the _INSTALLED flag.
        assert install_aiohttp_threaded_resolver() is True
    finally:
        _reset_state(orig_conn, orig_res)


def test_default_tcp_connector_uses_threaded_resolver_after_install():
    """A freshly created TCPConnector must pick up the threaded resolver."""
    import asyncio

    orig_conn = _connector.DefaultResolver
    orig_res = _resolver.DefaultResolver

    async def _make_and_check():
        conn = _connector.TCPConnector()
        try:
            assert isinstance(conn._resolver, _resolver.ThreadedResolver)
        finally:
            await conn.close()

    try:
        hardening._INSTALLED = False

        class _FakeAsyncResolver:
            pass

        _connector.DefaultResolver = _FakeAsyncResolver
        _resolver.DefaultResolver = _FakeAsyncResolver

        assert install_aiohttp_threaded_resolver() is True
        asyncio.run(_make_and_check())
    finally:
        _reset_state(orig_conn, orig_res)
