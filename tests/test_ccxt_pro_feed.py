"""Tests for the ccxt.pro real-time market-data feed."""
from __future__ import annotations

import asyncio
import time

import pytest

from core.marketdata.ccxt_pro_feed import CcxtProMarketFeed, _close_client_safely


class _FakeProClient:
    """Minimal stand-in for a ccxt.pro exchange client.

    ``watch_tickers`` returns a queued batch each call, then blocks (so the
    feed loop behaves like a real quiet socket) until cancelled.
    """

    def __init__(self, batches):
        self._batches = list(batches)
        self.closed = False
        self.load_markets_called = False
        self.watch_symbols = []

    async def load_markets(self):
        self.load_markets_called = True
        return {}

    async def watch_tickers(self, symbols):
        self.watch_symbols.append(list(symbols))
        if self._batches:
            return self._batches.pop(0)
        # Emulate a quiet market: block until the caller's wait_for times out
        # or the task is cancelled.
        await asyncio.sleep(3600)

    async def close(self):
        self.closed = True


class _FakeSession:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


def _make_feed(client, received, *, symbols=("BTC/USDT",)):
    async def on_tick(payload):
        received.append(payload)

    feed = CcxtProMarketFeed(
        on_tick=on_tick,
        symbols_provider=lambda: list(symbols),
        exchanges=["binance"],
        watch_timeout_sec=5.0,
        health_max_age_sec=5.0,
    )
    feed._build_client = lambda name: client  # type: ignore[method-assign]
    return feed


async def test_feed_pushes_normalized_ticks():
    client = _FakeProClient(
        [{"BTC/USDT": {"last": 50000.0, "bid": 49999.0, "ask": 50001.0, "timestamp": 1_700_000_000_000}}]
    )
    received: list = []
    feed = _make_feed(client, received)
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    # Give the loop time to deliver the first batch.
    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert received, "feed should have pushed at least one tick batch"
    payload = received[0]
    assert "binance" in payload
    assert payload["binance"]["BTC/USDT"]["last"] == 50000.0
    assert payload["binance"]["BTC/USDT"]["bid"] == 49999.0
    assert client.load_markets_called
    assert client.closed, "client should be closed on shutdown"


async def test_is_healthy_reflects_recent_push():
    client = _FakeProClient([{"BTC/USDT": {"last": 100.0, "timestamp": 1_700_000_000_000}}])
    received: list = []
    feed = _make_feed(client, received)
    stop = asyncio.Event()

    assert feed.is_healthy() is False  # nothing pushed yet

    task = asyncio.create_task(feed.run(stop))
    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)

    assert feed.is_healthy() is True
    assert "binance" in feed.healthy_exchanges()

    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    # Stale once the freshness window passes.
    feed._last_push_monotonic["binance"] = time.monotonic() - 999
    assert feed.is_healthy() is False


async def test_status_snapshot_records_watch_success():
    client = _FakeProClient([{"BTC/USDT": {"last": 100.0, "timestamp": 1_700_000_000_000}}])
    received: list = []
    feed = _make_feed(client, received)
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    status = feed.status_snapshot()
    assert status["watch_attempt_count"] >= 1
    assert status["watch_timeout_count"] == 0
    assert status["watch_error_count"] == 0
    assert status["last_error"] is None
    assert status["exchanges"]["binance"]["last_symbols"] == ["BTC/USDT"]
    assert status["exchanges"]["binance"]["last_payload_symbol_count"] == 1
    assert status["exchanges"]["binance"]["last_watch_started_at"]
    assert status["exchanges"]["binance"]["last_watch_completed_at"]


async def test_status_snapshot_records_watch_timeout():
    client = _FakeProClient([])
    received: list = []
    feed = _make_feed(client, received)
    feed._watch_timeout_sec = 0.01
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    for _ in range(100):
        if feed.status_snapshot()["watch_timeout_count"] >= 1:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    status = feed.status_snapshot()
    assert received == []
    assert status["watch_attempt_count"] >= 1
    assert status["watch_timeout_count"] >= 1
    assert status["exchanges"]["binance"]["last_symbols"] == ["BTC/USDT"]
    assert status["exchanges"]["binance"]["last_watch_timeout_at"]


async def test_status_snapshot_records_watch_error():
    class _BrokenClient(_FakeProClient):
        async def watch_tickers(self, symbols):
            self.watch_symbols.append(list(symbols))
            raise RuntimeError("socket dropped")

    client = _BrokenClient([])
    received: list = []
    feed = _make_feed(client, received)
    feed._reconnect_min_sec = 0.01
    feed._reconnect_max_sec = 0.02
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    for _ in range(100):
        if feed.status_snapshot()["watch_error_count"] >= 1:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    status = feed.status_snapshot()
    assert received == []
    assert status["watch_error_count"] >= 1
    assert "RuntimeError: socket dropped" in status["last_error"]
    assert status["exchanges"]["binance"]["last_error"] == status["last_error"]


async def test_normalize_skips_zero_and_nonnumeric():
    out = CcxtProMarketFeed._normalize_tickers(
        "binance",
        {
            "BTC/USDT": {"last": 0.0},          # zero -> skip
            "ETH/USDT": {"last": "abc"},        # non-numeric -> skip
            "SOL/USDT": {"last": 150.5, "bid": 150.4, "ask": 150.6},
            "JUNK": "not-a-dict",               # wrong type -> skip
        },
    )
    assert set(out.keys()) == {"SOL/USDT"}
    assert out["SOL/USDT"]["last"] == 150.5
    assert "timestamp" in out["SOL/USDT"]


async def test_normalize_strips_perp_settlement_suffix():
    """Futures symbols like BTC/USDT:USDT must map back to the UI's BTC/USDT key."""
    out = CcxtProMarketFeed._normalize_tickers(
        "binance",
        {
            "BTC/USDT:USDT": {"last": 50000.0, "timestamp": 1_700_000_000_000},
            "ETH/USDT": {"last": 2000.0, "timestamp": 1_700_000_000_000},
        },
    )
    assert set(out.keys()) == {"BTC/USDT", "ETH/USDT"}
    assert out["BTC/USDT"]["last"] == 50000.0


def test_binance_rest_future_maps_to_ccxt_pro_swap():
    assert CcxtProMarketFeed._ws_default_type("binance", "future") == "swap"
    assert CcxtProMarketFeed._ws_default_type("binance", "futures") == "swap"
    assert CcxtProMarketFeed._ws_default_type("binance", "perpetual") == "swap"
    assert CcxtProMarketFeed._ws_default_type("binance", "spot") == "spot"
    assert CcxtProMarketFeed._ws_default_type("okx", "future") == "future"


async def test_watch_error_triggers_reconnect_then_recovers():
    class _FlakyClient(_FakeProClient):
        def __init__(self):
            super().__init__([])
            self._calls = 0

        async def watch_tickers(self, symbols):
            self._calls += 1
            if self._calls == 1:
                raise RuntimeError("socket dropped")
            if self._calls == 2:
                return {"BTC/USDT": {"last": 42.0, "timestamp": 1_700_000_000_000}}
            await asyncio.sleep(3600)

    client = _FlakyClient()
    received: list = []
    feed = _make_feed(client, received)
    # Shrink backoff so the test is fast.
    feed._reconnect_min_sec = 0.01
    feed._reconnect_max_sec = 0.02
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    for _ in range(100):
        if received:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert received, "feed should recover and push after a transient error"
    assert received[0]["binance"]["BTC/USDT"]["last"] == 42.0


async def test_watch_error_closes_old_client_before_reconnect():
    class _BrokenThenHealthyClient(_FakeProClient):
        def __init__(self, *, broken: bool):
            super().__init__([{"BTC/USDT": {"last": 42.0, "timestamp": 1_700_000_000_000}}])
            self._broken = broken

        async def watch_tickers(self, symbols):
            self.watch_symbols.append(list(symbols))
            if self._broken:
                raise RuntimeError("socket dropped")
            return await super().watch_tickers(symbols)

    broken_client = _BrokenThenHealthyClient(broken=True)
    healthy_client = _BrokenThenHealthyClient(broken=False)
    clients = [broken_client, healthy_client]
    received: list = []
    feed = _make_feed(clients[0], received)
    feed._build_client = lambda name: clients.pop(0)  # type: ignore[method-assign]
    feed._reconnect_min_sec = 0.01
    feed._reconnect_max_sec = 0.02
    stop = asyncio.Event()

    task = asyncio.create_task(feed.run(stop))
    for _ in range(100):
        if received:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert received
    assert clients == []
    assert broken_client.closed is True
    assert healthy_client.closed is True
    assert feed.status_snapshot()["watch_error_count"] >= 1


async def test_close_client_safely_closes_session_when_client_close_fails():
    class _LeakyClient:
        def __init__(self):
            self.session = _FakeSession()

        async def close(self):
            raise RuntimeError("close failed before session cleanup")

    client = _LeakyClient()

    await _close_client_safely(client)

    assert client.session.closed is True


async def test_run_noops_without_exchanges():
    received: list = []

    async def on_tick(payload):
        received.append(payload)

    feed = CcxtProMarketFeed(
        on_tick=on_tick,
        symbols_provider=lambda: ["BTC/USDT"],
        exchanges=[],
    )
    stop = asyncio.Event()
    # Should return promptly without raising.
    await asyncio.wait_for(feed.run(stop), timeout=5.0)
    assert received == []
