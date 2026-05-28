"""Tests for the ccxt.pro real-time market-data feed."""
from __future__ import annotations

import asyncio
import time

import pytest

from core.marketdata.ccxt_pro_feed import CcxtProMarketFeed


class _FakeProClient:
    """Minimal stand-in for a ccxt.pro exchange client.

    ``watch_tickers`` returns a queued batch each call, then blocks (so the
    feed loop behaves like a real quiet socket) until cancelled.
    """

    def __init__(self, batches):
        self._batches = list(batches)
        self.closed = False
        self.load_markets_called = False

    async def load_markets(self):
        self.load_markets_called = True
        return {}

    async def watch_tickers(self, symbols):
        if self._batches:
            return self._batches.pop(0)
        # Emulate a quiet market: block until the caller's wait_for times out
        # or the task is cancelled.
        await asyncio.sleep(3600)

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
