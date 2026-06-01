"""Tests for the optional mark-price / funding WS stream in CcxtProMarketFeed."""
from __future__ import annotations

import asyncio


from core.marketdata.ccxt_pro_feed import CcxtProMarketFeed


class _FakeProClient:
    def __init__(self, mark_batches):
        self._mark_batches = list(mark_batches)
        self.closed = False
        self.markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT:USDT"}}
        self.currencies = {}
        self.load_markets_called = False
        self.mark_symbols = []
        self.set_markets_calls = []

    async def load_markets(self):
        self.load_markets_called = True
        return {}

    def set_markets(self, markets, currencies=None):
        self.set_markets_calls.append((markets, currencies))
        self.markets = markets

    async def watch_tickers(self, symbols):
        await asyncio.sleep(3600)  # no ticker pushes in these tests

    async def watch_mark_prices(self, symbols):
        self.mark_symbols.append(list(symbols))
        if self._mark_batches:
            return self._mark_batches.pop(0)
        await asyncio.sleep(3600)

    async def close(self):
        self.closed = True


def _make_feed(client, *, watch_mark_prices, mark_received, tmp_path, symbols=("BTC/USDT",)):
    async def on_tick(payload):
        pass

    async def on_mark(payload):
        mark_received.append(payload)

    feed = CcxtProMarketFeed(
        on_tick=on_tick,
        symbols_provider=lambda: list(symbols),
        exchanges=["binance"],
        watch_timeout_sec=5.0,
        health_max_age_sec=5.0,
        watch_mark_prices=watch_mark_prices,
        on_mark=on_mark,
    )
    feed._build_client = lambda name: client  # type: ignore[method-assign]
    feed._market_cache_dir = lambda: tmp_path  # type: ignore[method-assign]
    return feed


# ── pure helper / normalizer tests ──────────────────────────────────────────
def test_to_perp_symbol():
    assert CcxtProMarketFeed._to_perp_symbol("BTC/USDT") == "BTC/USDT:USDT"
    assert CcxtProMarketFeed._to_perp_symbol("ETH/USDC") == "ETH/USDC:USDC"
    assert CcxtProMarketFeed._to_perp_symbol("BTC/USDT:USDT") == "BTC/USDT:USDT"
    assert CcxtProMarketFeed._to_perp_symbol("") == ""


def test_normalize_mark_prices_from_raw_info():
    payload = {
        "BTC/USDT:USDT": {
            "info": {"s": "BTCUSDT", "p": "50000.5", "i": "49990.0", "r": "0.0001",
                     "T": 1700000000000, "E": 1699999999000}
        }
    }
    out = CcxtProMarketFeed._normalize_mark_prices("binance", payload)
    assert set(out.keys()) == {"BTC/USDT"}          # perp suffix stripped to UI key
    e = out["BTC/USDT"]
    assert e["mark"] == 50000.5
    assert e["index"] == 49990.0
    assert e["funding_rate"] == 0.0001
    assert e["next_funding_time"] == 1700000000000
    assert "timestamp" in e


def test_normalize_mark_prices_from_unified_fields():
    payload = {
        "ETH/USDT:USDT": {
            "markPrice": 3000.0, "indexPrice": 2999.0, "fundingRate": -0.00005,
            "fundingTimestamp": 1700000000000, "timestamp": 1699999999000,
        }
    }
    out = CcxtProMarketFeed._normalize_mark_prices("binance", payload)
    assert out["ETH/USDT"]["mark"] == 3000.0
    assert out["ETH/USDT"]["funding_rate"] == -0.00005


def test_normalize_mark_prices_skips_nonpositive_and_nondict():
    payload = {"BTC/USDT:USDT": {"markPrice": 0.0}, "JUNK": "not-a-dict"}
    assert CcxtProMarketFeed._normalize_mark_prices("binance", payload) == {}


# ── stream behaviour tests ──────────────────────────────────────────────────
async def test_mark_stream_off_by_default(tmp_path):
    client = _FakeProClient([
        {"BTC/USDT:USDT": {"info": {"s": "BTCUSDT", "p": "50000", "r": "0.0001"}}}
    ])
    mark_received: list = []
    feed = _make_feed(client, watch_mark_prices=False, mark_received=mark_received, tmp_path=tmp_path)
    stop = asyncio.Event()
    task = asyncio.create_task(feed.run(stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)
    assert mark_received == []
    assert client.mark_symbols == []          # mark loop never ran
    assert feed.status_snapshot()["exchanges"]["binance"]["mark_enabled"] is False


async def test_mark_stream_pushes_when_enabled(tmp_path):
    client = _FakeProClient([
        {"BTC/USDT:USDT": {"info": {"s": "BTCUSDT", "p": "50000.5", "i": "49990",
                                    "r": "0.0001", "T": 1700000000000, "E": 1699999999000}}}
    ])
    mark_received: list = []
    feed = _make_feed(client, watch_mark_prices=True, mark_received=mark_received, tmp_path=tmp_path)
    stop = asyncio.Event()
    task = asyncio.create_task(feed.run(stop))
    for _ in range(100):
        if mark_received:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert mark_received, "mark stream should push a normalized payload"
    payload = mark_received[0]
    assert "binance" in payload
    assert payload["binance"]["BTC/USDT"]["mark"] == 50000.5
    assert payload["binance"]["BTC/USDT"]["funding_rate"] == 0.0001
    assert client.mark_symbols and client.mark_symbols[0] == ["BTC/USDT:USDT"]
    status = feed.status_snapshot()["exchanges"]["binance"]
    assert status["mark_enabled"] is True
    assert status["mark_last_payload_symbol_count"] == 1
