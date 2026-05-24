"""Performance fixes (round 2): MLXGBoost booster cache + cex_arbitrage parallel."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _install_fake_xgboost(monkeypatch, booster_sequence):
    """Inject a fake xgboost module so _load_xgb_booster_cached can import it."""
    import sys
    booster_iter = iter(booster_sequence)

    class _FakeBooster:
        def __init__(self):
            self._handle = next(booster_iter)

        def load_model(self, path: str) -> None:  # noqa: D401
            self._loaded_from = path

    fake_xgb = SimpleNamespace(Booster=lambda: SimpleNamespace(
        _handle=next(booster_iter),
        load_model=lambda p: None,
    ))
    monkeypatch.setitem(sys.modules, "xgboost", fake_xgb)
    return fake_xgb


def test_xgb_booster_cache_returns_same_instance(tmp_path, monkeypatch):
    """Same model_path + mtime should return the same Booster instance."""
    from core.research import strategy_research

    fake_path = tmp_path / "fake_model.json"
    fake_path.write_text("{}", encoding="utf-8")
    strategy_research._XGB_BOOSTER_CACHE.clear()

    # Inject a fake xgboost with a single Booster instance.
    _install_fake_xgboost(monkeypatch, ["only"])

    b1 = strategy_research._load_xgb_booster_cached(str(fake_path))
    b2 = strategy_research._load_xgb_booster_cached(str(fake_path))
    assert b1 is b2, "Booster cache should return the same instance on repeated lookup"


def test_xgb_booster_cache_invalidates_on_mtime_change(tmp_path, monkeypatch):
    """A different mtime for the same path should trigger reload + evict the stale entry."""
    from core.research import strategy_research
    import os
    from pathlib import Path

    fake_path = tmp_path / "model_v.json"
    fake_path.write_text("{}", encoding="utf-8")
    strategy_research._XGB_BOOSTER_CACHE.clear()

    _install_fake_xgboost(monkeypatch, ["v1", "v2"])

    b1 = strategy_research._load_xgb_booster_cached(str(fake_path))
    # Bump mtime so the cache key differs.
    new_mtime = fake_path.stat().st_mtime + 5
    os.utime(fake_path, (new_mtime, new_mtime))
    b2 = strategy_research._load_xgb_booster_cached(str(fake_path))

    assert b1 is not b2
    resolved = str(Path(fake_path).resolve())
    matching = [k for k in strategy_research._XGB_BOOSTER_CACHE if k[0] == resolved]
    assert len(matching) == 1, "stale entry should be evicted when mtime changes"


def test_cex_arbitrage_update_prices_runs_concurrent():
    """update_prices should issue get_ticker calls concurrently via asyncio.gather."""
    from strategies.arbitrage.cex_arbitrage import CEXArbitrageStrategy

    call_order: list[str] = []

    async def slow_ticker(symbol: str):
        call_order.append("start")
        await asyncio.sleep(0.05)
        call_order.append("end")
        return SimpleNamespace(bid=100.0, ask=101.0, last=100.5)

    connectors = {}
    for name in ("binance", "okx", "bybit"):
        c = MagicMock()
        c.is_connected = True
        c.get_ticker = AsyncMock(side_effect=slow_ticker)
        connectors[name] = c

    strategy = CEXArbitrageStrategy()
    strategy.params = {"exchanges": list(connectors.keys())}

    import core.exchanges
    with patch.object(
        core.exchanges.exchange_manager,
        "get_exchange",
        side_effect=lambda name: connectors.get(name),
    ):
        import time
        t0 = time.monotonic()
        prices = asyncio.run(strategy.update_prices("BTC/USDT"))
        elapsed = time.monotonic() - t0

    assert set(prices.keys()) == {"binance", "okx", "bybit"}
    # 3 sequential calls at 0.05s each = 0.15s. Parallel should be ~0.05s.
    assert elapsed < 0.12, f"expected concurrent execution (~0.05s), got {elapsed:.3f}s"
    # All three started before any finished -> proves parallelism.
    starts = [i for i, e in enumerate(call_order) if e == "start"]
    ends = [i for i, e in enumerate(call_order) if e == "end"]
    assert max(starts) < min(ends), "all tickers should start before any finishes"


def test_cex_arbitrage_isolates_single_exchange_failure():
    """One exchange raising should not poison the other results."""
    from strategies.arbitrage.cex_arbitrage import CEXArbitrageStrategy

    async def ok_ticker(symbol: str):
        return SimpleNamespace(bid=100.0, ask=101.0, last=100.5)

    async def bad_ticker(symbol: str):
        raise RuntimeError("connection refused")

    good = MagicMock(); good.is_connected = True
    good.get_ticker = AsyncMock(side_effect=ok_ticker)
    bad = MagicMock(); bad.is_connected = True
    bad.get_ticker = AsyncMock(side_effect=bad_ticker)

    strategy = CEXArbitrageStrategy()
    strategy.params = {"exchanges": ["binance", "okx"]}

    import core.exchanges
    with patch.object(
        core.exchanges.exchange_manager,
        "get_exchange",
        side_effect=lambda name: {"binance": good, "okx": bad}[name],
    ):
        prices = asyncio.run(strategy.update_prices("BTC/USDT"))

    assert "binance" in prices
    assert "okx" not in prices  # failure isolated
