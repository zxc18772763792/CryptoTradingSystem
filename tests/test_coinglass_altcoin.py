from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from core.data import coinglass_altcoin


def test_build_exchange_altcoin_universe_uses_stale_disk_cache_when_refresh_times_out(
    monkeypatch,
    tmp_path,
):
    stale_payload = {
        "exchange": "binance",
        "symbols": ["LINK/USDT", "AAVE/USDT"],
        "count": 2,
        "source": "coinglass_altcoin_universe",
        "board_count": 1,
        "boards": [{"board": "defi", "market_cap_top2": ["LINK/USDT"], "onboard_top2": ["AAVE/USDT"]}],
        "updated_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
    }
    cache_path = tmp_path / "altcoin_universe_binance.json"
    cache_path.write_text(json.dumps(stale_payload, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_CACHE", {})
    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_REFRESH_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(coinglass_altcoin, "_universe_cache_path", lambda exchange: cache_path)

    async def slow_market_rows(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {"LINK/USDT": {"symbol": "LINK/USDT", "base_symbol": "LINK", "market_cap_usd": 1_000_000_000}}

    async def slow_onboard_map(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {}

    monkeypatch.setattr(coinglass_altcoin, "load_coinglass_market_snapshots", slow_market_rows)
    monkeypatch.setattr(coinglass_altcoin, "load_exchange_onboard_map", slow_onboard_map)

    payload = asyncio.run(coinglass_altcoin.build_exchange_altcoin_universe("binance"))

    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT"]
    assert payload["stale_fallback"] is True
    assert any("coinglass universe refresh fallback for binance" in item for item in payload["warnings"])


def test_load_cached_exchange_altcoin_universe_can_return_stale_disk_cache(
    monkeypatch,
    tmp_path,
):
    stale_payload = {
        "exchange": "binance",
        "symbols": ["LINK/USDT"],
        "count": 1,
        "source": "coinglass_altcoin_universe",
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
    }
    cache_path = tmp_path / "altcoin_universe_binance.json"
    cache_path.write_text(json.dumps(stale_payload, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_CACHE", {})
    monkeypatch.setattr(coinglass_altcoin, "_universe_cache_path", lambda exchange: cache_path)

    assert coinglass_altcoin.load_cached_exchange_altcoin_universe("binance") is None
    assert coinglass_altcoin.load_cached_exchange_altcoin_universe("binance", allow_stale=True) == stale_payload


def test_build_exchange_altcoin_universe_uses_stale_cache_when_budget_headroom_is_too_low(
    monkeypatch,
    tmp_path,
):
    stale_payload = {
        "exchange": "binance",
        "symbols": ["LINK/USDT", "AAVE/USDT"],
        "count": 2,
        "source": "coinglass_altcoin_universe",
        "updated_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
    }
    cache_path = tmp_path / "altcoin_universe_binance.json"
    cache_path.write_text(json.dumps(stale_payload, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_CACHE", {})
    monkeypatch.setattr(coinglass_altcoin, "_universe_cache_path", lambda exchange: cache_path)
    monkeypatch.setattr(coinglass_altcoin, "coinglass_enabled", lambda: True)

    async def fake_budget_state():
        return SimpleNamespace(minute_remaining=2)

    monkeypatch.setattr(coinglass_altcoin, "get_coinglass_budget_state", fake_budget_state)
    monkeypatch.setattr(
        coinglass_altcoin,
        "coinglass_minute_headroom",
        lambda state, manual=False: 1,
    )

    async def fail_market_rows(*args, **kwargs):
        raise AssertionError("market rows should not load when minute headroom is too low")

    async def fail_onboard_map(*args, **kwargs):
        raise AssertionError("onboard map should not load when minute headroom is too low")

    monkeypatch.setattr(coinglass_altcoin, "load_coinglass_market_snapshots", fail_market_rows)
    monkeypatch.setattr(coinglass_altcoin, "load_exchange_onboard_map", fail_onboard_map)

    payload = asyncio.run(coinglass_altcoin.build_exchange_altcoin_universe("binance"))

    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT"]
    assert payload["stale_fallback"] is True
    assert any("minute headroom too low" in item for item in payload["warnings"])


def test_build_exchange_altcoin_universe_caps_market_pages_when_budget_is_limited(
    monkeypatch,
    tmp_path,
):
    cache_path = tmp_path / "altcoin_universe_binance.json"

    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_CACHE", {})
    monkeypatch.setattr(coinglass_altcoin, "_universe_cache_path", lambda exchange: cache_path)
    monkeypatch.setattr(coinglass_altcoin, "coinglass_enabled", lambda: True)

    async def fake_budget_state():
        return SimpleNamespace(minute_remaining=4)

    monkeypatch.setattr(coinglass_altcoin, "get_coinglass_budget_state", fake_budget_state)
    monkeypatch.setattr(
        coinglass_altcoin,
        "coinglass_minute_headroom",
        lambda state, manual=False: 3,
    )

    seen: dict[str, int] = {}

    async def fake_market_rows(exchange: str, **kwargs):
        seen["max_pages"] = kwargs.get("max_pages")
        return {
            "LINK/USDT": {
                "symbol": "LINK/USDT",
                "base_symbol": "LINK",
                "market_cap_usd": 1_000_000_000,
            },
            "AAVE/USDT": {
                "symbol": "AAVE/USDT",
                "base_symbol": "AAVE",
                "market_cap_usd": 500_000_000,
            },
        }

    async def fake_onboard_map(exchange: str, **kwargs):
        return {
            "AAVE": {
                "symbol": "AAVE/USDT",
                "base_symbol": "AAVE",
                "onboard_ts": 1_700_000_000.0,
                "onboard_date": "2023-11-14T00:00:00+00:00",
            }
        }

    async def fake_load_currency_tags(symbols, *, refresh=False):
        return {
            "LINK": ["defi"],
            "AAVE": ["defi"],
        }

    monkeypatch.setattr(coinglass_altcoin, "load_coinglass_market_snapshots", fake_market_rows)
    monkeypatch.setattr(coinglass_altcoin, "load_exchange_onboard_map", fake_onboard_map)
    monkeypatch.setattr(coinglass_altcoin, "load_currency_tags", fake_load_currency_tags)

    payload = asyncio.run(coinglass_altcoin.build_exchange_altcoin_universe("binance"))

    assert seen["max_pages"] == 2
    assert payload["source"] == "coinglass_altcoin_universe"
    assert payload["count"] == 2
    assert set(payload["symbols"]) == {"LINK/USDT", "AAVE/USDT"}
