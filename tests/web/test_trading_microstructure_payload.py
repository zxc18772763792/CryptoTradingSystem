import asyncio

import pytest

from web.api import trading as trading_api


def test_market_microstructure_includes_orderbook_and_flow_availability(monkeypatch):
    trading_api._MICROSTRUCTURE_SNAPSHOT_CACHE.clear()

    async def fake_orderbook(*args, **kwargs):
        return {
            "available": True,
            "bids": [[100.0, 2.0]],
            "asks": [[100.2, 1.5]],
            "timestamp": 1,
        }

    async def fake_flow(*args, **kwargs):
        return {
            "available": True,
            "count": 12,
            "buy_volume": 8.0,
            "sell_volume": 4.0,
            "imbalance": 0.333333,
        }

    async def fake_funding_basis(*args, **kwargs):
        return {"funding": {"available": False}, "basis": {"available": False}}

    async def fake_oi(*args, **kwargs):
        return {
            "available": True,
            "source": "binance_public",
            "error": None,
            "volume": 12345.6,
            "value": 987654321.0,
            "change_pct_1h": 2.5,
            "timestamp": "2026-03-12T00:00:00+00:00",
            "sample_size": 13,
        }

    async def fake_options(*args, **kwargs):
        return {
            "available": True,
            "currency": "BTC",
            "atm_iv": 0.55,
            "skew_25d": 0.07,
            "put_call_ratio": 1.4,
            "signal": "fear",
        }

    async def fake_long_short(*args, **kwargs):
        return {
            "available": True,
            "source": "binance_public",
            "error": None,
            "symbol": "BTC/USDT",
            "long_ratio": 0.56,
            "short_ratio": 0.44,
            "long_short_ratio": 1.2727,
            "sample_size": 12,
            "timestamp": "2026-03-12T00:00:00+00:00",
        }

    monkeypatch.setattr(trading_api, "_fetch_orderbook", fake_orderbook)
    monkeypatch.setattr(trading_api, "_fetch_trade_imbalance", fake_flow)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_funding_and_basis", fake_funding_basis)
    monkeypatch.setattr(trading_api, "_fetch_open_interest_snapshot", fake_oi)
    monkeypatch.setattr(trading_api, "_fetch_options_snapshot", fake_options)
    monkeypatch.setattr(trading_api, "_fetch_long_short_ratio_snapshot", fake_long_short)

    payload = asyncio.run(trading_api.get_market_microstructure(exchange="binance", symbol="BTC/USDT", depth_limit=20))

    assert payload["orderbook"]["available"] is True
    assert payload["orderbook"]["mid_price"] > 0
    assert payload["aggressor_flow"]["available"] is True
    assert payload["aggressor_flow"]["imbalance"] == pytest.approx(0.333333, rel=1e-6)
    assert payload["oi"]["available"] is True
    assert payload["oi"]["change_pct_1h"] == pytest.approx(2.5, rel=1e-6)
    assert payload["long_short_ratio"]["available"] is True
    assert payload["long_short_ratio"]["long_short_ratio"] == pytest.approx(1.2727, rel=1e-6)
    assert payload["options"]["available"] is True
    assert payload["options"]["skew_25d"] == pytest.approx(0.07, rel=1e-6)


def test_market_microstructure_preserves_flow_error_flag(monkeypatch):
    trading_api._MICROSTRUCTURE_SNAPSHOT_CACHE.clear()

    async def fake_orderbook(*args, **kwargs):
        return {
            "available": True,
            "bids": [[100.0, 2.0]],
            "asks": [[100.2, 1.5]],
            "timestamp": 1,
        }

    async def fake_flow(*args, **kwargs):
        return {
            "available": False,
            "error": "timeout_or_cancelled",
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }

    async def fake_funding_basis(*args, **kwargs):
        return {"funding": {"available": False}, "basis": {"available": False}}

    async def fake_oi(*args, **kwargs):
        return {
            "available": False,
            "source": "binance_public",
            "error": "oi_timeout",
            "volume": 0.0,
            "value": 0.0,
            "change_pct_1h": None,
            "timestamp": None,
            "sample_size": 0,
        }

    async def fake_long_short(*args, **kwargs):
        return {
            "available": False,
            "source": "binance_public",
            "error": "ls_timeout",
            "symbol": "BTC/USDT",
            "long_ratio": 0.0,
            "short_ratio": 0.0,
            "long_short_ratio": 0.0,
            "sample_size": 0,
            "timestamp": None,
        }

    monkeypatch.setattr(trading_api, "_fetch_orderbook", fake_orderbook)
    monkeypatch.setattr(trading_api, "_fetch_trade_imbalance", fake_flow)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_funding_and_basis", fake_funding_basis)
    monkeypatch.setattr(trading_api, "_fetch_open_interest_snapshot", fake_oi)
    monkeypatch.setattr(trading_api, "_fetch_long_short_ratio_snapshot", fake_long_short)

    payload = asyncio.run(trading_api.get_market_microstructure(exchange="binance", symbol="BTC/USDT", depth_limit=20))

    assert payload["aggressor_flow"]["available"] is False
    assert payload["aggressor_flow"]["error"] == "timeout_or_cancelled"
    assert payload["oi"]["available"] is False
    assert payload["oi"]["error"] == "oi_timeout"
    assert payload["long_short_ratio"]["available"] is False
    assert payload["long_short_ratio"]["error"] == "ls_timeout"


def test_gate_orderbook_uses_public_fallback_when_connector_missing(monkeypatch):
    async def fake_gate_orderbook(*args, **kwargs):
        return {
            "available": True,
            "bids": [[100.0, 3.0]],
            "asks": [[100.1, 2.5]],
            "timestamp": 123,
            "source": "gate_public",
        }

    monkeypatch.setattr(trading_api, "_fetch_gate_public_orderbook", fake_gate_orderbook)
    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", lambda _exchange: None)

    payload = asyncio.run(trading_api._fetch_orderbook(exchange="gate", symbol="BTC/USDT", limit=20))

    assert payload["available"] is True
    assert payload["source"] == "gate_public"
    assert payload["bids"]
    assert payload["asks"]


def test_gate_trade_imbalance_uses_public_fallback_when_connector_missing(monkeypatch):
    async def fake_gate_flow(*args, **kwargs):
        return {
            "available": True,
            "count": 100,
            "buy_volume": 80.0,
            "sell_volume": 20.0,
            "imbalance": 0.6,
            "source": "gate_public",
        }

    monkeypatch.setattr(trading_api, "_fetch_gate_public_trade_imbalance", fake_gate_flow)
    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", lambda _exchange: None)

    payload = asyncio.run(trading_api._fetch_trade_imbalance(exchange="gate", symbol="BTC/USDT", limit=200))

    assert payload["available"] is True
    assert payload["source"] == "gate_public"
    assert payload["count"] == 100
    assert payload["imbalance"] == pytest.approx(0.6, rel=1e-6)


def test_gate_funding_basis_prefers_gate_public_before_binance_fallback(monkeypatch):
    async def fake_gate_fb(*args, **kwargs):
        return {
            "funding": {
                "available": True,
                "funding_rate": 0.0002,
                "next_funding_time": "2026-04-14T08:00:00+00:00",
                "source": "gate_public",
            },
            "basis": {
                "available": True,
                "spot_price": 100.0,
                "perp_price": 100.12,
                "basis_pct": 0.12,
                "source": "gate_public",
            },
        }

    async def fake_binance_fb(*args, **kwargs):
        return {"funding": {"available": False}, "basis": {"available": False}}

    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", lambda _exchange: None)
    monkeypatch.setattr(trading_api, "_fetch_gate_public_funding_and_basis", fake_gate_fb)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_funding_and_basis", fake_binance_fb)

    payload = asyncio.run(trading_api._fetch_funding_basis_snapshot(exchange="gate", symbol="BTC/USDT"))

    assert payload["funding"]["available"] is True
    assert payload["basis"]["available"] is True
    assert payload["funding"]["source"] == "gate_public"
    assert payload["basis"]["source"] == "gate_public"


def test_gate_open_interest_prefers_gate_public_when_available(monkeypatch):
    async def fake_gate_oi(*args, **kwargs):
        return {
            "available": True,
            "source": "gate_public",
            "error": None,
            "symbol": "BTC/USDT",
            "volume": 123456.0,
            "value": 9876543.21,
            "change_pct_1h": None,
            "timestamp": "2026-04-14T00:00:00+00:00",
            "sample_size": 1,
        }

    async def fake_binance_oi(*args, **kwargs):
        return {
            "available": False,
            "source": "binance_public",
            "error": "fallback_should_not_be_used",
            "symbol": "BTC/USDT",
            "volume": 0.0,
            "value": 0.0,
            "change_pct_1h": None,
            "timestamp": None,
            "sample_size": 0,
        }

    monkeypatch.setattr(trading_api, "_fetch_gate_public_open_interest", fake_gate_oi)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_open_interest", fake_binance_oi)

    payload = asyncio.run(trading_api._fetch_open_interest_snapshot(exchange="gate", symbol="BTC/USDT"))

    assert payload["available"] is True
    assert payload["source"] == "gate_public"
    assert payload["value"] == pytest.approx(9876543.21, rel=1e-9)


def test_gate_long_short_ratio_prefers_gate_public_when_available(monkeypatch):
    async def fake_gate_ls(*args, **kwargs):
        return {
            "available": True,
            "source": "gate_public",
            "error": None,
            "symbol": "BTC/USDT",
            "long_ratio": 0.57,
            "short_ratio": 0.43,
            "long_short_ratio": 1.3256,
            "sample_size": 1,
            "timestamp": "2026-04-14T00:00:00+00:00",
        }

    async def fake_binance_ls(*args, **kwargs):
        return {
            "available": False,
            "source": "binance_public",
            "error": "fallback_should_not_be_used",
            "symbol": "BTC/USDT",
            "long_ratio": 0.0,
            "short_ratio": 0.0,
            "long_short_ratio": 0.0,
            "sample_size": 0,
            "timestamp": None,
        }

    monkeypatch.setattr(trading_api, "_fetch_gate_public_long_short_ratio", fake_gate_ls)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_long_short_ratio", fake_binance_ls)

    payload = asyncio.run(trading_api._fetch_long_short_ratio_snapshot(exchange="gate", symbol="BTC/USDT"))

    assert payload["available"] is True
    assert payload["source"] == "gate_public"
    assert payload["long_short_ratio"] == pytest.approx(1.3256, rel=1e-9)
