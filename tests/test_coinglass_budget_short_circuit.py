from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from core.data import coinglass_client as client_module
from core.data import coinglass_feature_builder as builder_module


def test_discover_capabilities_stops_after_rate_limit(monkeypatch, tmp_path: Path):
    api_spec_path = tmp_path / "api_spec.json"
    capability_path = tmp_path / "capability.json"
    monkeypatch.setattr(client_module, "_API_SPEC_PATH", api_spec_path)
    monkeypatch.setattr(client_module, "_CAPABILITY_MATRIX_PATH", capability_path)

    async def fake_fetch_api_spec(self, *, manual=True):
        return {"payload": {"openapi": "3.0.0"}}

    calls: list[str] = []

    async def fake_request_json(self, path, *, params, manual):
        calls.append(path)
        if path.endswith("/open-interest/exchange-list"):
            return {"status_code": 200, "latency_ms": 12, "payload": {"data": [{"symbol": "BTC"}]}}
        raise client_module.CoinglassError("http_429:request_failed")

    monkeypatch.setattr(client_module.CoinglassClient, "fetch_api_spec", fake_fetch_api_spec)
    monkeypatch.setattr(client_module.CoinglassClient, "_request_json", fake_request_json)

    result = asyncio.run(
        client_module.discover_and_persist_coinglass_capabilities(
            datasets=[
                "open_interest_exchange_list",
                "funding_rate_exchange_list",
                "liquidation_history",
            ]
        )
    )

    assert result["available_count"] == 1
    assert result["stopped_early"] is True
    assert result["stop_reason"] == "http_429:request_failed"
    assert calls == [
        "/v4/api/futures/open-interest/exchange-list",
        "/v4/api/futures/funding-rate/exchange-list",
    ]


def test_update_cache_stops_across_symbols_after_budget_guard(monkeypatch):
    monkeypatch.setattr(builder_module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(builder_module, "persist_raw_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(builder_module, "persist_normalized_rows", lambda **kwargs: pd.DataFrame([{"symbol": "BTC"}]))
    monkeypatch.setattr(builder_module, "persist_symbol_registry", lambda symbols: None)
    monkeypatch.setattr(builder_module, "build_derivatives_snapshot", lambda symbol: None)

    async def fake_record_status(**kwargs):
        return None

    async def fake_budget_state():
        class _Budget:
            def to_dict(self):
                return {"minute_remaining": 0}

        return _Budget()

    monkeypatch.setattr(builder_module, "record_coinglass_ingest_status", fake_record_status)
    monkeypatch.setattr(builder_module, "get_coinglass_budget_state", fake_budget_state)

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def request_dataset(self, manifest, *, symbol=None, exchange=None, interval=None, manual=False):
            self.calls.append((manifest.dataset, symbol))
            if manifest.dataset == "open_interest_exchange_list" and symbol == "BTC":
                return {
                    "route": manifest.routes[0],
                    "params": {"exchange": "Binance", "interval": interval},
                    "request_key": "req-1",
                    "latency_ms": 25,
                    "payload": {"data": [{"symbol": symbol}]},
                }
            raise builder_module.CoinglassBudgetExceeded("minute_budget_exhausted")

    fake_client = _FakeClient()
    monkeypatch.setattr(builder_module, "CoinglassClient", lambda: fake_client)

    result = asyncio.run(
        builder_module.update_coinglass_cache(
            symbols=["BTC", "ETH"],
            datasets=["open_interest_exchange_list", "funding_rate_exchange_list"],
            manual=True,
            max_symbols_per_run=2,
        )
    )

    assert result["stopped_early"] is True
    assert result["stop_reason"] == "minute_budget_exhausted"
    assert result["updated"] == [
        {
            "dataset": "open_interest_exchange_list",
            "symbol": "BTC",
            "rows_written": 1,
            "latency_ms": 25,
        }
    ]
    assert result["errors"] == [
        {
            "dataset": "funding_rate_exchange_list",
            "symbol": "BTC",
            "error": "minute_budget_exhausted",
        }
    ]
    assert fake_client.calls == [
        ("open_interest_exchange_list", "BTC"),
        ("funding_rate_exchange_list", "BTC"),
    ]


def test_manual_update_caps_planned_requests_to_minute_limit(monkeypatch):
    monkeypatch.setattr(builder_module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(builder_module.settings, "COINGLASS_RATE_LIMIT_PER_MIN", 3, raising=False)
    monkeypatch.setattr(builder_module, "persist_raw_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(builder_module, "persist_normalized_rows", lambda **kwargs: pd.DataFrame([{"symbol": "BTC"}]))
    monkeypatch.setattr(builder_module, "persist_symbol_registry", lambda symbols: None)
    monkeypatch.setattr(builder_module, "build_derivatives_snapshot", lambda symbol: None)

    async def fake_record_status(**kwargs):
        return None

    async def fake_budget_state():
        class _Budget:
            def to_dict(self):
                return {"minute_remaining": 3}

        return _Budget()

    monkeypatch.setattr(builder_module, "record_coinglass_ingest_status", fake_record_status)
    monkeypatch.setattr(builder_module, "get_coinglass_budget_state", fake_budget_state)

    class _FakeClient:
        def __init__(self):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def request_dataset(self, manifest, *, symbol=None, exchange=None, interval=None, manual=False):
            self.calls.append((manifest.dataset, symbol, manual))
            return {
                "route": manifest.routes[0],
                "params": {"exchange": exchange, "interval": interval},
                "request_key": f"{manifest.dataset}:{symbol}",
                "latency_ms": 10,
                "payload": {"data": [{"symbol": symbol}]},
            }

    fake_client = _FakeClient()
    monkeypatch.setattr(builder_module, "CoinglassClient", lambda: fake_client)

    result = asyncio.run(
        builder_module.update_coinglass_cache(
            symbols=["BTC", "ETH"],
            datasets=[
                "open_interest_exchange_list",
                "funding_rate_exchange_list",
                "liquidation_history",
            ],
            manual=True,
            max_symbols_per_run=2,
        )
    )

    assert result["stopped_early"] is True
    assert result["stop_reason"] == "manual_refresh_limited_by_3_per_min_budget"
    assert result["symbols"] == ["BTC", "ETH"]
    assert result["datasets"] == ["open_interest_exchange_list"]
    assert fake_client.calls == [
        ("open_interest_exchange_list", "BTC", True),
        ("open_interest_exchange_list", "ETH", True),
    ]


def test_non_manual_update_caps_symbols_when_minute_headroom_is_tighter_than_symbol_count(monkeypatch):
    monkeypatch.setattr(builder_module, "_NON_MANUAL_DATASET_CURSOR", 0)
    monkeypatch.setattr(builder_module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(builder_module, "persist_raw_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(builder_module, "persist_normalized_rows", lambda **kwargs: pd.DataFrame([{"symbol": "BTC"}]))
    monkeypatch.setattr(builder_module, "persist_symbol_registry", lambda symbols: None)
    monkeypatch.setattr(builder_module, "build_derivatives_snapshot", lambda symbol: None)
    monkeypatch.setattr(builder_module, "coinglass_minute_headroom", lambda budget_state, manual=False: 4)
    monkeypatch.setattr(builder_module.settings, "COINGLASS_RATE_LIMIT_PER_MIN", 30, raising=False)

    async def fake_record_status(**kwargs):
        return None

    async def fake_budget_state():
        class _Budget:
            def to_dict(self):
                return {"minute_remaining": 4}

        return _Budget()

    monkeypatch.setattr(builder_module, "record_coinglass_ingest_status", fake_record_status)
    monkeypatch.setattr(builder_module, "get_coinglass_budget_state", fake_budget_state)

    class _FakeClient:
        def __init__(self):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def request_dataset(self, manifest, *, symbol=None, exchange=None, interval=None, manual=False):
            self.calls.append((manifest.dataset, symbol, manual))
            return {
                "route": manifest.routes[0],
                "params": {"exchange": exchange, "interval": interval},
                "request_key": f"{manifest.dataset}:{symbol}",
                "latency_ms": 10,
                "payload": {"data": [{"symbol": symbol}]},
            }

    fake_client = _FakeClient()
    monkeypatch.setattr(builder_module, "CoinglassClient", lambda: fake_client)

    result = asyncio.run(
        builder_module.update_coinglass_cache(
            symbols=["BTC", "ETH", "SOL"],
            datasets=["open_interest_exchange_list", "funding_rate_exchange_list"],
            manual=False,
            max_symbols_per_run=3,
        )
    )

    assert result["stopped_early"] is True
    assert result["stop_reason"] == "non_manual_refresh_limited_by_30_per_min_budget"
    assert result["symbols"] == ["BTC"]
    assert result["datasets"] == ["open_interest_exchange_list"]
    assert fake_client.calls == [
        ("open_interest_exchange_list", "BTC", False),
    ]


def test_update_cache_limits_non_manual_refresh_to_minute_headroom(monkeypatch):
    monkeypatch.setattr(builder_module, "_NON_MANUAL_DATASET_CURSOR", 0)
    monkeypatch.setattr(builder_module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(builder_module, "persist_raw_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(builder_module, "persist_normalized_rows", lambda **kwargs: pd.DataFrame([{"symbol": "BTC"}]))
    monkeypatch.setattr(builder_module, "persist_symbol_registry", lambda symbols: None)
    monkeypatch.setattr(builder_module, "build_derivatives_snapshot", lambda symbol: None)
    monkeypatch.setattr(builder_module.settings, "COINGLASS_RATE_LIMIT_PER_MIN", 30, raising=False)

    async def fake_record_status(**kwargs):
        return None

    class _Budget:
        minute_remaining = 8
        daily_remaining = 100
        monthly_remaining = 100

        def to_dict(self):
            return {"minute_remaining": self.minute_remaining}

    async def fake_budget_state():
        return _Budget()

    monkeypatch.setattr(builder_module, "record_coinglass_ingest_status", fake_record_status)
    monkeypatch.setattr(builder_module, "get_coinglass_budget_state", fake_budget_state)

    class _FakeClient:
        def __init__(self):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def request_dataset(self, manifest, *, symbol=None, exchange=None, interval=None, manual=False):
            self.calls.append((manifest.dataset, manual))
            return {
                "route": manifest.routes[0],
                "params": {"exchange": exchange, "interval": interval},
                "request_key": f"req-{manifest.dataset}",
                "latency_ms": 1,
                "payload": {"data": [{"symbol": symbol}]},
            }

    fake_client = _FakeClient()
    monkeypatch.setattr(builder_module, "CoinglassClient", lambda: fake_client)

    result = asyncio.run(
        builder_module.update_coinglass_cache(
            symbols=["BTC"],
            datasets=[
                "open_interest_exchange_list",
                "open_interest_history",
                "funding_rate_exchange_list",
                "funding_rate_history",
                "taker_buy_sell_volume_exchange_list",
            ],
            manual=False,
            max_symbols_per_run=1,
        )
    )

    assert result["stopped_early"] is True
    assert result["stop_reason"] == "non_manual_refresh_limited_by_30_per_min_budget"
    assert result["datasets"] == ["open_interest_exchange_list", "open_interest_history", "funding_rate_exchange_list"]
    assert fake_client.calls == [
        ("open_interest_exchange_list", False),
        ("open_interest_history", False),
        ("funding_rate_exchange_list", False),
    ]

    # The next budget-limited sweep must continue past the head of the list, or the
    # tail datasets (funding history, taker volume, long/short) never refresh.
    fake_client.calls.clear()
    followup = asyncio.run(
        builder_module.update_coinglass_cache(
            symbols=["BTC"],
            datasets=[
                "open_interest_exchange_list",
                "open_interest_history",
                "funding_rate_exchange_list",
                "funding_rate_history",
                "taker_buy_sell_volume_exchange_list",
            ],
            manual=False,
            max_symbols_per_run=1,
        )
    )

    assert followup["datasets"] == [
        "funding_rate_history",
        "taker_buy_sell_volume_exchange_list",
        "open_interest_exchange_list",
    ]
