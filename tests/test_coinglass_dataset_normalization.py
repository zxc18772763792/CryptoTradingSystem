from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from core.data import coinglass_client as client_module
from core.data import coinglass_feature_builder as builder_module
from core.data.coinglass_registry import get_coinglass_manifest


def test_manifest_params_add_range_and_pair_symbol_mapping():
    taker_manifest = get_coinglass_manifest("taker_buy_sell_volume_exchange_list")
    taker_params = client_module._manifest_params(
        taker_manifest,
        taker_manifest.routes[0],
        symbol="BTC/USDT",
        exchange=None,
        interval="h4",
        limit=None,
    )
    assert taker_params["symbol"] == "BTC"
    assert taker_params["range"] == "4h"

    liquidation_manifest = get_coinglass_manifest("liquidation_history")
    liquidation_params = client_module._manifest_params(
        liquidation_manifest,
        liquidation_manifest.routes[0],
        symbol="BTC/USDT",
        exchange="Binance",
        interval="h4",
        limit=2,
    )
    assert liquidation_params["exchange"] == "Binance"
    assert liquidation_params["symbol"] == "BTCUSDT"
    assert liquidation_params["interval"] == "h4"
    assert liquidation_params["limit"] == 2

    ratio_manifest = get_coinglass_manifest("global_long_short_account_ratio_history")
    ratio_params = client_module._manifest_params(
        ratio_manifest,
        ratio_manifest.routes[0],
        symbol="BTC/USDT",
        exchange="OKX",
        interval="h4",
        limit=None,
    )
    assert ratio_params["exchange"] == "OKX"
    assert ratio_params["symbol"] == "BTC-USDT-SWAP"


def test_history_manifest_params_use_defaults_and_pair_symbol_mapping():
    oi_manifest = get_coinglass_manifest("open_interest_history")
    oi_params = client_module._manifest_params(
        oi_manifest,
        oi_manifest.routes[0],
        symbol="BTC/USDT",
        exchange=None,
        interval=None,
        limit=3,
    )
    assert oi_params["exchange"] == "Binance"
    assert oi_params["symbol"] == "BTCUSDT"
    assert oi_params["interval"] == "h1"
    assert oi_params["unit"] == "usd"
    assert oi_params["limit"] == 3

    funding_manifest = get_coinglass_manifest("funding_rate_history")
    funding_params = client_module._manifest_params(
        funding_manifest,
        funding_manifest.routes[0],
        symbol="BTC/USDT",
        exchange="OKX",
        interval=None,
        limit=4,
    )
    assert funding_params["exchange"] == "OKX"
    assert funding_params["symbol"] == "BTC-USDT-SWAP"
    assert funding_params["interval"] == "h1"
    assert funding_params["limit"] == 4

    taker_history_manifest = get_coinglass_manifest("taker_buy_sell_volume_history")
    taker_history_params = client_module._manifest_params(
        taker_history_manifest,
        taker_history_manifest.routes[0],
        symbol="BTC/USDT",
        exchange="Bybit",
        interval=None,
        limit=5,
    )
    assert taker_history_params["exchange"] == "Bybit"
    assert taker_history_params["symbol"] == "BTCUSDT"
    assert taker_history_params["interval"] == "h1"
    assert taker_history_params["limit"] == 5


def test_normalize_dataset_response_rejects_business_error_payload():
    result = client_module.normalize_dataset_response(
        dataset="taker_buy_sell_volume_exchange_list",
        request_meta={"symbol": "BTC"},
        response_payload={"code": "400", "msg": "Required String parameter 'range' is not present"},
    )

    assert result["status"] == "failed"
    assert result["rows"] == []
    assert result["details"]["response_code"] == 400


def test_normalize_funding_rate_exchange_list_filters_symbol_and_flattens_margin_lists():
    payload = {
        "code": 0,
        "data": [
            {
                "symbol": "BTC",
                "stablecoin_margin_list": [{"exchange": "Binance", "funding_rate": 0.0012}],
                "token_margin_list": [{"exchange": "Bybit", "funding_rate": 0.0024}],
            },
            {
                "symbol": "ETH",
                "stablecoin_margin_list": [{"exchange": "Binance", "funding_rate": 0.0031}],
                "token_margin_list": [],
            },
        ],
    }

    result = client_module.normalize_dataset_response(
        dataset="funding_rate_exchange_list",
        request_meta={"symbol": "BTC"},
        response_payload=payload,
    )

    assert result["status"] == "ok"
    assert len(result["rows"]) == 2
    assert {row["symbol"] for row in result["rows"]} == {"BTC"}
    assert {row["margin_type"] for row in result["rows"]} == {"stablecoin", "token"}
    assert {row["exchange"] for row in result["rows"]} == {"Binance", "Bybit"}


def test_normalize_funding_arbitrage_requires_exact_symbol_match():
    payload = {
        "code": 0,
        "data": [
            {"symbol": "ETH", "buy": {"exchange": "Binance", "funding_rate": -0.01}, "sell": {"exchange": "Bybit", "funding_rate": 0.02}},
        ],
    }

    result = client_module.normalize_dataset_response(
        dataset="funding_arbitrage",
        request_meta={"symbol": "BTC"},
        response_payload=payload,
    )

    assert result["status"] == "empty"
    assert result["rows"] == []


def test_normalize_history_dataset_rows_flattens_ohlc_fields():
    oi_result = client_module.normalize_dataset_response(
        dataset="open_interest_history",
        request_meta={"symbol": "BTC", "exchange": "Binance", "interval": "h1"},
        response_payload={
            "code": 0,
            "data": [
                {"time": 1710000000000, "open": "100", "high": "125", "low": "95", "close": "118"},
            ],
        },
    )
    assert oi_result["status"] == "ok"
    assert oi_result["rows"][0]["open_interest_open"] == 100.0
    assert oi_result["rows"][0]["open_interest_high"] == 125.0
    assert oi_result["rows"][0]["open_interest_low"] == 95.0
    assert oi_result["rows"][0]["open_interest_close"] == 118.0
    assert oi_result["rows"][0]["open_interest_usd"] == 118.0

    funding_result = client_module.normalize_dataset_response(
        dataset="funding_rate_history",
        request_meta={"symbol": "BTC", "exchange": "Binance", "interval": "h1"},
        response_payload={
            "code": 0,
            "data": [
                {"time": 1710000000000, "open": "0.0004", "high": "0.0007", "low": "0.0002", "close": "0.0006"},
            ],
        },
    )
    assert funding_result["status"] == "ok"
    assert funding_result["rows"][0]["funding_rate_open"] == 0.0004
    assert funding_result["rows"][0]["funding_rate_high"] == 0.0007
    assert funding_result["rows"][0]["funding_rate_low"] == 0.0002
    assert funding_result["rows"][0]["funding_rate_close"] == 0.0006
    assert funding_result["rows"][0]["funding_rate"] == 0.0006


def test_persist_normalized_rows_keeps_margin_variants(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(client_module, "_NORMALIZED_ROOT", tmp_path)

    manifest = get_coinglass_manifest("funding_rate_exchange_list")
    frame = client_module.persist_normalized_rows(
        dataset="funding_rate_exchange_list",
        manifest=manifest,
        route=manifest.routes[0],
        request_meta={"symbol": "BTC", "request_key": "req-1", "latency_ms": 12},
        response_payload=[
            {"symbol": "BTC", "exchange": "Binance", "margin_type": "stablecoin", "funding_rate": 0.0010},
            {"symbol": "BTC", "exchange": "Binance", "margin_type": "token", "funding_rate": 0.0020},
        ],
    )

    assert len(frame.index) == 2
    assert set(frame["row_variant"].tolist()) == {"stablecoin", "token"}


def test_build_derivatives_snapshot_prefers_aggregate_oi_and_flattened_funding(monkeypatch):
    monkeypatch.setattr(
        builder_module,
        "_dataset_payloads",
        lambda symbol: {
            "open_interest_exchange_list": [
                {"exchange": "All", "symbol": "BTC", "open_interest_usd": 100.0, "open_interest_change_percent_1h": 2.0},
                {"exchange": "Binance", "symbol": "BTC", "open_interest_usd": 60.0, "open_interest_change_percent_1h": 5.0},
                {"exchange": "OKX", "symbol": "BTC", "open_interest_usd": 40.0, "open_interest_change_percent_1h": -1.0},
            ],
            "funding_rate_exchange_list": [
                {"exchange": "Binance", "symbol": "BTC", "margin_type": "stablecoin", "funding_rate": 0.0010},
                {"exchange": "Bybit", "symbol": "BTC", "margin_type": "stablecoin", "funding_rate": 0.0020},
                {"exchange": "Binance", "symbol": "BTC", "margin_type": "token", "funding_rate": 0.0040},
            ],
            "taker_buy_sell_volume_exchange_list": [
                {"exchange": "Binance", "taker_buy_volume": 80.0, "taker_sell_volume": 20.0}
            ],
            "liquidation_history": [
                {"long_liquidation_usd": 10.0, "short_liquidation_usd": 30.0, "burst_score": 0.4}
            ],
            "global_long_short_account_ratio_history": [
                {"long_short_ratio": 1.2}
            ],
            "funding_arbitrage": [],
        },
    )

    snapshot = builder_module.build_derivatives_snapshot("BTC/USDT")

    assert snapshot is not None
    assert snapshot.oi_usd == 100.0
    assert snapshot.oi_change_1h == 2.0
    assert snapshot.funding_rate == 0.0015
    assert snapshot.funding_rate_oi_weighted == 0.0015
    assert snapshot.taker_buy_sell_imbalance == 0.6
    assert snapshot.payload["funding_exchange_count"] == 2
    assert snapshot.payload["liquidation_burst_score"] == 0.4


def test_build_derivatives_snapshot_derives_history_features_and_labels(monkeypatch):
    start = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)

    oi_history_rows = []
    funding_history_rows = []
    ratio_rows = []
    for offset in range(25):
        ts = (start + timedelta(hours=offset)).isoformat()
        oi_close = 100.0 + (offset * 5.0)
        if offset == 24:
            oi_close = 250.0
        funding_close = 0.0005 + (offset * 0.00002)
        if offset == 24:
            funding_close = 0.0018
        ratio_value = 0.92 + (offset * 0.013)
        oi_history_rows.append(
            {
                "_source_ts": ts,
                "_exchange": "Binance",
                "_interval": "h1",
                "close": oi_close,
                "open_interest_close": oi_close,
            }
        )
        funding_history_rows.append(
            {
                "_source_ts": ts,
                "_exchange": "Binance",
                "_interval": "h1",
                "close": funding_close,
                "funding_rate_close": funding_close,
            }
        )
        ratio_rows.append(
            {
                "_source_ts": ts,
                "_exchange": "Binance",
                "_interval": "h1",
                "long_short_ratio": ratio_value,
            }
        )

    monkeypatch.setattr(
        builder_module,
        "_dataset_payloads",
        lambda symbol: {
            "open_interest_exchange_list": [
                {
                    "exchange": "All",
                    "symbol": "BTC",
                    "open_interest_usd": 220.0,
                    "open_interest_change_percent_1h": 1.0,
                    "open_interest_change_percent_24h": 8.0,
                }
            ],
            "open_interest_history": oi_history_rows,
            "funding_rate_exchange_list": [
                {
                    "exchange": "Binance",
                    "symbol": "BTC",
                    "margin_type": "stablecoin",
                    "funding_rate": 0.0018,
                    "open_interest_usd": 140.0,
                },
                {
                    "exchange": "OKX",
                    "symbol": "BTC",
                    "margin_type": "stablecoin",
                    "funding_rate": 0.0022,
                    "open_interest_usd": 80.0,
                },
            ],
            "funding_rate_history": funding_history_rows,
            "taker_buy_sell_volume_exchange_list": [
                {"exchange": "Binance", "taker_buy_volume": 160.0, "taker_sell_volume": 40.0}
            ],
            "liquidation_history": [
                {
                    "_source_ts": (start + timedelta(hours=24)).isoformat(),
                    "long_liquidation_usd": 5_000_000.0,
                    "short_liquidation_usd": 55_000_000.0,
                    "burst_score": 0.72,
                }
            ],
            "global_long_short_account_ratio_history": ratio_rows,
            "funding_arbitrage": [
                {
                    "_source_ts": (start + timedelta(hours=24)).isoformat(),
                    "symbol": "BTC",
                    "basis_pct": 0.045,
                    "oi_weighted_funding_rate": 0.0020,
                }
            ],
        },
    )

    snapshot = builder_module.build_derivatives_snapshot("BTC/USDT")

    assert snapshot is not None
    assert snapshot.payload["history_ready"] is True
    assert snapshot.payload["history_exchange"] == "Binance"
    assert snapshot.payload["history_interval"] == "h1"
    assert snapshot.payload["funding_zscore"] is not None
    assert snapshot.payload["funding_zscore"] > 0
    assert snapshot.payload["funding_reversion_speed"] is not None
    assert snapshot.payload["long_short_ratio_change_24h"] is not None
    assert snapshot.payload["long_short_ratio_change_24h"] > 0
    assert snapshot.payload["oi_change_1h_history"] is not None
    assert snapshot.payload["oi_change_24h_history"] is not None
    assert snapshot.payload["crowded_long"] is True
    assert snapshot.payload["squeeze_building"] is True
    assert snapshot.payload["basis_dislocation"] is True
    assert snapshot.payload["order_flow_confirmed"] is True
    assert "crowded_long" in snapshot.payload["derivatives_labels"]
    assert "squeeze_building" in snapshot.payload["derivatives_labels"]
    assert "basis_dislocation" in snapshot.payload["derivatives_labels"]
    context = builder_module.build_coinglass_runtime_context(snapshot.to_dict())
    assert context["history_ready"] is True
    assert context["history_exchange"] == "Binance"
    assert context["derivatives_heat_score"] == snapshot.payload["derivatives_heat_score"]


def test_build_derivatives_snapshot_flags_flow_divergence(monkeypatch):
    start = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)
    oi_history_rows = []
    funding_history_rows = []
    ratio_rows = []
    for offset in range(25):
        ts = (start + timedelta(hours=offset)).isoformat()
        oi_close = 100.0 + (offset * 5.0)
        funding_close = 0.0003 + (offset * 0.00001)
        ratio_value = 1.0 + (offset * 0.002)
        oi_history_rows.append({"_source_ts": ts, "_exchange": "Binance", "_interval": "h1", "close": oi_close})
        funding_history_rows.append({"_source_ts": ts, "_exchange": "Binance", "_interval": "h1", "close": funding_close})
        ratio_rows.append({"_source_ts": ts, "_exchange": "Binance", "_interval": "h1", "long_short_ratio": ratio_value})

    monkeypatch.setattr(
        builder_module,
        "_dataset_payloads",
        lambda symbol: {
            "open_interest_exchange_list": [{"exchange": "All", "symbol": "BTC", "open_interest_usd": 220.0}],
            "open_interest_history": oi_history_rows,
            "funding_rate_exchange_list": [
                {"exchange": "Binance", "symbol": "BTC", "margin_type": "stablecoin", "funding_rate": 0.0006}
            ],
            "funding_rate_history": funding_history_rows,
            "taker_buy_sell_volume_exchange_list": [
                {"exchange": "Binance", "taker_buy_volume": 20.0, "taker_sell_volume": 80.0}
            ],
            "liquidation_history": [{"long_liquidation_usd": 1_000_000.0, "short_liquidation_usd": 2_000_000.0}],
            "global_long_short_account_ratio_history": ratio_rows,
            "funding_arbitrage": [],
        },
    )

    snapshot = builder_module.build_derivatives_snapshot("BTC/USDT")

    assert snapshot is not None
    assert snapshot.payload["flow_divergence"] is True
    assert snapshot.payload["flow_divergence_score"] >= 0.55
    assert snapshot.payload["order_flow_confirmed"] is False


def test_latest_payload_rows_prefers_latest_ingested_request_and_filters_mismatched_symbol(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "normalized_symbol": "BTC",
                "request_key": "req-old",
                "source_ts": "2026-04-18T12:30:00+00:00",
                "ingested_at": "2026-04-18T11:00:00+00:00",
                "exchange": "aggregate",
                "payload_json": '{"symbol":"ALLINDEX","spread":0.5}',
            },
            {
                "normalized_symbol": "BTC",
                "request_key": "req-new",
                "source_ts": "2026-04-17T20:00:00+00:00",
                "ingested_at": "2026-04-18T12:00:00+00:00",
                "exchange": "aggregate",
                "payload_json": '{"symbol":"BTC","spread":0.1}',
            },
        ]
    )
    monkeypatch.setattr(builder_module, "load_dataset_rows_for_symbol", lambda dataset, symbol: frame)

    rows = builder_module._latest_payload_rows("funding_arbitrage", "BTC")

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC"


def test_build_coinglass_overview_prefers_snapshot_active_datasets(monkeypatch):
    async def fake_load_snapshot(symbol: str):
        return {
            "timestamp": "2026-04-18T12:00:00+00:00",
            "payload": {
                "active_datasets": ["open_interest_exchange_list", "funding_rate_exchange_list"],
                "payload_counts": {"open_interest_exchange_list": 3, "funding_rate_exchange_list": 2},
            },
        }

    async def fake_statuses(*, symbol=None):
        return [
            {"dataset": "open_interest_exchange_list", "status": "failed", "rows_written": 0},
            {"dataset": "funding_rate_exchange_list", "status": "empty", "rows_written": 0},
        ]

    async def fake_budget():
        class _Budget:
            minute_remaining = 8
            daily_remaining = 49900
            monthly_remaining = 499000
            key_configured = True

        return _Budget()

    monkeypatch.setattr(builder_module, "load_latest_derivatives_snapshot", fake_load_snapshot)
    monkeypatch.setattr(builder_module, "load_coinglass_ingest_statuses", fake_statuses)
    monkeypatch.setattr(builder_module, "get_coinglass_budget_state", fake_budget)

    payload = asyncio.run(builder_module.build_coinglass_overview_payload("BTC/USDT"))

    assert payload["available"] is True
    assert payload["active_datasets"] == sorted(["open_interest_exchange_list", "funding_rate_exchange_list"])


def test_normalize_taker_history_row_computes_imbalance():
    row = {"buyVolUsd": 1_000_000.0, "sellVolUsd": 600_000.0}
    result = client_module._normalize_taker_row(row)
    assert result["taker_buy_volume"] == 1_000_000.0
    assert result["taker_sell_volume"] == 600_000.0
    assert abs(result["taker_buy_sell_imbalance"] - (400_000.0 / 1_600_000.0)) < 1e-9


def test_feature_builder_computes_taker_imbalance_from_history():
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    taker_history_rows = [
        {"_source_ts": (now - timedelta(hours=i)).isoformat(), "taker_buy_sell_imbalance": 0.10 + i * 0.01}
        for i in range(5, -1, -1)
    ]
    series = builder_module._history_value_series(taker_history_rows, "taker_buy_sell_imbalance")
    bars_1h = 1
    bars_4h = 4
    imbalance_1h = builder_module._series_mean(series, bars_1h)
    imbalance_4h = builder_module._series_mean(series, bars_4h)
    assert imbalance_1h is not None
    assert imbalance_4h is not None
    assert abs(imbalance_1h - series[-1][1]) < 1e-9
    assert imbalance_4h > imbalance_1h
