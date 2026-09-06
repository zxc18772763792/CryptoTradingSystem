from __future__ import annotations

import asyncio

import pandas as pd
import pytest
from fastapi import HTTPException

from core.data import alpha_market_data
from core.data.binance_alpha_collector import _SQLiteStore, _parse_kline_rows
from web.api import data as data_api


def _seed_alpha_database(path):
    store = _SQLiteStore(path)
    captured_at = "2026-09-06T00:05:00+00:00"
    rows = _parse_kline_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        "1m",
        [
            [1788652800000, "1", "2", "0.5", "1.5", "4", 1788652859999, "6", 2, "2", "3", "0"],
            [1788652860000, "1.5", "2.5", "1", "2", "5", 1788652919999, "10", 3, "3", "6", "0"],
        ],
        captured_at=captured_at,
    )
    store.persist_batch(
        captured_at=captured_at,
        tokens=[
            {
                "alphaId": "ALPHA_175",
                "officialSymbol": "ALPHA_175USDT",
                "symbol": "TEST",
                "name": "Test Alpha",
                "chainName": "BSC",
                "price": "2",
                "volume24h": "1000",
                "liquidity": "500",
            }
        ],
        klines=rows,
        trades=[],
        orderbooks=[],
        run_id="seed-run",
        started_at="2026-09-06T00:00:00+00:00",
        finished_at=captured_at,
        run_status="ok",
        summary={"kline_rows": len(rows)},
    )


def test_alpha_read_model_lists_and_loads_collector_data(tmp_path):
    database = tmp_path / "alpha_market.db"
    _seed_alpha_database(database)

    catalog = alpha_market_data.list_alpha_symbols(path=database)
    frame = alpha_market_data.load_alpha_klines(
        symbol="ALPHA175/USDT",
        timeframe="1m",
        limit=1,
        align="tail",
        path=database,
    )
    coverage = alpha_market_data.get_alpha_coverage(
        symbol="ALPHA_175USDT",
        timeframe="1m",
        path=database,
    )
    ticker = alpha_market_data.load_alpha_ticker(symbol="ALPHA_175USDT", path=database)

    assert catalog["symbols"] == ["ALPHA175/USDT"]
    assert catalog["symbol_meta"]["ALPHA175/USDT"]["display_symbol"] == "TEST"
    assert catalog["symbol_meta"]["ALPHA175/USDT"]["quote_asset"] == "USDT"
    assert catalog["timeframes"] == ["1m"]
    assert list(frame["close"]) == [2.0]
    assert frame.index[0] == pd.Timestamp("2026-09-06T00:01:00")
    assert coverage["available"] is True
    assert coverage["rows"] == 2
    assert ticker["last"] == 2.0
    assert ticker["official_symbol"] == "ALPHA_175USDT"
    assert ticker["source_type"] == "collector_sqlite"


def test_alpha_kline_api_never_calls_parquet_or_exchange(monkeypatch):
    index = pd.date_range("2026-09-06T00:00:00", periods=2, freq="1min")
    frame = pd.DataFrame(
        {
            "open": [1.0, 1.5],
            "high": [2.0, 2.5],
            "low": [0.5, 1.0],
            "close": [1.5, 2.0],
            "volume": [4.0, 5.0],
        },
        index=index,
    )

    def fake_alpha_load(**kwargs):
        assert kwargs["symbol"] == "ALPHA175/USDT"
        assert kwargs["limit"] == 100
        assert kwargs["align"] == "tail"
        return frame

    async def forbidden_parquet(**kwargs):
        raise AssertionError("Alpha must not read the normal Parquet store")

    async def forbidden_exchange(*args, **kwargs):
        raise AssertionError("Alpha must not call a normal exchange connector")

    monkeypatch.setattr(data_api, "load_alpha_klines", fake_alpha_load)
    monkeypatch.setattr(data_api.data_storage, "load_klines_from_parquet", forbidden_parquet)
    monkeypatch.setattr(data_api, "_safe_exchange_call", forbidden_exchange)

    result = asyncio.run(
        data_api.get_klines(
            exchange="binance",
            symbol="ALPHA175/USDT",
            timeframe="1m",
            limit=100,
        )
    )

    assert result["actual_exchange"] == "binance_alpha"
    assert result["source_type"] == "collector_sqlite"
    assert result["managed"] is True
    assert [row["close"] for row in result["data"]] == [1.5, 2.0]


def test_alpha_write_and_cross_source_routes_reject_synthetic_symbol():
    with pytest.raises(HTTPException) as repair_error:
        asyncio.run(
            data_api.repair_data_integrity(
                exchange="binance",
                symbol="ALPHA175/USDT",
                timeframe="1m",
            )
        )
    with pytest.raises(HTTPException) as cross_error:
        asyncio.run(
            data_api.cross_validate_data(
                symbol="ALPHA175/USDT",
                timeframe="1m",
                primary_exchange="binance",
                secondary_exchange="gate",
            )
        )

    assert repair_error.value.status_code == 409
    assert cross_error.value.status_code == 409


def test_alpha_symbol_detection_does_not_capture_regular_assets():
    assert alpha_market_data.is_alpha_symbol("ALPHA175/USDT") is True
    assert alpha_market_data.is_alpha_symbol("ALPHA_175USDC") is True
    assert alpha_market_data.is_alpha_symbol("ALPHA_10USDT") is True
    assert alpha_market_data.is_alpha_symbol("ALPHA/USDT") is False
    assert alpha_market_data.is_alpha_symbol("ALPHABET/USDT") is False


def test_alpha_ticker_routes_use_collector_snapshots(monkeypatch):
    ticker = {
        "exchange": "binance_alpha",
        "symbol": "ALPHA175/USDT",
        "official_symbol": "ALPHA_175USDC",
        "last": 1.25,
        "source": "binance_alpha_collector",
    }

    monkeypatch.setattr(data_api, "load_alpha_ticker", lambda **kwargs: dict(ticker))
    monkeypatch.setattr(
        data_api,
        "list_alpha_symbols",
        lambda: {
            "symbols": ["ALPHA175/USDT"],
            "symbol_meta": {
                "ALPHA175/USDT": {
                    "display_symbol": "TEST",
                    "official_symbol": "ALPHA_175USDC",
                    "quote_asset": "USDC",
                    "price": 1.25,
                    "price_change_24h": 5.0,
                    "volume_24h": 1000.0,
                }
            },
        },
    )

    single = asyncio.run(data_api.get_ticker("binance", "ALPHA_175USDC"))
    many = asyncio.run(data_api.get_tickers("binance_alpha"))

    assert single == ticker
    assert many["exchange"] == "binance_alpha"
    assert many["tickers"][0]["quote_asset"] == "USDC"
    assert many["tickers"][0]["change_24h"] == 0.05


def test_data_page_exposes_alpha_as_managed_source():
    template = (data_api._PROJECT_ROOT / "web/templates/index.html").read_text(encoding="utf-8")
    app_js = (data_api._PROJECT_ROOT / "web/static/js/app.js").read_text(encoding="utf-8")

    assert '<option value="binance_alpha">Binance Alpha（后台采集）</option>' in template
    assert 'id="data-source-status"' in template
    assert 'id="data-managed-sources"' in template
    assert "if(isManagedAlphaSource(exchange))return false;" in app_js
    assert "loadDataSourceStatus(ex)" in app_js
