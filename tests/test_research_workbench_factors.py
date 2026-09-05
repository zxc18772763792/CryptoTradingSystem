from __future__ import annotations

import asyncio

from web.api import research as research_api


def test_compact_fama_rejects_zero_filled_async_placeholder():
    placeholder = {
        "points": 0,
        "universe_size": 0,
        "latest": {"MKT": 0.0, "MOM": 0.0},
        "series": [],
        "error": "Fama 因子正在后台计算",
        "served_mode": "fallback",
    }

    assert research_api._compact_fama(placeholder) == {}


def test_compact_fama_keeps_legitimate_flat_series():
    payload = {
        "points": 2,
        "universe_size": 2,
        "latest": {"MKT": 0.0, "MOM": 0.0},
        "series": [
            {"timestamp": "2026-07-16T00:00:00Z", "MKT": 0.0, "MOM": 0.0},
            {"timestamp": "2026-07-16T00:05:00Z", "MKT": 0.0, "MOM": 0.0},
        ],
        "served_mode": "live",
    }

    compact = research_api._compact_fama(payload)

    assert compact["points"] == 2
    assert len(compact["series"]) == 2
    assert compact["served_mode"] == "live"


def test_workbench_factors_reuses_factor_library_while_fama_is_warming(monkeypatch):
    async def fake_factor_library(**kwargs):
        return {
            "exchange": "binance",
            "timeframe": "5m",
            "symbols_used": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "points": 900,
            "factors": ["MKT", "SMB", "HML", "MOM", "RMW", "CMA", "VOL"],
            "universe_size": 3,
            "universe_quality": "normal",
            "latest": {"MKT": 0.0012, "MOM": -0.0034, "SMB": 0.0008},
            "mean_24": {"MKT": 0.0004, "MOM": 0.0011, "SMB": 0.0002},
            "std_24": {"MKT": 0.002, "MOM": 0.003, "SMB": 0.001},
            "series": [
                {
                    "timestamp": "2026-07-16T00:00:00Z",
                    "MKT": 0.001,
                    "SMB": 0.0004,
                    "HML": -0.0002,
                    "MOM": 0.0025,
                    "RMW": 0.0001,
                    "CMA": -0.0003,
                    "VOL": 0.0006,
                }
            ],
            "asset_scores": [{"symbol": "BTC/USDT", "score": 0.5}],
            "warnings": [],
        }

    async def fake_fama(**kwargs):
        return {
            "points": 0,
            "universe_size": 0,
            "latest": {"MKT": 0.0, "MOM": 0.0},
            "series": [],
            "error": "Fama 因子正在后台计算",
            "served_mode": "fallback",
        }

    async def fake_cross_asset(**kwargs):
        return {}

    monkeypatch.setattr(research_api, "get_factor_library", fake_factor_library)
    monkeypatch.setattr(research_api, "get_fama_like_factors", fake_fama)
    monkeypatch.setattr(research_api, "get_multi_assets_overview", fake_cross_asset)

    profile = research_api.ResearchProfile(
        primary_symbol="BTC/USDT",
        universe_symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"],
        timeframe="5m",
        lookback=1200,
    )
    module = asyncio.run(research_api._build_factors_module(profile))

    fama = module["payload"]["fama"]
    assert module["status"] == "degraded"
    assert module["summary"]["mom"] == -0.0034
    assert fama["served_mode"] == "factor_library_fallback"
    assert fama["points"] == 900
    assert fama["latest"]["MOM"] == -0.0034
    assert fama["series"][0]["MOM"] == 0.0025
    assert any("复用因子库" in warning for warning in module["warnings"])
