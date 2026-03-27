from __future__ import annotations

import asyncio

import numpy as np
import pandas as pd


def _series_bundle(rows: int = 720) -> dict[str, pd.Series]:
    idx = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    rng = np.random.default_rng(seed=17)

    base = pd.Series(300 + np.cumsum(rng.normal(0, 1.0, rows)), index=idx).clip(lower=50)
    pair_good = pd.Series(base.values * 0.94 + 8 + rng.normal(0, 0.5, rows), index=idx).clip(lower=20)
    alt = pd.Series(180 + np.cumsum(rng.normal(0, 1.8, rows)), index=idx).clip(lower=20)
    alt_pair = pd.Series(130 + np.cumsum(rng.normal(0, 3.4, rows)), index=idx).clip(lower=10)

    return {
        "AAA/USDT": base,
        "BBB/USDT": pair_good,
        "CCC/USDT": alt,
        "DDD/USDT": alt_pair,
    }


def _inverse_pair_bundle(rows: int = 720) -> tuple[pd.Series, pd.Series]:
    idx = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    rng = np.random.default_rng(seed=33)
    leader = pd.Series(120 + np.cumsum(rng.normal(0, 0.9, rows)), index=idx).clip(lower=20)
    follower = pd.Series(250 - 0.85 * leader.values + rng.normal(0, 0.8, rows), index=idx).clip(lower=20)
    return leader, follower


def test_pair_scan_metrics_prefers_positive_correlated_pair():
    from web.api import data as data_api

    bundle = _series_bundle()
    unrelated = pd.Series(
        90 + np.cumsum(np.random.default_rng(seed=99).normal(0, 4.2, len(bundle["AAA/USDT"]))),
        index=bundle["AAA/USDT"].index,
    ).clip(lower=5)

    good = data_api._pair_scan_pair_metrics(
        symbol1="AAA/USDT",
        symbol2="BBB/USDT",
        close1=bundle["AAA/USDT"],
        close2=bundle["BBB/USDT"],
        lookback=720,
    )
    bad = data_api._pair_scan_pair_metrics(
        symbol1="AAA/USDT",
        symbol2="ZZZ/USDT",
        close1=bundle["AAA/USDT"],
        close2=unrelated,
        lookback=720,
    )

    assert good is not None
    assert good["score"] > 0
    assert good["level_corr"] > 0.9
    assert good["return_corr"] > 0.4
    assert bad is None


def test_pair_scan_metrics_accepts_negative_correlated_pair():
    from web.api import data as data_api

    leader, follower = _inverse_pair_bundle()
    result = data_api._pair_scan_pair_metrics(
        symbol1="INV_A/USDT",
        symbol2="INV_B/USDT",
        close1=leader,
        close2=follower,
        lookback=720,
    )

    assert result is not None
    assert result["correlation_regime"] == "negative_corr"
    assert result["level_corr"] < -0.55
    assert result["return_corr"] < -0.15
    assert result["hedge_ratio"] < 0


def test_pairs_ranking_route_returns_top_pair(monkeypatch):
    from web.api import data as data_api

    bundle = _series_bundle()

    async def fake_get_research_symbols(exchange: str = "binance"):
        return {
            "exchange": exchange,
            "symbols": ["AAA/USDT", "BBB/USDT", "CCC/USDT", "DDD/USDT"],
        }

    def fake_expand(exchange: str, timeframe: str, requested: list[str], min_symbols: int, max_symbols: int, excluded_symbols=None):
        return requested[:max_symbols]

    async def fake_load_pair_scan_series(exchange: str, timeframe: str, symbols: list[str], lookback: int):
        return {symbol: bundle[symbol] for symbol in symbols if symbol in bundle}

    monkeypatch.setattr(data_api, "get_research_symbols", fake_get_research_symbols)
    monkeypatch.setattr(data_api, "_expand_symbols_with_local", fake_expand)
    monkeypatch.setattr(data_api, "_load_pair_scan_series", fake_load_pair_scan_series)

    payload = asyncio.run(
        data_api.get_research_pairs_ranking(
            exchange="binance",
            timeframe="1h",
            limit=5,
        )
    )

    assert payload["exchange"] == "binance"
    assert payload["timeframe"] == "1h"
    assert payload["lookback_period"] == 720
    assert payload["loaded_symbol_count"] == 4
    assert payload["eligible_pair_count"] >= 1
    assert len(payload["pairs"]) >= 1
    assert payload["pairs"][0]["primary_symbol"] == "AAA/USDT"
    assert payload["pairs"][0]["pair_symbol"] == "BBB/USDT"
    assert "AAA/USDT" in payload["top_symbols"]
    assert "BBB/USDT" in payload["top_symbols"]
