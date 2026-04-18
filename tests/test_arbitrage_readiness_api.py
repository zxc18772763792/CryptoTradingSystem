from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import data as data_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    return app


def _make_frame(bars: int) -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=bars, freq="h", tz="UTC")
    return pd.DataFrame({"close": [float(i + 1) for i in range(bars)]}, index=index)


def _make_pairs_loader(primary_bars: int, secondary_bars: int):
    async def fake_load_backtest_inputs(strategy: str, symbol: str, timeframe: str, params: dict[str, Any]):
        primary_symbol = str(symbol or "BTC/USDT")
        pair_symbol = str(params.get("pair_symbol") or "ETH/USDT")
        primary_df = _make_frame(primary_bars)
        secondary_df = _make_frame(secondary_bars)
        market_bundle = {
            primary_symbol: primary_df,
            pair_symbol: secondary_df,
        }
        return primary_df, market_bundle, primary_symbol

    return fake_load_backtest_inputs


def _make_single_loader(bars: int):
    async def fake_load_backtest_inputs(strategy: str, symbol: str, timeframe: str, params: dict[str, Any]):
        frame = _make_frame(bars)
        resolved_symbol = str(symbol or "BTC/USDT")
        return frame, {resolved_symbol: frame}, resolved_symbol

    return fake_load_backtest_inputs


def test_arbitrage_readiness_route_flags_data_gap_before_backtest(monkeypatch):
    calls = {"backtest": 0}

    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": True, "description": "近似回测"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_pairs_loader(primary_bars=120, secondary_bars=120))

    def fake_run_backtest_core(**kwargs):
        calls["backtest"] += 1
        return {}

    monkeypatch.setattr(data_api, "_run_backtest_core", fake_run_backtest_core)

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "PairsTradingStrategy",
                "symbol": "BTC/USDT",
                "pair_symbol": "ETH/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert calls["backtest"] == 0
    assert payload["data_status"]["ready"] is False
    assert payload["recommended_action"] == "先补数据"
    assert payload["backtest_status"]["headline"] == "近似价差回测"
    assert payload["cost_status"]["status"] == "unknown"
    assert payload["gates"]["backtest_required"] is True
    assert payload["gates"]["blocked_reasons"][0].startswith("双腿历史数据不足")


def test_arbitrage_readiness_route_marks_live_only_strategies_as_observe(monkeypatch):
    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": False, "reason": "依赖实时盘口 / 跨场所执行"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_single_loader(bars=1400))

    def fake_run_backtest_core(**kwargs):
        raise AssertionError("live-only strategy should not run lightweight backtest")

    monkeypatch.setattr(data_api, "_run_backtest_core", fake_run_backtest_core)

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "CEXArbitrageStrategy",
                "symbol": "BTC/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data_status"]["ready"] is True
    assert payload["backtest_status"]["headline"] == "仅实时验证"
    assert payload["cost_status"]["headline"] == "仅实时验证"
    assert payload["entry_status"]["headline"] == "仅实时验证"
    assert payload["recommended_action"] == "加入观察"
    assert payload["gates"]["live_only"] is True
    assert payload["gates"]["backtest_required"] is False


def test_arbitrage_readiness_route_surfaces_cost_consumes_edge(monkeypatch):
    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": True, "description": "近似回测"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_pairs_loader(primary_bars=1400, secondary_bars=1400))
    monkeypatch.setattr(
        data_api,
        "_run_backtest_core",
        lambda **kwargs: {
            "gross_total_return": 6.4,
            "total_return": -0.8,
            "cost_drag_return_pct": 7.2,
            "estimated_trade_cost_usd": 42.5,
            "quality_flag": "ok",
            "anomaly_bar_ratio": 0.0,
            "signal_bias": "long_spread_bias",
            "z_score_last": 2.8,
        },
    )

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "PairsTradingStrategy",
                "symbol": "BTC/USDT",
                "pair_symbol": "ETH/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data_status"]["ready"] is True
    assert payload["entry_status"]["entry_ready"] is True
    assert payload["cost_status"]["status"] == "fail"
    assert payload["cost_status"]["headline"] == "成本吞噬边际"
    assert payload["recommended_action"] == "先回测"
    assert payload["gates"]["cost_blocked"] is True
    assert "手续费/滑点已吃掉策略边际" in "".join(payload["gates"]["blocked_reasons"])


def test_arbitrage_readiness_route_allows_open_when_all_gates_pass(monkeypatch):
    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": True, "description": "近似回测"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_pairs_loader(primary_bars=1500, secondary_bars=1500))
    monkeypatch.setattr(
        data_api,
        "_run_backtest_core",
        lambda **kwargs: {
            "gross_total_return": 12.6,
            "total_return": 10.9,
            "cost_drag_return_pct": 1.7,
            "estimated_trade_cost_usd": 18.3,
            "quality_flag": "ok",
            "anomaly_bar_ratio": 0.0,
            "signal_bias": "short_spread_bias",
            "z_score_last": -3.1,
        },
    )

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "PairsTradingStrategy",
                "symbol": "BTC/USDT",
                "pair_symbol": "ETH/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data_status"]["ready"] is True
    assert payload["backtest_status"]["headline"] == "近似价差回测"
    assert payload["cost_status"]["status"] == "pass"
    assert payload["entry_status"]["state"] == "short_spread"
    assert payload["entry_status"]["entry_ready"] is True
    assert payload["recommended_action"] == "允许开仓"
    assert payload["gates"]["live_only"] is False
    assert payload["gates"]["backtest_required"] is False
    assert payload["gates"]["entry_ready"] is True


def test_arbitrage_readiness_route_surfaces_coinglass_risk_cards_for_pairs(monkeypatch):
    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": True, "description": "pairs_backtest"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_pairs_loader(primary_bars=1500, secondary_bars=1500))
    monkeypatch.setattr(
        data_api,
        "_run_backtest_core",
        lambda **kwargs: {
            "gross_total_return": 12.6,
            "total_return": 10.9,
            "cost_drag_return_pct": 1.7,
            "estimated_trade_cost_usd": 18.3,
            "quality_flag": "ok",
            "anomaly_bar_ratio": 0.0,
            "signal_bias": "short_spread_bias",
            "z_score_last": -3.1,
        },
    )
    monkeypatch.setattr(data_api.settings, "COINGLASS_ENABLED", True, raising=False)

    async def fake_coinglass_overview(symbol: str, refresh: bool = False, manual: bool = False):
        if symbol == "BTC/USDT":
            return {
                "available": True,
                "snapshot": {
                    "symbol": symbol,
                    "basis_pct": 0.034,
                    "payload": {
                        "history_ready": True,
                        "funding_zscore": 2.1,
                        "basis_dislocation": True,
                        "liquidation_burst_score": 0.71,
                        "long_short_ratio_change_24h": 0.12,
                        "derivatives_labels": ["basis_dislocation", "crowded_long"],
                    },
                },
            }
        return {
            "available": True,
            "snapshot": {
                "symbol": symbol,
                "basis_pct": 0.015,
                "payload": {
                    "history_ready": False,
                    "funding_zscore": 1.6,
                    "derivatives_labels": ["history_incomplete"],
                },
            },
        }

    monkeypatch.setattr(data_api, "build_coinglass_overview_payload", fake_coinglass_overview)

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "PairsTradingStrategy",
                "symbol": "BTC/USDT",
                "pair_symbol": "ETH/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["derivatives_overlay"]["available"] is True
    assert payload["derivatives_overlay"]["sample_size"] == 2
    assert payload["derivatives_overlay"]["alert_count"] == 2
    assert payload["gates"]["derivatives_alert"] is True
    assert payload["derivatives_overlay"]["cards"][0]["symbol"] == "BTC/USDT"
    assert payload["derivatives_overlay"]["cards"][0]["headline"] == "Funding / Basis 异常"
    assert "basis_dislocation" in payload["derivatives_overlay"]["cards"][0]["labels"]


def test_arbitrage_readiness_route_adds_coinglass_alerts_for_cex_arbitrage(monkeypatch):
    monkeypatch.setattr(
        data_api,
        "get_backtest_strategy_info",
        lambda strategy: {"supported": False, "reason": "live_only"},
    )
    monkeypatch.setattr(data_api, "_load_backtest_inputs", _make_single_loader(bars=1400))
    monkeypatch.setattr(data_api.settings, "COINGLASS_ENABLED", True, raising=False)

    async def fake_coinglass_overview(symbol: str, refresh: bool = False, manual: bool = False):
        return {
            "available": True,
            "snapshot": {
                "symbol": symbol,
                "basis_pct": 0.028,
                "payload": {
                    "history_ready": True,
                    "funding_zscore": 1.9,
                    "order_flow_confirmed": True,
                    "derivatives_labels": ["order_flow_confirmed", "basis_dislocation"],
                },
            },
        }

    monkeypatch.setattr(data_api, "build_coinglass_overview_payload", fake_coinglass_overview)

    with TestClient(_build_app()) as client:
        response = client.get(
            "/api/data/research/arbitrage-readiness",
            params={
                "strategy": "CEXArbitrageStrategy",
                "symbol": "BTC/USDT",
                "timeframe": "1h",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_action"] == "加入观察"
    assert payload["derivatives_overlay"]["sample_size"] == 1
    assert payload["derivatives_overlay"]["alert_count"] == 1
    assert payload["gates"]["derivatives_alert"] is True
    assert payload["derivatives_overlay"]["cards"][0]["headline"] == "Funding / Basis 异常"
    assert any("funding_z" in item for item in payload["derivatives_overlay"]["alerts"])
