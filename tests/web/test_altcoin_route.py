from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import altcoin as altcoin_api


def _scan_payload():
    rows = [
        {
            "symbol": "AAA/USDT",
            "layout_score": 0.81,
            "alert_score": 0.62,
            "anomaly_score": 0.58,
            "accumulation_score": 0.78,
            "control_score": 0.49,
            "chain_confirmation_score": 0.52,
            "derivatives_heat_score": 0.66,
            "squeeze_score": 0.61,
            "crowding_risk_score": 0.22,
            "liquidity_trap_score": 0.18,
            "flow_confirmation_score": 0.57,
            "risk_penalty": 0.08,
            "signal_state": "布局吸筹",
            "tags": ["布局吸筹"],
            "reasons_proxy": ["量价压缩后承接增强"],
            "reasons_chain": ["community flow 偏正"],
            "data_quality": {
                "market_data_freshness": 0.91,
                "snapshot_freshness": 0.74,
                "derivatives_data_freshness": 0.82,
                "chain_quality": 0.66,
                "derivatives_present": True,
                "degraded_reason": [],
            },
            "freshness": {
                "market_label": "fresh",
                "snapshot_label": "fresh",
                "derivatives_label": "fresh",
                "derivatives_age_sec": 180.0,
            },
            "derivatives_context": {
                "available": True,
                "timestamp": "2026-04-18T11:57:00+00:00",
                "age_sec": 180.0,
                "freshness_label": "fresh",
                "source_name": "coinglass_cache",
                "capture_status": "ok",
                "source_error": None,
            },
            "metrics": {
                "return_1_bar": 0.02,
                "return_3_bar": 0.05,
                "return_6_bar": 0.09,
                "volume_burst_ratio": 1.8,
                "range_expansion_ratio": 1.2,
                "spread_bps": 5.0,
                "order_flow_imbalance": 0.11,
                "whale_count": 2,
                "oi_change_1h": 0.08,
                "funding_rate": 0.0003,
                "basis_pct": 0.0015,
                "long_short_ratio": 1.08,
                "taker_buy_sell_imbalance": 0.19,
                "crowding_score": 0.44,
                "distribution_score": 0.23,
                "depth_thinness_score": 0.18,
                "percentiles": {
                    "return_shock": 0.55,
                    "volume_burst": 0.52,
                    "range_expansion": 0.49,
                    "compression_inverse": 0.80,
                    "drift_stability": 0.72,
                    "absorption_proxy": 0.68,
                    "close_control": 0.51,
                    "liquidity_thinness": 0.42,
                    "community_flow": 0.64,
                    "announcements": 0.47,
                    "funding_basis": 0.38,
                    "whale_context": 0.41,
                    "derivatives_heat": 0.74,
                    "squeeze_signal": 0.69,
                    "crowding_risk": 0.24,
                    "liquidity_trap": 0.21,
                    "flow_confirmation": 0.62,
                },
            },
            "sparkline": [100, 101, 102, 104],
            "has_alert_rule": False,
        },
        {
            "symbol": "BBB/USDT",
            "layout_score": 0.65,
            "alert_score": 0.88,
            "anomaly_score": 0.91,
            "accumulation_score": 0.42,
            "control_score": 0.63,
            "chain_confirmation_score": 0.57,
            "derivatives_heat_score": 0.72,
            "squeeze_score": 0.83,
            "crowding_risk_score": 0.71,
            "liquidity_trap_score": 0.36,
            "flow_confirmation_score": 0.64,
            "risk_penalty": 0.11,
            "signal_state": "异动启动",
            "tags": ["异动启动"],
            "reasons_proxy": ["收益冲击显著抬升"],
            "reasons_chain": ["announcements 偏强"],
            "data_quality": {
                "market_data_freshness": 0.87,
                "snapshot_freshness": 0.69,
                "derivatives_data_freshness": 0.58,
                "chain_quality": 0.61,
                "derivatives_present": True,
                "degraded_reason": [],
            },
            "freshness": {
                "market_label": "fresh",
                "snapshot_label": "watch",
                "derivatives_label": "watch",
                "derivatives_age_sec": 960.0,
            },
            "derivatives_context": {
                "available": True,
                "timestamp": "2026-04-18T11:44:00+00:00",
                "age_sec": 960.0,
                "freshness_label": "watch",
                "source_name": "coinglass_cache",
                "capture_status": "ok",
                "source_error": None,
            },
            "metrics": {
                "return_1_bar": 0.06,
                "return_3_bar": 0.13,
                "return_6_bar": 0.18,
                "volume_burst_ratio": 2.4,
                "range_expansion_ratio": 1.9,
                "spread_bps": 8.0,
                "order_flow_imbalance": 0.18,
                "whale_count": 4,
                "oi_change_1h": 0.12,
                "funding_rate": 0.0011,
                "basis_pct": 0.0044,
                "long_short_ratio": 1.16,
                "taker_buy_sell_imbalance": 0.31,
                "crowding_score": 0.79,
                "distribution_score": 0.33,
                "depth_thinness_score": 0.29,
                "percentiles": {
                    "return_shock": 0.94,
                    "volume_burst": 0.89,
                    "range_expansion": 0.86,
                    "compression_inverse": 0.31,
                    "drift_stability": 0.35,
                    "absorption_proxy": 0.27,
                    "close_control": 0.63,
                    "liquidity_thinness": 0.55,
                    "community_flow": 0.58,
                    "announcements": 0.71,
                    "funding_basis": 0.55,
                    "whale_context": 0.68,
                    "derivatives_heat": 0.82,
                    "squeeze_signal": 0.88,
                    "crowding_risk": 0.79,
                    "liquidity_trap": 0.33,
                    "flow_confirmation": 0.72,
                },
            },
            "sparkline": [100, 103, 107, 112],
            "has_alert_rule": True,
        },
    ]
    return {
        "exchange": "binance",
        "timeframe": "4h",
        "rows": rows,
        "symbols_requested": ["AAA/USDT", "BBB/USDT"],
        "symbols_used": ["AAA/USDT", "BBB/USDT"],
        "excluded_retired": [],
        "warnings": [],
        "generated_at": "2026-04-18T12:00:00+00:00",
        "cache": {"cache_key": "demo", "hit": False, "age_sec": 0.0, "ttl_sec": 300},
    }


def test_altcoin_scan_route_sorts_and_limits(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    monkeypatch.setattr(altcoin_api, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    response = client.get("/api/altcoin/radar/scan?sort_by=alert&limit=1&symbols=AAA/USDT,BBB/USDT")
    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"]["sort_by"] == "alert"
    assert payload["scan_meta"]["limit"] == 1
    assert payload["scan_meta"]["row_count_before_limit"] == 2
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["symbol"] == "BBB/USDT"
    assert payload["rows"][0]["rank"] == 1


def test_altcoin_detail_route_returns_selected_row(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    async def fake_get_onchain_overview(**kwargs):
        return {"context": "ok", "symbol": kwargs["symbol"]}

    monkeypatch.setattr(altcoin_api, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api, "get_onchain_overview", fake_get_onchain_overview)

    response = client.get("/api/altcoin/radar/detail?symbol=AAA/USDT&symbols=AAA/USDT,BBB/USDT")
    assert response.status_code == 200
    payload = response.json()

    assert payload["selected_row"]["symbol"] == "AAA/USDT"
    assert payload["proxy_breakdown"]["scores"]["layout"] == 0.81
    assert payload["proxy_breakdown"]["scores"]["derivatives_heat"] == 0.66
    assert any(item["label"] == "Derivatives Heat" for item in payload["proxy_breakdown"]["components"])
    assert any(item["label"] == "flow confirmation" for item in payload["chain_breakdown"]["components"])
    assert payload["derivatives_context"]["freshness_label"] == "fresh"
    assert payload["derivatives_context"]["source_name"] == "coinglass_cache"
    assert payload["selected_row"]["freshness"]["derivatives_label"] == "fresh"
    assert payload["selected_row"]["metrics"]["long_short_ratio"] == 1.08
    assert payload["chain_breakdown"]["onchain_context"] == {"context": "ok", "symbol": "AAA/USDT"}
    assert payload["scan_meta"]["exchange"] == "binance"
