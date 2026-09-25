from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import altcoin as altcoin_api


def test_alpha_symbol_detection_requires_numeric_collector_id():
    assert altcoin_api._is_alpha_symbol("ALPHA175/USDT") is True
    assert altcoin_api._is_alpha_symbol("ALPHA_175USDC") is True
    assert altcoin_api._is_alpha_symbol("ALPHA/USDT") is False
    assert altcoin_api._is_alpha_symbol("ALPHABET/USDT") is False


def test_public_exchange_snapshot_loader_skips_alpha_symbols(monkeypatch):
    calls = []

    async def forbidden_fetch(url):
        calls.append(url)
        raise AssertionError("Alpha symbols must not reach ordinary Binance tickers")

    monkeypatch.setattr(altcoin_api.scan, "_fetch_binance_public_tickers", forbidden_fetch)
    result = asyncio.run(
        altcoin_api._load_exchange_public_market_snapshots(
            exchange="binance",
            symbols=["ALPHA_175USDC"],
        )
    )

    assert result == {}
    assert calls == []


def _ops_headers() -> dict[str, str]:
    return {"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"}


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


def _notification_rule(
    *,
    rule_id: str,
    rule_type: str,
    symbol: str,
    exchange: str,
    timeframe: str,
    universe_symbols: list[str],
    score_key: str,
    threshold: float,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
    config_key: str = "",
    enabled: bool = True,
):
    return {
        "id": rule_id,
        "enabled": enabled,
        "rule_type": rule_type,
        "params": {
            "exchange": exchange,
            "timeframe": timeframe,
            "symbol": symbol,
            "universe_symbols": universe_symbols,
            "score_key": score_key,
            "threshold": threshold,
            "mode": mode,
            "view": view,
            "universe_scope": universe_scope,
            "config_key": config_key,
        },
    }


def test_altcoin_scan_route_sorts_and_limits(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    response = client.get("/api/altcoin/radar/scan?sort_by=alert&limit=1&symbols=AAA/USDT,BBB/USDT")
    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"]["sort_by"] == "alert"
    assert payload["scan_meta"]["limit"] == 1
    assert payload["scan_meta"]["row_count_before_limit"] == 2
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["symbol"] == "BBB/USDT"
    assert payload["rows"][0]["rank"] == 1


def test_altcoin_scan_route_dedupes_symbol_formats_and_tags(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    scan_payload = _scan_payload()
    duplicate = dict(scan_payload["rows"][0])
    duplicate["symbol"] = "AAAUSDT"
    duplicate["tags"] = ["布局吸筹", "布局吸筹"]
    scan_payload["rows"].append(duplicate)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        assert kwargs["symbols"] == ["AAA/USDT", "BBB/USDT"]
        return scan_payload

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    response = client.get(
        "/api/altcoin/radar/scan?sort_by=priority&limit=30&symbols=AAAUSDT,AAA-USDT,BBB/USDT"
    )

    assert response.status_code == 200
    payload = response.json()
    assert {row["symbol"] for row in payload["rows"]} == {"AAA/USDT", "BBB/USDT"}
    by_symbol = {row["symbol"]: row for row in payload["rows"]}
    assert by_symbol["AAA/USDT"]["tags"] == ["布局吸筹"]
    assert payload["scan_meta"]["row_count_before_limit"] == 2


def test_altcoin_radar_research_proposal_endpoint(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.research import orchestrator as orchestrator_module

    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_radar_detail(**kwargs):
        return {
            "selected_row": {
                "symbol": "AAAUSDT",
                "priority_score": 0.88,
                "signal_state": "ignition",
                "next_best_action": "open_research",
                "market_state_snapshot_id": "mss-1",
            },
            "action_plan": {
                "risk_hypothesis": "crowding can unwind quickly",
                "invalidate_conditions": ["priority falls below 0.5"],
            },
            "scan_meta": {"exchange": "binance", "timeframe": "1h"},
        }

    captured = {}

    def fake_create_manual_proposal(app_arg, **kwargs):
        captured.update(kwargs)
        now = datetime.now(timezone.utc)
        return ResearchProposal(
            proposal_id="proposal-radar",
            created_at=now,
            updated_at=now,
            status="draft",
            source=kwargs["source"],
            thesis=kwargs["thesis"],
            target_symbols=kwargs["symbols"],
            target_timeframes=kwargs["timeframes"],
            market_regime=kwargs["market_regime"],
            metadata=kwargs["metadata"],
        )

    monkeypatch.setattr(altcoin_api.detail, "get_altcoin_radar_detail", fake_get_altcoin_radar_detail)
    monkeypatch.setattr(orchestrator_module, "create_manual_proposal", fake_create_manual_proposal)
    monkeypatch.setenv("OPS_TOKEN", "test-token")

    response = client.post(
        "/api/altcoin/radar/AAAUSDT/research-proposal?timeframe=1h",
        headers=_ops_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["proposal_id"] == "proposal-radar"
    assert payload["next_action"] == "open_ai_research_proposal"
    assert captured["source"] == "hybrid"
    assert captured["metadata"]["origin_source"] == "altcoin_radar"
    assert captured["metadata"]["market_state_snapshot_id"] == "mss-1"


def test_altcoin_scan_route_uses_view_to_override_timeframe(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    captured = {}

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        captured.update(kwargs)
        return _scan_payload()

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    response = client.get("/api/altcoin/radar/scan?view=15m&timeframe=4h&mode=perp&universe_scope=expanded")
    assert response.status_code == 200
    assert captured["timeframe"] == "15m"
    assert captured["view"] == "15m"
    assert captured["mode"] == "perp"
    assert captured["universe_scope"] == "expanded"


def test_altcoin_detail_route_returns_selected_row(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    captured_onchain = {}

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    async def fake_get_onchain_overview(**kwargs):
        captured_onchain.update(kwargs)
        return {"context": "ok", "symbol": kwargs["symbol"]}

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.detail, "get_onchain_overview", fake_get_onchain_overview)

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
    assert captured_onchain["chain"] == "auto"


def test_altcoin_detail_route_accepts_view_and_mode(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    captured = {}

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        captured.update(kwargs)
        return _scan_payload()

    async def fake_get_onchain_overview(**kwargs):
        return {"context": "ok", "symbol": kwargs["symbol"]}

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.detail, "get_onchain_overview", fake_get_onchain_overview)

    response = client.get("/api/altcoin/radar/detail?symbol=AAA/USDT&view=15m&mode=perp&universe_scope=watchlist")
    assert response.status_code == 200
    assert captured["timeframe"] == "15m"
    assert captured["view"] == "15m"
    assert captured["mode"] == "perp"
    assert captured["universe_scope"] == "watchlist"


def test_resolve_universe_watchlist_scope_without_symbols_does_not_warn(monkeypatch):
    monkeypatch.setattr(altcoin_api.scan, "get_watchlist_symbols", lambda: ["ORDI/USDT", "PEPE/USDT"])

    def fake_retired_filter(**kwargs):
        return list(kwargs["requested"]), []

    monkeypatch.setattr(altcoin_api.scan, "_research_retired_filter", fake_retired_filter)

    requested, filtered, excluded_retired, warnings = asyncio.run(
        altcoin_api._resolve_universe(
            exchange="binance",
            timeframe="4h",
            symbols=[],
            exclude_retired=True,
            universe_scope="watchlist",
        )
    )

    assert requested == ["ORDI/USDT", "PEPE/USDT"]
    assert filtered == ["ORDI/USDT", "PEPE/USDT"]
    assert excluded_retired == []
    assert warnings == []


def test_resolve_universe_warns_only_when_explicit_symbols_fallback(monkeypatch):
    async def fake_get_research_symbols(exchange: str, include_major: bool = True):
        assert include_major is False
        return {"symbols": ["AAA/USDT", "BBB/USDT"]}

    def fake_retired_filter(**kwargs):
        requested = list(kwargs["requested"])
        if requested == ["BAD/USDT"]:
            return [], ["BAD/USDT"]
        return requested, []

    monkeypatch.setattr(altcoin_api.scan, "get_research_symbols", fake_get_research_symbols)
    monkeypatch.setattr(altcoin_api.scan, "_research_retired_filter", fake_retired_filter)

    requested, filtered, excluded_retired, warnings = asyncio.run(
        altcoin_api._resolve_universe(
            exchange="binance",
            timeframe="4h",
            symbols=["BAD/USDT"],
            exclude_retired=True,
            universe_scope="research",
        )
    )

    assert requested == ["AAA/USDT", "BBB/USDT"]
    assert filtered == ["AAA/USDT", "BBB/USDT"]
    assert excluded_retired == []
    assert warnings == ["传入的 symbols 过滤后为空或不可用，已回退到 research universe 默认币池。"]


def test_altcoin_detail_route_backfills_missing_chain_percentiles(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        payload = _scan_payload()
        payload["rows"][0]["metrics"]["percentiles"].update(
            {
                "community_flow": None,
                "announcements": None,
                "funding_basis": None,
                "whale_context": None,
            }
        )
        return payload

    async def fake_get_onchain_overview(**kwargs):
        return {"context": "ok", "symbol": kwargs["symbol"]}

    async def fake_load_detail_live_chain_context(**kwargs):
        return (
            {
                "flow_proxy": {"imbalance": 0.16, "buy_ratio": 0.62},
                "announcements": [{"title": "listing"}],
            },
            {
                "count": 3,
                "transactions": [{"btc": 12.0}],
            },
        )

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.detail, "get_onchain_overview", fake_get_onchain_overview)
    monkeypatch.setattr(altcoin_api.detail, "_load_detail_live_chain_context", fake_load_detail_live_chain_context)

    response = client.get("/api/altcoin/radar/detail?symbol=AAA/USDT&symbols=AAA/USDT,BBB/USDT")
    assert response.status_code == 200
    payload = response.json()

    components = {item["label"]: item["pctile"] for item in payload["chain_breakdown"]["components"]}
    assert components["community flow"] is not None
    assert components["announcements"] is not None
    assert components["funding/basis"] is not None
    assert components["whale context"] is not None
    assert payload["selected_row"]["metrics"]["percentiles"]["announcements"] is not None


def test_map_derivatives_rows_to_requested_symbols_matches_base_alias():
    rows = [
        SimpleNamespace(symbol="BTC", timestamp=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)),
        SimpleNamespace(symbol="ETH/USDT", timestamp=datetime(2026, 4, 18, 11, 59, tzinfo=timezone.utc)),
    ]

    matched = altcoin_api._map_derivatives_rows_to_requested_symbols(
        rows,
        requested_symbols=["BTC/USDT", "ETH/USDT"],
    )

    assert matched["BTC/USDT"].symbol == "BTC"
    assert matched["ETH/USDT"].symbol == "ETH/USDT"


def test_build_altcoin_notification_context_filters_benchmark_rows(monkeypatch):
    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {
            "generated_at": "2026-04-18T12:00:00+00:00",
            "warnings": [],
            "rows": [
                {
                    "symbol": "BTC/USDT",
                    "alt_eligible": False,
                    "layout_score": 0.92,
                    "alert_score": 0.88,
                    "control_score": 0.77,
                },
                {
                    "symbol": "AVAX/USDT",
                    "alt_eligible": True,
                    "layout_score": 0.71,
                    "alert_score": 0.83,
                    "control_score": 0.62,
                },
            ],
        }

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    context = asyncio.run(
        altcoin_api.build_altcoin_notification_context(
            [
                {
                    "id": "r1",
                    "enabled": True,
                    "rule_type": "altcoin_score_above",
                    "params": {
                        "config_key": "cfg-1",
                        "exchange": "binance",
                        "timeframe": "4h",
                        "symbol": "AVAX/USDT",
                        "universe_symbols": ["BTC/USDT", "AVAX/USDT"],
                    },
                }
            ]
        )
    )

    rows = context["scans"]["cfg-1"]["rows"]
    assert [row["symbol"] for row in rows] == ["AVAX/USDT"]
    assert "BTC/USDT" not in context["scans"]["cfg-1"]["sort_indexes"]["layout"]


def test_build_altcoin_notification_context_includes_event_driven_rule_types(monkeypatch):
    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {
            "generated_at": "2026-04-20T12:00:00+00:00",
            "warnings": [],
            "rows": [
                {
                    "symbol": "PEPE/USDT",
                    "alt_eligible": True,
                    "layout_score": 0.66,
                    "alert_score": 0.74,
                    "control_score": 0.41,
                    "narrative_heat_score": 0.81,
                    "event_flags": ["altcoin_narrative_heat_spike"],
                }
            ],
        }

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    context = asyncio.run(
        altcoin_api.build_altcoin_notification_context(
            [
                {
                    "id": "r-narrative",
                    "enabled": True,
                    "rule_type": "altcoin_narrative_heat_spike",
                    "params": {
                        "config_key": "cfg-narrative",
                        "exchange": "binance",
                        "timeframe": "1h",
                        "symbol": "PEPE/USDT",
                        "universe_symbols": ["PEPE/USDT"],
                        "mode": "narrative",
                        "view": "1h",
                        "universe_scope": "watchlist",
                    },
                }
            ]
        )
    )

    rows = context["scans"]["cfg-narrative"]["rows"]
    assert [row["symbol"] for row in rows] == ["PEPE/USDT"]
    assert context["scans"]["cfg-narrative"]["sort_indexes"]["narrative"]["PEPE/USDT"] == 1


def test_compute_scan_payload_exposes_contextual_alert_rules(monkeypatch):
    symbols = ["PEPE/USDT", "WIF/USDT"]
    config_key = altcoin_api.build_altcoin_notification_config_key(
        exchange="binance",
        timeframe="1h",
        universe_symbols=symbols,
        exclude_retired=True,
        mode="narrative",
        view="1h",
        universe_scope="watchlist",
    )

    async def fake_resolve_universe(**kwargs):
        return symbols, symbols, [], []

    async def fake_load_market_frames(**kwargs):
        return ({symbol: {"close": [1, 2, 3]} for symbol in symbols}, [])

    async def fake_market_snapshots(**kwargs):
        return {}

    async def fake_factor_library(**kwargs):
        return {}

    async def fake_multi_assets_overview(**kwargs):
        return {"retired_filter": {"excluded_symbols": []}}

    async def fake_snapshot_maps(**kwargs):
        return ({}, {}, {}, {})

    async def fake_load_active_altcoin_rules():
        return [
            _notification_rule(
                rule_id="r-match",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-mode-mismatch",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=symbols,
                score_key="narrative",
                threshold=0.65,
                mode="combined",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-view-mismatch",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="4h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-scope-mismatch",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="research",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-config-mismatch",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key="other-config",
            ),
        ]

    def fake_build_altcoin_rows(**kwargs):
        assert kwargs["alerted_symbols"] == ["PEPE/USDT"]
        return [
            {"symbol": "PEPE/USDT", "tags": ["叙事升温"]},
            {"symbol": "WIF/USDT", "tags": []},
        ]

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_coinglass_market_snapshots", fake_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "get_factor_library", fake_factor_library)
    monkeypatch.setattr(altcoin_api.scan, "get_multi_assets_overview", fake_multi_assets_overview)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fake_snapshot_maps)
    monkeypatch.setattr(altcoin_api.scan, "_load_active_altcoin_rules", fake_load_active_altcoin_rules)
    monkeypatch.setattr(altcoin_api.scan, "build_altcoin_rows", fake_build_altcoin_rows)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="1h",
            symbols=symbols,
            exclude_retired=True,
            refresh=False,
            universe_scope="watchlist",
            mode="narrative",
            view="1h",
        )
    )

    by_symbol = {row["symbol"]: row for row in payload["rows"]}
    assert by_symbol["PEPE/USDT"]["has_alert_rule"] is True
    assert by_symbol["PEPE/USDT"]["alert_rules"][0]["id"] == "r-match"
    assert by_symbol["PEPE/USDT"]["alert_rules"][0]["kind"] == "narrative"
    assert by_symbol["PEPE/USDT"]["alert_rules"][0]["config_key"] == config_key
    assert by_symbol["WIF/USDT"]["has_alert_rule"] is False
    assert by_symbol["WIF/USDT"]["alert_rules"] == []


def test_compute_scan_payload_uses_public_ticker_fallback_when_coinglass_unavailable(monkeypatch):
    symbols = ["AAA/USDT"]
    captured = {}

    async def fake_resolve_universe(**kwargs):
        return symbols, symbols, [], []

    async def fake_load_market_frames(**kwargs):
        return ({}, ["AAA/USDT local K empty skipped"])

    async def fake_market_snapshots(**kwargs):
        raise RuntimeError("minute_budget_exhausted")

    async def fake_public_snapshots(**kwargs):
        assert kwargs["symbols"] == symbols
        return {
            "AAA/USDT": {
                "symbol": "AAA/USDT",
                "timestamp": "2026-04-18T12:00:00+00:00",
                "source_name": "binance_futures_ticker_24h",
                "current_price": 12.5,
                "quote_volume_24h": 9_000_000.0,
                "price_change_percent_24h": 5.5,
            }
        }

    async def fake_factor_library(**kwargs):
        return {"warnings": []}

    async def fake_multi_assets_overview(**kwargs):
        return {"retired_filter": {"excluded_symbols": []}}

    async def fake_snapshot_maps(**kwargs):
        return ({}, {}, {}, {})

    async def fake_load_active_altcoin_rules():
        return []

    def fake_build_altcoin_rows(**kwargs):
        captured.update(kwargs)
        return [{"symbol": "AAA/USDT", "tags": [], "data_quality": {"degraded_reason": []}}]

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_coinglass_market_snapshots", fake_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "_load_exchange_public_market_snapshots", fake_public_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "get_factor_library", fake_factor_library)
    monkeypatch.setattr(altcoin_api.scan, "get_multi_assets_overview", fake_multi_assets_overview)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fake_snapshot_maps)
    monkeypatch.setattr(altcoin_api.scan, "_load_active_altcoin_rules", fake_load_active_altcoin_rules)
    monkeypatch.setattr(altcoin_api.scan, "build_altcoin_rows", fake_build_altcoin_rows)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="4h",
            symbols=symbols,
            exclude_retired=True,
            refresh=True,
            universe_scope="research",
            mode="combined",
            view="4h",
        )
    )

    assert payload["symbols_used"] == ["AAA/USDT"]
    assert captured["market_snapshots"]["AAA/USDT"]["source_name"] == "binance_futures_ticker_24h"
    assert captured["derivatives_snapshots"] == {}
    assert any("minute_budget_exhausted" in warning for warning in payload["warnings"])
    assert any("exchange public ticker" in warning for warning in payload["warnings"])
    assert not any("local K empty" in warning for warning in payload["warnings"])
    assert any("using live market snapshots instead" in warning for warning in payload["warnings"])


def test_compute_scan_payload_preserves_alpha_catalog_capture_time(monkeypatch):
    symbol = "ALPHA175/USDT"
    captured = {}

    async def fake_load_market_frames(**kwargs):
        return {}, []

    async def fake_load_alpha_token_catalog(**kwargs):
        return {
            "tokens": [{"alphaId": "ALPHA_175", "price": "1.5"}],
            "updated_at": "2026-09-05T00:00:00+00:00",
            "stale": True,
        }

    def fake_build_alpha_market_snapshots(tokens, *, timestamp=None):
        captured["tokens"] = tokens
        captured["timestamp"] = timestamp
        return {
            symbol: {
                "symbol": symbol,
                "timestamp": timestamp,
                "source_name": "binance_alpha",
                "current_price": 1.5,
            }
        }

    async def fake_snapshot_maps(**kwargs):
        return {}, {}, {}, {}

    async def fake_rules():
        return []

    def fake_build_rows(**kwargs):
        return [{"symbol": symbol, "tags": []}]

    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_alpha_token_catalog", fake_load_alpha_token_catalog)
    monkeypatch.setattr(altcoin_api.scan, "build_alpha_market_snapshots", fake_build_alpha_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fake_snapshot_maps)
    monkeypatch.setattr(altcoin_api.scan, "_load_active_altcoin_rules", fake_rules)
    monkeypatch.setattr(altcoin_api.scan, "build_altcoin_rows", fake_build_rows)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="1h",
            symbols=[symbol],
            exclude_retired=True,
            refresh=False,
            universe_scope="alpha",
            resolved_universe=([symbol], [symbol], [], []),
        )
    )

    assert payload["symbols_used"] == [symbol]
    assert captured["timestamp"] == "2026-09-05T00:00:00+00:00"


def test_compute_scan_payload_never_falls_back_to_exchange_for_alpha(monkeypatch):
    symbol = "ALPHA175/USDT"
    public_calls = []

    async def fake_load_market_frames(**kwargs):
        return {}, []

    async def fake_load_alpha_token_catalog(**kwargs):
        return {
            "tokens": [{"alphaId": "ALPHA_175", "price": "0"}],
            "updated_at": "2026-09-05T00:00:00+00:00",
            "stale": True,
        }

    async def fake_public_snapshots(**kwargs):
        public_calls.append(kwargs)
        return {}

    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_alpha_token_catalog", fake_load_alpha_token_catalog)
    monkeypatch.setattr(altcoin_api.scan, "build_alpha_market_snapshots", lambda *args, **kwargs: {})
    monkeypatch.setattr(altcoin_api.scan, "_load_exchange_public_market_snapshots", fake_public_snapshots)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="1h",
            symbols=[symbol],
            exclude_retired=True,
            refresh=False,
            universe_scope="alpha",
            resolved_universe=([symbol], [symbol], [], []),
        )
    )

    assert public_calls == []
    assert payload["symbols_used"] == []


def test_compute_scan_payload_excludes_stale_frames_from_factor_inputs(monkeypatch):
    symbols = ["AAA/USDT", "BBB/USDT"]
    now = datetime.now(timezone.utc)
    captured = {}

    stale_frame = pd.DataFrame(
        {"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.0], "volume": [10_000.0]},
        index=[datetime(2026, 1, 1, tzinfo=timezone.utc)],
    )
    fresh_frame = pd.DataFrame(
        {"open": [9.5], "high": [10.5], "low": [9.0], "close": [10.0], "volume": [5_000.0]},
        index=[now - timedelta(hours=1)],
    )

    async def fake_resolve_universe(**kwargs):
        return symbols, symbols, [], []

    async def fake_load_market_frames(**kwargs):
        return ({"AAA/USDT": stale_frame, "BBB/USDT": fresh_frame}, [])

    async def fake_market_snapshots(**kwargs):
        return {
            "AAA/USDT": {
                "symbol": "AAA/USDT",
                "timestamp": now.isoformat(),
                "source_name": "binance_futures_ticker_24h",
                "capture_status": "ok",
                "current_price": 12.5,
                "quote_volume_24h": 9_000_000.0,
                "price_change_percent_24h": 5.5,
            }
        }

    async def fake_public_snapshots(**kwargs):
        return {}

    async def fake_factor_library(**kwargs):
        captured["factor_symbols"] = kwargs["symbols"]
        return {"warnings": [], "asset_scores": [{"symbol": "BBB/USDT", "liquidity": 0.4}]}

    async def fake_multi_assets_overview(**kwargs):
        captured["multi_symbols"] = kwargs["symbols"]
        return {"retired_filter": {"excluded_symbols": []}, "assets": [{"symbol": "BBB/USDT"}], "correlation": {}}

    async def fake_snapshot_maps(**kwargs):
        captured["snapshot_symbols"] = list(kwargs["symbols"])
        return ({}, {}, {}, {})

    async def fake_load_active_altcoin_rules():
        return []

    def fake_build_altcoin_rows(**kwargs):
        captured["market_frames"] = dict(kwargs["market_frames"])
        captured["market_snapshots"] = dict(kwargs["market_snapshots"])
        return [
            {"symbol": "AAA/USDT", "tags": [], "data_quality": {"degraded_reason": []}},
            {"symbol": "BBB/USDT", "tags": [], "data_quality": {"degraded_reason": []}},
        ]

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_coinglass_market_snapshots", fake_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "_load_exchange_public_market_snapshots", fake_public_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "get_factor_library", fake_factor_library)
    monkeypatch.setattr(altcoin_api.scan, "get_multi_assets_overview", fake_multi_assets_overview)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fake_snapshot_maps)
    monkeypatch.setattr(altcoin_api.scan, "_load_active_altcoin_rules", fake_load_active_altcoin_rules)
    monkeypatch.setattr(altcoin_api.scan, "build_altcoin_rows", fake_build_altcoin_rows)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="4h",
            symbols=symbols,
            exclude_retired=True,
            refresh=True,
            universe_scope="research",
            mode="combined",
            view="4h",
        )
    )

    assert set(payload["symbols_used"]) == {"AAA/USDT", "BBB/USDT"}
    assert captured["factor_symbols"] == "BBB/USDT"
    assert captured["multi_symbols"] == "BBB/USDT"
    assert set(captured["snapshot_symbols"]) == {"AAA/USDT", "BBB/USDT"}
    assert set(captured["market_frames"]) == {"BBB/USDT"}
    assert captured["market_snapshots"]["AAA/USDT"]["current_price"] == 12.5
    assert any("Ignored stale local K-line frames for 1 symbols" in warning for warning in payload["warnings"])


def test_compute_scan_payload_drops_stale_frames_without_market_snapshot(monkeypatch):
    symbols = ["AAA/USDT"]
    stale_frame = pd.DataFrame(
        {"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.0], "volume": [10_000.0]},
        index=[datetime(2026, 1, 1, tzinfo=timezone.utc)],
    )

    async def fake_resolve_universe(**kwargs):
        return symbols, symbols, [], []

    async def fake_load_market_frames(**kwargs):
        return ({"AAA/USDT": stale_frame}, [])

    async def fake_market_snapshots(**kwargs):
        raise RuntimeError("coinglass_down")

    async def fake_public_snapshots(**kwargs):
        return {}

    async def fail_if_called(**kwargs):
        raise AssertionError("stale-only scan should not compute dependent payloads")

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_coinglass_market_snapshots", fake_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "_load_exchange_public_market_snapshots", fake_public_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "get_factor_library", fail_if_called)
    monkeypatch.setattr(altcoin_api.scan, "get_multi_assets_overview", fail_if_called)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fail_if_called)

    payload = asyncio.run(
        altcoin_api._compute_scan_payload(
            exchange="binance",
            timeframe="4h",
            symbols=symbols,
            exclude_retired=True,
            refresh=True,
            universe_scope="research",
            mode="combined",
            view="4h",
        )
    )

    assert payload["rows"] == []
    assert payload["symbols_used"] == []
    assert any("coinglass_down" in warning for warning in payload["warnings"])
    assert any("Exchange public ticker fallback returned no data for 1 symbols" in warning for warning in payload["warnings"])
    assert any("no fresh market snapshot" in warning for warning in payload["warnings"])


def test_get_altcoin_scan_snapshot_resolves_universe_only_once(monkeypatch):
    altcoin_api._clear_altcoin_scan_cache()
    resolve_calls = 0

    async def fake_resolve_universe(**kwargs):
        nonlocal resolve_calls
        resolve_calls += 1
        return ["AAA/USDT"], ["AAA/USDT"], [], ["预加载警告"]

    async def fake_load_market_frames(**kwargs):
        return ({"AAA/USDT": {"close": [1, 2, 3]}}, [])

    async def fake_market_snapshots(**kwargs):
        return {}

    async def fake_factor_library(**kwargs):
        return {"warnings": []}

    async def fake_multi_assets_overview(**kwargs):
        return {"retired_filter": {"excluded_symbols": []}}

    async def fake_snapshot_maps(**kwargs):
        return ({}, {}, {}, {})

    async def fake_load_active_altcoin_rules():
        return []

    def fake_build_altcoin_rows(**kwargs):
        return [{"symbol": "AAA/USDT", "tags": [], "data_quality": {}}]

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_load_market_frames", fake_load_market_frames)
    monkeypatch.setattr(altcoin_api.scan, "load_coinglass_market_snapshots", fake_market_snapshots)
    monkeypatch.setattr(altcoin_api.scan, "get_factor_library", fake_factor_library)
    monkeypatch.setattr(altcoin_api.scan, "get_multi_assets_overview", fake_multi_assets_overview)
    monkeypatch.setattr(altcoin_api.scan, "_load_snapshot_maps", fake_snapshot_maps)
    monkeypatch.setattr(altcoin_api.scan, "_load_active_altcoin_rules", fake_load_active_altcoin_rules)
    monkeypatch.setattr(altcoin_api.scan, "build_altcoin_rows", fake_build_altcoin_rows)

    payload = asyncio.run(
        altcoin_api.get_altcoin_scan_snapshot(
            exchange="binance",
            timeframe="4h",
            symbols=["AAA/USDT"],
            exclude_retired=True,
        )
    )

    assert resolve_calls == 1
    assert payload["rows"][0]["symbol"] == "AAA/USDT"
    assert "预加载警告" in payload["warnings"]
    assert payload["cache"]["hit"] is False
    altcoin_api._clear_altcoin_scan_cache()


def test_altcoin_scan_cache_lock_creation_is_thread_safe():
    cache_key = "race-key"
    altcoin_api._ALTCOIN_SCAN_LOCKS.pop(cache_key, None)

    def acquire_lock_id() -> int:
        return id(altcoin_api._cache_lock(cache_key))

    with ThreadPoolExecutor(max_workers=8) as executor:
        lock_ids = list(executor.map(lambda _: acquire_lock_id(), range(25)))

    assert len(set(lock_ids)) == 1
    assert altcoin_api._ALTCOIN_SCAN_LOCKS[cache_key] is altcoin_api._cache_lock(cache_key)

    altcoin_api._ALTCOIN_SCAN_LOCKS.pop(cache_key, None)


def test_get_altcoin_scan_snapshot_serves_stale_cache_while_refreshing(monkeypatch):
    altcoin_api._clear_altcoin_scan_cache()
    state = {"compute_calls": 0, "gate": None}

    async def fake_resolve_universe(**kwargs):
        return ["AAA/USDT"], ["AAA/USDT"], [], []

    async def fake_compute_scan_payload(**kwargs):
        state["compute_calls"] += 1
        await state["gate"].wait()
        return {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [{"symbol": "AAA/USDT", "tags": ["fresh"], "data_quality": {}}],
            "symbols_requested": ["AAA/USDT"],
            "symbols_used": ["AAA/USDT"],
            "excluded_retired": [],
            "warnings": ["fresh warning"],
            "generated_at": "2026-04-21T12:00:00+00:00",
            "universe_meta": {},
        }

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_compute_scan_payload", fake_compute_scan_payload)

    cache_key = altcoin_api._cache_key(
        exchange="binance",
        timeframe="4h",
        symbols=["AAA/USDT"],
        exclude_retired=True,
        mode="combined",
        view="4h",
        universe_scope="research",
    )
    altcoin_api._ALTCOIN_SCAN_CACHE[cache_key] = {
        "stored_at": time.time() - 900.0,
        "payload": {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [{"symbol": "AAA/USDT", "tags": ["cached"], "data_quality": {}}],
            "symbols_requested": ["AAA/USDT"],
            "symbols_used": ["AAA/USDT"],
            "excluded_retired": [],
            "warnings": ["cached warning"],
            "generated_at": "2026-04-21T11:55:00+00:00",
            "universe_meta": {},
        },
    }

    async def runner():
        state["gate"] = asyncio.Event()
        payload = await altcoin_api.get_altcoin_scan_snapshot(
            exchange="binance",
            timeframe="4h",
            symbols=["AAA/USDT"],
            exclude_retired=True,
            refresh=False,
            mode="combined",
            view="4h",
            universe_scope="research",
        )
        task = altcoin_api._ALTCOIN_SCAN_REFRESH_TASKS.get(cache_key)
        assert task is not None
        assert task.done() is False
        assert payload["rows"][0]["tags"] == ["cached"]
        assert payload["cache"]["hit"] is True
        assert payload["cache"]["stale"] is True
        assert payload["cache"]["refreshing"] is True
        assert payload["cache"]["served_mode"] == "stale_cache_refresh"
        assert any("background refresh" in warning for warning in payload["warnings"])
        state["gate"].set()
        refreshed = await asyncio.shield(task)
        return payload, refreshed

    payload, refreshed = asyncio.run(runner())

    assert state["compute_calls"] == 1
    assert payload["generated_at"] == "2026-04-21T11:55:00+00:00"
    assert refreshed["cache"]["hit"] is False
    assert refreshed["cache"]["stale"] is False
    assert refreshed["rows"][0]["tags"] == ["fresh"]
    assert any("fresh warning" in warning for warning in refreshed["warnings"])
    assert cache_key not in altcoin_api._ALTCOIN_SCAN_REFRESH_TASKS
    altcoin_api._clear_altcoin_scan_cache()


def test_get_altcoin_scan_snapshot_force_refresh_awaits_new_payload(monkeypatch):
    altcoin_api._clear_altcoin_scan_cache()
    state = {"compute_calls": 0}

    async def fake_resolve_universe(**kwargs):
        return ["AAA/USDT"], ["AAA/USDT"], [], []

    async def fake_compute_scan_payload(**kwargs):
        state["compute_calls"] += 1
        return {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [
                {
                    "symbol": "AAA/USDT",
                    "tags": ["fresh"],
                    "freshness": {"using_market_snapshot": True, "market_source_type": "live_snapshot"},
                    "data_quality": {"market_data_freshness": 0.95, "using_market_snapshot": True},
                }
            ],
            "symbols_requested": ["AAA/USDT"],
            "symbols_used": ["AAA/USDT"],
            "excluded_retired": [],
            "warnings": ["fresh warning"],
            "generated_at": "2026-04-21T12:00:00+00:00",
            "universe_meta": {},
        }

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_compute_scan_payload", fake_compute_scan_payload)

    cache_key = altcoin_api._cache_key(
        exchange="binance",
        timeframe="4h",
        symbols=["AAA/USDT"],
        exclude_retired=True,
        mode="combined",
        view="4h",
        universe_scope="research",
    )
    altcoin_api._ALTCOIN_SCAN_CACHE[cache_key] = {
        "stored_at": time.time(),
        "payload": {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [{"symbol": "AAA/USDT", "tags": ["cached"], "data_quality": {}}],
            "symbols_requested": ["AAA/USDT"],
            "symbols_used": ["AAA/USDT"],
            "excluded_retired": [],
            "warnings": ["cached warning"],
            "generated_at": "2026-04-21T11:55:00+00:00",
            "universe_meta": {},
        },
    }

    payload = asyncio.run(
        altcoin_api.get_altcoin_scan_snapshot(
            exchange="binance",
            timeframe="4h",
            symbols=["AAA/USDT"],
            exclude_retired=True,
            refresh=True,
            mode="combined",
            view="4h",
            universe_scope="research",
        )
    )

    assert state["compute_calls"] == 1
    assert payload["rows"][0]["tags"] == ["fresh"]
    assert payload["cache"]["hit"] is False
    assert payload["cache"]["served_mode"] == "live_compute"
    assert any("fresh warning" in warning for warning in payload["warnings"])
    assert not any("cached warning" in warning for warning in payload["warnings"])
    altcoin_api._clear_altcoin_scan_cache()


def test_evict_altcoin_scan_cache_keeps_newest_entries():
    from web.api.altcoin import cache as cache_module

    altcoin_api._clear_altcoin_scan_cache()
    limit = cache_module._ALTCOIN_SCAN_CACHE_MAX_ENTRIES
    for idx in range(limit + 3):
        altcoin_api._ALTCOIN_SCAN_CACHE[f"k{idx}"] = {"stored_at": float(idx), "payload": {}}

    cache_module._evict_altcoin_scan_cache()

    assert len(altcoin_api._ALTCOIN_SCAN_CACHE) == limit
    assert "k0" not in altcoin_api._ALTCOIN_SCAN_CACHE
    assert f"k{limit + 2}" in altcoin_api._ALTCOIN_SCAN_CACHE
    altcoin_api._clear_altcoin_scan_cache()


def test_get_altcoin_scan_snapshot_serves_sibling_universe_while_refreshing(monkeypatch):
    altcoin_api._clear_altcoin_scan_cache()
    state = {"compute_calls": 0, "gate": None}

    async def fake_resolve_universe(**kwargs):
        return ["AAA/USDT", "BBB/USDT"], ["AAA/USDT", "BBB/USDT"], [], []

    async def fake_compute_scan_payload(**kwargs):
        state["compute_calls"] += 1
        await state["gate"].wait()
        return {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [{"symbol": "BBB/USDT", "tags": ["fresh"], "data_quality": {}}],
            "symbols_requested": ["AAA/USDT", "BBB/USDT"],
            "symbols_used": ["AAA/USDT", "BBB/USDT"],
            "excluded_retired": [],
            "warnings": [],
            "generated_at": "2026-04-21T12:00:00+00:00",
            "universe_meta": {},
        }

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_compute_scan_payload", fake_compute_scan_payload)

    key_args = dict(exchange="binance", timeframe="4h", exclude_retired=True, mode="combined", view="4h", universe_scope="research")
    old_key = altcoin_api._cache_key(symbols=["AAA/USDT"], **key_args)
    new_key = altcoin_api._cache_key(symbols=["AAA/USDT", "BBB/USDT"], **key_args)
    other_tf_key = altcoin_api._cache_key(symbols=["AAA/USDT"], **dict(key_args, timeframe="1h", view="1h"))
    altcoin_api._ALTCOIN_SCAN_CACHE[other_tf_key] = {
        "stored_at": time.time(),
        "payload": {"rows": [{"symbol": "AAA/USDT", "tags": ["wrong_timeframe"]}], "warnings": []},
    }
    altcoin_api._ALTCOIN_SCAN_CACHE[old_key] = {
        "stored_at": time.time() - 60.0,
        "payload": {"rows": [{"symbol": "AAA/USDT", "tags": ["previous_universe"]}], "warnings": []},
    }

    async def runner():
        state["gate"] = asyncio.Event()
        payload = await altcoin_api.get_altcoin_scan_snapshot(symbols=[], refresh=False, **key_args)
        task = altcoin_api._ALTCOIN_SCAN_REFRESH_TASKS.get(new_key)
        assert task is not None and task.done() is False
        state["gate"].set()
        await asyncio.shield(task)
        return payload

    payload = asyncio.run(runner())

    assert payload["rows"][0]["tags"] == ["previous_universe"]
    assert payload["cache"]["cache_key"] == new_key
    assert payload["cache"]["served_mode"] == "universe_changed_refresh"
    assert payload["cache"]["refreshing"] is True
    assert state["compute_calls"] == 1
    altcoin_api._clear_altcoin_scan_cache()


def test_get_altcoin_scan_snapshot_does_not_cache_all_stale_market_data(monkeypatch):
    altcoin_api._clear_altcoin_scan_cache()
    state = {"compute_calls": 0}

    async def fake_resolve_universe(**kwargs):
        return ["AAA/USDT"], ["AAA/USDT"], [], []

    async def fake_compute_scan_payload(**kwargs):
        state["compute_calls"] += 1
        return {
            "exchange": "binance",
            "timeframe": "4h",
            "rows": [
                {
                    "symbol": "AAA/USDT",
                    "tags": ["stale"],
                    "freshness": {"using_market_snapshot": False, "market_source_type": "local_kline"},
                    "data_quality": {"market_data_freshness": 0.0, "using_market_snapshot": False},
                }
            ],
            "symbols_requested": ["AAA/USDT"],
            "symbols_used": ["AAA/USDT"],
            "excluded_retired": [],
            "warnings": ["stale warning"],
            "generated_at": "2026-04-21T12:00:00+00:00",
            "universe_meta": {},
        }

    monkeypatch.setattr(altcoin_api.scan, "_resolve_universe", fake_resolve_universe)
    monkeypatch.setattr(altcoin_api.scan, "_compute_scan_payload", fake_compute_scan_payload)

    async def runner():
        first = await altcoin_api.get_altcoin_scan_snapshot(
            exchange="binance",
            timeframe="4h",
            symbols=["AAA/USDT"],
            exclude_retired=True,
            refresh=False,
            mode="combined",
            view="4h",
            universe_scope="research",
        )
        second = await altcoin_api.get_altcoin_scan_snapshot(
            exchange="binance",
            timeframe="4h",
            symbols=["AAA/USDT"],
            exclude_retired=True,
            refresh=False,
            mode="combined",
            view="4h",
            universe_scope="research",
        )
        return first, second

    first, second = asyncio.run(runner())

    assert state["compute_calls"] == 2
    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is False
    assert any("not cached" in warning for warning in first["warnings"])
    assert any("not cached" in warning for warning in second["warnings"])
    altcoin_api._clear_altcoin_scan_cache()


def test_create_altcoin_alert_preset_rejects_benchmark_symbol(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {
            "rows": [
                {
                    "symbol": "BTC/USDT",
                    "alt_eligible": False,
                }
            ]
        }

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)

    response = client.post(
        "/api/altcoin/alerts/preset",
        json={
            "preset": "异动预警",
            "exchange": "binance",
            "timeframe": "4h",
            "symbol": "BTC/USDT",
            "universe_symbols": ["BTC/USDT", "AVAX/USDT"],
            "channels": ["feishu"],
        },
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "benchmark symbols are not supported for altcoin radar alerts"


def test_create_altcoin_alert_preset_supports_phase1_event_presets(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {
            "rows": [
                {
                    "symbol": "ORDI/USDT",
                    "alt_eligible": True,
                }
            ]
        }

    async def fake_list_rules():
        return []

    async def fake_add_rule(**kwargs):
        return kwargs

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(altcoin_api.notification_manager, "add_rule", fake_add_rule)

    response = client.post(
        "/api/altcoin/alerts/preset",
        json={
            "preset": "点火预警",
            "exchange": "binance",
            "timeframe": "15m",
            "symbol": "ORDI/USDT",
            "universe_symbols": ["ORDI/USDT", "SATS/USDT"],
            "channels": ["feishu"],
        },
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rule"]["rule_type"] == "altcoin_ignition_cross_up"
    assert payload["rule"]["params"]["score_key"] == "ignition"


def test_create_altcoin_alert_preset_supports_narrative_event_preset(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {
            "rows": [
                {
                    "symbol": "PEPE/USDT",
                    "alt_eligible": True,
                }
            ]
        }

    async def fake_list_rules():
        return []

    async def fake_add_rule(**kwargs):
        return kwargs

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(altcoin_api.notification_manager, "add_rule", fake_add_rule)

    response = client.post(
        "/api/altcoin/alerts/preset",
        json={
            "preset": "叙事预警",
            "exchange": "binance",
            "timeframe": "1h",
            "symbol": "PEPE/USDT",
            "universe_symbols": ["PEPE/USDT", "WIF/USDT"],
            "channels": ["feishu"],
            "mode": "narrative",
            "view": "1h",
            "universe_scope": "watchlist",
        },
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rule"]["rule_type"] == "altcoin_narrative_heat_spike"
    assert payload["rule"]["params"]["score_key"] == "narrative"
    assert payload["rule"]["params"]["mode"] == "narrative"
    assert payload["rule"]["params"]["universe_scope"] == "watchlist"


def test_create_altcoin_alert_preset_dedupe_respects_config_key(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    added_rules = []
    universe_symbols = ["PEPE/USDT", "WIF/USDT"]

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return {"rows": [{"symbol": "PEPE/USDT", "alt_eligible": True}]}

    async def fake_list_rules():
        return [
            _notification_rule(
                rule_id="r-existing-other-config",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key="other-config",
            )
        ]

    async def fake_add_rule(**kwargs):
        added_rules.append(kwargs)
        return {"id": "r-new", **kwargs}

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(altcoin_api.notification_manager, "add_rule", fake_add_rule)

    response = client.post(
        "/api/altcoin/alerts/preset",
        json={
            "preset": "叙事预警",
            "exchange": "binance",
            "timeframe": "1h",
            "symbol": "PEPE/USDT",
            "universe_symbols": universe_symbols,
            "channels": ["feishu"],
            "mode": "narrative",
            "view": "1h",
            "universe_scope": "watchlist",
        },
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["existing"] is False
    assert payload["rule"]["id"] == "r-new"
    assert payload["rule_meta"]["kind"] == "narrative"
    assert payload["rule_meta"]["config_key"]
    assert len(added_rules) == 1
    assert added_rules[0]["params"]["config_key"] == payload["rule_meta"]["config_key"]


def test_delete_altcoin_alert_preset_removes_only_matching_preset_and_config(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    deleted_ids = []
    universe_symbols = ["PEPE/USDT", "WIF/USDT"]
    config_key = altcoin_api.build_altcoin_notification_config_key(
        exchange="binance",
        timeframe="1h",
        universe_symbols=universe_symbols,
        exclude_retired=True,
        mode="narrative",
        view="1h",
        universe_scope="watchlist",
    )

    async def fake_list_rules():
        return [
            _notification_rule(
                rule_id="r-match",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-other-preset",
                rule_type="altcoin_score_above",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="alert",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-other-config",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key="other-config",
            ),
            _notification_rule(
                rule_id="r-other-symbol",
                rule_type="altcoin_narrative_heat_spike",
                symbol="WIF/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
        ]

    async def fake_delete_rule(rule_id):
        deleted_ids.append(rule_id)
        return True

    monkeypatch.setattr(altcoin_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(altcoin_api.notification_manager, "delete_rule", fake_delete_rule)

    response = client.delete(
        "/api/altcoin/alerts/preset"
        "?exchange=binance"
        "&timeframe=1h"
        "&symbol=PEPE%2FUSDT"
        "&symbols=PEPE%2FUSDT,WIF%2FUSDT"
        "&preset=%E5%8F%99%E4%BA%8B%E9%A2%84%E8%AD%A6"
        "&mode=narrative"
        "&view=1h"
        "&universe_scope=watchlist",
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert deleted_ids == ["r-match"]
    assert payload["deleted_count"] == 1
    assert payload["deleted_rules"][0]["kind"] == "narrative"
    assert payload["config_key"] == config_key


def test_delete_altcoin_alert_preset_without_preset_removes_all_symbol_rules_in_current_config(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    deleted_ids = []
    universe_symbols = ["PEPE/USDT", "WIF/USDT"]
    config_key = altcoin_api.build_altcoin_notification_config_key(
        exchange="binance",
        timeframe="1h",
        universe_symbols=universe_symbols,
        exclude_retired=True,
        mode="narrative",
        view="1h",
        universe_scope="watchlist",
    )

    async def fake_list_rules():
        return [
            _notification_rule(
                rule_id="r-narrative",
                rule_type="altcoin_narrative_heat_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="narrative",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-control",
                rule_type="altcoin_crowding_risk_spike",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="control",
                threshold=0.72,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
            _notification_rule(
                rule_id="r-other-config",
                rule_type="altcoin_score_above",
                symbol="PEPE/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="alert",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key="other-config",
            ),
            _notification_rule(
                rule_id="r-other-symbol",
                rule_type="altcoin_score_above",
                symbol="WIF/USDT",
                exchange="binance",
                timeframe="1h",
                universe_symbols=universe_symbols,
                score_key="alert",
                threshold=0.65,
                mode="narrative",
                view="1h",
                universe_scope="watchlist",
                config_key=config_key,
            ),
        ]

    async def fake_delete_rule(rule_id):
        deleted_ids.append(rule_id)
        return True

    monkeypatch.setattr(altcoin_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(altcoin_api.notification_manager, "delete_rule", fake_delete_rule)

    response = client.delete(
        "/api/altcoin/alerts/preset"
        "?exchange=binance"
        "&timeframe=1h"
        "&symbol=PEPE%2FUSDT"
        "&symbols=PEPE%2FUSDT,WIF%2FUSDT"
        "&mode=narrative"
        "&view=1h"
        "&universe_scope=watchlist",
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert set(deleted_ids) == {"r-narrative", "r-control"}
    assert payload["deleted_count"] == 2
    assert payload["symbol"] == "PEPE/USDT"
    assert payload["preset"] is None
    assert payload["config_key"] == config_key


def test_altcoin_radar_watchlist_routes(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    state = {"symbols": ["ORDI/USDT", "PEPE/USDT"]}

    def fake_get_watchlist_symbols():
        return list(state["symbols"])

    def fake_add_watchlist_symbol(symbol):
        if symbol not in state["symbols"]:
            state["symbols"].append(symbol)
        return list(state["symbols"])

    def fake_remove_watchlist_symbol(symbol):
        state["symbols"] = [item for item in state["symbols"] if item != symbol]
        return list(state["symbols"])

    monkeypatch.setattr(altcoin_api.universe, "get_watchlist_symbols", fake_get_watchlist_symbols)
    monkeypatch.setattr(altcoin_api.universe, "add_watchlist_symbol", fake_add_watchlist_symbol)
    monkeypatch.setattr(altcoin_api.universe, "remove_watchlist_symbol", fake_remove_watchlist_symbol)
    monkeypatch.setenv("OPS_TOKEN", "test-token")

    resp_get = client.get("/api/altcoin/radar/watchlist")
    assert resp_get.status_code == 200
    assert resp_get.json()["symbols"] == ["ORDI/USDT", "PEPE/USDT"]

    resp_add = client.post("/api/altcoin/radar/watchlist", json={"symbol": "WIF/USDT"}, headers=_ops_headers())
    assert resp_add.status_code == 200
    assert "WIF/USDT" in resp_add.json()["symbols"]

    resp_delete = client.delete("/api/altcoin/radar/watchlist?symbol=WIF%2FUSDT", headers=_ops_headers())
    assert resp_delete.status_code == 200
    assert "WIF/USDT" not in resp_delete.json()["symbols"]


def test_altcoin_detail_unknown_symbol_fast_404(monkeypatch):
    """Regression: a symbol outside the universe used to run the FULL onchain
    pipeline (~25s) and return 200 with an empty detail. It must 404 fast and
    never touch the external sources."""
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)
    external_calls = {}

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    async def fake_get_onchain_overview(**kwargs):
        external_calls["onchain"] = True
        return {}

    async def fake_live_chain(**kwargs):
        external_calls["live_chain"] = True
        return ({}, {})

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.detail, "get_onchain_overview", fake_get_onchain_overview)
    monkeypatch.setattr(altcoin_api.detail, "_load_detail_live_chain_context", fake_live_chain)

    response = client.get("/api/altcoin/radar/detail?symbol=NOPE/USDT")
    assert response.status_code == 404
    assert "NOPE/USDT" in response.json()["detail"]
    assert external_calls == {}, "unknown symbol must not trigger external sources"


def test_altcoin_detail_degrades_when_onchain_hangs(monkeypatch):
    """Regression: an unbounded onchain call hung the whole detail endpoint
    past the frontend's 60s budget. With the per-source budget the endpoint
    returns quickly with a warning and partial data."""
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        return _scan_payload()

    async def hanging_onchain(**kwargs):
        await asyncio.sleep(5)
        return {"never": "returned"}

    async def fake_live_chain(**kwargs):
        return ({}, {})

    monkeypatch.setattr(altcoin_api.detail, "DETAIL_ONCHAIN_TIMEOUT_SEC", 0.2)
    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    monkeypatch.setattr(altcoin_api.detail, "get_onchain_overview", hanging_onchain)
    monkeypatch.setattr(altcoin_api.detail, "_load_detail_live_chain_context", fake_live_chain)

    started = time.monotonic()
    response = client.get("/api/altcoin/radar/detail?symbol=AAA/USDT&symbols=AAA/USDT")
    elapsed = time.monotonic() - started

    assert response.status_code == 200
    assert elapsed < 3.0, "bounded detail must not wait for the hanging source"
    payload = response.json()
    assert any("timed out" in str(w) for w in payload.get("warnings") or []), payload.get("warnings")
    assert payload["selected_row"]["symbol"] == "AAA/USDT"


def test_altcoin_watchlist_reports_retired_and_unlisted_symbols(monkeypatch):
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    client = TestClient(app)

    monkeypatch.setattr(altcoin_api.universe, "get_watchlist_symbols", lambda: ["ORDI/USDT", "PEPE/USDT", "DEADCOIN/USDT"])
    monkeypatch.setattr(
        altcoin_api.universe,
        "_research_retired_filter",
        lambda exchange, timeframe, requested, exclude: (
            [s for s in requested if s != "ORDI/USDT"],
            ["ORDI/USDT"],
        ),
    )

    async def fake_live_map(*, exchange, symbols):
        # PEPE listed with a real price; ORDI listed; DEADCOIN absent entirely.
        return {
            "ORDI/USDT": {"current_price": 12.5},
            "PEPE/USDT": {"current_price": 0.00001},
        }

    monkeypatch.setattr(altcoin_api.scan, "_load_exchange_public_market_snapshots", fake_live_map)

    response = client.get("/api/altcoin/radar/watchlist")
    assert response.status_code == 200
    payload = response.json()
    # User entries are never silently removed -- only flagged.
    assert payload["symbols"] == ["ORDI/USDT", "PEPE/USDT", "DEADCOIN/USDT"]
    assert payload["retired_symbols"] == ["ORDI/USDT"]
    assert payload["retired_count"] == 1
    assert payload["unlisted_symbols"] == ["DEADCOIN/USDT"]
    assert payload["unlisted_count"] == 1


def test_warm_default_scan_cache_uses_default_combo(monkeypatch):
    captured = {}

    async def fake_get_altcoin_scan_snapshot(**kwargs):
        captured.update(kwargs)
        return {"rows": []}

    monkeypatch.setattr(altcoin_api.scan, "get_altcoin_scan_snapshot", fake_get_altcoin_scan_snapshot)
    asyncio.run(altcoin_api.warm_default_scan_cache())

    assert captured["exchange"] == altcoin_api.DEFAULT_EXCHANGE
    assert captured["timeframe"] == altcoin_api.DEFAULT_TIMEFRAME
    assert captured["mode"] == "combined"
    assert captured["refresh"] is False
