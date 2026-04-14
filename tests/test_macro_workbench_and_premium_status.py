from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd


def _recent_snapshot_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _history_micro_snapshot() -> dict:
    return {
        "timestamp": _recent_snapshot_ts(),
        "orderbook": {"mid_price": 100000.0, "spread_bps": 2.5},
        "aggressor_flow": {"count": 8, "imbalance": 0.18},
        "large_orders": [
            {"side": "bid", "notional": 5000000.0},
            {"side": "ask", "notional": 3000000.0},
        ],
        "iceberg_detection": {"candidate_count": 2},
        "long_short_ratio": {"available": True, "long_short_ratio": 1.22},
        "funding_rate": {"available": True, "funding_rate": 0.0001},
        "spot_futures_basis": {"available": True, "basis_pct": 0.12},
    }


def _history_community_snapshot() -> dict:
    return {
        "timestamp": _recent_snapshot_ts(),
        "announcements": [{"title": "Listing update"}],
        "whale_transfers": {"count": 2},
        "flow_proxy": {"count": 6, "imbalance": 0.11},
        "security_alerts": {"events": []},
    }


def _news_summary(events_count: int = 2) -> dict:
    return {
        "events_count": events_count,
        "feed_count": 0,
        "raw_count": 0,
        "scope": "symbol",
        "sentiment": {"positive": events_count, "neutral": 0, "negative": 0},
    }


def test_market_state_exposes_macro_snapshot(monkeypatch):
    from web.api import research as module

    monkeypatch.setattr(module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"}))
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(return_value={"events": [{"name": "CPI", "time_utc": _recent_snapshot_ts(), "importance": "high"}]}),
    )
    monkeypatch.setattr(module, "_build_news_summary", AsyncMock(return_value=_news_summary(3)))
    monkeypatch.setattr(module, "_load_macro_snapshot_payload", AsyncMock(return_value={
        "vix": 18.5,
        "dxy": 99.2,
        "tnx_10y": 4.15,
        "fed_rate": 3.64,
        "cpi_yoy": 2.8,
        "ppi_yoy": 1.2,
        "ppi_cpi_gap": -1.6,
        "m1_yoy": 4.5,
        "m2_yoy": 6.1,
        "m1_m2_gap": -1.6,
        "cn_cpi_yoy": 1.0,
        "cn_ppi_yoy": 0.5,
        "cn_ppi_cpi_gap": -0.5,
        "cn_m1_yoy": 1.2,
        "cn_m2_yoy": 7.4,
        "cn_m1_m2_gap": -6.2,
    }))
    monkeypatch.setattr(module, "_load_latest_microstructure_snapshot", AsyncMock(return_value=_history_micro_snapshot()))
    monkeypatch.setattr(module, "_load_latest_community_snapshot", AsyncMock(return_value=_history_community_snapshot()))
    monkeypatch.setattr(module, "_load_latest_whale_snapshot", AsyncMock(return_value={"count": 2, "transactions": []}))
    monkeypatch.setattr(module, "get_market_microstructure", AsyncMock(side_effect=AssertionError("live microstructure should be skipped")))
    monkeypatch.setattr(module, "get_community_overview", AsyncMock(side_effect=AssertionError("live community should be skipped")))

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert "data.macro.snapshot" in result["source_labels"]
    assert result["payload"]["macro_snapshot"]["ppi_cpi_gap"] == -1.6
    assert result["payload"]["macro_summary"]["scissors_spread_pp"] == -1.6
    assert result["payload"]["macro_summary"]["liquidity_scissors_spread_pp"] == -1.6
    assert result["payload"]["macro_summary"]["china_scissors_spread_pp"] == -0.5
    assert result["payload"]["macro_summary"]["china_liquidity_scissors_spread_pp"] == -6.2
    assert "PPI-CPI" in result["summary"]["macro_focus"]
    assert "China:" in result["summary"]["macro_focus"]
    assert result["payload"]["sentiment_dashboard"]["macro"]["fed_rate"] == 3.64
    assert result["payload"]["sentiment_dashboard"]["macro_regions"]["china"]["cpi_yoy"] == 1.0
    assert result["payload"]["macro_regions"]["us"]["fed_rate"] == 3.64


def test_premium_data_status_reports_cached_fred_macro(tmp_path, monkeypatch):
    from web.api import ai_research as ai_module

    monkeypatch.chdir(tmp_path)
    macro_dir = tmp_path / "data" / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    pd.Series({"2026-03-01": 3.64}, name="fed_rate", dtype=float).to_frame().to_parquet(macro_dir / "fed_rate.parquet")
    pd.Series({"2026-03-01": -1.6}, name="ppi_cpi_gap", dtype=float).to_frame().to_parquet(macro_dir / "ppi_cpi_gap.parquet")
    pd.Series({"2026-03-01": 1.3}, name="m1_m2_gap", dtype=float).to_frame().to_parquet(macro_dir / "m1_m2_gap.parquet")
    pd.Series({"2026-03-01": 1.0}, name="cn_cpi_yoy", dtype=float).to_frame().to_parquet(macro_dir / "cn_cpi_yoy.parquet")
    pd.Series({"2026-03-01": 0.5}, name="cn_ppi_yoy", dtype=float).to_frame().to_parquet(macro_dir / "cn_ppi_yoy.parquet")
    pd.Series({"2026-03-01": -0.5}, name="cn_ppi_cpi_gap", dtype=float).to_frame().to_parquet(macro_dir / "cn_ppi_cpi_gap.parquet")

    monkeypatch.setattr(
        "core.data.macro_collector.load_macro_snapshot",
        lambda: {
            "fed_rate": 3.64,
            "cpi_yoy": 2.8,
            "ppi_yoy": 1.2,
            "ppi_cpi_gap": -1.6,
            "m1_m2_gap": 1.3,
            "cn_cpi_yoy": 1.0,
            "cn_ppi_yoy": 0.5,
            "cn_ppi_cpi_gap": -0.5,
        },
    )
    monkeypatch.setattr(
        "core.data.macro_collector.group_macro_snapshot",
        lambda snap: {
            "market": {"vix": None, "dxy": None, "tnx_10y": None},
            "us": {"fed_rate": 3.64, "cpi_yoy": 2.8, "ppi_yoy": 1.2, "ppi_cpi_gap": -1.6, "m1_m2_gap": 1.3},
            "china": {"cn_cpi_yoy": 1.0, "cn_ppi_yoy": 0.5, "cn_ppi_cpi_gap": -0.5},
        },
    )
    monkeypatch.setattr("core.data.macro_collector._api_key", lambda: "")

    result = asyncio.run(ai_module.get_premium_data_status())
    source = result["sources"]["fred_macro"]

    assert source["available"] is True
    assert source["key_configured"] is False
    assert source["has_cached_data"] is True
    assert "ppi_cpi_gap" in source["active_series"]
    assert "m1_m2_gap" in source["active_series"]
    assert "cn_ppi_cpi_gap" in source["active_series"]
    assert source["last_updated"] is not None
    assert source["regions"]["china"]["cn_cpi_yoy"] == 1.0
    assert source["upstreams"]["china_macro"] == "stats.gov.cn + pbc.gov.cn"
    assert result["focus_regions"] == ["us", "china"]


def test_sources_health_includes_ai_news_and_ml_inventory(tmp_path, monkeypatch):
    from core.data.options_collector import options_collector
    from web.api import ai_research as ai_module

    monkeypatch.chdir(tmp_path)

    class FakeNewsManager:
        def __init__(self, *args, **kwargs):
            self.sources = ["jin10", "rss", "gdelt"]

    class FakeOptionsSnapshot:
        def to_dict(self):
            return {
                "available": True,
                "currency": "BTC",
                "atm_iv": 0.55,
                "atm_iv_pct": 55.0,
                "skew_25d": 0.02,
                "put_call_ratio": 0.91,
                "n_calls": 12,
                "n_puts": 10,
                "signal": "neutral",
                "timestamp": _recent_snapshot_ts(),
            }

    monkeypatch.setattr("core.news.collectors.manager.MultiSourceNewsCollector", FakeNewsManager)
    monkeypatch.setattr(
        "core.data.macro_collector.load_macro_snapshot",
        lambda: {
            "fed_rate": 3.64,
            "cpi_yoy": 2.8,
            "ppi_yoy": 1.2,
            "ppi_cpi_gap": -1.6,
            "m1_m2_gap": -1.1,
            "cn_cpi_yoy": 1.0,
            "cn_ppi_yoy": 0.5,
            "cn_ppi_cpi_gap": -0.5,
            "cn_m1_yoy": 1.2,
            "cn_m2_yoy": 7.4,
            "cn_m1_m2_gap": -6.2,
        },
    )
    monkeypatch.setattr(
        "core.data.macro_collector.group_macro_snapshot",
        lambda snap: {
            "market": {"vix": 18.5, "dxy": 99.2, "tnx_10y": 4.15},
            "us": {"fed_rate": snap["fed_rate"], "ppi_cpi_gap": snap["ppi_cpi_gap"], "m1_m2_gap": snap["m1_m2_gap"]},
            "china": {"cn_cpi_yoy": snap["cn_cpi_yoy"], "cn_ppi_cpi_gap": snap["cn_ppi_cpi_gap"], "cn_m1_m2_gap": snap["cn_m1_m2_gap"]},
        },
    )
    monkeypatch.setattr("core.data.macro_collector._api_key", lambda: "")
    monkeypatch.setattr(
        ai_module.FundingRateProvider,
        "load_local_cache",
        lambda self, symbol, exchange=None: pd.Series(
            [0.0001, 0.00012],
            index=pd.to_datetime(
                [
                    datetime.now(timezone.utc) - pd.Timedelta(hours=8),
                    datetime.now(timezone.utc),
                ]
            ),
            dtype=float,
        ),
    )
    monkeypatch.setattr(
        ai_module.news_db,
        "summarize_news_raw_coverage",
        AsyncMock(
            return_value={
                "raw_news_total": 18,
                "events_total": 6,
                "source_summary": {
                    "jin10": {"inserted_count": 8, "latest_at": _recent_snapshot_ts(), "failure_rate": 0.0},
                    "rss": {"inserted_count": 6, "latest_at": _recent_snapshot_ts(), "failure_rate": 0.0},
                    "gdelt": {"inserted_count": 4, "latest_at": _recent_snapshot_ts(), "failure_rate": 0.0},
                },
            }
        ),
    )
    monkeypatch.setattr(
        ai_module.news_db,
        "list_source_states",
        AsyncMock(
            return_value=[
                {
                    "source": "jin10",
                    "updated_at": _recent_snapshot_ts(),
                    "last_success_at": _recent_snapshot_ts(),
                    "last_error": None,
                    "error_count": 0,
                    "success_count": 3,
                },
                {
                    "source": "rss",
                    "updated_at": _recent_snapshot_ts(),
                    "last_success_at": _recent_snapshot_ts(),
                    "last_error": None,
                    "error_count": 0,
                    "success_count": 3,
                },
                {
                    "source": "gdelt",
                    "updated_at": _recent_snapshot_ts(),
                    "last_success_at": _recent_snapshot_ts(),
                    "last_error": None,
                    "error_count": 0,
                    "success_count": 3,
                },
            ]
        ),
    )
    monkeypatch.setattr(ai_module.news_db, "get_llm_queue_stats", AsyncMock(return_value={"pending_total": 0, "counts": {}}))
    monkeypatch.setattr("core.data.google_trends_collector.load_latest", lambda keyword="bitcoin": 78.0)
    monkeypatch.setattr(options_collector, "_cache", {"BTC": (0.0, FakeOptionsSnapshot())}, raising=False)
    monkeypatch.setattr(ai_module.settings, "OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setattr(ai_module.settings, "OPENAI_BACKUP_API_KEY", "")

    funding_dir = tmp_path / "data" / "funding" / "binance"
    funding_dir.mkdir(parents=True, exist_ok=True)
    (funding_dir / "BTC_USDT_funding.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        ai_module.live_decision_router,
        "get_runtime_config",
        lambda: {
            "enabled": False,
            "mode": "shadow",
            "provider": "codex",
            "provider_requested": "codex",
            "provider_fallback": False,
            "model": "gpt-5.4",
            "providers": {
                "codex": {"available": True, "default_model": "gpt-5.4", "base_url": "https://api.example.com"},
                "claude": {"available": True, "default_model": "claude-3-5-sonnet-latest", "base_url": "https://anthropic.example.com"},
                "glm": {"available": False, "default_model": "GLM-4.5-Air", "base_url": "https://glm.example.com"},
            },
        },
    )
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {
            "enabled": True,
            "mode": "execute",
            "provider": "codex",
            "provider_requested": "codex",
            "provider_fallback": False,
            "model": "gpt-5.4",
            "runtime_profile": "paper_longrun",
            "allow_live": False,
            "safety": {"status": "ready"},
            "providers": {
                "codex": {"available": True, "default_model": "gpt-5.4", "base_url": "https://api.example.com"},
                "claude": {"available": True, "default_model": "claude-3-5-sonnet-latest", "base_url": "https://anthropic.example.com"},
                "glm": {"available": False, "default_model": "GLM-4.5-Air", "base_url": "https://glm.example.com"},
            },
        },
    )
    monkeypatch.setattr("importlib.util.find_spec", lambda name: object() if name == "xgboost" else None)

    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "ml_signal_xgb").write_text("legacy-model-name", encoding="utf-8")

    result = asyncio.run(ai_module.get_sources_health())

    assert result["focus_regions"] == ["us", "china"]
    assert result["summary"]["support_assessment"] == "sufficient"

    macro = result["categories"]["macro"]["sources"]["fred_macro"]
    assert macro["health"] == "healthy"
    assert macro["regions"]["china"]["cn_ppi_cpi_gap"] == -0.5

    news = result["categories"]["news"]["sources"]["jin10"]
    assert news["health"] == "healthy"
    assert news["snapshot"]["inserted_count"] == 8
    assert result["categories"]["news"]["runtime"]["llm_queue"]["pending_total"] == 0

    research_llm = result["categories"]["ai_sources"]["sources"]["research_context_llm"]
    assert research_llm["health"] == "healthy"
    assert research_llm["snapshot"]["provider"] == "codex"

    ml_model = result["categories"]["ai_sources"]["sources"]["ml_signal_model"]
    assert ml_model["health"] == "degraded"
    assert any("Non-canonical model filename" in item for item in ml_model["issues"])
    assert ml_model["snapshot"]["alternative_candidates"] == [str(Path("models") / "ml_signal_xgb")]
