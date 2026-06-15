from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest


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


def _patch_public_market_sources(
    monkeypatch, module, *, fear_greed=None, market_breadth=None
):
    monkeypatch.setattr(
        module,
        "_load_public_fear_greed_snapshot",
        AsyncMock(return_value=dict(fear_greed or {})),
    )
    monkeypatch.setattr(
        module,
        "_load_public_market_breadth_snapshot",
        AsyncMock(return_value=dict(market_breadth or {})),
    )


def test_market_state_exposes_macro_snapshot(monkeypatch):
    from web.api import research as module

    _patch_public_market_sources(monkeypatch, module)
    monkeypatch.setattr(
        module, "_load_preferred_coinglass_overview", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_analytics_history_status", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"})
    )
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(
            return_value={
                "events": [
                    {
                        "name": "CPI",
                        "time_utc": _recent_snapshot_ts(),
                        "importance": "high",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(
        module, "_build_news_summary", AsyncMock(return_value=_news_summary(3))
    )
    monkeypatch.setattr(
        module,
        "_load_macro_snapshot_payload",
        AsyncMock(
            return_value={
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
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_microstructure_snapshot",
        AsyncMock(return_value=_history_micro_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_community_snapshot",
        AsyncMock(return_value=_history_community_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_whale_snapshot",
        AsyncMock(return_value={"count": 2, "transactions": []}),
    )
    monkeypatch.setattr(
        module,
        "get_market_microstructure",
        AsyncMock(side_effect=AssertionError("live microstructure should be skipped")),
    )
    monkeypatch.setattr(
        module,
        "get_community_overview",
        AsyncMock(side_effect=AssertionError("live community should be skipped")),
    )

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert "data.macro.snapshot" in result["source_labels"]
    assert result["payload"]["macro_snapshot"]["ppi_cpi_gap"] == -1.6
    assert result["payload"]["macro_summary"]["scissors_spread_pp"] == -1.6
    assert result["payload"]["macro_summary"]["liquidity_scissors_spread_pp"] == -1.6
    assert result["payload"]["macro_summary"]["china_scissors_spread_pp"] == -0.5
    assert (
        result["payload"]["macro_summary"]["china_liquidity_scissors_spread_pp"] == -6.2
    )
    assert "PPI-CPI" in result["summary"]["macro_focus"]
    assert "China:" in result["summary"]["macro_focus"]
    assert result["payload"]["sentiment_dashboard"]["macro"]["fed_rate"] == 3.64
    assert (
        result["payload"]["sentiment_dashboard"]["macro_source_summary"][
            "source_status"
        ]
        == "cache_fresh"
    )
    assert (
        result["payload"]["macro_source_summary"]["groups"]["market"]["available_count"]
        == 3
    )
    assert result["summary"]["macro_source_status"] == "cache_fresh"
    assert (
        result["payload"]["sentiment_dashboard"]["macro_regions"]["china"]["cpi_yoy"]
        == 1.0
    )
    assert result["payload"]["macro_regions"]["us"]["fed_rate"] == 3.64


def test_market_state_marks_stale_macro_cache_as_degraded(monkeypatch):
    from web.api import research as module

    stale_ts = (datetime.now(timezone.utc) - timedelta(days=95)).isoformat()

    _patch_public_market_sources(monkeypatch, module)
    monkeypatch.setattr(
        module, "_load_preferred_coinglass_overview", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_analytics_history_status", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"})
    )
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(
            return_value={
                "events": [
                    {
                        "name": "CPI",
                        "time_utc": _recent_snapshot_ts(),
                        "importance": "high",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(
        module, "_build_news_summary", AsyncMock(return_value=_news_summary(3))
    )
    monkeypatch.setattr(
        module,
        "_load_macro_snapshot_payload",
        AsyncMock(
            return_value={
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
                "_meta": {
                    "source": "yfinance+yahoo_chart+fred+stats.gov.cn+pbc.gov.cn",
                    "latest_timestamp": stale_ts,
                    "groups": {
                        "market": {
                            "provider": "yfinance+yahoo_chart",
                            "available_count": 3,
                            "total_series": 3,
                            "latest_timestamp": stale_ts,
                        },
                        "us": {
                            "provider": "fred",
                            "available_count": 7,
                            "total_series": 7,
                            "latest_timestamp": stale_ts,
                        },
                        "china": {
                            "provider": "stats.gov.cn+pbc.gov.cn",
                            "available_count": 6,
                            "total_series": 6,
                            "latest_timestamp": stale_ts,
                        },
                    },
                },
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_microstructure_snapshot",
        AsyncMock(return_value=_history_micro_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_community_snapshot",
        AsyncMock(return_value=_history_community_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_whale_snapshot",
        AsyncMock(return_value={"count": 2, "transactions": []}),
    )
    monkeypatch.setattr(
        module,
        "get_market_microstructure",
        AsyncMock(side_effect=AssertionError("live microstructure should be skipped")),
    )
    monkeypatch.setattr(
        module,
        "get_community_overview",
        AsyncMock(side_effect=AssertionError("live community should be skipped")),
    )

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "degraded"
    assert result["payload"]["macro_source_summary"]["stale"] is True
    assert result["payload"]["macro_source_summary"]["source_status"] == "cache_stale"
    assert any("宏观缓存偏旧" in warning for warning in result["warnings"])


def test_onchain_module_exposes_derivatives_shadow_summary(monkeypatch):
    from web.api import research as module

    _patch_public_market_sources(monkeypatch, module)
    monkeypatch.setattr(
        module,
        "get_onchain_overview",
        AsyncMock(
            return_value={
                "degraded": False,
                "served_mode": "cache",
                "whale_activity": {"count": 3},
                "defi_tvl": {"chain": "Ethereum"},
                "funding_rate_multi_source": {"count": 4, "mean_rate_pct": 0.08},
                "fear_greed_index": {
                    "available": True,
                    "value": 67,
                    "classification": "greed",
                },
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_community_snapshot",
        AsyncMock(
            return_value={
                "announcements": [{"title": "listing"}],
                "whale_transfers": {"count": 1},
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_whale_snapshot",
        AsyncMock(return_value={"count": 2, "transactions": []}),
    )
    monkeypatch.setattr(
        module, "_build_news_summary", AsyncMock(return_value=_news_summary(3))
    )
    monkeypatch.setattr(
        module,
        "get_analytics_history_status",
        AsyncMock(
            return_value={
                "collectors": [
                    {
                        "collector": "derivatives",
                        "status": "ok",
                        "available": True,
                        "details": {
                            "provider": "coinglass",
                            "freshness_sec": 180.0,
                            "active_datasets": [
                                "funding_rate_exchange_list",
                                "open_interest_exchange_list",
                            ],
                            "quota_headroom": {"daily_remaining": 49900},
                            "snapshot": {
                                "timestamp": "2026-04-18T10:02:00Z",
                                "crowding_score": 0.76,
                                "squeeze_score": 0.71,
                                "distribution_score": 0.24,
                                "basis_pct": 0.013,
                                "taker_buy_sell_imbalance": 0.18,
                                "payload": {
                                    "history_ready": True,
                                    "history_exchange": "Binance",
                                    "history_interval": "h1",
                                    "funding_mean": 0.0008,
                                    "funding_zscore": 1.7,
                                    "funding_reversion_speed": 0.21,
                                    "long_short_ratio_change_24h": 0.14,
                                    "liquidation_burst_score": 0.62,
                                    "derivatives_heat_score": 0.83,
                                    "crowded_long": True,
                                    "squeeze_building": True,
                                    "order_flow_confirmed": True,
                                    "derivatives_labels": [
                                        "crowded_long",
                                        "squeeze_building",
                                        "order_flow_confirmed",
                                    ],
                                },
                            },
                        },
                    }
                ]
            }
        ),
    )

    result = asyncio.run(module._build_onchain_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert module.get_onchain_overview.await_args.kwargs["chain"] == "auto"
    assert result["summary"]["derivatives_status"] == "ok"
    assert result["summary"]["derivatives_source_status"] == "live"
    assert result["summary"]["derivatives_dataset_count"] == 2
    assert result["summary"]["derivatives_freshness_sec"] == 180.0
    assert result["payload"]["derivatives_summary"]["provider"] == "coinglass"
    assert result["payload"]["derivatives_source_summary"]["source_status"] == "live"
    assert (
        result["payload"]["derivatives_summary"]["quota_headroom"]["daily_remaining"]
        == 49900
    )
    assert result["payload"]["derivatives_summary"]["funding_mean_rate_pct"] == 0.08
    assert result["payload"]["derivatives_summary"]["history_ready"] is True
    assert result["payload"]["derivatives_summary"]["history_interval"] == "h1"
    assert result["payload"]["derivatives_summary"]["funding_zscore"] == 1.7
    assert result["payload"]["derivatives_summary"]["derivatives_labels"] == [
        "crowded_long",
        "squeeze_building",
        "order_flow_confirmed",
    ]


def test_premium_data_status_reports_cached_fred_macro(tmp_path, monkeypatch):
    from web.api import ai_research as ai_module

    ai_module._reset_sources_health_cache_for_tests()
    monkeypatch.chdir(tmp_path)
    macro_dir = tmp_path / "data" / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    pd.Series({"2026-03-01": 3.64}, name="fed_rate", dtype=float).to_frame().to_parquet(
        macro_dir / "fed_rate.parquet"
    )
    pd.Series(
        {"2026-03-01": -1.6}, name="ppi_cpi_gap", dtype=float
    ).to_frame().to_parquet(macro_dir / "ppi_cpi_gap.parquet")
    pd.Series({"2026-03-01": 1.3}, name="m1_m2_gap", dtype=float).to_frame().to_parquet(
        macro_dir / "m1_m2_gap.parquet"
    )
    pd.Series(
        {"2026-03-01": 1.0}, name="cn_cpi_yoy", dtype=float
    ).to_frame().to_parquet(macro_dir / "cn_cpi_yoy.parquet")
    pd.Series(
        {"2026-03-01": 0.5}, name="cn_ppi_yoy", dtype=float
    ).to_frame().to_parquet(macro_dir / "cn_ppi_yoy.parquet")
    pd.Series(
        {"2026-03-01": -0.5}, name="cn_ppi_cpi_gap", dtype=float
    ).to_frame().to_parquet(macro_dir / "cn_ppi_cpi_gap.parquet")

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
            "us": {
                "fed_rate": 3.64,
                "cpi_yoy": 2.8,
                "ppi_yoy": 1.2,
                "ppi_cpi_gap": -1.6,
                "m1_m2_gap": 1.3,
            },
            "china": {"cn_cpi_yoy": 1.0, "cn_ppi_yoy": 0.5, "cn_ppi_cpi_gap": -0.5},
        },
    )
    monkeypatch.setattr("core.data.macro_collector._api_key", lambda: "")
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.load_coinglass_status_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "freshness_sec": 120.0,
                "active_datasets": ["derivatives", "open_interest_exchange_list"],
                "status": [{"dataset": "derivatives", "status": "ok"}],
                "quota_headroom": {
                    "minute_remaining": 8,
                    "daily_remaining": 49900,
                    "monthly_remaining": 499000,
                },
                "key_configured": True,
            }
        ),
    )
    # Patch news_db async calls to prevent aiosqlite deadlock in full-suite runs
    from core.news.storage import db as _news_db
    monkeypatch.setattr(_news_db, "summarize_news_raw_coverage", AsyncMock(return_value={}))
    monkeypatch.setattr(_news_db, "list_source_states", AsyncMock(return_value=[]))
    monkeypatch.setattr(_news_db, "get_llm_queue_stats", AsyncMock(return_value={}))

    result = asyncio.run(ai_module.get_premium_data_status())
    source = result["sources"]["fred_macro"]
    coinglass = result["sources"]["coinglass"]

    assert source["available"] is True
    assert source["key_configured"] is False
    assert source["has_cached_data"] is True
    assert "ppi_cpi_gap" in source["active_series"]
    assert "m1_m2_gap" in source["active_series"]
    assert "cn_ppi_cpi_gap" in source["active_series"]
    assert source["last_updated"] is not None
    assert source["regions"]["china"]["cn_cpi_yoy"] == 1.0
    assert source["upstreams"]["china_macro"] == "stats.gov.cn + pbc.gov.cn"
    assert coinglass["available"] is True
    assert coinglass["has_cached_data"] is True
    assert coinglass["snapshot"]["active_datasets"] == [
        "derivatives",
        "open_interest_exchange_list",
    ]
    assert coinglass["snapshot"]["quota_headroom"]["daily_remaining"] == 49900
    assert result["focus_regions"] == ["us", "china"]


def test_load_macro_snapshot_metadata_reads_group_timestamps(tmp_path, monkeypatch):
    from core.data import macro_collector as module

    monkeypatch.chdir(tmp_path)
    macro_dir = tmp_path / "data" / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    pd.Series({"2026-04-18": 18.5}, name="vix", dtype=float).to_frame().to_parquet(
        macro_dir / "vix.parquet"
    )
    pd.Series({"2026-03-01": 3.64}, name="fed_rate", dtype=float).to_frame().to_parquet(
        macro_dir / "fed_rate.parquet"
    )
    pd.Series(
        {"2026-02-15": 1.0}, name="cn_cpi_yoy", dtype=float
    ).to_frame().to_parquet(macro_dir / "cn_cpi_yoy.parquet")

    snapshot = module.load_macro_snapshot()
    metadata = module.load_macro_snapshot_metadata(snapshot)

    assert snapshot["vix"] == 18.5
    assert metadata["source"] == "yfinance+yahoo_chart+fred+stats.gov.cn+pbc.gov.cn"
    assert metadata["groups"]["market"]["available_count"] == 1
    assert metadata["groups"]["us"]["available_count"] == 1
    assert metadata["groups"]["china"]["available_count"] == 1
    assert str(metadata["groups"]["market"]["latest_timestamp"]).startswith(
        "2026-04-18"
    )
    assert str(metadata["groups"]["us"]["latest_timestamp"]).startswith("2026-03-01")
    assert str(metadata["groups"]["china"]["latest_timestamp"]).startswith("2026-02-15")


def test_sources_health_includes_ai_news_and_ml_inventory(tmp_path, monkeypatch):
    from core.data.options_collector import options_collector
    from web.api import ai_research as ai_module

    ai_module._reset_sources_health_cache_for_tests()
    monkeypatch.chdir(tmp_path)

    class FakeNewsManager:
        def __init__(self, *args, **kwargs):
            self.sources = ["jin10", "rss", "gdelt", "coinglass_newsflash"]

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

    monkeypatch.setattr(
        "core.news.collectors.manager.MultiSourceNewsCollector", FakeNewsManager
    )
    monkeypatch.setattr(
        "core.data.coinglass_client.coinglass_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "core.data.coinglass_client.coinglass_key_configured",
        lambda: True,
    )

    def fake_coinglass_dataset_rows(dataset, symbol):
        if dataset in {"options_info", "option_max_pain"}:
            return pd.DataFrame(
                [
                    {
                        "source_ts": _recent_snapshot_ts(),
                        "payload_json": '{"symbol":"BTC","option_put_call_ratio":1.1}',
                    }
                ]
            )
        if dataset in {"spot_coin_netflow", "exchange_balance_list"}:
            return pd.DataFrame(
                [
                    {
                        "source_ts": _recent_snapshot_ts(),
                        "payload_json": '{"symbol":"BTC","spot_exchange_netflow_usd":1000000}',
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(
        "core.data.coinglass_client.load_dataset_rows_for_symbol",
        fake_coinglass_dataset_rows,
    )
    monkeypatch.setenv("NEWS_ENABLE_COINGLASS_NEWSFLASH", "1")
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
            "us": {
                "fed_rate": snap["fed_rate"],
                "ppi_cpi_gap": snap["ppi_cpi_gap"],
                "m1_m2_gap": snap["m1_m2_gap"],
            },
            "china": {
                "cn_cpi_yoy": snap["cn_cpi_yoy"],
                "cn_ppi_cpi_gap": snap["cn_ppi_cpi_gap"],
                "cn_m1_m2_gap": snap["cn_m1_m2_gap"],
            },
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
                    "jin10": {
                        "inserted_count": 8,
                        "latest_at": _recent_snapshot_ts(),
                        "failure_rate": 0.0,
                    },
                    "rss": {
                        "inserted_count": 6,
                        "latest_at": _recent_snapshot_ts(),
                        "failure_rate": 0.0,
                    },
                    "gdelt": {
                        "inserted_count": 4,
                        "latest_at": _recent_snapshot_ts(),
                        "failure_rate": 0.0,
                    },
                    "coinglass_newsflash": {
                        "inserted_count": 5,
                        "latest_at": _recent_snapshot_ts(),
                        "failure_rate": 0.0,
                    },
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
                {
                    "source": "coinglass_newsflash",
                    "updated_at": _recent_snapshot_ts(),
                    "last_success_at": _recent_snapshot_ts(),
                    "last_error": None,
                    "error_count": 0,
                    "success_count": 3,
                    "paused_until": "2026-04-01T00:00:00+00:00",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        ai_module.news_db,
        "get_llm_queue_stats",
        AsyncMock(return_value={"pending_total": 0, "counts": {}}),
    )
    monkeypatch.setattr(
        "core.data.google_trends_collector.load_latest", lambda keyword="bitcoin": 78.0
    )
    monkeypatch.setattr(
        options_collector,
        "load_cached_snapshot",
        lambda currency="BTC": FakeOptionsSnapshot(),
    )
    monkeypatch.setattr(ai_module.settings, "OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setattr(ai_module.settings, "OPENAI_BACKUP_API_KEY", "")

    funding_dir = tmp_path / "data" / "funding" / "binance"
    funding_dir.mkdir(parents=True, exist_ok=True)
    (funding_dir / "BTC_USDT_funding.parquet").write_text(
        "placeholder", encoding="utf-8"
    )

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
                "codex": {
                    "available": True,
                    "default_model": "gpt-5.4",
                    "base_url": "https://api.example.com",
                },
                "claude": {
                    "available": True,
                    "default_model": "claude-3-5-sonnet-latest",
                    "base_url": "https://anthropic.example.com",
                },
                "glm": {
                    "available": False,
                    "default_model": "GLM-4.5-Air",
                    "base_url": "https://glm.example.com",
                },
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
                "codex": {
                    "available": True,
                    "default_model": "gpt-5.4",
                    "base_url": "https://api.example.com",
                },
                "claude": {
                    "available": True,
                    "default_model": "claude-3-5-sonnet-latest",
                    "base_url": "https://anthropic.example.com",
                },
                "glm": {
                    "available": False,
                    "default_model": "GLM-4.5-Air",
                    "base_url": "https://glm.example.com",
                },
            },
        },
    )
    monkeypatch.setattr(
        "importlib.util.find_spec", lambda name: object() if name == "xgboost" else None
    )

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
    coinglass_newsflash = result["categories"]["news"]["sources"]["coinglass_newsflash"]
    assert coinglass_newsflash["health"] == "healthy"
    assert "Paused until" not in " ".join(coinglass_newsflash["issues"])

    coinglass_options = result["categories"]["options"]["sources"]["coinglass_options"]
    assert coinglass_options["health"] == "healthy"
    assert "options_info" in coinglass_options["snapshot"]["active_datasets"]

    coinglass_onchain = result["categories"]["premium_onchain"]["sources"]["coinglass_onchain"]
    assert coinglass_onchain["health"] == "healthy"
    assert "spot_coin_netflow" in coinglass_onchain["snapshot"]["active_datasets"]

    research_llm = result["categories"]["ai_sources"]["sources"]["research_context_llm"]
    assert research_llm["health"] == "healthy"
    assert research_llm["snapshot"]["provider"] == "codex"

    ml_model = result["categories"]["ai_sources"]["sources"]["ml_signal_model"]
    assert ml_model["health"] == "degraded"
    assert any("Non-canonical model filename" in item for item in ml_model["issues"])
    assert ml_model["snapshot"]["alternative_candidates"] == [
        str(Path("models") / "ml_signal_xgb")
    ]
