from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import research as research_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(research_api.router, prefix="/api/research")
    return app


def test_workbench_recommendations_return_structured_actions_and_ai_brief():
    payload = {
        "profile": {
            "exchange": "binance",
            "primary_symbol": "BTC/USDT",
            "universe_symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "timeframe": "5m",
            "lookback": 1200,
            "exclude_retired": True,
            "horizon": "short_intraday",
        },
        "overview": {"market_regime": "bullish_breakout"},
        "modules": {
            "market_state": {
                "payload": {
                    "regime": {"bias": "bullish", "regime": "bullish_breakout", "confidence": 0.78},
                    "sentiment_dashboard": {"news": {"events_count": 4}},
                }
            },
            "factors": {
                "payload": {
                    "factor_library": {
                        "asset_scores": [
                            {"symbol": "BTC/USDT"},
                            {"symbol": "ETH/USDT"},
                            {"symbol": "SOL/USDT"},
                        ]
                    }
                }
            },
            "cross_asset": {
                "payload": {
                    "cross_asset": {
                        "count": 5,
                        "leader_symbol": "SOL/USDT",
                    }
                }
            },
            "onchain": {
                "payload": {
                    "onchain": {
                        "degraded": True,
                        "whale_activity": {"count": 2},
                    },
                    "news_summary": {"events_count": 4},
                    "derivatives_summary": {
                        "available": True,
                        "status": "ok",
                        "provider": "coinglass",
                        "freshness_sec": 180.0,
                        "dataset_count": 2,
                        "funding_mean_rate_pct": 0.08,
                        "history_ready": True,
                        "history_interval": "h1",
                        "funding_zscore": 1.7,
                        "crowded_long": True,
                        "order_flow_confirmed": True,
                        "derivatives_labels": ["crowded_long", "order_flow_confirmed"],
                    },
                }
            },
            "discipline": {
                "payload": {
                    "behavior_report": {
                        "overtrading_warning": False,
                        "impulsive_ratio": 0.12,
                    }
                }
            },
        },
    }

    with TestClient(_build_app()) as client:
        response = client.post("/api/research/workbench/recommendations", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["headline"] == "bullish_breakout"
    assert data["focus_symbols"] == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert data["factor_focus"][0]["symbol"] == "BTC/USDT"
    assert data["factor_focus"][0]["score"] == 0.0
    assert data["ai_brief"]["planner_regime"] == "breakout"
    assert data["ai_brief"]["symbols"] == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert data["ai_brief"]["timeframes"] == ["5m", "15m", "1h"]
    assert data["ai_brief"]["factor_focus"][1]["symbol"] == "ETH/USDT"
    assert data["ai_brief"]["derivatives_context"]["available"] is True
    assert data["ai_brief"]["derivatives_context"]["provider"] == "coinglass"
    assert data["ai_brief"]["derivatives_context"]["freshness_sec"] == 180.0
    assert data["ai_brief"]["derivatives_context"]["history_ready"] is True
    assert data["ai_brief"]["derivatives_context"]["history_interval"] == "h1"
    assert data["ai_brief"]["derivatives_context"]["funding_zscore"] == 1.7
    assert "Derivatives shadow: ok / coinglass / 2 datasets" in data["ai_brief"]["prompt_context"]
    assert any("Derivatives" in item for item in data["ai_brief"]["thesis"])
    assert any("Derivatives funding z-score: +1.70." == item for item in data["ai_brief"]["thesis"])
    assert any("Order flow is confirming" in item for item in data["ai_brief"]["thesis"])
    assert any("funding and crowding" in item for item in data["ai_brief"]["next_steps"])
    assert any("open interest continue to confirm the squeeze setup" in item for item in data["ai_brief"]["next_steps"])
    assert data["source_meta"]["served_mode"] == "unknown"
    assert data["source_meta"]["universe_size"] == 0
    assert any(item["kind"] == "ai_prefill" for item in data["action_items"])
    assert any(
        item["kind"] == "backtest" and item["params"]["strategy_type"] == "DonchianBreakoutStrategy"
        for item in data["action_items"]
    )
    assert any(item["kind"] == "module" and item["module"] == "onchain" for item in data["action_items"])
    assert any(item["title"] == "Next Step" for item in data["insight_cards"])
    assert any(item["title"] == "Risk Note" for item in data["insight_cards"])
    assert any(
        item["tone"] == "neutral" and "BTC/USDT score 0.00" in str(item.get("body") or "")
        for item in data["insight_cards"]
    )
    assert any("Factor focus: BTC/USDT(0.00)" in str(item.get("body") or "") for item in data["insight_cards"])


def test_workbench_recommendations_prefer_market_state_derivatives_and_news_when_onchain_is_sparse():
    payload = {
        "profile": {
            "exchange": "binance",
            "primary_symbol": "BTC/USDT",
            "universe_symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "timeframe": "5m",
            "lookback": 1200,
            "exclude_retired": True,
            "horizon": "short_intraday",
        },
        "overview": {"market_regime": "trend_bullish"},
        "modules": {
            "market_state": {
                "payload": {
                    "regime": {"bias": "bullish", "regime": "trend_bullish", "confidence": 0.74},
                    "sentiment_dashboard": {
                        "news": {
                            "events_count": 3,
                            "feed_count": 1,
                            "raw_count": 2,
                            "scope": "symbol",
                        }
                    },
                    "derivatives_summary": {
                        "available": True,
                        "status": "ok",
                        "provider": "coinglass",
                        "freshness_sec": 120.0,
                        "dataset_count": 3,
                        "history_ready": True,
                        "history_interval": "h1",
                        "funding_zscore": 1.2,
                        "funding_mean_rate_pct": 0.05,
                        "long_short_ratio": 1.28,
                        "basis_pct": 0.16,
                    },
                }
            },
            "factors": {
                "payload": {
                    "factor_library": {
                        "asset_scores": [
                            {"symbol": "BTC/USDT"},
                            {"symbol": "ETH/USDT"},
                            {"symbol": "SOL/USDT"},
                        ]
                    }
                }
            },
            "cross_asset": {
                "payload": {
                    "cross_asset": {
                        "count": 4,
                        "leader_symbol": "BTC/USDT",
                    }
                }
            },
            "onchain": {
                "payload": {
                    "onchain": {
                        "degraded": False,
                        "whale_activity": {"count": 0},
                    },
                    "news_summary": {
                        "events_count": 0,
                        "feed_count": 0,
                        "raw_count": 0,
                        "scope": "global_fallback",
                    },
                    "derivatives_summary": {
                        "available": False,
                        "status": "missing",
                        "provider": "coinglass",
                        "dataset_count": 0,
                    },
                }
            },
            "discipline": {
                "payload": {
                    "behavior_report": {
                        "overtrading_warning": False,
                        "impulsive_ratio": 0.08,
                    }
                }
            },
        },
    }

    with TestClient(_build_app()) as client:
        response = client.post("/api/research/workbench/recommendations", json=payload)

    assert response.status_code == 200
    data = response.json()

    assert data["ai_brief"]["derivatives_context"]["available"] is True
    assert data["ai_brief"]["derivatives_context"]["dataset_count"] == 3
    assert "Derivatives shadow: ok / coinglass / 3 datasets" in data["ai_brief"]["prompt_context"]
    assert "Derivatives shadow is unavailable; do not rely on crowding/funding confirmation." not in data["avoid_conditions"]
    assert "Derivatives shadow is missing, so crowding/funding confirmation is incomplete." not in data["avoid_conditions"]
    assert "Symbol-level news coverage is sparse; avoid event-only decisions." not in data["avoid_conditions"]
    assert not any(item.get("kind") == "module" and item.get("module") == "onchain" for item in data["action_items"])
