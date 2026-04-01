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
        "overview": {"market_regime": "上涨突破"},
        "modules": {
            "market_state": {
                "payload": {
                    "regime": {"bias": "bullish", "regime": "上涨突破", "confidence": 0.78},
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
    assert data["headline"] == "上涨突破"
    assert data["focus_symbols"] == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert data["factor_focus"][0]["symbol"] == "BTC/USDT"
    assert data["factor_focus"][0]["score"] == 0.0
    assert data["ai_brief"]["planner_regime"] == "breakout"
    assert data["ai_brief"]["symbols"] == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert data["ai_brief"]["timeframes"] == ["5m", "15m", "1h"]
    assert data["ai_brief"]["factor_focus"][1]["symbol"] == "ETH/USDT"
    assert data["source_meta"]["served_mode"] == "unknown"
    assert data["source_meta"]["universe_size"] == 0
    assert any(item["kind"] == "ai_prefill" for item in data["action_items"])
    assert any(
        item["kind"] == "backtest" and item["params"]["strategy_type"] == "DonchianBreakoutStrategy"
        for item in data["action_items"]
    )
    assert any(item["kind"] == "module" and item["module"] == "onchain" for item in data["action_items"])
    assert any(item["title"] == "因子观察" for item in data["insight_cards"])
    assert any(item["title"] == "研究观察" for item in data["insight_cards"])
