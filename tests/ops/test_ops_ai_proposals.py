from __future__ import annotations

import asyncio
import time

from core.ops.service import api as ops_api


def test_create_ai_proposal_generates_templates_and_registry_entry(client, ops_headers):
    payload = {
        "thesis": "市场处于消息驱动与趋势共振阶段，先研究新闻驱动趋势策略组合。",
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "market_regime": "news_event",
        "timeframes": ["5m", "15m"],
    }

    response = client.post("/ops/ai/proposal", json=payload, headers=ops_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True

    proposal = body["data"]["proposal"]
    assert proposal["proposal_id"].startswith("proposal-")
    assert proposal["target_symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert proposal["target_timeframes"] == ["5m", "15m"]
    assert "MarketSentimentStrategy" in proposal["strategy_templates"]
    assert "news_events" in proposal["required_features"]
    assert proposal["metadata"]["created_by"] == "pytest"


def test_get_ai_proposal_round_trip(client, ops_headers):
    created = client.post(
        "/ops/ai/proposal",
        json={
            "thesis": "双资产横截面分化扩大时，研究统计套利与因子套利。",
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "market_regime": "stat_arb",
        },
        headers=ops_headers,
    ).json()
    proposal_id = created["data"]["proposal"]["proposal_id"]

    fetched = client.get(f"/ops/ai/proposal/{proposal_id}", headers=ops_headers)
    assert fetched.status_code == 200
    fetched_body = fetched.json()
    assert fetched_body["ok"] is True
    assert fetched_body["data"]["proposal"]["proposal_id"] == proposal_id
    assert "FamaFactorArbitrageStrategy" in fetched_body["data"]["proposal"]["strategy_templates"]

    listed = client.get("/ops/ai/proposals?limit=10", headers=ops_headers)
    assert listed.status_code == 200
    listed_body = listed.json()
    assert listed_body["ok"] is True
    assert any(item["proposal_id"] == proposal_id for item in listed_body["data"]["items"])


def test_run_ai_proposal_sync_updates_status_and_result(client, ops_headers, monkeypatch):
    async def fake_run_strategy_research(config):
        return {
            "exchange": config.exchange,
            "symbol": config.symbol,
            "timeframes": list(config.timeframes),
            "strategies": list(config.strategies),
            "runs": 6,
            "valid_runs": 4,
            "best": {
                "strategy": config.strategies[0],
                "timeframe": config.timeframes[0],
                "total_return": 18.5,
                "sharpe_ratio": 1.6,
                "max_drawdown": 7.2,
            },
            "csv_path": "research_out.csv",
            "markdown_path": "research_out.md",
        }

    monkeypatch.setattr(ops_api, "run_strategy_research", fake_run_strategy_research)

    created = client.post(
        "/ops/ai/proposal",
        json={
            "thesis": "趋势延续与新闻催化叠加，先做趋势策略研究。",
            "symbols": ["BTCUSDT"],
            "strategy_templates": ["MAStrategy", "MACDStrategy"],
            "timeframes": ["15m", "1h"],
        },
        headers=ops_headers,
    ).json()
    proposal_id = created["data"]["proposal"]["proposal_id"]

    response = client.post(
        f"/ops/ai/proposal/{proposal_id}/run",
        json={"background": False, "exchange": "binance", "days": 14},
        headers=ops_headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["data"]["proposal"]["status"] == "validated"
    assert body["data"]["research_result"]["best"]["strategy"] == "MAStrategy"
    validation = body["data"]["proposal"]["validation_summary"]
    assert validation["decision"] in {"paper", "shadow", "live_candidate"}
    assert validation["deployment_score"] > 0
    assert validation["metrics"]["valid_runs"] == 4

    validation_resp = client.get(f"/ops/ai/proposal/{proposal_id}/validation", headers=ops_headers)
    assert validation_resp.status_code == 200
    validation_body = validation_resp.json()
    assert validation_body["ok"] is True
    assert validation_body["data"]["validation_summary"]["deployment_score"] == validation["deployment_score"]


def test_run_ai_proposal_background_sets_queued_job(client, ops_headers, monkeypatch):
    async def fake_run_strategy_research(config):
        await asyncio.sleep(0.01)
        return {
            "exchange": config.exchange,
            "symbol": config.symbol,
            "timeframes": list(config.timeframes),
            "strategies": list(config.strategies),
            "runs": 2,
            "valid_runs": 0,
            "best": None,
            "csv_path": "research_bg.csv",
            "markdown_path": "research_bg.md",
        }

    monkeypatch.setattr(ops_api, "run_strategy_research", fake_run_strategy_research)

    created = client.post(
        "/ops/ai/proposal",
        json={
            "thesis": "多资产分化背景下测试统计套利草案。",
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "market_regime": "stat_arb",
        },
        headers=ops_headers,
    ).json()
    proposal_id = created["data"]["proposal"]["proposal_id"]

    response = client.post(
        f"/ops/ai/proposal/{proposal_id}/run",
        json={"background": True, "exchange": "binance", "days": 7},
        headers=ops_headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["data"]["proposal"]["status"] == "research_queued"
    assert body["data"]["job"]["proposal_id"] == proposal_id

    time.sleep(0.05)
    fetched = client.get(f"/ops/ai/proposal/{proposal_id}", headers=ops_headers).json()
    assert fetched["ok"] is True
    assert fetched["data"]["proposal"]["status"] in {"research_running", "rejected", "validated"}
