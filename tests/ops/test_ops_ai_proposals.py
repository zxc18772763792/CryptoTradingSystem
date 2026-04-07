from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from config.settings import settings
from core.ops.service import api as ops_api
from core.research import orchestrator as ai_orchestrator


def _runtime_snapshot_path() -> Path:
    return (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "runtime" / "eligibility_snapshot.json").resolve()


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
                "win_rate": 58.0,
                "total_trades": 48,
            },
            "csv_path": "research_out.csv",
            "markdown_path": "research_out.md",
        }

    monkeypatch.setattr(ai_orchestrator, "run_strategy_research", fake_run_strategy_research)

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
    assert body["data"]["proposal"]["status"] in {"validated", "paper_running", "shadow_running", "live_candidate"}
    assert body["data"]["research_result"]["best"]["strategy"] == "MAStrategy"
    assert body["data"]["candidate"]["strategy"] == "MAStrategy"
    validation = body["data"]["proposal"]["validation_summary"]
    assert validation["decision"] in {"paper", "shadow", "live_candidate"}
    assert validation["deployment_score"] > 0
    assert validation["metrics"]["valid_runs"] == 4

    validation_resp = client.get(f"/ops/ai/proposal/{proposal_id}/validation", headers=ops_headers)
    assert validation_resp.status_code == 200
    validation_body = validation_resp.json()
    assert validation_body["ok"] is True
    assert validation_body["data"]["validation_summary"]["deployment_score"] == validation["deployment_score"]

    snapshot_path = _runtime_snapshot_path()
    assert snapshot_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    records = list(snapshot.get("records") or [])
    candidate_id = body["data"]["candidate"]["candidate_id"]
    selected = next(item for item in records if item["candidate_id"] == candidate_id)
    assert snapshot["schema_version"] == "runtime_eligibility.v1"
    assert selected["proposal_id"] == proposal_id
    assert selected["symbol"] == "BTC/USDT"
    assert selected["timeframe"] == "15m"


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

    monkeypatch.setattr(ai_orchestrator, "run_strategy_research", fake_run_strategy_research)

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


def test_ai_candidate_and_lifecycle_endpoints(client, ops_headers, monkeypatch):
    async def fake_run_strategy_research(config):
        return {
            "exchange": config.exchange,
            "symbol": config.symbol,
            "timeframes": list(config.timeframes),
            "strategies": list(config.strategies),
            "runs": 5,
            "valid_runs": 3,
            "best": {
                "strategy": config.strategies[0],
                "timeframe": config.timeframes[0],
                "total_return": 9.5,
                "sharpe_ratio": 1.05,
                "max_drawdown": 8.0,
                "win_rate": 54.0,
                "total_trades": 24,
                "score": 22.5,
            },
            "quality_counts": {"ok": 3},
            "csv_path": "research_ops.csv",
            "markdown_path": "research_ops.md",
        }

    monkeypatch.setattr(ai_orchestrator, "run_strategy_research", fake_run_strategy_research)

    created = client.post(
        "/ops/ai/proposal",
        json={
            "thesis": "测试 AI workbench 的 candidate、lifecycle 与 promote 接口。",
            "symbols": ["BTCUSDT"],
            "strategy_templates": ["MAStrategy"],
            "timeframes": ["15m"],
        },
        headers=ops_headers,
    ).json()
    proposal_id = created["data"]["proposal"]["proposal_id"]

    run_resp = client.post(
        f"/ops/ai/proposal/{proposal_id}/run",
        json={"background": False, "exchange": "binance", "days": 30},
        headers=ops_headers,
    ).json()
    candidate_id = run_resp["data"]["candidate"]["candidate_id"]

    lifecycle = client.get(f"/ops/ai/proposal/{proposal_id}/lifecycle", headers=ops_headers).json()
    assert lifecycle["ok"] is True
    assert lifecycle["data"]["count"] >= 2

    candidates = client.get("/ops/ai/candidates?limit=10", headers=ops_headers).json()
    assert candidates["ok"] is True
    assert any(item["candidate_id"] == candidate_id for item in candidates["data"]["items"])

    promote = client.post(
        f"/ops/ai/candidate/{candidate_id}/promote",
        json={"target": run_resp["data"]["candidate"]["promotion"]["decision"]},
        headers=ops_headers,
    ).json()
    assert promote["ok"] is True
    assert promote["data"]["candidate_id"] == candidate_id
    assert promote["data"]["promotion"]["decision"] in {"paper", "shadow", "live_candidate"}

    snapshot_path = _runtime_snapshot_path()
    assert snapshot_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    promoted = next(item for item in snapshot.get("records", []) if item["candidate_id"] == candidate_id)
    assert promoted["proposal_id"] == proposal_id
    assert promoted["status"] in {"paper_running", "live_candidate"}
    if promoted["status"] == "paper_running":
        assert promoted["runtime_mode_cap"] == "paper_execute"
    else:
        assert promoted["runtime_mode_cap"] == "live_candidate_only"
