from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd

from config.settings import settings
from core.ai.proposal_schemas import ProposalValidationSummary, ResearchProposal
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.research.experiment_registry import CandidateRegistry, ProposalRegistry
from core.research.experiment_schemas import StrategyCandidate


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sample_df() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=120, freq="15min")
    close = [100.0 + i * 0.2 for i in range(len(idx))]
    return pd.DataFrame(
        {
            "open": close,
            "high": [v + 0.1 for v in close],
            "low": [v - 0.1 for v in close],
            "close": close,
            "volume": [10.0] * len(close),
        },
        index=idx,
    )


def _seed_runtime_research(monkeypatch, tmp_path: Path) -> tuple[ResearchProposal, StrategyCandidate]:
    data_storage_path = tmp_path / "storage" / "klines"
    monkeypatch.setattr(settings, "DATA_STORAGE_PATH", str(data_storage_path), raising=False)
    base_dir = (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "ai").resolve()

    proposal_registry = ProposalRegistry(base_dir / "proposals.json")
    candidate_registry = CandidateRegistry(base_dir / "candidates.json")

    now = _now()
    summary = ProposalValidationSummary(
        computed_at=now,
        decision="paper",
        edge_score=75.0,
        risk_score=80.0,
        stability_score=70.0,
        efficiency_score=72.0,
        deployment_score=78.0,
        is_score=1.7,
        oos_score=1.2,
        wf_stability=0.74,
        robustness_score=69.0,
        reasons=["paper ready"],
    )
    proposal = ResearchProposal(
        proposal_id="proposal-phase4-runtime",
        created_at=now,
        updated_at=now,
        status="validated",
        research_mode="hybrid",
        thesis="Use the current champion candidate as runtime research context.",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=["EMAStrategy"],
    )
    champion = StrategyCandidate(
        candidate_id="cand-phase4-champion",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-phase4-runtime",
        created_at=now,
        strategy="OpenAI EMA Draft",
        timeframe="1h",
        symbol="BTC/USDT",
        score=88.0,
        validation_summary=summary,
        promotion_target="paper",
        status="paper_running",
        metadata={
            "exchange": "binance",
            "research_mode": "hybrid",
            "search_role": "champion",
            "champion_candidate_id": "cand-phase4-champion",
            "champion_strategy": "OpenAI EMA Draft",
            "decision_engine": "openai",
            "strategy_family": "ai_openai",
            "search_summary": {
                "loop_enabled": True,
                "evaluated_drafts": 4,
                "accepted_drafts": 2,
                "rejected_drafts": 2,
                "champion_draft_id": "draft-ema-01",
            },
        },
    )
    challenger = StrategyCandidate(
        candidate_id="cand-phase4-challenger",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-phase4-runtime",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        score=92.0,
        validation_summary=summary,
        promotion_target="paper",
        status="new",
        metadata={
            "exchange": "binance",
            "research_mode": "hybrid",
            "search_role": "challenger",
            "champion_candidate_id": "cand-phase4-champion",
            "champion_strategy": "OpenAI EMA Draft",
            "decision_engine": "traditional",
            "strategy_family": "traditional",
        },
    )

    proposal_registry.save(proposal)
    candidate_registry.save(champion)
    candidate_registry.save(challenger)
    return proposal, champion


def test_resolve_runtime_research_context_prefers_active_champion(monkeypatch, tmp_path: Path):
    _, champion = _seed_runtime_research(monkeypatch, tmp_path)

    context = resolve_runtime_research_context(
        exchange="binance",
        symbol="BTC/USDT",
        timeframe="1h",
    )

    assert context["available"] is True
    assert context["candidate_count"] == 2
    assert context["selection_reason"] == "active_runtime_candidate"
    assert context["selected_candidate"]["candidate_id"] == champion.candidate_id
    assert context["research_champion"]["candidate_id"] == champion.candidate_id
    assert context["selected_candidate"]["proposal_id"] == "proposal-phase4-runtime"


def test_autonomous_agent_run_once_does_not_attach_research_refs(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    _, champion = _seed_runtime_research(monkeypatch, tmp_path)
    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.84,
                "strength": 0.78,
                "leverage": 4,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
                "reason": "aligned_with_champion",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", timeframe="1h", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    signal = submit_mock.await_args.args[0]
    assert champion.candidate_id == "cand-phase4-champion"
    assert "research_context_available" not in signal.metadata
    assert "research_candidate_id" not in signal.metadata
    assert "research_proposal_id" not in signal.metadata
    assert "research_champion_candidate_id" not in signal.metadata
    assert result["status"]["last_research_context"] is None


def test_live_decision_router_includes_research_context(monkeypatch, tmp_path: Path):
    import core.ai.live_decision_router as live_module
    from core.ai.live_decision_router import LiveAIDecisionRouter

    _, champion = _seed_runtime_research(monkeypatch, tmp_path)
    monkeypatch.setattr(live_module, "_OVERLAY_PATH", tmp_path / "ai_runtime_config.json")
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "enforce", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_PROVIDER", "codex", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODEL", "gpt-5.4", raising=False)

    async def _fake_call_provider(**kwargs):
        payload = json.loads(kwargs["user_prompt"])
        research_context = payload["input"]["research_context"]
        assert research_context["available"] is True
        assert research_context["selected_candidate"]["candidate_id"] == champion.candidate_id
        assert research_context["research_champion"]["candidate_id"] == champion.candidate_id
        return {"action": "allow", "reason": "aligned_with_research_context", "confidence": 0.77}

    monkeypatch.setattr(router, "_call_provider", _fake_call_provider)

    result = asyncio.run(
        router.evaluate_signal(
            trading_mode="live",
            strategy="OpenAI EMA Draft",
            symbol="BTC/USDT",
            signal_type="buy",
            signal_strength=0.81,
            price=65000.0,
            account_equity=10000.0,
            order_value=500.0,
            leverage=3.0,
            timeframe="1h",
            existing_position={"side": "long", "quantity": 0.02},
            trade_policy={"allow_long": True, "allow_short": True},
            metadata={"exchange": "binance", "account_id": "main"},
        )
    )

    assert result["action"] == "allow"
    assert result["allowed"] is True
    assert result["research_context"]["selected_candidate"]["candidate_id"] == champion.candidate_id
