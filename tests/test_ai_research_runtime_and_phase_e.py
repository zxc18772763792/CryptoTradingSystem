from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pandas as pd


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_promote_candidate_auto_runtime_limit(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.deployment.promotion_engine import promote_candidate
    from core.research.experiment_schemas import PromotionDecision, StrategyCandidate

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-runtime",
        created_at=now,
        updated_at=now,
        thesis="runtime auto policy",
        status="validated",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-runtime",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-runtime",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 26},
        metadata={"best": {"total_trades": 60}, "exchange": "binance"},
    )
    promotion = PromotionDecision(
        candidate_id=candidate.candidate_id,
        decision="paper",
        reason="test auto runtime",
        constraints={"allocation_cap": 0.1},
        created_at=now,
    )

    register_mock = MagicMock(return_value=True)
    start_mock = AsyncMock(return_value=True)
    persist_mock = AsyncMock(return_value=None)
    app = MagicMock()
    app.state.ai_lifecycle_registry = MagicMock()
    app.state.ai_lifecycle_registry.append = MagicMock()
    app.state.ai_experiment_registry = MagicMock()
    app.state.ai_experiment_registry.get = MagicMock(return_value=SimpleNamespace(days=30))

    monkeypatch.setattr("core.deployment.promotion_engine._resolve_strategy_class", lambda _: object)
    monkeypatch.setattr("core.deployment.promotion_engine.get_strategy_defaults", lambda _: {})
    monkeypatch.setattr("core.deployment.promotion_engine.execution_engine.get_trading_mode", lambda: "paper")
    monkeypatch.setattr("core.deployment.promotion_engine.strategy_manager.register_strategy", register_mock)
    monkeypatch.setattr("core.deployment.promotion_engine.strategy_manager.start_strategy", start_mock)
    monkeypatch.setattr("core.deployment.promotion_engine.persist_strategy_snapshot", persist_mock)

    result = asyncio.run(
        promote_candidate(
            app,
            proposal=proposal,
            candidate=candidate,
            promotion=promotion,
            actor="unit_test",
        )
    )

    assert result["runtime_status"] == "paper_running"
    runtime_minutes = register_mock.call_args.kwargs["runtime_limit_minutes"]
    assert isinstance(runtime_minutes, int)
    assert runtime_minutes > 0
    runtime_meta = candidate.metadata["promotion_runtime"]
    assert runtime_meta["runtime_limit_minutes"] == runtime_minutes
    assert runtime_meta["runtime_policy"]["source"] in {"observed", "inferred", "promotion_constraint"}


def test_promote_candidate_runtime_override(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.deployment.promotion_engine import promote_candidate
    from core.research.experiment_schemas import PromotionDecision, StrategyCandidate

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-runtime-override",
        created_at=now,
        updated_at=now,
        thesis="runtime override policy",
        status="validated",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-runtime-override",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-runtime-override",
        created_at=now,
        strategy="MAStrategy",
        timeframe="15m",
        symbol="BTC/USDT",
        params={"fast_period": 5, "slow_period": 20},
        metadata={"exchange": "binance"},
    )
    promotion = PromotionDecision(
        candidate_id=candidate.candidate_id,
        decision="paper",
        reason="manual runtime override",
        constraints={"allocation_cap": 0.1, "runtime_limit_minutes": 1800},
        created_at=now,
    )

    register_mock = MagicMock(return_value=True)
    start_mock = AsyncMock(return_value=True)
    persist_mock = AsyncMock(return_value=None)
    app = MagicMock()
    app.state.ai_lifecycle_registry = MagicMock()
    app.state.ai_lifecycle_registry.append = MagicMock()
    app.state.ai_experiment_registry = MagicMock()
    app.state.ai_experiment_registry.get = MagicMock(return_value=None)

    monkeypatch.setattr("core.deployment.promotion_engine._resolve_strategy_class", lambda _: object)
    monkeypatch.setattr("core.deployment.promotion_engine.get_strategy_defaults", lambda _: {})
    monkeypatch.setattr("core.deployment.promotion_engine.execution_engine.get_trading_mode", lambda: "paper")
    monkeypatch.setattr("core.deployment.promotion_engine.strategy_manager.register_strategy", register_mock)
    monkeypatch.setattr("core.deployment.promotion_engine.strategy_manager.start_strategy", start_mock)
    monkeypatch.setattr("core.deployment.promotion_engine.persist_strategy_snapshot", persist_mock)

    asyncio.run(
        promote_candidate(
            app,
            proposal=proposal,
            candidate=candidate,
            promotion=promotion,
            actor="unit_test",
        )
    )
    assert register_mock.call_args.kwargs["runtime_limit_minutes"] == 1800


def test_quick_register_uses_correct_promote_signature(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import PromotionDecision, StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-quick-register",
        created_at=now,
        updated_at=now,
        thesis="quick register",
        status="validated",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-quick-register",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-quick-register",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 5},
        metadata={"promotion_pending_human_gate": True},
        promotion=PromotionDecision(
            candidate_id="cand-quick-register",
            decision="paper",
            reason="test",
            constraints={},
            created_at=now,
        ),
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                ai_candidate_registry=SimpleNamespace(save=lambda _: None),
            )
        )
    )
    promote_mock = AsyncMock(
        return_value={
            "candidate": candidate,
            "proposal": proposal,
            "promotion": candidate.promotion,
            "runtime_status": "paper_running",
            "registered_strategy_name": "test_strategy_name",
        }
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "save_proposal", lambda app, p: p)
    monkeypatch.setattr(ai_module, "write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(ai_module.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr("core.deployment.promotion_engine.promote_candidate", promote_mock)

    result = asyncio.run(
        ai_module.quick_register_candidate(
            request,
            "cand-quick-register",
            ai_module.AIQuickRegisterRequest(allocation_pct=0.12),
        )
    )
    assert result["runtime_status"] == "paper_running"
    assert candidate.promotion.constraints["allocation_cap"] == 0.12
    assert promote_mock.await_count == 1
    kwargs = promote_mock.await_args.kwargs
    assert "promotion" in kwargs
    assert kwargs["promotion"] is candidate.promotion


def test_param_sensitivity_endpoint(monkeypatch):
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    candidate = StrategyCandidate(
        candidate_id="cand-sensitivity",
        proposal_id="proposal-sensitivity",
        experiment_id="exp-sensitivity",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 10, "slow_period": 30, "note": "ignore-nonnumeric"},
        metadata={},
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    df = pd.DataFrame(
        {"open": [1.0, 1.1, 1.2], "high": [1.1, 1.2, 1.3], "low": [0.9, 1.0, 1.1], "close": [1.0, 1.15, 1.2], "volume": [10, 12, 11]},
        index=pd.date_range("2024-01-01", periods=3, freq="1h"),
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr("web.api.backtest._load_backtest_inputs", AsyncMock(return_value=(df, None, "BTC/USDT")))
    monkeypatch.setattr("web.api.backtest._attach_backtest_enrichment_if_needed", AsyncMock(return_value=df))
    monkeypatch.setattr("web.api.backtest._run_backtest_core", MagicMock(return_value={"sharpe_ratio": 1.25}))

    result = asyncio.run(ai_module.get_candidate_param_sensitivity(request, "cand-sensitivity", max_params=2))
    assert result["candidate_id"] == "cand-sensitivity"
    assert len(result["items"]) == 2
    assert all("param" in row for row in result["items"])


def test_phase_e_ui_functions_present():
    repo_root = Path(__file__).resolve().parents[1]
    js_text = (repo_root / "web" / "static" / "js" / "ai_research.js").read_text(encoding="utf-8")
    assert "function _renderValidationPipeline" in js_text
    assert "function renderLifecycleStepper" in js_text
    assert "function _renderApprovalMeta" in js_text
    assert "function loadParamSensitivity" in js_text
    assert "function openCompareModal" in js_text
