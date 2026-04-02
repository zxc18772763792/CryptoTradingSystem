from __future__ import annotations

import asyncio
import pytest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pandas as pd


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _build_ai_research_request(candidate_save=None):
    registry = SimpleNamespace(save=MagicMock(side_effect=candidate_save))
    lifecycle_registry = MagicMock()
    lifecycle_registry.append = MagicMock()
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                ai_candidate_registry=registry,
                ai_lifecycle_registry=lifecycle_registry,
            )
        )
    )


def test_transition_proposal_allows_validated_to_rejected():
    from core.ai.proposal_schemas import ResearchProposal
    from core.deployment.promotion_engine import transition_proposal

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-transition-reject",
        created_at=now,
        updated_at=now,
        thesis="transition check",
        status="validated",
    )
    lifecycle_registry = MagicMock()
    lifecycle_registry.append = MagicMock()

    result = transition_proposal(
        proposal,
        to_state="rejected",
        lifecycle_registry=lifecycle_registry,
        actor="unit_test",
        reason="validation gate rejected candidate",
    )

    assert result.status == "rejected"
    assert lifecycle_registry.append.call_count == 1
    record = lifecycle_registry.append.call_args.args[0]
    assert record.from_state == "validated"
    assert record.to_state == "rejected"


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


def test_activate_ai_candidate_live_requires_live_mode(monkeypatch):
    from fastapi import HTTPException

    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-live-mode-check",
        created_at=now,
        updated_at=now,
        thesis="live mode gate",
        status="paper_running",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-live-mode-check",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-live-mode-check",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 26},
        status="paper_running",
        metadata={"exchange": "binance"},
    )
    request = _build_ai_research_request()
    ensure_mock = AsyncMock(return_value={"registered_strategy_name": "unused"})

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "_ensure_candidate_runtime_strategy", ensure_mock)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "paper")

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            ai_module.activate_ai_candidate_live(
                request,
                candidate.candidate_id,
                ai_module.AICandidateActivateLiveRequest(notes="should fail outside live mode"),
            )
        )

    assert exc.value.status_code == 400
    assert "switch to live mode first" in exc.value.detail
    assert ensure_mock.await_count == 0


def test_activate_ai_candidate_live_governance_rejects_unapproved_candidate(monkeypatch):
    from fastapi import HTTPException

    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-live-governance-denied",
        created_at=now,
        updated_at=now,
        thesis="governance gate deny",
        status="paper_running",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-live-governance-denied",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-live-governance-denied",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 26},
        status="paper_running",
        metadata={"exchange": "binance"},
    )
    request = _build_ai_research_request()
    ensure_mock = AsyncMock(return_value={"registered_strategy_name": "unused"})

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "_ensure_candidate_runtime_strategy", ensure_mock)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(ai_module, "write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(ai_module.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            ai_module.activate_ai_candidate_live(
                request,
                candidate.candidate_id,
                ai_module.AICandidateActivateLiveRequest(notes="should fail under governance"),
            )
        )

    assert exc.value.status_code == 400
    assert "human-approved for live activation" in exc.value.detail
    assert ensure_mock.await_count == 0


def test_activate_ai_candidate_live_rejects_pending_human_gate(monkeypatch):
    from fastapi import HTTPException

    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-live-pending-gate",
        created_at=now,
        updated_at=now,
        thesis="pending approval gate",
        status="live_candidate",
        metadata={"promotion_pending_human_gate": True},
    )
    candidate = StrategyCandidate(
        candidate_id="cand-live-pending-gate",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-live-pending-gate",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 26},
        status="live_candidate",
        metadata={
            "exchange": "binance",
            "promotion_pending_human_gate": True,
            "human_approved_at": now.isoformat(),
            "human_approved_target": "live_candidate",
        },
    )
    request = _build_ai_research_request()
    ensure_mock = AsyncMock(return_value={"registered_strategy_name": "unused"})

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "_ensure_candidate_runtime_strategy", ensure_mock)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(ai_module, "write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(ai_module.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            ai_module.activate_ai_candidate_live(
                request,
                candidate.candidate_id,
                ai_module.AICandidateActivateLiveRequest(notes="pending gate should block"),
            )
        )

    assert exc.value.status_code == 400
    assert "pending human approval" in exc.value.detail
    assert ensure_mock.await_count == 0


def test_activate_ai_candidate_live_transitions_live_candidate(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-live-candidate",
        created_at=now,
        updated_at=now,
        thesis="live candidate activate",
        status="live_candidate",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-live-candidate",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-live-candidate",
        created_at=now,
        strategy="MAStrategy",
        timeframe="15m",
        symbol="ETH/USDT",
        params={"fast_period": 5, "slow_period": 20},
        status="live_candidate",
        metadata={
            "exchange": "binance",
            "human_approved_at": now.isoformat(),
            "human_approved_target": "live_candidate",
        },
    )
    request = _build_ai_research_request()
    save_proposal_mock = MagicMock()

    async def _fake_ensure(app, cand, target_mode="live"):
        cand.metadata["registered_strategy_name"] = "cand_live_strategy"
        cand.metadata["promotion_runtime"] = {
            "mode": target_mode,
            "registered_strategy_name": "cand_live_strategy",
            "started": True,
        }
        return {
            "registered_strategy_name": "cand_live_strategy",
            "allocation": 0.15,
            "runtime_limit_minutes": 720,
            "runtime_policy": {"runtime_limit_minutes": 720, "source": "unit_test"},
        }

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "_ensure_candidate_runtime_strategy", _fake_ensure)
    monkeypatch.setattr(ai_module, "save_proposal", save_proposal_mock)
    monkeypatch.setattr(ai_module, "write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(ai_module.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)

    result = asyncio.run(
        ai_module.activate_ai_candidate_live(
            request,
            candidate.candidate_id,
            ai_module.AICandidateActivateLiveRequest(notes="human confirmed from AI research"),
        )
    )

    assert result["runtime_status"] == "live_running"
    assert result["registered_strategy_name"] == "cand_live_strategy"
    assert candidate.status == "live_running"
    assert proposal.status == "live_running"
    assert candidate.metadata["live_activation_source"] == "ai_research"
    assert candidate.metadata["registered_strategy_name"] == "cand_live_strategy"
    assert request.app.state.ai_candidate_registry.save.call_count == 1
    assert save_proposal_mock.call_count == 1


def test_activate_ai_candidate_live_promotes_paper_running_candidate(monkeypatch):
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate
    from web.api import ai_research as ai_module

    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-paper-running-live",
        created_at=now,
        updated_at=now,
        thesis="paper running to live",
        status="paper_running",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-paper-running-live",
        proposal_id=proposal.proposal_id,
        experiment_id="exp-paper-running-live",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 12, "slow_period": 48},
        status="paper_running",
        metadata={
            "exchange": "binance",
            "registered_strategy_name": "paper_runtime_strategy",
            "promotion_runtime": {
                "mode": "paper",
                "registered_strategy_name": "paper_runtime_strategy",
            },
        },
    )
    request = _build_ai_research_request()
    save_proposal_mock = MagicMock()

    async def _fake_ensure(app, cand, target_mode="live"):
        assert cand.metadata["registered_strategy_name"] == "paper_runtime_strategy"
        cand.metadata["promotion_runtime"] = {
            "mode": target_mode,
            "registered_strategy_name": "paper_runtime_strategy",
            "started": True,
        }
        return {
            "registered_strategy_name": "paper_runtime_strategy",
            "allocation": 0.1,
            "runtime_limit_minutes": 1440,
            "runtime_policy": {"runtime_limit_minutes": 1440, "source": "existing_strategy"},
        }

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)
    monkeypatch.setattr(ai_module, "_ensure_candidate_runtime_strategy", _fake_ensure)
    monkeypatch.setattr(ai_module, "save_proposal", save_proposal_mock)
    monkeypatch.setattr(ai_module, "write_audit", AsyncMock(return_value=None))
    monkeypatch.setattr(ai_module.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)

    result = asyncio.run(
        ai_module.activate_ai_candidate_live(
            request,
            candidate.candidate_id,
            ai_module.AICandidateActivateLiveRequest(notes="upgrade paper runtime into live"),
        )
    )

    assert result["runtime_status"] == "live_running"
    assert result["registered_strategy_name"] == "paper_runtime_strategy"
    assert candidate.status == "live_running"
    assert proposal.status == "live_running"
    assert candidate.metadata["promotion_runtime"]["mode"] == "live"
    assert request.app.state.ai_candidate_registry.save.call_count == 1
    assert save_proposal_mock.call_count == 1


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


def test_research_onchain_ui_mentions_premium_sources():
    repo_root = Path(__file__).resolve().parents[1]
    js_text = (repo_root / "web" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert "premium_external" in js_text
    assert "高级源快照" in js_text


def test_ai_research_readiness_mentions_premium_sources():
    repo_root = Path(__file__).resolve().parents[1]
    js_text = (repo_root / "web" / "static" / "js" / "ai_research.js").read_text(encoding="utf-8")
    assert "/premium-data/status" in js_text
    assert "高级数据源" in js_text
    assert "/live-signals" in js_text


def test_ai_research_and_agent_live_signal_panels_are_separated():
    repo_root = Path(__file__).resolve().parents[1]
    html_text = (repo_root / "web" / "templates" / "index.html").read_text(encoding="utf-8")
    js_text = (repo_root / "web" / "static" / "js" / "ai_research.js").read_text(encoding="utf-8")

    assert "ai-research-live-signals-panel" in html_text
    assert "ai-agent-live-signals-panel" in html_text
    assert "研究候选运行信号" in html_text
    assert "自治代理聚合信号" in html_text
    assert "ai-live-signals-panel" not in html_text
    assert "renderLiveSignalPanels" in js_text
    assert js_text.count("async function loadLiveSignals()") == 1
    assert "/autonomous-agent/live-signals" in js_text
    assert "当前研究冠军" not in html_text
    assert "只使用已有研究结果" not in html_text
    assert "基于实时行情、聚合信号、风控和执行状态独立给出动作并尝试下单。" in html_text


def test_ai_research_live_activation_flow_hooks_present():
    repo_root = Path(__file__).resolve().parents[1]
    js_text = (repo_root / "web" / "static" / "js" / "ai_research.js").read_text(encoding="utf-8")
    assert "function activateCandidateLive" in js_text
    assert "/trading/mode/request" in js_text
    assert "/trading/mode/confirm" in js_text
    assert "/candidates/${encodeURIComponent(safeCandidateId)}/activate-live" in js_text
    assert "btn-activate-live" in js_text


def test_premium_data_status_treats_cached_data_as_available(monkeypatch):
    from web.api import ai_research as ai_module

    monkeypatch.setattr("core.data.glassnode_collector.load_glassnode_snapshot", lambda: {"sopr": 1.02, "mvrv_z": None})
    monkeypatch.setattr("core.data.glassnode_collector._api_key", lambda: "")

    result = asyncio.run(ai_module.get_premium_data_status())
    source = result["sources"]["glassnode"]
    assert source["has_cached_data"] is True
    assert source["key_configured"] is False
    assert source["available"] is True


def test_candidate_symbol_and_strategy_helpers_support_legacy_fields():
    from web.api import ai_research as ai_module

    current = SimpleNamespace(symbol="ETHUSDT", strategy="Trend")
    legacy = SimpleNamespace(symbols=["BTC/USDT"], strategy_name="MeanRev")
    missing = SimpleNamespace()

    assert ai_module._candidate_primary_symbol(current) == "ETH/USDT"
    assert ai_module._candidate_primary_symbol(legacy) == "BTC/USDT"
    assert ai_module._candidate_primary_symbol(missing) == "BTC/USDT"
    assert ai_module._candidate_strategy_name(current) == "Trend"
    assert ai_module._candidate_strategy_name(legacy) == "MeanRev"
    assert ai_module._candidate_strategy_name(missing) == "unknown"


def _patch_live_signals_watchlist(monkeypatch, ai_module, *, runtime_cfg=None, selection=None):
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: dict(runtime_cfg or {}),
    )
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_symbol_scan_preview",
        AsyncMock(return_value=dict(selection or {})),
    )
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_symbol_scan",
        AsyncMock(return_value=dict(selection or {})),
    )
    if not runtime_cfg and not selection:
        monkeypatch.setattr(ai_module, "_build_live_signal_watchlist_symbols", lambda **kwargs: [])


def test_live_signals_works_with_symbol_field_candidates(monkeypatch):
    from web.api import ai_research as ai_module

    candidate = SimpleNamespace(
        candidate_id="cand-live-signals",
        strategy="MAStrategy",
        symbol="BTC/USDT",
        status="paper_running",
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    class _Signal:
        def to_dict(self):
            return {"direction": "LONG", "components": {"factor": {"confidence": 0.62}}}

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [candidate])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr(
        "core.data.data_storage.load_klines_from_parquet",
        AsyncMock(return_value=pd.DataFrame({"close": [1.0, 1.1, 1.2]})),
    )
    monkeypatch.setattr(
        "core.ai.signal_aggregator.signal_aggregator",
        SimpleNamespace(aggregate=AsyncMock(return_value=_Signal())),
    )

    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["count"] == 1
    assert result["candidate_count"] == 1
    assert result["watchlist_count"] == 0
    assert [section["id"] for section in result["sections"]] == ["candidates"]
    item = result["candidate_items"][0]
    assert item["candidate_id"] == "cand-live-signals"
    assert item["strategy"] == "MAStrategy"
    assert item["symbol"] == "BTC/USDT"
    assert item["timeframe"] == "1h"
    assert item["source"] == "candidate"
    assert item["signal"]["direction"] == "LONG"


def test_runtime_config_contains_ai_live_decision(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(
        ai_module.live_decision_router,
        "get_runtime_config",
        lambda: {"enabled": False, "mode": "shadow", "provider": "glm", "model": "GLM-4.5-Air"},
    )

    result = asyncio.run(ai_module.get_ai_runtime_config(request))
    assert "ai_live_decision" in result
    assert result["ai_live_decision"]["provider"] == "glm"
    assert result["ai_live_decision"]["mode"] == "shadow"


def test_update_runtime_config_live_decision_endpoint(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)

    async def _fake_update_runtime_config(**kwargs):
        return {
            "enabled": bool(kwargs.get("enabled")),
            "mode": str(kwargs.get("mode") or "shadow"),
            "provider": str(kwargs.get("provider") or "glm"),
            "model": str(kwargs.get("model") or ""),
        }

    monkeypatch.setattr(ai_module.live_decision_router, "update_runtime_config", _fake_update_runtime_config)

    payload = ai_module.AILiveDecisionConfigUpdateRequest(
        enabled=True,
        mode="enforce",
        provider="claude",
        model="claude-3-5-sonnet-latest",
    )
    result = asyncio.run(ai_module.update_ai_live_decision_runtime_config(request, payload))
    assert result["updated"] is True
    assert result["config"]["enabled"] is True
    assert result["config"]["mode"] == "enforce"
    assert result["config"]["provider"] == "claude"


# ── Step 2 回归：ml_model 可用性场景 ─────────────────────────────────────────

def test_live_signals_ml_model_unavailable(monkeypatch):
    """ml_model_loaded=False when aggregator has no _ml_model attribute."""
    from web.api import ai_research as ai_module

    class _Signal:
        def to_dict(self):
            return {"direction": "FLAT", "components": {}}

    stub_aggregator = SimpleNamespace(aggregate=AsyncMock(return_value=_Signal()))
    # stub_aggregator intentionally has NO _ml_model

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr("core.ai.signal_aggregator.signal_aggregator", stub_aggregator)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["ml_model_loaded"] is False


def test_live_signals_ml_model_loaded_true(monkeypatch):
    """ml_model_loaded=True when aggregator._ml_model.is_loaded() returns True."""
    from web.api import ai_research as ai_module

    class _Signal:
        def to_dict(self):
            return {"direction": "LONG", "components": {}}

    ml_stub = SimpleNamespace(is_loaded=lambda: True)
    stub_aggregator = SimpleNamespace(aggregate=AsyncMock(return_value=_Signal()), _ml_model=ml_stub)

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr("core.ai.signal_aggregator.signal_aggregator", stub_aggregator)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["ml_model_loaded"] is True


def test_live_signals_ml_model_loaded_false(monkeypatch):
    """ml_model_loaded=False when model exists but is_loaded() returns False."""
    from web.api import ai_research as ai_module

    ml_stub = SimpleNamespace(is_loaded=lambda: False)
    stub_aggregator = SimpleNamespace(
        aggregate=AsyncMock(return_value=SimpleNamespace(to_dict=lambda: {})),
        _ml_model=ml_stub,
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr("core.ai.signal_aggregator.signal_aggregator", stub_aggregator)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["ml_model_loaded"] is False


# ── Step 3 回归：exchange/symbol 解析 ────────────────────────────────────────

def test_candidate_exchange_from_direct_attribute():
    from web.api import ai_research as ai_module

    cand = SimpleNamespace(exchange="bybit", metadata={})
    assert ai_module._candidate_exchange(cand) == "bybit"


def test_candidate_exchange_from_metadata():
    from web.api import ai_research as ai_module

    cand = SimpleNamespace(metadata={"exchange": "okx"})
    assert ai_module._candidate_exchange(cand) == "okx"


def test_candidate_exchange_default_fallback():
    from web.api import ai_research as ai_module

    cand = SimpleNamespace()
    assert ai_module._candidate_exchange(cand) == "binance"


def test_live_signals_uses_candidate_exchange(monkeypatch):
    """Market data loads with candidate's exchange, not hardcoded 'binance'."""
    from web.api import ai_research as ai_module

    candidate = SimpleNamespace(
        candidate_id="cand-okx",
        strategy="RSIStrategy",
        symbol="ETH/USDT",
        status="paper_running",
        exchange="okx",
        metadata={"exchange": "okx"},
    )

    class _Signal:
        def to_dict(self):
            return {"direction": "SHORT", "components": {}}

    captured_exchange = []

    async def _fake_load(*args, **kwargs):
        exchange = kwargs.get("exchange")
        if exchange is None and args:
            exchange = args[0]
        captured_exchange.append(exchange)
        return pd.DataFrame({"close": [1.0, 1.1]})

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [candidate])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr("core.data.data_storage.load_klines_from_parquet", _fake_load)
    monkeypatch.setattr(
        "core.ai.signal_aggregator.signal_aggregator",
        SimpleNamespace(aggregate=AsyncMock(return_value=_Signal())),
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    asyncio.run(ai_module.get_live_signals(request))
    assert captured_exchange == ["okx"]


# ── Step 5 回归：注册状态机语义 ───────────────────────────────────────────────

def test_live_signals_uses_candidate_timeframe(monkeypatch):
    from web.api import ai_research as ai_module

    candidate = SimpleNamespace(
        candidate_id="cand-4h",
        strategy="MomentumStrategy",
        symbol="BTC/USDT",
        timeframe="4h",
        status="paper_running",
        metadata={},
    )
    captured_timeframes = []

    class _Signal:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.61, "components": {}}

    async def _fake_load_signal_market_data(*, exchange, symbol, timeframe="1h", limit=120):
        captured_timeframes.append(timeframe)
        return pd.DataFrame({"close": [1.0, 1.1, 1.2]}), {
            "market_data_exchange": exchange,
            "market_data_symbol": symbol,
            "market_data_timeframe": timeframe,
            "market_data_rows": 3,
            "market_data_last_bar_at": None,
            "market_data_age_sec": None,
            "market_data_stale": False,
            "market_data_source": "unit_test",
            "market_data_load_error": None,
        }

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [candidate])
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr(ai_module, "_load_signal_market_data", _fake_load_signal_market_data)
    monkeypatch.setattr(
        "core.ai.signal_aggregator.signal_aggregator",
        SimpleNamespace(aggregate=AsyncMock(return_value=_Signal())),
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["candidate_items"][0]["timeframe"] == "4h"
    assert captured_timeframes == ["4h"]


def test_live_signals_dedupes_duplicate_candidates(monkeypatch):
    from web.api import ai_research as ai_module

    older = datetime(2025, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2025, 1, 2, tzinfo=timezone.utc)
    candidates = [
        SimpleNamespace(
            candidate_id="cand-paper-01",
            strategy="Breakout",
            symbol="ETH/USDT",
            timeframe="30m",
            params={"fast": 20, "slow": 50},
            status="paper_running",
            created_at=older,
            metadata={},
        ),
        SimpleNamespace(
            candidate_id="cand-live-02",
            strategy="Breakout",
            symbol="ETH/USDT",
            timeframe="30m",
            params={"slow": 50, "fast": 20},
            status="live_running",
            created_at=newer,
            metadata={},
        ),
    ]

    class _Signal:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.58, "components": {}}

    load_calls = []

    async def _fake_load_signal_market_data(*, exchange, symbol, timeframe="1h", limit=120):
        load_calls.append((exchange, symbol, timeframe))
        return pd.DataFrame({"close": [1.0, 0.9, 0.8]}), {
            "market_data_exchange": exchange,
            "market_data_symbol": symbol,
            "market_data_timeframe": timeframe,
            "market_data_rows": 3,
            "market_data_last_bar_at": None,
            "market_data_age_sec": None,
            "market_data_stale": False,
            "market_data_source": "unit_test",
            "market_data_load_error": None,
        }

    aggregate = AsyncMock(return_value=_Signal())
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: candidates)
    _patch_live_signals_watchlist(monkeypatch, ai_module)
    monkeypatch.setattr(ai_module, "_load_signal_market_data", _fake_load_signal_market_data)
    monkeypatch.setattr(
        "core.ai.signal_aggregator.signal_aggregator",
        SimpleNamespace(aggregate=aggregate),
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_live_signals(request))
    assert result["candidate_count"] == 1
    assert result["count"] == 1
    item = result["candidate_items"][0]
    assert item["candidate_id"] == "cand-live-02"
    assert item["status"] == "live_running"
    assert item["duplicate_count"] == 2
    assert set(item["candidate_ids"]) == {"cand-paper-01", "cand-live-02"}
    assert load_calls == [("binance", "ETH/USDT", "30m")]
    assert aggregate.await_count == 1


def test_live_signals_includes_watchlist_section(monkeypatch):
    from web.api import ai_research as ai_module

    class _Signal:
        def to_dict(self):
            return {"direction": "FLAT", "confidence": 0.31, "components": {}}

    captured_calls = []

    async def _fake_load_signal_market_data(*, exchange, symbol, timeframe="1h", limit=120):
        captured_calls.append((exchange, symbol, timeframe))
        return pd.DataFrame({"close": [1.0, 1.0, 1.0]}), {
            "market_data_exchange": exchange,
            "market_data_symbol": symbol,
            "market_data_timeframe": timeframe,
            "market_data_rows": 3,
            "market_data_last_bar_at": None,
            "market_data_age_sec": None,
            "market_data_stale": False,
            "market_data_source": "unit_test",
            "market_data_load_error": None,
        }

    selection = {
        "selected_symbol": "ETH/USDT",
        "top_candidates": [
            {
                "rank": 1,
                "symbol": "ETH/USDT",
                "research": {
                    "candidate_id": "cand-eth-01",
                    "strategy": "TrendHunter",
                    "status": "live_candidate",
                },
            },
            {
                "rank": 2,
                "symbol": "BTC/USDT",
                "research": {
                    "candidate_id": "cand-btc-02",
                    "strategy": "TrendHunter",
                    "status": "paper_running",
                },
            },
        ],
        "scan_config": {
            "exchange": "okx",
            "timeframe": "15m",
            "universe_symbols": ["ETH/USDT", "BTC/USDT", "SOL/USDT"],
        },
    }
    runtime_cfg = {
        "strategy_name": "AI_AutonomousAgent",
        "exchange": "binance",
        "timeframe": "5m",
        "universe_symbols": ["SOL/USDT"],
    }

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    _patch_live_signals_watchlist(monkeypatch, ai_module, runtime_cfg=runtime_cfg, selection=selection)
    monkeypatch.setattr(ai_module, "_load_signal_market_data", _fake_load_signal_market_data)
    monkeypatch.setattr(
        "core.ai.signal_aggregator.signal_aggregator",
        SimpleNamespace(aggregate=AsyncMock(return_value=_Signal())),
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    result = asyncio.run(ai_module.get_autonomous_agent_live_signals(request))
    assert result["candidate_count"] == 0
    assert result["watchlist_count"] == 3
    assert [section["id"] for section in result["sections"]] == ["watchlist"]
    assert [item["symbol"] for item in result["watchlist_items"]] == ["ETH/USDT", "BTC/USDT", "SOL/USDT"]
    assert result["watchlist_items"][0]["selected"] is True
    assert result["watchlist_items"][0]["status"] == "selected"
    assert all(item["timeframe"] == "15m" for item in result["watchlist_items"])
    assert captured_calls == [
        ("okx", "ETH/USDT", "15m"),
        ("okx", "BTC/USDT", "15m"),
        ("okx", "SOL/USDT", "15m"),
    ]


def test_promote_candidate_paper_in_live_mode_raises_http_400(monkeypatch):
    """Human approve / quick-register must return 400 (not 500) when system is in live mode."""
    from fastapi import HTTPException
    from web.api import ai_research as ai_module

    async def _mock_promote(*args, **kwargs):
        raise RuntimeError("系统当前运行在 'live' 模式，无法自动注册纸盘策略。")

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr("core.deployment.promotion_engine.promote_candidate", _mock_promote)

    cand = SimpleNamespace(
        candidate_id="cand-step5",
        proposal_id="prop-step5",
        metadata={"promotion_pending_human_gate": True},
        promotion=SimpleNamespace(
            decision="paper",
            constraints={},
            model_dump=lambda mode=None: {},
        ),
        validation_summary=None,
    )
    proposal = SimpleNamespace(
        proposal_id="prop-step5",
        metadata={},
        model_dump=lambda mode=None: {},
    )

    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: cand)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, pid: proposal)

    payload = ai_module.AIHumanApprovalRequest(target="paper", notes="test")

    try:
        asyncio.run(ai_module.human_approve_candidate(request, "cand-step5", payload))
        assert False, "should have raised HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "模式" in exc.detail or "paper" in exc.detail.lower()


def test_register_ai_candidate_returns_http_400_when_live_mode_blocks_paper(monkeypatch):
    from fastapi import HTTPException
    from web.api import ai_research as ai_module

    request = _build_ai_research_request()
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")

    promote_mock = AsyncMock(return_value={})
    monkeypatch.setattr(ai_module, "promote_existing_candidate", promote_mock)

    payload = ai_module.AICandidateRegisterRequest(mode="paper", name=None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(ai_module.register_ai_candidate(request, "cand-live-register", payload))

    assert exc_info.value.status_code == 400
    assert "live" in str(exc_info.value.detail).lower()
    assert "paper" in str(exc_info.value.detail).lower()
    assert "live_candidate" in str(exc_info.value.detail)
    assert promote_mock.await_count == 0


def test_register_ai_candidate_maps_runtime_error_to_http_400(monkeypatch):
    from fastapi import HTTPException
    from web.api import ai_research as ai_module

    request = _build_ai_research_request()
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "paper")

    async def _mock_promote(*args, **kwargs):
        raise RuntimeError("strategy start failed during paper promotion")

    monkeypatch.setattr(ai_module, "promote_existing_candidate", _mock_promote)

    payload = ai_module.AICandidateRegisterRequest(mode="paper", name=None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(ai_module.register_ai_candidate(request, "cand-runtime-error", payload))

    assert exc_info.value.status_code == 400
    assert "strategy start failed" in str(exc_info.value.detail)


def test_step6_autonomous_agent_ui_present():
    """Step 6: AI自治代理控制面板必须出现在 HTML 中."""
    repo_root = Path(__file__).resolve().parents[1]
    html = (repo_root / "web" / "templates" / "index.html").read_text(encoding="utf-8")
    assert "ai-agent-card" in html
    assert "agentStart" in html
    assert "agentStop" in html
    assert "agentRunOnce" in html
    assert "ai-agent-journal" in html


def test_step6_autonomous_agent_js_functions_present():
    """Step 6: JS 里必须有 loadAgentStatus / agentStart / agentStop / agentRunOnce."""
    repo_root = Path(__file__).resolve().parents[1]
    js = (repo_root / "web" / "static" / "js" / "ai_research.js").read_text(encoding="utf-8")
    assert "function loadAgentStatus" in js
    assert "function agentStart" in js
    assert "function agentStop" in js
    assert "function agentRunOnce" in js
    assert "window.agentStart" in js
    assert "/ai/autonomous-agent/status" in js
    assert "/ai/autonomous-agent/journal" in js


def test_promotion_error_message_is_chinese_and_contains_mode(monkeypatch):
    """The RuntimeError message from promotion_engine contains the current mode."""
    from core.deployment.promotion_engine import promote_candidate
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import PromotionDecision, StrategyCandidate

    now = datetime.now(timezone.utc)
    proposal = ResearchProposal(
        proposal_id="prop-mode-msg",
        created_at=now,
        updated_at=now,
        thesis="mode msg test",
        status="validated",
    )
    candidate = StrategyCandidate(
        candidate_id="cand-mode-msg",
        proposal_id="prop-mode-msg",
        experiment_id="exp-mode-msg",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={},
        metadata={"exchange": "binance"},
    )
    promotion = PromotionDecision(
        candidate_id="cand-mode-msg",
        decision="paper",
        reason="test",
        constraints={},
        created_at=now,
    )

    app = MagicMock()
    app.state.ai_lifecycle_registry = MagicMock()
    app.state.ai_lifecycle_registry.append = MagicMock()
    app.state.ai_experiment_registry = MagicMock()
    app.state.ai_experiment_registry.get = MagicMock(return_value=SimpleNamespace(days=30))

    monkeypatch.setattr("core.deployment.promotion_engine._resolve_strategy_class", lambda _: object)
    monkeypatch.setattr("core.deployment.promotion_engine.get_strategy_defaults", lambda _: {})
    monkeypatch.setattr("core.deployment.promotion_engine.execution_engine.get_trading_mode", lambda: "live")

    try:
        asyncio.run(promote_candidate(app, proposal=proposal, candidate=candidate, promotion=promotion, actor="test"))
        assert False, "should have raised RuntimeError"
    except RuntimeError as exc:
        msg = str(exc)
        assert "live" in msg
        assert "paper" in msg  # must mention target mode


# ── Phase C: CUSUM auto-draft ─────────────────────────────────────────────────

def test_auto_draft_replacement_creates_proposal(monkeypatch):
    """_auto_draft_replacement must call create_manual_proposal with correct args."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    from core.monitoring.cusum_watcher import _auto_draft_replacement

    created_proposals = []

    def _fake_create(app, **kwargs):
        created_proposals.append({
            "thesis": kwargs.get("thesis", ""),
            "symbols": kwargs.get("symbols", []),
            "actor": kwargs.get("actor", ""),
            "source_candidate": (kwargs.get("metadata") or {}).get("parent_candidate_id"),
        })
        return SimpleNamespace(proposal_id="prop-auto-draft")

    candidate = SimpleNamespace(
        candidate_id="cand-cusum-c",
        strategy="BollingerStrategy",
        symbols=["ETH/USDT"],
        timeframes=["15m", "1h"],
        status="paper_running",
    )
    decay_result = {"decay_pct": 14.2, "cusum_low": -3.1}
    app = MagicMock()

    with patch("core.research.orchestrator.create_manual_proposal", _fake_create):
        _auto_draft_replacement(app, candidate, decay_result)

    assert len(created_proposals) == 1
    p = created_proposals[0]
    assert "ETH/USDT" in p["symbols"]
    assert p["actor"] == "cusum_auto"
    assert "BollingerStrategy" in p["thesis"]
    assert p["source_candidate"] == "cand-cusum-c"


def test_auto_draft_replacement_non_fatal_on_error(monkeypatch):
    """_auto_draft_replacement must not raise even if create_manual_proposal fails."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    from core.monitoring.cusum_watcher import _auto_draft_replacement

    candidate = SimpleNamespace(
        candidate_id="cand-error",
        strategy="Test",
        symbols=["BTC/USDT"],
        timeframes=["1h"],
        status="paper_running",
    )
    app = MagicMock()

    with patch("core.research.orchestrator.create_manual_proposal", side_effect=RuntimeError("boom")):
        _auto_draft_replacement(app, candidate, {"decay_pct": 5.0})  # must not raise


# ── Phase D: Order preview endpoint ──────────────────────────────────────────

def test_order_preview_returns_direction_and_size(monkeypatch):
    """generate_order_preview must return direction, size_usdt, stop/take levels."""
    from web.api import ai_research as ai_module

    class _Sig:
        direction = "LONG"
        confidence = 0.72
        requires_approval = False
        blocked_by_risk = False
        risk_reason = None
        components = {"llm": {"direction": "LONG", "confidence": 0.8, "weight": 0.4}}
        timestamp = datetime.now(timezone.utc)
        def to_dict(self): return {"direction": self.direction, "confidence": self.confidence}

    cand = SimpleNamespace(
        candidate_id="cand-preview",
        status="validated",
        symbol="BTC/USDT",
        metadata={"allocation_pct": 0.05},
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: cand)

    # generate_order_preview uses app.state._signal_aggregator (lazy singleton),
    # so pre-set it directly on the mock state.
    fake_agg = SimpleNamespace(aggregate=AsyncMock(return_value=_Sig()))
    app_state = SimpleNamespace(_signal_aggregator=fake_agg)
    request = SimpleNamespace(app=SimpleNamespace(state=app_state))
    result = asyncio.run(ai_module.generate_order_preview(request, "cand-preview"))

    assert result["direction"] == "LONG"
    assert result["confidence"] == pytest.approx(0.72, abs=0.01)
    assert result["symbol"] == "BTC/USDT"
    assert result["size_usdt"] > 0
    assert result["stop_loss_pct"] > 0
    assert result["take_profit_pct"] > 0
    assert "components" in result


def test_order_preview_rejects_wrong_status(monkeypatch):
    """generate_order_preview must return 400 for unsupported candidate status."""
    from fastapi import HTTPException
    from web.api import ai_research as ai_module

    cand = SimpleNamespace(
        candidate_id="cand-draft",
        status="draft",
        symbol="BTC/USDT",
        metadata={},
    )
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: cand)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    try:
        asyncio.run(ai_module.generate_order_preview(request, "cand-draft"))
        assert False, "should have raised HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
