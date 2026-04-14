from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI, HTTPException

from core.ai.proposal_schemas import ResearchProposal
from core.research import orchestrator as orchestrator_module
from core.research.experiment_registry import CandidateRegistry, LifecycleRegistry, ProposalRegistry
from core.research.experiment_schemas import LifecycleRecord, StrategyCandidate


def _build_app(tmp_path) -> FastAPI:
    app = FastAPI()
    app.state.ai_candidate_registry = CandidateRegistry(tmp_path / "candidates.json")
    app.state.ai_proposal_registry = ProposalRegistry(tmp_path / "proposals.json")
    app.state.ai_lifecycle_registry = LifecycleRegistry(tmp_path / "lifecycle.json")
    return app


def _build_candidate(*, candidate_id: str, proposal_id: str, status: str = "new") -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=candidate_id,
        proposal_id=proposal_id,
        experiment_id="exp-1",
        created_at=datetime.now(timezone.utc),
        strategy="EMAStrategy",
        timeframe="5m",
        symbol="BTC/USDT",
        status=status,
        metadata={},
    )


def _build_proposal(*, proposal_id: str) -> ResearchProposal:
    now = datetime.now(timezone.utc)
    return ResearchProposal(
        proposal_id=proposal_id,
        created_at=now,
        updated_at=now,
        thesis="cleanup test proposal",
        target_symbols=["BTC/USDT"],
        target_timeframes=["5m"],
        strategy_templates=["EMAStrategy"],
    )


def test_delete_orphan_candidate_removes_candidate_and_lifecycle(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    candidate = _build_candidate(candidate_id="cand-orphan", proposal_id="proposal-missing")
    app.state.ai_candidate_registry.save(candidate)
    app.state.ai_lifecycle_registry.append(
        LifecycleRecord(
            object_type="candidate",
            object_id="cand-orphan",
            to_state="new",
            actor="test",
            ts=datetime.now(timezone.utc),
            reason="seed",
        )
    )

    monkeypatch.setattr(orchestrator_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(orchestrator_module, "_refresh_runtime_eligibility_snapshot_safe", lambda **kwargs: None)

    result = orchestrator_module.delete_orphan_candidate(app, candidate_id="cand-orphan", actor="tester")

    assert result["deleted"] is True
    assert result["deleted_counts"]["candidate"] == 1
    assert result["deleted_counts"]["lifecycle_records"] == 1
    assert app.state.ai_candidate_registry.get("cand-orphan") is None
    assert app.state.ai_lifecycle_registry.list_for_object("candidate", "cand-orphan", limit=None) == []


def test_delete_orphan_candidate_rejects_linked_candidate(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    proposal = _build_proposal(proposal_id="proposal-1")
    candidate = _build_candidate(candidate_id="cand-linked", proposal_id=proposal.proposal_id)
    app.state.ai_proposal_registry.save(proposal)
    app.state.ai_candidate_registry.save(candidate)

    monkeypatch.setattr(orchestrator_module, "ensure_ai_research_runtime_state", lambda app: None)

    with pytest.raises(HTTPException) as exc_info:
        orchestrator_module.delete_orphan_candidate(app, candidate_id="cand-linked", actor="tester")

    assert exc_info.value.status_code == 409
    assert "existing proposal" in str(exc_info.value.detail)
    assert app.state.ai_candidate_registry.get("cand-linked") is not None


def test_delete_orphan_candidate_rejects_running_candidate(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    candidate = _build_candidate(candidate_id="cand-running", proposal_id="proposal-missing", status="paper_running")
    app.state.ai_candidate_registry.save(candidate)

    monkeypatch.setattr(orchestrator_module, "ensure_ai_research_runtime_state", lambda app: None)

    with pytest.raises(HTTPException) as exc_info:
        orchestrator_module.delete_orphan_candidate(app, candidate_id="cand-running", actor="tester")

    assert exc_info.value.status_code == 409
    assert "cannot delete" in str(exc_info.value.detail)
    assert app.state.ai_candidate_registry.get("cand-running") is not None
