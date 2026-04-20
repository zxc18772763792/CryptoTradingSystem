from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException

from config.settings import settings
from core.ai.proposal_schemas import ResearchProposal
from core.research import orchestrator as orchestrator_module
from core.research.experiment_registry import (
    CandidateRegistry,
    ExperimentRegistry,
    ExperimentRunRegistry,
    LifecycleRegistry,
    ProposalRegistry,
)
from core.research.experiment_schemas import (
    ExperimentRun,
    ExperimentSpec,
    LifecycleRecord,
    StrategyCandidate,
)


def _build_app(tmp_path) -> FastAPI:
    app = FastAPI()
    app.state.ai_candidate_registry = CandidateRegistry(tmp_path / "candidates.json")
    app.state.ai_experiment_registry = ExperimentRegistry(tmp_path / "experiments.json")
    app.state.ai_experiment_run_registry = ExperimentRunRegistry(tmp_path / "experiment_runs.json")
    app.state.ai_proposal_registry = ProposalRegistry(tmp_path / "proposals.json")
    app.state.ai_lifecycle_registry = LifecycleRegistry(tmp_path / "lifecycle.json")
    app.state.research_jobs = {}
    app.state.research_job_tasks = {}
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


def _build_experiment(*, experiment_id: str, proposal_id: str) -> ExperimentSpec:
    return ExperimentSpec(
        experiment_id=experiment_id,
        proposal_id=proposal_id,
        created_at=datetime.now(timezone.utc),
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["5m"],
        strategies=["EMAStrategy"],
    )


def _build_run(*, run_id: str, experiment_id: str) -> ExperimentRun:
    return ExperimentRun(
        run_id=run_id,
        experiment_id=experiment_id,
        started_at=datetime.now(timezone.utc),
        status="running",
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


def test_exit_ai_proposal_stops_runtime_and_retires_candidate(tmp_path, monkeypatch):
    from web.api import ai_research as ai_module
    import core.strategies as strategies_module
    from core.strategies import persistence as persistence_module

    app = _build_app(tmp_path)
    proposal = _build_proposal(proposal_id="proposal-running")
    proposal.status = "paper_running"
    proposal.metadata = {}
    app.state.ai_proposal_registry.save(proposal)

    candidate = _build_candidate(candidate_id="cand-running", proposal_id=proposal.proposal_id, status="paper_running")
    candidate.metadata = {
        "registered_strategy_name": "paper_strategy",
        "promotion_runtime": {
            "mode": "paper",
            "registered_strategy_name": "paper_strategy",
            "started": True,
        },
    }
    app.state.ai_candidate_registry.save(candidate)

    class _FakeStrategyManager:
        def __init__(self):
            self.stopped = []
            self.unregistered = []

        def get_strategy(self, name: str):
            return object() if name == "paper_strategy" else None

        def get_strategy_info(self, name: str):
            return {"name": name} if name == "paper_strategy" else None

        async def stop_strategy(self, name: str):
            self.stopped.append(name)
            return True

        def unregister_strategy(self, name: str):
            self.unregistered.append(name)
            return True

    fake_manager = _FakeStrategyManager()
    delete_snapshot = AsyncMock(return_value=True)

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(strategies_module, "strategy_manager", fake_manager)
    monkeypatch.setattr(persistence_module, "delete_strategy_snapshot", delete_snapshot)

    result = asyncio.run(
        ai_module.exit_ai_proposal(
            SimpleNamespace(app=app),
            proposal.proposal_id,
            ai_module.AIRetireRequest(notes="queue exit"),
        )
    )

    saved_candidate = app.state.ai_candidate_registry.get(candidate.candidate_id)
    saved_proposal = app.state.ai_proposal_registry.get(proposal.proposal_id)

    assert result["status"] == "retired"
    assert result["retired_candidates"] == 1
    assert result["stopped_strategies"] == ["paper_strategy"]
    assert saved_candidate is not None
    assert saved_candidate.status == "retired"
    assert saved_candidate.metadata["promotion_runtime"]["started"] is False
    assert saved_candidate.metadata["promotion_runtime"]["unregistered"] is True
    assert saved_proposal is not None
    assert saved_proposal.status == "retired"
    assert fake_manager.stopped == ["paper_strategy"]
    assert fake_manager.unregistered == ["paper_strategy"]
    delete_snapshot.assert_awaited_once_with("paper_strategy")


def test_exit_ai_candidate_endpoint_retires_orphan_runtime_candidate(tmp_path, monkeypatch):
    from web.api import ai_research as ai_module
    import core.strategies as strategies_module
    from core.strategies import persistence as persistence_module

    app = _build_app(tmp_path)
    candidate = _build_candidate(candidate_id="cand-orphan-running", proposal_id="proposal-missing", status="live_running")
    candidate.metadata = {
        "registered_strategy_name": "live_strategy",
        "promotion_runtime": {
            "mode": "live",
            "registered_strategy_name": "live_strategy",
            "started": True,
        },
    }
    app.state.ai_candidate_registry.save(candidate)

    class _FakeStrategyManager:
        def __init__(self):
            self.stopped = []
            self.unregistered = []

        def get_strategy(self, name: str):
            return object() if name == "live_strategy" else None

        def get_strategy_info(self, name: str):
            return {"name": name} if name == "live_strategy" else None

        async def stop_strategy(self, name: str):
            self.stopped.append(name)
            return True

        def unregister_strategy(self, name: str):
            self.unregistered.append(name)
            return True

    fake_manager = _FakeStrategyManager()
    delete_snapshot = AsyncMock(return_value=True)

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(strategies_module, "strategy_manager", fake_manager)
    monkeypatch.setattr(persistence_module, "delete_strategy_snapshot", delete_snapshot)

    result = asyncio.run(
        ai_module.exit_ai_candidate_endpoint(
            SimpleNamespace(app=app),
            candidate.candidate_id,
            ai_module.AIRetireRequest(notes="queue exit"),
        )
    )

    saved_candidate = app.state.ai_candidate_registry.get(candidate.candidate_id)

    assert result["status"] == "retired"
    assert result["strategy_name"] == "live_strategy"
    assert saved_candidate is not None
    assert saved_candidate.status == "retired"
    assert saved_candidate.metadata["promotion_runtime"]["started"] is False
    assert saved_candidate.metadata["promotion_runtime"]["unregistered"] is True
    assert fake_manager.stopped == ["live_strategy"]
    assert fake_manager.unregistered == ["live_strategy"]
    delete_snapshot.assert_awaited_once_with("live_strategy")


def test_ensure_runtime_state_recovers_missing_proposal_from_candidate(tmp_path, monkeypatch):
    app = FastAPI()
    storage_root = tmp_path / "historical"
    storage_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(settings, "DATA_STORAGE_PATH", storage_root, raising=False)

    base_dir = (storage_root / ".." / "research" / "ai").resolve()
    candidate_registry = CandidateRegistry(base_dir / "candidates.json")
    candidate = _build_candidate(
        candidate_id="cand-recover",
        proposal_id="proposal-missing-recover",
        status="paper_running",
    )
    candidate.metadata = {
        "registered_strategy_name": "EMA_ai_recovered",
        "search_budget": {"max_templates": 3, "max_strategy_drafts": 1, "max_backtest_runs": 12, "exploration_bias": 0.2},
    }
    candidate_registry.save(candidate)

    orchestrator_module.ensure_ai_research_runtime_state(app)

    recovered = app.state.ai_proposal_registry.get(candidate.proposal_id)
    assert recovered is not None
    assert recovered.proposal_id == candidate.proposal_id
    assert recovered.status == "paper_running"
    assert recovered.latest_candidate_id == candidate.candidate_id
    assert recovered.latest_experiment_id == candidate.experiment_id
    assert recovered.target_symbols == ["BTC/USDT"]
    assert recovered.target_timeframes == ["5m"]
    assert recovered.strategy_templates == ["EMAStrategy"]
    assert recovered.metadata["recovered_from_orphan_candidate"] is True
    assert recovered.metadata["recovered_candidate_id"] == candidate.candidate_id
    assert recovered.metadata["registered_strategy_name"] == "EMA_ai_recovered"

    lifecycle = app.state.ai_lifecycle_registry.list_for_object("proposal", candidate.proposal_id, limit=None)
    assert len(lifecycle) == 1
    assert lifecycle[0].reason == "recovered missing proposal from candidate registry"


def test_background_job_cancellation_marks_research_records_terminal(tmp_path, monkeypatch):
    app = _build_app(tmp_path)
    proposal = _build_proposal(proposal_id="proposal-cancelled")
    proposal.status = "research_running"
    proposal.metadata = {"last_research_job_id": "job-cancelled"}
    app.state.ai_proposal_registry.save(proposal)

    experiment = _build_experiment(
        experiment_id="exp-cancelled",
        proposal_id=proposal.proposal_id,
    )
    experiment.status = "running"
    app.state.ai_experiment_registry.save(experiment)

    run = _build_run(
        run_id="run-cancelled",
        experiment_id=experiment.experiment_id,
    )
    app.state.ai_experiment_run_registry.save(run)

    app.state.research_jobs["job-cancelled"] = {
        "job_id": "job-cancelled",
        "proposal_id": proposal.proposal_id,
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "status": "running",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "error": None,
        "result": None,
        "progress": {"phase": "research_running", "message": "running"},
    }

    monkeypatch.setattr(orchestrator_module, "_persist_research_jobs", lambda app: None)
    monkeypatch.setattr(
        orchestrator_module,
        "_finalize_research_run",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )

    config = orchestrator_module.ResearchConfig(
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["5m"],
        strategies=["EMAStrategy"],
        days=7,
        commission_rate=0.0004,
        slippage_bps=2.0,
        initial_capital=10000.0,
        output_dir=tmp_path / "outputs",
    )

    asyncio.run(
        orchestrator_module._run_proposal_background_job(
            app,
            job_id="job-cancelled",
            proposal_id=proposal.proposal_id,
            experiment_id=experiment.experiment_id,
            run_id=run.run_id,
            request_payload={},
            config=config,
            actor="tester",
        )
    )

    saved_proposal = app.state.ai_proposal_registry.get(proposal.proposal_id)
    saved_experiment = app.state.ai_experiment_registry.get(experiment.experiment_id)
    saved_run = app.state.ai_experiment_run_registry.get(run.run_id)
    saved_job = app.state.research_jobs["job-cancelled"]

    assert saved_job["status"] == "cancelled"
    assert saved_proposal is not None
    assert saved_proposal.status == "rejected"
    assert saved_proposal.metadata["last_research_error"] == "cancelled"
    assert saved_experiment is not None
    assert saved_experiment.status == "failed"
    assert saved_run is not None
    assert saved_run.status == "failed"
    assert saved_run.error == "cancelled"
