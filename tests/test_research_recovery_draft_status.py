import json
from datetime import datetime, timezone
from types import SimpleNamespace

from core.ai.proposal_schemas import ResearchProposal
from core.research.experiment_registry import ExperimentRunRegistry, LifecycleRegistry, ProposalRegistry
from core.research.experiment_schemas import ExperimentRun
from core.research import orchestrator as orchestrator_module
from core.research.orchestrator import _prune_finished_research_job_tasks, _recover_stale_jobs_on_startup


def test_stale_research_proposal_recovers_to_draft(tmp_path, monkeypatch):
    proposal_registry = ProposalRegistry(tmp_path / "proposals.json")
    run_registry = ExperimentRunRegistry(tmp_path / "runs.json")
    lifecycle_registry = LifecycleRegistry(tmp_path / "lifecycle.json")
    now = datetime.now(timezone.utc)
    proposal_registry.save(
        ResearchProposal(
            proposal_id="proposal-stale",
            created_at=now,
            updated_at=now,
            status="research_running",
            source="ai",
            thesis="stale research",
            metadata={},
        )
    )
    run_registry.save(
        ExperimentRun(
            run_id="run-stale",
            experiment_id="experiment-stale",
            status="running",
            started_at=now,
        )
    )
    app = SimpleNamespace(
        state=SimpleNamespace(
            ai_proposal_registry=proposal_registry,
            ai_experiment_run_registry=run_registry,
            ai_lifecycle_registry=lifecycle_registry,
            research_jobs={"job-stale": {"status": "running"}},
        )
    )
    monkeypatch.setattr("core.research.orchestrator._persist_research_jobs", lambda _app: None)

    _recover_stale_jobs_on_startup(app)

    recovered = proposal_registry.get("proposal-stale")
    assert recovered is not None
    assert recovered.status == "draft"
    assert recovered.metadata["last_research_error"] == "service restart; research job did not complete"
    assert recovered.metadata["research_recovery_reason"] == "service restart; research job did not complete"
    assert recovered.metadata["recovered_from_status"] == "research_running"

    lifecycle = lifecycle_registry.list_for_object("proposal", "proposal-stale", limit=None)
    assert lifecycle[0].from_state == "research_running"
    assert lifecycle[0].to_state == "draft"
    assert lifecycle[0].reason == "service restart; research job did not complete"

    recovered_run = run_registry.get("run-stale")
    assert recovered_run is not None
    assert recovered_run.status == "failed"
    assert recovered_run.error == "service restart; research job did not complete"
    assert app.state.research_jobs["job-stale"]["status"] == "failed"
    assert app.state.research_jobs["job-stale"]["recovery_reason"] == "service restart; research job did not complete"


def test_prune_finished_research_job_tasks_clears_done_entries():
    class DoneTask:
        def done(self):
            return True

    class ActiveTask:
        def done(self):
            return False

    app = SimpleNamespace(
        state=SimpleNamespace(
            research_job_tasks={"done-job": DoneTask(), "active-job": ActiveTask()},
        )
    )

    removed = _prune_finished_research_job_tasks(app)

    assert removed == 1
    assert "done-job" not in app.state.research_job_tasks
    assert "active-job" in app.state.research_job_tasks


def test_persist_research_jobs_uses_atomic_tmp_replace(tmp_path, monkeypatch):
    target = tmp_path / "research_jobs.json"
    app = SimpleNamespace(
        state=SimpleNamespace(
            ai_research_jobs_path=target,
            research_jobs={"job-1": {"status": "running"}},
        )
    )
    calls = []
    original_replace = orchestrator_module.os.replace

    def _capture_replace(src: str, dst: str) -> None:
        calls.append((src, dst))
        original_replace(src, dst)

    monkeypatch.setattr(orchestrator_module.os, "replace", _capture_replace)

    orchestrator_module._persist_research_jobs(app)

    assert calls == [(str(target.with_suffix(target.suffix + ".tmp")), str(target))]
    assert json.loads(target.read_text(encoding="utf-8")) == {"job-1": {"status": "running"}}
    assert not target.with_suffix(target.suffix + ".tmp").exists()
