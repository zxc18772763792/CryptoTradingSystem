from __future__ import annotations

import asyncio
from fastapi import HTTPException
from types import SimpleNamespace
from unittest.mock import AsyncMock


class _StubModel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def model_dump(self, mode: str = "json"):
        return dict(self.__dict__)


def _build_request():
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                research_jobs={},
            )
        )
    )


def _build_background_run_result(proposal_id: str, job_id: str = "job-1", status: str = "pending"):
    proposal = _StubModel(proposal_id=proposal_id, metadata={"last_research_job_id": job_id}, status="research_queued")
    experiment = _StubModel(experiment_id="exp-1")
    run = _StubModel(run_id="run-1")
    return {
        "job": {
            "job_id": job_id,
            "proposal_id": proposal_id,
            "status": status,
            "created_at": "2026-04-08T00:00:00+00:00",
            "progress": {"phase": "queued", "message": "任务已入队"},
        },
        "proposal": proposal,
        "experiment": experiment,
        "run": run,
    }


def test_oneclick_research_deploy_queues_background_job(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-1", metadata={}, status="draft")
    run_mock = AsyncMock(return_value=_build_background_run_result("p-1", "job-101"))

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": ["note"], "filtered_templates": ["RSIStrategy"]},
    )
    monkeypatch.setattr(ai_module, "run_proposal", run_mock)
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickResearchDeployRequest(goal="oneclick queued background flow")
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))

    assert result["proposal_id"] == "p-1"
    assert result["job_id"] == "job-101"
    assert result["status"] == "pending"
    assert result["outcome"] == "queued"
    assert result["generated"]["planner_notes"] == ["note"]
    assert run_mock.await_count == 1
    assert run_mock.await_args.kwargs["background"] is True


def test_oneclick_research_deploy_returns_existing_job_on_conflict(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    request.app.state.research_jobs["job-existing"] = {
        "job_id": "job-existing",
        "proposal_id": "p-2",
        "status": "running",
        "progress": {"phase": "research_running", "message": "正在研究"},
    }
    proposal = _StubModel(proposal_id="p-2", metadata={"last_research_job_id": "job-existing"}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(
        ai_module,
        "run_proposal",
        AsyncMock(side_effect=HTTPException(status_code=409, detail="already running")),
    )

    payload = ai_module.AIOneClickResearchDeployRequest(goal="oneclick conflict flow")
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))

    assert result["status"] == "already_running"
    assert result["job_id"] == "job-existing"
    assert result["job"]["status"] == "running"
    assert result["outcome"] == "queued"


def test_oneclick_research_deploy_returns_http_400_on_value_error(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-3", metadata={}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(
        ai_module,
        "run_proposal",
        AsyncMock(side_effect=ValueError("not enough data for research")),
    )

    payload = ai_module.AIOneClickResearchDeployRequest(goal="oneclick value error flow")

    try:
        asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))
        assert False, "should have raised HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "not enough data" in exc.detail


def test_oneclick_deploy_candidate_non_governance_calls_register(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    candidate = _StubModel(
        candidate_id="c-1",
        proposal_id="p-1",
        metadata={"recommended_runtime_target": "paper"},
        promotion=None,
        status="validated",
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)

    register_mock = AsyncMock(return_value={"runtime_status": "paper_running", "registered_strategy_name": "S1"})
    monkeypatch.setattr(ai_module, "register_ai_candidate", register_mock)
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickDeployRequest(candidate_id="c-1", target="paper")
    result = asyncio.run(ai_module.oneclick_deploy_candidate(request, payload))

    assert result["target"] == "paper"
    assert result["deploy"]["action"] == "register"
    assert result["runtime_status"] == "paper_running"
    assert register_mock.await_count == 1


def test_oneclick_deploy_candidate_governance_paper_calls_quick_register(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    candidate = _StubModel(
        candidate_id="c-2",
        proposal_id="p-2",
        metadata={},
        promotion=_StubModel(decision="paper"),
        status="validated",
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)

    quick_mock = AsyncMock(return_value={"runtime_status": "paper_running"})
    monkeypatch.setattr(ai_module, "quick_register_candidate", quick_mock)
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickDeployRequest(candidate_id="c-2", target="paper")
    result = asyncio.run(ai_module.oneclick_deploy_candidate(request, payload))

    assert result["target"] == "paper"
    assert result["deploy"]["action"] == "quick_register"
    assert result["runtime_status"] == "paper_running"
    assert quick_mock.await_count == 1


def test_oneclick_deploy_candidate_governance_live_candidate_calls_human_approve(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    candidate = _StubModel(
        candidate_id="c-3",
        proposal_id="p-3",
        metadata={},
        promotion=_StubModel(decision="live_candidate"),
        status="validated",
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)

    approve_mock = AsyncMock(return_value={"runtime_status": "live_candidate"})
    monkeypatch.setattr(ai_module, "human_approve_candidate", approve_mock)
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())

    payload = ai_module.AIOneClickDeployRequest(candidate_id="c-3", target="live_candidate")
    result = asyncio.run(ai_module.oneclick_deploy_candidate(request, payload))

    assert result["target"] == "live_candidate"
    assert result["deploy"]["action"] == "human_approve"
    assert result["runtime_status"] == "live_candidate"
    assert approve_mock.await_count == 1


def test_oneclick_deploy_candidate_skip_deploy(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    candidate = _StubModel(
        candidate_id="c-4",
        proposal_id="p-4",
        metadata={},
        promotion=_StubModel(decision="paper"),
        status="validated",
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickDeployRequest(
        candidate_id="c-4",
        target="paper",
        skip_deploy=True,
    )
    result = asyncio.run(ai_module.oneclick_deploy_candidate(request, payload))

    assert result["deploy"]["performed"] is False
    assert result["deploy"]["action"] is None
    assert result["runtime_status"] is None


def test_oneclick_deploy_candidate_auto_paper_in_live_mode_returns_nonfatal_blocker(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    candidate = _StubModel(
        candidate_id="c-4-live",
        proposal_id="p-4-live",
        metadata={"recommended_runtime_target": "paper"},
        promotion=_StubModel(decision="paper"),
        status="validated",
    )

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(ai_module, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(ai_module.execution_engine, "get_trading_mode", lambda: "live")

    register_mock = AsyncMock()
    monkeypatch.setattr(ai_module, "register_ai_candidate", register_mock)
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickDeployRequest(candidate_id="c-4-live", target="auto")
    result = asyncio.run(ai_module.oneclick_deploy_candidate(request, payload))

    assert result["target"] == "paper"
    assert result["outcome"] == "completed_without_compatible_runtime_target"
    assert result["deploy"]["performed"] is False
    assert result["runtime_status"] is None
    assert result["current_trading_mode"] == "live"
    assert result["manual_action_required"] is True
    assert result["manual_target_options"] == ["live_candidate"]
    assert "live" in result["deploy"]["reasons"][0]
    assert register_mock.await_count == 0


def test_job_status_returns_nested_job_progress_and_result(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(
        proposal_id="p-5",
        metadata={"last_research_job_id": "job-5"},
        status="validated",
        validation_summary=None,
    )
    candidate = _StubModel(candidate_id="c-5", status="validated")
    request.app.state.research_jobs["job-5"] = {
        "job_id": "job-5",
        "proposal_id": "p-5",
        "status": "completed",
        "started_at": "2026-04-08T00:00:00+00:00",
        "finished_at": "2026-04-08T00:10:00+00:00",
        "progress": {"phase": "completed", "message": "研究完成"},
        "result": {
            "proposal_id": "p-5",
            "experiment_id": "exp-5",
            "run_id": "run-5",
            "proposal": {
                "proposal_id": "p-5",
                "status": "validated",
                "latest_candidate_id": "c-5",
            },
            "candidate": {
                "candidate_id": "c-5",
                "status": "validated",
                "proposal_id": "p-5",
            },
            "candidate_id": "c-5",
            "promotion": {"decision": "paper"},
            "proposal_reason": None,
        },
    }

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "get_proposal", lambda app, proposal_id: proposal)

    result = asyncio.run(ai_module.get_ai_proposal_job_status(request, "p-5"))

    assert result["job_status"] == "completed"
    assert result["job"]["progress"]["phase"] == "completed"
    assert result["job"]["result"]["candidate"]["candidate_id"] == "c-5"


def test_run_proposal_endpoint_returns_http_400_on_value_error(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(
        ai_module,
        "run_proposal",
        AsyncMock(side_effect=ValueError("not enough data for research")),
    )

    payload = ai_module.AIProposalRunRequest(background=False)

    try:
        asyncio.run(ai_module.run_ai_proposal_endpoint(request, "proposal-1", payload))
        assert False, "should have raised HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "not enough data" in exc.detail
