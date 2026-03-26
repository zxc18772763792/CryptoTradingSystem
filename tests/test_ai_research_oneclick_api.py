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
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))


def _build_run_result(proposal_id: str, candidate_id: str, decision: str = "paper"):
    proposal = _StubModel(proposal_id=proposal_id, metadata={}, status="validated")
    candidate = _StubModel(candidate_id=candidate_id, status="validated")
    promotion = _StubModel(decision=decision)
    experiment = _StubModel(experiment_id="exp-1")
    run = _StubModel(run_id="run-1")
    return {
        "proposal": proposal,
        "candidate": candidate,
        "promotion": promotion,
        "experiment": experiment,
        "run": run,
        "research_result": {"ok": True},
    }


def test_oneclick_non_governance_calls_register(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-1", metadata={}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(ai_module, "run_proposal", AsyncMock(return_value=_build_run_result("p-1", "c-1", "paper")))

    register_mock = AsyncMock(return_value={"runtime_status": "paper_running", "registered_strategy_name": "S1"})
    monkeypatch.setattr(ai_module, "register_ai_candidate", register_mock)
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())

    payload = ai_module.AIOneClickResearchDeployRequest(goal="oneclick non governance flow")
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))
    assert result["target"] == "paper"
    assert result["deploy"]["action"] == "register"
    assert result["runtime_status"] == "paper_running"
    assert register_mock.await_count == 1


def test_oneclick_governance_paper_calls_quick_register(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-2", metadata={}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(ai_module, "run_proposal", AsyncMock(return_value=_build_run_result("p-2", "c-2", "paper")))

    quick_mock = AsyncMock(return_value={"runtime_status": "paper_running"})
    monkeypatch.setattr(ai_module, "quick_register_candidate", quick_mock)
    monkeypatch.setattr(ai_module, "human_approve_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())

    payload = ai_module.AIOneClickResearchDeployRequest(goal="oneclick governance paper flow", target="paper")
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))
    assert result["target"] == "paper"
    assert result["deploy"]["action"] == "quick_register"
    assert result["runtime_status"] == "paper_running"
    assert quick_mock.await_count == 1


def test_oneclick_governance_live_candidate_calls_human_approve(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-3", metadata={}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", True, raising=False)
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(
        ai_module,
        "run_proposal",
        AsyncMock(return_value=_build_run_result("p-3", "c-3", "live_candidate")),
    )

    approve_mock = AsyncMock(return_value={"runtime_status": "live_candidate"})
    monkeypatch.setattr(ai_module, "human_approve_candidate", approve_mock)
    monkeypatch.setattr(ai_module, "quick_register_candidate", AsyncMock())
    monkeypatch.setattr(ai_module, "register_ai_candidate", AsyncMock())

    payload = ai_module.AIOneClickResearchDeployRequest(
        goal="oneclick governance live candidate flow",
        target="live_candidate",
    )
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))
    assert result["target"] == "live_candidate"
    assert result["deploy"]["action"] == "human_approve"
    assert result["runtime_status"] == "live_candidate"
    assert approve_mock.await_count == 1


def test_oneclick_skip_deploy(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-4", metadata={}, status="draft")

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "_serialize_proposal", lambda app, p: {"proposal_id": p.proposal_id})
    monkeypatch.setattr(ai_module.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(
        ai_module,
        "generate_planned_proposal",
        lambda *args, **kwargs: {"proposal": proposal, "planner_notes": [], "filtered_templates": []},
    )
    monkeypatch.setattr(ai_module, "run_proposal", AsyncMock(return_value=_build_run_result("p-4", "c-4", "paper")))

    register_mock = AsyncMock(return_value={"runtime_status": "paper_running"})
    monkeypatch.setattr(ai_module, "register_ai_candidate", register_mock)

    payload = ai_module.AIOneClickResearchDeployRequest(
        goal="oneclick skip deploy flow",
        skip_deploy=True,
    )
    result = asyncio.run(ai_module.oneclick_ai_research_deploy(request, payload))
    assert result["deploy"]["performed"] is False
    assert result["deploy"]["action"] is None
    assert result["runtime_status"] is None
    assert register_mock.await_count == 0


def test_oneclick_research_deploy_returns_http_400_on_value_error(monkeypatch):
    from web.api import ai_research as ai_module

    request = _build_request()
    proposal = _StubModel(proposal_id="p-5", metadata={}, status="draft")

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
