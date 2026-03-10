from __future__ import annotations

import asyncio
import secrets
from pathlib import Path

from fastapi import APIRouter, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth
from core.research import orchestrator as ai_orchestrator


router = APIRouter()


def _record_proposal_lifecycle(app, proposal, *, actor: str, from_state: str | None, reason: str) -> None:
    ai_orchestrator.record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="proposal",
        object_id=proposal.proposal_id,
        from_state=from_state,
        to_state=str(proposal.status),
        actor=actor,
        reason=reason,
    )


async def _run_ai_proposal_job(app, *, job_id: str, proposal_id: str, config, actor: str, request_payload: dict) -> None:
    ops_api._ensure_ops_runtime_state(app)
    jobs = app.state.research_jobs
    job = jobs.get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = ops_api._now_utc().isoformat()
    jobs[job_id] = job

    proposal = ops_api._proposal_from_registry(app, proposal_id)
    old_status = str(proposal.status)
    proposal.status = "research_running"
    proposal.metadata["last_research_request"] = request_payload
    proposal.metadata["last_research_job_id"] = job_id
    ops_api._save_proposal(app, proposal)
    _record_proposal_lifecycle(app, proposal, actor=actor, from_state=old_status, reason="background research started")

    try:
        result = await ai_orchestrator.run_strategy_research(config)
        summary, candidates, candidate = ai_orchestrator._create_candidates_from_result(proposal, app.state.ai_experiment_registry.get(job["experiment_id"]), result)

        experiment = app.state.ai_experiment_registry.get(job["experiment_id"])
        if experiment is not None:
            experiment.status = "completed"
            app.state.ai_experiment_registry.save(experiment)
        run = app.state.ai_experiment_run_registry.get(job["run_id"])
        if run is not None:
            run.status = "completed"
            run.finished_at = ops_api._now_utc()
            run.result = result
            app.state.ai_experiment_run_registry.save(run)

        proposal = ops_api._proposal_from_registry(app, proposal_id)
        old_status = str(proposal.status)
        proposal = ops_api._apply_research_result_to_proposal(proposal, result, job_id=job_id)
        proposal.latest_experiment_id = job.get("experiment_id")
        if candidate is not None:
            proposal.latest_candidate_id = candidate.candidate_id
        ops_api._save_proposal(app, proposal)
        _record_proposal_lifecycle(app, proposal, actor=actor, from_state=old_status, reason="background research finished")

        for item in candidates:
            app.state.ai_candidate_registry.save(item)
            ai_orchestrator.record_lifecycle(
                app.state.ai_lifecycle_registry,
                object_type="candidate",
                object_id=item.candidate_id,
                from_state=None,
                to_state=str(item.status),
                actor=actor,
                reason="candidate created from research result",
                metadata={"proposal_id": proposal_id, "job_id": job_id},
            )

        latest_path = Path(app.state.research_latest_path)
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(
            ops_api.json.dumps(
                {
                    "job_id": job_id,
                    "proposal_id": proposal_id,
                    "experiment_id": job.get("experiment_id"),
                    "run_id": job.get("run_id"),
                    "request_summary": request_payload,
                    "csv_path": result.get("csv_path"),
                    "markdown_path": result.get("markdown_path"),
                    "top_result_summary": result.get("best"),
                    "finished_at": ops_api._now_utc().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        job.update(
            {
                "status": "completed",
                "finished_at": ops_api._now_utc().isoformat(),
                "result": {
                    "proposal_id": proposal_id,
                    "candidate_id": candidate.candidate_id if candidate else None,
                    "validation_summary": summary.model_dump(mode="json"),
                },
                "error": None,
            }
        )
    except Exception as exc:
        proposal = ops_api._proposal_from_registry(app, proposal_id)
        old_status = str(proposal.status)
        proposal.status = "rejected"
        proposal.metadata["last_research_error"] = str(exc)
        proposal.metadata["last_research_job_id"] = job_id
        ops_api._save_proposal(app, proposal)
        _record_proposal_lifecycle(app, proposal, actor=actor, from_state=old_status, reason=f"background research failed: {exc}")
        job.update(
            {
                "status": "failed",
                "finished_at": ops_api._now_utc().isoformat(),
                "result": None,
                "error": str(exc),
            }
        )
    finally:
        jobs[job_id] = job
        app.state.research_job_tasks.pop(job_id, None)


@router.post("/ai/proposal")
async def create_ai_proposal(request: Request, payload: ops_api.AIProposalCreateRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/ai/proposal", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            proposal = ops_api._build_ai_research_proposal(payload, auth.actor)
            proposal = ops_api._save_proposal(request.app, proposal)
            _record_proposal_lifecycle(request.app, proposal, actor=auth.actor, from_state=None, reason="proposal created")
            audit_state["extra"] = {
                "proposal_id": proposal.proposal_id,
                "templates": len(proposal.strategy_templates),
                "symbols": len(proposal.target_symbols),
            }
            return ops_api._ok(
                {
                    "proposal": proposal.model_dump(mode="json"),
                    "registry_path": str(request.app.state.ai_proposal_registry.path),
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/ai/proposals")
async def list_ai_proposals(request: Request, limit: int = 20):
    ops_api._ensure_ops_runtime_state(request.app)
    rows = ops_api.list_ai_proposal_items(request.app, limit=limit)
    return ops_api._ok(
        {
            "items": [item.model_dump(mode="json") for item in rows],
            "count": len(rows),
            "registry_path": str(request.app.state.ai_proposal_registry.path),
        }
    )


@router.get("/ai/proposal/{proposal_id}")
async def get_ai_proposal(request: Request, proposal_id: str = ops_api.FPath(...)):
    ops_api._ensure_ops_runtime_state(request.app)
    item = ops_api.get_ai_proposal_item(request.app, proposal_id)
    return ops_api._ok({"proposal": item.model_dump(mode="json")})


@router.delete("/ai/proposal/{proposal_id}")
async def delete_ai_proposal(request: Request, proposal_id: str = ops_api.FPath(...)):
    auth = get_request_auth(request)
    async with ops_audit_scope(
        actor=auth.actor,
        endpoint="/ops/ai/proposal",
        method="DELETE",
        params={"proposal_id": proposal_id},
        ip=auth.client_ip,
    ) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            result = ops_api.delete_ai_proposal_item(
                request.app,
                proposal_id=proposal_id,
                actor=auth.actor,
            )
            audit_state["extra"] = result.get("deleted_counts", {})
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/ai/proposal/{proposal_id}/validation")
async def get_ai_proposal_validation(request: Request, proposal_id: str = ops_api.FPath(...)):
    item = ops_api.get_ai_proposal_item(request.app, proposal_id)
    return ops_api._ok(
        {
            "proposal_id": proposal_id,
            "status": item.status,
            "validation_summary": item.validation_summary.model_dump(mode="json") if item.validation_summary else None,
        }
    )


@router.get("/ai/proposal/{proposal_id}/lifecycle")
async def get_ai_proposal_lifecycle(request: Request, proposal_id: str = ops_api.FPath(...), limit: int = 200):
    ops_api._ensure_ops_runtime_state(request.app)
    _ = ops_api.get_ai_proposal_item(request.app, proposal_id)
    rows = ops_api.list_ai_lifecycle(request.app, "proposal", proposal_id, limit=limit)
    return ops_api._ok({"proposal_id": proposal_id, "items": rows, "count": len(rows)})


@router.post("/ai/proposal/{proposal_id}/run")
async def run_ai_proposal(request: Request, proposal_id: str, payload: ops_api.AIProposalRunRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/ai/proposal/run", method="POST", params={"proposal_id": proposal_id, **params}, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            proposal = ops_api._proposal_from_registry(request.app, proposal_id)
            config = ops_api._build_research_config_from_proposal(proposal, payload)
            experiment = ai_orchestrator._build_experiment_spec(proposal, config, auth.actor)
            run = ai_orchestrator._build_experiment_run(experiment.experiment_id)
            request.app.state.ai_experiment_registry.save(experiment)
            request.app.state.ai_experiment_run_registry.save(run)

            request_payload = {
                "proposal_id": proposal_id,
                "exchange": config.exchange,
                "symbol": config.symbol,
                "days": config.days,
                "timeframes": list(config.timeframes),
                "strategies": list(config.strategies),
                "commission_rate": float(config.commission_rate),
                "slippage_bps": float(config.slippage_bps),
                "initial_capital": float(config.initial_capital),
                "background": bool(payload.background),
                "experiment_id": experiment.experiment_id,
                "run_id": run.run_id,
            }

            proposal.latest_experiment_id = experiment.experiment_id
            proposal.metadata["last_research_request"] = request_payload
            if payload.background:
                old_status = str(proposal.status)
                proposal.status = "research_queued"
                ops_api._save_proposal(request.app, proposal)
                _record_proposal_lifecycle(request.app, proposal, actor=auth.actor, from_state=old_status, reason="proposal research queued")
                job_id = f"proposal-research-{int(ops_api._now_utc().timestamp())}-{secrets.token_hex(4)}"
                job = {
                    "job_id": job_id,
                    "proposal_id": proposal_id,
                    "experiment_id": experiment.experiment_id,
                    "run_id": run.run_id,
                    "status": "pending",
                    "created_at": ops_api._now_utc().isoformat(),
                    "started_at": None,
                    "finished_at": None,
                    "request": request_payload,
                    "result": None,
                    "error": None,
                }
                request.app.state.research_jobs[job_id] = job
                task = asyncio.create_task(
                    _run_ai_proposal_job(
                        request.app,
                        job_id=job_id,
                        proposal_id=proposal_id,
                        config=config,
                        actor=auth.actor,
                        request_payload=request_payload,
                    ),
                    name=f"ops_ai_research_{job_id}",
                )
                request.app.state.research_job_tasks[job_id] = task
                audit_state["extra"] = {"proposal_id": proposal_id, "job_id": job_id}
                return ops_api._ok(
                    {
                        "job": job,
                        "proposal": proposal.model_dump(mode="json"),
                        "experiment": experiment.model_dump(mode="json"),
                        "run": run.model_dump(mode="json"),
                    }
                )

            old_status = str(proposal.status)
            proposal.status = "research_running"
            ops_api._save_proposal(request.app, proposal)
            _record_proposal_lifecycle(request.app, proposal, actor=auth.actor, from_state=old_status, reason="proposal research started")

            experiment.status = "running"
            request.app.state.ai_experiment_registry.save(experiment)
            run.status = "running"
            run.started_at = ops_api._now_utc()
            request.app.state.ai_experiment_run_registry.save(run)

            research_result = await ai_orchestrator.run_strategy_research(config)
            summary, candidates, candidate = ai_orchestrator._create_candidates_from_result(proposal, experiment, research_result)

            run.status = "completed"
            run.finished_at = ops_api._now_utc()
            run.result = research_result
            request.app.state.ai_experiment_run_registry.save(run)
            experiment.status = "completed"
            request.app.state.ai_experiment_registry.save(experiment)

            proposal = ops_api._proposal_from_registry(request.app, proposal_id)
            old_status = str(proposal.status)
            proposal = ops_api._apply_research_result_to_proposal(proposal, research_result, job_id=None)
            proposal.latest_experiment_id = experiment.experiment_id
            if candidate is not None:
                proposal.latest_candidate_id = candidate.candidate_id
            ops_api._save_proposal(request.app, proposal)
            _record_proposal_lifecycle(request.app, proposal, actor=auth.actor, from_state=old_status, reason="proposal research finished")

            for item in candidates:
                request.app.state.ai_candidate_registry.save(item)
                ai_orchestrator.record_lifecycle(
                    request.app.state.ai_lifecycle_registry,
                    object_type="candidate",
                    object_id=item.candidate_id,
                    from_state=None,
                    to_state=str(item.status),
                    actor=auth.actor,
                    reason="candidate created from research result",
                    metadata={"proposal_id": proposal_id, "run_id": run.run_id},
                )

            if payload.background:
                return ops_api._err("background branch should have returned earlier")
            audit_state["extra"] = {"proposal_id": proposal_id, "status": proposal.status, "candidate_id": candidate.candidate_id if candidate else None}
            return ops_api._ok(
                {
                    "proposal": proposal.model_dump(mode="json"),
                    "experiment": experiment.model_dump(mode="json"),
                    "run": run.model_dump(mode="json"),
                    "candidate": candidate.model_dump(mode="json") if candidate else None,
                    "promotion": candidate.promotion.model_dump(mode="json") if candidate and candidate.promotion else None,
                    "research_result": research_result,
                    "validation_summary": summary.model_dump(mode="json"),
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/ai/experiments")
async def list_ai_experiments(request: Request, limit: int = 50):
    ops_api._ensure_ops_runtime_state(request.app)
    rows = ops_api.list_ai_experiment_items(request.app, limit=limit)
    return ops_api._ok({"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)})


@router.get("/ai/experiment/{experiment_id}")
async def get_ai_experiment(request: Request, experiment_id: str = ops_api.FPath(...)):
    ops_api._ensure_ops_runtime_state(request.app)
    item = ops_api.get_ai_experiment_item(request.app, experiment_id)
    return ops_api._ok({"experiment": item.model_dump(mode="json")})


@router.get("/ai/experiment/{experiment_id}/runs")
async def get_ai_experiment_runs(request: Request, experiment_id: str = ops_api.FPath(...), limit: int = 100):
    ops_api._ensure_ops_runtime_state(request.app)
    _ = ops_api.get_ai_experiment_item(request.app, experiment_id)
    rows = ops_api.list_ai_experiment_runs(request.app, experiment_id, limit=limit)
    return ops_api._ok({"experiment_id": experiment_id, "items": [row.model_dump(mode="json") for row in rows], "count": len(rows)})


@router.get("/ai/candidates")
async def list_ai_candidates(request: Request, limit: int = 50):
    ops_api._ensure_ops_runtime_state(request.app)
    rows = ops_api.list_ai_candidate_items(request.app, limit=limit)
    return ops_api._ok({"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)})


@router.get("/ai/candidate/{candidate_id}")
async def get_ai_candidate(request: Request, candidate_id: str = ops_api.FPath(...)):
    ops_api._ensure_ops_runtime_state(request.app)
    item = ops_api.get_ai_candidate_item(request.app, candidate_id)
    return ops_api._ok({"candidate": item.model_dump(mode="json")})


@router.post("/ai/candidate/{candidate_id}/promote")
async def promote_ai_candidate(request: Request, candidate_id: str, payload: ops_api.AICandidatePromotionRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/ai/candidate/promote", method="POST", params={"candidate_id": candidate_id, **params}, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            result = await ops_api.promote_existing_candidate(request.app, candidate_id=candidate_id, actor=auth.actor, target=payload.target)
            audit_state["extra"] = {
                "candidate_id": candidate_id,
                "runtime_status": result.get("runtime_status"),
                "registered_strategy_name": result.get("registered_strategy_name"),
            }
            return ops_api._ok(
                {
                    "candidate_id": candidate_id,
                    "candidate": result["candidate"].model_dump(mode="json"),
                    "proposal": result["proposal"].model_dump(mode="json"),
                    "promotion": result["promotion"].model_dump(mode="json"),
                    "runtime_status": result.get("runtime_status"),
                    "registered_strategy_name": result.get("registered_strategy_name"),
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/ai/promotions")
async def get_ai_promotions(request: Request, limit: int = 50):
    ops_api._ensure_ops_runtime_state(request.app)
    rows = ops_api.list_ai_promotions(request.app, limit=limit)
    return ops_api._ok({"items": rows, "count": len(rows)})


@router.get("/ai/deployments/status")
async def get_ai_deployments_status(request: Request):
    ops_api._ensure_ops_runtime_state(request.app)
    return ops_api._ok(ops_api.get_ai_deployment_status(request.app))
