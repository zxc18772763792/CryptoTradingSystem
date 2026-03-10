from __future__ import annotations

from fastapi import APIRouter, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth


router = APIRouter()


@router.get("/governance/strategies")
async def governance_list_strategies(request: Request, limit: int = 100):
    auth = get_request_auth(request)
    rows = await ops_api.list_governance_strategy_specs(limit=limit)
    return ops_api._ok({"items": rows, "count": len(rows), "actor_role": auth.role})


@router.get("/governance/users")
async def governance_list_users(request: Request, limit: int = 100):
    _ = get_request_auth(request)
    rows = await ops_api.list_governance_api_users(limit=limit)
    return ops_api._ok({"items": rows, "count": len(rows)})


@router.post("/governance/users/upsert")
async def governance_upsert_user(request: Request, payload: ops_api.GovernanceApiUserUpsertRequest):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    result = await ops_api.governance_upsert_api_user(
        actor=identity,
        name=payload.name,
        role=payload.role,
        api_key=payload.api_key,
        is_active=payload.is_active,
    )
    return ops_api._ok(result)


@router.post("/governance/strategy/propose")
async def governance_propose_strategy_endpoint(request: Request, payload: ops_api.GovernanceStrategyProposeRequest):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    params = payload.model_dump(mode="json")
    async with ops_audit_scope(
        actor=auth.actor,
        endpoint="/ops/governance/strategy/propose",
        method="POST",
        params=params,
        ip=auth.client_ip,
    ) as audit_state:
        try:
            result = await ops_api.governance_propose_strategy(
                identity,
                strategy_id=payload.strategy_id,
                name=payload.name,
                strategy_class=payload.strategy_class,
                params=payload.params,
                guardrails=payload.guardrails,
                metrics=payload.metrics,
                regime=payload.regime,
            )
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            raise


@router.post("/governance/strategy/{strategy_id}/{version}/approve")
async def governance_approve_strategy(
    request: Request,
    strategy_id: str,
    version: int,
    payload: ops_api.GovernanceStrategyTransitionRequest,
):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    return ops_api._ok(
        await ops_api.governance_transition_strategy(
            identity,
            strategy_id=strategy_id,
            version=version,
            target="approved",
            note=payload.note,
        )
    )


@router.post("/governance/strategy/{strategy_id}/{version}/promote_paper")
async def governance_promote_paper(
    request: Request,
    strategy_id: str,
    version: int,
    payload: ops_api.GovernanceStrategyTransitionRequest,
):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    return ops_api._ok(
        await ops_api.governance_transition_strategy(
            identity,
            strategy_id=strategy_id,
            version=version,
            target="paper",
            note=payload.note,
        )
    )


@router.post("/governance/strategy/{strategy_id}/{version}/request_live")
async def governance_request_live(
    request: Request,
    strategy_id: str,
    version: int,
    payload: ops_api.GovernanceStrategyTransitionRequest,
):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    return ops_api._ok(
        await ops_api.governance_transition_strategy(
            identity,
            strategy_id=strategy_id,
            version=version,
            target="live",
            note=payload.note or "request_live",
        )
    )


@router.post("/governance/strategy/{strategy_id}/{version}/approve_live")
async def governance_approve_live(
    request: Request,
    strategy_id: str,
    version: int,
    payload: ops_api.GovernanceStrategyTransitionRequest,
):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    return ops_api._ok(
        await ops_api.governance_transition_strategy(
            identity,
            strategy_id=strategy_id,
            version=version,
            target="live",
            note=payload.note or "approve_live",
        )
    )


@router.post("/governance/strategy/{strategy_id}/{version}/retire")
async def governance_retire_strategy(
    request: Request,
    strategy_id: str,
    version: int,
    payload: ops_api.GovernanceStrategyTransitionRequest,
):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    return ops_api._ok(
        await ops_api.governance_transition_strategy(
            identity,
            strategy_id=strategy_id,
            version=version,
            target="retired",
            note=payload.note,
        )
    )


@router.get("/governance/risk/current")
async def governance_risk_current(request: Request):
    _ = get_request_auth(request)
    current = await ops_api.get_active_risk_config()
    return ops_api._ok(current)


@router.get("/governance/risk/changes")
async def governance_risk_changes(request: Request, limit: int = 100):
    _ = get_request_auth(request)
    rows = await ops_api.list_risk_change_requests(limit=limit)
    return ops_api._ok({"items": rows, "count": len(rows)})


@router.post("/governance/risk/request_change")
async def governance_risk_request_change(request: Request, payload: ops_api.GovernanceRiskChangeRequest):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    result = await ops_api.governance_request_risk_change(
        identity=identity,
        proposed_config=payload.proposed_config,
        reason=payload.reason,
    )
    runtime = getattr(request.app.state, "governance_runtime", {}) or {}
    runtime["risk_config_version"] = result.get("proposed_version") or runtime.get("risk_config_version")
    if result.get("status") == "applied":
        cfg = payload.proposed_config.model_dump()
        runtime["reduce_only"] = bool(cfg.get("reduce_only", runtime.get("reduce_only", False)))
        runtime["kill_switch"] = bool(cfg.get("kill_switch", runtime.get("kill_switch", False)))
    request.app.state.governance_runtime = runtime
    return ops_api._ok(result)


@router.post("/governance/risk/approve_change/{request_id}")
async def governance_risk_approve_change(request: Request, request_id: str):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    result = await ops_api.approve_risk_change(identity=identity, request_id=request_id)
    runtime = getattr(request.app.state, "governance_runtime", {}) or {}
    runtime["risk_config_version"] = result.get("proposed_version")
    runtime["reduce_only"] = bool(result.get("config", {}).get("reduce_only", runtime.get("reduce_only", False)))
    runtime["kill_switch"] = bool(result.get("config", {}).get("kill_switch", runtime.get("kill_switch", False)))
    request.app.state.governance_runtime = runtime
    return ops_api._ok(result)


@router.post("/governance/risk/set_reduce_only")
async def governance_set_reduce_only(request: Request, payload: ops_api.GovernanceRiskToggleRequest):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    current = await ops_api.get_active_risk_config()
    cfg = dict(current.get("config") or {})
    cfg["reduce_only"] = bool(payload.enabled)
    result = await ops_api.governance_request_risk_change(
        identity=identity,
        proposed_config=ops_api.RiskConfigPayload.model_validate(cfg),
        reason=payload.reason or ("set reduce_only" if payload.enabled else "unset reduce_only"),
    )
    runtime = getattr(request.app.state, "governance_runtime", {}) or {}
    runtime["reduce_only"] = bool(payload.enabled)
    request.app.state.governance_runtime = runtime
    return ops_api._ok(result)


@router.post("/governance/risk/kill_switch")
async def governance_set_kill_switch(request: Request, payload: ops_api.GovernanceRiskToggleRequest):
    auth = get_request_auth(request)
    identity = ops_api._governance_identity_from_auth(auth)
    current = await ops_api.get_active_risk_config()
    cfg = dict(current.get("config") or {})
    cfg["kill_switch"] = bool(payload.enabled)
    result = await ops_api.governance_request_risk_change(
        identity=identity,
        proposed_config=ops_api.RiskConfigPayload.model_validate(cfg),
        reason=payload.reason or ("set kill_switch" if payload.enabled else "unset kill_switch"),
    )
    runtime = getattr(request.app.state, "governance_runtime", {}) or {}
    runtime["kill_switch"] = bool(payload.enabled)
    request.app.state.governance_runtime = runtime
    return ops_api._ok(result)


@router.post("/governance/audit/query")
async def governance_query_audit(request: Request, payload: ops_api.GovernanceAuditQuery):
    _ = get_request_auth(request)
    rows = await ops_api.list_governance_audit_records(
        module=payload.module,
        action=payload.action,
        actor=payload.actor,
        trace_id=payload.trace_id,
        limit=payload.limit,
    )
    return ops_api._ok({"items": rows, "count": len(rows)})
