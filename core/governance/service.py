from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import and_, func, select

from config.database import (
    ApiUser,
    AuditRecord,
    RiskChangeRequest,
    RiskConfig,
    StrategyApproval,
    StrategySpec,
    async_session_maker,
)
from config.settings import settings
from core.governance.audit import GovernanceAuditEvent, new_trace_id, write_audit
from core.governance.rbac import GovernanceIdentity, has_permission
from core.governance.rbac import hash_api_key as _hash_api_key
from core.governance.schemas import RiskConfigPayload, StrategyLifecycleState
from core.risk.risk_manager import risk_manager


_STRATEGY_TRANSITIONS: Dict[str, set[str]] = {
    "proposed": {"approved", "retired"},
    "approved": {"paper", "retired"},
    "paper": {"live", "retired"},
    "live": {"retired"},
    "retired": set(),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_role(role: str) -> str:
    return str(role or "").upper().strip()


def _require_permission(identity: GovernanceIdentity, permission: str) -> None:
    if has_permission(identity.role, permission):
        return
    raise HTTPException(status_code=403, detail=f"permission denied: {permission}")


def _risk_cfg_from_runtime() -> Dict[str, Any]:
    report = risk_manager.get_risk_report()
    limits = dict(report.get("limits") or {})
    return {
        "max_leverage": float(limits.get("max_leverage", 3.0) or 3.0),
        "max_position_notional_pct": float(limits.get("max_position_size", 0.1) or 0.1),
        "max_trade_risk_pct": float(limits.get("max_position_size", 0.1) or 0.1),
        "max_daily_drawdown_pct": float(limits.get("max_daily_loss_ratio", 0.02) or 0.02),
        "spread_limit_bps": 25.0,
        "data_staleness_limit_ms": 60_000,
        "allowed_symbols": [],
        "allowed_timeframes": [],
        "reduce_only": False,
        "kill_switch": bool(report.get("trading_halted", False)),
    }


def _diff_dict(base: Dict[str, Any], proposed: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    keys = set(base.keys()) | set(proposed.keys())
    for key in sorted(keys):
        left = base.get(key)
        right = proposed.get(key)
        if left == right:
            continue
        out[key] = {"from": left, "to": right}
    return out


def _is_list_expanded(base: Any, proposed: Any) -> bool:
    if not isinstance(base, list) or not isinstance(proposed, list):
        return False
    left = {str(x) for x in base}
    right = {str(x) for x in proposed}
    return right > left


def _risk_delta_score(base: Dict[str, Any], proposed: Dict[str, Any]) -> float:
    score = 0.0
    rules: List[Tuple[str, float]] = [
        ("max_leverage", 2.0),
        ("max_position_notional_pct", 2.0),
        ("max_trade_risk_pct", 2.0),
        ("max_daily_drawdown_pct", 1.5),
        ("spread_limit_bps", 1.0),
        ("data_staleness_limit_ms", 1.0),
    ]
    for key, weight in rules:
        a = float(base.get(key, 0.0) or 0.0)
        b = float(proposed.get(key, 0.0) or 0.0)
        if b > a:
            score += weight
    for key in ("kill_switch", "reduce_only"):
        if bool(base.get(key, False)) and not bool(proposed.get(key, False)):
            score += 1.0
    if _is_list_expanded(base.get("allowed_symbols"), proposed.get("allowed_symbols")):
        score += 1.0
    if _is_list_expanded(base.get("allowed_timeframes"), proposed.get("allowed_timeframes")):
        score += 1.0
    return round(score, 3)


def _is_increase_risk(base: Dict[str, Any], proposed: Dict[str, Any]) -> bool:
    return _risk_delta_score(base, proposed) > 0


async def ensure_risk_config_initialized(actor: str = "system") -> Dict[str, Any]:
    async with async_session_maker() as session:
        result = await session.execute(select(RiskConfig).where(RiskConfig.is_active.is_(True)).limit(1))
        row = result.scalars().first()
        if row:
            return {
                "version": int(row.version),
                "config": dict(row.config or {}),
                "is_active": bool(row.is_active),
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }

        cfg = _risk_cfg_from_runtime()
        first = RiskConfig(
            version=1,
            config=cfg,
            is_active=True,
            created_by=str(actor or "system"),
            created_at=_now(),
            activated_at=_now(),
        )
        session.add(first)
        await session.commit()
        return {
            "version": 1,
            "config": cfg,
            "is_active": True,
            "created_at": first.created_at.isoformat() if first.created_at else None,
        }


async def get_active_risk_config() -> Dict[str, Any]:
    await ensure_risk_config_initialized()
    async with async_session_maker() as session:
        result = await session.execute(select(RiskConfig).where(RiskConfig.is_active.is_(True)).limit(1))
        row = result.scalars().first()
        if row is None:
            raise HTTPException(status_code=500, detail="no active risk config")
        return {
            "version": int(row.version),
            "config": dict(row.config or {}),
            "is_active": bool(row.is_active),
            "created_by": str(row.created_by or "system"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "activated_at": row.activated_at.isoformat() if row.activated_at else None,
        }


async def _activate_risk_config(
    *,
    base_version: int,
    config: Dict[str, Any],
    actor: GovernanceIdentity,
) -> Dict[str, Any]:
    async with async_session_maker() as session:
        res_max = await session.execute(select(func.max(RiskConfig.version)))
        current_max = int(res_max.scalar() or 0)
        new_version = max(current_max, int(base_version)) + 1

        await session.execute(
            RiskConfig.__table__.update().where(RiskConfig.is_active.is_(True)).values(is_active=False)
        )
        row = RiskConfig(
            version=new_version,
            config=dict(config or {}),
            is_active=True,
            created_by=str(actor.actor or "system"),
            created_at=_now(),
            activated_at=_now(),
        )
        session.add(row)
        await session.commit()

    # runtime risk manager sync (existing keys only)
    risk_manager.update_parameters(
        {
            "max_position_size": float(config.get("max_position_notional_pct", risk_manager.max_position_size)),
            "max_daily_loss_ratio": float(config.get("max_daily_drawdown_pct", risk_manager.max_daily_loss_ratio)),
            "max_leverage": float(config.get("max_leverage", risk_manager.max_leverage)),
        }
    )
    if bool(config.get("kill_switch", False)):
        # Reuse existing halt path.
        risk_manager._trading_halted = True  # noqa: SLF001
        risk_manager._halt_reason = "governance kill_switch enabled"  # noqa: SLF001
    else:
        risk_manager.reset_halt()
    return {"version": new_version, "config": config}


async def request_risk_change(
    identity: GovernanceIdentity,
    proposed_config: RiskConfigPayload,
    reason: str = "",
) -> Dict[str, Any]:
    current = await get_active_risk_config()
    base_cfg = dict(current.get("config") or {})
    next_cfg = proposed_config.model_dump()
    diff = _diff_dict(base_cfg, next_cfg)
    score = _risk_delta_score(base_cfg, next_cfg)
    increase = _is_increase_risk(base_cfg, next_cfg)

    status = "pending" if increase else "approved"
    if not increase and _normalize_role(identity.role) in {"OPERATOR", "RISK_OWNER", "SYSTEM"}:
        status = "applied"

    request_id = f"rcr_{int(_now().timestamp())}_{secrets.token_hex(3)}"
    proposed_version = None
    if status == "applied":
        activated = await _activate_risk_config(
            base_version=int(current.get("version") or 1),
            config=next_cfg,
            actor=identity,
        )
        proposed_version = int(activated["version"])

    async with async_session_maker() as session:
        row = RiskChangeRequest(
            request_id=request_id,
            base_version=int(current.get("version") or 1),
            proposed_version=proposed_version,
            status=status,
            requested_by=str(identity.actor or "system"),
            requested_role=_normalize_role(identity.role),
            approved_by=(str(identity.actor) if status == "applied" else None),
            approved_role=(_normalize_role(identity.role) if status == "applied" else None),
            risk_delta_score=float(score),
            diff=diff,
            reason=str(reason or ""),
            created_at=_now(),
            updated_at=_now(),
        )
        session.add(row)
        await session.commit()

    trace_id = new_trace_id()
    await write_audit(
        GovernanceAuditEvent(
            module="governance.risk",
            action="request_risk_change",
            actor=identity.actor,
            role=identity.role,
            trace_id=trace_id,
            input_payload={"base": base_cfg, "proposed": next_cfg, "reason": reason},
            output_payload={"request_id": request_id, "status": status, "risk_delta_score": score},
            payload_json={"diff": diff, "increase_risk": increase},
        )
    )
    return {
        "request_id": request_id,
        "status": status,
        "base_version": int(current.get("version") or 1),
        "proposed_version": proposed_version,
        "risk_delta_score": score,
        "diff": diff,
        "increase_risk": increase,
    }


async def approve_risk_change(identity: GovernanceIdentity, request_id: str) -> Dict[str, Any]:
    _require_permission(identity, "approve_risk_change")
    async with async_session_maker() as session:
        result = await session.execute(select(RiskChangeRequest).where(RiskChangeRequest.request_id == str(request_id)))
        row = result.scalars().first()
        if row is None:
            raise HTTPException(status_code=404, detail="risk change request not found")
        if str(row.status) not in {"pending", "approved"}:
            raise HTTPException(status_code=409, detail=f"request status={row.status} cannot approve")

        current = await get_active_risk_config()
        base_cfg = dict(current.get("config") or {})
        diff = dict(row.diff or {})
        proposed_cfg = dict(base_cfg)
        for key, patch in diff.items():
            if isinstance(patch, dict):
                proposed_cfg[key] = patch.get("to")

        activated = await _activate_risk_config(
            base_version=int(current.get("version") or 1),
            config=proposed_cfg,
            actor=identity,
        )

        row.status = "applied"
        row.approved_by = str(identity.actor or "system")
        row.approved_role = _normalize_role(identity.role)
        row.proposed_version = int(activated["version"])
        row.updated_at = _now()
        session.add(row)
        await session.commit()

    await write_audit(
        GovernanceAuditEvent(
            module="governance.risk",
            action="approve_risk_change",
            actor=identity.actor,
            role=identity.role,
            input_payload={"request_id": request_id},
            output_payload={"status": "applied", "proposed_version": int(activated["version"])},
            payload_json={"request_id": request_id},
        )
    )
    return {
        "request_id": request_id,
        "status": "applied",
        "proposed_version": int(activated["version"]),
        "config": proposed_cfg,
    }


async def list_risk_change_requests(limit: int = 100) -> List[Dict[str, Any]]:
    async with async_session_maker() as session:
        result = await session.execute(
            select(RiskChangeRequest)
            .order_by(RiskChangeRequest.created_at.desc())
            .limit(max(1, min(int(limit or 0), 500)))
        )
        rows = result.scalars().all()
    return [
        {
            "request_id": str(row.request_id),
            "base_version": int(row.base_version),
            "proposed_version": int(row.proposed_version) if row.proposed_version is not None else None,
            "status": str(row.status),
            "requested_by": str(row.requested_by),
            "requested_role": str(row.requested_role),
            "approved_by": str(row.approved_by or ""),
            "approved_role": str(row.approved_role or ""),
            "risk_delta_score": float(row.risk_delta_score or 0.0),
            "diff": dict(row.diff or {}),
            "reason": str(row.reason or ""),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


async def _next_strategy_version(session: Any, strategy_id: str) -> int:
    result = await session.execute(
        select(func.max(StrategySpec.version)).where(StrategySpec.strategy_id == str(strategy_id))
    )
    return int(result.scalar() or 0) + 1


async def propose_strategy(
    identity: GovernanceIdentity,
    *,
    strategy_id: str,
    name: str,
    strategy_class: str,
    params: Dict[str, Any],
    guardrails: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    regime: str = "mixed",
) -> Dict[str, Any]:
    _require_permission(identity, "propose_strategy")
    async with async_session_maker() as session:
        version = await _next_strategy_version(session, strategy_id)
        row = StrategySpec(
            strategy_id=str(strategy_id),
            version=version,
            name=str(name),
            strategy_class=str(strategy_class),
            status="proposed",
            params=dict(params or {}),
            guardrails=dict(guardrails or {}),
            metrics=dict(metrics or {}),
            regime=str(regime or "mixed"),
            created_by=str(identity.actor or "system"),
            created_at=_now(),
            updated_at=_now(),
        )
        session.add(row)
        await session.commit()
    await write_audit(
        GovernanceAuditEvent(
            module="governance.strategy",
            action="propose_strategy",
            actor=identity.actor,
            role=identity.role,
            input_payload={
                "strategy_id": strategy_id,
                "name": name,
                "strategy_class": strategy_class,
                "params": params,
            },
            output_payload={"version": version, "status": "proposed"},
        )
    )
    return {
        "strategy_id": str(strategy_id),
        "version": int(version),
        "status": "proposed",
    }


async def _get_strategy_spec(session: Any, strategy_id: str, version: int) -> StrategySpec:
    result = await session.execute(
        select(StrategySpec).where(
            and_(
                StrategySpec.strategy_id == str(strategy_id),
                StrategySpec.version == int(version),
            )
        )
    )
    row = result.scalars().first()
    if row is None:
        raise HTTPException(status_code=404, detail="strategy spec not found")
    return row


def _check_transition(current: str, target: str) -> None:
    allowed = _STRATEGY_TRANSITIONS.get(str(current), set())
    if str(target) not in allowed:
        raise HTTPException(status_code=409, detail=f"invalid strategy transition {current}->{target}")


def _transition_permission(target: str) -> str:
    mapping = {
        "approved": "approve_strategy",
        "paper": "promote_paper",
        "live": "approve_live",
        "retired": "retire_strategy",
    }
    return mapping.get(str(target), "approve_strategy")


async def transition_strategy(
    identity: GovernanceIdentity,
    *,
    strategy_id: str,
    version: int,
    target: StrategyLifecycleState,
    note: str = "",
) -> Dict[str, Any]:
    if str(target) == "live":
        if not (has_permission(identity.role, "request_live") or has_permission(identity.role, "approve_live")):
            raise HTTPException(status_code=403, detail="permission denied: request_live/approve_live")
    else:
        _require_permission(identity, _transition_permission(str(target)))
    require_dual = bool(getattr(settings, "REQUIRE_DUAL_APPROVAL_FOR_LIVE", True))

    async with async_session_maker() as session:
        row = await _get_strategy_spec(session, strategy_id, version)
        current_status = str(row.status)
        _check_transition(current_status, str(target))

        transition_label = f"{current_status}->{target}"
        if str(target) == "live" and require_dual:
            approval = StrategyApproval(
                strategy_id=str(strategy_id),
                version=int(version),
                transition=transition_label,
                approver=str(identity.actor or "system"),
                approver_role=_normalize_role(identity.role),
                approved=True,
                note=str(note or ""),
                created_at=_now(),
            )
            session.add(approval)
            await session.commit()

            result = await session.execute(
                select(StrategyApproval).where(
                    and_(
                        StrategyApproval.strategy_id == str(strategy_id),
                        StrategyApproval.version == int(version),
                        StrategyApproval.transition == transition_label,
                        StrategyApproval.approved.is_(True),
                    )
                )
            )
            approvals = result.scalars().all()
            roles = {str(item.approver_role or "").upper() for item in approvals}
            if not {"RESEARCH_LEAD", "RISK_OWNER"}.issubset(roles):
                return {
                    "strategy_id": str(strategy_id),
                    "version": int(version),
                    "status": current_status,
                    "pending_approvals": sorted(list({"RESEARCH_LEAD", "RISK_OWNER"} - roles)),
                    "approvals_collected": sorted(list(roles)),
                    "require_dual_approval": True,
                }

        row.status = str(target)
        row.updated_at = _now()
        session.add(row)
        session.add(
            StrategyApproval(
                strategy_id=str(strategy_id),
                version=int(version),
                transition=transition_label,
                approver=str(identity.actor or "system"),
                approver_role=_normalize_role(identity.role),
                approved=True,
                note=str(note or ""),
                created_at=_now(),
            )
        )
        await session.commit()

    await write_audit(
        GovernanceAuditEvent(
            module="governance.strategy",
            action="transition_strategy",
            actor=identity.actor,
            role=identity.role,
            input_payload={
                "strategy_id": strategy_id,
                "version": version,
                "target": target,
            },
            output_payload={"status": str(target)},
        )
    )
    return {
        "strategy_id": str(strategy_id),
        "version": int(version),
        "status": str(target),
    }


async def list_strategy_specs(limit: int = 100) -> List[Dict[str, Any]]:
    async with async_session_maker() as session:
        result = await session.execute(
            select(StrategySpec)
            .order_by(StrategySpec.updated_at.desc())
            .limit(max(1, min(int(limit or 0), 500)))
        )
        rows = result.scalars().all()
    return [
        {
            "strategy_id": str(row.strategy_id),
            "version": int(row.version),
            "name": str(row.name),
            "strategy_class": str(row.strategy_class),
            "status": str(row.status),
            "params": dict(row.params or {}),
            "guardrails": dict(row.guardrails or {}),
            "metrics": dict(row.metrics or {}),
            "regime": str(row.regime or "mixed"),
            "created_by": str(row.created_by or "system"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]


async def list_audit_records(
    *,
    module: Optional[str] = None,
    action: Optional[str] = None,
    actor: Optional[str] = None,
    trace_id: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    async with async_session_maker() as session:
        stmt = select(AuditRecord).order_by(AuditRecord.created_at.desc()).limit(max(1, min(int(limit or 0), 2000)))
        if module:
            stmt = stmt.where(AuditRecord.module == str(module))
        if action:
            stmt = stmt.where(AuditRecord.action == str(action))
        if actor:
            stmt = stmt.where(AuditRecord.actor == str(actor))
        if trace_id:
            stmt = stmt.where(AuditRecord.trace_id == str(trace_id))
        result = await session.execute(stmt)
        rows = result.scalars().all()
    return [
        {
            "id": int(row.id),
            "trace_id": str(row.trace_id),
            "actor": str(row.actor),
            "role": str(row.role),
            "module": str(row.module),
            "action": str(row.action),
            "status": str(row.status),
            "input_hash": str(row.input_hash or ""),
            "output_hash": str(row.output_hash or ""),
            "payload_json": dict(row.payload_json or {}),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


async def upsert_api_user(
    *,
    actor: GovernanceIdentity,
    name: str,
    role: str,
    api_key: str,
    is_active: bool = True,
) -> Dict[str, Any]:
    _require_permission(actor, "deploy_config")
    key_hash = _hash_api_key(api_key)
    async with async_session_maker() as session:
        result = await session.execute(select(ApiUser).where(ApiUser.api_key_hash == key_hash))
        row = result.scalars().first()
        if row is None:
            row = ApiUser(
                name=str(name),
                role=_normalize_role(role),
                api_key_hash=key_hash,
                is_active=bool(is_active),
                created_at=_now(),
                updated_at=_now(),
            )
        else:
            row.name = str(name)
            row.role = _normalize_role(role)
            row.is_active = bool(is_active)
            row.updated_at = _now()
        session.add(row)
        await session.commit()
    await write_audit(
        GovernanceAuditEvent(
            module="governance.rbac",
            action="upsert_api_user",
            actor=actor.actor,
            role=actor.role,
            input_payload={"name": name, "role": role, "is_active": is_active},
            output_payload={"api_key_hash_prefix": key_hash[:12], "is_active": is_active},
        )
    )
    return {
        "name": str(name),
        "role": _normalize_role(role),
        "is_active": bool(is_active),
        "api_key_hash_prefix": key_hash[:12],
    }


async def list_api_users(limit: int = 100) -> List[Dict[str, Any]]:
    async with async_session_maker() as session:
        result = await session.execute(
            select(ApiUser).order_by(ApiUser.updated_at.desc()).limit(max(1, min(int(limit or 0), 500)))
        )
        rows = result.scalars().all()
    return [
        {
            "name": str(row.name),
            "role": str(row.role),
            "is_active": bool(row.is_active),
            "api_key_hash_prefix": str(row.api_key_hash or "")[:12],
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]
