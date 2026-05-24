"""Risk / circuit-breaker administrative API (Phase 4.2)."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.audit import audit_logger
from core.ops.service.auth import get_request_auth
from core.risk.circuit_breaker import circuit_breaker, run_circuit_breaker_checks
from web.api.auth import require_sensitive_ops_auth

router = APIRouter()


class CircuitBreakerResetRequest(BaseModel):
    scope: str = Field(..., description="'portfolio' or 'strategy'")
    strategy_name: Optional[str] = Field(default=None, description="required when scope='strategy'")
    confirm: bool = Field(default=False, description="must be true to actually reset")
    note: Optional[str] = Field(default=None, description="operator note for audit log")


@router.get("/circuit-breaker")
async def get_circuit_breaker_state(request: Request):
    """Return current circuit breaker state (read-only, no auth required)."""
    try:
        from core.risk.circuit_breaker import _resolve_active_strategy_names  # noqa: PLC0415

        active_names = _resolve_active_strategy_names()
    except Exception:
        active_names = None
    snap = circuit_breaker.snapshot(active_strategy_names=active_names)
    if active_names is not None:
        snap["strategies"] = dict(snap.get("active_strategies") or {})
    return snap


@router.post("/circuit-breaker/evaluate", dependencies=[Depends(require_sensitive_ops_auth)])
async def evaluate_circuit_breaker(
    request: Request,
):
    """Force an immediate evaluation pass. Useful for ops manual checks."""
    report = run_circuit_breaker_checks()
    return report


@router.post("/circuit-breaker/reset", dependencies=[Depends(require_sensitive_ops_auth)])
async def reset_circuit_breaker(
    request: Request,
    payload: CircuitBreakerResetRequest,
):
    """Manually clear a tripped state. Requires explicit ``confirm=true``."""
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="confirm must be true to reset circuit breaker")

    auth = get_request_auth(request)
    operator = str(getattr(auth, "actor", "") or "unknown")
    scope = (payload.scope or "").strip().lower()
    if scope == "portfolio":
        changed = circuit_breaker.reset_portfolio(operator)
        action = "portfolio_reset"
        target = "portfolio"
    elif scope == "strategy":
        name = (payload.strategy_name or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="strategy_name required for scope='strategy'")
        changed = circuit_breaker.reset_strategy(name, operator)
        action = "strategy_reset"
        target = name
    else:
        raise HTTPException(status_code=400, detail=f"unknown scope: {scope!r}")

    try:
        await audit_logger.log(
            actor=operator,
            action=f"circuit_breaker.{action}",
            target=target,
            details={
                "changed": bool(changed),
                "note": payload.note or "",
            },
        )
    except Exception:
        pass

    return {
        "scope": scope,
        "target": target,
        "changed": bool(changed),
        "state": circuit_breaker.snapshot(),
    }
