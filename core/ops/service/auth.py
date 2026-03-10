from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi import HTTPException, Request, status

from core.governance.rbac import resolve_api_key_identity


@dataclass
class OpsAuthContext:
    actor: str
    role: str
    token_present: bool
    api_key_present: bool
    client_ip: str


def get_ops_token(required: bool = True) -> str:
    token = str(os.getenv("OPS_TOKEN") or "").strip()
    if required and not token:
        raise RuntimeError("OPS_TOKEN is required for Ops API")
    return token


def ops_token_configured() -> bool:
    return bool(get_ops_token(required=False))


async def require_ops_auth(request: Request) -> OpsAuthContext:
    api_key = str(request.headers.get("X-API-KEY") or "").strip()
    actor = str(request.headers.get("X-OPS-CALLER") or "").strip() or "openclaw"
    client_ip = ""
    try:
        if request.client:
            client_ip = str(request.client.host or "")
    except Exception:
        client_ip = ""

    if api_key:
        identity = await resolve_api_key_identity(api_key)
        if identity is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid api key")
        resolved_actor = str(identity.actor or actor)
        ctx = OpsAuthContext(
            actor=resolved_actor,
            role=str(identity.role or "OPERATOR"),
            token_present=False,
            api_key_present=True,
            client_ip=client_ip,
        )
        request.state.ops_auth = ctx
        return ctx

    try:
        expected = get_ops_token(required=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    received = str(request.headers.get("X-OPS-TOKEN") or "").strip()
    if not received or received != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid ops token")

    ctx = OpsAuthContext(
        actor=actor,
        role="SYSTEM",
        token_present=True,
        api_key_present=False,
        client_ip=client_ip,
    )
    request.state.ops_auth = ctx
    return ctx


def get_request_auth(request: Request) -> OpsAuthContext:
    ctx = getattr(request.state, "ops_auth", None)
    if isinstance(ctx, OpsAuthContext):
        return ctx
    actor = str(request.headers.get("X-OPS-CALLER") or "").strip() or "openclaw"
    client_ip = ""
    try:
        if request.client:
            client_ip = str(request.client.host or "")
    except Exception:
        client_ip = ""
    return OpsAuthContext(
        actor=actor,
        role="SYSTEM",
        token_present=False,
        api_key_present=False,
        client_ip=client_ip,
    )
