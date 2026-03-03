from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi import HTTPException, Request, status


@dataclass
class OpsAuthContext:
    actor: str
    token_present: bool
    client_ip: str


def get_ops_token(required: bool = True) -> str:
    token = str(os.getenv("OPS_TOKEN") or "").strip()
    if required and not token:
        raise RuntimeError("OPS_TOKEN is required for Ops API")
    return token


def ops_token_configured() -> bool:
    return bool(get_ops_token(required=False))


async def require_ops_auth(request: Request) -> OpsAuthContext:
    try:
        expected = get_ops_token(required=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    received = str(request.headers.get("X-OPS-TOKEN") or "").strip()
    if not received or received != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid ops token")

    actor = str(request.headers.get("X-OPS-CALLER") or "").strip() or "openclaw"
    client_ip = ""
    try:
        if request.client:
            client_ip = str(request.client.host or "")
    except Exception:
        client_ip = ""

    ctx = OpsAuthContext(actor=actor, token_present=True, client_ip=client_ip)
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
    return OpsAuthContext(actor=actor, token_present=False, client_ip=client_ip)
