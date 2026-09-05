from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from core.governance.rbac import GovernanceIdentity
from core.governance.service import upsert_api_user
from core.ops.service import auth as ops_auth_module
from core.ops.service.api import GovernanceApiUserUpsertRequest, create_router


def _identity(role: str) -> GovernanceIdentity:
    return GovernanceIdentity(
        actor=f"{role.lower()}_test",
        role=role,
        api_key_present=True,
        token_present=False,
        client_ip="127.0.0.1",
    )


def test_engineer_cannot_mint_system_api_user(monkeypatch):
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_identity("ENGINEER")),
    )

    response = client.post(
        "/ops/governance/users/upsert",
        headers={"X-API-KEY": "engineer-auth-key"},
        json={
            "name": "forbidden-system-user",
            "role": "SYSTEM",
            "api_key": "test-only-StrongGeneratedKey_0123456789abcdef",
            "is_active": True,
        },
    )

    assert response.status_code == 403
    assert "role delegation denied" in response.json()["detail"]


def test_api_user_request_normalizes_allowlisted_role():
    payload = GovernanceApiUserUpsertRequest(
        name="engineer",
        role="engineer",
        api_key="test-only-StrongGeneratedKey_0123456789abcdef",
    )

    assert payload.role == "ENGINEER"


def test_api_user_request_rejects_unknown_role_and_short_key():
    with pytest.raises(ValidationError):
        GovernanceApiUserUpsertRequest.model_validate(
            {
            "name": "invalid",
            "role": "SUPERADMIN",
            "api_key": "test-only-StrongGeneratedKey_0123456789abcdef",
            }
        )
    with pytest.raises(ValidationError):
        GovernanceApiUserUpsertRequest.model_validate(
            {"name": "invalid", "role": "ENGINEER", "api_key": "short"}
        )


def test_api_user_service_rejects_weak_key_when_called_directly():
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            upsert_api_user(
                actor=_identity("SYSTEM"),
                name="weak-key-user",
                role="ENGINEER",
                api_key="short",
            )
        )

    assert exc_info.value.status_code == 422
