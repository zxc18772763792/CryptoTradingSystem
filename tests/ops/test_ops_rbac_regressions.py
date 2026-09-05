from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.routing import APIRoute

from core.governance.rbac import GovernanceIdentity
from core.ops.service import ai_routes, auth as ops_auth_module, polymarket_routes, research_routes


@pytest.mark.parametrize(
    "router",
    [ai_routes.router, research_routes.router, polymarket_routes.router],
)
def test_ops_mutation_routes_declare_permissions(router):
    missing = []
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        if not (set(route.methods or set()) & {"POST", "PUT", "PATCH", "DELETE"}):
            continue
        if not route.dependencies:
            missing.append(f"{sorted(route.methods or set())} {route.path}")

    assert missing == []


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        (
            "post",
            "/ops/ai/proposal",
            {"json": {"thesis": "auditor must not create this proposal"}},
        ),
        ("post", "/ops/research/run", {"json": {}}),
        ("post", "/ops/polymarket/arm_trading", {}),
    ],
)
def test_auditor_cannot_mutate_ops_ai_research_or_polymarket(
    client,
    monkeypatch,
    method: str,
    path: str,
    kwargs: dict,
):
    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(
            return_value=GovernanceIdentity(
                actor="auditor_test",
                role="AUDITOR",
                api_key_present=True,
                token_present=False,
                client_ip="127.0.0.1",
            )
        ),
    )

    response = getattr(client, method)(path, headers={"X-API-KEY": "auditor-test-key"}, **kwargs)

    assert response.status_code == 403
