from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ops.service.api import create_router


@pytest.fixture
def ops_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    monkeypatch.delenv("OPS_ALLOW_MANUAL_SIGNAL", raising=False)

    app = FastAPI()
    app.include_router(create_router())
    app.state.live_approvals = {}
    app.state.research_jobs = {}
    app.state.research_latest_path = tmp_path / "latest.json"
    app.state.ai_proposal_registry_path = tmp_path / "ai_proposals.json"
    app.state.ops_exchange_init_error = None
    app.state.ops_standalone = False
    app.state.ops_enabled = True
    return app


@pytest.fixture
def client(ops_app: FastAPI):
    with TestClient(ops_app) as test_client:
        yield test_client


@pytest.fixture
def ops_headers() -> dict[str, str]:
    return {
        "X-OPS-TOKEN": "test-token",
        "X-OPS-CALLER": "pytest",
    }
