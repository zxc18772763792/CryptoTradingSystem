from __future__ import annotations

import asyncio
from types import SimpleNamespace

from fastapi import HTTPException


def _request():
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                ml_jobs={},
                ml_models={},
                ml_job_tasks={},
            )
        )
    )


class _FakeBackend:
    def __init__(self):
        self.calls = []

    async def train_model(self, *, request, payload, job):
        self.calls.append(("train", job["status"], payload.model_name))
        assert job["status"] == "running"
        return {
            "model_id": "model-1",
            "evaluation": {"accuracy": 0.91},
            "artifact_path": "artifacts/model-1.json",
        }

    async def factorize_model(self, *, request, model_id, payload, job=None):
        self.calls.append(("factorize", (job or {}).get("status"), model_id))
        return {"model_id": model_id, "factors": ["momentum", "volatility"]}

    async def register_model(self, *, request, model_id, payload, model):
        self.calls.append(("register", model_id))
        return {"registered": True, "name": payload.name or model_id}

    async def deploy_paper_model(self, *, request, model_id, payload, model):
        self.calls.append(("deploy_paper", model_id, payload.allocation_pct))
        return {"deployed": True, "allocation_pct": payload.allocation_pct}

    async def oneclick(self, *, request, payload, job):
        self.calls.append(("oneclick", job["status"], payload.model_name))
        return {"model_id": "model-2"}


def test_train_job_transitions_and_persists_model(monkeypatch):
    from web.api import ml as ml_module

    request = _request()
    backend = _FakeBackend()
    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: backend)

    payload = ml_module.MLTrainRequest(
        model_name="demo",
        model_id="model-1",
        symbols=["BTC/USDT"],
        timeframes=["1h"],
        background=False,
        factorize=True,
    )
    result = asyncio.run(ml_module.train_job(request, payload))

    assert result["status"] == "completed"
    assert result["progress"]["phase"] == "completed"
    assert request.app.state.ml_jobs[result["job_id"]]["status"] == "completed"
    assert request.app.state.ml_models["model-1"]["status"] == "evaluated"
    assert backend.calls[0][0] == "train"
    assert backend.calls[1][0] == "factorize"

    diag = asyncio.run(ml_module.diagnostics(request))
    assert diag["ok"] is True
    assert diag["job_counts"]["completed"] == 1
    assert diag["models_total"] == 1

    listed = asyncio.run(ml_module.list_jobs(request))
    assert listed["count"] == 1
    assert listed["items"][0]["job_id"] == result["job_id"]

    fetched = asyncio.run(ml_module.get_job(request, result["job_id"]))
    assert fetched["status"] == "completed"


def test_register_deploy_factorize_and_oneclick(monkeypatch):
    from web.api import ml as ml_module

    request = _request()
    backend = _FakeBackend()
    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: backend)
    request.app.state.ml_models["model-1"] = {
        "model_id": "model-1",
        "name": "demo",
        "status": "trained",
    }

    registered = asyncio.run(
        ml_module.register_model(
            request,
            "model-1",
            ml_module.MLRegisterRequest(name="registered-demo", metadata={"source": "test"}),
        )
    )
    assert registered["status"] == "registered"
    assert registered["registration"]["registered"] is True

    deployed = asyncio.run(
        ml_module.deploy_model_paper(
            request,
            "model-1",
            ml_module.MLDeployPaperRequest(allocation_pct=0.12),
        )
    )
    assert deployed["status"] == "paper_deployed"
    assert deployed["paper_deploy"]["allocation_pct"] == 0.12

    factorized = asyncio.run(
        ml_module.factorize_model(
            request,
            "model-1",
            ml_module.MLFactorizeRequest(symbols=["BTC/USDT"], timeframes=["4h"]),
        )
    )
    assert factorized["status"] == "factorized"
    assert factorized["factorization"]["factors"] == ["momentum", "volatility"]

    oneclick = asyncio.run(
        ml_module.oneclick(
            request,
            ml_module.MLOneClickRequest(
                model_name="oneclick-demo",
                model_id="model-2",
                background=False,
            ),
        )
    )
    assert oneclick["status"] == "completed"
    assert request.app.state.ml_models["model-2"]["status"] == "completed"
    assert any(call[0] == "oneclick" for call in backend.calls)


def test_failed_train_returns_readable_error_and_missing_job_404(monkeypatch):
    from web.api import ml as ml_module

    request = _request()

    class _BrokenBackend:
        async def train_model(self, **_kwargs):
            raise ValueError("insufficient data for training")

    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: _BrokenBackend())

    payload = ml_module.MLTrainRequest(model_name="broken", background=False)
    result = asyncio.run(ml_module.train_job(request, payload))
    assert result["status"] == "failed"
    assert "insufficient data" in result["error"]
    assert result["progress"]["phase"] == "failed"

    try:
        asyncio.run(ml_module.get_job(request, "missing-job"))
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 404
        assert "missing-job" in str(exc.detail)
