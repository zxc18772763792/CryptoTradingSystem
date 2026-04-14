from __future__ import annotations

import asyncio
from types import SimpleNamespace

from fastapi import HTTPException
import pandas as pd


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


def test_core_factorize_uses_real_loader_signature(monkeypatch, tmp_path):
    from web.api import ml as ml_module

    request = _request()
    request.app.state.ml_models["model-1"] = {
        "model_id": "model-1",
        "name": "demo",
        "status": "trained",
        "symbol": "BTC/USDT",
        "timeframe": "1h",
        "artifact": {
            "model_path": str(tmp_path / "model.json"),
            "exchange": "binance",
        },
    }

    captured = {}

    class _FakeSignalModel:
        def is_loaded(self):
            return True

        def predict(self, features, symbol=""):
            return SimpleNamespace(
                long_prob=0.72,
                short_prob=0.28,
                confidence=0.72,
                model_version="fake_v1",
                direction="LONG",
            )

    def fake_load_from_path(*, path: str, threshold: float = 0.55):
        captured["path"] = path
        captured["threshold"] = threshold
        return _FakeSignalModel()

    async def fake_load_klines_from_parquet(**_kwargs):
        return pd.DataFrame(
            {
                "open": [100.0 + i for i in range(30)],
                "high": [101.0 + i for i in range(30)],
                "low": [99.0 + i for i in range(30)],
                "close": [100.5 + i for i in range(30)],
                "volume": [1000.0 + i for i in range(30)],
            }
        )

    monkeypatch.setattr(ml_module, "_FACTOR_ROOT", tmp_path)
    monkeypatch.setattr(ml_module.MLSignalModel, "load_from_path", fake_load_from_path)
    monkeypatch.setattr(ml_module.data_storage, "load_klines_from_parquet", fake_load_klines_from_parquet)

    result = asyncio.run(
        ml_module.factorize_model(
            request,
            "model-1",
            ml_module.MLFactorizeRequest(symbols=["BTC/USDT"], timeframes=["1h"]),
        )
    )

    assert result["status"] == "factorized"
    assert result["factorization"]["factor_count"] == 1
    assert captured["path"].endswith("model.json")
    assert captured["threshold"] == 0.55


def test_core_deploy_paper_preserves_exchange_and_threshold(monkeypatch, tmp_path):
    from web.api import ml as ml_module

    request = _request()
    request.app.state.ml_models["model-1"] = {
        "model_id": "model-1",
        "name": "demo",
        "status": "registered",
        "symbol": "BTC/USDT",
        "timeframe": "4h",
        "artifact": {
            "model_path": str(tmp_path / "model.json"),
            "exchange": "binance",
            "manifest": {
                "metrics": {"prediction_threshold": 0.67},
                "training_window": {"exchange": "binance"},
            },
        },
    }

    captured = {}

    monkeypatch.setattr(ml_module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(ml_module.strategy_manager, "get_strategy", lambda name: None)

    def fake_register_strategy(**kwargs):
        captured["register"] = kwargs
        return True

    async def fake_start_strategy(name):
        captured["started"] = name
        return True

    async def fake_persist_strategy_snapshot(name, state_override=None):
        captured["persisted"] = (name, state_override)
        return True

    monkeypatch.setattr(ml_module.strategy_manager, "register_strategy", fake_register_strategy)
    monkeypatch.setattr(ml_module.strategy_manager, "start_strategy", fake_start_strategy)
    monkeypatch.setattr(ml_module, "persist_strategy_snapshot", fake_persist_strategy_snapshot)
    monkeypatch.setattr(ml_module, "_upsert_registry", lambda model_id, patch: {"model_id": model_id, **patch})

    result = asyncio.run(
        ml_module.deploy_model_paper(
            request,
            "model-1",
            ml_module.MLDeployPaperRequest(allocation_pct=0.15),
        )
    )

    assert result["status"] == "paper_deployed"
    assert result["paper_deploy"]["allocation_pct"] == 0.15
    assert captured["register"]["params"]["model_path"].endswith("model.json")
    assert captured["register"]["params"]["threshold"] == 0.67
    assert captured["register"]["params"]["exchange"] == "binance"
    assert captured["register"]["timeframe"] == "4h"
    assert captured["started"].startswith("ML_XGB_")
    assert captured["persisted"][1] == "running"


def test_background_train_job_cleans_up_task(monkeypatch):
    from web.api import ml as ml_module

    request = _request()
    backend = _FakeBackend()
    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: backend)

    async def _scenario():
        queued = await ml_module.train_job(
            request,
            ml_module.MLTrainRequest(model_name="demo-bg", background=True, factorize=False),
        )
        job_id = queued["job_id"]
        task = request.app.state.ml_job_tasks[job_id]
        await task
        return job_id

    job_id = asyncio.run(_scenario())

    assert request.app.state.ml_jobs[job_id]["status"] == "completed"
    assert job_id not in request.app.state.ml_job_tasks


def test_background_oneclick_job_cleans_up_task(monkeypatch):
    from web.api import ml as ml_module

    request = _request()
    backend = _FakeBackend()
    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: backend)

    async def _scenario():
        queued = await ml_module.oneclick(
            request,
            ml_module.MLOneClickRequest(model_name="oneclick-bg", model_id="model-bg", background=True),
        )
        job_id = queued["job_id"]
        task = request.app.state.ml_job_tasks[job_id]
        await task
        return job_id

    job_id = asyncio.run(_scenario())

    assert request.app.state.ml_jobs[job_id]["status"] == "completed"
    assert request.app.state.ml_models["model-2"]["status"] == "completed"
    assert job_id not in request.app.state.ml_job_tasks


def test_deploy_paper_can_restore_model_from_registry_after_restart(monkeypatch):
    from web.api import ml as ml_module

    request = _request()
    request.app.state.ml_models = {}

    registry_entry = {
        "model_id": "model-r1",
        "name": "registry-demo",
        "status": "registered",
        "model_path": "artifacts/model-r1/model.json",
        "manifest": {
            "symbol": "ETH/USDT",
            "timeframe": "1h",
            "metrics": {"prediction_threshold": 0.63},
            "training_window": {"exchange": "binance"},
        },
        "strategy_defaults": {
            "strategy_class": "MLXGBoostStrategy",
            "params": {
                "model_path": "artifacts/model-r1/model.json",
                "threshold": 0.63,
                "exchange": "binance",
            },
        },
        "metadata": {"source": "registry"},
    }

    captured = {}
    monkeypatch.setattr(ml_module, "_load_registry", lambda: {"model-r1": registry_entry})
    monkeypatch.setattr(ml_module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(ml_module.strategy_manager, "get_strategy", lambda name: None)
    monkeypatch.setattr(ml_module, "_upsert_registry", lambda model_id, patch: {"model_id": model_id, **patch})

    def fake_register_strategy(**kwargs):
        captured["register"] = kwargs
        return True

    async def fake_start_strategy(name):
        captured["started"] = name
        return True

    async def fake_persist_strategy_snapshot(name, state_override=None):
        captured["persisted"] = (name, state_override)
        return True

    monkeypatch.setattr(ml_module.strategy_manager, "register_strategy", fake_register_strategy)
    monkeypatch.setattr(ml_module.strategy_manager, "start_strategy", fake_start_strategy)
    monkeypatch.setattr(ml_module, "persist_strategy_snapshot", fake_persist_strategy_snapshot)

    result = asyncio.run(
        ml_module.deploy_model_paper(
            request,
            "model-r1",
            ml_module.MLDeployPaperRequest(allocation_pct=0.2),
        )
    )

    assert result["status"] == "paper_deployed"
    assert captured["register"]["params"]["threshold"] == 0.63
    assert captured["register"]["params"]["exchange"] == "binance"
    assert captured["register"]["symbols"] == ["ETH/USDT"]
    assert request.app.state.ml_models["model-r1"]["artifact"]["model_path"].endswith("model.json")


def test_factorize_can_restore_model_from_registry_after_restart(monkeypatch, tmp_path):
    from web.api import ml as ml_module

    request = _request()
    request.app.state.ml_models = {}

    registry_entry = {
        "model_id": "model-r2",
        "name": "registry-factorize",
        "status": "registered",
        "model_path": str(tmp_path / "model.json"),
        "manifest": {
            "symbol": "BTC/USDT",
            "timeframe": "4h",
            "training_window": {"exchange": "binance"},
        },
    }
    captured = {}

    class _FakeSignalModel:
        def is_loaded(self):
            return True

        def predict(self, features, symbol=""):
            captured["symbol"] = symbol
            return SimpleNamespace(
                long_prob=0.71,
                short_prob=0.29,
                confidence=0.71,
                model_version="fake_v1",
                direction="LONG",
            )

    async def fake_load_klines_from_parquet(**_kwargs):
        return pd.DataFrame(
            {
                "open": [100.0 + i for i in range(30)],
                "high": [101.0 + i for i in range(30)],
                "low": [99.0 + i for i in range(30)],
                "close": [100.5 + i for i in range(30)],
                "volume": [1000.0 + i for i in range(30)],
            }
        )

    monkeypatch.setattr(ml_module, "_load_registry", lambda: {"model-r2": registry_entry})
    monkeypatch.setattr(ml_module, "_FACTOR_ROOT", tmp_path)
    monkeypatch.setattr(ml_module.MLSignalModel, "load_from_path", lambda *, path, threshold=0.55: _FakeSignalModel())
    monkeypatch.setattr(ml_module.data_storage, "load_klines_from_parquet", fake_load_klines_from_parquet)
    monkeypatch.setattr(ml_module, "_upsert_registry", lambda model_id, patch: {"model_id": model_id, **patch})

    result = asyncio.run(
        ml_module.factorize_model(
            request,
            "model-r2",
            ml_module.MLFactorizeRequest(),
        )
    )

    assert result["status"] == "factorized"
    assert result["factorization"]["factor_count"] == 1
    assert captured["symbol"] == "BTC/USDT"
    assert request.app.state.ml_models["model-r2"]["timeframe"] == "4h"


def test_core_register_persists_strategy_defaults_and_market_identity(monkeypatch, tmp_path):
    from web.api import ml as ml_module

    request = _request()
    request.app.state.ml_models["model-reg"] = {
        "model_id": "model-reg",
        "name": "demo",
        "status": "trained",
        "symbol": "SOL/USDT",
        "timeframe": "4h",
        "exchange": "binance",
        "artifact": {
            "model_path": str(tmp_path / "model.json"),
            "exchange": "binance",
            "manifest": {
                "symbol": "SOL/USDT",
                "timeframe": "4h",
                "metrics": {"prediction_threshold": 0.66},
                "training_window": {"exchange": "binance"},
            },
        },
    }

    captured = {}
    monkeypatch.setattr(ml_module, "_upsert_registry", lambda model_id, patch: captured.setdefault("patch", {"model_id": model_id, **patch}))

    result = asyncio.run(
        ml_module.register_model(
            request,
            "model-reg",
            ml_module.MLRegisterRequest(name="registered-demo"),
        )
    )

    assert result["status"] == "registered"
    assert captured["patch"]["symbol"] == "SOL/USDT"
    assert captured["patch"]["timeframe"] == "4h"
    assert captured["patch"]["exchange"] == "binance"
    assert captured["patch"]["strategy_defaults"]["params"]["threshold"] == 0.66
    assert captured["patch"]["strategy_defaults"]["params"]["exchange"] == "binance"


def test_oneclick_model_status_reflects_deployment_result(monkeypatch):
    from web.api import ml as ml_module

    request = _request()

    class _OneClickDeployBackend:
        async def oneclick(self, *, request, payload, job):
            return {
                "model_id": "model-oneclick",
                "registration": {"registered": True},
                "deployment": {"deployed": True},
            }

    monkeypatch.setattr(ml_module, "_resolve_backend", lambda _request: _OneClickDeployBackend())

    result = asyncio.run(
        ml_module.oneclick(
            request,
            ml_module.MLOneClickRequest(model_name="deploying-model", model_id="model-oneclick", background=False),
        )
    )

    assert result["status"] == "completed"
    assert request.app.state.ml_models["model-oneclick"]["status"] == "paper_deployed"
