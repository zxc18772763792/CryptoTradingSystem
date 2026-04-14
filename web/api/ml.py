"""ML API router for training, registration, deployment, and factorization."""
from __future__ import annotations

import asyncio
import inspect
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from core.ai.ml_signal import MLSignalModel, build_feature_frame
from core.data import data_storage
from core.ml.pipeline import (
    MANIFEST_FILE_NAME,
    MODEL_FILE_NAME,
    PipelineError,
    diagnose_environment,
    run_signal_training_pipeline,
)
from core.strategies import strategy_manager
from core.strategies.persistence import persist_strategy_snapshot
from core.trading.execution_engine import execution_engine
from strategies.ai.ml_xgboost_strategy import MLXGBoostStrategy

router = APIRouter()

ML_JOB_STATUSES = {"queued", "running", "evaluating", "completed", "failed"}
_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT_ROOT = _ROOT / "models" / "ml_signal_xgb"
_REGISTRY_PATH = _ROOT / "runtime" / "ml" / "model_registry.json"
_FACTOR_ROOT = _ROOT / "runtime" / "ml" / "factors"


class MLTrainRequest(BaseModel):
    model_name: str = Field(default="ml_signal")
    model_id: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list)
    training_window_days: int = Field(default=365, ge=30, le=3650)
    background: bool = True
    factorize: bool = True
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MLRegisterRequest(BaseModel):
    name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MLDeployPaperRequest(BaseModel):
    allocation_pct: float = Field(default=0.05, ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MLFactorizeRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list)
    lookback_days: int = Field(default=30, ge=5, le=3650)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MLOneClickRequest(BaseModel):
    model_name: str = Field(default="ml_signal")
    model_id: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list)
    training_window_days: int = Field(default=365, ge=30, le=3650)
    allocation_pct: float = Field(default=0.05, ge=0.0, le=1.0)
    background: bool = True
    register_model: bool = True
    deploy_paper: bool = True
    factorize: bool = True
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _state(app: Any) -> Any:
    return getattr(app, "state", app)


def _ensure_ml_state(app: Any) -> None:
    state = _state(app)
    if getattr(state, "ml_jobs", None) is None:
        state.ml_jobs = {}
    if getattr(state, "ml_models", None) is None:
        state.ml_models = {}
    if getattr(state, "ml_job_tasks", None) is None:
        state.ml_job_tasks = {}
    if getattr(state, "ml_diagnostics", None) is None:
        state.ml_diagnostics = {}


def _job_id(prefix: str = "ml") -> str:
    return f"{prefix}-{int(_now_utc().timestamp())}-{secrets.token_hex(4)}"


def _normalize_status(status: Any) -> str:
    value = str(status or "").strip().lower()
    return value if value in ML_JOB_STATUSES else "failed"


def _job_message(status: str) -> str:
    return {
        "queued": "Job queued and waiting to start.",
        "running": "Training or registration is in progress.",
        "evaluating": "Model evaluation is in progress.",
        "completed": "Job completed successfully.",
        "failed": "Job failed. Check the error message for details.",
    }.get(status, "Unknown job state.")


def _job_snapshot(job: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(job or {})
    status = _normalize_status(payload.get("status"))
    progress = dict(payload.get("progress") or {})
    progress.setdefault("phase", status)
    progress.setdefault("message", _job_message(status))
    payload["status"] = status
    payload["progress"] = progress
    return payload


def _list_jobs(state: Any) -> List[Dict[str, Any]]:
    jobs = list((getattr(state, "ml_jobs", {}) or {}).values())
    return sorted(
        (_job_snapshot(dict(job)) for job in jobs),
        key=lambda item: str(item.get("created_at") or ""),
        reverse=True,
    )


def _model_snapshot(model: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(model or {})
    payload.setdefault("status", "unregistered")
    return payload


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _readable_error(exc: Exception, *, context: str) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    if context and context.lower() not in message.lower():
        return f"{context}: {message}"
    return message


def _normalize_symbol(symbols: Iterable[str]) -> str:
    for item in symbols:
        value = str(item or "").strip().upper()
        if value:
            return value
    return "BTC/USDT"


def _normalize_timeframe(timeframes: Iterable[str]) -> str:
    for item in timeframes:
        value = str(item or "").strip().lower()
        if value:
            return value
    return "1h"


def _safe_strategy_suffix(raw: str) -> str:
    chars: List[str] = []
    for ch in str(raw or "").lower():
        if ch.isalnum():
            chars.append(ch)
    text = "".join(chars)
    return text[:24] if text else secrets.token_hex(4)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _load_registry() -> Dict[str, Dict[str, Any]]:
    payload = _load_json(_REGISTRY_PATH, {"models": []})
    rows = payload.get("models") if isinstance(payload, dict) else []
    items: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        model_id = str(row.get("model_id") or "").strip()
        if not model_id:
            continue
        items[model_id] = dict(row)
    return items


def _save_registry(items: Dict[str, Dict[str, Any]]) -> None:
    rows = list(items.values())
    rows.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
    _save_json(_REGISTRY_PATH, {"models": rows})


def _upsert_registry(model_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    items = _load_registry()
    now_iso = _now_utc().isoformat()
    current = dict(items.get(model_id) or {})
    if not current:
        current["model_id"] = model_id
        current["created_at"] = now_iso
    current.update(dict(patch or {}))
    current["model_id"] = model_id
    current["updated_at"] = now_iso
    items[model_id] = current
    _save_registry(items)
    return current


def _resolve_model_manifest(model: Dict[str, Any], registry_entry: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    artifact = dict(model.get("artifact") or {})
    manifest = dict(artifact.get("manifest") or {})
    if manifest:
        return manifest

    manifest_path = str(artifact.get("manifest_path") or "").strip()
    if manifest_path:
        loaded = _load_json(Path(manifest_path), {})
        if isinstance(loaded, dict):
            return loaded

    registry_entry = dict(registry_entry or {})
    loaded = registry_entry.get("manifest")
    if isinstance(loaded, dict):
        return dict(loaded)

    return {}


def _restore_model_from_registry(model_id: str, registry_entry: Dict[str, Any]) -> Dict[str, Any]:
    entry = dict(registry_entry or {})
    if not entry:
        return {}

    strategy_defaults = dict((entry.get("strategy_defaults") or {}).get("params") or {})
    manifest_path = str(entry.get("manifest_path") or "").strip()
    model_path = str(entry.get("model_path") or strategy_defaults.get("model_path") or "").strip()
    manifest = {}
    if isinstance(entry.get("manifest"), dict):
        manifest = dict(entry.get("manifest") or {})
    elif manifest_path:
        loaded = _load_json(Path(manifest_path), {})
        if isinstance(loaded, dict):
            manifest = loaded

    training_window = dict(manifest.get("training_window") or {})
    exchange = str(
        strategy_defaults.get("exchange")
        or entry.get("exchange")
        or training_window.get("exchange")
        or "binance"
    ).strip().lower() or "binance"

    model: Dict[str, Any] = {
        "model_id": model_id,
        "name": str(entry.get("name") or model_id),
        "status": str(entry.get("status") or "registered"),
        "created_at": str(entry.get("created_at") or _now_utc().isoformat()),
        "updated_at": str(entry.get("updated_at") or ""),
        "metadata": dict(entry.get("metadata") or {}),
        "symbol": str(entry.get("symbol") or manifest.get("symbol") or "BTC/USDT"),
        "timeframe": str(entry.get("timeframe") or manifest.get("timeframe") or "1h"),
        "exchange": exchange,
        "artifact": {
            "manifest": manifest,
            "manifest_path": manifest_path,
            "model_path": model_path,
            "exchange": exchange,
        },
    }
    if entry.get("strategy_defaults"):
        model["strategy_defaults"] = dict(entry.get("strategy_defaults") or {})
    if isinstance(entry.get("deployment"), dict):
        model["paper_deploy"] = dict(entry.get("deployment") or {})
    factor_snapshot_path = str(entry.get("factor_snapshot_path") or "").strip()
    if factor_snapshot_path:
        model["factorization"] = {
            "factor_snapshot_path": factor_snapshot_path,
            "factor_count": int(entry.get("factor_count") or 0),
        }
    return model


def _load_model_from_state_or_registry(
    state: Any,
    model_id: str,
    *,
    default_name: Optional[str] = None,
    allow_create: bool = False,
) -> Dict[str, Any]:
    models = getattr(state, "ml_models", {}) or {}
    state_model = dict(models.get(model_id) or {})
    registry_entry = _load_registry().get(model_id) or {}
    if state_model or registry_entry:
        restored = _restore_model_from_registry(model_id, registry_entry) if registry_entry else {}
        merged = dict(restored)
        merged.update(state_model)
        for key in ("artifact", "metadata", "registration", "paper_deploy", "factorization", "strategy_defaults"):
            if isinstance(restored.get(key), dict) or isinstance(state_model.get(key), dict):
                merged[key] = {**dict(restored.get(key) or {}), **dict(state_model.get(key) or {})}
        _save_model(state, merged)
        return merged
    if allow_create:
        return _get_or_create_model(state, model_id, default_name=default_name)
    return {}


class _CoreMLBackend:
    async def diagnostics(self, **_: Any) -> Dict[str, Any]:
        diagnostics = diagnose_environment().to_dict()
        registry = _load_registry()
        return {
            "environment": diagnostics,
            "artifact_root": str(_ARTIFACT_ROOT),
            "registry_path": str(_REGISTRY_PATH),
            "registry_models": len(registry),
        }

    async def train_model(self, *, payload: MLTrainRequest, **_: Any) -> Dict[str, Any]:
        symbol = _normalize_symbol(payload.symbols)
        timeframe = _normalize_timeframe(payload.timeframes)
        exchange = str(payload.parameters.get("exchange") or payload.metadata.get("exchange") or "binance").strip().lower() or "binance"
        end_at = _now_utc()
        start_at = end_at - timedelta(days=int(payload.training_window_days))
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_at,
            end_time=end_at,
        )
        if df is None or df.empty:
            raise RuntimeError(
                f"missing dataset for {exchange}:{symbol}:{timeframe}; "
                "run data sync first and retry"
            )

        params = dict(payload.parameters or {})
        run = await asyncio.to_thread(
            run_signal_training_pipeline,
            df=df,
            output_root=_ARTIFACT_ROOT,
            symbol=symbol,
            timeframe=timeframe,
            exchange=exchange,
            forward_bars=int(params.get("forward_bars", 4)),
            test_size=float(params.get("test_size", 0.2)),
            n_estimators=int(params.get("n_estimators", 300)),
            max_depth=int(params.get("max_depth", 5)),
            learning_rate=float(params.get("learning_rate", 0.05)),
            scale_pos_weight=float(params.get("scale_pos_weight", 0.0)),
            prediction_threshold=float(params.get("prediction_threshold", 0.55)),
            min_rows=int(params.get("min_rows", 120)),
        )

        manifest = dict(run.manifest)
        manifest_path = Path(run.artifact_dir) / MANIFEST_FILE_NAME
        model_path = Path(run.artifact_dir) / MODEL_FILE_NAME
        return {
            "model_id": str(manifest.get("model_id")),
            "artifact_dir": str(run.artifact_dir),
            "manifest_path": str(manifest_path),
            "model_path": str(model_path),
            "manifest": manifest,
            "metrics": dict(run.metrics),
            "gate": run.gate.to_dict(),
            "feature_importances": dict(run.feature_importances),
            "symbol": symbol,
            "timeframe": timeframe,
            "exchange": exchange,
        }

    async def register_model(
        self,
        *,
        model_id: str,
        payload: MLRegisterRequest,
        model: Dict[str, Any],
        **_: Any,
    ) -> Dict[str, Any]:
        artifact = dict(model.get("artifact") or {})
        manifest = dict(artifact.get("manifest") or {})
        if not manifest:
            manifest_path = str(artifact.get("manifest_path") or "").strip()
            if manifest_path:
                manifest = _load_json(Path(manifest_path), {})
        if not manifest:
            raise RuntimeError("manifest missing; train model first")

        model_path = str(artifact.get("model_path") or "")
        if not model_path:
            artifact_dir = str(artifact.get("artifact_dir") or "")
            if artifact_dir:
                model_path = str(Path(artifact_dir) / MODEL_FILE_NAME)
        if not model_path:
            raise RuntimeError("model path missing; cannot register")

        training_window = dict(manifest.get("training_window") or {})
        exchange = str(
            training_window.get("exchange")
            or artifact.get("exchange")
            or model.get("exchange")
            or "binance"
        ).strip().lower() or "binance"
        symbol = str(manifest.get("symbol") or model.get("symbol") or "BTC/USDT")
        timeframe = str(manifest.get("timeframe") or model.get("timeframe") or "1h")
        threshold = float((manifest.get("metrics") or {}).get("prediction_threshold", 0.55))

        entry = _upsert_registry(
            model_id,
            {
                "name": str(payload.name or model.get("name") or model_id),
                "status": "registered",
                "symbol": symbol,
                "timeframe": timeframe,
                "exchange": exchange,
                "manifest": manifest,
                "manifest_path": str(artifact.get("manifest_path") or ""),
                "model_path": model_path,
                "strategy_defaults": {
                    "strategy_class": "MLXGBoostStrategy",
                    "params": {
                        "model_path": model_path,
                        "threshold": threshold,
                        "exchange": exchange,
                    },
                },
                "metadata": {**dict(model.get("metadata") or {}), **dict(payload.metadata or {})},
            },
        )
        return {"registered": True, "registry_entry": entry}

    async def deploy_paper_model(
        self,
        *,
        model_id: str,
        payload: MLDeployPaperRequest,
        model: Dict[str, Any],
        **_: Any,
    ) -> Dict[str, Any]:
        if execution_engine.get_trading_mode() != "paper":
            current = execution_engine.get_trading_mode()
            raise RuntimeError(f"deployment requires paper mode, current={current}")

        artifact = dict(model.get("artifact") or {})
        registry = _load_registry().get(model_id) or {}
        manifest = _resolve_model_manifest(model, registry)
        registry_strategy_defaults = dict((registry.get("strategy_defaults") or {}).get("params") or {})
        model_path = str(artifact.get("model_path") or "").strip()
        if not model_path:
            artifact_dir = str(artifact.get("artifact_dir") or "").strip()
            if artifact_dir:
                model_path = str(Path(artifact_dir) / MODEL_FILE_NAME)
        if not model_path:
            model_path = str(registry.get("model_path") or registry_strategy_defaults.get("model_path") or "").strip()
        if not model_path:
            raise RuntimeError("model path missing; register model first")

        training_window = dict(manifest.get("training_window") or {})
        threshold = float(
            registry_strategy_defaults.get("threshold")
            or (manifest.get("metrics") or {}).get("prediction_threshold")
            or 0.55
        )
        exchange = str(
            registry_strategy_defaults.get("exchange")
            or artifact.get("exchange")
            or model.get("exchange")
            or training_window.get("exchange")
            or "binance"
        ).strip().lower() or "binance"

        strategy_name = f"ML_XGB_{_safe_strategy_suffix(model_id)}"
        if strategy_manager.get_strategy(strategy_name):
            strategy_name = f"{strategy_name}_{secrets.token_hex(2)}"

        ok = strategy_manager.register_strategy(
            name=strategy_name,
            strategy_class=MLXGBoostStrategy,
            params={
                "model_path": model_path,
                "threshold": threshold,
                "exchange": exchange,
            },
            symbols=[str(model.get("symbol") or "BTC/USDT")],
            timeframe=str(model.get("timeframe") or "1h"),
            allocation=float(payload.allocation_pct),
            metadata={
                "source": "ml_api",
                "model_id": model_id,
                "lineage": {"model_id": model_id, "model_path": model_path},
                **dict(payload.metadata or {}),
            },
        )
        if not ok:
            raise RuntimeError(f"strategy registration failed: {strategy_name}")
        started = await strategy_manager.start_strategy(strategy_name)
        if not started:
            raise RuntimeError(f"strategy start failed: {strategy_name}")
        await persist_strategy_snapshot(strategy_name, state_override="running")

        _upsert_registry(
            model_id,
            {
                "status": "paper_deployed",
                "deployment": {
                    "strategy_name": strategy_name,
                    "allocation_pct": float(payload.allocation_pct),
                    "deployed_at": _now_utc().isoformat(),
                    "trading_mode": execution_engine.get_trading_mode(),
                    "exchange": exchange,
                    "threshold": threshold,
                },
            },
        )
        return {
            "deployed": True,
            "strategy_name": strategy_name,
            "allocation_pct": float(payload.allocation_pct),
            "trading_mode": execution_engine.get_trading_mode(),
            "exchange": exchange,
            "threshold": threshold,
        }

    async def factorize_model(
        self,
        *,
        model_id: str,
        payload: MLFactorizeRequest,
        model: Dict[str, Any],
        **_: Any,
    ) -> Dict[str, Any]:
        artifact = dict(model.get("artifact") or {})
        model_path = str(artifact.get("model_path") or "").strip()
        if not model_path:
            artifact_dir = str(artifact.get("artifact_dir") or "").strip()
            if artifact_dir:
                model_path = str(Path(artifact_dir) / MODEL_FILE_NAME)
        if not model_path:
            registry = _load_registry().get(model_id) or {}
            model_path = str(registry.get("model_path") or "").strip()
        if not model_path:
            raise RuntimeError("model path missing; cannot factorize")

        signal_model = MLSignalModel.load_from_path(path=model_path, threshold=0.55)
        if not signal_model.is_loaded():
            raise RuntimeError("model is not loadable; check xgboost/model artifact")

        symbols = list(payload.symbols or [str(model.get("symbol") or "BTC/USDT")])
        timeframes = list(payload.timeframes or [str(model.get("timeframe") or "1h")])
        exchange = str((artifact.get("exchange") or model.get("exchange") or "binance")).strip().lower() or "binance"
        end_at = _now_utc()
        start_at = end_at - timedelta(days=int(payload.lookback_days))

        factors: List[Dict[str, Any]] = []
        for symbol in symbols:
            for timeframe in timeframes:
                df = await data_storage.load_klines_from_parquet(
                    exchange=exchange,
                    symbol=str(symbol),
                    timeframe=str(timeframe),
                    start_time=start_at,
                    end_time=end_at,
                )
                if df is None or df.empty or len(df) < 20:
                    continue
                features = build_feature_frame(df)
                latest = signal_model.predict(features, symbol=str(symbol))
                previous_score = 0.0
                if len(features) > 1:
                    previous = signal_model.predict(features.iloc[:-1], symbol=str(symbol))
                    previous_score = float(previous.long_prob - previous.short_prob)
                score = float(latest.long_prob - latest.short_prob)
                factors.append(
                    {
                        "symbol": str(symbol),
                        "timeframe": str(timeframe),
                        "ml_score": round(score, 6),
                        "ml_score_delta": round(score - previous_score, 6),
                        "ml_long_prob": round(float(latest.long_prob), 6),
                        "ml_regime_score": round(score * float(latest.confidence or 0.0), 6),
                        "model_version": str(latest.model_version or ""),
                        "direction": str(latest.direction or "FLAT"),
                    }
                )

        if not factors:
            raise RuntimeError("factorization produced no rows; verify data availability")

        _FACTOR_ROOT.mkdir(parents=True, exist_ok=True)
        factor_path = _FACTOR_ROOT / f"{model_id}_{int(_now_utc().timestamp())}.json"
        _save_json(
            factor_path,
            {
                "model_id": model_id,
                "generated_at": _now_utc().isoformat(),
                "rows": factors,
                "metadata": dict(payload.metadata or {}),
            },
        )
        _upsert_registry(
            model_id,
            {
                "status": "factorized",
                "factor_snapshot_path": str(factor_path),
                "factor_count": len(factors),
            },
        )
        return {"factor_snapshot_path": str(factor_path), "factor_count": len(factors), "rows": factors}

    async def oneclick(self, *, payload: MLOneClickRequest, request: Request, **_: Any) -> Dict[str, Any]:
        train_payload = MLTrainRequest(
            model_name=payload.model_name,
            model_id=payload.model_id,
            symbols=list(payload.symbols or []),
            timeframes=list(payload.timeframes or []),
            training_window_days=int(payload.training_window_days),
            background=False,
            factorize=False,
            parameters=dict(payload.parameters or {}),
            metadata=dict(payload.metadata or {}),
        )
        train_result = await self.train_model(payload=train_payload)
        model_id = str(train_result.get("model_id") or payload.model_id or "")
        if not model_id:
            raise RuntimeError("oneclick failed to resolve model_id")
        state = _state(request.app)
        model = _get_or_create_model(state, model_id, default_name=payload.model_name)
        model["artifact"] = train_result
        model["symbol"] = _normalize_symbol(payload.symbols)
        model["timeframe"] = _normalize_timeframe(payload.timeframes)
        _save_model(state, model)

        registration = None
        if payload.register_model:
            registration = await self.register_model(
                model_id=model_id,
                payload=MLRegisterRequest(name=payload.model_name, metadata=dict(payload.metadata or {})),
                model=model,
            )
        deployment = None
        if payload.deploy_paper:
            deployment = await self.deploy_paper_model(
                model_id=model_id,
                payload=MLDeployPaperRequest(allocation_pct=float(payload.allocation_pct), metadata=dict(payload.metadata or {})),
                model=model,
            )
        factorization = None
        if payload.factorize:
            factorization = await self.factorize_model(
                model_id=model_id,
                payload=MLFactorizeRequest(
                    symbols=list(payload.symbols or []),
                    timeframes=list(payload.timeframes or []),
                    lookback_days=max(7, min(int(payload.training_window_days), 90)),
                    metadata=dict(payload.metadata or {}),
                ),
                model=model,
            )

        return {
            "model_id": model_id,
            "training": train_result,
            "registration": registration,
            "deployment": deployment,
            "factorization": factorization,
        }


def _resolve_backend(request: Request) -> Any:
    state = _state(request.app)
    backend = getattr(state, "ml_pipeline", None)
    if backend is not None:
        return backend
    return _CoreMLBackend()


def _backend_capabilities(backend: Any) -> Dict[str, bool]:
    return {
        "train": callable(getattr(backend, "train_model", None)) or callable(getattr(backend, "train", None)),
        "register": callable(getattr(backend, "register_model", None)),
        "deploy_paper": callable(getattr(backend, "deploy_paper_model", None)),
        "factorize": callable(getattr(backend, "factorize_model", None)),
        "oneclick": callable(getattr(backend, "oneclick", None)),
    }


def _backend_name(backend: Any) -> str:
    if backend is None:
        return "unavailable"
    module = getattr(backend, "__module__", "")
    qualname = getattr(backend, "__class__", type(backend)).__name__
    if module and module != "builtins":
        return f"{module}.{qualname}"
    return qualname


def _get_or_create_model(state: Any, model_id: str, *, default_name: Optional[str] = None) -> Dict[str, Any]:
    models = getattr(state, "ml_models", {})
    model = dict(models.get(model_id) or {})
    if not model:
        model = {
            "model_id": model_id,
            "name": default_name or model_id,
            "status": "unregistered",
            "created_at": _now_utc().isoformat(),
        }
    model["model_id"] = model_id
    model.setdefault("name", default_name or model_id)
    return model


def _save_model(state: Any, model: Dict[str, Any]) -> Dict[str, Any]:
    model = dict(model)
    model_id = str(model.get("model_id") or "").strip()
    if not model_id:
        raise ValueError("model_id is required")
    model["model_id"] = model_id
    models = getattr(state, "ml_models", {})
    models[model_id] = model
    state.ml_models = models
    return model


def _new_job(state: Any, *, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = _job_id("ml-job")
    job = {
        "job_id": job_id,
        "job_type": job_type,
        "status": "queued",
        "created_at": _now_utc().isoformat(),
        "started_at": None,
        "finished_at": None,
        "updated_at": _now_utc().isoformat(),
        "request": payload,
        "result": None,
        "error": None,
        "progress": {"phase": "queued", "message": _job_message("queued")},
    }
    state.ml_jobs[job_id] = job
    return job


def _set_job(
    job: Dict[str, Any],
    status: str,
    *,
    phase: Optional[str] = None,
    message: Optional[str] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    normalized = _normalize_status(status)
    job["status"] = normalized
    job["updated_at"] = _now_utc().isoformat()
    if normalized == "running" and not job.get("started_at"):
        job["started_at"] = _now_utc().isoformat()
    if normalized in {"completed", "failed"}:
        job["finished_at"] = _now_utc().isoformat()
    progress = dict(job.get("progress") or {})
    progress["phase"] = phase or normalized
    progress["message"] = message or _job_message(normalized)
    if error is not None:
        job["error"] = error
    job["progress"] = progress
    return job


async def _backend_call(backend: Any, *names: str, **kwargs: Any) -> Any:
    for name in names:
        func = getattr(backend, name, None)
        if callable(func):
            try:
                signature = inspect.signature(func)
                if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
                    call_kwargs = dict(kwargs)
                else:
                    call_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
            except Exception:
                call_kwargs = dict(kwargs)
            return await _maybe_await(func(**call_kwargs))
    raise AttributeError(f"backend does not expose any of: {', '.join(names)}")


async def _run_train_workflow(request: Request, job_id: str, payload: MLTrainRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    job = state.ml_jobs.get(job_id) or {}
    _set_job(job, "running", phase="training", message="Collecting dataset and fitting model.")
    state.ml_jobs[job_id] = job

    try:
        backend = _resolve_backend(request)
        result = await _backend_call(backend, "train_model", "train", request=request, payload=payload, job=job)
        result = dict(result or {})
        model_id = str(result.get("model_id") or payload.model_id or job_id).strip()
        model = _get_or_create_model(state, model_id, default_name=payload.model_name)
        model.update(
            {
                "model_id": model_id,
                "name": payload.model_name,
                "status": "trained",
                "trained_at": _now_utc().isoformat(),
                "symbol": _normalize_symbol(payload.symbols),
                "timeframe": _normalize_timeframe(payload.timeframes),
                "training_window_days": int(payload.training_window_days),
                "parameters": dict(payload.parameters or {}),
                "metadata": dict(payload.metadata or {}),
                "artifact": result,
            }
        )
        _save_model(state, model)
        _set_job(job, "evaluating", phase="evaluating", message="Evaluating model outputs and optional factors.")
        state.ml_jobs[job_id] = job

        factorization: Dict[str, Any] = {}
        if payload.factorize:
            factorization = dict(
                await _backend_call(
                    backend,
                    "factorize_model",
                    "factorize",
                    request=request,
                    model_id=model_id,
                    payload=MLFactorizeRequest(
                        symbols=list(payload.symbols or []),
                        timeframes=list(payload.timeframes or []),
                        lookback_days=max(7, min(int(payload.training_window_days), 120)),
                        metadata=dict(payload.metadata or {}),
                    ),
                    model=model,
                )
                or {}
            )

        gate = dict(result.get("gate") or {})
        metrics = dict(result.get("metrics") or {})
        model["gate"] = gate
        model["metrics"] = metrics
        model["factorization"] = factorization
        model["status"] = "evaluated"
        model["evaluated_at"] = _now_utc().isoformat()
        _save_model(state, model)

        _set_job(job, "completed", phase="completed", message="Model training completed.")
        job["result"] = {
            "model_id": model_id,
            "model": _model_snapshot(model),
            "gate": gate,
            "metrics": metrics,
            "factorization": factorization,
            "backend": _backend_name(backend),
        }
        state.ml_jobs[job_id] = job
        return _job_snapshot(job)
    except PipelineError as exc:
        if exc.stage == "gate" and isinstance(exc.details, dict):
            manifest_path_raw = str(exc.details.get("manifest_path") or "").strip()
            manifest = _load_json(Path(manifest_path_raw), {}) if manifest_path_raw else {}
            model_id = str(manifest.get("model_id") or "").strip()
            if model_id:
                archived_model = _get_or_create_model(state, model_id, default_name=payload.model_name)
                archived_model.update(
                    {
                        "status": "archived_gate_failed",
                        "trained_at": _now_utc().isoformat(),
                        "symbol": _normalize_symbol(payload.symbols),
                        "timeframe": _normalize_timeframe(payload.timeframes),
                        "training_window_days": int(payload.training_window_days),
                        "parameters": dict(payload.parameters or {}),
                        "metadata": dict(payload.metadata or {}),
                        "artifact": {
                            "artifact_dir": str(exc.details.get("artifact_dir") or ""),
                            "manifest_path": manifest_path_raw,
                            "manifest": manifest,
                            "metrics": dict(exc.details.get("metrics") or {}),
                            "gate": {
                                "passed": False,
                                "reasons": list(exc.details.get("reasons") or []),
                                "thresholds": dict(exc.details.get("thresholds") or {}),
                            },
                        },
                    }
                )
                _save_model(state, archived_model)
        error = _readable_error(exc, context="ML training failed")
        _set_job(job, "failed", phase="failed", message=error, error=error)
        state.ml_jobs[job_id] = job
        return _job_snapshot(job)
    except Exception as exc:
        error = _readable_error(exc, context="ML training failed")
        _set_job(job, "failed", phase="failed", message=error, error=error)
        state.ml_jobs[job_id] = job
        return _job_snapshot(job)
    finally:
        state.ml_job_tasks.pop(job_id, None)


async def _run_register_workflow(request: Request, model_id: str, payload: MLRegisterRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    model = _load_model_from_state_or_registry(state, model_id, default_name=model_id, allow_create=True)
    try:
        backend = _resolve_backend(request)
        result = await _backend_call(
            backend,
            "register_model",
            "register",
            request=request,
            model_id=model_id,
            payload=payload,
            model=model,
        )
        result = dict(result or {})
        model.update(
            {
                "status": "registered",
                "registered_at": _now_utc().isoformat(),
                "name": payload.name or model.get("name") or model_id,
                "registration": result,
                "metadata": {**dict(model.get("metadata") or {}), **dict(payload.metadata or {})},
            }
        )
        _save_model(state, model)
        return _model_snapshot(model)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=_readable_error(exc, context="ML model registration failed")) from exc


async def _run_deploy_paper_workflow(request: Request, model_id: str, payload: MLDeployPaperRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    model = _load_model_from_state_or_registry(state, model_id)
    if not model:
        raise HTTPException(status_code=404, detail=f"model {model_id} not found")
    try:
        backend = _resolve_backend(request)
        result = await _backend_call(
            backend,
            "deploy_paper_model",
            "deploy_paper",
            request=request,
            model_id=model_id,
            payload=payload,
            model=model,
        )
        result = dict(result or {})
        model.update(
            {
                "status": "paper_deployed",
                "paper_deployed_at": _now_utc().isoformat(),
                "paper_deploy": {
                    "allocation_pct": float(payload.allocation_pct),
                    "metadata": dict(payload.metadata or {}),
                    "result": result,
                },
            }
        )
        _save_model(state, model)
        return _model_snapshot(model)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=_readable_error(exc, context="ML paper deployment failed")) from exc


async def _run_factorize_workflow(request: Request, model_id: str, payload: MLFactorizeRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    model = _load_model_from_state_or_registry(state, model_id)
    if not model:
        raise HTTPException(status_code=404, detail=f"model {model_id} not found")
    try:
        backend = _resolve_backend(request)
        result = await _backend_call(
            backend,
            "factorize_model",
            "factorize",
            request=request,
            model_id=model_id,
            payload=payload,
            model=model,
        )
        result = dict(result or {})
        model.update(
            {
                "status": "factorized",
                "factorized_at": _now_utc().isoformat(),
                "factorization": result,
                "factorization_request": payload.model_dump(mode="json"),
            }
        )
        _save_model(state, model)
        return _model_snapshot(model)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=_readable_error(exc, context="ML factorization failed")) from exc


async def _run_oneclick_workflow(request: Request, job_id: str, payload: MLOneClickRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    job = state.ml_jobs.get(job_id) or {}
    _set_job(job, "running", phase="training", message="Starting one-click ML workflow.")
    state.ml_jobs[job_id] = job

    try:
        backend = _resolve_backend(request)
        result = await _backend_call(backend, "oneclick", "run_oneclick", request=request, payload=payload, job=job)
        result = dict(result or {})
        model_id = str(result.get("model_id") or payload.model_id or job_id).strip()
        model = _get_or_create_model(state, model_id, default_name=payload.model_name)
        next_status = "completed"
        if result.get("deployment"):
            next_status = "paper_deployed"
        elif result.get("factorization"):
            next_status = "factorized"
        elif result.get("registration"):
            next_status = "registered"
        model.update(
            {
                "model_id": model_id,
                "name": payload.model_name,
                "status": next_status,
                "oneclick": result,
                "metadata": dict(payload.metadata or {}),
            }
        )
        _save_model(state, model)
        _set_job(job, "completed", phase="completed", message="One-click ML workflow completed.")
        job["result"] = {
            "model_id": model_id,
            "model": _model_snapshot(model),
            "backend": _backend_name(backend),
        }
        state.ml_jobs[job_id] = job
        return _job_snapshot(job)
    except Exception as exc:
        error = _readable_error(exc, context="ML one-click workflow failed")
        _set_job(job, "failed", phase="failed", message=error, error=error)
        state.ml_jobs[job_id] = job
        return _job_snapshot(job)
    finally:
        state.ml_job_tasks.pop(job_id, None)


@router.get("/diagnostics")
async def diagnostics(request: Request) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    backend = _resolve_backend(request)
    capabilities = _backend_capabilities(backend)
    jobs = _list_jobs(state)
    registry_items = _load_registry()
    counts = {status: sum(1 for job in jobs if job.get("status") == status) for status in ML_JOB_STATUSES}
    backend_diag = {}
    try:
        backend_diag = dict(await _backend_call(backend, "diagnostics", request=request) or {})
    except Exception:
        backend_diag = {}
    return {
        "ok": True,
        "backend": _backend_name(backend),
        "capabilities": capabilities,
        "job_counts": counts,
        "jobs_total": len(jobs),
        "models_total": len(set((getattr(state, "ml_models", {}) or {}).keys()) | set(registry_items.keys())),
        "recent_jobs": jobs[:10],
        "state_keys": sorted(k for k in vars(state).keys() if k.startswith("ml_")),
        "backend_diagnostics": backend_diag,
    }


@router.post("/jobs/train")
async def train_job(request: Request, payload: MLTrainRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    job = _new_job(state, job_type="train", payload=payload.model_dump(mode="json"))
    if payload.background:
        task = asyncio.create_task(_run_train_workflow(request, job["job_id"], payload), name=f"ml_train_{job['job_id']}")
        state.ml_job_tasks[job["job_id"]] = task
        return _job_snapshot(job)
    return await _run_train_workflow(request, job["job_id"], payload)


@router.get("/jobs/{job_id}")
async def get_job(request: Request, job_id: str) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    job = dict((getattr(state, "ml_jobs", {}) or {}).get(job_id) or {})
    if not job:
        raise HTTPException(status_code=404, detail=f"ML job {job_id} not found")
    return _job_snapshot(job)


@router.get("/jobs")
async def list_jobs(request: Request) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    jobs = _list_jobs(state)
    return {"ok": True, "items": jobs, "count": len(jobs)}


@router.post("/models/{model_id}/register")
async def register_model(request: Request, model_id: str, payload: Optional[MLRegisterRequest] = None) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    payload = payload or MLRegisterRequest()
    return await _run_register_workflow(request, model_id, payload)


@router.post("/models/{model_id}/deploy/paper")
async def deploy_model_paper(request: Request, model_id: str, payload: Optional[MLDeployPaperRequest] = None) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    payload = payload or MLDeployPaperRequest()
    return await _run_deploy_paper_workflow(request, model_id, payload)


@router.post("/models/{model_id}/factorize")
async def factorize_model(request: Request, model_id: str, payload: Optional[MLFactorizeRequest] = None) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    payload = payload or MLFactorizeRequest()
    return await _run_factorize_workflow(request, model_id, payload)


@router.post("/oneclick")
async def oneclick(request: Request, payload: MLOneClickRequest) -> Dict[str, Any]:
    _ensure_ml_state(request.app)
    state = _state(request.app)
    job = _new_job(state, job_type="oneclick", payload=payload.model_dump(mode="json"))
    if payload.background:
        task = asyncio.create_task(_run_oneclick_workflow(request, job["job_id"], payload), name=f"ml_oneclick_{job['job_id']}")
        state.ml_job_tasks[job["job_id"]] = task
        return _job_snapshot(job)
    return await _run_oneclick_workflow(request, job["job_id"], payload)
