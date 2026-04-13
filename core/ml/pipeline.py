"""Reusable ML pipeline for training and packaging signal models."""
from __future__ import annotations

import importlib
import importlib.util
import json
import math
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


FEATURE_SET_VERSION = "ml_signal_v1"
FEATURE_COLUMNS: List[str] = [
    "rsi",
    "macd",
    "macd_signal",
    "macd_hist",
    "ema_fast",
    "ema_slow",
    "bb_upper",
    "bb_lower",
    "bb_mid",
    "atr",
    "volume_ratio",
    "momentum",
    "close",
    "high",
    "low",
    "open",
    "volume",
]
MODEL_FILE_NAME = "model.json"
MANIFEST_FILE_NAME = "manifest.json"
METRICS_FILE_NAME = "metrics.json"
FEATURE_IMPORTANCES_FILE_NAME = "feature_importances.json"


class PipelineError(RuntimeError):
    """Raised when an ML pipeline stage fails."""

    def __init__(self, stage: str, message: str, *, details: Optional[Mapping[str, Any]] = None):
        self.stage = str(stage)
        self.message = str(message)
        self.details = dict(details or {})
        detail_text = ""
        if self.details:
            detail_text = f" details={json.dumps(self.details, ensure_ascii=True, sort_keys=True, default=str)}"
        super().__init__(f"[{self.stage}] {self.message}{detail_text}")


@dataclass(frozen=True)
class DependencyStatus:
    available: bool
    version: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "version": self.version,
            "error": self.error,
        }


@dataclass(frozen=True)
class MLEnvironmentDiagnostics:
    python_version: str
    xgboost: DependencyStatus
    sklearn: DependencyStatus

    def to_dict(self) -> Dict[str, Any]:
        return {
            "python_version": self.python_version,
            "xgboost": self.xgboost.to_dict(),
            "sklearn": self.sklearn.to_dict(),
        }


@dataclass(frozen=True)
class MLDataSet:
    frame: pd.DataFrame
    features: pd.DataFrame
    labels: pd.Series
    feature_columns: List[str]
    forward_bars: int
    feature_set_version: str

    @property
    def sample_count(self) -> int:
        return int(len(self.features))


@dataclass(frozen=True)
class MLDataSplit:
    X_train: pd.DataFrame
    X_test: pd.DataFrame
    y_train: pd.Series
    y_test: pd.Series
    test_size: float

    @property
    def train_samples(self) -> int:
        return int(len(self.X_train))

    @property
    def test_samples(self) -> int:
        return int(len(self.X_test))


@dataclass(frozen=True)
class GateResult:
    passed: bool
    reasons: List[str] = field(default_factory=list)
    thresholds: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "reasons": list(self.reasons),
            "thresholds": dict(self.thresholds),
        }


@dataclass(frozen=True)
class MLTrainingRun:
    model: Any
    artifact_dir: Path
    manifest: Dict[str, Any]
    metrics: Dict[str, Any]
    gate: GateResult
    diagnostics: MLEnvironmentDiagnostics
    dataset: MLDataSet
    split: MLDataSplit
    feature_importances: Dict[str, float]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce")
    else:
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    return series.astype(float)


def _safe_column_name(value: str) -> str:
    cleaned: List[str] = []
    for char in str(value).strip().lower():
        if char.isalnum():
            cleaned.append(char)
        elif char in {" ", "/", "-", "."}:
            cleaned.append("_")
    result = "".join(cleaned).strip("_")
    return result or "model"


def _optional_dependency_status(module_name: str) -> DependencyStatus:
    try:
        spec = importlib.util.find_spec(module_name)
    except Exception as exc:  # pragma: no cover - defensive
        return DependencyStatus(available=False, error=str(exc))
    if spec is None:
        return DependencyStatus(available=False, error="not installed")
    try:
        module = sys.modules.get(module_name)
        if module is None:
            module = importlib.import_module(module_name)
        version = getattr(module, "__version__", None)
        return DependencyStatus(available=True, version=str(version) if version is not None else None)
    except Exception as exc:
        return DependencyStatus(available=False, error=str(exc))


def diagnose_environment() -> MLEnvironmentDiagnostics:
    """Return a small dependency report for the ML toolchain."""
    return MLEnvironmentDiagnostics(
        python_version=sys.version.split()[0],
        xgboost=_optional_dependency_status("xgboost"),
        sklearn=_optional_dependency_status("sklearn"),
    )


def assert_environment_ready(*, require_xgboost: bool = True, require_sklearn: bool = False) -> MLEnvironmentDiagnostics:
    diagnostics = diagnose_environment()
    issues: List[str] = []
    if require_xgboost and not diagnostics.xgboost.available:
        issues.append(f"xgboost unavailable ({diagnostics.xgboost.error or 'unknown error'})")
    if require_sklearn and not diagnostics.sklearn.available:
        issues.append(f"sklearn unavailable ({diagnostics.sklearn.error or 'unknown error'})")
    if issues:
        raise PipelineError(
            "environment",
            "ML environment check failed",
            details={"issues": issues, "diagnostics": diagnostics.to_dict()},
        )
    return diagnostics


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build the canonical ML feature frame from OHLCV input."""
    if df is None or df.empty:
        return pd.DataFrame(columns=FEATURE_COLUMNS)

    close = _ensure_series(df, "close")
    high = _ensure_series(df, "high")
    low = _ensure_series(df, "low")
    open_ = _ensure_series(df, "open")
    volume = _ensure_series(df, "volume")

    out = pd.DataFrame(index=df.index)
    out["open"] = open_
    out["high"] = high
    out["low"] = low
    out["close"] = close
    out["volume"] = volume

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=13, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(com=13, adjust=False, min_periods=14).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    ema12 = close.ewm(span=12, adjust=False, min_periods=6).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=13).mean()
    macd_line = ema12 - ema26
    macd_signal = macd_line.ewm(span=9, adjust=False, min_periods=5).mean()
    out["macd"] = macd_line
    out["macd_signal"] = macd_signal
    out["macd_hist"] = macd_line - macd_signal

    out["ema_fast"] = close.ewm(span=8, adjust=False, min_periods=4).mean()
    out["ema_slow"] = close.ewm(span=21, adjust=False, min_periods=10).mean()

    bb_mid = close.rolling(20, min_periods=10).mean()
    bb_std = close.rolling(20, min_periods=10).std()
    out["bb_mid"] = bb_mid
    out["bb_upper"] = bb_mid + 2.0 * bb_std
    out["bb_lower"] = bb_mid - 2.0 * bb_std

    hl = high - low
    hpc = (high - close.shift(1)).abs()
    lpc = (low - close.shift(1)).abs()
    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    out["atr"] = tr.ewm(com=13, adjust=False, min_periods=14).mean()

    vol_ma = volume.rolling(20, min_periods=10).mean()
    out["volume_ratio"] = volume / (vol_ma + 1e-9)
    out["momentum"] = close.pct_change(14)

    return out.reindex(columns=FEATURE_COLUMNS).replace([np.inf, -np.inf], np.nan)


def generate_labels(close: pd.Series, forward_bars: int = 4) -> pd.Series:
    """Label 1 when the future close is above the current close."""
    close = pd.to_numeric(close, errors="coerce")
    forward_return = close.shift(-forward_bars) / close - 1.0
    labels = pd.Series(np.nan, index=close.index, dtype="float64")
    valid_mask = forward_return.notna()
    labels.loc[valid_mask] = (forward_return.loc[valid_mask] > 0).astype(int)
    return labels


def build_dataset(
    df: pd.DataFrame,
    *,
    forward_bars: int,
    feature_set_version: str = FEATURE_SET_VERSION,
    min_rows: int = 100,
) -> MLDataSet:
    if df is None or df.empty:
        raise PipelineError("dataset", "input OHLCV dataframe is empty")
    missing = [column for column in ("open", "high", "low", "close", "volume") if column not in df.columns]
    if missing:
        raise PipelineError(
            "dataset",
            "input OHLCV dataframe is missing required columns",
            details={"missing_columns": missing},
        )

    features = build_feature_frame(df)
    labels = generate_labels(df["close"], forward_bars=forward_bars)
    frame = features.copy()
    frame["_label"] = labels
    frame = frame.dropna()

    if len(frame) < min_rows:
        raise PipelineError(
            "dataset",
            "insufficient training rows after feature/label preparation",
            details={
                "rows": int(len(frame)),
                "min_rows": int(min_rows),
                "forward_bars": int(forward_bars),
            },
        )

    clean_features = frame[FEATURE_COLUMNS].astype(float)
    clean_labels = frame["_label"].astype(int)
    return MLDataSet(
        frame=frame,
        features=clean_features,
        labels=clean_labels,
        feature_columns=list(FEATURE_COLUMNS),
        forward_bars=int(forward_bars),
        feature_set_version=str(feature_set_version),
    )


def split_dataset(dataset: MLDataSet, *, test_size: float) -> MLDataSplit:
    if not 0.0 < float(test_size) < 1.0:
        raise PipelineError("split", "test_size must be between 0 and 1", details={"test_size": float(test_size)})

    sample_count = dataset.sample_count
    test_count = max(1, int(round(sample_count * float(test_size))))
    train_count = sample_count - test_count
    if train_count <= 0:
        raise PipelineError(
            "split",
            "test_size leaves no training samples",
            details={"samples": sample_count, "test_size": float(test_size)},
        )
    if test_count <= 0:
        raise PipelineError(
            "split",
            "test_size leaves no test samples",
            details={"samples": sample_count, "test_size": float(test_size)},
        )

    X_train = dataset.features.iloc[:train_count].copy()
    X_test = dataset.features.iloc[train_count:].copy()
    y_train = dataset.labels.iloc[:train_count].copy()
    y_test = dataset.labels.iloc[train_count:].copy()

    if X_train.empty or X_test.empty:
        raise PipelineError(
            "split",
            "chronological split produced an empty partition",
            details={"train_samples": int(len(X_train)), "test_samples": int(len(X_test))},
        )

    return MLDataSplit(
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        test_size=float(test_size),
    )


def _manual_binary_report(y_true: Sequence[int], y_pred: Sequence[int]) -> Dict[str, Any]:
    y_true_arr = np.asarray(list(y_true), dtype=int)
    y_pred_arr = np.asarray(list(y_pred), dtype=int)
    total = int(len(y_true_arr))
    correct = int(np.sum(y_true_arr == y_pred_arr))
    accuracy = float(correct / total) if total else 0.0

    report: Dict[str, Any] = {}
    weighted_precision = 0.0
    weighted_recall = 0.0
    weighted_f1 = 0.0
    total_support = max(total, 1)

    for label in (0, 1):
        true_mask = y_true_arr == label
        pred_mask = y_pred_arr == label
        tp = int(np.sum(true_mask & pred_mask))
        fp = int(np.sum(~true_mask & pred_mask))
        fn = int(np.sum(true_mask & ~pred_mask))
        support = int(np.sum(true_mask))
        precision = float(tp / (tp + fp)) if (tp + fp) else 0.0
        recall = float(tp / (tp + fn)) if (tp + fn) else 0.0
        f1 = float((2 * precision * recall) / (precision + recall)) if (precision + recall) else 0.0
        report[str(label)] = {
            "precision": precision,
            "recall": recall,
            "f1-score": f1,
            "support": support,
        }
        weight = support / total_support
        weighted_precision += precision * weight
        weighted_recall += recall * weight
        weighted_f1 += f1 * weight

    report["accuracy"] = accuracy
    report["macro avg"] = {
        "precision": float((report["0"]["precision"] + report["1"]["precision"]) / 2.0),
        "recall": float((report["0"]["recall"] + report["1"]["recall"]) / 2.0),
        "f1-score": float((report["0"]["f1-score"] + report["1"]["f1-score"]) / 2.0),
        "support": total,
    }
    report["weighted avg"] = {
        "precision": float(weighted_precision),
        "recall": float(weighted_recall),
        "f1-score": float(weighted_f1),
        "support": total,
    }
    return report


def _manual_auc(y_true: Sequence[int], y_score: Sequence[float]) -> float:
    y_true_arr = np.asarray(list(y_true), dtype=int)
    y_score_arr = np.asarray(list(y_score), dtype=float)
    pos = int(np.sum(y_true_arr == 1))
    neg = int(np.sum(y_true_arr == 0))
    if pos == 0 or neg == 0:
        return float("nan")
    ranks = pd.Series(y_score_arr).rank(method="average").to_numpy(dtype=float)
    sum_ranks_pos = float(ranks[y_true_arr == 1].sum())
    auc = (sum_ranks_pos - (pos * (pos + 1) / 2.0)) / (pos * neg)
    return float(max(0.0, min(1.0, auc)))


def evaluate_model(model: Any, split: MLDataSplit, *, threshold: float = 0.55) -> Dict[str, Any]:
    try:
        proba = model.predict_proba(split.X_test)
    except Exception as exc:
        raise PipelineError("evaluation", "model.predict_proba failed", details={"error": str(exc)}) from exc

    if proba is None or len(proba) == 0:
        raise PipelineError("evaluation", "model returned no probabilities")

    proba_array = np.asarray(proba, dtype=float)
    if proba_array.ndim != 2 or proba_array.shape[1] < 2:
        raise PipelineError(
            "evaluation",
            "predict_proba returned an unexpected shape",
            details={"shape": list(proba_array.shape)},
        )

    long_prob = proba_array[:, 1]
    short_prob = 1.0 - long_prob
    predictions = (long_prob >= float(threshold)).astype(int)

    try:
        from sklearn.metrics import classification_report, roc_auc_score  # type: ignore

        classification = classification_report(split.y_test, predictions, output_dict=True, zero_division=0)
        auc = float(roc_auc_score(split.y_test, long_prob))
        metric_source = "sklearn"
    except Exception:
        classification = _manual_binary_report(split.y_test, predictions)
        auc = _manual_auc(split.y_test, long_prob)
        metric_source = "manual"

    metrics = {
        "metric_source": metric_source,
        "auc": None if math.isnan(float(auc)) else round(float(auc), 6),
        "prediction_threshold": round(float(threshold), 6),
        "train_samples": split.train_samples,
        "test_samples": split.test_samples,
        "positive_rate_train": round(float(split.y_train.mean()), 6) if len(split.y_train) else 0.0,
        "positive_rate_test": round(float(split.y_test.mean()), 6) if len(split.y_test) else 0.0,
        "classification_report": classification,
        "test_predictions": [int(value) for value in predictions.tolist()],
        "test_long_prob": [round(float(value), 6) for value in long_prob.tolist()],
        "test_short_prob": [round(float(value), 6) for value in short_prob.tolist()],
        "test_true": [int(value) for value in split.y_test.tolist()],
    }
    return metrics


def train_xgboost_classifier(
    split: MLDataSplit,
    *,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    scale_pos_weight: float,
    random_state: int = 42,
) -> Tuple[Any, Dict[str, float]]:
    try:
        import xgboost as xgb  # type: ignore
    except ImportError as exc:
        raise PipelineError("environment", "xgboost is not installed", details={"hint": "pip install xgboost"}) from exc

    pos = int(split.y_train.sum())
    neg = int(len(split.y_train) - pos)
    actual_spw = float(scale_pos_weight) if float(scale_pos_weight) > 0 else float(neg / max(pos, 1))

    model = xgb.XGBClassifier(
        n_estimators=int(n_estimators),
        max_depth=int(max_depth),
        learning_rate=float(learning_rate),
        scale_pos_weight=actual_spw,
        eval_metric="logloss",
        verbosity=0,
        random_state=int(random_state),
        tree_method="hist",
    )
    try:
        model.fit(split.X_train, split.y_train, eval_set=[(split.X_test, split.y_test)], verbose=False)
    except Exception as exc:
        raise PipelineError("training", "xgboost model fit failed", details={"error": str(exc)}) from exc

    feature_importances: Dict[str, float] = {}
    if hasattr(model, "feature_importances_"):
        for column, importance in zip(split.X_train.columns, model.feature_importances_):
            feature_importances[str(column)] = round(float(importance), 6)

    return model, feature_importances


def apply_quality_gate(
    metrics: Mapping[str, Any],
    *,
    min_auc: float = 0.52,
    min_f1: float = 0.40,
    min_precision: float = 0.40,
    min_recall: float = 0.40,
    min_train_samples: int = 80,
    min_test_samples: int = 20,
) -> GateResult:
    thresholds = {
        "min_auc": float(min_auc),
        "min_f1": float(min_f1),
        "min_precision": float(min_precision),
        "min_recall": float(min_recall),
        "min_train_samples": float(min_train_samples),
        "min_test_samples": float(min_test_samples),
    }
    reasons: List[str] = []
    auc_value = metrics.get("auc")
    try:
        auc_float = float(auc_value)
    except Exception:
        auc_float = float("nan")
    if auc_value is None or math.isnan(auc_float) or auc_float < min_auc:
        reasons.append(f"auc below gate ({auc_value!r} < {min_auc})")

    report = dict(metrics.get("classification_report") or {})
    positive_report = dict(report.get("1") or {})
    positive_precision = float(positive_report.get("precision", 0.0) or 0.0)
    positive_recall = float(positive_report.get("recall", 0.0) or 0.0)
    positive_f1 = float(positive_report.get("f1-score", 0.0) or 0.0)
    if positive_f1 < min_f1:
        reasons.append(f"positive-class f1 below gate ({positive_f1:.4f} < {min_f1})")
    if positive_precision < min_precision:
        reasons.append(f"positive-class precision below gate ({positive_precision:.4f} < {min_precision})")
    if positive_recall < min_recall:
        reasons.append(f"positive-class recall below gate ({positive_recall:.4f} < {min_recall})")

    train_samples = int(metrics.get("train_samples", 0) or 0)
    test_samples = int(metrics.get("test_samples", 0) or 0)
    if train_samples < min_train_samples:
        reasons.append(f"train sample count too low ({train_samples} < {min_train_samples})")
    if test_samples < min_test_samples:
        reasons.append(f"test sample count too low ({test_samples} < {min_test_samples})")

    passed = not reasons
    return GateResult(passed=passed, reasons=reasons, thresholds=thresholds)


def get_source_commit(default: str = "unknown") -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        commit = result.stdout.strip()
        return commit or default
    except Exception:
        return default


def create_model_id(
    *,
    symbol: str,
    timeframe: str,
    source_commit: str,
    created_at: Optional[datetime] = None,
) -> str:
    created_at = created_at or _utc_now()
    short_commit = str(source_commit or "unknown")[:8] or "unknown"
    stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    return "_".join(
        [
            "mlsig",
            _safe_column_name(symbol),
            _safe_column_name(timeframe),
            stamp,
            short_commit,
        ]
    )


def build_manifest(
    *,
    model_id: str,
    feature_set_version: str,
    training_window: Mapping[str, Any],
    symbol: str,
    timeframe: str,
    metrics: Mapping[str, Any],
    created_at: datetime,
    source_commit: str,
    environment: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "model_id": str(model_id),
        "feature_set_version": str(feature_set_version),
        "training_window": dict(training_window),
        "symbol": str(symbol),
        "timeframe": str(timeframe),
        "metrics": dict(metrics),
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "source_commit": str(source_commit),
        "environment": dict(environment),
        "feature_columns": list(FEATURE_COLUMNS),
    }


def save_model_artifacts(
    *,
    output_root: Path,
    model: Any,
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
    feature_importances: Optional[Mapping[str, float]] = None,
) -> Path:
    model_id = str(manifest.get("model_id") or "").strip()
    if not model_id:
        raise PipelineError("artifact", "manifest is missing model_id")

    artifact_dir = Path(output_root) / model_id
    artifact_dir.mkdir(parents=True, exist_ok=False)

    model_path = artifact_dir / MODEL_FILE_NAME
    manifest_path = artifact_dir / MANIFEST_FILE_NAME
    metrics_path = artifact_dir / METRICS_FILE_NAME

    if not hasattr(model, "save_model"):
        raise PipelineError("artifact", "model object does not expose save_model")
    try:
        model.save_model(str(model_path))
    except Exception as exc:
        raise PipelineError("artifact", "failed to save model", details={"error": str(exc)}) from exc

    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(dict(manifest), handle, ensure_ascii=True, indent=2, sort_keys=True, default=str)

    with metrics_path.open("w", encoding="utf-8") as handle:
        json.dump(dict(metrics), handle, ensure_ascii=True, indent=2, sort_keys=True, default=str)

    if feature_importances is not None:
        feature_importances_path = artifact_dir / FEATURE_IMPORTANCES_FILE_NAME
        with feature_importances_path.open("w", encoding="utf-8") as handle:
            json.dump(dict(feature_importances), handle, ensure_ascii=True, indent=2, sort_keys=True, default=str)

    return artifact_dir


def run_signal_training_pipeline(
    *,
    df: pd.DataFrame,
    output_root: Path,
    symbol: str,
    timeframe: str,
    exchange: str,
    forward_bars: int,
    test_size: float,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    scale_pos_weight: float,
    prediction_threshold: float = 0.55,
    min_rows: int = 100,
    gate_thresholds: Optional[Mapping[str, float]] = None,
    fail_on_gate: bool = True,
) -> MLTrainingRun:
    diagnostics = assert_environment_ready(require_xgboost=True, require_sklearn=False)
    dataset = build_dataset(df, forward_bars=forward_bars, min_rows=min_rows)
    split = split_dataset(dataset, test_size=test_size)
    model, feature_importances = train_xgboost_classifier(
        split,
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        scale_pos_weight=scale_pos_weight,
    )
    metrics = evaluate_model(model, split, threshold=prediction_threshold)

    allowed_gate_keys = {"min_auc", "min_f1", "min_precision", "min_recall", "min_train_samples", "min_test_samples"}
    gate_kwargs = {key: value for key, value in dict(gate_thresholds or {}).items() if key in allowed_gate_keys}
    gate = apply_quality_gate(metrics, **gate_kwargs)

    created_at = _utc_now()
    source_commit = get_source_commit()
    model_id = create_model_id(
        symbol=symbol,
        timeframe=timeframe,
        source_commit=source_commit,
        created_at=created_at,
    )
    training_window = {
        "exchange": str(exchange),
        "symbol": str(symbol),
        "timeframe": str(timeframe),
        "lookback_rows": int(len(df)),
        "forward_bars": int(forward_bars),
        "start_at": pd.Timestamp(df.index.min()).isoformat() if len(df.index) else None,
        "end_at": pd.Timestamp(df.index.max()).isoformat() if len(df.index) else None,
    }
    manifest = build_manifest(
        model_id=model_id,
        feature_set_version=dataset.feature_set_version,
        training_window=training_window,
        symbol=symbol,
        timeframe=timeframe,
        metrics={
            **metrics,
            "quality_gate": gate.to_dict(),
        },
        created_at=created_at,
        source_commit=source_commit,
        environment=diagnostics.to_dict(),
    )
    artifact_dir = save_model_artifacts(
        output_root=Path(output_root),
        model=model,
        manifest=manifest,
        metrics={
            **metrics,
            "quality_gate": gate.to_dict(),
        },
        feature_importances=feature_importances,
    )
    if not gate.passed and fail_on_gate:
        raise PipelineError(
            "gate",
            "ML quality gate failed",
            details={
                "reasons": gate.reasons,
                "metrics": metrics,
                "thresholds": gate.thresholds,
                "artifact_dir": str(artifact_dir),
                "manifest_path": str(Path(artifact_dir) / MANIFEST_FILE_NAME),
            },
        )
    return MLTrainingRun(
        model=model,
        artifact_dir=artifact_dir,
        manifest=manifest,
        metrics=metrics,
        gate=gate,
        diagnostics=diagnostics,
        dataset=dataset,
        split=split,
        feature_importances=feature_importances,
    )
