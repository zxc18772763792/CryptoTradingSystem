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


# v2 (2026-09-30): every feature is scale-free (ratios, returns, bounded
# oscillators). v1 fed raw price/volume LEVELS (close, EMAs, Bollinger bands,
# ATR, MACD in price units) to a tree model trained on BTC alone; applied to
# altcoins every row fell into BTC's "low price" leaves and the model said
# LONG on 100% of alt bars (flipping to ~95% SHORT when the same alt was merely
# rescaled to BTC's price). Level features cannot transfer across coins.
FEATURE_SET_VERSION = "ml_signal_v2"
FEATURE_COLUMNS: List[str] = [
    "rsi",
    "macd_pct",
    "macd_signal_pct",
    "macd_hist_pct",
    "ema_fast_gap",
    "ema_slow_gap",
    "bb_position",
    "bb_width",
    "atr_pct",
    "volume_ratio",
    "momentum",
    "ret_1",
    "ret_4",
    "body_pct",
    "range_pct",
]
# Test-set share of one-sided predictions above which a model is rejected: a
# classifier that (almost) always says LONG carries no timing information.
MAX_ONE_SIDE_SHARE = 0.90
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
    safe_close = close.where(close > 0)

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
    out["macd_pct"] = macd_line / safe_close
    out["macd_signal_pct"] = macd_signal / safe_close
    out["macd_hist_pct"] = (macd_line - macd_signal) / safe_close

    out["ema_fast_gap"] = close.ewm(span=8, adjust=False, min_periods=4).mean() / safe_close - 1.0
    out["ema_slow_gap"] = close.ewm(span=21, adjust=False, min_periods=10).mean() / safe_close - 1.0

    bb_mid = close.rolling(20, min_periods=10).mean()
    bb_std = close.rolling(20, min_periods=10).std()
    band = 4.0 * bb_std
    out["bb_position"] = (close - (bb_mid - 2.0 * bb_std)) / band.where(band > 0)
    out["bb_width"] = band / bb_mid.where(bb_mid > 0)

    hl = high - low
    hpc = (high - close.shift(1)).abs()
    lpc = (low - close.shift(1)).abs()
    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    out["atr_pct"] = tr.ewm(com=13, adjust=False, min_periods=14).mean() / safe_close

    vol_ma = volume.rolling(20, min_periods=10).mean()
    out["volume_ratio"] = volume / (vol_ma + 1e-9)
    out["momentum"] = close.pct_change(14, fill_method=None)
    out["ret_1"] = close.pct_change(1, fill_method=None)
    out["ret_4"] = close.pct_change(4, fill_method=None)
    out["body_pct"] = (close - open_) / open_.where(open_ > 0)
    out["range_pct"] = (high - low) / safe_close

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
    feature_columns: Optional[Sequence[str]] = None,
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

    selected_columns = [
        str(column).strip()
        for column in (feature_columns or FEATURE_COLUMNS)
        if str(column).strip() in FEATURE_COLUMNS
    ]
    if not selected_columns:
        raise PipelineError(
            "dataset",
            "no valid feature columns selected",
            details={"requested_feature_columns": list(feature_columns or [])},
        )

    clean_features = frame[selected_columns].astype(float)
    clean_labels = frame["_label"].astype(int)
    return MLDataSet(
        frame=frame,
        features=clean_features,
        labels=clean_labels,
        feature_columns=list(selected_columns),
        forward_bars=int(forward_bars),
        feature_set_version=str(feature_set_version),
    )


def build_pooled_dataset(
    frames: Mapping[str, pd.DataFrame],
    *,
    forward_bars: int,
    min_rows_per_symbol: int = 200,
) -> Tuple[MLDataSet, pd.Series]:
    """Features/labels for several symbols stacked in time order.

    Returns the dataset (RangeIndex rows) and the matching bar timestamps and
    symbols (``_ts``/``_symbol`` columns of ``dataset.frame``). Labels never
    cross symbols: each coin's label uses only its own future close.
    """
    parts: List[pd.DataFrame] = []
    for symbol, df in frames.items():
        try:
            one = build_dataset(df, forward_bars=forward_bars, min_rows=min_rows_per_symbol)
        except PipelineError:
            continue
        part = one.frame.copy()
        part["_ts"] = pd.to_datetime(part.index, utc=True)
        part["_symbol"] = str(symbol)
        parts.append(part.reset_index(drop=True))
    if not parts:
        raise PipelineError("dataset", "no symbol had enough rows for pooled training")
    frame = pd.concat(parts, ignore_index=True).sort_values(["_ts", "_symbol"], kind="mergesort").reset_index(drop=True)
    dataset = MLDataSet(
        frame=frame,
        features=frame[FEATURE_COLUMNS].astype(float),
        labels=frame["_label"].astype(int),
        feature_columns=list(FEATURE_COLUMNS),
        forward_bars=int(forward_bars),
        feature_set_version=FEATURE_SET_VERSION,
    )
    return dataset, frame["_symbol"]


def split_pooled_by_time(dataset: MLDataSet, *, test_size: float, bar_seconds: int) -> MLDataSplit:
    """Chronological split on bar time for a pooled dataset, with a purge gap.

    Rows whose label window (forward_bars) would reach into the test period are
    dropped from training, so no training label overlaps a test bar.
    """
    if not 0.0 < float(test_size) < 1.0:
        raise PipelineError("split", "test_size must be between 0 and 1", details={"test_size": float(test_size)})
    ts = pd.to_datetime(dataset.frame["_ts"], utc=True)
    cut = ts.quantile(1.0 - float(test_size))
    purge = pd.Timedelta(seconds=int(bar_seconds) * int(dataset.forward_bars))
    train_mask = (ts < cut - purge).to_numpy()
    test_mask = (ts >= cut).to_numpy()
    if not train_mask.any() or not test_mask.any():
        raise PipelineError("split", "pooled time split produced an empty partition")
    return MLDataSplit(
        X_train=dataset.features[train_mask].copy(),
        X_test=dataset.features[test_mask].copy(),
        y_train=dataset.labels[train_mask].copy(),
        y_test=dataset.labels[test_mask].copy(),
        test_size=float(test_size),
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
    max_one_side_share: float = MAX_ONE_SIDE_SHARE,
) -> GateResult:
    thresholds = {
        "max_one_side_share": float(max_one_side_share),
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

    long_prob = [float(v) for v in (metrics.get("test_long_prob") or [])]
    threshold = float(metrics.get("prediction_threshold") or 0.55)
    if long_prob:
        long_share = sum(v >= threshold for v in long_prob) / len(long_prob)
        short_share = sum((1.0 - v) >= threshold for v in long_prob) / len(long_prob)
        if max(long_share, short_share) > max_one_side_share:
            reasons.append(
                f"one-sided predictions (long {long_share:.1%} / short {short_share:.1%} > {max_one_side_share:.0%})"
            )
    for symbol, share in dict(metrics.get("per_symbol_one_side_share") or {}).items():
        if float(share) > max_one_side_share:
            reasons.append(f"one-sided predictions on {symbol} ({float(share):.1%} > {max_one_side_share:.0%})")
            break

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
    training_symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    symbols = [str(x) for x in (training_symbols or [symbol])]
    return {
        # "pooled": trained across coins on scale-free features, usable on any
        # coin; "single": only on the coin it was trained on.
        "symbol_scope": "pooled" if len(symbols) > 1 else "single",
        "training_symbols": symbols,
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
    feature_columns: Optional[Sequence[str]] = None,
) -> MLTrainingRun:
    diagnostics = assert_environment_ready(require_xgboost=True, require_sklearn=False)
    dataset = build_dataset(
        df,
        forward_bars=forward_bars,
        min_rows=min_rows,
        feature_columns=feature_columns,
    )
    split = split_dataset(dataset, test_size=test_size)
    model, feature_importances = train_xgboost_classifier(
        split,
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        scale_pos_weight=scale_pos_weight,
    )
    metrics = evaluate_model(model, split, threshold=prediction_threshold)

    allowed_gate_keys = {"min_auc", "min_f1", "min_precision", "min_recall", "min_train_samples", "min_test_samples",
                         "max_one_side_share"}
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
            "feature_columns": list(dataset.feature_columns),
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
            "feature_columns": list(dataset.feature_columns),
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


_PER_ROW_METRIC_KEYS = ("test_predictions", "test_long_prob", "test_short_prob", "test_true")


def run_pooled_training_pipeline(
    *,
    frames: Mapping[str, pd.DataFrame],
    output_root: Path,
    timeframe: str,
    bar_seconds: int,
    exchange: str,
    forward_bars: int,
    test_size: float,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    scale_pos_weight: float,
    prediction_threshold: float = 0.55,
    gate_thresholds: Optional[Mapping[str, float]] = None,
    fail_on_gate: bool = True,
) -> MLTrainingRun:
    """Train one model across many coins on scale-free features.

    The split is on bar time (not row order) with a purge of ``forward_bars``
    so no training label overlaps the test period. Besides the classification
    gate, a model whose predictions are one-sided overall or on any single
    coin is rejected. Per-row test predictions are summarised, not stored.
    """
    diagnostics = assert_environment_ready(require_xgboost=True, require_sklearn=False)
    dataset, symbols = build_pooled_dataset(frames, forward_bars=forward_bars)
    split = split_pooled_by_time(dataset, test_size=test_size, bar_seconds=bar_seconds)
    model, feature_importances = train_xgboost_classifier(
        split,
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        scale_pos_weight=scale_pos_weight,
    )
    metrics = evaluate_model(model, split, threshold=prediction_threshold)

    test_symbols = symbols.loc[split.X_test.index].to_numpy()
    long_prob = np.asarray(metrics["test_long_prob"], dtype=float)
    per_symbol: Dict[str, float] = {}
    for symbol in sorted(set(test_symbols)):
        mask = test_symbols == symbol
        if mask.sum() < 50:
            continue
        lp = long_prob[mask]
        per_symbol[str(symbol)] = round(float(max(np.mean(lp >= prediction_threshold),
                                                  np.mean((1.0 - lp) >= prediction_threshold))), 4)
    metrics["per_symbol_one_side_share"] = per_symbol
    metrics["test_long_share"] = round(float(np.mean(long_prob >= prediction_threshold)), 4)
    metrics["test_short_share"] = round(float(np.mean((1.0 - long_prob) >= prediction_threshold)), 4)

    allowed_gate_keys = {"min_auc", "min_f1", "min_precision", "min_recall", "min_train_samples", "min_test_samples",
                         "max_one_side_share"}
    gate_kwargs = {key: value for key, value in dict(gate_thresholds or {}).items() if key in allowed_gate_keys}
    gate = apply_quality_gate(metrics, **gate_kwargs)
    stored = {k: v for k, v in metrics.items() if k not in _PER_ROW_METRIC_KEYS}

    created_at = _utc_now()
    source_commit = get_source_commit()
    model_id = create_model_id(symbol="pooled", timeframe=timeframe, source_commit=source_commit, created_at=created_at)
    ts = pd.to_datetime(dataset.frame["_ts"], utc=True)
    training_window = {
        "exchange": str(exchange),
        "symbol": "pooled",
        "timeframe": str(timeframe),
        "symbols": sorted(set(symbols)),
        "rows": int(dataset.sample_count),
        "forward_bars": int(forward_bars),
        "start_at": ts.min().isoformat(),
        "end_at": ts.max().isoformat(),
        "test_starts_at": pd.to_datetime(dataset.frame.loc[split.X_test.index, "_ts"], utc=True).min().isoformat(),
    }
    manifest = build_manifest(
        model_id=model_id,
        feature_set_version=dataset.feature_set_version,
        training_window=training_window,
        symbol="pooled",
        timeframe=timeframe,
        metrics={**stored, "quality_gate": gate.to_dict(), "feature_columns": list(dataset.feature_columns)},
        created_at=created_at,
        source_commit=source_commit,
        environment=diagnostics.to_dict(),
        training_symbols=sorted(set(symbols)),
    )
    artifact_dir = save_model_artifacts(
        output_root=Path(output_root),
        model=model,
        manifest=manifest,
        metrics={**stored, "quality_gate": gate.to_dict(), "feature_columns": list(dataset.feature_columns)},
        feature_importances=feature_importances,
    )
    if not gate.passed and fail_on_gate:
        raise PipelineError(
            "gate",
            "ML quality gate failed",
            details={"reasons": gate.reasons, "auc": stored.get("auc"), "artifact_dir": str(artifact_dir)},
        )
    return MLTrainingRun(
        model=model,
        artifact_dir=artifact_dir,
        manifest=manifest,
        metrics=stored,
        gate=gate,
        diagnostics=diagnostics,
        dataset=dataset,
        split=split,
        feature_importances=feature_importances,
    )
