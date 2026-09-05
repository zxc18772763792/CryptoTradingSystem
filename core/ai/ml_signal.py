"""XGBoost-based ML directional signal model.

The model is a binary classifier that predicts whether price will be
higher or lower N bars ahead. It wraps XGBoost and fails closed to FLAT
when required inference features are missing or invalid.

If xgboost is not installed or the model file does not exist, every
call to ``predict()`` returns a ``FLAT`` signal with confidence 0.

Typical usage::

    model = MLSignalModel.load_from_path("models/ml_signal_xgb.json")
    result = model.predict(features_df, symbol="BTC/USDT")
"""
from __future__ import annotations

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.ml.pipeline import FEATURE_SET_VERSION
from core.ml.pipeline import FEATURE_COLUMNS as PIPELINE_FEATURE_COLUMNS
from core.ml.pipeline import build_feature_frame as pipeline_build_feature_frame
from core.ml.pipeline import MANIFEST_FILE_NAME


# Canonical feature column order used during training and inference.
# The training script must produce exactly these columns (in any order;
# the model reindexes to this list).
FEATURE_COLS: List[str] = list(PIPELINE_FEATURE_COLUMNS)


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build the canonical ML feature frame from OHLCV data.

    Training, backtesting, and live inference must share the same feature
    engineering logic to avoid silent train/serve skew.
    """
    return pipeline_build_feature_frame(df).reindex(columns=FEATURE_COLS)


@dataclass
class MLSignalResult:
    """Result returned by :class:`MLSignalModel.predict`."""

    symbol: str
    direction: str          # "LONG" | "SHORT" | "FLAT"
    confidence: float       # 0 – 1
    long_prob: float
    short_prob: float
    feature_importances: Dict[str, float] = field(default_factory=dict)
    model_version: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": round(self.confidence, 6),
            "long_prob": round(self.long_prob, 6),
            "short_prob": round(self.short_prob, 6),
            "feature_importances": {
                k: round(v, 6) for k, v in self.feature_importances.items()
            },
            "model_version": self.model_version,
        }


class MLSignalModel:
    """XGBoost binary classifier: price goes up (1) or down/flat (0)."""

    MODEL_VERSION = "xgb_v1"

    def __init__(self, model_path: str, threshold: float = 0.55):
        self._model_path = str(model_path)
        self._threshold = max(0.5, min(1.0, float(threshold)))
        self._model: Optional[Any] = None
        self._model_backend = ""
        self._feature_names: List[str] = list(FEATURE_COLS)
        self._manifest: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load the model from disk.  Silent on failure – returns FLAT signals."""
        try:
            import xgboost as xgb  # noqa: PLC0415
        except ImportError:
            logger.warning("xgboost not installed; MLSignalModel will return FLAT signals")
            return

        if not os.path.exists(self._model_path):
            logger.warning(
                f"MLSignalModel: model file not found at '{self._model_path}'; "
                "run scripts/train_ml_signal.py to create it"
            )
            return

        try:
            manifest = self._load_manifest()
            self._validate_manifest(manifest)
            model: Any
            backend = "classifier"
            classifier_error: Optional[Exception] = None
            try:
                model = xgb.XGBClassifier()
                model.load_model(self._model_path)
            except Exception as exc:
                classifier_error = exc
                booster_class = getattr(xgb, "Booster", None)
                if booster_class is None:
                    raise
                # XGBoost 2.1's sklearn wrapper is incompatible with newer
                # sklearn tag validation (for example sklearn 1.9 raises
                # ``_estimator_type undefined`` while loading).  The artifact
                # itself is a native XGBoost model, so load it through Booster
                # without changing or retraining the model binary.
                model = booster_class()
                model.load_model(self._model_path)
                backend = "booster"
                logger.warning(
                    "MLSignalModel: sklearn wrapper load failed; using native "
                    f"Booster compatibility path: {classifier_error}"
                )
            self._model = model
            self._model_backend = backend
            self._manifest = dict(manifest)
            # prefer feature names stored in the model
            model_feature_names: Optional[List[str]] = None
            if backend == "booster":
                raw_feature_names = getattr(model, "feature_names", None)
                if raw_feature_names:
                    model_feature_names = [str(name) for name in raw_feature_names]
            else:
                try:
                    raw_feature_names = model.feature_names_in_
                    if raw_feature_names is not None:
                        model_feature_names = [str(name) for name in raw_feature_names]
                except (AttributeError, RuntimeError, TypeError, ValueError):
                    model_feature_names = None
            self._feature_names = model_feature_names or list(
                manifest.get("feature_columns") or FEATURE_COLS
            )
            logger.info(
                f"MLSignalModel loaded: path={self._model_path}, "
                f"features={len(self._feature_names)}, backend={backend}"
            )
        except Exception as exc:
            logger.warning(f"MLSignalModel: failed to load model: {exc}")

    def is_loaded(self) -> bool:
        return self._model is not None

    def predict(self, features: pd.DataFrame, symbol: str = "") -> MLSignalResult:
        """Return directional signal for the last row of *features*."""
        flat = MLSignalResult(
            symbol=symbol,
            direction="FLAT",
            confidence=0.0,
            long_prob=0.0,
            short_prob=0.0,
            model_version=self.MODEL_VERSION,
        )
        if self._model is None or features is None or features.empty:
            return flat

        try:
            row = self._align_features(features)
            if self._model_backend == "booster":
                import xgboost as xgb  # noqa: PLC0415

                matrix = xgb.DMatrix(row, feature_names=list(self._feature_names))
                raw_probability = np.asarray(self._model.predict(matrix), dtype=float).reshape(-1)
                if raw_probability.size != 1:
                    raise ValueError(
                        f"expected one booster probability, received {raw_probability.size}"
                    )
                long_prob = float(raw_probability[0])
            else:
                proba = self._model.predict_proba(row)
                # class 1 = price goes up (LONG)
                long_prob = (
                    float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
                )
            if not np.isfinite(long_prob):
                raise ValueError("model returned a non-finite probability")
            long_prob = max(0.0, min(1.0, long_prob))
            short_prob = 1.0 - long_prob

            if long_prob >= self._threshold:
                direction, confidence = "LONG", long_prob
            elif short_prob >= self._threshold:
                direction, confidence = "SHORT", short_prob
            else:
                direction, confidence = "FLAT", max(long_prob, short_prob)

            importances: Dict[str, float] = {}
            if self._model_backend == "booster":
                raw_scores = self._model.get_score(importance_type="gain")
                total = sum(max(0.0, float(score)) for score in raw_scores.values())
                if total > 0:
                    importances = {
                        str(name): round(max(0.0, float(score)) / total, 6)
                        for name, score in raw_scores.items()
                    }
            else:
                try:
                    raw_importances = self._model.feature_importances_
                except (AttributeError, RuntimeError, TypeError, ValueError):
                    raw_importances = None
                if raw_importances is not None:
                    for name, score in zip(self._feature_names, raw_importances):
                        importances[str(name)] = round(float(score), 6)

            return MLSignalResult(
                symbol=symbol,
                direction=direction,
                confidence=round(confidence, 6),
                long_prob=round(long_prob, 6),
                short_prob=round(short_prob, 6),
                feature_importances=importances,
                model_version=self.MODEL_VERSION,
            )
        except Exception as exc:
            logger.debug(f"MLSignalModel.predict failed for '{symbol}': {exc}")
            return flat

    # ------------------------------------------------------------------
    # Class-method constructor
    # ------------------------------------------------------------------

    @classmethod
    def load_from_path(cls, path: str, threshold: float = 0.55) -> "MLSignalModel":
        model = cls(model_path=path, threshold=threshold)
        model.load()
        return model

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _manifest_candidates(self) -> List[Path]:
        model_path = Path(self._model_path)
        candidates: List[Path] = []
        if model_path.is_dir():
            candidates.append(model_path / MANIFEST_FILE_NAME)
        else:
            candidates.append(model_path.with_suffix(".manifest.json"))
            candidates.append(model_path.parent / MANIFEST_FILE_NAME)
        seen: set[str] = set()
        unique: List[Path] = []
        for path in candidates:
            key = str(path.resolve())
            if key not in seen:
                seen.add(key)
                unique.append(path)
        return unique

    def _load_manifest(self) -> Dict[str, Any]:
        for path in self._manifest_candidates():
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    return payload
            except Exception as exc:
                raise ValueError(f"invalid ML manifest at {path}: {exc}") from exc
        raise ValueError(
            "missing ML manifest; expected feature_set_version and feature_columns "
            f"next to {self._model_path}"
        )

    def _validate_manifest(self, manifest: Dict[str, Any]) -> None:
        feature_set_version = str(manifest.get("feature_set_version") or "")
        if feature_set_version != FEATURE_SET_VERSION:
            raise ValueError(
                f"ML manifest feature_set_version mismatch: {feature_set_version!r} "
                f"!= {FEATURE_SET_VERSION!r}"
            )
        feature_columns = manifest.get("feature_columns")
        if not isinstance(feature_columns, list) or not all(isinstance(col, str) for col in feature_columns):
            raise ValueError("ML manifest feature_columns must be a list of strings")
        if list(feature_columns) != FEATURE_COLS:
            raise ValueError("ML manifest feature_columns do not match inference feature columns")

    def _align_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select last row and require the expected feature columns."""
        row = df.tail(1).copy()
        missing = [col for col in self._feature_names if col not in row.columns]
        if missing:
            raise ValueError(f"missing ML feature columns: {missing[:8]}")
        aligned = row[self._feature_names]
        if aligned.isna().any(axis=None):
            nan_cols = aligned.columns[aligned.isna().any()].tolist()
            raise ValueError(f"invalid NaN ML feature columns: {nan_cols[:8]}")
        return aligned
