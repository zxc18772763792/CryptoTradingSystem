"""XGBoost-based ML directional signal model.

The model is a binary classifier that predicts whether price will be
higher or lower N bars ahead.  It wraps XGBoost and is intentionally
lenient about missing features (fills with 0).

If xgboost is not installed or the model file does not exist, every
call to ``predict()`` returns a ``FLAT`` signal with confidence 0.

Typical usage::

    model = MLSignalModel.load_from_path("models/ml_signal_xgb.json")
    result = model.predict(features_df, symbol="BTC/USDT")
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.ml.pipeline import FEATURE_COLUMNS as PIPELINE_FEATURE_COLUMNS
from core.ml.pipeline import build_feature_frame as pipeline_build_feature_frame


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
        self._feature_names: List[str] = list(FEATURE_COLS)

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
            model = xgb.XGBClassifier()
            model.load_model(self._model_path)
            self._model = model
            # prefer feature names stored in the model
            if hasattr(model, "feature_names_in_") and model.feature_names_in_ is not None:
                self._feature_names = list(model.feature_names_in_)
            logger.info(
                f"MLSignalModel loaded: path={self._model_path}, "
                f"features={len(self._feature_names)}"
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
            proba = self._model.predict_proba(row)
            # class 1 = price goes up (LONG)
            long_prob = float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
            short_prob = 1.0 - long_prob

            if long_prob >= self._threshold:
                direction, confidence = "LONG", long_prob
            elif short_prob >= self._threshold:
                direction, confidence = "SHORT", short_prob
            else:
                direction, confidence = "FLAT", max(long_prob, short_prob)

            importances: Dict[str, float] = {}
            if hasattr(self._model, "feature_importances_"):
                for name, score in zip(
                    self._feature_names, self._model.feature_importances_
                ):
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

    def _align_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select last row and reindex to the expected feature columns."""
        row = df.tail(1).copy()
        for col in self._feature_names:
            if col not in row.columns:
                row[col] = 0.0
        return row[self._feature_names].fillna(0.0)
