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


# Canonical feature column order used during training and inference.
# The training script must produce exactly these columns (in any order;
# the model reindexes to this list).
FEATURE_COLS: List[str] = [
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


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build the canonical ML feature frame from OHLCV data."""
    if df is None or df.empty:
        return pd.DataFrame(columns=FEATURE_COLS)

    close = pd.to_numeric(df.get("close"), errors="coerce")
    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")
    open_ = pd.to_numeric(df.get("open"), errors="coerce")
    volume = pd.to_numeric(df.get("volume"), errors="coerce").replace(0, np.nan)

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta).clip(lower=0).rolling(14, min_periods=14).mean()
    rsi = 100.0 - 100.0 / (1.0 + gain / loss.replace(0, np.nan))
    ema_fast = close.ewm(span=8, adjust=False).mean()
    ema_slow = close.ewm(span=21, adjust=False).mean()
    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    bb_mid = close.rolling(20, min_periods=20).mean()
    bb_std = close.rolling(20, min_periods=20).std()
    true_range = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.ewm(span=14, adjust=False).mean()
    volume_ma = volume.rolling(20, min_periods=20).mean()

    features = pd.DataFrame(
        {
            "rsi": rsi,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_hist": macd - macd_signal,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "bb_upper": bb_mid + 2.0 * bb_std,
            "bb_lower": bb_mid - 2.0 * bb_std,
            "bb_mid": bb_mid,
            "atr": atr,
            "volume_ratio": volume / volume_ma.replace(0, np.nan),
            "momentum": close.pct_change(14),
            "close": close,
            "high": high,
            "low": low,
            "open": open_,
            "volume": volume.fillna(0.0),
        },
        index=df.index,
    )
    return features.reindex(columns=FEATURE_COLS).replace([np.inf, -np.inf], np.nan).fillna(0.0)


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
