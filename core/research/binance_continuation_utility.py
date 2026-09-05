"""Frozen, serializable continuation-utility model helpers.

The model uses only completed 4h path features.  Fitting is research-only;
forward prediction is a pure NumPy transformation with no execution imports.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression


@dataclass(frozen=True)
class FrozenUtilityModel:
    version: str
    features: list[str]
    lower: list[float]
    upper: list[float]
    medians: list[float]
    means: list[float]
    scales: list[float]
    coefficients: list[float]
    intercept: float
    selection_threshold: float
    selection_quantile: float
    checkpoint_hours: int
    training_rows: int
    training_positives: int
    training_last_decision_utc: str
    maximum_label_maturity_utc: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FrozenUtilityModel":
        return cls(**payload)

    def predict_score(self, rows: pd.DataFrame) -> np.ndarray:
        frame = rows.reindex(columns=self.features).apply(pd.to_numeric, errors="coerce")
        lower = pd.Series(self.lower, index=self.features, dtype=float)
        upper = pd.Series(self.upper, index=self.features, dtype=float)
        medians = pd.Series(self.medians, index=self.features, dtype=float)
        means = pd.Series(self.means, index=self.features, dtype=float)
        scales = pd.Series(self.scales, index=self.features, dtype=float).replace(0, 1.0)
        clean = frame.clip(lower=lower, upper=upper, axis=1).fillna(medians)
        standardized = ((clean - means) / scales).to_numpy(float)
        logits = standardized @ np.asarray(self.coefficients, dtype=float) + float(self.intercept)
        logits = np.clip(logits, -40.0, 40.0)
        return 1.0 / (1.0 + np.exp(-logits))


def fit_frozen_utility_model(
    rows: pd.DataFrame,
    *,
    features: Sequence[str],
    label_column: str,
    version: str,
    checkpoint_hours: int,
    selection_quantile: float,
    maturity_days: int = 14,
) -> FrozenUtilityModel:
    training = rows.copy()
    training["decision_time"] = pd.to_datetime(training["decision_time"], utc=True)
    training = training[training[label_column].notna()].sort_values("decision_time").reset_index(drop=True)
    x = training.reindex(columns=list(features)).apply(pd.to_numeric, errors="coerce")
    lower = x.quantile(0.01)
    upper = x.quantile(0.99)
    x = x.clip(lower=lower, upper=upper, axis=1)
    medians = x.median().fillna(0.0)
    x = x.fillna(medians)
    means = x.mean()
    scales = x.std(ddof=0).replace(0, 1.0).fillna(1.0)
    standardized = ((x - means) / scales).to_numpy(float)
    target = training[label_column].astype(int).to_numpy()
    if len(training) < 30 or np.unique(target).size < 2:
        raise ValueError("utility model requires at least 30 rows and both target classes")
    classifier = LogisticRegression(
        C=0.20,
        class_weight="balanced",
        max_iter=3000,
        solver="lbfgs",
    )
    classifier.fit(standardized, target)
    train_score = classifier.predict_proba(standardized)[:, 1]
    last_decision = pd.Timestamp(training["decision_time"].max())
    return FrozenUtilityModel(
        version=version,
        features=list(features),
        lower=[float(lower[name]) for name in features],
        upper=[float(upper[name]) for name in features],
        medians=[float(medians[name]) for name in features],
        means=[float(means[name]) for name in features],
        scales=[float(scales[name]) for name in features],
        coefficients=[float(value) for value in classifier.coef_[0]],
        intercept=float(classifier.intercept_[0]),
        selection_threshold=float(np.quantile(train_score, selection_quantile)),
        selection_quantile=float(selection_quantile),
        checkpoint_hours=int(checkpoint_hours),
        training_rows=int(len(training)),
        training_positives=int(target.sum()),
        training_last_decision_utc=last_decision.isoformat(),
        maximum_label_maturity_utc=(last_decision + pd.Timedelta(days=maturity_days)).isoformat(),
    )
