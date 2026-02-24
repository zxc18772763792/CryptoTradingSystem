"""Registry helpers for time-series factors."""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from core.factors_ts.base import TimeSeriesFactor
from core.factors_ts.impl import FACTOR_CLASS_MAP


def list_factors() -> List[str]:
    return sorted(FACTOR_CLASS_MAP.keys())


def build_factor(name: str, params: Dict[str, Any] | None = None) -> TimeSeriesFactor:
    key = str(name or "").strip().lower()
    if key not in FACTOR_CLASS_MAP:
        raise KeyError(f"Unknown TS factor: {name}")
    params = dict(params or {})
    klass = FACTOR_CLASS_MAP[key]
    return klass(**params)


def compute_factor(name: str, df: pd.DataFrame, params: Dict[str, Any] | None = None) -> pd.Series:
    factor = build_factor(name=name, params=params)
    return factor(df)

