"""Base types for time-series factors."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Sequence

import pandas as pd


@dataclass
class TimeSeriesFactor(ABC):
    """Abstract time-series factor.

    `compute(df)` must only use information available up to each timestamp t.
    """

    name: str
    inputs: Sequence[str] = field(default_factory=tuple)
    lookback: int = 1
    frequency: str = "bar"
    params: Dict[str, Any] = field(default_factory=dict)

    def validate_inputs(self, df: pd.DataFrame) -> None:
        missing = [c for c in self.inputs if c not in df.columns]
        if missing:
            raise ValueError(f"Factor {self.name} missing columns: {missing}")

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def __call__(self, df: pd.DataFrame) -> pd.Series:
        self.validate_inputs(df)
        out = self.compute(df)
        if not isinstance(out, pd.Series):
            out = pd.Series(out, index=df.index)
        out = out.reindex(df.index)
        out.name = self.name
        return out

