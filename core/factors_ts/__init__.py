"""Time-series factor library for high-frequency research."""

from core.factors_ts.base import TimeSeriesFactor
from core.factors_ts.impl import (
    ATRPctFactor,
    EMASlopeFactor,
    RealizedVolFactor,
    RetLogFactor,
    SpreadProxyFactor,
    VolumeZFactor,
    ZScorePriceFactor,
)
from core.factors_ts.registry import build_factor, compute_factor, list_factors

__all__ = [
    "TimeSeriesFactor",
    "RetLogFactor",
    "EMASlopeFactor",
    "ZScorePriceFactor",
    "RealizedVolFactor",
    "ATRPctFactor",
    "SpreadProxyFactor",
    "VolumeZFactor",
    "build_factor",
    "compute_factor",
    "list_factors",
]

