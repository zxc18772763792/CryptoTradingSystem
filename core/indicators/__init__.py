"""Shared technical indicator helpers.

This package centralizes indicator math that was previously duplicated across
multiple strategies. Two design rules:

1. Each helper accepts a price/OHLC DataFrame and returns a pandas object —
   strategies remain responsible for thresholding and signal emission.
2. Distinct smoothing flavors (e.g. Wilder EWM vs rolling SMA) live as
   separate named functions; we never silently switch one for the other,
   because they produce numerically different results.
"""

from core.indicators.adx import sma_adx, wilder_adx
from core.indicators.oscillators import oscillator_entry_strength
from core.indicators.rolling import (
    rolling_mad,
    rolling_sortino,
    rolling_var_quantile,
)

__all__ = [
    "sma_adx",
    "wilder_adx",
    "oscillator_entry_strength",
    "rolling_mad",
    "rolling_sortino",
    "rolling_var_quantile",
]
