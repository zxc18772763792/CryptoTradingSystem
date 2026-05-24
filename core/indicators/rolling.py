"""Vectorised replacements for slow ``rolling.apply(lambda)`` patterns.

These helpers exist because pandas ``rolling.apply`` with a Python lambda is
~50–100× slower than the equivalent numpy stride-trick implementation. On
typical 5-minute backtest windows (~5000 bars) the difference is measurable
in wall-clock seconds per strategy run.

Each helper documents the exact mathematical equivalent it replaces.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def rolling_mad(series: pd.Series, window: int) -> pd.Series:
    """Mean absolute deviation around the **window mean** (not the global mean).

    Equivalent to::

        series.rolling(window).apply(
            lambda x: np.abs(x - x.mean()).mean(),
            raw=False,
        )

    but ~50× faster on large frames thanks to ``sliding_window_view``.

    NaN handling: any window containing a NaN returns NaN (matches the
    Python-loop semantics used by the audit-flagged call sites). Bars before
    the window is full also return NaN.
    """
    n = int(max(1, window))
    arr = np.asarray(series.values, dtype=np.float64)
    if arr.size < n:
        return pd.Series(np.full(arr.size, np.nan), index=series.index)

    # sliding_window_view: shape (len - n + 1, n).
    win = np.lib.stride_tricks.sliding_window_view(arr, window_shape=n)
    # Any NaN in a window -> NaN result for that window.
    nan_mask = np.isnan(win).any(axis=1)
    means = win.mean(axis=1, keepdims=True)
    mad_values = np.abs(win - means).mean(axis=1)
    mad_values[nan_mask] = np.nan

    out = np.empty(arr.size, dtype=np.float64)
    out[: n - 1] = np.nan
    out[n - 1 :] = mad_values
    return pd.Series(out, index=series.index, name=series.name)


def rolling_var_quantile(
    returns: pd.Series,
    window: int,
    confidence: float,
    *,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """Rolling Value-at-Risk (lower-tail quantile).

    Equivalent to::

        def calc_var(series):
            r = series.dropna()
            if len(r) < window // 2:
                return np.nan
            return np.percentile(r, (1 - confidence) * 100)
        returns.rolling(window).apply(calc_var, raw=False)

    Default ``min_periods=window`` matches pandas' ``rolling.apply`` default
    (which only invokes the callable once the full window has accumulated).
    The inner ``len(r) < window // 2`` guard from the original lambda is a
    no-op under that default — it only mattered if NaNs cut the available
    sample below half. We keep ``min_periods`` configurable so callers can
    opt into earlier emission when their input series has no NaNs.
    """
    n = int(max(1, window))
    if min_periods is None:
        min_periods = n
    alpha = 1.0 - float(confidence)
    return returns.rolling(n, min_periods=min_periods).quantile(alpha)


def rolling_sortino(
    returns: pd.Series,
    window: int,
    *,
    min_periods: Optional[int] = None,
    min_downside_obs: int = 2,
) -> pd.Series:
    """Rolling Sortino ratio (mean / downside-RMS).

    Equivalent to::

        def calc_sortino(series):
            r = series.dropna()
            if len(r) < window // 2:
                return np.nan
            mean_ret = r.mean()
            downside = r[r < 0]
            if len(downside) < 2:
                return np.nan
            downside_std = np.sqrt((downside ** 2).mean())
            return mean_ret / downside_std if downside_std > 0 else np.nan
        returns.rolling(window).apply(calc_sortino, raw=False)

    Vectorised via separate rolling means of (returns, negative-only
    squared returns, negative count).
    """
    n = int(max(1, window))
    if min_periods is None:
        min_periods = n  # match pandas rolling.apply default

    # Rolling mean is straightforward.
    mean_ret = returns.rolling(n, min_periods=min_periods).mean()

    # Negative-only squared returns. clip(upper=0) replaces positives with 0
    # so their squared contribution vanishes from the sum.
    neg_squared = returns.clip(upper=0.0) ** 2
    neg_sq_sum = neg_squared.rolling(n, min_periods=min_periods).sum()
    neg_count = (returns < 0).astype("float64").rolling(
        n, min_periods=min_periods
    ).sum()

    # Avoid divide-by-zero: where neg_count < min_downside_obs the window has
    # too little downside data to estimate the denominator. Set to NaN.
    safe_count = neg_count.where(neg_count >= float(min_downside_obs))
    downside_std = np.sqrt(neg_sq_sum / safe_count)

    # Final sortino. NaN propagates where any required term is NaN, and
    # where downside_std == 0 we'd hit divide-by-zero -> replace with NaN.
    sortino = mean_ret / downside_std.replace(0.0, np.nan)
    return sortino
