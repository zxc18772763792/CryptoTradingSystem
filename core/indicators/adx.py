"""Average Directional Index (ADX) helpers.

Two flavors are exposed:

* :func:`wilder_adx` — canonical Wilder EWM smoothing (textbook ADX). Used by
  the technical ``ADXTrendStrategy``.
* :func:`sma_adx` — simpler rolling-mean smoothing. Used by
  ``TrendFollowingStrategy``.

These are NOT interchangeable: their numeric outputs differ measurably,
especially near regime changes. Strategies pick the variant they were tuned
against. Both share the same +DM / -DM / TR construction, which was the
historical source of subtle copy/paste bugs (e.g. the in-place ``plus_dm``
overwrite that broke the ``minus_dm > plus_dm`` comparison).
"""
from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd


def _directional_components(
    high: pd.Series, low: pd.Series, close: pd.Series
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Return (plus_dm, minus_dm, true_range).

    Crucially we cache ``up_move`` / ``down_move`` before constructing
    ``plus_dm`` so the boolean masks compare the *original* moves, not a
    truncated copy. This is the safe pattern that should be reused
    everywhere.
    """
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm_mask = (up_move > down_move) & (up_move > 0)
    minus_dm_mask = (down_move > up_move) & (down_move > 0)
    plus_dm = up_move.where(plus_dm_mask, 0.0)
    minus_dm = down_move.where(minus_dm_mask, 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return plus_dm, minus_dm, tr


def wilder_adx(data: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
    """Compute Wilder-smoothed ADX, +DI, -DI.

    Uses ``ewm(alpha=1/period)`` which matches the classic Wilder formula
    (closer to what charting platforms like TradingView display).

    Parameters
    ----------
    data : DataFrame with ``high``, ``low``, ``close`` columns.
    period : smoothing window. Must be ``>= 1``.

    Returns
    -------
    dict with keys ``"plus_di"``, ``"minus_di"``, ``"adx"``.
    """
    plus_dm, minus_dm, tr = _directional_components(
        data["high"], data["low"], data["close"]
    )
    alpha = 1.0 / float(max(1, int(period)))
    atr = tr.ewm(alpha=alpha, adjust=False).mean()
    atr_safe = atr.replace(0, np.nan)
    plus_di = 100.0 * (plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_safe)
    minus_di = 100.0 * (minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_safe)
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = ((plus_di - minus_di).abs() / di_sum) * 100.0
    adx = dx.ewm(alpha=alpha, adjust=False).mean()
    return {"plus_di": plus_di, "minus_di": minus_di, "adx": adx}


def sma_adx(
    data: pd.DataFrame, period: int = 14
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Compute SMA-smoothed ADX, +DI, -DI.

    Uses ``rolling(period).mean()`` rather than Wilder EWM. The values are
    less smooth than :func:`wilder_adx` and respond faster to price changes.
    Returned as a tuple to preserve the historical signature used by
    ``TrendFollowingStrategy``.

    Returns
    -------
    (adx, plus_di, minus_di)
    """
    plus_dm, minus_dm, tr = _directional_components(
        data["high"], data["low"], data["close"]
    )
    period_int = int(max(1, period))
    atr = tr.rolling(period_int).mean()
    atr_safe = atr.replace(0, np.nan)
    plus_di = 100.0 * (plus_dm.rolling(period_int).mean() / atr_safe)
    minus_di = 100.0 * (minus_dm.rolling(period_int).mean() / atr_safe)
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / di_sum
    adx = dx.rolling(period_int).mean()
    return adx, plus_di, minus_di
