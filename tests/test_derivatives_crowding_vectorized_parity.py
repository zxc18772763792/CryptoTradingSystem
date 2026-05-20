"""Parity test for the vectorized crowding-scores computation.

Before this refactor ``prepare_derivatives_features`` always ran a Python
``iterrows`` loop calling ``calculate_crowding_scores`` per row even when
the columns were already present. The vectorized replacement
(``_vectorized_crowding_scores``) must match the original per-row output
bit-for-bit on representative inputs so any future regressions surface
before they affect a live strategy.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.structural.derivatives_crowding import (
    _vectorized_crowding_scores,
    calculate_crowding_scores,
)


def _make_frame(seed: int, n: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "oi_change_z": rng.standard_normal(n) * 1.5,
            "funding_z": rng.standard_normal(n),
            "long_short_ratio_z": rng.standard_normal(n) * 0.7,
            "basis_z": rng.standard_normal(n) * 0.5,
            "taker_imbalance_z": rng.standard_normal(n) * 1.2,
        }
    )


@pytest.mark.parametrize("seed", [0, 7, 42, 123, 2026])
def test_vectorized_matches_per_row(seed):
    df = _make_frame(seed)
    long_v, short_v = _vectorized_crowding_scores(df)

    long_ref = np.empty(len(df))
    short_ref = np.empty(len(df))
    for i, (_, row) in enumerate(df.iterrows()):
        long_ref[i], short_ref[i] = calculate_crowding_scores(row)

    np.testing.assert_allclose(long_v, long_ref, atol=1e-12, rtol=1e-12,
                               err_msg=f"seed={seed}: long scores diverged")
    np.testing.assert_allclose(short_v, short_ref, atol=1e-12, rtol=1e-12,
                               err_msg=f"seed={seed}: short scores diverged")


def test_vectorized_handles_nan_inputs():
    df = pd.DataFrame(
        {
            "oi_change_z": [0.0, np.nan, 2.0, -1.0, np.inf],
            "funding_z": [1.5, 0.0, np.nan, -0.5, 0.0],
            "long_short_ratio_z": [0.0, 1.0, 0.5, np.nan, 0.0],
            "basis_z": [0.0, 0.0, 0.0, 0.0, np.nan],
            "taker_imbalance_z": [0.0, 0.5, np.nan, 1.0, -np.inf],
        }
    )
    long_v, short_v = _vectorized_crowding_scores(df)

    long_ref = np.empty(len(df))
    short_ref = np.empty(len(df))
    for i, (_, row) in enumerate(df.iterrows()):
        long_ref[i], short_ref[i] = calculate_crowding_scores(row)

    np.testing.assert_allclose(long_v, long_ref, atol=1e-12, rtol=1e-12)
    np.testing.assert_allclose(short_v, short_ref, atol=1e-12, rtol=1e-12)


def test_vectorized_handles_missing_columns():
    # When taker_imbalance_z is missing, calculate_crowding_scores falls back
    # to taker_buy_imbalance_z. The vectorized form must do the same.
    df = pd.DataFrame(
        {
            "oi_change_z": [0.5, 1.0],
            "funding_z": [0.3, -0.3],
            "long_short_ratio_z": [0.1, 0.1],
            "basis_z": [0.0, 0.0],
            "taker_buy_imbalance_z": [0.8, -0.8],
        }
    )
    long_v, short_v = _vectorized_crowding_scores(df)

    long_ref = np.empty(len(df))
    short_ref = np.empty(len(df))
    for i, (_, row) in enumerate(df.iterrows()):
        long_ref[i], short_ref[i] = calculate_crowding_scores(row)

    np.testing.assert_allclose(long_v, long_ref, atol=1e-12, rtol=1e-12)
    np.testing.assert_allclose(short_v, short_ref, atol=1e-12, rtol=1e-12)


def test_prepare_features_no_op_when_already_prepared():
    """The fast-path skip must return the SAME object (no copy) when every
    output column already exists — that's what makes detect_flush_reversal
    O(1) instead of O(n) when called from a strategy that already prepared
    the frame."""
    from core.structural.derivatives_crowding import (  # noqa: PLC0415
        _PREPARED_COLUMNS,
        prepare_derivatives_features,
    )

    idx = pd.date_range("2026-01-01", periods=10, freq="1h")
    base = {col: np.zeros(10) for col in _PREPARED_COLUMNS}
    base["close"] = np.linspace(100, 110, 10)
    df = pd.DataFrame(base, index=idx)

    out = prepare_derivatives_features(df)
    assert out is df, "expected the same DataFrame reference when all columns present"
