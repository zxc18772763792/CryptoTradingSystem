from __future__ import annotations

import pytest

from core.research.validation_gate import _deflated_sharpe_ratio


def test_deflated_sharpe_uses_excess_kurtosis_adjustment() -> None:
    baseline = _deflated_sharpe_ratio(
        sharpe=1.4,
        n_trials=8,
        n_obs=120,
        skewness=0.0,
        kurtosis=3.0,
    )
    lower_kurtosis = _deflated_sharpe_ratio(
        sharpe=1.4,
        n_trials=8,
        n_obs=120,
        skewness=0.0,
        kurtosis=1.0,
    )
    higher_kurtosis = _deflated_sharpe_ratio(
        sharpe=1.4,
        n_trials=8,
        n_obs=120,
        skewness=0.0,
        kurtosis=5.0,
    )

    assert lower_kurtosis < baseline < higher_kurtosis
    assert baseline == pytest.approx(0.3072, abs=0.0001)
