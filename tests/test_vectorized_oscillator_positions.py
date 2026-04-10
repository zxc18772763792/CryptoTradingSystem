from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategies.factor_based.factor_strategies import CCIStrategy, StochRSIStrategy, WilliamsRStrategy
from web.api.backtest import _build_positions, _replay_signal_strategy_position


def _oscillating_df(rows: int = 360, freq: str = "1h") -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq=freq)
    base = np.linspace(0, 18 * np.pi, rows)
    close = pd.Series(
        100.0
        + np.sin(base) * 6.0
        + np.sin(base * 0.37) * 2.5
        + np.cos(base * 0.13) * 1.2,
        index=index,
    )
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + 1.0
    low = np.minimum(open_, close) - 1.0
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": np.full(rows, 1000.0),
        },
        index=index,
    )


@pytest.mark.parametrize(
    ("strategy_name", "strategy_class", "params"),
    [
        ("WilliamsRStrategy", WilliamsRStrategy, {"period": 14}),
        ("CCIStrategy", CCIStrategy, {"period": 20, "constant": 0.015}),
        ("StochRSIStrategy", StochRSIStrategy, {"rsi_period": 14, "stoch_period": 14}),
    ],
)
@pytest.mark.parametrize(
    "policy",
    [
        {"allow_long": True, "allow_short": True, "reverse_on_signal": True},
        {"allow_long": True, "allow_short": False, "reverse_on_signal": True},
    ],
)
def test_vectorized_oscillator_positions_match_signal_replay(
    strategy_name: str,
    strategy_class: type,
    params: dict,
    policy: dict,
) -> None:
    df = _oscillating_df()
    merged_params = dict(params)
    merged_params.update(policy)

    replay_position = _replay_signal_strategy_position(
        strategy_class,
        df,
        params=merged_params,
        allow_long=bool(policy["allow_long"]),
        allow_short=bool(policy["allow_short"]),
        reverse_on_signal=bool(policy["reverse_on_signal"]),
    ).fillna(0.0)

    vectorized_position = pd.to_numeric(
        _build_positions(strategy_name, df, merged_params),
        errors="coerce",
    ).fillna(0.0)

    pd.testing.assert_series_equal(vectorized_position.astype(float), replay_position.astype(float))
