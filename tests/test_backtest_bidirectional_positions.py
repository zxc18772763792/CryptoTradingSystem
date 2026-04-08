import numpy as np
import pandas as pd
import pytest

import web.api.backtest as backtest_api
from web.api.backtest import (
    _apply_protective_position_rules,
    _build_positions,
    _extract_trade_points,
    _trade_stats,
)


def _trend_df(rows: int = 80, start: float = 100.0, end: float = 60.0, freq: str = "1h") -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq=freq)
    close = pd.Series(np.linspace(start, end, rows), index=index)
    return pd.DataFrame(
        {
            "open": close.shift(1).fillna(close.iloc[0]),
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": np.linspace(100, 200, rows),
        },
        index=index,
    )


def _wave_df(rows: int = 120, freq: str = "1h") -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq=freq)
    close = pd.Series(100 + np.sin(np.linspace(0, 8 * np.pi, rows)) * 5, index=index)
    return pd.DataFrame(
        {
            "open": close.shift(1).fillna(close.iloc[0]),
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": np.full(rows, 200.0),
        },
        index=index,
    )


def test_ma_backtest_defaults_to_bidirectional_short_positions():
    df = _trend_df()
    position = _build_positions("MAStrategy", df, {"fast_period": 3, "slow_period": 8})

    assert float(position.min()) == -1.0
    assert float(position.iloc[-1]) == -1.0


def test_trade_points_and_stats_track_short_round_trip():
    index = pd.date_range("2024-01-01", periods=4, freq="1h")
    close = pd.Series([100.0, 100.0, 98.0, 97.0], index=index)
    position = pd.Series([0.0, -1.0, -1.0, 0.0], index=index)

    trade_points = _extract_trade_points(close, position)
    stats = _trade_stats(close, position)

    assert trade_points["entries"] == 1
    assert trade_points["exits"] == 1
    assert trade_points["open_points"][0]["direction"] == "short"
    assert trade_points["close_points"][0]["direction"] == "short"
    assert stats == {"entries": 1, "exits": 1, "completed": 1, "win_rate": 100.0}


def test_trade_points_keep_same_bar_reversal_as_close_then_open():
    index = pd.date_range("2024-01-01", periods=3, freq="1h")
    close = pd.Series([100.0, 101.0, 99.0], index=index)
    position = pd.Series([0.0, 1.0, -1.0], index=index)

    trade_points = _extract_trade_points(close, position)
    stats = _trade_stats(close, position)

    assert trade_points["entries"] == 2
    assert trade_points["exits"] == 1
    assert trade_points["close_points"][0]["timestamp"] == index[2].isoformat()
    assert trade_points["close_points"][0]["direction"] == "long"
    assert trade_points["close_points"][0]["reason"] == "reverse"
    assert trade_points["open_points"][1]["timestamp"] == index[2].isoformat()
    assert trade_points["open_points"][1]["direction"] == "short"
    assert stats == {"entries": 2, "exits": 1, "completed": 1, "win_rate": 0.0}


def test_protective_rules_take_profit_short_position():
    index = pd.date_range("2024-01-01", periods=4, freq="1h")
    df = pd.DataFrame(
        {
            "close": [100.0, 100.0, 98.0, 97.0],
            "high": [100.5, 100.5, 100.0, 98.0],
            "low": [99.5, 99.5, 97.5, 96.5],
        },
        index=index,
    )
    raw_position = pd.Series([0.0, -1.0, -1.0, -1.0], index=index)

    effective, stats = _apply_protective_position_rules(
        df,
        raw_position,
        stop_loss_pct=0.02,
        take_profit_pct=0.02,
    )

    assert stats["forced_take_exits"] == 1
    assert stats["forced_stop_exits"] == 0
    assert float(effective.iloc[2]) == 0.0


def test_run_backtest_core_uses_protective_execution_price_for_trade_points(monkeypatch: pytest.MonkeyPatch):
    index = pd.date_range("2024-01-01", periods=4, freq="1h")
    df = pd.DataFrame(
        {
            "open": [100.0, 100.0, 100.0, 97.0],
            "high": [100.5, 100.5, 100.2, 97.5],
            "low": [99.5, 99.5, 97.5, 96.5],
            "close": [100.0, 100.0, 97.0, 97.0],
            "volume": [100.0, 100.0, 100.0, 100.0],
        },
        index=index,
    )
    raw_position = pd.Series([0.0, -1.0, -1.0, 0.0], index=index)

    monkeypatch.setattr(backtest_api, "_min_required_bars", lambda timeframe: 2)
    monkeypatch.setattr(backtest_api, "_build_positions", lambda strategy, frame, params=None: raw_position)

    result = backtest_api._run_backtest_core(
        strategy="MAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000.0,
        params={"fast_period": 3, "slow_period": 8},
        include_series=True,
        use_stop_take=True,
        stop_loss_pct=0.02,
        take_profit_pct=0.02,
    )

    assert result["forced_take_exits"] == 1
    assert result["trade_points"]["open_points"][0]["timestamp"] == index[1].isoformat()
    assert result["trade_points"]["close_points"][0]["timestamp"] == index[2].isoformat()
    assert result["trade_points"]["close_points"][0]["reason"] == "take_profit"
    assert result["trade_points"]["close_points"][0]["price"] == pytest.approx(98.0)


@pytest.mark.parametrize(
    ("strategy", "params"),
    [
        ("WilliamsRStrategy", {"period": 14}),
        ("CCIStrategy", {"period": 20}),
        ("StochRSIStrategy", {"rsi_period": 14, "stoch_period": 14}),
    ],
)
def test_supported_registry_oscillator_backtests_have_runtime_branch(strategy: str, params: dict):
    df = _wave_df()

    position = _build_positions(strategy, df, params)

    assert len(position) == len(df)
    assert set(float(v) for v in position.dropna().unique()).issubset({-1.0, 0.0, 1.0})
