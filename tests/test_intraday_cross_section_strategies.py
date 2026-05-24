import numpy as np
import pandas as pd
import pytest

import strategies as strategy_module
from config.strategy_registry import get_backtest_strategy_info, get_strategy_defaults
from strategies.quantitative.intraday_cross_section import (
    INTRADAY_CROSS_SECTION_SPECS,
    build_intraday_cross_section_weights,
    build_ohlcv_panels,
    calc_close_location,
    calc_residual_return,
    calc_return,
    cross_section_rank_select,
)
from web.api.backtest import _run_backtest_core


STRATEGY_CLASSES = [
    "ResidualMom48hStrategy",
    "Ret24hReversalStrategy",
    "RelRet24hReversalStrategy",
    "ResidualMom24hStrategy",
    "CloseLocation48hStrategy",
]


def _panel_frames(rows: int = 620, assets: int = 6) -> dict[str, pd.DataFrame]:
    index = pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC")
    frames: dict[str, pd.DataFrame] = {}
    for i in range(assets):
        symbol = f"ASSET{i + 1}/USDT"
        trend = np.linspace(100.0, 100.0 + i * 12.0, rows)
        wave = np.sin(np.arange(rows) / (18.0 + i)) * (0.5 + i * 0.05)
        close = pd.Series(trend + wave + i, index=index).abs() + 10.0
        open_ = close.shift(1).fillna(close.iloc[0])
        high = np.maximum(open_, close) * (1.002 + i * 0.0002)
        low = np.minimum(open_, close) * (0.998 - i * 0.0002)
        frames[symbol] = pd.DataFrame(
            {
                "open": open_.to_numpy(),
                "high": high.to_numpy(),
                "low": low.to_numpy(),
                "close": close.to_numpy(),
                "volume": np.full(rows, 10000.0 + i * 1000.0),
                "symbol": [symbol] * rows,
            },
            index=index,
        )
    return frames


def test_intraday_cross_section_strategies_are_registered():
    for class_name in STRATEGY_CLASSES:
        assert class_name in strategy_module.ALL_STRATEGIES
        assert getattr(strategy_module, class_name) is not None
        defaults = get_strategy_defaults(class_name)
        info = get_backtest_strategy_info(class_name)
        assert defaults["timeframe"] == "5m"
        assert defaults["exchange"] == "binance"
        assert defaults["market_type"] == "future"
        assert defaults["rebalance_bars"] == 288
        assert defaults["allow_short"] is True
        assert defaults["fee_bps_per_side"] == 5.0
        assert defaults["max_slippage_bps_per_side"] == 20.0
        assert info["backtest_supported"] is True


def test_return_factor_uses_past_shift_only():
    index = pd.date_range("2026-01-01", periods=6, freq="5min")
    close = pd.DataFrame({"A/USDT": [10.0, 11.0, 12.0, 13.0, 14.0, 1500.0]}, index=index)

    ret_before_future_spike = calc_return(close.iloc[:5], 2).iloc[-1, 0]
    ret_same_timestamp_with_future_spike = calc_return(close, 2).iloc[4, 0]

    assert ret_before_future_spike == ret_same_timestamp_with_future_spike
    assert ret_same_timestamp_with_future_spike == 14.0 / 12.0 - 1.0


def test_residual_return_is_cross_sectional_same_timestamp_mean():
    close = pd.DataFrame(
        {
            "A/USDT": [100.0, 110.0, 120.0],
            "B/USDT": [100.0, 90.0, 80.0],
            "C/USDT": [100.0, 100.0, 100.0],
        },
        index=pd.date_range("2026-01-01", periods=3, freq="5min"),
    )
    residual = calc_residual_return(close, 2).iloc[-1]
    raw = pd.Series({"A/USDT": 0.20, "B/USDT": -0.20, "C/USDT": 0.0})

    assert residual["A/USDT"] == pytest.approx(raw["A/USDT"] - raw.mean())
    assert residual["B/USDT"] == pytest.approx(raw["B/USDT"] - raw.mean())
    assert residual["C/USDT"] == pytest.approx(raw["C/USDT"] - raw.mean())


def test_close_location_handles_zero_range_without_inf():
    index = pd.date_range("2026-01-01", periods=2, freq="5min")
    high = pd.DataFrame({"A/USDT": [10.0, 10.0]}, index=index)
    low = pd.DataFrame({"A/USDT": [10.0, 9.0]}, index=index)
    close = pd.DataFrame({"A/USDT": [10.0, 9.5]}, index=index)

    location = calc_close_location(None, high, low, close)

    assert np.isfinite(location.to_numpy()).all()
    assert location.iloc[0, 0] == 0.5
    assert location.iloc[1, 0] == 0.5


def test_rank_direction_mapping_for_all_five_strategies():
    factor = pd.Series(
        {
            "LOW1/USDT": -3.0,
            "LOW2/USDT": -2.0,
            "MID/USDT": 0.0,
            "HIGH1/USDT": 2.0,
            "HIGH2/USDT": 3.0,
        }
    )

    for class_name in [
        "ResidualMom48hStrategy",
        "Ret24hReversalStrategy",
        "RelRet24hReversalStrategy",
        "ResidualMom24hStrategy",
    ]:
        selection = cross_section_rank_select(factor, 0.4, 0.4, INTRADAY_CROSS_SECTION_SPECS[class_name].direction)
        assert selection["long_symbols"] == ["LOW1/USDT", "LOW2/USDT"]
        assert selection["short_symbols"] == ["HIGH2/USDT", "HIGH1/USDT"]

    selection = cross_section_rank_select(
        factor,
        0.4,
        0.4,
        INTRADAY_CROSS_SECTION_SPECS["CloseLocation48hStrategy"].direction,
    )
    assert selection["long_symbols"] == ["HIGH2/USDT", "HIGH1/USDT"]
    assert selection["short_symbols"] == ["LOW1/USDT", "LOW2/USDT"]


def test_intraday_cross_section_weight_builder_outputs_equal_weight_baskets():
    frames = _panel_frames(rows=620, assets=6)
    panels = build_ohlcv_panels(frames)
    result = build_intraday_cross_section_weights(
        INTRADAY_CROSS_SECTION_SPECS["Ret24hReversalStrategy"],
        panels,
        params={
            "long_quantile": 0.34,
            "short_quantile": 0.34,
            "rebalance_bars": 288,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
            "max_portfolio_leverage": 1.0,
        },
    )
    latest = result["weights"].iloc[-1]

    assert latest[latest > 0].nunique() == 1
    assert latest[latest < 0].nunique() == 1
    assert latest[latest > 0].sum() <= 0.5
    assert latest[latest < 0].sum() >= -0.5
    assert len(result["rebalance_rows"]) >= 1


def test_weight_builder_honors_lookback_bars_override():
    frames = _panel_frames(rows=40, assets=6)
    panels = build_ohlcv_panels(frames)
    result = build_intraday_cross_section_weights(
        INTRADAY_CROSS_SECTION_SPECS["Ret24hReversalStrategy"],
        panels,
        params={
            "lookback_bars": 6,
            "rebalance_bars": 6,
            "long_quantile": 0.34,
            "short_quantile": 0.34,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
            "max_portfolio_leverage": 1.0,
        },
    )

    assert result["lookback_bars"] == 6
    assert len(result["rebalance_rows"]) >= 1


def test_intraday_cross_section_backtest_core_runs_with_next_bar_execution():
    frames = _panel_frames(rows=620, assets=6)
    symbols = list(frames)
    result = _run_backtest_core(
        strategy="Ret24hReversalStrategy",
        df=frames[symbols[0]],
        timeframe="5m",
        initial_capital=10000.0,
        params={
            **get_strategy_defaults("Ret24hReversalStrategy"),
            "universe_symbols": symbols,
            "long_quantile": 0.34,
            "short_quantile": 0.34,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
        },
        market_bundle=frames,
        include_series=True,
    )

    assert result["portfolio_mode"] == "intraday_cross_section_long_short"
    assert result["strategy_id"] == "ret_24h"
    assert result["universe_size"] >= 4
    assert result["final_capital"] > 0
    assert result["commission_rate"] == 0.0005
    assert result["slippage_bps"] >= 2.0
    assert result["series"]
