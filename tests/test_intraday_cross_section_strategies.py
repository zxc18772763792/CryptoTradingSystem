import asyncio
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import strategies as strategy_module
from config.strategy_registry import get_backtest_strategy_catalog, get_backtest_strategy_info, get_strategy_defaults
from core.strategies.strategy_base import SignalType
from strategies.quantitative import intraday_cross_section as ics_module
from strategies.quantitative.intraday_cross_section import (
    INTRADAY_CROSS_SECTION_SPECS,
    Ret24hReversalStrategy,
    build_intraday_cross_section_weights,
    build_ohlcv_panels,
    calc_close_location,
    calc_residual_return,
    calc_return,
    compute_intraday_factor_panel,
    cross_section_rank_select,
    execution_mode_permissions,
)
from web.api.backtest import _run_backtest_core


TOP5_STRATEGY_CLASSES = [
    "ResidualMom48hStrategy",
    "Ret24hReversalStrategy",
    "RelRet24hReversalStrategy",
    "ResidualMom24hStrategy",
    "CloseLocation48hStrategy",
]
SECOND_ROUND_STRATEGY_CLASSES = [
    "ReturnEntropy4hStrategy",
    "FalseBreakoutSupply24hStrategy",
    "RangeAsymmetry48hStrategy",
    "SessionAsiaFlow24hStrategy",
    "SessionFlowRotation24hStrategy",
    "VolumeWeightedReturn24hStrategy",
    "WickImbalance48hStrategy",
    "TurnoverEntropy48hStrategy",
    "BodyVolumeCorr24hStrategy",
    "CorrBreakdown24h72hStrategy",
    "ExtremeRecency48hStrategy",
    "UpDownBetaSpread24h72hStrategy",
    "DirectionalRangeEfficiency48hStrategy",
    "CrossSectionalStress4hStrategy",
    "SignImbalance4hStrategy",
    "VWAPSlope24hStrategy",
    "VWAPGap48hStrategy",
    "RelativeVolShock24hStrategy",
    "LeadMarketResponse24h72hStrategy",
    "BreakCountBalance24hStrategy",
]
STRATEGY_CLASSES = TOP5_STRATEGY_CLASSES + SECOND_ROUND_STRATEGY_CLASSES


def _freq(timeframe: str) -> str:
    return {"5m": "5min", "15m": "15min", "1h": "1h"}.get(str(timeframe), "5min")


def _panel_frames(rows: int = 980, assets: int = 6, timeframe: str = "5m") -> dict[str, pd.DataFrame]:
    index = pd.date_range("2026-01-01", periods=rows, freq=_freq(timeframe), tz="UTC")
    frames: dict[str, pd.DataFrame] = {}
    for i in range(assets):
        symbol = f"ASSET{i + 1}/USDT"
        trend = np.linspace(100.0, 100.0 + i * 12.0, rows)
        wave = np.sin(np.arange(rows) / (18.0 + i)) * (0.5 + i * 0.08)
        pulse = np.cos(np.arange(rows) / (31.0 + i)) * (0.4 + i * 0.04)
        close = pd.Series(trend + wave + pulse + i, index=index).abs() + 10.0
        open_ = close.shift(1).fillna(close.iloc[0])
        range_add = (np.sin(np.arange(rows) / 17.0) + 1.0) * 0.0004
        high = np.maximum(open_, close) * (1.002 + i * 0.0002 + range_add)
        low = np.minimum(open_, close) * (0.998 - i * 0.0002 - range_add)
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


def _simple_runtime_plan(timestamp: pd.Timestamp) -> dict:
    return {
        "rebalance_timestamp": timestamp,
        "price": {"AAA/USDT": 100.0, "BBB/USDT": 200.0},
        "target_weights": {"AAA/USDT": 0.25, "BBB/USDT": -0.25},
        "long_symbols": ["AAA/USDT"],
        "short_symbols": ["BBB/USDT"],
        "factor": {"AAA/USDT": -1.0, "BBB/USDT": 1.0},
        "rank": {"AAA/USDT": 0.0, "BBB/USDT": 1.0},
        "rebalance_bars": 12,
    }


def test_intraday_cross_section_strategies_are_registered():
    for class_name in STRATEGY_CLASSES:
        spec = INTRADAY_CROSS_SECTION_SPECS[class_name]
        assert class_name in strategy_module.ALL_STRATEGIES
        assert getattr(strategy_module, class_name) is not None
        defaults = get_strategy_defaults(class_name)
        info = get_backtest_strategy_info(class_name)
        allow_long, allow_short = execution_mode_permissions(spec.execution_mode)
        assert defaults["strategy_id"] == spec.strategy_id
        assert defaults["timeframe"] == spec.timeframe
        assert defaults["exchange"] == "binance"
        assert defaults["market_type"] == "future"
        assert defaults["rebalance_bars"] == spec.rebalance_bars
        assert defaults["execution_mode"] == spec.execution_mode
        assert defaults["allow_long"] is allow_long
        assert defaults["allow_short"] is allow_short
        assert defaults["fee_bps_per_side"] == 5.0
        assert defaults["max_slippage_bps_per_side"] == 20.0
        assert info["backtest_supported"] is True
        assert info["strategy_kind"] == "factor_template"
        assert info["template_locked"] is True
        assert info["template_spec"]["strategy_id"] == spec.strategy_id
        assert info["template_spec"]["timeframe"] == spec.timeframe


def test_intraday_cross_section_catalog_exposes_template_metadata():
    rows = get_backtest_strategy_catalog(["Ret24hReversalStrategy", "MAStrategy"])
    by_name = {row["name"]: row for row in rows}

    factor = by_name["Ret24hReversalStrategy"]
    classic = by_name["MAStrategy"]

    assert factor["strategy_kind"] == "factor_template"
    assert factor["template_locked"] is True
    assert factor["factor_family"] == "cross_section"
    assert factor["live_verdict"] == "priority"
    assert "timeframe" in factor["locked_fields"]
    assert factor["template_spec"]["execution_mode"] == "spread_low_minus_high"

    assert classic["strategy_kind"] == "classic"
    assert classic["template_locked"] is False
    assert classic["template_spec"] == {}


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


def test_rank_direction_mapping_for_existing_spread_strategies():
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


def test_execution_mode_rank_mapping_for_second_round_modes():
    factor = pd.Series(
        {
            "LOW1/USDT": -3.0,
            "LOW2/USDT": -2.0,
            "MID/USDT": 0.0,
            "HIGH1/USDT": 2.0,
            "HIGH2/USDT": 3.0,
        }
    )

    low_spread = cross_section_rank_select(factor, 0.4, 0.4, direction="low", execution_mode="spread_low_minus_high")
    assert low_spread["long_symbols"] == ["LOW1/USDT", "LOW2/USDT"]
    assert low_spread["short_symbols"] == ["HIGH2/USDT", "HIGH1/USDT"]

    short_low = cross_section_rank_select(factor, 0.4, 0.4, direction="low", execution_mode="short_low")
    assert short_low["long_symbols"] == []
    assert short_low["short_symbols"] == ["LOW1/USDT", "LOW2/USDT"]

    short_high = cross_section_rank_select(factor, 0.4, 0.4, direction="high", execution_mode="short_high")
    assert short_high["long_symbols"] == []
    assert short_high["short_symbols"] == ["HIGH2/USDT", "HIGH1/USDT"]

    long_high = cross_section_rank_select(factor, 0.4, 0.4, direction="high", execution_mode="long_high")
    assert long_high["long_symbols"] == ["HIGH2/USDT", "HIGH1/USDT"]
    assert long_high["short_symbols"] == []


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


def test_cross_section_runtime_skips_stale_local_plan(monkeypatch):
    strategy = Ret24hReversalStrategy(
        name="test_cross_section_stale",
        params={
            "universe_symbols": ["AAA/USDT", "BBB/USDT"],
            "max_data_age_seconds": 60,
        },
    )
    old_ts = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=2)

    async def fake_load_universe_frames(_universe):
        return {}

    monkeypatch.setattr(strategy, "_load_universe_frames", fake_load_universe_frames)
    monkeypatch.setattr(ics_module, "latest_intraday_cross_section_plan", lambda *_args, **_kwargs: _simple_runtime_plan(old_ts))

    signals = asyncio.run(strategy.generate_signals_async("AAA/USDT"))

    assert signals == []
    assert strategy._last_rebalance_key is None


def test_cross_section_runtime_resizes_existing_target_weight(monkeypatch):
    strategy = Ret24hReversalStrategy(
        name="test_cross_section_resize",
        params={
            "universe_symbols": ["AAA/USDT", "BBB/USDT"],
            "max_data_age_seconds": 3600,
            "rebalance_weight_tolerance": 0.01,
        },
    )
    plan_ts = pd.Timestamp.now(tz="UTC").floor("min")

    async def fake_load_universe_frames(_universe):
        return {}

    monkeypatch.setattr(strategy, "_load_universe_frames", fake_load_universe_frames)
    monkeypatch.setattr(ics_module, "latest_intraday_cross_section_plan", lambda *_args, **_kwargs: _simple_runtime_plan(plan_ts))
    monkeypatch.setattr(
        strategy,
        "_active_positions_from_position_manager",
        lambda: (
            {"AAA/USDT": SimpleNamespace(metadata={"target_weight": 0.10})},
            {"BBB/USDT": SimpleNamespace(metadata={"target_weight": -0.25})},
        ),
    )

    signals = asyncio.run(strategy.generate_signals_async("AAA/USDT"))

    assert [(sig.symbol, sig.signal_type) for sig in signals] == [
        ("AAA/USDT", SignalType.CLOSE_LONG),
        ("AAA/USDT", SignalType.BUY),
    ]
    assert signals[0].metadata["close_only"] is True
    assert signals[0].metadata["close_reason"] == "intraday_cross_section_rebalance_resize"
    assert signals[1].metadata["rebalance_resize"] is True
    assert signals[1].metadata["target_weight"] == pytest.approx(0.25)


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


@pytest.mark.parametrize("class_name", SECOND_ROUND_STRATEGY_CLASSES)
def test_second_round_factor_panel_computes_without_nonfinite_tail(class_name):
    spec = INTRADAY_CROSS_SECTION_SPECS[class_name]
    rows = spec.lookback_bars + spec.rebalance_bars + 64
    frames = _panel_frames(rows=rows, assets=8, timeframe=spec.timeframe)
    panels = build_ohlcv_panels(frames)

    factor = compute_intraday_factor_panel(spec, panels)
    tail = factor.tail(min(24, len(factor))).stack().dropna()

    assert not tail.empty
    assert np.isfinite(tail.to_numpy()).all()


@pytest.mark.parametrize("class_name", SECOND_ROUND_STRATEGY_CLASSES)
def test_second_round_backtest_core_runs_with_strategy_timeframe(class_name):
    spec = INTRADAY_CROSS_SECTION_SPECS[class_name]
    rows = spec.lookback_bars + spec.rebalance_bars + 64
    frames = _panel_frames(rows=rows, assets=8, timeframe=spec.timeframe)
    symbols = list(frames)
    result = _run_backtest_core(
        strategy=class_name,
        df=frames[symbols[0]],
        timeframe=spec.timeframe,
        initial_capital=10000.0,
        params={
            **get_strategy_defaults(class_name),
            "universe_symbols": symbols,
            "long_quantile": 0.25,
            "short_quantile": 0.25,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
        },
        market_bundle=frames,
        include_series=False,
    )

    assert result["portfolio_mode"] == "intraday_cross_section_long_short"
    assert result["strategy_id"] == spec.strategy_id
    assert result["execution_mode"] == spec.execution_mode
    assert result["universe_size"] >= 4
    assert result["final_capital"] > 0
