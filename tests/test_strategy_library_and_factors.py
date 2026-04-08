"""Strategy library and factor library regression tests."""
import asyncio
from types import SimpleNamespace

import numpy as np
import pandas as pd

import strategies as strategy_module
from config import strategy_registry
from config.strategy_registry import DEFAULT_START_ALL_STRATEGIES, get_backtest_strategy_info, get_strategy_registry_entry
from core.backtest.cost_models import fee_rate, slippage_rate
from strategies import ALL_STRATEGIES
from strategies.quantitative import fama_factor_arbitrage as fama_factor_module
from core.data.factor_library import build_factor_library
from web.api.backtest import _optimize_strategy_on_df, _run_backtest_core, is_strategy_backtest_supported


def _sample_ohlcv(symbol: str, rows: int = 420, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed=seed)
    index = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    close = 50000 + np.cumsum(rng.normal(0, 80, size=rows))
    close = pd.Series(close, index=index).abs() + 1000
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 30, size=rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 30, size=rows))
    volume = np.abs(rng.normal(2000, 400, size=rows)) + 100

    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high,
            "low": low,
            "close": close.values,
            "volume": volume,
            "symbol": [symbol] * rows,
        },
        index=index,
    )


def _trend_ohlcv(symbol: str = "BTC/USDT", rows: int = 240) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    close = pd.Series(np.linspace(100.0, 220.0, rows), index=index)
    open_ = close.shift(1).fillna(close.iloc[0] * 0.998)
    high = close * 1.003
    low = close * 0.997
    volume = pd.Series(np.full(rows, 1200.0), index=index)
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": volume.values,
            "symbol": [symbol] * rows,
        },
        index=index,
    )


def test_strategy_library_sync_signals_do_not_crash():
    base_df = _sample_ohlcv("BTC/USDT", rows=420, seed=42)
    pair_df = _sample_ohlcv("ETH/USDT", rows=420, seed=99)

    for strategy_name in ALL_STRATEGIES:
        klass = getattr(strategy_module, strategy_name, None)
        if klass is None:
            continue

        strategy = klass(name=f"test_{strategy_name}", params={})
        required = strategy.get_required_data() or {}

        if bool(required.get("requires_pair", False)):
            signals = strategy.generate_signals(base_df, pair_df)  # type: ignore[arg-type]
        else:
            signals = strategy.generate_signals(base_df)

        assert isinstance(signals, list), f"{strategy_name} should return list"


def test_optional_dex_strategies_still_register_in_library_without_web3_runtime():
    assert "DEXArbitrageStrategy" in ALL_STRATEGIES
    assert "FlashLoanArbitrageStrategy" in ALL_STRATEGIES
    assert getattr(strategy_module, "DEXArbitrageStrategy", None) is not None
    assert getattr(strategy_module, "FlashLoanArbitrageStrategy", None) is not None


def test_strategy_registry_exposes_single_source_metadata():
    ma = get_strategy_registry_entry("MAStrategy")
    fama = get_backtest_strategy_info("FamaFactorArbitrageStrategy")

    assert ma["defaults"]["fast_period"] == 20
    assert ma["timeframe"] == "15m"
    assert "MAStrategy" in DEFAULT_START_ALL_STRATEGIES
    assert fama["backtest_supported"] is True


def test_strategy_catalog_and_library_use_same_effective_defaults():
    from web.api import strategies as strategies_api

    catalog = asyncio.run(strategies_api.get_strategy_catalog())
    library = asyncio.run(strategies_api.get_strategy_library())

    catalog_by_name = {row["name"]: row for row in catalog["strategies"]}
    library_by_name = {row["name"]: row for row in library["library"]}

    for name in ["MAStrategy", "PairsTradingStrategy", "MultiFactorHFStrategy", "MLXGBoostStrategy"]:
        assert library_by_name[name]["sample_params"] == catalog_by_name[name]["defaults"]

    ma = catalog_by_name["MAStrategy"]["defaults"]
    pairs = catalog_by_name["PairsTradingStrategy"]["defaults"]
    multi = catalog_by_name["MultiFactorHFStrategy"]["defaults"]
    ml = catalog_by_name["MLXGBoostStrategy"]["defaults"]

    assert ma["fast_period"] == 20
    assert pairs["market_type"] == "future"
    assert pairs["allow_short"] is True
    assert "factors" in multi and "gates" in multi and "risk" in multi
    assert ml["model_path"].endswith("models\\ml_signal_xgb.json")


def test_arbitrage_registry_backtest_support_flags_match_ui_routing():
    pairs = get_backtest_strategy_info("PairsTradingStrategy")
    fama = get_backtest_strategy_info("FamaFactorArbitrageStrategy")
    cex = get_backtest_strategy_info("CEXArbitrageStrategy")
    tri = get_backtest_strategy_info("TriangularArbitrageStrategy")

    assert pairs["backtest_supported"] is True
    assert fama["backtest_supported"] is True
    assert cex["backtest_supported"] is False
    assert tri["backtest_supported"] is False
    assert cex["reason"]
    assert tri["reason"]


def test_mlxgboost_backtest_support_reflects_runtime_dependencies(monkeypatch):
    monkeypatch.setattr(
        strategy_registry,
        "_mlxgboost_backtest_support_status",
        lambda: (False, "runtime missing"),
    )

    info = strategy_registry.get_backtest_strategy_info("MLXGBoostStrategy")
    catalog = strategy_registry.get_backtest_strategy_catalog(["MLXGBoostStrategy"])

    assert info["backtest_supported"] is False
    assert info["reason"] == "runtime missing"
    assert catalog[0]["backtest_supported"] is False
    assert catalog[0]["reason"] == "runtime missing"
    assert strategy_registry.is_strategy_backtest_supported("MLXGBoostStrategy") is False


def test_shared_cost_model_helpers_support_flat_and_dynamic_modes():
    flat_cfg = SimpleNamespace(
        fee_model="flat",
        commission_rate=0.0004,
        slippage_model="flat",
        slippage=0.0002,
    )
    assert fee_rate(flat_cfg) == 0.0004
    assert slippage_rate(flat_cfg) == 0.0002

    dynamic_cfg = SimpleNamespace(
        slippage_model="dynamic",
        slippage=0.0,
        dynamic_slip={"min_slip": 0.0001, "k_atr": 0.15, "k_rv": 0.8, "k_spread": 0.5},
    )
    dynamic_rate = slippage_rate(dynamic_cfg, window=_sample_ohlcv("BTC/USDT", rows=240, seed=123))
    assert dynamic_rate >= 0.0001


def _factor_input(assets: int = 6, rows: int = 600, seed: int = 7):
    rng = np.random.default_rng(seed=seed)
    index = pd.date_range(start="2024-01-01", periods=rows, freq="h")

    close_cols = {}
    volume_cols = {}
    for i in range(assets):
        sym = f"ASSET{i+1}"
        drift = 0.02 + i * 0.01
        noise = rng.normal(0, 1.0 + i * 0.05, size=rows)
        path = 100 + np.cumsum(drift + noise)
        close = pd.Series(path, index=index).abs() + 1
        vol = np.abs(rng.normal(10000 + i * 500, 2000, size=rows)) + 100
        close_cols[sym] = close
        volume_cols[sym] = pd.Series(vol, index=index)

    close_df = pd.DataFrame(close_cols)
    volume_df = pd.DataFrame(volume_cols)
    return close_df, volume_df


def test_factor_library_builds_multi_factor_outputs():
    close_df, volume_df = _factor_input(assets=6, rows=600)
    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3)

    assert not result.factors.empty
    for col in ["MKT", "SMB", "MOM", "REV", "VOL", "LIQ", "VAL", "QMJ", "BAB"]:
        assert col in result.factors.columns

    assert not result.asset_scores.empty
    assert "symbol" in result.asset_scores.columns
    assert "score" in result.asset_scores.columns


def test_factor_library_supports_small_universe():
    close_df, volume_df = _factor_input(assets=2, rows=500, seed=11)
    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3)

    assert not result.factors.empty
    assert set(["MKT", "SMB", "MOM", "VOL"]).issubset(result.factors.columns)


def test_factor_library_key_style_factors_are_not_all_zero():
    close_df, volume_df = _factor_input(assets=8, rows=960, seed=23)
    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3, timeframe="5m")

    tail = result.factors.tail(240)
    for col in ["HML", "MOM", "LIQ", "RMW", "CMA", "QMJ", "BAB"]:
        assert col in tail.columns
        assert tail[col].abs().sum() > 0, f"{col} should have non-zero signal in recent window"


def test_fama_factor_strategy_is_supported_in_web_backtest():
    assert is_strategy_backtest_supported("FamaFactorArbitrageStrategy")

    close_df, volume_df = _factor_input(assets=6, rows=720, seed=17)
    symbols = list(close_df.columns)
    bundle = {}
    for idx, symbol in enumerate(symbols):
        close = close_df[symbol]
        open_ = close.shift(1).fillna(close.iloc[0])
        high = np.maximum(open_, close) * (1 + 0.002 + idx * 0.0001)
        low = np.minimum(open_, close) * (1 - 0.002 - idx * 0.0001)
        bundle[symbol] = pd.DataFrame(
            {
                "open": open_.values,
                "high": high.values,
                "low": low.values,
                "close": close.values,
                "volume": volume_df[symbol].values,
                "symbol": [symbol] * len(close_df),
            },
            index=close_df.index,
        )

    result = _run_backtest_core(
        strategy="FamaFactorArbitrageStrategy",
        df=bundle[symbols[0]],
        timeframe="1h",
        initial_capital=10000,
        params={
            "universe_symbols": symbols,
            "top_n": 2,
            "quantile": 0.34,
            "min_abs_score": 0.0,
            "rebalance_interval_minutes": 60,
            "allow_long": True,
            "allow_short": True,
        },
        include_series=True,
        market_bundle=bundle,
    )

    assert result["portfolio_mode"] == "cross_sectional_long_short"
    assert result["universe_size"] >= 2
    assert result["final_capital"] > 0
    assert "series" in result and result["series"]


def test_fama_runtime_rebalance_offloads_cpu_work(monkeypatch):
    close_df, volume_df = _factor_input(assets=6, rows=720, seed=29)
    symbols = [f"{col}/USDT" for col in close_df.columns]
    frames = {}
    for idx, base_symbol in enumerate(close_df.columns):
        symbol = symbols[idx]
        close = close_df[base_symbol]
        open_ = close.shift(1).fillna(close.iloc[0])
        high = np.maximum(open_, close) * (1 + 0.002 + idx * 0.0001)
        low = np.minimum(open_, close) * (1 - 0.002 - idx * 0.0001)
        frames[symbol] = pd.DataFrame(
            {
                "open": open_.values,
                "high": high.values,
                "low": low.values,
                "close": close.values,
                "volume": volume_df[base_symbol].values,
                "symbol": [symbol] * len(close_df),
            },
            index=close_df.index,
        )

    strategy = fama_factor_module.FamaFactorArbitrageStrategy(
        name="test_fama_runtime",
        params={
            "universe_symbols": symbols,
            "min_universe_size": 4,
            "top_n": 2,
            "quantile": 0.34,
            "min_abs_score": 0.0,
            "rebalance_interval_minutes": 60,
        },
    )

    async def fake_load_universe_frames(universe):
        return {sym: frames[sym] for sym in universe if sym in frames}

    to_thread_calls = []

    async def fake_to_thread(func, *args, **kwargs):
        to_thread_calls.append(getattr(func, "__name__", "unknown"))
        return func(*args, **kwargs)

    monkeypatch.setattr(strategy, "_load_universe_frames", fake_load_universe_frames)
    monkeypatch.setattr(fama_factor_module.asyncio, "to_thread", fake_to_thread)

    signals = asyncio.run(strategy.generate_signals_async(symbols[0]))

    assert to_thread_calls == ["_build_fama_rebalance_plan"]
    assert isinstance(signals, list)
    assert strategy._last_rebalance_at is not None


def test_backtest_core_stop_take_switch_forces_protective_exits():
    df = _trend_ohlcv(rows=260)
    params = {"fast_period": 3, "slow_period": 8}
    base = _run_backtest_core(
        strategy="EMAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000,
        params=params,
        include_series=False,
    )
    with_protection = _run_backtest_core(
        strategy="EMAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000,
        params=params,
        include_series=False,
        use_stop_take=True,
        stop_loss_pct=0.001,
        take_profit_pct=0.001,
    )

    assert base["use_stop_take"] is False
    assert with_protection["use_stop_take"] is True
    assert with_protection["forced_protective_exits"] > 0
    assert with_protection["take_profit_pct"] == 0.001
    assert with_protection["stop_loss_pct"] == 0.001


def test_optimize_can_include_stop_take_params_when_enabled():
    df = _trend_ohlcv(rows=260)
    result = _optimize_strategy_on_df(
        strategy="EMAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000,
        commission_rate=0.0004,
        slippage_bps=2.0,
        objective="total_return",
        max_trials=18,
        use_stop_take=True,
        stop_loss_pct=0.01,
        take_profit_pct=0.03,
    )
    assert int(result.get("trials") or 0) > 0
    top_rows = list(result.get("top") or [])
    assert top_rows
    best_params = dict((result.get("best") or {}).get("params") or {})
    assert "stop_loss_pct" in best_params
    assert "take_profit_pct" in best_params
    best_metrics = dict((result.get("best") or {}).get("metrics") or {})
    assert best_metrics.get("stop_loss_pct") == best_params.get("stop_loss_pct")
    assert best_metrics.get("take_profit_pct") == best_params.get("take_profit_pct")

    for row in list(result.get("top") or [])[:5]:
        params = dict(row.get("params") or {})
        metrics = dict(row.get("metrics") or {})
        assert metrics.get("stop_loss_pct") == params.get("stop_loss_pct")
        assert metrics.get("take_profit_pct") == params.get("take_profit_pct")
