from __future__ import annotations

import asyncio
import json

import numpy as np
import pandas as pd


def _pair_frame(close: pd.Series, symbol: str) -> pd.DataFrame:
    close = pd.to_numeric(close, errors="coerce")
    open_ = close.shift(1).fillna(close.iloc[0])
    high = close + 0.35
    low = close - 0.35
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": np.full(len(close), 1000.0),
            "symbol": [symbol] * len(close),
        },
        index=close.index,
    )


def _mean_reverting_pair_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    idx = pd.date_range(start="2025-01-01", periods=120, freq="h")
    base = pd.Series(100 + np.linspace(0, 16, len(idx)) + np.sin(np.linspace(0, 9, len(idx))), index=idx)
    spread = pd.Series(np.random.default_rng(seed=12).normal(0, 0.08, len(idx)), index=idx)
    spread.iloc[70] = -6.0
    spread.iloc[71] = -3.0
    spread.iloc[72] = -1.1
    spread.iloc[73] = -0.2
    spread.iloc[74] = 0.0
    spread.iloc[75] = 0.1
    primary = pd.Series(base.values * 1.18 + spread.values, index=idx)
    secondary = base
    return _pair_frame(primary, "AAA/USDT"), _pair_frame(secondary, "BBB/USDT")


def _rename_symbol(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    renamed = df.copy()
    renamed["symbol"] = symbol
    return renamed


def test_run_backtest_custom_pairs_strategy_uses_dual_leg_spread(monkeypatch):
    from web.api import backtest as backtest_api

    primary_df, pair_df = _mean_reverting_pair_frames()

    async def fake_load_backtest_df(symbol: str, timeframe: str, start_time=None, end_time=None):
        mapping = {
            "AAA/USDT": primary_df,
            "BBB/USDT": pair_df,
        }
        return mapping.get(str(symbol), pd.DataFrame()).copy()

    monkeypatch.setattr(backtest_api, "_load_backtest_df", fake_load_backtest_df)

    payload = asyncio.run(
        backtest_api.run_backtest_custom(
            strategy="PairsTradingStrategy",
            symbol="AAA/USDT",
            timeframe="1h",
            initial_capital=10000,
            include_series=True,
            params_json=json.dumps(
                {
                    "pair_symbol": "BBB/USDT",
                    "lookback_period": 30,
                    "entry_z_score": 1.5,
                    "exit_z_score": 0.5,
                    "hedge_ratio_method": "ols",
                    "allow_negative_hedge_ratio": True,
                    "min_hedge_ratio": -5.0,
                    "max_hedge_ratio": 5.0,
                }
            ),
        )
    )

    assert payload["portfolio_mode"] == "pairs_spread_dual_leg"
    assert payload["symbol"] == "AAA/USDT"
    assert payload["pair_symbol"] == "BBB/USDT"
    assert payload["data_points"] >= 100
    assert payload["pair_data_points"] >= 100
    assert payload["total_trades"] >= 1
    assert payload["entry_signals"] >= 1
    assert payload["exit_signals"] >= 1
    assert payload["trade_points"]["open_points"]
    assert payload["trade_points"]["close_points"]
    assert payload["series"]
    assert "pair_close" in payload["series"][0]
    assert "spread" in payload["series"][0]
    assert payload["hedge_ratio_last"] > 0
    assert payload["pair_regime"] == "positive_corr"
    assert payload["common_pnl"]["metadata"]["pair_symbol"] == "BBB/USDT"


def test_run_backtest_custom_pairs_strategy_auto_switches_counter_leg(monkeypatch):
    from web.api import backtest as backtest_api

    primary_df, pair_df = _mean_reverting_pair_frames()
    primary_df = _rename_symbol(primary_df, "ETH/USDT")
    pair_df = _rename_symbol(pair_df, "BTC/USDT")

    async def fake_load_backtest_df(symbol: str, timeframe: str, start_time=None, end_time=None):
        mapping = {
            "ETH/USDT": primary_df,
            "BTC/USDT": pair_df,
        }
        return mapping.get(str(symbol), pd.DataFrame()).copy()

    monkeypatch.setattr(backtest_api, "_load_backtest_df", fake_load_backtest_df)

    payload = asyncio.run(
        backtest_api.run_backtest_custom(
            strategy="PairsTradingStrategy",
            symbol="ETH/USDT",
            timeframe="1h",
            initial_capital=10000,
            include_series=False,
            params_json=json.dumps(
                {
                    "lookback_period": 30,
                    "entry_z_score": 1.5,
                    "exit_z_score": 0.5,
                    "hedge_ratio_method": "ols",
                }
            ),
        )
    )

    assert payload["portfolio_mode"] == "pairs_spread_dual_leg"
    assert payload["symbol"] == "ETH/USDT"
    assert payload["pair_symbol"] == "BTC/USDT"
    assert payload["total_trades"] >= 1


def test_compare_backtests_supports_pairs_strategy_dual_leg_mode(monkeypatch):
    from web.api import backtest as backtest_api

    primary_df, pair_df = _mean_reverting_pair_frames()
    original_defaults = backtest_api.get_strategy_defaults

    async def fake_load_backtest_df(symbol: str, timeframe: str, start_time=None, end_time=None):
        mapping = {
            "AAA/USDT": primary_df,
            "BBB/USDT": pair_df,
        }
        return mapping.get(str(symbol), pd.DataFrame()).copy()

    monkeypatch.setattr(backtest_api, "_load_backtest_df", fake_load_backtest_df)
    monkeypatch.setattr(
        backtest_api,
        "get_strategy_defaults",
        lambda strategy: (
            {**dict(original_defaults(strategy) or {}), "pair_symbol": "BBB/USDT"}
            if strategy == "PairsTradingStrategy"
            else original_defaults(strategy)
        ),
    )

    payload = asyncio.run(
        backtest_api.compare_backtests(
            strategies="PairsTradingStrategy",
            symbol="AAA/USDT",
            timeframe="1h",
            initial_capital=10000,
            pre_optimize=False,
        )
    )

    assert payload["results"]
    row = payload["results"][0]
    assert row["strategy"] == "PairsTradingStrategy"
    assert row["portfolio_mode"] == "pairs_spread_dual_leg"
    assert row["pair_symbol"] == "BBB/USDT"


def test_compare_backtests_keeps_pairs_strategy_when_primary_matches_default_pair(monkeypatch):
    from web.api import backtest as backtest_api

    primary_df, pair_df = _mean_reverting_pair_frames()
    primary_df = _rename_symbol(primary_df, "ETH/USDT")
    pair_df = _rename_symbol(pair_df, "BTC/USDT")

    async def fake_load_backtest_df(symbol: str, timeframe: str, start_time=None, end_time=None):
        mapping = {
            "ETH/USDT": primary_df,
            "BTC/USDT": pair_df,
        }
        return mapping.get(str(symbol), pd.DataFrame()).copy()

    monkeypatch.setattr(backtest_api, "_load_backtest_df", fake_load_backtest_df)

    payload = asyncio.run(
        backtest_api.compare_backtests(
            strategies="PairsTradingStrategy,MAStrategy",
            symbol="ETH/USDT",
            timeframe="1h",
            initial_capital=10000,
            pre_optimize=False,
        )
    )

    rows = {row["strategy"]: row for row in payload["results"]}
    assert "error" not in rows["PairsTradingStrategy"]
    assert rows["PairsTradingStrategy"]["portfolio_mode"] == "pairs_spread_dual_leg"
    assert rows["PairsTradingStrategy"]["pair_symbol"] == "BTC/USDT"
