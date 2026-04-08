from __future__ import annotations

import asyncio

import numpy as np
import pandas as pd


def _compare_frame(rows: int = 288, symbol: str = "BTC/USDT") -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=rows, freq="5min")
    close = pd.Series(100 + np.linspace(0, 6, rows) + np.sin(np.linspace(0, 12, rows)), index=idx)
    return pd.DataFrame(
        {
            "open": close.shift(1).fillna(close.iloc[0]).values,
            "high": (close + 0.5).values,
            "low": (close - 0.5).values,
            "close": close.values,
            "volume": np.full(rows, 1000.0),
            "symbol": [symbol] * rows,
        },
        index=idx,
    )


def test_compare_optimization_plan_caps_intraday_large_sets():
    from web.api import backtest as backtest_api

    plan = backtest_api._build_compare_optimization_plan(
        strategy_count=9,
        eligible_count=9,
        timeframe="5m",
        data_points=288,
        requested_trials=48,
        pre_optimize=True,
    )

    assert plan["adaptive_capped"] is True
    assert plan["selected_count"] == 8
    assert plan["effective_trials"] == 24
    assert "快速预优化" in plan["summary"]


def test_compare_optimization_plan_keeps_requested_trials_for_small_sets():
    from web.api import backtest as backtest_api

    plan = backtest_api._build_compare_optimization_plan(
        strategy_count=3,
        eligible_count=3,
        timeframe="1h",
        data_points=720,
        requested_trials=48,
        pre_optimize=True,
    )

    assert plan["adaptive_capped"] is False
    assert plan["selected_count"] == 3
    assert plan["effective_trials"] == 48


def test_compare_backtests_limits_preoptimization_scope_for_intraday(monkeypatch):
    from web.api import backtest as backtest_api

    df = _compare_frame()
    strategies = [
        "MAStrategy",
        "EMAStrategy",
        "RSIStrategy",
        "MACDStrategy",
        "BollingerBandsStrategy",
        "MomentumStrategy",
        "TrendFollowingStrategy",
        "StochasticStrategy",
        "VWAPReversionStrategy",
    ]
    score_map = {name: float(len(strategies) - idx) for idx, name in enumerate(strategies)}
    optimize_calls: list[dict] = []

    async def fake_load_backtest_df(symbol: str, timeframe: str, start_time=None, end_time=None):
        return df.copy()

    async def fake_attach_backtest_enrichment_if_needed(strategy: str, df: pd.DataFrame, symbol: str, start_time=None, end_time=None):
        enriched = df.copy()
        enriched.attrs["strategy_family"] = "traditional"
        enriched.attrs["decision_engine"] = "rule"
        enriched.attrs["data_mode"] = "OHLCV"
        return enriched

    def fake_run_backtest_core(strategy: str, df: pd.DataFrame, timeframe: str, initial_capital: float, **kwargs):
        base = score_map[strategy]
        return {
            "strategy": strategy,
            "total_return": base / 100.0,
            "sharpe_ratio": base / 10.0,
            "max_drawdown": 0.02,
            "win_rate": 55.0,
            "total_trades": 8,
            "quality_flag": "ok",
            "recommended_min_bars": 72,
            "zero_trade_reason": "",
            "cost_drag_return_pct": 0.001,
        }

    def fake_optimize_strategy_on_df(strategy: str, df: pd.DataFrame, timeframe: str, initial_capital: float, max_trials: int, **kwargs):
        optimize_calls.append({"strategy": strategy, "max_trials": int(max_trials)})
        improved = fake_run_backtest_core(strategy, df, timeframe, initial_capital)
        improved["total_return"] += 0.01
        improved["sharpe_ratio"] += 0.1
        return {
            "strategy": strategy,
            "objective": "total_return",
            "trials": int(max_trials),
            "failed_trials": 0,
            "best": {
                "params": {"fast_period": 5},
                "metrics": improved,
                "score": improved["total_return"],
            },
            "top": [],
            "all_trials": [],
            "failures": [],
        }

    monkeypatch.setattr(backtest_api, "_load_backtest_df", fake_load_backtest_df)
    monkeypatch.setattr(backtest_api, "_attach_backtest_enrichment_if_needed", fake_attach_backtest_enrichment_if_needed)
    monkeypatch.setattr(backtest_api, "_run_backtest_core", fake_run_backtest_core)
    monkeypatch.setattr(backtest_api, "_optimize_strategy_on_df", fake_optimize_strategy_on_df)

    payload = asyncio.run(
        backtest_api.compare_backtests(
            strategies=",".join(strategies),
            symbol="BTC/USDT",
            timeframe="5m",
            initial_capital=10000,
            start_date="2025-01-01",
            end_date="2025-01-01",
            pre_optimize=True,
            optimize_max_trials=48,
        )
    )

    assert payload["compare_optimization"]["adaptive_capped"] is True
    assert payload["compare_optimization"]["selected_count"] == 8
    assert payload["compare_optimization"]["effective_trials"] == 24
    assert len(optimize_calls) == 8
    assert all(item["max_trials"] == 24 for item in optimize_calls)

    optimized_rows = [row for row in payload["results"] if row.get("optimization_applied")]
    skipped_rows = [row for row in payload["results"] if row.get("optimization_skipped_for_budget")]
    assert len(optimized_rows) == 8
    assert len(skipped_rows) == 1
    assert "快速预优化" in payload["compare_optimization"]["summary"]
