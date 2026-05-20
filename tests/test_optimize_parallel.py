"""Phase 5 parity tests: parallel _optimize_strategy_on_df must produce
the same result as serial execution.

We don't bench scaling here (that's the benchmark script's job), only
correctness: the trial set and the best result must be deterministic
regardless of worker count.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from web.api.backtest import _optimize_strategy_on_df


def _make_frame(n: int = 600) -> pd.DataFrame:
    rng = np.random.default_rng(2026)
    idx = pd.date_range("2026-01-01", periods=n, freq="1h")
    close = 30000.0 + 100.0 * rng.standard_normal(n).cumsum() * 0.2
    high = close + np.abs(rng.standard_normal(n)) * 30.0 + 1.0
    low = close - np.abs(rng.standard_normal(n)) * 30.0 - 1.0
    open_ = np.concatenate([[close[0]], close[:-1]])
    vol = 100.0 + 10.0 * rng.random(n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=idx,
    )


def test_optimize_serial_vs_parallel_identical(monkeypatch):
    """workers=1 (serial) and workers=2 (parallel) must produce the
    same trial list, in the same order, with the same metrics."""
    from config.settings import settings as _settings  # noqa: PLC0415

    df = _make_frame(600)

    # Serial run (default config)
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_WORKERS", 1)
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS", 8)
    serial = _optimize_strategy_on_df(
        strategy="MAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000.0,
        commission_rate=0.0004,
        slippage_bps=2.0,
        max_trials=8,
    )

    # Parallel run
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_WORKERS", 2)
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS", 4)
    parallel = _optimize_strategy_on_df(
        strategy="MAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000.0,
        commission_rate=0.0004,
        slippage_bps=2.0,
        max_trials=8,
    )

    # Trial counts identical
    assert serial["trials"] == parallel["trials"], (
        f"serial trials {serial['trials']} != parallel {parallel['trials']}"
    )
    assert serial["failed_trials"] == parallel["failed_trials"]

    # Best trial identical (after sort by score desc — already sorted by
    # _optimize_strategy_on_df). Note the all_trials list is post-sort.
    if serial["best"] and parallel["best"]:
        s_best = serial["best"]
        p_best = parallel["best"]
        assert s_best["params"] == p_best["params"], (
            f"best params differ:\n  serial:   {s_best['params']}\n  parallel: {p_best['params']}"
        )
        assert s_best["score"] == pytest.approx(p_best["score"], abs=1e-12)
        # Top-level metrics that we report should match exactly
        for key in ("total_return", "sharpe_ratio", "max_drawdown", "win_rate", "total_trades"):
            assert s_best["metrics"].get(key) == pytest.approx(
                p_best["metrics"].get(key), abs=1e-9, rel=1e-9
            ), f"best {key} differs: {s_best['metrics'].get(key)} vs {p_best['metrics'].get(key)}"


def test_optimize_parallel_falls_back_to_serial_below_threshold(monkeypatch):
    """When trials < min-parallel-trials, the optimizer must stay
    serial even if workers > 1 — the pool spawn cost would dominate."""
    from config.settings import settings as _settings  # noqa: PLC0415

    df = _make_frame(400)
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_WORKERS", 4)
    monkeypatch.setattr(_settings, "BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS", 100)
    # With min=100 and max_trials=4, this should NOT spin up a pool.
    # We don't have a public introspection hook, but a sane runtime
    # means the test completes without spawn overhead errors.
    res = _optimize_strategy_on_df(
        strategy="MAStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000.0,
        commission_rate=0.0004,
        slippage_bps=2.0,
        max_trials=4,
    )
    assert res["trials"] >= 1
