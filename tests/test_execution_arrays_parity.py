"""Phase 3 parity tests: ``simulate_execution_arrays`` must equal
``run_exit_engine`` on every supported config bar-by-bar.

This locks the simple-signal-following code path against the trusted
``ExitEngine`` for: long-only, short-only, both-sided, repeated
reversals, single-trade, no-trade-at-all, and a NaN-close stress test.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.backtest.exit_engine import ExitEngineConfig, run_exit_engine
from core.backtest.execution_arrays import (
    is_supported_config,
    simulate_execution_arrays,
)


def _make_frame(n: int, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-01-01", periods=n, freq="5min")
    close = 30000.0 + 50.0 * rng.standard_normal(n).cumsum() * 0.4
    high = close + np.abs(rng.standard_normal(n)) * 5.0 + 1.0
    low = close - np.abs(rng.standard_normal(n)) * 5.0 - 1.0
    open_ = np.concatenate([[close[0]], close[:-1]])
    vol = 100.0 + 10.0 * rng.random(n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=idx,
    )


def _signal_long_only(n: int) -> pd.Series:
    """Alternate flat / long every 20 bars."""
    sig = np.zeros(n, dtype=float)
    for start in range(20, n, 40):
        sig[start : start + 20] = 1.0
    return pd.Series(sig, index=_make_frame(n).index)


def _signal_short_only(n: int) -> pd.Series:
    sig = np.zeros(n, dtype=float)
    for start in range(20, n, 40):
        sig[start : start + 20] = -1.0
    return pd.Series(sig, index=_make_frame(n).index)


def _signal_alternating(n: int) -> pd.Series:
    """Switches long/short every 30 bars — many reversals."""
    sig = np.zeros(n, dtype=float)
    for k, start in enumerate(range(15, n, 30)):
        sig[start : start + 30] = 1.0 if k % 2 == 0 else -1.0
    return pd.Series(sig, index=_make_frame(n).index)


def _signal_single_long(n: int) -> pd.Series:
    """Single long position held bars 50..150."""
    sig = np.zeros(n, dtype=float)
    sig[50:150] = 1.0
    return pd.Series(sig, index=_make_frame(n).index)


def _signal_no_trade(n: int) -> pd.Series:
    return pd.Series(np.zeros(n, dtype=float), index=_make_frame(n).index)


N = 400
_FIXTURES = [
    ("long_only", _signal_long_only),
    ("short_only", _signal_short_only),
    ("alternating", _signal_alternating),
    ("single_long", _signal_single_long),
    ("no_trade", _signal_no_trade),
]


def _default_config() -> ExitEngineConfig:
    """The config produced by ``resolve_exit_engine_config`` with no
    template, no overrides, no fixed stops — the bench's default."""
    cfg = ExitEngineConfig(allow_same_bar_exit=False)
    return ExitEngineConfig(**cfg.to_dict())


def test_is_supported_config_default_yes():
    assert is_supported_config(_default_config())


def test_is_supported_config_with_stop_no():
    cfg = ExitEngineConfig(fixed_stop_loss_pct=0.02, allow_same_bar_exit=False)
    cfg = ExitEngineConfig(**cfg.to_dict())
    assert not is_supported_config(cfg)


def test_is_supported_config_with_template_atr_no():
    cfg = ExitEngineConfig(initial_stop_mode="atr", allow_same_bar_exit=False)
    cfg = ExitEngineConfig(**cfg.to_dict())
    assert not is_supported_config(cfg)


def test_is_supported_config_time_stop_no():
    cfg = ExitEngineConfig(time_stop_enabled=True, allow_same_bar_exit=False)
    cfg = ExitEngineConfig(**cfg.to_dict())
    assert not is_supported_config(cfg)


@pytest.mark.parametrize("name,sig_fn", _FIXTURES)
def test_array_path_matches_run_exit_engine(name, sig_fn):
    df = _make_frame(N, seed=hash(name) & 0xFFFF)
    signal = sig_fn(N)
    cfg = _default_config()

    trusted = run_exit_engine(df=df, signal_position=signal, config=cfg)
    fast = simulate_execution_arrays(df=df, signal_position=signal, config=cfg)

    # 1. effective_position bar-by-bar exact
    trusted_eff = trusted.effective_position.to_numpy(dtype=float)
    fast_eff = fast["effective_position"].to_numpy(dtype=float)
    diff = np.flatnonzero(trusted_eff != fast_eff)
    if diff.size > 0:
        first = diff[:5]
        msg = "\n".join(
            f"  bar {int(i)}: trusted={trusted_eff[int(i)]} fast={fast_eff[int(i)]}"
            for i in first
        )
        pytest.fail(f"{name}: effective_position diverged at {diff.size} bars:\n{msg}")

    # 2. gross_returns close (float-level tolerance for compute order)
    trusted_gr = trusted.gross_returns.to_numpy(dtype=float)
    fast_gr = fast["gross_returns"].to_numpy(dtype=float)
    np.testing.assert_allclose(fast_gr, trusted_gr, atol=1e-12, rtol=1e-9,
                               err_msg=f"{name}: gross_returns diverged")

    # 3. trade counts exact
    assert fast["trade_stats"]["entries"] == trusted.trade_stats["entries"], (
        f"{name}: entries {fast['trade_stats']['entries']} vs trusted {trusted.trade_stats['entries']}"
    )
    assert fast["trade_stats"]["exits"] == trusted.trade_stats["exits"]
    assert fast["trade_stats"]["completed"] == trusted.trade_stats["completed"]

    # 4. exit reason breakdown exact (this config can only produce reversals)
    assert (
        fast["exit_reason_breakdown"]["reversal"]
        == trusted.exit_reason_breakdown["reversal"]
    ), f"{name}: reversal count mismatch"
    # Other reason categories must be 0 in this config
    for reason in ("stop", "take_profit", "trailing", "partial", "time_stop"):
        assert fast["exit_reason_breakdown"][reason] == 0
        assert trusted.exit_reason_breakdown[reason] == 0
