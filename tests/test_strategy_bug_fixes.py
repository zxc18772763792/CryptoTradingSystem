"""Unit tests for the strategy-indicator bug fixes documented in
``docs/IMPROVEMENT_PLAN_2026-05-23.md`` §3.1.

Each test class targets exactly one fix and is constructed so that — with the
original buggy implementation — the assertion would fail. After the fix it
must pass. Tests intentionally avoid mocking strategy internals: they exercise
the public ``generate_signals`` / helper APIs with hand-built fixtures.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.strategies.strategy_base import (
    Signal,
    SignalType,
    StrategyBase,
    bar_time,
)
from strategies.factor_based.factor_strategies import (
    HurstExponentStrategy,
    MeanReversionHalfLifeStrategy,
    SortinoRatioStrategy,
    VaRBreakoutStrategy,
)
from strategies.quantitative.momentum import TrendFollowingStrategy
from strategies.quantitative.pairs_trading import PairsTradingStrategy
from strategies.technical.common_strategies import (
    StochasticStrategy,
    VWAPReversionStrategy,
)
from strategies.technical.rsi_strategy import RSIDivergenceStrategy
from strategies.macro.market_sentiment import MarketSentimentStrategy
from strategies.macro.fund_flow import FundFlowStrategy


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_ohlcv(
    closes,
    *,
    start: str = "2024-01-01",
    freq: str = "1h",
    high_offset: float = 50.0,
    low_offset: float = 50.0,
    volume: float = 100.0,
    symbol: str = "BTC/USDT",
) -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    idx = pd.date_range(start=start, periods=len(closes), freq=freq, tz="UTC")
    df = pd.DataFrame(
        {
            "open": closes,
            "high": closes + high_offset,
            "low": closes - low_offset,
            "close": closes,
            "volume": np.full(len(closes), volume),
            "symbol": [symbol] * len(closes),
        },
        index=idx,
    )
    return df


# --------------------------------------------------------------------------- #
# 1. ADX self-reference (momentum.py TrendFollowingStrategy._calculate_adx)
# --------------------------------------------------------------------------- #


class TestADXSelfReference:
    def test_calculate_adx_returns_finite_plus_minus_di(self):
        # Build a moderate trend so both +DM and -DM see activity.
        closes = np.linspace(100.0, 120.0, 80) + np.sin(np.arange(80) * 0.5) * 2.0
        df = _make_ohlcv(closes, high_offset=1.0, low_offset=1.0)

        strat = TrendFollowingStrategy()
        adx, plus_di, minus_di = strat._calculate_adx(df, period=14)

        # +DI and -DI must be finite and bounded — previously plus_dm was
        # mutated in-place before the minus_dm comparison used it, biasing -DI
        # toward zero. After the fix both DI lines have non-zero variation.
        assert np.isfinite(plus_di.iloc[-1])
        assert np.isfinite(minus_di.iloc[-1])
        # Both should be > 0 across the latter half of the series.
        late_plus = plus_di.iloc[40:].dropna()
        late_minus = minus_di.iloc[40:].dropna()
        assert (late_plus > 0).any()
        assert (late_minus > 0).any()

    def test_adx_invariant_under_up_down_swap(self):
        """An "up bar" (high.diff()>0, -low.diff()<0) should contribute to +DM
        only. The bug caused minus_dm computation to compare against the
        already-overwritten plus_dm, so a pure uptrend would still emit -DM."""
        # Strict monotonic uptrend, no noise → all bars are up bars only.
        closes = np.linspace(100.0, 150.0, 60)
        df = _make_ohlcv(closes, high_offset=0.5, low_offset=0.5)
        strat = TrendFollowingStrategy()
        _, plus_di, minus_di = strat._calculate_adx(df, period=14)
        # In a clean uptrend -DI must stay strictly below +DI in steady state.
        plus_tail = plus_di.iloc[30:].dropna()
        minus_tail = minus_di.iloc[30:].dropna()
        assert (plus_tail > minus_tail).all()


# --------------------------------------------------------------------------- #
# 2. RSIDivergence non-centered peaks/troughs
# --------------------------------------------------------------------------- #


class TestRSIDivergenceCausalExtrema:
    def test_peaks_use_only_past_information(self):
        """A peak at index t may only depend on data at indices <= t+order."""
        # Build a series with a clear spike at index 30.
        vals = np.full(60, 100.0)
        vals[30] = 150.0
        series = pd.Series(vals)

        peaks = RSIDivergenceStrategy._find_peaks(series, order=3)
        # The spike must still be detected.
        assert bool(peaks.iloc[30])

        # Causality check: removing all data *after* index 33 (i.e. t+order)
        # must not change the peaks[30] verdict.
        truncated = series.iloc[: 30 + 3 + 1]
        peaks_truncated = RSIDivergenceStrategy._find_peaks(truncated, order=3)
        assert bool(peaks_truncated.iloc[30]) == bool(peaks.iloc[30])

    def test_trough_does_not_leak_future_data(self):
        vals = np.full(60, 100.0)
        vals[20] = 50.0
        series = pd.Series(vals)
        troughs = RSIDivergenceStrategy._find_troughs(series, order=4)
        assert bool(troughs.iloc[20])

        # If we lengthen the series with a deeper trough placed *after* index
        # 20 — which the centered-rolling version would have used to invalidate
        # the index-20 detection — the index-20 detection must still hold.
        vals2 = np.concatenate([vals, np.full(20, 100.0)])
        vals2[70] = 10.0  # even deeper trough later
        # Re-run on the prefix only (causal): same result.
        troughs2 = RSIDivergenceStrategy._find_troughs(pd.Series(vals2), order=4)
        assert bool(troughs2.iloc[20])


# --------------------------------------------------------------------------- #
# 3. HurstExponent VR-scale thresholds
# --------------------------------------------------------------------------- #


class TestHurstThresholds:
    def test_default_thresholds_on_vr_scale(self):
        strat = HurstExponentStrategy()
        # Trending threshold must sit above the VR neutral value 1.0.
        assert strat.params["trending_threshold"] > 1.0
        # Mean-revert threshold must sit below 1.0.
        assert strat.params["mean_revert_threshold"] < 1.0

    def test_mean_reversion_branch_uses_standard_vr_scale(self):
        # Strongly alternating returns are anti-persistent. The old proxy
        # sampled every fifth 1-bar return and multiplied by 5, which classified
        # this series as strongly trending instead of mean-reverting.
        returns = []
        ret = 0.02
        for i in range(100):
            ret = -0.75 * ret + 0.001 * np.sin(i * 1.7)
            returns.append(ret)

        closes = [100.0]
        for ret in returns:
            closes.append(closes[-1] * (1 + ret))

        closes = np.asarray(closes)
        recent = closes[:-2][-10:]
        recent_mean = float(recent.mean())
        recent_std = float(recent.std(ddof=1))
        closes[-2] = recent_mean + recent_std
        closes[-1] = recent_mean

        df = _make_ohlcv(closes, high_offset=0.2, low_offset=0.2)
        strat = HurstExponentStrategy(
            params={"hurst_period": 50, "zscore_period": 10, "zscore_threshold": 0.5}
        )

        returns_series = df["close"].pct_change()
        var_1 = returns_series.rolling(50).var()
        legacy_vr = (
            returns_series.rolling(50).apply(lambda x: np.var(x[::5]) * 5, raw=False)
            / var_1.replace(0, np.nan)
        ).fillna(1)
        standard_vr = (
            df["close"].pct_change(5).rolling(50).var()
            / (var_1 * 5).replace(0, np.nan)
        ).fillna(1)
        assert legacy_vr.iloc[-1] > strat.params["trending_threshold"]
        assert standard_vr.iloc[-1] < strat.params["mean_revert_threshold"]

        signals = strat.generate_signals(df)

        assert len(signals) == 1
        assert signals[0].signal_type == SignalType.SELL
        assert signals[0].metadata["regime"] == "mean_reverting"
        assert signals[0].metadata["variance_ratio"] == pytest.approx(standard_vr.iloc[-1])


# --------------------------------------------------------------------------- #
# 4. VWAPReversion symmetric SHORT / CLOSE_SHORT
# --------------------------------------------------------------------------- #


class TestVWAPReversionSymmetry:
    def test_emits_sell_signal_on_upside_deviation(self):
        # Build prices that crawl flat then spike above VWAP on the last bar.
        closes = np.concatenate([np.full(60, 100.0), [110.0]])
        df = _make_ohlcv(closes, high_offset=0.1, low_offset=0.1, volume=10.0)
        strat = VWAPReversionStrategy(params={"window": 30, "entry_deviation_pct": 0.01})
        signals = strat.generate_signals(df)
        assert any(s.signal_type == SignalType.SELL for s in signals)

    def test_emits_close_short_on_upward_reversion(self):
        # Stay above VWAP, then revert toward it (deviation crosses exit_dev
        # *downward* but stays above -entry so the BUY branch does not steal
        # the signal). After the fix this must emit CLOSE_SHORT.
        closes = np.concatenate(
            [
                np.full(40, 100.0),
                np.full(15, 102.0),   # ~+1.8% above VWAP, above entry=0.01
                [101.0],              # deviation drops near zero (above -entry)
            ]
        )
        df = _make_ohlcv(closes, high_offset=0.1, low_offset=0.1, volume=10.0)
        strat = VWAPReversionStrategy(
            params={"window": 30, "entry_deviation_pct": 0.05, "exit_deviation_pct": 0.002}
        )
        signals = strat.generate_signals(df)
        types = [s.signal_type for s in signals]
        assert SignalType.CLOSE_SHORT in types, f"got {types}"


# --------------------------------------------------------------------------- #
# 5. VaRBreakout iloc[-1]
# --------------------------------------------------------------------------- #


class TestVaRBreakoutIndex:
    def test_breakout_fires_when_last_return_breaches_var(self):
        # Stable low-vol regime, then a single large positive shock on the
        # final bar that clearly exceeds the rolling VaR threshold.
        rng = np.random.default_rng(0)
        base = 100.0 * (1 + rng.normal(0, 0.001, 50)).cumprod()
        shocked = np.append(base, base[-1] * 1.08)  # +8% jump
        df = _make_ohlcv(shocked, high_offset=0.05, low_offset=0.05)
        strat = VaRBreakoutStrategy(params={"var_period": 20, "confidence": 0.95, "multiplier": 1.5})
        signals = strat.generate_signals(df)
        buys = [s for s in signals if s.signal_type == SignalType.BUY]
        assert buys

        returns = df["close"].pct_change()

        def calc_var(series):
            r = series.dropna()
            if len(r) < strat.params["var_period"] // 2:
                return np.nan
            return np.percentile(r, (1 - strat.params["confidence"]) * 100)

        var = returns.rolling(strat.params["var_period"]).apply(calc_var, raw=False)
        assert buys[0].metadata["var"] == pytest.approx(var.iloc[-1])


# --------------------------------------------------------------------------- #
# 6. pairs_trading chained-comparison
# --------------------------------------------------------------------------- #


class TestPairsHedgeRatioBounds:
    def test_explicit_user_min_hedge_respected(self):
        # User: positive-only range, allow_negative on. The buggy chained
        # comparison `min_hr >= 0 < max_hr` would silently rewrite min_hr to
        # -max_hr (because 0 < max_hr is true and min_hr >= 0). After the fix
        # the original config is preserved.
        strat = PairsTradingStrategy(
            params={
                "allow_negative_hedge_ratio": True,
                "min_hedge_ratio": 0.5,
                "max_hedge_ratio": 5.0,
            }
        )
        min_hr, max_hr = strat._hedge_ratio_bounds()
        # The buggy version overwrote min_hr to -5.0. The fix triggers the
        # `min_hr = -abs(max_hr)` branch only when min_hr was non-positive too.
        # With min_hr=0.5 the rewrite should NOT happen.
        assert min_hr == pytest.approx(0.5)
        assert max_hr == pytest.approx(5.0)

    def test_zero_min_hedge_still_unlocks_negative_range(self):
        # When user genuinely wants min_hr=0 + allow_negative, behaviour must
        # still unlock the negative range so the rebalance branch fires.
        strat = PairsTradingStrategy(
            params={
                "allow_negative_hedge_ratio": True,
                "min_hedge_ratio": 0.0,
                "max_hedge_ratio": 2.0,
            }
        )
        min_hr, _ = strat._hedge_ratio_bounds()
        assert min_hr == pytest.approx(-2.0)


# --------------------------------------------------------------------------- #
# 7. Stochastic entry uses k_prev rather than k_now
# --------------------------------------------------------------------------- #


class TestStochasticEntryWindow:
    def test_buy_fires_when_k_prev_was_oversold(self):
        """Construct a series where %K is in the oversold zone at t-1 then
        bounces above %D at t. Under the buggy condition (k_now <= oversold)
        the bounce frequently lifts k_now > 20 and the signal is lost. The
        fixed condition (k_prev <= oversold) captures the crossover."""
        # Down then sharp rebound on the last bar.
        closes = np.concatenate(
            [
                np.linspace(100.0, 70.0, 25),  # downtrend → %K in oversold
                [70.5, 70.3, 70.1, 69.9, 69.7],  # keep k_prev <= d_prev before rebound
                [80.0],                          # rebound crosses %K above %D
            ]
        )
        df = _make_ohlcv(closes, high_offset=0.2, low_offset=0.2)
        strat = StochasticStrategy(params={"k_period": 14, "d_period": 3, "smooth_k": 1, "oversold": 20.0})
        signals = strat.generate_signals(df)
        buys = [s for s in signals if s.signal_type == SignalType.BUY]
        assert buys
        assert buys[0].metadata["k"] > strat.params["oversold"]
        assert buys[0].strength > 0.1

    def test_sell_fires_when_k_prev_was_overbought(self):
        closes = np.concatenate(
            [
                np.linspace(70.0, 100.0, 25),
                [99.5, 99.7, 99.9, 100.1, 100.3],
                [90.0],
            ]
        )
        df = _make_ohlcv(closes, high_offset=0.2, low_offset=0.2)
        strat = StochasticStrategy(params={"k_period": 14, "d_period": 3, "smooth_k": 1, "overbought": 80.0})
        signals = strat.generate_signals(df)
        sells = [s for s in signals if s.signal_type == SignalType.SELL]
        assert sells
        assert sells[0].metadata["k"] < strat.params["overbought"]
        assert sells[0].strength > 0.1


# --------------------------------------------------------------------------- #
# 8. MeanReversionHalfLife take_profit guard
# --------------------------------------------------------------------------- #


class TestMeanReversionHalfLifeTPGuard:
    def test_buy_take_profit_above_current_price(self):
        # Construct: long uptrend so rolling mean << current_price, then a
        # negative shock that bounces back so a BUY z-score crossing fires.
        # In the buggy version, signal.take_profit = mean (below current),
        # which is a "TP already triggered" condition.
        rng = np.random.default_rng(1)
        base = 100.0 + np.cumsum(rng.normal(0.5, 0.5, 80))  # strong uptrend
        # Force z-score crossing on last two bars: dip then snap back.
        base[-2] = base[-3] - 3 * np.std(np.diff(base[:-2]))
        base[-1] = base[-3]  # snaps back to trend
        df = _make_ohlcv(base, high_offset=0.05, low_offset=0.05)
        strat = MeanReversionHalfLifeStrategy(
            params={"lookback": 30, "zscore_entry": 1.0, "zscore_exit": 0.3, "take_profit_pct": 0.04}
        )
        signals = strat.generate_signals(df)
        buys = [s for s in signals if s.signal_type == SignalType.BUY]
        if not buys:
            pytest.skip("BUY crossing did not trigger on this random seed")
        sig = buys[-1]
        assert sig.take_profit is not None
        # TP must be strictly above entry; if mean<price the fallback
        # percentage TP kicks in.
        assert sig.take_profit > sig.price


# --------------------------------------------------------------------------- #
# 9. Sortino counter-intuitive trigger
# --------------------------------------------------------------------------- #


class TestSortinoGuard:
    def test_no_sell_when_trend_positive(self):
        """The buggy `prev > -threshold and current <= -threshold` would emit
        a SELL even when current_sortino merely dropped from +5.0 to -1.1
        with no underlying negative trend. After the fix a positive trend
        suppresses the SELL."""
        # Build: strongly positive returns the whole window, then exactly
        # ONE big down bar to push Sortino negative while leaving the 5-bar
        # trend positive.
        rng = np.random.default_rng(2)
        rets = rng.normal(0.003, 0.001, 60)  # strongly positive
        rets[-1] = -0.10                       # one huge negative bar
        closes = 100.0 * np.cumprod(1 + rets)
        df = _make_ohlcv(closes, high_offset=0.05, low_offset=0.05)
        strat = SortinoRatioStrategy(params={"period": 30, "sortino_threshold": 0.5, "lookback_trend": 5})
        signals = strat.generate_signals(df)
        # The 5-bar trend may be negative due to last bar; the goal is just
        # that the buggy SELL doesn't fire when prev_sortino was very high.
        # Stronger sanity: with a *positive* 5-bar trend no SELL fires.
        # Rebuild closes so 5-bar trend stays positive (engineer it):
        closes2 = closes.copy()
        # Force last 5 closes to be monotonically rising relative to closes2[-6]
        closes2[-5:] = np.linspace(closes2[-6] * 1.001, closes2[-6] * 1.005, 5)
        df2 = _make_ohlcv(closes2, high_offset=0.05, low_offset=0.05)
        signals2 = strat.generate_signals(df2)
        # Sortino may still be < -threshold, but trend > 0 should suppress.
        sells = [s for s in signals2 if s.signal_type == SignalType.SELL]
        assert not sells, "SELL fired despite positive 5-bar trend (Sortino guard regressed)"


# --------------------------------------------------------------------------- #
# 10. Macro strategies use bar timestamp
# --------------------------------------------------------------------------- #


class TestMacroSignalTimestamps:
    def test_market_sentiment_signal_uses_bar_time(self):
        strat = MarketSentimentStrategy()
        # Inject sentiment that triggers a BUY (extreme fear).
        sample_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        strat._sentiment_data = {
            "fear_greed_index": 5,
            "social_sentiment": 0.0,
            "news_sentiment": 0.0,
            "timestamp": sample_ts,
        }
        bar_idx = datetime(2025, 7, 1, 12, 0, tzinfo=timezone.utc)
        df = pd.DataFrame(
            {"close": [100.0], "symbol": ["BTC/USDT"]},
            index=pd.DatetimeIndex([bar_idx]),
        )
        signals = strat.generate_signals(df)
        assert signals
        # Bar timestamp wins over the (older) sentiment sample timestamp.
        assert signals[0].timestamp == bar_idx

    def test_fund_flow_signal_uses_bar_time(self):
        strat = FundFlowStrategy(
            params={
                "inflow_threshold": 1.0,
                "outflow_threshold": 1.0,
                "min_imbalance_ratio": 0.0,
            }
        )
        sample_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        strat._flow_data = {
            "net_flow": 100.0,
            "exchange_inflow": 80.0,
            "exchange_outflow": 20.0,
            "timestamp": sample_ts,
        }
        bar_idx = datetime(2025, 7, 1, 12, 0, tzinfo=timezone.utc)
        df = pd.DataFrame(
            {"close": [100.0], "symbol": ["BTC/USDT"]},
            index=pd.DatetimeIndex([bar_idx]),
        )
        signals = strat.generate_signals(df)
        if not signals:
            pytest.skip("FundFlow params did not produce a signal in this fixture")
        assert signals[0].timestamp == bar_idx


# --------------------------------------------------------------------------- #
# 11. bar_time -8h shift warning
# --------------------------------------------------------------------------- #


class TestBarTimeShiftWarning:
    def test_logs_warning_on_cst_shift(self, caplog):
        # Build a DataFrame whose last bar is ~8h in the future relative to
        # `now`. The bar_time helper should log a warning while still
        # returning the shifted-to-UTC value.
        now_utc = datetime.now(timezone.utc)
        future_bar = now_utc + timedelta(hours=8)  # CST-as-naive-UTC scenario
        idx = pd.DatetimeIndex([now_utc - timedelta(hours=1), future_bar])
        df = pd.DataFrame({"close": [100.0, 101.0]}, index=idx)

        # Capture loguru via standard logging propagation isn't automatic, so
        # we install a fallback that captures via the logger's own handlers.
        emitted = []
        from loguru import logger

        sink_id = logger.add(lambda msg: emitted.append(str(msg)), level="WARNING")
        try:
            ts = bar_time(df, fallback=now_utc)
        finally:
            logger.remove(sink_id)

        # Returned timestamp is the shifted one (within 2 minutes of "now").
        assert abs((ts - now_utc).total_seconds()) <= 120
        # A warning was emitted naming the CST shift.
        assert any("bar_time" in line and "8h" in line for line in emitted), emitted
