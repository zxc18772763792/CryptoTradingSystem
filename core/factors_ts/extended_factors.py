"""
Extended time-series factors based on academic research.

References:
- Jegadeesh & Titman (1993) - Momentum
- Amihud (2002) - Illiquidity
- Parkinson (1980) - Volatility Estimation
- Garman & Klass (1980) - Volatility Estimation
- Yang & Zhang (2000) - Volatility Estimation
- Kyle (1985) - Market Microstructure
- Easley et al. (2012) - VPIN
- Hurst (1951) - Long-term Memory
- Ornstein-Uhlenbeck - Mean Reversion
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any
import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


def _safe_std(x: pd.Series, window: int) -> pd.Series:
    return x.rolling(window, min_periods=max(3, min(window, 5))).std(ddof=0)


def _safe_mean(x: pd.Series, window: int) -> pd.Series:
    return x.rolling(window, min_periods=max(3, min(window, 5))).mean()


# ============================================================
# Momentum and Trend Factors
# ============================================================

@dataclass
class ROCFactor(TimeSeriesFactor):
    """Rate of Change (ROC) factor - classic momentum indicator.

    Source: Murphy, J. (1999). Technical Analysis of the Financial Markets.
    """
    def __init__(self, period: int = 14):
        period = int(period)
        super().__init__(
            name=f"roc_{period}",
            inputs=("close",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")
        return ((close - close.shift(n)) / close.shift(n) * 100).replace([np.inf, -np.inf], np.nan)


@dataclass
class PriceAccelerationFactor(TimeSeriesFactor):
    """Price acceleration factor - second derivative of price momentum.

    Measures the rate of change of momentum, identifying acceleration/deceleration.
    """
    def __init__(self, fast: int = 5, slow: int = 15):
        fast = int(fast)
        slow = int(slow)
        super().__init__(
            name=f"price_accel_{fast}_{slow}",
            inputs=("close",),
            lookback=slow * 2 + 5,
            params={"fast": fast, "slow": slow}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        close = pd.to_numeric(df["close"], errors="coerce")
        fast = int(self.params["fast"])
        slow = int(self.params["slow"])

        # First derivative: momentum
        fast_mom = close.pct_change(fast)
        slow_mom = close.pct_change(slow)

        # Second derivative: acceleration (fast momentum - slow momentum)
        accel = (fast_mom - slow_mom) / slow_mom.replace(0, np.nan).abs()
        return accel.replace([np.inf, -np.inf], np.nan)


@dataclass
class AroonFactor(TimeSeriesFactor):
    """Aroon factor - measures trend strength and direction.

    Source: Tushar Chande (1995). Aroon indicator.
    Range: -100 to +100, positive = uptrend, negative = downtrend.
    """
    def __init__(self, period: int = 25):
        period = int(period)
        super().__init__(
            name=f"aroon_{period}",
            inputs=("high", "low"),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")

        # Aroon Up: periods since highest high
        # Aroon Down: periods since lowest low
        aroon_up = high.rolling(n + 1).apply(lambda x: (n - np.argmax(x)) / n * 100, raw=True)
        aroon_down = low.rolling(n + 1).apply(lambda x: (n - np.argmin(x)) / n * 100, raw=True)

        # Aroon Oscillator
        return aroon_up - aroon_down


@dataclass
class KAMAFactor(TimeSeriesFactor):
    """Kaufman's Adaptive Moving Average factor.

    Source: Kaufman, P. (1995). Smarter Trading.
    Adapts to market volatility - more responsive in trending markets.
    """
    def __init__(self, period: int = 10, fast: int = 2, slow: int = 30):
        period = int(period)
        super().__init__(
            name=f"kama_{period}",
            inputs=("close",),
            lookback=period + 10,
            params={"period": period, "fast": fast, "slow": slow}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        close = pd.to_numeric(df["close"], errors="coerce")
        n = int(self.params["period"])
        fast = int(self.params["fast"])
        slow = int(self.params["slow"])

        # Efficiency Ratio (ER)
        change = (close - close.shift(n)).abs()
        volatility = (close - close.shift(1)).abs().rolling(n).sum()
        er = change / volatility.replace(0, np.nan)

        # Smoothing Constant (SC)
        fast_sc = 2 / (fast + 1)
        slow_sc = 2 / (slow + 1)
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2

        # KAMA
        kama = close.copy()
        for i in range(n, len(close)):
            if pd.notna(sc.iloc[i]) and pd.notna(kama.iloc[i-1]):
                kama.iloc[i] = kama.iloc[i-1] + sc.iloc[i] * (close.iloc[i] - kama.iloc[i-1])

        # Return KAMA distance from price (normalized)
        return ((kama - close) / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


# ============================================================
# Volatility Factors
# ============================================================

@dataclass
class ParkinsonVolFactor(TimeSeriesFactor):
    """Parkinson volatility estimator.

    Source: Parkinson, M. (1980). The Extreme Value Method.
    Uses high-low range for more efficient volatility estimation.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"parkinson_vol_{period}",
            inputs=("high", "low"),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")

        # Parkinson variance: (1/(4*ln(2))) * (ln(H/L))^2
        hl_log = np.log(high / low.replace(0, np.nan))
        variance = (hl_log ** 2) / (4 * np.log(2))

        # Rolling sum and annualize (for crypto, assume 365*24 = 8760 hours)
        return np.sqrt(variance.rolling(n).mean())


@dataclass
class GarmanKlassVolFactor(TimeSeriesFactor):
    """Garman-Klass volatility estimator.

    Source: Garman, M. & Klass, M. (1980). On the Estimation of Security Price Volatilities.
    More efficient than close-to-close volatility.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"garman_klass_vol_{period}",
            inputs=("high", "low", "close", "open"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        open_ = pd.to_numeric(df.get("open", close.shift(1)), errors="coerce")

        # Garman-Klass variance
        hl_ratio = np.log(high / low.replace(0, np.nan))
        co_ratio = np.log(close / open_.replace(0, np.nan))

        variance = 0.5 * (hl_ratio ** 2) - (2 * np.log(2) - 1) * (co_ratio ** 2)

        return np.sqrt(variance.rolling(n).mean())


@dataclass
class YangZhangVolFactor(TimeSeriesFactor):
    """Yang-Zhang volatility estimator.

    Source: Yang, D. & Zhang, Q. (2000). Drift-independent volatility estimation.
    Handles overnight jumps and drift, optimal for crypto markets.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"yang_zhang_vol_{period}",
            inputs=("high", "low", "close", "open"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        open_ = pd.to_numeric(df.get("open", close.shift(1)), errors="coerce")

        prev_close = close.shift(1)

        # Overnight volatility (close to open)
        overnight_ret = np.log(open_ / prev_close.replace(0, np.nan))
        overnight_var = overnight_ret.rolling(n).var()

        # Open-to-close volatility
        daily_ret = np.log(close / open_.replace(0, np.nan))
        daily_var = daily_ret.rolling(n).var()

        # Rogers-Satchell volatility (intraday)
        hl_log = np.log(high / low.replace(0, np.nan))
        ho_log = np.log(high / open_.replace(0, np.nan))
        lo_log = np.log(low / open_.replace(0, np.nan))
        co_log = np.log(close / open_.replace(0, np.nan))

        rs_var = (ho_log * (ho_log - co_log) + lo_log * (lo_log - co_log)).rolling(n).mean()

        # Yang-Zhang combined
        k = 0.34 / (1.34 + (n + 1) / (n - 1))
        yz_var = overnight_var + k * daily_var + (1 - k) * rs_var

        return np.sqrt(yz_var)


@dataclass
class UlcerIndexFactor(TimeSeriesFactor):
    """Ulcer Index - measures downside risk/stress.

    Source: Martin, P. & McCann, B. (1989). The Investor's Guide to Fidelity Funds.
    Focuses only on drawdowns, ignores upside volatility.
    """
    def __init__(self, period: int = 14):
        period = int(period)
        super().__init__(
            name=f"ulcer_index_{period}",
            inputs=("close",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        # Rolling maximum
        rolling_max = close.rolling(n).max()

        # Percentage drawdown
        drawdown_pct = ((close - rolling_max) / rolling_max.replace(0, np.nan)) * 100

        # Ulcer Index = sqrt(mean(squared_drawdowns))
        return np.sqrt((drawdown_pct ** 2).rolling(n).mean())


# ============================================================
# Liquidity and Volume Factors
# ============================================================

@dataclass
class OVIFactor(TimeSeriesFactor):
    """On-Balance Volume (OBV) factor - cumulative volume flow.

    Source: Granville, J. (1963). Granville's New Key to Stock Market Profits.
    """
    def __init__(self, smooth: int = 20):
        smooth = int(smooth)
        super().__init__(
            name=f"obv_{smooth}",
            inputs=("close", "volume"),
            lookback=smooth + 2,
            params={"smooth": smooth}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["smooth"])
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Direction: +1 if close up, -1 if down, 0 if unchanged
        direction = np.sign(close.diff())

        # OBV: cumulative signed volume
        obv = (direction * volume).cumsum()

        # Normalize by rolling mean and return z-score
        obv_ma = obv.rolling(n).mean()
        obv_std = obv.rolling(n).std().replace(0, np.nan)

        return ((obv - obv_ma) / obv_std).replace([np.inf, -np.inf], np.nan)


@dataclass
class MFIFactor(TimeSeriesFactor):
    """Money Flow Index (MFI) factor - volume-weighted RSI.

    Source: Quong & Soudack (1989).
    Range: 0-100, overbought > 80, oversold < 20.
    """
    def __init__(self, period: int = 14):
        period = int(period)
        super().__init__(
            name=f"mfi_{period}",
            inputs=("high", "low", "close", "volume"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Typical Price
        tp = (high + low + close) / 3

        # Money Flow
        mf = tp * volume

        # Positive and Negative Money Flow
        pos_mf = mf.where(tp > tp.shift(1), 0)
        neg_mf = mf.where(tp < tp.shift(1), 0)

        # Money Flow Ratio
        pos_sum = pos_mf.rolling(n).sum()
        neg_sum = neg_mf.rolling(n).sum()
        mf_ratio = pos_sum / neg_sum.replace(0, np.nan)

        # MFI
        mfi = 100 - (100 / (1 + mf_ratio))

        # Return centered MFI (deviation from 50)
        return mfi - 50


@dataclass
class AmihudIlliquidityFactor(TimeSeriesFactor):
    """Amihud illiquidity factor.

    Source: Amihud, Y. (2002). Illiquidity and stock returns.
    Measures price impact per unit of volume.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"amihud_{period}",
            inputs=("close", "volume"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Daily illiquidity: |return| / volume
        ret = close.pct_change().abs()
        dollar_vol = (volume * close).replace(0, np.nan)
        illiq = ret / dollar_vol

        # Rolling mean illiquidity (negated so high value = more liquid)
        return -np.log1p(illiq.rolling(n).mean().replace(0, np.nan))


@dataclass
class VWAPFactor(TimeSeriesFactor):
    """Volume Weighted Average Price factor.

    Measures price relative to VWAP - mean reversion indicator.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"vwap_{period}",
            inputs=("high", "low", "close", "volume"),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Typical Price
        tp = (high + low + close) / 3

        # Rolling VWAP
        cum_tp_vol = (tp * volume).rolling(n).sum()
        cum_vol = volume.rolling(n).sum()
        vwap = cum_tp_vol / cum_vol.replace(0, np.nan)

        # Deviation from VWAP (normalized)
        return ((close - vwap) / vwap).replace([np.inf, -np.inf], np.nan)


# ============================================================
# Microstructure Factors
# ============================================================

@dataclass
class KyleLambdaFactor(TimeSeriesFactor):
    """Kyle's Lambda - price impact coefficient proxy.

    Source: Kyle, A. (1985). Continuous auctions and insider trading.
    Measures market depth/liquidity.
    """
    def __init__(self, period: int = 20):
        period = int(period)
        super().__init__(
            name=f"kyle_lambda_{period}",
            inputs=("close", "volume"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Return and signed volume
        ret = close.pct_change().abs()
        signed_vol = np.sign(close.diff()) * volume

        # Kyle's lambda proxy: |return| / |signed_volume|
        # Rolling regression approximation
        lambda_proxy = ret / volume.replace(0, np.nan)

        return -lambda_proxy.rolling(n).mean()  # Negated: high = more liquid


@dataclass
class OrderFlowImbalanceFactor(TimeSeriesFactor):
    """Order Flow Imbalance (OFI) factor proxy.

    Source: Cont et al. (2014). Price dynamics in a Markovian limit order book.
    Approximated using high/low/close relationship.
    """
    def __init__(self, period: int = 10):
        period = int(period)
        super().__init__(
            name=f"ofi_{period}",
            inputs=("high", "low", "close", "volume"),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        # Proxy: (close - (high + low) / 2) / range * volume
        mid = (high + low) / 2
        rng = (high - low).replace(0, np.nan)

        # Buying pressure: close above mid = buying
        imbalance = ((close - mid) / rng * volume).fillna(0)

        # Cumulative imbalance
        cum_imbalance = imbalance.rolling(n).sum()

        # Normalize
        return (cum_imbalance - cum_imbalance.rolling(n).mean()) / cum_imbalance.rolling(n).std().replace(0, np.nan)


@dataclass
class TradeIntensityFactor(TimeSeriesFactor):
    """Trade intensity factor.

    Measures the concentration of volume relative to typical levels.
    """
    def __init__(self, fast: int = 5, slow: int = 20):
        fast = int(fast)
        slow = int(slow)
        super().__init__(
            name=f"trade_intensity_{fast}_{slow}",
            inputs=("volume",),
            lookback=slow + 1,
            params={"fast": fast, "slow": slow}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        volume = pd.to_numeric(df["volume"], errors="coerce")
        fast = int(self.params["fast"])
        slow = int(self.params["slow"])

        fast_vol = volume.rolling(fast).mean()
        slow_vol = volume.rolling(slow).mean()

        return (fast_vol / slow_vol.replace(0, np.nan) - 1).replace([np.inf, -np.inf], np.nan)


# ============================================================
# Statistical Arbitrage Factors
# ============================================================

@dataclass
class HurstExponentFactor(TimeSeriesFactor):
    """Hurst Exponent - measures long-term memory.

    Source: Hurst, H. (1951). Long-term storage capacity of reservoirs.
    H > 0.5: trending, H < 0.5: mean-reverting, H = 0.5: random walk.
    """
    def __init__(self, period: int = 100):
        period = max(30, int(period))  # Need enough data for reliable estimate
        super().__init__(
            name=f"hurst_{period}",
            inputs=("close",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        def calc_hurst(series):
            if len(series.dropna()) < n:
                return np.nan

            # Rescaled range analysis
            prices = series.dropna().values
            if len(prices) < 20:
                return np.nan

            returns = np.diff(np.log(prices))
            mean_ret = np.mean(returns)
            deviations = returns - mean_ret
            cumulative = np.cumsum(deviations)
            R = np.max(cumulative) - np.min(cumulative)
            S = np.std(returns)

            if S == 0:
                return np.nan

            # Hurst approximation
            return np.log(R / S) / np.log(len(returns))

        # Rolling Hurst
        return close.rolling(n).apply(calc_hurst, raw=False)


@dataclass
class HalfLifeFactor(TimeSeriesFactor):
    """Mean Reversion Half-Life factor.

    Based on Ornstein-Uhlenbeck process.
    Measures how quickly price reverts to mean.
    """
    def __init__(self, period: int = 60):
        period = max(20, int(period))
        super().__init__(
            name=f"half_life_{period}",
            inputs=("close",),
            lookback=period + 2,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        def calc_half_life(series):
            if len(series.dropna()) < n // 2:
                return np.nan

            prices = series.dropna().values
            if len(prices) < 10:
                return np.nan

            # AR(1) regression: y[t] = a + b * y[t-1] + e
            y = np.log(prices[1:])
            x = np.log(prices[:-1])

            # OLS
            try:
                b = np.cov(y, x)[0, 1] / np.var(x)
                if b >= 1 or b <= 0:  # No mean reversion
                    return np.nan
                half_life = -np.log(2) / np.log(b)
                return min(half_life, n)  # Cap at window
            except:
                return np.nan

        # Rolling half-life (negated: low = faster reversion = more signal)
        return -close.rolling(n).apply(calc_half_life, raw=False)


@dataclass
class VarianceRatioFactor(TimeSeriesFactor):
    """Variance Ratio test factor.

    Source: Lo & MacKinlay (1988). Stock market prices do not follow random walks.
    Tests for mean reversion vs momentum.
    """
    def __init__(self, period: int = 30, lag: int = 5):
        period = max(20, int(period))
        lag = max(2, int(lag))
        super().__init__(
            name=f"variance_ratio_{period}_{lag}",
            inputs=("close",),
            lookback=period + lag + 1,
            params={"period": period, "lag": lag}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        lag = int(self.params["lag"])
        close = pd.to_numeric(df["close"], errors="coerce")

        def calc_vr(series):
            if len(series.dropna()) < n:
                return np.nan

            prices = series.dropna().values
            returns = np.diff(np.log(prices))

            if len(returns) < lag * 2:
                return np.nan

            # Variance of 1-period returns
            var_1 = np.var(returns)

            # Variance of lag-period returns
            lag_returns = np.diff(np.log(prices[::lag]))
            if len(lag_returns) < 5:
                return np.nan
            var_lag = np.var(lag_returns) * lag

            if var_1 == 0:
                return np.nan

            # VR > 1: momentum, VR < 1: mean reversion
            return var_lag / var_1 - 1

        return close.rolling(n).apply(calc_vr, raw=False)


# ============================================================
# Risk Factors
# ============================================================

@dataclass
class VaRFactor(TimeSeriesFactor):
    """Value at Risk (VaR) factor.

    Historical simulation VaR at 95% confidence.
    """
    def __init__(self, period: int = 20, confidence: float = 0.95):
        period = int(period)
        confidence = float(confidence)
        super().__init__(
            name=f"var_{period}",
            inputs=("close",),
            lookback=period + 2,
            params={"period": period, "confidence": confidence}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        conf = float(self.params["confidence"])
        close = pd.to_numeric(df["close"], errors="coerce")

        returns = close.pct_change()

        # Historical VaR (negated loss)
        def calc_var(series):
            if len(series.dropna()) < n // 2:
                return np.nan
            return -np.percentile(series.dropna(), (1 - conf) * 100)

        return -returns.rolling(n).apply(calc_var, raw=False)


@dataclass
class CVaRFactor(TimeSeriesFactor):
    """Conditional VaR (Expected Shortfall) factor.

    Average loss beyond VaR threshold.
    """
    def __init__(self, period: int = 20, confidence: float = 0.95):
        period = int(period)
        confidence = float(confidence)
        super().__init__(
            name=f"cvar_{period}",
            inputs=("close",),
            lookback=period + 2,
            params={"period": period, "confidence": confidence}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        conf = float(self.params["confidence"])
        close = pd.to_numeric(df["close"], errors="coerce")

        returns = close.pct_change()

        def calc_cvar(series):
            if len(series.dropna()) < n // 2:
                return np.nan
            r = series.dropna()
            var_threshold = np.percentile(r, (1 - conf) * 100)
            return -r[r <= var_threshold].mean()

        return -returns.rolling(n).apply(calc_cvar, raw=False)


@dataclass
class MaxDrawdownFactor(TimeSeriesFactor):
    """Maximum Drawdown factor.

    Rolling maximum drawdown over the period.
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"max_dd_{period}",
            inputs=("close",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        # Rolling max
        rolling_max = close.rolling(n).max()

        # Drawdown
        drawdown = (close - rolling_max) / rolling_max.replace(0, np.nan)

        return -drawdown  # Negated so higher = lower risk


@dataclass
class SortinoRatioFactor(TimeSeriesFactor):
    """Sortino Ratio factor - risk-adjusted return using downside deviation.

    Source: Sortino, F. & Price, L. (1994). Performance measurement in a downside risk framework.
    """
    def __init__(self, period: int = 30, target: float = 0.0):
        period = int(period)
        target = float(target)
        super().__init__(
            name=f"sortino_{period}",
            inputs=("close",),
            lookback=period + 2,
            params={"period": period, "target": target}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        target = float(self.params["target"])
        close = pd.to_numeric(df["close"], errors="coerce")

        returns = close.pct_change()

        def calc_sortino(series):
            r = series.dropna()
            if len(r) < n // 2:
                return np.nan

            mean_ret = r.mean()

            # Downside deviation
            downside = r[r < target] - target
            if len(downside) < 2:
                return np.nan

            downside_std = np.sqrt((downside ** 2).mean())

            if downside_std == 0:
                return np.nan

            return mean_ret / downside_std

        return returns.rolling(n).apply(calc_sortino, raw=False)


@dataclass
class CalmarRatioFactor(TimeSeriesFactor):
    """Calmar Ratio factor - return / maximum drawdown.

    Source: Young, T. (1991). Calmar Ratio.
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"calmar_{period}",
            inputs=("close",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        # Annualized return
        returns = close.pct_change()
        ann_return = returns.rolling(n).mean() * 365 * 24  # Crypto: 24h markets

        # Maximum drawdown
        rolling_max = close.rolling(n).max()
        max_dd = (close - rolling_max) / rolling_max.replace(0, np.nan)

        # Calmar ratio
        return (ann_return / max_dd.abs().replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


# ============================================================
# Technical Analysis Factors
# ============================================================

@dataclass
class WilliamsRFactor(TimeSeriesFactor):
    """Williams %R factor.

    Source: Williams, L. (1973). How I Made $1,000,000 Trading Commodities.
    Overbought/oversold oscillator.
    """
    def __init__(self, period: int = 14):
        period = int(period)
        super().__init__(
            name=f"williams_r_{period}",
            inputs=("high", "low", "close"),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")

        highest = high.rolling(n).max()
        lowest = low.rolling(n).min()

        # Williams %R: -100 to 0
        wr = (highest - close) / (highest - lowest).replace(0, np.nan) * -100

        # Centered around -50
        return wr + 50


@dataclass
class CCIFactor(TimeSeriesFactor):
    """Commodity Channel Index (CCI) factor.

    Source: Lambert, D. (1980). Commodity Channel Index.
    """
    def __init__(self, period: int = 20, constant: float = 0.015):
        period = int(period)
        constant = float(constant)
        super().__init__(
            name=f"cci_{period}",
            inputs=("high", "low", "close"),
            lookback=period + 1,
            params={"period": period, "constant": constant}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        constant = float(self.params["constant"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")

        # Typical Price
        tp = (high + low + close) / 3

        # SMA of TP
        sma = tp.rolling(n).mean()

        # Mean Deviation
        mad = tp.rolling(n).apply(lambda x: np.abs(x - x.mean()).mean())

        # CCI
        return (tp - sma) / (constant * mad.replace(0, np.nan))


@dataclass
class StochRSIFactor(TimeSeriesFactor):
    """Stochastic RSI factor.

    Source: Chande & Kroll (1994). The New Technical Trader.
    RSI of RSI - more sensitive oscillator.
    """
    def __init__(self, rsi_period: int = 14, stoch_period: int = 14):
        rsi_period = int(rsi_period)
        stoch_period = int(stoch_period)
        super().__init__(
            name=f"stoch_rsi_{rsi_period}_{stoch_period}",
            inputs=("close",),
            lookback=rsi_period + stoch_period + 5,
            params={"rsi_period": rsi_period, "stoch_period": stoch_period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        rsi_n = int(self.params["rsi_period"])
        stoch_n = int(self.params["stoch_period"])
        close = pd.to_numeric(df["close"], errors="coerce")

        # Calculate RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)

        avg_gain = gain.rolling(rsi_n).mean()
        avg_loss = loss.rolling(rsi_n).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        # Stochastic of RSI
        rsi_min = rsi.rolling(stoch_n).min()
        rsi_max = rsi.rolling(stoch_n).max()

        stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan) * 100

        return stoch_rsi - 50  # Center around 0


@dataclass
class WilliamsADFactor(TimeSeriesFactor):
    """Williams Accumulation/Distribution factor.

    Source: Williams, L. (1972). How I Made $1,000,000 Last Year Trading Commodities.
    """
    def __init__(self, smooth: int = 20):
        smooth = int(smooth)
        super().__init__(
            name=f"williams_ad_{smooth}",
            inputs=("high", "low", "close", "volume"),
            lookback=smooth + 2,
            params={"smooth": smooth}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["smooth"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        prev_close = close.shift(1)

        # True High/Low
        true_high = pd.concat([high, prev_close], axis=1).max(axis=1)
        true_low = pd.concat([low, prev_close], axis=1).min(axis=1)

        # AD calculation
        ad = np.where(
            close > prev_close,
            close - true_low,
            np.where(
                close < prev_close,
                close - true_high,
                0
            )
        ) * volume

        ad_series = pd.Series(ad, index=df.index).cumsum()

        # Normalize
        ad_ma = ad_series.rolling(n).mean()
        ad_std = ad_series.rolling(n).std().replace(0, np.nan)

        return ((ad_series - ad_ma) / ad_std).replace([np.inf, -np.inf], np.nan)


# ============================================================
# Factor Registration
# ============================================================

EXTENDED_FACTOR_CLASS_MAP = {
    # Momentum and Trend
    "roc": ROCFactor,
    "price_accel": PriceAccelerationFactor,
    "aroon": AroonFactor,
    "kama": KAMAFactor,

    # Volatility
    "parkinson_vol": ParkinsonVolFactor,
    "garman_klass_vol": GarmanKlassVolFactor,
    "yang_zhang_vol": YangZhangVolFactor,
    "ulcer_index": UlcerIndexFactor,

    # Liquidity and Volume
    "obv": OVIFactor,
    "mfi": MFIFactor,
    "amihud": AmihudIlliquidityFactor,
    "vwap": VWAPFactor,

    # Microstructure
    "kyle_lambda": KyleLambdaFactor,
    "ofi": OrderFlowImbalanceFactor,
    "trade_intensity": TradeIntensityFactor,

    # Statistical Arbitrage
    "hurst": HurstExponentFactor,
    "half_life": HalfLifeFactor,
    "variance_ratio": VarianceRatioFactor,

    # Risk
    "var": VaRFactor,
    "cvar": CVaRFactor,
    "max_dd": MaxDrawdownFactor,
    "sortino": SortinoRatioFactor,
    "calmar": CalmarRatioFactor,

    # Technical
    "williams_r": WilliamsRFactor,
    "cci": CCIFactor,
    "stoch_rsi": StochRSIFactor,
    "williams_ad": WilliamsADFactor,
}
