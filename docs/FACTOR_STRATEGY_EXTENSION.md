# Factor and Strategy Extension Documentation

## Overview

This document describes the extended time-series factors and factor-based strategies added to the crypto trading system.

## New Time-Series Factors

### Location
`core/factors_ts/extended_factors.py`

### Factor Categories

#### 1. Momentum and Trend Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `roc` | ROCFactor | Rate of Change - classic momentum indicator | Murphy (1999) |
| `price_accel` | PriceAccelerationFactor | Second derivative of price momentum | - |
| `aroon` | AroonFactor | Trend strength and direction | Chande (1995) |
| `kama` | KAMAFactor | Kaufman's Adaptive Moving Average | Kaufman (1995) |

#### 2. Volatility Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `parkinson_vol` | ParkinsonVolFactor | Volatility using high-low range | Parkinson (1980) |
| `garman_klass_vol` | GarmanKlassVolFactor | Efficient volatility estimator | Garman & Klass (1980) |
| `yang_zhang_vol` | YangZhangVolFactor | Handles overnight jumps | Yang & Zhang (2000) |
| `ulcer_index` | UlcerIndexFactor | Downside risk measure | Martin & McCann (1989) |

#### 3. Liquidity and Volume Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `obv` | OVIFactor | On-Balance Volume | Granville (1963) |
| `mfi` | MFIFactor | Money Flow Index | Quong & Soudack (1989) |
| `amihud` | AmihudIlliquidityFactor | Price impact per unit volume | Amihud (2002) |
| `vwap` | VWAPFactor | Volume Weighted Average Price | - |

#### 4. Microstructure Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `kyle_lambda` | KyleLambdaFactor | Price impact coefficient | Kyle (1985) |
| `ofi` | OrderFlowImbalanceFactor | Order flow imbalance proxy | Cont et al. (2014) |
| `trade_intensity` | TradeIntensityFactor | Volume concentration | - |

#### 5. Statistical Arbitrage Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `hurst` | HurstExponentFactor | Long-term memory measure | Hurst (1951) |
| `half_life` | HalfLifeFactor | Mean reversion speed | Ornstein-Uhlenbeck |
| `variance_ratio` | VarianceRatioFactor | Random walk test | Lo & MacKinlay (1988) |

#### 6. Risk Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `var` | VaRFactor | Value at Risk | - |
| `cvar` | CVaRFactor | Conditional VaR (Expected Shortfall) | - |
| `max_dd` | MaxDrawdownFactor | Maximum drawdown | - |
| `sortino` | SortinoRatioFactor | Downside risk-adjusted return | Sortino & Price (1994) |
| `calmar` | CalmarRatioFactor | Return / max drawdown | Young (1991) |

#### 7. Technical Analysis Factors

| Factor | Name | Description | Academic Source |
|--------|------|-------------|-----------------|
| `williams_r` | WilliamsRFactor | Williams %R oscillator | Williams (1973) |
| `cci` | CCIFactor | Commodity Channel Index | Lambert (1980) |
| `stoch_rsi` | StochRSIFactor | Stochastic of RSI | Chande & Kroll (1994) |
| `williams_ad` | WilliamsADFactor | Accumulation/Distribution | Williams (1972) |

---

## New Factor-Based Strategies

### Location
`strategies/factor_based/factor_strategies.py`

### Strategy- Factor Mapping

Each strategy corresponds to a factor and generates signals based on factor value thresholds:

| Strategy | Corresponding Factor | Signal Logic |
|----------|---------------------|--------------|
| ROCStrategy | roc | ROC crossing thresholds |
| PriceAccelerationStrategy | price_accel | Acceleration crossing zero |
| AroonStrategy | aroon | Aroon oscillator crossing |
| ParkinsonVolStrategy | parkinson_vol | Volatility regime changes |
| UlcerIndexStrategy | ulcer_index | Risk regime changes |
| MFIStrategy | mfi | Overbought/oversold levels |
| VWAPStrategy | vwap | Price deviation from VWAP |
| OBVStrategy | obv | Volume divergence |
| OrderFlowImbalanceStrategy | ofi | Buying/selling pressure |
| TradeIntensityStrategy | trade_intensity | Volume surge detection |
| MeanReversionHalfLifeStrategy | half_life | Z-score mean reversion |
| HurstExponentStrategy | hurst | Regime-adaptive signals |
| VaRBreakoutStrategy | var | Abnormal return detection |
| MaxDrawdownStrategy | max_dd | Drawdown recovery |
| SortinoRatioStrategy | sortino | Risk-adjusted trend following |
| WilliamsRStrategy | williams_r | Overbought/oversold |
| CCIStrategy | cci | CCI level crossing |
| StochRSIStrategy | stoch_rsi | Stochastic RSI levels |

---

## Usage Examples

### Using Factors Directly

```python
from core.factors_ts.impl import FACTOR_CLASS_MAP

# Build a factor
factor_class = FACTOR_CLASS_MAP["parkinson_vol"]
factor = factor_class(period=20)

# Compute on data
result = factor(data_df)  # Returns pd.Series
```

### Using Strategies

```python
from strategies.factor_based import MFIStrategy

# Create strategy
strategy = MFIStrategy(params={
    "period": 14,
    "oversold": 20,
    "overbought": 80,
})

# Generate signals
signals = strategy.generate_signals(data)
```

### Multi-Factor Strategy Configuration

```yaml
# config/strategy_factor_based.yaml
timeframe: "5m"

factors:
  - name: "roc"
    params: { period: 14 }
    weight: 0.15
    transform: "zscore"

  - name: "mfi"
    params: { period: 14 }
    weight: 0.12
    transform: "none"

  - name: "sortino"
    params: { period: 30 }
    weight: 0.10
    transform: "zscore"
```

---

## Total Summary

- **New Factors**: 26
- **New Strategies**: 18
- **Total Strategy Count**: ~43 (25 existing + 18 new)
- **Total Factor Count**: ~33 (7 basic + 26 extended)

---

## Academic References

1. Amihud, Y. (2002). Illiquidity and stock returns: cross-section and time-series effects. Journal of Financial Markets.
2. Cont, R., et al. (2014). Price dynamics in a Markovian limit order book. SIAM Journal on Financial Mathematics.
3. Garman, M. & Klass, M. (1980). On the estimation of security price volatilities from historical data. Journal of Business.
4. Hurst, H. (1951). Long-term storage capacity of reservoirs. Transactions of the American Society of Civil Engineers.
5. Kyle, A. (1985). Continuous auctions and insider trading. Econometrica.
6. Lo, A. & MacKinlay, A. (1988). Stock market prices do not follow random walks. Review of Financial Studies.
7. Parkinson, M. (1980). The extreme value method for estimating the variance of the rate of return. Journal of Business.
8. Sortino, F. & Price, L. (1994). Performance measurement in a downside risk framework. Journal of Investing.
9. Yang, D. & Zhang, Q. (2000). Drift-independent volatility estimation based on high, low, open, and close prices. Journal of Business.
