# Exit Refactor Report

- Total strategies scanned in repo: 46
- Backtest-supported strategies evaluated: 41 / 41
- Exit templates compared: Original, ReversalOnly, ATRTrail, PartialPlusATR, SignalPlusTimeStop
- Cost model retained from current repo defaults: commission `0.0004`, slippage `2bps`, existing OHLCV backtest matching preserved.
- Default robust recommendation: `SignalPlusTimeStop`

## Template Fit

- ATRTrail fit: StochasticStrategy, WilliamsRStrategy, MeanReversionHalfLifeStrategy, MFIStrategy, ParkinsonVolStrategy, WhaleActivityStrategy
- PartialPlusATR fit: UlcerIndexStrategy
- Best left on original reversal exit: MAStrategy, EMAStrategy, TrendFollowingStrategy, AroonStrategy, RSIStrategy, RSIDivergenceStrategy, BollingerBandsStrategy, CCIStrategy, StochRSIStrategy, ROCStrategy, PriceAccelerationStrategy, MeanReversionStrategy, BollingerMeanReversionStrategy, VWAPReversionStrategy, VWAPStrategy, BollingerSqueezeStrategy, OBVStrategy, TradeIntensityStrategy, VaRBreakoutStrategy, MaxDrawdownStrategy, FamaFactorArbitrageStrategy, MultiFactorHFStrategy, MarketSentimentStrategy, SocialSentimentStrategy, FundFlowStrategy
- Time stop meaningfully reduced giveback: MACDStrategy, MACDHistogramStrategy, ADXTrendStrategy, WilliamsRStrategy, CCIStrategy, MomentumStrategy, ROCStrategy, PriceAccelerationStrategy, MeanReversionStrategy, MeanReversionHalfLifeStrategy, DonchianBreakoutStrategy, UlcerIndexStrategy, SortinoRatioStrategy, HurstExponentStrategy, OrderFlowImbalanceStrategy, SocialSentimentStrategy
- Exit-parameter sensitivity / overfit risk: TrendFollowingStrategy, RSIDivergenceStrategy, MeanReversionHalfLifeStrategy, OBVStrategy, ParkinsonVolStrategy, UlcerIndexStrategy, OrderFlowImbalanceStrategy, WhaleActivityStrategy

## Recommendation

`SignalPlusTimeStop` was chosen as the default not because it delivered the single highest peak return, but because it produced the strongest median robustness score across the library with better OOS Sharpe / drawdown balance than the pure reversal baseline.

## Strategy Inventory

| Strategy | Direction | Stop | Take | Reversal | Time |
| --- | --- | --- | --- | --- | --- |
| MAStrategy | long_short | True | True | True | False |
| EMAStrategy | long_short | True | True | True | False |
| RSIStrategy | long_short | True | True | True | False |
| RSIDivergenceStrategy | long_short | True | True | True | False |
| MACDStrategy | long_short | True | True | True | False |
| MACDHistogramStrategy | long_short | True | True | True | False |
| BollingerBandsStrategy | long_short | True | True | True | False |
| BollingerSqueezeStrategy | long_short | True | True | True | False |
| DonchianBreakoutStrategy | long_short | True | True | True | False |
| StochasticStrategy | long_short | True | True | True | False |
| ADXTrendStrategy | long_short | True | True | True | False |
| VWAPReversionStrategy | long_short | True | True | True | False |
| MeanReversionStrategy | long_short | True | True | True | False |
| BollingerMeanReversionStrategy | long_short | True | True | True | False |
| MomentumStrategy | long_short | True | True | True | False |
| TrendFollowingStrategy | long_short | True | True | True | False |
| PairsTradingStrategy | spread_long_short | True | False | True | False |
| FamaFactorArbitrageStrategy | unknown | True | True | False | False |
| MultiFactorHFStrategy | unknown | True | True | True | False |
| CEXArbitrageStrategy | unknown | False | False | False | False |
| TriangularArbitrageStrategy | unknown | False | False | False | False |
| DEXArbitrageStrategy | unknown | False | False | False | False |
| FlashLoanArbitrageStrategy | unknown | False | False | False | False |
| MarketSentimentStrategy | long_short | True | True | True | False |
| SocialSentimentStrategy | long_short | True | True | True | False |
| FundFlowStrategy | long_short | True | True | True | False |
| WhaleActivityStrategy | unknown | True | True | True | False |
| MLXGBoostStrategy | unknown | True | True | True | False |
| ROCStrategy | long_short | True | True | True | False |
| PriceAccelerationStrategy | long_short | True | True | True | False |
| AroonStrategy | long_short | True | True | True | False |
| ParkinsonVolStrategy | long_short | True | True | True | False |
| UlcerIndexStrategy | long_short | True | True | True | False |
| MFIStrategy | long_short | True | True | True | False |
| VWAPStrategy | long_short | True | True | True | False |
| OBVStrategy | unknown | True | True | True | False |
| OrderFlowImbalanceStrategy | long_short | True | True | True | False |
| TradeIntensityStrategy | unknown | True | True | True | False |
| MeanReversionHalfLifeStrategy | unknown | True | True | True | False |
| HurstExponentStrategy | long_short | True | True | True | False |
| VaRBreakoutStrategy | long_short | True | True | True | False |
| MaxDrawdownStrategy | long_short | True | True | False | False |
| SortinoRatioStrategy | long_only | True | True | True | False |
| WilliamsRStrategy | long_short | True | True | True | False |
| CCIStrategy | long_short | True | True | True | False |
| StochRSIStrategy | unknown | True | True | True | False |

## Unsupported / Compatibility Notes

- CEXArbitrageStrategy: 依赖多交易所盘口/买卖价差，非单一OHLCV回测模型
- TriangularArbitrageStrategy: 依赖同交易所多交易对实时报价，不适用单一K线回测
- DEXArbitrageStrategy: 依赖链上流动性池与实时路由报价
- FlashLoanArbitrageStrategy: 依赖链上原子交易执行，K线回测无法刻画
- MLXGBoostStrategy: 当前环境缺少 xgboost，MLXGBoostStrategy 暂不可回测

## Notes

- `Original` reflects the current repo baseline under the existing backtest path.
- `ReversalOnly` is the unified-exit equivalent of baseline signal-only exits.
- `SignalPlusTimeStop` and `PartialPlusATR` were the two templates most likely to reduce profit giveback when OOS Sharpe did not deteriorate.
- `FamaFactorArbitrageStrategy` stays on its portfolio-style compatibility path; exit templates are reported for completeness, but the cross-sectional rebalance logic is intentionally left untouched.
