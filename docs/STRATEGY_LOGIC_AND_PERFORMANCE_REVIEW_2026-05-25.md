# Strategy Logic and Performance Review - 2026-05-25

Scope: registered strategy library in `config.strategy_registry.STRATEGY_REGISTRY`
after the second-round intraday cross-section strategy pack was connected.

Validation already completed:

- Import/export/instantiation parity: 74 registry entries, 74 package exports, no missing classes, no instantiation failures after duplicate strategy removal.
- Backtest smoke: 70 strategies marked backtest-supported completed `_run_backtest_core` with valid final capital before the duplicate removal. Post-cleanup rerun on 2026-05-26: `python scripts/check_backtest_supported_strategies.py` passed for 71 backtest-supported strategies.
- Live-only by design: 4 arbitrage strategies depend on live order books or on-chain execution and are not suitable for single-OHLCV backtests.
- Focused tests: intraday cross-section, strategy library/factors, backtest runtime consistency, signal regressions, and specialty strategy tests passed.

Important distinction: these checks prove runtime compatibility and catch many implementation regressions. They do not prove that every economic hypothesis is alpha-positive in live trading. Strategy correctness below means: causal data usage, coherent signal direction, framework-compatible signals/exits, and no obvious implementation contradiction found in the reviewed code path.

## Cross-Cutting Findings

1. Most classical single-symbol strategies are mechanically correct, but their standalone edge is likely regime-dependent. They should be traded behind volatility/trend/range regime gates, not as always-on systems.
2. Several macro/proxy strategies are correctly wired but rely on fallback proxies. Their live quality depends more on source freshness and point-in-time data quality than indicator math.
3. `MLXGBoostStrategy` is wired and smoke-tested. 2026-05-27 follow-up restored the canonical `models/ml_signal_xgb.manifest.json` sidecar and made registry support validate the manifest before counting the strategy as backtest-supported; calibration and drift checks remain required before live priority.
4. Intraday cross-section strategies are framework-correct and backtestable. Their next validation step is IC/turnover/slippage stability across symbols, market regimes, and listing-age filters.
5. `ResidualMom24hStrategy` was removed because it was identical to `RelRet24hReversalStrategy` in factor, lookback, direction, and execution behavior.

## Per-Strategy Review

| Strategy | Logic assessment | Main performance improvement |
|---|---|---|
| MAStrategy | Correct trend-cross logic with soft exit. | Use ATR/ADX regime gate and volatility-normalized MA spread threshold. |
| EMAStrategy | Correct EMA cross logic. | Add same soft-exit metadata parity as MA and tune fast/slow by walk-forward. |
| MACDStrategy | Correct MACD cross plus histogram exit. | Filter crosses by higher-timeframe trend and histogram slope. |
| MACDHistogramStrategy | Correct histogram threshold crossover. | Make `min_histogram` volatility-normalized instead of absolute. |
| ADXTrendStrategy | Correct +DI/-DI cross gated by ADX. | Add falling-ADX exit and ATR-based position sizing. |
| TrendFollowingStrategy | Correct MA cross with ADX filter. | Replace fixed take-profit with trailing stop in strong trends. |
| AroonStrategy | Correct Aroon oscillator threshold logic. | Add confirmation from realized volatility expansion. |
| RSIStrategy | Correct RSI reversal and profit-gated exit. | Use adaptive RSI bands by volatility/trend regime. |
| RSIDivergenceStrategy | Causal extrema detector avoids centered lookahead. | Score divergence by swing distance, volume, and trend context. |
| StochasticStrategy | Correct stochastic cross using previous-bar zone filter. | Add neutral-line exit and volatility filter to reduce churn. |
| BollingerBandsStrategy | Correct band bounce/decline logic. | Distinguish squeeze breakout vs mean reversion regimes. |
| WilliamsRStrategy | Correct oscillator strategy covered by neutral-line regression tests. | Tune exits around -50 with trend filter. |
| CCIStrategy | Correct CCI threshold strategy covered by neutral-line tests. | Normalize thresholds by asset volatility and liquidity bucket. |
| StochRSIStrategy | Correct oscillator threshold strategy covered by neutral-line tests. | Add minimum range/volume filter to avoid flat-market noise. |
| MomentumStrategy | Correct rate-of-change threshold crossover. | Use cross-sectional or regime-relative momentum instead of fixed 2%. |
| ROCStrategy | Correct ROC threshold and zero-line exit. | Optimize threshold by timeframe and add volatility scaling. |
| PriceAccelerationStrategy | Correct acceleration factor logic. | Clamp unstable ratios when slow momentum is tiny; add trend confirmation. |
| MeanReversionStrategy | Correct z-score reversion with explicit exit. | Replace rolling mean/std with robust median/MAD option. |
| BollingerMeanReversionStrategy | Correct lower/upper band reversion. | Add band-width and trend filters to avoid fading breakouts. |
| VWAPReversionStrategy | Correct rolling VWAP deviation reversion and exit. | Use session VWAP where intraday session boundaries matter. |
| VWAPStrategy | Correct factor-based VWAP signal path. | Add volume profile/session reset and liquidity filter. |
| MeanReversionHalfLifeStrategy | Correct half-life factor path. | Estimate half-life with rolling confidence and skip unstable regressions. |
| BollingerSqueezeStrategy | Correct squeeze/breakout style wiring. | Require post-squeeze volume expansion and directional close confirmation. |
| DonchianBreakoutStrategy | Correct shifted-channel breakout avoids same-bar lookahead. | Add ATR breakout buffer and trailing channel exit. |
| MFIStrategy | Correct volume oscillator signal path. | Use liquidity bucket thresholds and avoid low-volume false positives. |
| OBVStrategy | Correct OBV factor signal path. | Add OBV divergence scoring and price trend confirmation. |
| TradeIntensityStrategy | Correct microstructure proxy path. | Require spread/depth filters and cap trades during thin books. |
| ParkinsonVolStrategy | Correct high-low volatility percentile logic. | Separate low-vol breakout from high-vol reversal as two explicit modes. |
| UlcerIndexStrategy | Correct downside-risk factor path. | Use as risk gate/position scalar more than standalone alpha. |
| VaRBreakoutStrategy | Correct risk-breakout factor path. | Calibrate VaR windows per asset and add tail-volatility stop. |
| MaxDrawdownStrategy | Correct drawdown-risk factor path. | Use recovery confirmation before long entries and faster exits on new drawdown lows. |
| SortinoRatioStrategy | Correct downside-adjusted return factor path. | Use rolling confidence/min sample gate before emitting signals. |
| PairsTradingStrategy | Correct rolling spread, hedge-ratio bounds, and two-leg metadata. | Add cointegration/stationarity test and dynamic hedge-ratio stability gate. |
| FamaFactorArbitrageStrategy | Correct multi-asset score ranking and rebalance plan. | Add factor-neutral risk model, turnover penalty, and liquidity-aware weights. |
| ResidualMom48hStrategy | Correct cross-sectional residual reversal spec. | Validate IC by regime and cap turnover during market-wide shocks. |
| Ret24hReversalStrategy | Correct 24h cross-sectional reversal spec. | Add volatility/liquidity filters and skip listing/news shock windows. |
| RelRet24hReversalStrategy | Correct residual-return reversal spec. | Use beta-adjusted market residual instead of equal-weight residual only. |
| CloseLocation48hStrategy | Correct close-location cross-section spec. | Combine with volume/volatility confirmation to avoid candle-shape noise. |
| HurstExponentStrategy | Correct factor strategy path. | Use Hurst as regime selector for trend vs reversion systems. |
| OrderFlowImbalanceStrategy | Correct order-flow proxy factor path. | Use real order-book/trade imbalance and latency-aware sampling. |
| MultiFactorHFStrategy | Correct config-driven factor/gate/cooldown architecture. | Add walk-forward factor weights, transaction-cost-aware objective, and drift monitoring. |
| LiquidationOICrowdingStrategy | Correct conservative gate plus reset trade logic. | Improve with real liquidation/OI feed freshness checks and exchange-specific thresholds. |
| AltcoinDowntrendBounceShortStrategy | Correct short-only bounce-failure rule and time exit. | Add market beta filter, funding/borrow checks, and ATR stop option. |
| SupplyEventStrategy | Correct point-in-time event visibility and gate/trade split. | Add event surprise/float unlock size normalization and post-event absorption confirmation. |
| OnChainFlowRegimeStrategy | Correct slow regime scalar/optional trade logic. | Use primarily as portfolio scalar; validate stale-data handling and source latency. |
| MLXGBoostStrategy | Runtime-compatible; manifest-gated as of 2026-05-27, but not live-priority without calibration/drift evidence. | Add probability calibration, feature drift checks, and embargoed walk-forward tests. |
| CEXArbitrageStrategy | Correctly live-only; OHLCV backtest is inappropriate. | Improve with fee/slippage/order-book simulation and exchange latency model. |
| TriangularArbitrageStrategy | Correctly live-only; requires real-time same-exchange quotes. | Improve with executable-depth routing, fee tiers, and stale-quote rejection. |
| DEXArbitrageStrategy | Correctly live-only; depends on on-chain pool state. | Improve with gas/slippage/MEV-aware simulator and route freshness checks. |
| FlashLoanArbitrageStrategy | Correctly live-only; depends on atomic execution. | Improve with transaction simulation, revert-cost limits, and MEV protection. |
| MarketSentimentStrategy | Correct sentiment extreme/neutral-exit logic. | Replace fallback momentum proxy with cached point-in-time sentiment source. |
| SocialSentimentStrategy | Correct proxy-based sentiment wiring. | Add source weighting, spam filtering, and lag-aware sentiment decay. |
| FundFlowStrategy | Correct order-book imbalance proxy logic. | Use executed trade flow plus depth imbalance; normalize thresholds by symbol liquidity. |
| WhaleActivityStrategy | Correct large-trade aggregation pattern. | Deduplicate robustly across exchanges and classify aggressor side more accurately. |
| ReturnEntropy4hStrategy | Correct non-parametric entropy spec. | Test longer windows and combine with trend persistence filter. |
| FalseBreakoutSupply24hStrategy | Correct failed-breakout supply spec. | Verify sign with IC; add volume spike/rejection confirmation. |
| RangeAsymmetry48hStrategy | Correct intrabar range-shape spec. | Winsorize extreme wicks and liquidity-normalize ranges. |
| SessionAsiaFlow24hStrategy | Correct UTC Asia-session flow spec. | Validate UTC session mapping per exchange and asset behavior. |
| SessionFlowRotation24hStrategy | Correct Asia-vs-US flow rotation spec. | Add day-of-week and funding-window controls. |
| VolumeWeightedReturn24hStrategy | Correct volume-weighted pressure spec. | Use dollar-volume caps and exclude wash/illiquid volume spikes. |
| WickImbalance48hStrategy | Correct wick imbalance short-low spec. | Confirm direction with regime-specific IC; add liquidation spike filter. |
| TurnoverEntropy48hStrategy | Correct turnover concentration entropy spec. | Add concentration threshold and turnover-liquidity guard. |
| BodyVolumeCorr24hStrategy | Correct body/volume correlation spec. | Stabilize rolling correlation with minimum variance checks. |
| CorrBreakdown24h72hStrategy | Correct correlation-breakdown spec. | Use beta-adjusted residual correlation and market-stress gate. |
| ExtremeRecency48hStrategy | Correct extreme-recency timing spec. | Combine with breakout failure/continuation classifier before trading. |
| UpDownBetaSpread24h72hStrategy | Correct asymmetric beta spread with neutral missing-side handling. | Require minimum up/down market samples and beta stability score. |
| DirectionalRangeEfficiency48hStrategy | Correct liquidity-impact style spec. | Winsorize dollar-volume denominators and add spread proxy. |
| CrossSectionalStress4hStrategy | Correct relative-shock spec. | Add market-wide stress veto and position smaller during high dispersion. |
| SignImbalance4hStrategy | Correct non-parametric sign trend spec. | Test 6h/12h variants and require volume confirmation. |
| VWAPSlope24hStrategy | Correct VWAP slope/inventory spec. | Use session-reset VWAP and turnover-weighted slope. |
| VWAPGap48hStrategy | Correct close-to-inventory gap spec. | Separate mean-reversion gap from momentum gap by trend regime. |
| RelativeVolShock24hStrategy | Correct relative volatility shock spec. | Use as short-vol/avoidance gate unless IC confirms directional edge. |
| LeadMarketResponse24h72hStrategy | Correct lead-lag response spec. | Validate lag convention and add market-leader basket variants. |
| BreakCountBalance24hStrategy | Correct high/low break-count balance spec. | Add amplitude weighting so tiny breaks do not count equally. |

## Recommended Priority

1. P0: `MLXGBoostStrategy` manifest restore/validation is complete as of 2026-05-27; keep probability calibration and feature drift checks as the remaining live-priority gate.
2. P0: Add a strategy-quality report that computes IC, turnover, trade count, max drawdown, cost sensitivity, and sample stability for every backtest-supported strategy.
3. P1: Add regime gates to classical trend/reversion/oscillator strategies.
4. P1: Add point-in-time source validation for macro, event, on-chain, and derivatives strategies.
5. P1: Run walk-forward and out-of-sample tests for the 24 intraday cross-section strategies before enabling any as live priority.
