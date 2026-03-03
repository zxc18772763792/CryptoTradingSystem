"""Strategy exports."""
from core.strategies import (
    StrategyBase,
    Signal,
    SignalType,
    StrategyState,
    StrategyManager,
    strategy_manager,
    SignalGenerator,
    signal_generator,
)

from strategies.technical import (
    MAStrategy,
    EMAStrategy,
    RSIStrategy,
    RSIDivergenceStrategy,
    MACDStrategy,
    MACDHistogramStrategy,
    BollingerBandsStrategy,
    BollingerSqueezeStrategy,
    DonchianBreakoutStrategy,
    StochasticStrategy,
    ADXTrendStrategy,
    VWAPReversionStrategy,
)

from strategies.quantitative import (
    MeanReversionStrategy,
    BollingerMeanReversionStrategy,
    MomentumStrategy,
    TrendFollowingStrategy,
    PairsTradingStrategy,
    FamaFactorArbitrageStrategy,
    MultiFactorHFStrategy,
)

from strategies.arbitrage import (
    CEXArbitrageStrategy,
    TriangularArbitrageStrategy,
    DEXArbitrageStrategy,
    FlashLoanArbitrageStrategy,
)

from strategies.macro import (
    MarketSentimentStrategy,
    SocialSentimentStrategy,
    FundFlowStrategy,
    WhaleActivityStrategy,
)

# Factor-based strategies
try:
    from strategies.factor_based import (
        # Momentum and Trend
        ROCStrategy,
        PriceAccelerationStrategy,
        AroonStrategy,
        # Volatility
        ParkinsonVolStrategy,
        UlcerIndexStrategy,
        # Liquidity and Volume
        MFIStrategy,
        VWAPStrategy,
        OBVStrategy,
        # Microstructure
        OrderFlowImbalanceStrategy,
        TradeIntensityStrategy,
        # Statistical Arbitrage
        MeanReversionHalfLifeStrategy,
        HurstExponentStrategy,
        # Risk-Based
        VaRBreakoutStrategy,
        MaxDrawdownStrategy,
        SortinoRatioStrategy,
        # Technical Analysis
        WilliamsRStrategy,
        CCIStrategy,
        StochRSIStrategy,
    )
except ImportError:
    pass

ALL_STRATEGIES = [
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "RSIDivergenceStrategy",
    "MACDStrategy",
    "MACDHistogramStrategy",
    "BollingerBandsStrategy",
    "BollingerSqueezeStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "MomentumStrategy",
    "TrendFollowingStrategy",
    "PairsTradingStrategy",
    "FamaFactorArbitrageStrategy",
    "MultiFactorHFStrategy",
    "CEXArbitrageStrategy",
    "TriangularArbitrageStrategy",
    "DEXArbitrageStrategy",
    "FlashLoanArbitrageStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
    # Factor-based strategies
    "ROCStrategy",
    "PriceAccelerationStrategy",
    "AroonStrategy",
    "ParkinsonVolStrategy",
    "UlcerIndexStrategy",
    "MFIStrategy",
    "VWAPStrategy",
    "OBVStrategy",
    "OrderFlowImbalanceStrategy",
    "TradeIntensityStrategy",
    "MeanReversionHalfLifeStrategy",
    "HurstExponentStrategy",
    "VaRBreakoutStrategy",
    "MaxDrawdownStrategy",
    "SortinoRatioStrategy",
    "WilliamsRStrategy",
    "CCIStrategy",
    "StochRSIStrategy",
]

# Optional strategies may be unavailable if dependency is missing.
ALL_STRATEGIES = [name for name in ALL_STRATEGIES if globals().get(name) is not None]

__all__ = [
    "StrategyBase",
    "Signal",
    "SignalType",
    "StrategyState",
    "StrategyManager",
    "strategy_manager",
    "SignalGenerator",
    "signal_generator",
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "RSIDivergenceStrategy",
    "MACDStrategy",
    "MACDHistogramStrategy",
    "BollingerBandsStrategy",
    "BollingerSqueezeStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "MomentumStrategy",
    "TrendFollowingStrategy",
    "PairsTradingStrategy",
    "FamaFactorArbitrageStrategy",
    "MultiFactorHFStrategy",
    "CEXArbitrageStrategy",
    "TriangularArbitrageStrategy",
    "DEXArbitrageStrategy",
    "FlashLoanArbitrageStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
    # Factor-based strategies
    "ROCStrategy",
    "PriceAccelerationStrategy",
    "AroonStrategy",
    "ParkinsonVolStrategy",
    "UlcerIndexStrategy",
    "MFIStrategy",
    "VWAPStrategy",
    "OBVStrategy",
    "OrderFlowImbalanceStrategy",
    "TradeIntensityStrategy",
    "MeanReversionHalfLifeStrategy",
    "HurstExponentStrategy",
    "VaRBreakoutStrategy",
    "MaxDrawdownStrategy",
    "SortinoRatioStrategy",
    "WilliamsRStrategy",
    "CCIStrategy",
    "StochRSIStrategy",
    "ALL_STRATEGIES",
]

__all__ = [name for name in __all__ if name == "ALL_STRATEGIES" or globals().get(name) is not None]
