"""
Factor-based strategies module.

Each strategy is based on a corresponding time-series factor from the extended factor library.
"""
from strategies.factor_based.factor_strategies import (
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

__all__ = [
    # Momentum and Trend
    "ROCStrategy",
    "PriceAccelerationStrategy",
    "AroonStrategy",

    # Volatility
    "ParkinsonVolStrategy",
    "UlcerIndexStrategy",

    # Liquidity and Volume
    "MFIStrategy",
    "VWAPStrategy",
    "OBVStrategy",

    # Microstructure
    "OrderFlowImbalanceStrategy",
    "TradeIntensityStrategy",

    # Statistical Arbitrage
    "MeanReversionHalfLifeStrategy",
    "HurstExponentStrategy",

    # Risk-Based
    "VaRBreakoutStrategy",
    "MaxDrawdownStrategy",
    "SortinoRatioStrategy",

    # Technical Analysis
    "WilliamsRStrategy",
    "CCIStrategy",
    "StochRSIStrategy",
]
