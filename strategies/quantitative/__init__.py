"""
量化策略模块
"""
from strategies.quantitative.mean_reversion import MeanReversionStrategy, BollingerMeanReversionStrategy
from strategies.quantitative.momentum import MomentumStrategy, TrendFollowingStrategy
from strategies.quantitative.pairs_trading import PairsTradingStrategy
from strategies.quantitative.fama_factor_arbitrage import FamaFactorArbitrageStrategy
from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy

__all__ = [
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "MomentumStrategy",
    "TrendFollowingStrategy",
    "PairsTradingStrategy",
    "FamaFactorArbitrageStrategy",
    "MultiFactorHFStrategy",
]
