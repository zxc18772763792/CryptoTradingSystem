"""
量化策略模块
"""
from strategies.quantitative.mean_reversion import MeanReversionStrategy, BollingerMeanReversionStrategy
from strategies.quantitative.momentum import MomentumStrategy, TrendFollowingStrategy
from strategies.quantitative.pairs_trading import PairsTradingStrategy
from strategies.quantitative.fama_factor_arbitrage import FamaFactorArbitrageStrategy
from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy
from strategies.quantitative.liquidation_oi_crowding import LiquidationOICrowdingStrategy
from strategies.quantitative.altcoin_downtrend_bounce_short import AltcoinDowntrendBounceShortStrategy
from strategies.quantitative.oi_mcap_ambush import (
    AccumulationAmbushStrategy,
    IgnitionFastFollowStrategy,
    SqueezeFuelStrategy,
)
from strategies.quantitative.intraday_cross_section import (
    BodyVolumeCorr24hStrategy,
    BreakCountBalance24hStrategy,
    CloseLocation48hStrategy,
    CorrBreakdown24h72hStrategy,
    CrossSectionalStress4hStrategy,
    DirectionalRangeEfficiency48hStrategy,
    ExtremeRecency48hStrategy,
    FalseBreakoutSupply24hStrategy,
    LeadMarketResponse24h72hStrategy,
    RelRet24hReversalStrategy,
    RelativeVolShock24hStrategy,
    ResidualMom48hStrategy,
    Ret24hReversalStrategy,
    ReturnEntropy4hStrategy,
    RangeAsymmetry48hStrategy,
    SessionAsiaFlow24hStrategy,
    SessionFlowRotation24hStrategy,
    SignImbalance4hStrategy,
    TurnoverEntropy48hStrategy,
    UpDownBetaSpread24h72hStrategy,
    VolumeWeightedReturn24hStrategy,
    VWAPGap48hStrategy,
    VWAPSlope24hStrategy,
    WickImbalance48hStrategy,
)

__all__ = [
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "MomentumStrategy",
    "TrendFollowingStrategy",
    "PairsTradingStrategy",
    "FamaFactorArbitrageStrategy",
    "MultiFactorHFStrategy",
    "LiquidationOICrowdingStrategy",
    "AltcoinDowntrendBounceShortStrategy",
    "AccumulationAmbushStrategy",
    "SqueezeFuelStrategy",
    "IgnitionFastFollowStrategy",
    "ResidualMom48hStrategy",
    "Ret24hReversalStrategy",
    "RelRet24hReversalStrategy",
    "CloseLocation48hStrategy",
    "ReturnEntropy4hStrategy",
    "FalseBreakoutSupply24hStrategy",
    "RangeAsymmetry48hStrategy",
    "SessionAsiaFlow24hStrategy",
    "SessionFlowRotation24hStrategy",
    "VolumeWeightedReturn24hStrategy",
    "WickImbalance48hStrategy",
    "TurnoverEntropy48hStrategy",
    "BodyVolumeCorr24hStrategy",
    "CorrBreakdown24h72hStrategy",
    "ExtremeRecency48hStrategy",
    "UpDownBetaSpread24h72hStrategy",
    "DirectionalRangeEfficiency48hStrategy",
    "CrossSectionalStress4hStrategy",
    "SignImbalance4hStrategy",
    "VWAPSlope24hStrategy",
    "VWAPGap48hStrategy",
    "RelativeVolShock24hStrategy",
    "LeadMarketResponse24h72hStrategy",
    "BreakCountBalance24hStrategy",
]
