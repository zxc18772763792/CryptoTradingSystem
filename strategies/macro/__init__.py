"""
宏观策略模块
"""
from strategies.macro.market_sentiment import (
    MarketSentimentStrategy,
    SocialSentimentStrategy,
)
from strategies.macro.fund_flow import (
    FundFlowStrategy,
    WhaleActivityStrategy,
)
from strategies.macro.onchain_flow_regime import OnChainFlowRegimeStrategy

__all__ = [
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
    "OnChainFlowRegimeStrategy",
]
