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

__all__ = [
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
]
