"""
情绪数据模块

包含恐惧贪婪指数、社交情绪等数据采集和处理。
"""
from core.data.sentiment.fear_greed_collector import FearGreedCollector, FearGreedIndex

__all__ = [
    "FearGreedCollector",
    "FearGreedIndex",
]