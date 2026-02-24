"""
信号生成器模块
提供信号过滤、组合和验证功能
"""
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType


class SignalFilter:
    """信号过滤器"""

    def __init__(
        self,
        min_strength: float = 0.5,
        max_signals_per_hour: int = 10,
        cooldown_minutes: int = 5,
    ):
        self.min_strength = min_strength
        self.max_signals_per_hour = max_signals_per_hour
        self.cooldown_minutes = cooldown_minutes
        self._recent_signals: Dict[str, List[datetime]] = {}

    def filter(self, signal: Signal) -> bool:
        """
        过滤信号

        Args:
            signal: 待过滤的信号

        Returns:
            是否通过过滤
        """
        # 检查信号强度
        if signal.strength < self.min_strength:
            logger.debug(f"Signal filtered: strength {signal.strength} < {self.min_strength}")
            return False

        # 检查冷却时间
        key = f"{signal.symbol}_{signal.signal_type.value}"
        recent = self._recent_signals.get(key, [])

        # 清理过期记录
        cutoff = datetime.now() - timedelta(hours=1)
        recent = [t for t in recent if t > cutoff]
        self._recent_signals[key] = recent

        # 检查频率限制
        if len(recent) >= self.max_signals_per_hour:
            logger.debug(f"Signal filtered: max signals per hour reached for {key}")
            return False

        # 检查冷却时间
        if recent:
            last_time = max(recent)
            if datetime.now() - last_time < timedelta(minutes=self.cooldown_minutes):
                logger.debug(f"Signal filtered: cooldown period for {key}")
                return False

        # 记录信号
        self._recent_signals[key].append(signal.timestamp)
        return True


class SignalCombiner:
    """信号组合器"""

    def __init__(self):
        pass

    def combine(
        self,
        signals: List[Signal],
        method: str = "weighted_vote",
    ) -> Optional[Signal]:
        """
        组合多个信号

        Args:
            signals: 信号列表
            method: 组合方法 (weighted_vote, majority, average)

        Returns:
            组合后的信号
        """
        if not signals:
            return None

        if method == "weighted_vote":
            return self._weighted_vote(signals)
        elif method == "majority":
            return self._majority_vote(signals)
        elif method == "average":
            return self._average_signal(signals)
        else:
            return signals[0]

    def _weighted_vote(self, signals: List[Signal]) -> Signal:
        """加权投票"""
        buy_weight = sum(s.strength for s in signals if s.signal_type in [SignalType.BUY, SignalType.CLOSE_SHORT])
        sell_weight = sum(s.strength for s in signals if s.signal_type in [SignalType.SELL, SignalType.CLOSE_LONG])

        if buy_weight > sell_weight:
            signal_type = SignalType.BUY
            strength = buy_weight / (buy_weight + sell_weight)
        elif sell_weight > buy_weight:
            signal_type = SignalType.SELL
            strength = sell_weight / (buy_weight + sell_weight)
        else:
            signal_type = SignalType.HOLD
            strength = 0.5

        # 使用最新信号的基础信息
        latest = max(signals, key=lambda s: s.timestamp)
        return Signal(
            symbol=latest.symbol,
            signal_type=signal_type,
            price=latest.price,
            timestamp=datetime.now(),
            strategy_name="combined",
            strength=strength,
        )

    def _majority_vote(self, signals: List[Signal]) -> Signal:
        """多数投票"""
        buy_count = sum(1 for s in signals if s.signal_type in [SignalType.BUY, SignalType.CLOSE_SHORT])
        sell_count = sum(1 for s in signals if s.signal_type in [SignalType.SELL, SignalType.CLOSE_LONG])

        if buy_count > sell_count:
            signal_type = SignalType.BUY
        elif sell_count > buy_count:
            signal_type = SignalType.SELL
        else:
            signal_type = SignalType.HOLD

        latest = max(signals, key=lambda s: s.timestamp)
        return Signal(
            symbol=latest.symbol,
            signal_type=signal_type,
            price=latest.price,
            timestamp=datetime.now(),
            strategy_name="combined",
            strength=len(signals) / 10,  # 简单强度计算
        )

    def _average_signal(self, signals: List[Signal]) -> Signal:
        """平均信号"""
        avg_strength = sum(s.strength for s in signals) / len(signals)
        avg_price = sum(s.price for s in signals) / len(signals)

        # 找出最常见的信号类型
        type_counts = {}
        for s in signals:
            t = s.signal_type
            type_counts[t] = type_counts.get(t, 0) + 1

        signal_type = max(type_counts, key=type_counts.get)

        latest = max(signals, key=lambda s: s.timestamp)
        return Signal(
            symbol=latest.symbol,
            signal_type=signal_type,
            price=avg_price,
            timestamp=datetime.now(),
            strategy_name="combined",
            strength=avg_strength,
        )


class SignalValidator:
    """信号验证器"""

    def __init__(
        self,
        max_price_deviation: float = 0.05,  # 最大价格偏差
        require_stop_loss: bool = False,
        min_quantity: float = 0.0001,
    ):
        self.max_price_deviation = max_price_deviation
        self.require_stop_loss = require_stop_loss
        self.min_quantity = min_quantity

    def validate(
        self,
        signal: Signal,
        current_price: Optional[float] = None,
    ) -> bool:
        """
        验证信号

        Args:
            signal: 待验证的信号
            current_price: 当前价格

        Returns:
            是否验证通过
        """
        # 检查价格
        if signal.price <= 0:
            logger.warning(f"Invalid signal price: {signal.price}")
            return False

        # 检查价格偏差
        if current_price:
            deviation = abs(signal.price - current_price) / current_price
            if deviation > self.max_price_deviation:
                logger.warning(f"Signal price deviation too high: {deviation:.2%}")
                return False

        # 检查止损
        if self.require_stop_loss and signal.signal_type in [SignalType.BUY, SignalType.SELL]:
            if signal.stop_loss is None:
                logger.warning("Signal missing stop loss")
                return False

        # 检查数量
        if signal.quantity is not None and signal.quantity < self.min_quantity:
            logger.warning(f"Signal quantity too small: {signal.quantity}")
            return False

        return True


class SignalGenerator:
    """信号生成器（整合过滤、组合、验证）"""

    def __init__(
        self,
        min_strength: float = 0.5,
        max_signals_per_hour: int = 10,
        cooldown_minutes: int = 5,
        combine_method: str = "weighted_vote",
    ):
        self.filter = SignalFilter(
            min_strength=min_strength,
            max_signals_per_hour=max_signals_per_hour,
            cooldown_minutes=cooldown_minutes,
        )
        self.combiner = SignalCombiner()
        self.validator = SignalValidator()
        self.combine_method = combine_method

    def process(
        self,
        signals: List[Signal],
        current_price: Optional[float] = None,
    ) -> List[Signal]:
        """
        处理信号列表

        Args:
            signals: 原始信号列表
            current_price: 当前价格

        Returns:
            处理后的信号列表
        """
        # 过滤
        filtered = [s for s in signals if self.filter.filter(s)]

        # 验证
        validated = [
            s for s in filtered
            if self.validator.validate(s, current_price)
        ]

        return validated

    def combine_signals(
        self,
        signals: List[Signal],
    ) -> Optional[Signal]:
        """组合信号"""
        return self.combiner.combine(signals, self.combine_method)


# 全局信号生成器实例
signal_generator = SignalGenerator()
