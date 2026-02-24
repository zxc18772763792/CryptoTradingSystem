"""
仓位计算模块
"""
from typing import Optional, Dict, Any
from enum import Enum
from loguru import logger

from core.risk.risk_manager import risk_manager


class SizingMethod(Enum):
    """仓位计算方法"""
    FIXED = "fixed"  # 固定金额
    PERCENT = "percent"  # 固定比例
    RISK_PARITY = "risk_parity"  # 风险平价
    KELLY = "kelly"  # 凯利公式
    ATR_BASED = "atr_based"  # 基于ATR


class PositionSizer:
    """仓位计算器"""

    def __init__(self):
        self.default_method = SizingMethod.PERCENT
        self.default_risk_per_trade = 0.02  # 单笔风险2%
        self.default_position_pct = 0.05  # 默认仓位5%

    def calculate(
        self,
        method: SizingMethod,
        account_balance: float,
        entry_price: float,
        stop_loss: Optional[float] = None,
        atr: Optional[float] = None,
        win_rate: Optional[float] = None,
        avg_win_loss_ratio: Optional[float] = None,
        volatility: Optional[float] = None,
        **kwargs,
    ) -> float:
        """
        计算仓位大小

        Args:
            method: 计算方法
            account_balance: 账户余额
            entry_price: 入场价格
            stop_loss: 止损价格
            atr: ATR值
            win_rate: 胜率
            avg_win_loss_ratio: 平均盈亏比
            volatility: 波动率

        Returns:
            建议仓位数量
        """
        if method == SizingMethod.FIXED:
            return self._fixed_sizing(account_balance, entry_price, **kwargs)
        elif method == SizingMethod.PERCENT:
            return self._percent_sizing(account_balance, entry_price, **kwargs)
        elif method == SizingMethod.RISK_PARITY:
            return self._risk_parity_sizing(account_balance, entry_price, volatility, **kwargs)
        elif method == SizingMethod.KELLY:
            return self._kelly_sizing(account_balance, entry_price, win_rate, avg_win_loss_ratio, **kwargs)
        elif method == SizingMethod.ATR_BASED:
            return self._atr_based_sizing(account_balance, entry_price, atr, **kwargs)
        else:
            return self._percent_sizing(account_balance, entry_price, **kwargs)

    def _fixed_sizing(
        self,
        account_balance: float,
        entry_price: float,
        fixed_amount: float = 1000,
        **kwargs,
    ) -> float:
        """固定金额仓位"""
        return fixed_amount / entry_price

    def _percent_sizing(
        self,
        account_balance: float,
        entry_price: float,
        position_pct: Optional[float] = None,
        **kwargs,
    ) -> float:
        """固定比例仓位"""
        pct = position_pct or self.default_position_pct
        position_value = account_balance * pct
        return position_value / entry_price

    def _risk_parity_sizing(
        self,
        account_balance: float,
        entry_price: float,
        volatility: Optional[float] = None,
        target_risk: float = 0.02,
        **kwargs,
    ) -> float:
        """风险平价仓位"""
        if not volatility:
            # 如果没有波动率，使用默认比例
            return self._percent_sizing(account_balance, entry_price, **kwargs)

        # 根据波动率调整仓位
        # 目标是使每个仓位的波动风险相同
        position_value = (account_balance * target_risk) / volatility
        return position_value / entry_price

    def _kelly_sizing(
        self,
        account_balance: float,
        entry_price: float,
        win_rate: Optional[float] = None,
        avg_win_loss_ratio: Optional[float] = None,
        kelly_fraction: float = 0.5,  # 使用半凯利以降低风险
        **kwargs,
    ) -> float:
        """凯利公式仓位"""
        if not win_rate or not avg_win_loss_ratio:
            return self._percent_sizing(account_balance, entry_price, **kwargs)

        # 凯利公式: f* = p - (1-p)/b
        # p = 胜率, b = 盈亏比
        kelly_pct = win_rate - (1 - win_rate) / avg_win_loss_ratio

        # 限制在合理范围内
        kelly_pct = max(0, min(kelly_pct, 0.25))  # 最大25%

        # 应用凯利分数（通常是半凯利）
        adjusted_pct = kelly_pct * kelly_fraction

        position_value = account_balance * adjusted_pct
        return position_value / entry_price

    def _atr_based_sizing(
        self,
        account_balance: float,
        entry_price: float,
        atr: Optional[float] = None,
        risk_multiple: float = 2.0,
        risk_per_trade: Optional[float] = None,
        **kwargs,
    ) -> float:
        """基于ATR的仓位"""
        if not atr:
            return self._percent_sizing(account_balance, entry_price, **kwargs)

        risk = risk_per_trade or self.default_risk_per_trade
        risk_amount = account_balance * risk

        # 止损距离 = ATR * 风险倍数
        stop_distance = atr * risk_multiple

        # 仓位数量 = 风险金额 / 止损距离
        position_size = risk_amount / stop_distance

        return position_size

    def calculate_with_stop_loss(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
        risk_per_trade: Optional[float] = None,
    ) -> float:
        """
        基于止损计算仓位

        Args:
            account_balance: 账户余额
            entry_price: 入场价格
            stop_loss_price: 止损价格
            risk_per_trade: 单笔风险比例

        Returns:
            建议仓位数量
        """
        risk = risk_per_trade or self.default_risk_per_trade
        risk_amount = account_balance * risk
        price_risk = abs(entry_price - stop_loss_price)

        if price_risk <= 0:
            logger.warning("Invalid stop loss price, using default sizing")
            return self._percent_sizing(account_balance, entry_price)

        position_size = risk_amount / price_risk
        return position_size

    def adjust_for_correlation(
        self,
        base_size: float,
        existing_positions: Dict[str, float],
        correlation: float,
    ) -> float:
        """
        根据相关性调整仓位

        Args:
            base_size: 基础仓位
            existing_positions: 现有持仓
            correlation: 与现有持仓的相关性

        Returns:
            调整后的仓位
        """
        if not existing_positions or abs(correlation) < 0.3:
            return base_size

        # 高相关性时降低仓位
        if abs(correlation) > 0.7:
            adjustment = 0.5  # 降低50%
        elif abs(correlation) > 0.5:
            adjustment = 0.7  # 降低30%
        else:
            adjustment = 0.9  # 降低10%

        return base_size * adjustment


# 全局仓位计算器实例
position_sizer = PositionSizer()
