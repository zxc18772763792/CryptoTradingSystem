"""
止损管理模块
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from dataclasses import dataclass
from loguru import logger

from core.trading.position_manager import position_manager, PositionSide


class StopLossType(Enum):
    """止损类型"""
    FIXED = "fixed"  # 固定止损
    TRAILING = "trailing"  # 追踪止损
    ATR_BASED = "atr_based"  # ATR止损
    PERCENTAGE = "percentage"  # 百分比止损
    TIME_BASED = "time_based"  # 时间止损


@dataclass
class StopLossConfig:
    """止损配置"""
    type: StopLossType
    value: float  # 止损值（百分比或ATR倍数）
    trailing_activation: Optional[float] = None  # 追踪止损激活点
    time_limit: Optional[int] = None  # 时间限制（小时）


class StopLossManager:
    """止损管理器"""

    def __init__(self):
        self._stop_losses: Dict[str, StopLossConfig] = {}  # position_key -> config
        self._trailing_stops: Dict[str, float] = {}  # position_key -> highest_price

    def set_stop_loss(
        self,
        exchange: str,
        symbol: str,
        config: StopLossConfig,
    ) -> None:
        """设置止损"""
        key = f"{exchange}_{symbol}"
        self._stop_losses[key] = config
        logger.info(f"Stop loss set for {symbol}: {config.type.value} @ {config.value}")

    def remove_stop_loss(self, exchange: str, symbol: str) -> None:
        """移除止损"""
        key = f"{exchange}_{symbol}"
        self._stop_losses.pop(key, None)
        self._trailing_stops.pop(key, None)

    def calculate_stop_price(
        self,
        exchange: str,
        symbol: str,
        entry_price: float,
        current_price: float,
        position_side: PositionSide,
        atr: Optional[float] = None,
    ) -> Optional[float]:
        """
        计算止损价格

        Args:
            exchange: 交易所
            symbol: 交易对
            entry_price: 入场价格
            current_price: 当前价格
            position_side: 持仓方向
            atr: ATR值

        Returns:
            止损价格
        """
        key = f"{exchange}_{symbol}"
        config = self._stop_losses.get(key)

        if not config:
            return None

        stop_price = None

        if config.type == StopLossType.FIXED:
            stop_price = self._fixed_stop(entry_price, config.value, position_side)

        elif config.type == StopLossType.PERCENTAGE:
            stop_price = self._percentage_stop(entry_price, config.value, position_side)

        elif config.type == StopLossType.TRAILING:
            stop_price = self._trailing_stop(
                key, entry_price, current_price, config.value, position_side
            )

        elif config.type == StopLossType.ATR_BASED and atr:
            stop_price = self._atr_stop(entry_price, atr, config.value, position_side)

        return stop_price

    def _fixed_stop(
        self,
        entry_price: float,
        stop_value: float,
        position_side: PositionSide,
    ) -> float:
        """固定价格止损"""
        if position_side == PositionSide.LONG:
            return entry_price - stop_value
        else:
            return entry_price + stop_value

    def _percentage_stop(
        self,
        entry_price: float,
        percentage: float,
        position_side: PositionSide,
    ) -> float:
        """百分比止损"""
        if position_side == PositionSide.LONG:
            return entry_price * (1 - percentage)
        else:
            return entry_price * (1 + percentage)

    def _trailing_stop(
        self,
        key: str,
        entry_price: float,
        current_price: float,
        trail_percent: float,
        position_side: PositionSide,
    ) -> float:
        """追踪止损"""
        # 更新最高/最低价
        if key not in self._trailing_stops:
            self._trailing_stops[key] = entry_price

        if position_side == PositionSide.LONG:
            # 多头：追踪最高价
            self._trailing_stops[key] = max(self._trailing_stops[key], current_price)
            return self._trailing_stops[key] * (1 - trail_percent)
        else:
            # 空头：追踪最低价
            self._trailing_stops[key] = min(self._trailing_stops[key], current_price)
            return self._trailing_stops[key] * (1 + trail_percent)

    def _atr_stop(
        self,
        entry_price: float,
        atr: float,
        multiplier: float,
        position_side: PositionSide,
    ) -> float:
        """ATR止损"""
        if position_side == PositionSide.LONG:
            return entry_price - atr * multiplier
        else:
            return entry_price + atr * multiplier

    def check_stop_loss(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        position_side: PositionSide,
        atr: Optional[float] = None,
    ) -> bool:
        """
        检查是否触发止损

        Args:
            exchange: 交易所
            symbol: 交易对
            current_price: 当前价格
            position_side: 持仓方向
            atr: ATR值

        Returns:
            是否触发止损
        """
        position = position_manager.get_position(exchange, symbol)
        if not position:
            return False

        stop_price = self.calculate_stop_price(
            exchange, symbol,
            position.entry_price, current_price,
            position_side, atr
        )

        if stop_price is None:
            return False

        if position_side == PositionSide.LONG:
            triggered = current_price <= stop_price
        else:
            triggered = current_price >= stop_price

        if triggered:
            logger.warning(
                f"Stop loss triggered for {symbol}: "
                f"current={current_price}, stop={stop_price}"
            )

        return triggered

    def get_stop_loss_info(self, exchange: str, symbol: str) -> Optional[Dict]:
        """获取止损信息"""
        key = f"{exchange}_{symbol}"
        config = self._stop_losses.get(key)

        if not config:
            return None

        return {
            "type": config.type.value,
            "value": config.value,
            "trailing_activation": config.trailing_activation,
            "highest_price": self._trailing_stops.get(key),
        }

    def update_trailing_stop(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
    ) -> Optional[float]:
        """更新追踪止损"""
        key = f"{exchange}_{symbol}"
        config = self._stop_losses.get(key)

        if not config or config.type != StopLossType.TRAILING:
            return None

        position = position_manager.get_position(exchange, symbol)
        if not position:
            return None

        # 检查是否激活追踪止损
        if config.trailing_activation:
            profit_pct = (current_price - position.entry_price) / position.entry_price
            if position.side == PositionSide.SHORT:
                profit_pct = -profit_pct

            if profit_pct < config.trailing_activation:
                return None

        # 更新追踪
        return self._trailing_stop(
            key, position.entry_price, current_price,
            config.value, position.side
        )


class TakeProfitManager:
    """止盈管理器"""

    def __init__(self):
        self._take_profits: Dict[str, List[Dict]] = {}  # position_key -> targets

    def set_take_profit(
        self,
        exchange: str,
        symbol: str,
        targets: List[Dict],
    ) -> None:
        """
        设置止盈目标

        Args:
            exchange: 交易所
            symbol: 交易对
            targets: 止盈目标列表 [{"price": 100, "quantity_pct": 0.5}, ...]
        """
        key = f"{exchange}_{symbol}"
        self._take_profits[key] = targets
        logger.info(f"Take profit targets set for {symbol}: {len(targets)} levels")

    def check_take_profit(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        position_side: PositionSide,
    ) -> Optional[Dict]:
        """
        检查是否触发止盈

        Returns:
            触发的止盈信息，或None
        """
        key = f"{exchange}_{symbol}"
        targets = self._take_profits.get(key)

        if not targets:
            return None

        for target in targets:
            target_price = target["price"]

            if position_side == PositionSide.LONG:
                triggered = current_price >= target_price
            else:
                triggered = current_price <= target_price

            if triggered and not target.get("executed", False):
                target["executed"] = True
                logger.info(
                    f"Take profit triggered for {symbol}: "
                    f"price={current_price}, target={target_price}"
                )
                return target

        return None

    def remove_take_profit(self, exchange: str, symbol: str) -> None:
        """移除止盈"""
        key = f"{exchange}_{symbol}"
        self._take_profits.pop(key, None)


# 全局实例
stop_loss_manager = StopLossManager()
take_profit_manager = TakeProfitManager()
