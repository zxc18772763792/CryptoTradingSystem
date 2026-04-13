"""
Stop loss and take profit helpers.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from loguru import logger

from core.trading.position_manager import PositionSide, position_manager


class StopLossType(Enum):
    """Supported stop loss modes."""

    FIXED = "fixed"
    TRAILING = "trailing"
    ATR_BASED = "atr_based"
    PERCENTAGE = "percentage"
    TIME_BASED = "time_based"


@dataclass
class StopLossConfig:
    """Stop loss configuration."""

    type: StopLossType
    value: float
    trailing_activation: Optional[float] = None
    time_limit: Optional[int] = None


class StopLossManager:
    """Manage stop loss configurations per position bucket."""

    def __init__(self):
        self._stop_losses: Dict[str, StopLossConfig] = {}
        self._trailing_stops: Dict[str, float] = {}

    @staticmethod
    def _position_key(exchange: str, symbol: str, strategy: Optional[str] = None) -> str:
        return f"{exchange}_{symbol}::{str(strategy or '').strip()}"

    def set_stop_loss(
        self,
        exchange: str,
        symbol: str,
        config: StopLossConfig,
        strategy: Optional[str] = None,
    ) -> None:
        """Store stop loss config for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
        self._stop_losses[key] = config
        logger.info(f"Stop loss set for {symbol}: {config.type.value} @ {config.value}")

    def remove_stop_loss(self, exchange: str, symbol: str, strategy: Optional[str] = None) -> None:
        """Clear stop loss config for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
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
        strategy: Optional[str] = None,
    ) -> Optional[float]:
        """Calculate the current stop price for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
        config = self._stop_losses.get(key)
        if not config:
            return None

        if config.type == StopLossType.FIXED:
            return self._fixed_stop(entry_price, config.value, position_side)
        if config.type == StopLossType.PERCENTAGE:
            return self._percentage_stop(entry_price, config.value, position_side)
        if config.type == StopLossType.TRAILING:
            return self._trailing_stop(key, entry_price, current_price, config.value, position_side)
        if config.type == StopLossType.ATR_BASED and atr:
            return self._atr_stop(entry_price, atr, config.value, position_side)
        return None

    def _fixed_stop(
        self,
        entry_price: float,
        stop_value: float,
        position_side: PositionSide,
    ) -> float:
        if position_side == PositionSide.LONG:
            return entry_price - stop_value
        return entry_price + stop_value

    def _percentage_stop(
        self,
        entry_price: float,
        percentage: float,
        position_side: PositionSide,
    ) -> float:
        if position_side == PositionSide.LONG:
            return entry_price * (1 - percentage)
        return entry_price * (1 + percentage)

    def _trailing_stop(
        self,
        key: str,
        entry_price: float,
        current_price: float,
        trail_percent: float,
        position_side: PositionSide,
    ) -> float:
        if key not in self._trailing_stops:
            self._trailing_stops[key] = entry_price

        if position_side == PositionSide.LONG:
            self._trailing_stops[key] = max(self._trailing_stops[key], current_price)
            return self._trailing_stops[key] * (1 - trail_percent)

        self._trailing_stops[key] = min(self._trailing_stops[key], current_price)
        return self._trailing_stops[key] * (1 + trail_percent)

    def _atr_stop(
        self,
        entry_price: float,
        atr: float,
        multiplier: float,
        position_side: PositionSide,
    ) -> float:
        if position_side == PositionSide.LONG:
            return entry_price - atr * multiplier
        return entry_price + atr * multiplier

    def check_stop_loss(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        position_side: PositionSide,
        atr: Optional[float] = None,
        strategy: Optional[str] = None,
    ) -> bool:
        """Check whether the configured stop is triggered."""
        strategy_lookup = str(strategy).strip() if strategy is not None else None
        position = position_manager.get_position(exchange, symbol, strategy=strategy_lookup)
        if not position:
            return False

        stop_price = self.calculate_stop_price(
            exchange,
            symbol,
            position.entry_price,
            current_price,
            position_side,
            atr,
            strategy=strategy,
        )
        if stop_price is None:
            return False

        if position_side == PositionSide.LONG:
            triggered = current_price <= stop_price
        else:
            triggered = current_price >= stop_price

        if triggered:
            logger.warning(
                f"Stop loss triggered for {symbol}: current={current_price}, stop={stop_price}"
            )

        return triggered

    def get_stop_loss_info(
        self,
        exchange: str,
        symbol: str,
        strategy: Optional[str] = None,
    ) -> Optional[Dict]:
        """Return stored stop loss metadata for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
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
        strategy: Optional[str] = None,
    ) -> Optional[float]:
        """Advance a trailing stop if the position is eligible."""
        key = self._position_key(exchange, symbol, strategy)
        config = self._stop_losses.get(key)
        if not config or config.type != StopLossType.TRAILING:
            return None

        strategy_lookup = str(strategy).strip() if strategy is not None else None
        position = position_manager.get_position(exchange, symbol, strategy=strategy_lookup)
        if not position:
            return None

        if config.trailing_activation:
            profit_pct = (current_price - position.entry_price) / position.entry_price
            if position.side == PositionSide.SHORT:
                profit_pct = -profit_pct
            if profit_pct < config.trailing_activation:
                return None

        return self._trailing_stop(key, position.entry_price, current_price, config.value, position.side)


class TakeProfitManager:
    """Manage take profit targets per position bucket."""

    def __init__(self):
        self._take_profits: Dict[str, List[Dict]] = {}

    @staticmethod
    def _position_key(exchange: str, symbol: str, strategy: Optional[str] = None) -> str:
        return f"{exchange}_{symbol}::{str(strategy or '').strip()}"

    def set_take_profit(
        self,
        exchange: str,
        symbol: str,
        targets: List[Dict],
        strategy: Optional[str] = None,
    ) -> None:
        """Store take profit targets for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
        self._take_profits[key] = targets
        logger.info(f"Take profit targets set for {symbol}: {len(targets)} levels")

    def check_take_profit(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        position_side: PositionSide,
        strategy: Optional[str] = None,
    ) -> Optional[Dict]:
        """Return the first triggered take profit target, if any."""
        key = self._position_key(exchange, symbol, strategy)
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
                    f"Take profit triggered for {symbol}: price={current_price}, target={target_price}"
                )
                return target

        return None

    def remove_take_profit(self, exchange: str, symbol: str, strategy: Optional[str] = None) -> None:
        """Clear take profit targets for a position bucket."""
        key = self._position_key(exchange, symbol, strategy)
        self._take_profits.pop(key, None)


stop_loss_manager = StopLossManager()
take_profit_manager = TakeProfitManager()
