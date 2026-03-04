"""
策略基类模块
定义所有交易策略的通用接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
import pandas as pd
from loguru import logger


class SignalType(Enum):
    """信号类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE_LONG = "close_long"
    CLOSE_SHORT = "close_short"


class StrategyState(Enum):
    """策略状态"""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class Signal:
    """交易信号"""
    symbol: str
    signal_type: SignalType
    price: float
    timestamp: datetime
    strategy_name: str
    strength: float = 1.0  # 信号强度 0-1
    quantity: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "symbol": self.symbol,
            "signal_type": self.signal_type.value,
            "price": self.price,
            "timestamp": self.timestamp.isoformat(),
            "strategy_name": self.strategy_name,
            "strength": self.strength,
            "quantity": self.quantity,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "metadata": self.metadata,
        }


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    side: str  # long/short
    entry_price: float
    current_price: float
    quantity: float
    entry_time: datetime
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def update_price(self, current_price: float) -> None:
        """更新当前价格和盈亏"""
        self.current_price = current_price
        if self.side == "long":
            self.unrealized_pnl = (current_price - self.entry_price) * self.quantity
            self.unrealized_pnl_pct = (current_price - self.entry_price) / self.entry_price
        else:
            self.unrealized_pnl = (self.entry_price - current_price) * self.quantity
            self.unrealized_pnl_pct = (self.entry_price - current_price) / self.entry_price


class StrategyBase(ABC):
    """策略基类"""

    def __init__(
        self,
        name: str,
        params: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.params = params or {}
        self.state = StrategyState.IDLE
        self.positions: Dict[str, Position] = {}
        self.signals_history: List[Signal] = []
        self._data: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """
        生成交易信号

        Args:
            data: 市场数据DataFrame

        Returns:
            信号列表
        """
        pass

    @abstractmethod
    def get_required_data(self) -> Dict[str, Any]:
        """
        获取策略所需的数据要求

        Returns:
            数据要求配置
        """
        pass

    def initialize(self) -> None:
        """初始化策略"""
        self.state = StrategyState.IDLE
        self.positions.clear()
        self.signals_history.clear()
        logger.info(f"Strategy {self.name} initialized")

    def start(self) -> None:
        """启动策略"""
        self.state = StrategyState.RUNNING
        logger.info(f"Strategy {self.name} started")

    def stop(self) -> None:
        """停止策略"""
        self.state = StrategyState.STOPPED
        logger.info(f"Strategy {self.name} stopped")

    def pause(self) -> None:
        """暂停策略"""
        self.state = StrategyState.PAUSED
        logger.info(f"Strategy {self.name} paused")

    def resume(self) -> None:
        """恢复策略"""
        self.state = StrategyState.RUNNING
        logger.info(f"Strategy {self.name} resumed")

    def open_position(
        self,
        symbol: str,
        side: str,
        price: float,
        quantity: float,
        metadata: Optional[Dict] = None,
    ) -> Position:
        """开仓"""
        position = Position(
            symbol=symbol,
            side=side,
            entry_price=price,
            current_price=price,
            quantity=quantity,
            entry_time=datetime.now(),
            metadata=metadata or {},
        )
        self.positions[symbol] = position
        logger.info(f"Opened {side} position for {symbol} at {price}, quantity: {quantity}")
        return position

    def close_position(
        self,
        symbol: str,
        price: float,
    ) -> Optional[Position]:
        """平仓"""
        position = self.positions.pop(symbol, None)
        if position:
            position.update_price(price)
            logger.info(
                f"Closed {position.side} position for {symbol} at {price}, "
                f"PnL: {position.unrealized_pnl:.2f} ({position.unrealized_pnl_pct*100:.2f}%)"
            )
        return position

    def update_positions(self, prices: Dict[str, float]) -> None:
        """更新所有持仓价格"""
        for symbol, position in self.positions.items():
            if symbol in prices:
                position.update_price(prices[symbol])

    def get_position(self, symbol: str) -> Optional[Position]:
        """获取持仓"""
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        """是否持有仓位"""
        return symbol in self.positions

    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有持仓"""
        return self.positions

    def add_signal_to_history(self, signal: Signal) -> None:
        """添加信号到历史记录"""
        self.signals_history.append(signal)
        # 只保留最近1000条
        if len(self.signals_history) > 1000:
            self.signals_history = self.signals_history[-1000:]

    def get_recent_signals(self, count: int = 100) -> List[Signal]:
        """获取最近的信号"""
        return self.signals_history[-count:]

    def set_param(self, key: str, value: Any) -> None:
        """设置参数"""
        self.params[key] = value
        logger.debug(f"Strategy {self.name} param {key} set to {value}")

    def get_param(self, key: str, default: Any = None) -> Any:
        """获取参数"""
        return self.params.get(key, default)

    def validate_params(self) -> bool:
        """验证参数"""
        return True

    def get_info(self) -> Dict:
        """获取策略信息"""
        return {
            "name": self.name,
            "state": self.state.value,
            "params": self.params,
            "positions_count": len(self.positions),
            "signals_count": len(self.signals_history),
        }

    @staticmethod
    def normalize_strength(raw_value: float, lookback_values: list) -> float:
        """Normalize signal strength using rolling percentile ranking."""
        if not lookback_values:
            return 0.5
        pct = sum(1 for v in lookback_values if v <= raw_value) / len(lookback_values)
        return round(max(0.1, min(1.0, pct)), 3)

    @property
    def min_bars(self) -> int:
        """Minimum number of bars required for signal generation."""
        period = self.params.get('period', 20)
        return max(int(period) * 2, 50)

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self.state == StrategyState.RUNNING

    @property
    def is_idle(self) -> bool:
        """是否空闲"""
        return self.state == StrategyState.IDLE
