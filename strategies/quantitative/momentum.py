"""
动量策略
"""
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from core.strategies.strategy_base import (
    StrategyBase,
    Signal,
    SignalType,
)


class MomentumStrategy(StrategyBase):
    """动量策略"""

    def __init__(
        self,
        name: str = "Momentum",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "lookback_period": 14,
            "momentum_threshold": 0.02,  # 动量阈值（2%）
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_momentum(self, data: pd.DataFrame) -> pd.Series:
        """计算动量（价格变化率）"""
        period = self.params["lookback_period"]
        momentum = data["close"] / data["close"].shift(period) - 1
        return momentum

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["lookback_period"] + 5:
            return []

        signals = []

        momentum = self._calculate_momentum(data)

        current_momentum = momentum.iloc[-1]
        prev_momentum = momentum.iloc[-2]

        current_price = data["close"].iloc[-1]
        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        threshold = self.params["momentum_threshold"]

        # 正动量突破阈值 - 买入
        if prev_momentum < threshold and current_momentum >= threshold:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(current_momentum / threshold, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={"momentum": current_momentum}
            )
            signals.append(signal)
            logger.info(f"Momentum BUY for {symbol}: momentum={current_momentum:.4f}")

        # 负动量突破阈值 - 卖出
        elif prev_momentum > -threshold and current_momentum <= -threshold:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_momentum) / threshold, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={"momentum": current_momentum}
            )
            signals.append(signal)
            logger.info(f"Momentum SELL for {symbol}: momentum={current_momentum:.4f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["lookback_period"] + 10,
        }


class TrendFollowingStrategy(StrategyBase):
    """趋势跟踪策略"""

    def __init__(
        self,
        name: str = "Trend_Following",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "short_period": 20,
            "long_period": 50,
            "adx_threshold": 25,  # ADX阈值（趋势强度）
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_adx(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算ADX（平均趋向指数）"""
        high = data["high"]
        low = data["low"]
        close = data["close"]

        # 计算+DM和-DM
        plus_dm = high.diff()
        minus_dm = -low.diff()

        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

        # 计算TR
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 平滑
        atr = tr.rolling(period).mean()
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

        # 计算DX和ADX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(period).mean()

        return adx, plus_di, minus_di

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["long_period"] + 20:
            return []

        signals = []

        # 计算移动平均
        short_ma = data["close"].rolling(self.params["short_period"]).mean()
        long_ma = data["close"].rolling(self.params["long_period"]).mean()

        # 计算ADX
        adx, plus_di, minus_di = self._calculate_adx(data)

        current_price = data["close"].iloc[-1]
        current_short_ma = short_ma.iloc[-1]
        current_long_ma = long_ma.iloc[-1]
        current_adx = adx.iloc[-1]

        prev_short_ma = short_ma.iloc[-2]
        prev_long_ma = long_ma.iloc[-2]

        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # 只有在趋势足够强时才发出信号
        if current_adx >= self.params["adx_threshold"]:
            # 短期均线上穿长期均线，且趋势向上
            if prev_short_ma <= prev_long_ma and current_short_ma > current_long_ma:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=min(current_adx / 50, 1.0),  # ADX越强，信号越强
                    stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                    take_profit=current_price * (1 + self.params["take_profit_pct"]),
                    metadata={
                        "adx": current_adx,
                        "trend": "up",
                    }
                )
                signals.append(signal)
                logger.info(f"Trend following BUY for {symbol}: ADX={current_adx:.2f}")

            # 短期均线下穿长期均线，且趋势向下
            elif prev_short_ma >= prev_long_ma and current_short_ma < current_long_ma:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=min(current_adx / 50, 1.0),
                    stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                    take_profit=current_price * (1 - self.params["take_profit_pct"]),
                    metadata={
                        "adx": current_adx,
                        "trend": "down",
                    }
                )
                signals.append(signal)
                logger.info(f"Trend following SELL for {symbol}: ADX={current_adx:.2f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": self.params["long_period"] + 30,
        }
