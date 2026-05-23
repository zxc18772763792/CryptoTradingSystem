"""
策略基类模块
定义所有交易策略的通用接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
import math
from typing import Optional, List, Dict, Any
import pandas as pd
from loguru import logger


def bar_time(data: Any, *, fallback: Optional[datetime] = None) -> datetime:
    """Timestamp of the latest bar in ``data``, normalized to tz-aware UTC.

    A strategy signal belongs to the bar that triggered it, not to the wall
    clock at which ``generate_signals`` happened to run — using ``now()``
    breaks replay alignment and the cross-strategy conflict window in
    ``strategy_manager`` (which compares signal timestamps). Falls back to
    wall-clock UTC only when ``data`` has no usable datetime index (e.g.
    real-time/opportunity-driven inputs without bars).
    """
    try:
        idx = getattr(data, "index", None)
        if idx is not None and len(idx):
            last = idx[-1]
            # Only treat as a bar time when it is genuinely datetime-like.
            # A plain int/RangeIndex would be (mis)read by pd.Timestamp as a
            # 1970 epoch offset, so it must fall through to the wall clock.
            is_dt = (
                isinstance(idx, pd.DatetimeIndex)
                or pd.api.types.is_datetime64_any_dtype(getattr(idx, "dtype", None))
                or isinstance(last, (pd.Timestamp, datetime))
            )
            if is_dt:
                ts = pd.Timestamp(last)
                if not pd.isna(ts):
                    py = ts.to_pydatetime()
                    if py.tzinfo is None:
                        py = py.replace(tzinfo=timezone.utc)
                    py = py.astimezone(timezone.utc)
                    fallback_utc = fallback if fallback is not None else datetime.now(timezone.utc)
                    if fallback_utc.tzinfo is None:
                        fallback_utc = fallback_utc.replace(tzinfo=timezone.utc)
                    else:
                        fallback_utc = fallback_utc.astimezone(timezone.utc)
                    if py > fallback_utc + timedelta(minutes=2):
                        shifted = py - timedelta(hours=8)
                        if shifted <= fallback_utc + timedelta(minutes=2):
                            # Heuristic: a bar timestamp ~8h in the future
                            # almost always means the index was Asia/Shanghai
                            # (UTC+8) mislabeled as naive UTC. Surface a
                            # warning so the upstream feed gets fixed rather
                            # than silently shifting indefinitely.
                            logger.warning(
                                "bar_time: detected naive +8h offset on input "
                                "index — applying -8h CST→UTC shift "
                                f"({py.isoformat()} → {shifted.isoformat()}). "
                                "Upstream collector should provide tz-aware UTC."
                            )
                            return shifted
                    return py
    except Exception:
        pass
    return fallback if fallback is not None else datetime.now(timezone.utc)


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
            self.unrealized_pnl_pct = (
                (current_price - self.entry_price) / self.entry_price
                if self.entry_price > 0
                else 0.0
            )
        else:
            self.unrealized_pnl = (self.entry_price - current_price) * self.quantity
            self.unrealized_pnl_pct = (
                (self.entry_price - current_price) / self.entry_price
                if self.entry_price > 0
                else 0.0
            )


class StrategyBase(ABC):
    """策略基类"""

    # Whether ``generate_signals`` writes into its input DataFrame. Default True
    # is the safe assumption — the backtest replay loop will hand the strategy
    # an isolated copy on every bar. Strategies that ONLY read from ``data``
    # (e.g. simple technical indicators that use ``data["close"].rolling(...)``)
    # can override this to ``False`` to opt into a zero-copy view path. Setting
    # this False is a *contract*: violating it will silently corrupt the parent
    # frame during a backtest.
    mutates_input: bool = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        generate = cls.__dict__.get("generate_signals")
        if generate is None or getattr(generate, "_finalizes_strategy_signals", False):
            return

        @wraps(generate)
        def _wrapped_generate(self, *args, **kwargs):
            signals = generate(self, *args, **kwargs)
            data = args[0] if args else kwargs.get("data")
            return self._finalize_generated_signals(data, signals)

        _wrapped_generate._finalizes_strategy_signals = True  # type: ignore[attr-defined]
        setattr(cls, "generate_signals", _wrapped_generate)

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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Return an active exit signal for an existing position, if any.

        Concrete strategies can override this with indicator-specific logic.
        The base implementation provides a conservative generic profit lock:
        after a position has a small ATR-scaled floating profit, close when
        price crosses back through a short moving average.
        """
        metadata = dict(getattr(position, "metadata", {}) or {})
        if metadata.get("generic_check_exit_enabled") is False:
            return None
        if data is None or len(data) < 4 or "close" not in getattr(data, "columns", []):
            return None
        try:
            close = pd.to_numeric(data["close"], errors="coerce")
            if close.dropna().shape[0] < 4:
                return None
            current_price = float(close.iloc[-1])
            prev_price = float(close.iloc[-2])
            ma = close.rolling(min(10, max(3, len(close) // 3)), min_periods=3).mean()
            current_ma = float(ma.iloc[-1])
            prev_ma = float(ma.iloc[-2])
            entry_price = float(getattr(position, "entry_price", 0.0) or 0.0)
        except Exception:
            return None
        if not all(math.isfinite(v) for v in (current_price, prev_price, current_ma, prev_ma, entry_price)):
            return None
        if entry_price <= 0 or current_price <= 0:
            return None

        atr_pct = self.compute_atr_pct(data, default=float(metadata.get("atr_pct") or 0.01))
        min_profit_pct = max(0.002, float(atr_pct or 0.01) * 0.5)
        raw_side = getattr(position, "side", "")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        symbol = str(getattr(position, "symbol", "") or "UNKNOWN")
        timestamp = bar_time(data)

        if side == "long":
            pnl_pct = (current_price - entry_price) / entry_price
            crossed_down = prev_price >= prev_ma and current_price < current_ma
            if pnl_pct >= min_profit_pct and crossed_down:
                return Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_LONG,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.7,
                    metadata={
                        "close_reason": "generic_sma_profit_lock",
                        "close_only": True,
                        "atr_pct": float(atr_pct or 0.0),
                        "generic_check_exit": True,
                    },
                )
        elif side == "short":
            pnl_pct = (entry_price - current_price) / entry_price
            crossed_up = prev_price <= prev_ma and current_price > current_ma
            if pnl_pct >= min_profit_pct and crossed_up:
                return Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_SHORT,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.7,
                    metadata={
                        "close_reason": "generic_sma_profit_lock",
                        "close_only": True,
                        "atr_pct": float(atr_pct or 0.0),
                        "generic_check_exit": True,
                    },
                )
        return None

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"", "0", "false", "no", "off"}:
                return False
            if text in {"1", "true", "yes", "on"}:
                return True
        return bool(value)

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            out = float(value)
        except Exception:
            return default
        if not math.isfinite(out):
            return default
        return out

    @staticmethod
    def _has_ohlc(data: Any) -> bool:
        columns = set(getattr(data, "columns", []))
        return {"high", "low", "close"}.issubset(columns)

    def compute_atr_pct(
        self,
        data: pd.DataFrame,
        period: int = 14,
        *,
        default: Optional[float] = 0.01,
    ) -> Optional[float]:
        if data is None or len(data) == 0 or "close" not in getattr(data, "columns", []):
            return default
        try:
            close = pd.to_numeric(data["close"], errors="coerce")
            reference_price = self._safe_float(close.iloc[-1])
            if reference_price is None or reference_price <= 0:
                return default
            if self._has_ohlc(data):
                high = pd.to_numeric(data["high"], errors="coerce")
                low = pd.to_numeric(data["low"], errors="coerce")
                prev_close = close.shift(1)
                tr = pd.concat(
                    [
                        (high - low).abs(),
                        (high - prev_close).abs(),
                        (low - prev_close).abs(),
                    ],
                    axis=1,
                ).max(axis=1)
            else:
                tr = close.diff().abs()
            tr = pd.to_numeric(tr, errors="coerce").dropna()
            if tr.empty:
                return default
            window = tr.tail(max(1, int(period or 14)))
            atr = self._safe_float(window.mean())
            if atr is None or atr <= 0:
                return default
            atr_pct = atr / float(reference_price)
            if 0 < atr_pct < 1:
                return float(atr_pct)
        except Exception:
            return default
        return default

    def _use_atr_stops_for_signal(self, signal: Signal) -> bool:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        if metadata.get("use_atr_stops") is not None:
            return self._as_bool(metadata.get("use_atr_stops"), True)
        if self.params.get("use_atr_stops") is not None:
            return self._as_bool(self.params.get("use_atr_stops"), True)
        return str(self.__class__.__module__ or "").startswith("strategies.")

    def _finalize_generated_signals(self, data: Any, signals: Any) -> Any:
        if not isinstance(signals, list) or not signals:
            return signals
        atr_period = int(self.params.get("atr_period") or 14)
        atr_pct = self.compute_atr_pct(data, period=atr_period, default=0.01)
        has_ohlc = self._has_ohlc(data)
        finalized: List[Signal] = []
        for signal in signals:
            if not isinstance(signal, Signal):
                finalized.append(signal)
                continue
            metadata = dict(signal.metadata or {})
            if atr_pct is not None:
                metadata.setdefault("atr_pct", float(atr_pct))
                metadata.setdefault("profit_management_atr_pct", float(atr_pct))
            is_entry = signal.signal_type in {SignalType.BUY, SignalType.SELL}
            if is_entry and atr_pct is not None and has_ohlc and self._use_atr_stops_for_signal(signal):
                price = self._safe_float(signal.price)
                if price is None or price <= 0:
                    try:
                        price = self._safe_float(pd.to_numeric(data["close"], errors="coerce").iloc[-1])
                    except Exception:
                        price = None
                if price is not None and price > 0:
                    sl_mult = float(self.params.get("atr_stop_loss_mult") or metadata.get("atr_stop_loss_mult") or 1.5)
                    tp_mult = float(self.params.get("atr_take_profit_mult") or metadata.get("atr_take_profit_mult") or 3.0)
                    stop_pct = max(0.0001, float(atr_pct) * max(0.0, sl_mult))
                    take_pct = max(0.0001, float(atr_pct) * max(0.0, tp_mult))
                    metadata["stop_loss_pct"] = float(stop_pct)
                    metadata["take_profit_pct"] = float(take_pct)
                    metadata["atr_stop_loss_mult"] = float(sl_mult)
                    metadata["atr_take_profit_mult"] = float(tp_mult)
                    metadata["atr_protection_applied"] = True
                    if signal.signal_type == SignalType.BUY:
                        signal.stop_loss = float(price) * (1.0 - stop_pct)
                        signal.take_profit = float(price) * (1.0 + take_pct)
                    else:
                        signal.stop_loss = float(price) * (1.0 + stop_pct)
                        signal.take_profit = float(price) * (1.0 - take_pct)
            signal.metadata = metadata
            finalized.append(signal)
        return finalized

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
            entry_time=datetime.now(timezone.utc),
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

    def _bar_time(self, data: Any, *, fallback: Optional[datetime] = None) -> datetime:
        """Bar timestamp for Signal.timestamp — see module-level bar_time()."""
        return bar_time(data, fallback=fallback)

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
