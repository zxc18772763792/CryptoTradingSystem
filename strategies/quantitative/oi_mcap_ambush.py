"""OI/市值 埋伏策略族（小市值 + 高合约持仓结构）。

三个模式共用同一数据契约：1h OHLCV 之外需要 enrichment 列
``oi_usd``（美元计 OI，前向填充）、``funding_rate``（8h 费率，前向填充）、
``mcap_usd``（日频市值，前向填充）。缺列时策略 fail-closed 返回空信号，
不会退化成纯技术策略乱开仓。

- AccumulationAmbushStrategy（模式A 吸筹埋伏）：OI 多日爬升而价格横盘、
  费率未拥挤时现货式建仓，赔率优先，靠追踪止损吃尾部。
- SqueezeFuelStrategy（模式B 逼空燃料）：深负费率 + OI 堆积 + 突破 24h 高点
  触发，费率翻正（燃料耗尽）即离场。
- IgnitionFastFollowStrategy（模式C 点火快跟）：不预测，放量突破点火后
  快进快出，时间止损兜底。

回测须用 scripts/build_ambush_dataset.py 产出的 enriched 数据集
（scripts/backtest_ambush_modes.py 负责拼列与聚合）。
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.strategies.strategy_base import Signal, SignalType, StrategyBase

ENRICHMENT_COLUMNS = ("oi_usd", "funding_rate", "mcap_usd")


def _tail_values(data: pd.DataFrame, column: str, rows: int):
    """Numeric tail as ndarray — the per-bar hot path must never build Series."""
    if column not in data.columns or not len(data):
        return None
    raw = data[column].to_numpy(copy=False)[-max(2, int(rows)):]
    try:
        return raw.astype(float, copy=False)
    except (TypeError, ValueError):
        return pd.to_numeric(pd.Series(raw), errors="coerce").to_numpy()


def _last_float(data: pd.DataFrame, column: str, default: float = float("nan")) -> float:
    values = _tail_values(data, column, 5)
    if values is None or not len(values):
        return default
    for value in values[::-1]:
        if math.isfinite(value):
            return float(value)
    return default


def _pct_change_over(values, bars: int) -> float:
    if values is None:
        return float("nan")
    clean = values[np.isfinite(values)]
    if len(clean) <= bars:
        return float("nan")
    base = float(clean[-bars - 1])
    if base <= 0 or not math.isfinite(base):
        return float("nan")
    return float(clean[-1]) / base - 1.0


class _AmbushBase(StrategyBase):
    """吸筹/逼空/点火三模式的公共骨架：数据校验、冷却、边沿触发。"""

    mutates_input = False
    required_enrichment: tuple = ENRICHMENT_COLUMNS

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        merged = dict(self.default_params())
        if params:
            merged.update(params)
        merged.setdefault("use_atr_stops", False)
        super().__init__(name, merged)
        self._last_entry_bar_ts: Dict[str, pd.Timestamp] = {}

    @classmethod
    def default_params(cls) -> Dict[str, Any]:
        return {}

    def _symbol_of(self, data: pd.DataFrame) -> str:
        if "symbol" in data.columns and len(data):
            raw = str(data["symbol"].iloc[-1] or "").strip()
            if raw:
                return raw
        return str(self.params.get("symbol") or "UNKNOWN")

    def _enrichment_ready(self, data: pd.DataFrame) -> bool:
        for column in self.required_enrichment:
            if not math.isfinite(_last_float(data, column)):
                return False
        return True

    def _cooldown_active(self, symbol: str, now_ts: pd.Timestamp, cooldown_bars: int, bar_seconds: int) -> bool:
        last = self._last_entry_bar_ts.get(symbol)
        if last is None:
            return False
        elapsed = (now_ts - last).total_seconds()
        return elapsed < cooldown_bars * bar_seconds

    def _mark_entry(self, symbol: str, now_ts: pd.Timestamp) -> None:
        self._last_entry_bar_ts[symbol] = now_ts

    def _bar_seconds(self, data: pd.DataFrame) -> int:
        try:
            tail = data.index[-6:]
            deltas = pd.Series(tail).diff().dropna()
            seconds = float(deltas.median().total_seconds())
            if math.isfinite(seconds) and seconds > 0:
                return int(seconds)
        except Exception:
            pass
        return 3600

    def _structure_ok(self, data: pd.DataFrame) -> bool:
        mcap = _last_float(data, "mcap_usd")
        oi = _last_float(data, "oi_usd")
        if not (math.isfinite(mcap) and mcap > 0 and math.isfinite(oi) and oi > 0):
            return False
        mcap_min = float(self.params.get("mcap_min_usd", 30e6))
        mcap_max = float(self.params.get("mcap_max_usd", 800e6))
        oi_mcap_min = float(self.params.get("oi_mcap_min", 0.15))
        if not (mcap_min <= mcap <= mcap_max):
            return False
        return (oi / mcap) >= oi_mcap_min

    def _base_entry_metadata(self, data: pd.DataFrame) -> Dict[str, Any]:
        mcap = _last_float(data, "mcap_usd")
        oi = _last_float(data, "oi_usd")
        return {
            "oi_usd": oi,
            "mcap_usd": mcap,
            "oi_mcap_ratio": (oi / mcap) if (math.isfinite(mcap) and mcap > 0 and math.isfinite(oi)) else None,
            "funding_rate": _last_float(data, "funding_rate"),
            "use_atr_stops": False,
            "generic_check_exit_enabled": False,
        }

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "structural_derivatives",
            "columns": ["open", "high", "low", "close", "volume", *self.required_enrichment],
            "min_length": int(self.params.get("min_bars_required", 200)),
        }


class AccumulationAmbushStrategy(_AmbushBase):
    """模式A：吸筹指纹埋伏（OI 爬升 + 价格横盘 + 费率不拥挤）。"""

    @classmethod
    def default_params(cls) -> Dict[str, Any]:
        return {
            "mcap_min_usd": 30e6,
            "mcap_max_usd": 800e6,
            "oi_mcap_min": 0.15,
            "oi_rise_bars": 168,          # 7d @1h
            "oi_rise_min": 0.25,          # OI 7日涨幅 ≥ +25%
            "price_flat_max": 0.10,       # 7日 |涨跌| ≤ 10%
            "funding_avg_bars": 168,
            "funding_avg_max": 0.00005,   # 7日平均费率 ≤ 0.005%/8h
            "min_history_bars": 720,      # 上市 ≥30 天
            "stop_loss_pct": 0.18,
            "trailing_stop_pct": 0.25,
            "time_stop_days": 21,
            "oi_collapse_exit": 0.20,     # OI 3日回落 ≥20% 结构失效
            "funding_extreme_exit": 0.0010,
            "cooldown_bars": 336,         # 14d
            "min_bars_required": 400,
        }

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or len(data) < int(self.params["min_bars_required"]):
            return []
        if not self._enrichment_ready(data) or not self._structure_ok(data):
            return []

        symbol = self._symbol_of(data)
        now_ts = pd.Timestamp(data.index[-1])
        bar_seconds = self._bar_seconds(data)
        if self._cooldown_active(symbol, now_ts, int(self.params["cooldown_bars"]), bar_seconds):
            return []

        rise_bars = int(self.params["oi_rise_bars"])
        oi_rise = _pct_change_over(_tail_values(data, "oi_usd", rise_bars + 8), rise_bars)
        price_move = _pct_change_over(_tail_values(data, "close", rise_bars + 8), rise_bars)
        funding_values = _tail_values(data, "funding_rate", int(self.params["funding_avg_bars"]))
        funding_clean = funding_values[np.isfinite(funding_values)] if funding_values is not None else np.array([])
        funding_avg = float(funding_clean.mean()) if len(funding_clean) else float("nan")

        fingerprint = (
            math.isfinite(oi_rise)
            and math.isfinite(price_move)
            and oi_rise >= float(self.params["oi_rise_min"])
            and abs(price_move) <= float(self.params["price_flat_max"])
            and math.isfinite(funding_avg)
            and funding_avg <= float(self.params["funding_avg_max"])
        )
        if not fingerprint:
            return []

        # 边沿触发：上一根 bar 不满足指纹时才进场，避免整段吸筹期反复触发。
        prev = data.iloc[:-1]
        if len(prev) > int(self.params["min_bars_required"]):
            prev_rise = _pct_change_over(_tail_values(prev, "oi_usd", rise_bars + 8), rise_bars)
            prev_move = _pct_change_over(_tail_values(prev, "close", rise_bars + 8), rise_bars)
            if (
                math.isfinite(prev_rise)
                and math.isfinite(prev_move)
                and prev_rise >= float(self.params["oi_rise_min"])
                and abs(prev_move) <= float(self.params["price_flat_max"])
            ):
                return []

        price = _last_float(data, "close")
        if not (math.isfinite(price) and price > 0):
            return []
        metadata = self._base_entry_metadata(data)
        metadata.update(
            {
                "mode": "A_accumulation_ambush",
                "oi_rise": oi_rise,
                "price_move": price_move,
                "funding_avg": funding_avg,
                "trailing_stop_pct": float(self.params["trailing_stop_pct"]),
                "time_stop_enabled": True,
                "time_stop_minutes": int(float(self.params["time_stop_days"]) * 1440),
                "time_stop_timeframe": "1h",
            }
        )
        self._mark_entry(symbol, now_ts)
        return [
            Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=price,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=min(1.0, 0.5 + oi_rise / 2.0),
                stop_loss=price * (1.0 - float(self.params["stop_loss_pct"])),
                metadata=metadata,
            )
        ]

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        side = str(getattr(position, "side", "") or "").lower()
        if side != "long" or data is None or len(data) < 80:
            return None
        funding = _last_float(data, "funding_rate")
        oi_3d = _pct_change_over(_tail_values(data, "oi_usd", 80), 72)
        collapse = math.isfinite(oi_3d) and oi_3d <= -float(self.params["oi_collapse_exit"])
        crowded = math.isfinite(funding) and funding >= float(self.params["funding_extreme_exit"])
        if not (collapse or crowded):
            return None
        price = _last_float(data, "close")
        if not (math.isfinite(price) and price > 0):
            return None
        return Signal(
            symbol=str(getattr(position, "symbol", self._symbol_of(data))),
            signal_type=SignalType.CLOSE_LONG,
            price=price,
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=0.9,
            metadata={
                "close_reason": "oi_collapse" if collapse else "funding_crowded",
                "close_only": True,
                "oi_change_3d": oi_3d,
                "funding_rate": funding,
            },
        )


class SqueezeFuelStrategy(_AmbushBase):
    """模式B：逼空燃料（深负费率 + OI 堆积 + 突破触发）。"""

    @classmethod
    def default_params(cls) -> Dict[str, Any]:
        return {
            "mcap_min_usd": 20e6,
            "mcap_max_usd": 800e6,
            "oi_mcap_min": 0.30,
            "funding_max": -0.0005,      # ≤ -0.05%/8h
            "oi_rise_bars": 72,          # 3d
            "oi_rise_min": 0.15,
            "breakout_bars": 24,         # 突破前 24h 最高价
            "stop_loss_pct": 0.12,
            "take_profit_pct": 0.40,
            "trailing_stop_pct": 0.15,
            "time_stop_days": 7,
            "funding_flip_exit": 0.0005,
            "cooldown_bars": 72,
            "min_bars_required": 200,
        }

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or len(data) < int(self.params["min_bars_required"]):
            return []
        if not self._enrichment_ready(data) or not self._structure_ok(data):
            return []

        symbol = self._symbol_of(data)
        now_ts = pd.Timestamp(data.index[-1])
        if self._cooldown_active(symbol, now_ts, int(self.params["cooldown_bars"]), self._bar_seconds(data)):
            return []

        funding = _last_float(data, "funding_rate")
        if not (math.isfinite(funding) and funding <= float(self.params["funding_max"])):
            return []
        rise_bars = int(self.params["oi_rise_bars"])
        oi_rise = _pct_change_over(_tail_values(data, "oi_usd", rise_bars + 8), rise_bars)
        if not (math.isfinite(oi_rise) and oi_rise >= float(self.params["oi_rise_min"])):
            return []

        breakout_bars = int(self.params["breakout_bars"])
        high = _tail_values(data, "high", breakout_bars + 2)
        close = _tail_values(data, "close", breakout_bars + 2)
        if high is None or close is None or len(high) <= breakout_bars + 1 or len(close) < 2:
            return []
        price = float(close[-1])
        prev_close = float(close[-2])
        prior_high = float(np.nanmax(high[-breakout_bars - 1 : -1]))
        if not all(math.isfinite(v) for v in (price, prev_close, prior_high)):
            return []
        if not (price > prior_high and prev_close <= prior_high):
            return []

        metadata = self._base_entry_metadata(data)
        metadata.update(
            {
                "mode": "B_squeeze_fuel",
                "oi_rise_3d": oi_rise,
                "breakout_level": prior_high,
                "trailing_stop_pct": float(self.params["trailing_stop_pct"]),
                "time_stop_enabled": True,
                "time_stop_minutes": int(float(self.params["time_stop_days"]) * 1440),
                "time_stop_timeframe": "1h",
            }
        )
        self._mark_entry(symbol, now_ts)
        return [
            Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=price,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=min(1.0, 0.5 + abs(funding) * 400),
                stop_loss=price * (1.0 - float(self.params["stop_loss_pct"])),
                take_profit=price * (1.0 + float(self.params["take_profit_pct"])),
                metadata=metadata,
            )
        ]

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        side = str(getattr(position, "side", "") or "").lower()
        if side != "long" or data is None or len(data) < 10:
            return None
        funding = _last_float(data, "funding_rate")
        if not (math.isfinite(funding) and funding >= float(self.params["funding_flip_exit"])):
            return None
        price = _last_float(data, "close")
        if not (math.isfinite(price) and price > 0):
            return None
        return Signal(
            symbol=str(getattr(position, "symbol", self._symbol_of(data))),
            signal_type=SignalType.CLOSE_LONG,
            price=price,
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=0.85,
            metadata={"close_reason": "funding_flip_positive", "close_only": True, "funding_rate": funding},
        )


class IgnitionFastFollowStrategy(_AmbushBase):
    """模式C：点火快跟（放量突破确认后介入，快进快出）。"""

    @classmethod
    def default_params(cls) -> Dict[str, Any]:
        return {
            "mcap_min_usd": 20e6,
            "mcap_max_usd": 1000e6,
            "oi_mcap_min": 0.15,
            "volume_z_min": 4.0,
            "volume_z_window": 720,      # 30d hourly
            "ret_1h_min": 0.06,
            "breakout_bars": 168,        # 7d 收盘新高
            "oi_jump_bars": 4,
            "oi_jump_min": 0.08,
            "max_runup_48h": 0.80,       # 已经翻倍途中不追
            "stop_loss_pct": 0.08,
            "take_profit_pct": 0.25,
            "trailing_stop_pct": 0.10,
            "time_stop_hours": 48,
            "cooldown_bars": 24,
            "min_bars_required": 240,
        }

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or len(data) < int(self.params["min_bars_required"]):
            return []
        if not self._enrichment_ready(data) or not self._structure_ok(data):
            return []

        symbol = self._symbol_of(data)
        now_ts = pd.Timestamp(data.index[-1])
        if self._cooldown_active(symbol, now_ts, int(self.params["cooldown_bars"]), self._bar_seconds(data)):
            return []

        close_pair = _tail_values(data, "close", 3)
        if close_pair is None or len(close_pair) < 2:
            return []
        price = float(close_pair[-1])
        prev_price = float(close_pair[-2])
        if not (math.isfinite(price) and math.isfinite(prev_price) and prev_price > 0):
            return []
        ret_1h = price / prev_price - 1.0
        if ret_1h < float(self.params["ret_1h_min"]):
            return []

        volume_values = _tail_values(data, "volume", int(self.params["volume_z_window"]))
        window = volume_values[np.isfinite(volume_values)] if volume_values is not None else np.array([])
        if len(window) < 60:
            return []
        mean = float(window.mean())
        std = float(window.std(ddof=1))
        vol_z = (float(window[-1]) - mean) / std if std > 0 else 0.0
        if vol_z < float(self.params["volume_z_min"]):
            return []

        breakout_bars = int(self.params["breakout_bars"])
        close = _tail_values(data, "close", breakout_bars + 2)
        if close is None or len(close) <= breakout_bars:
            return []
        if price <= float(np.nanmax(close[-breakout_bars - 1 : -1])):
            return []

        runup_48h = _pct_change_over(close, 48)
        if math.isfinite(runup_48h) and runup_48h >= float(self.params["max_runup_48h"]):
            return []

        jump_bars = int(self.params["oi_jump_bars"])
        oi_jump = _pct_change_over(_tail_values(data, "oi_usd", jump_bars + 6), jump_bars)
        if not (math.isfinite(oi_jump) and oi_jump >= float(self.params["oi_jump_min"])):
            return []

        metadata = self._base_entry_metadata(data)
        metadata.update(
            {
                "mode": "C_ignition_fast_follow",
                "volume_z": vol_z,
                "ret_1h": ret_1h,
                "oi_jump_4h": oi_jump,
                "trailing_stop_pct": float(self.params["trailing_stop_pct"]),
                "time_stop_enabled": True,
                "time_stop_minutes": int(float(self.params["time_stop_hours"]) * 60),
                "time_stop_timeframe": "1h",
            }
        )
        self._mark_entry(symbol, now_ts)
        return [
            Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=price,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=min(1.0, 0.4 + vol_z / 20.0 + oi_jump),
                stop_loss=price * (1.0 - float(self.params["stop_loss_pct"])),
                take_profit=price * (1.0 + float(self.params["take_profit_pct"])),
                metadata=metadata,
            )
        ]
