from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def _signed_state(value: Any) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if not np.isfinite(numeric) or numeric == 0:
        return 0.0
    return 1.0 if numeric > 0 else -1.0


def _safe_positive(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if not np.isfinite(numeric) or numeric <= 0:
        return None
    return float(numeric)


def _direction_label(direction: float) -> str:
    return "long" if float(direction) > 0 else "short"


def _directional_return(direction: float, start_price: float, end_price: float) -> float:
    if not np.isfinite(start_price) or start_price <= 0 or not np.isfinite(end_price) or end_price <= 0:
        return 0.0
    return float(direction) * ((float(end_price) / float(start_price)) - 1.0)


def _hit_level(direction: float, low_price: float, high_price: float, level: Optional[float]) -> bool:
    if level is None or not np.isfinite(level):
        return False
    if direction > 0:
        return np.isfinite(low_price) and float(low_price) <= float(level)
    return np.isfinite(high_price) and float(high_price) >= float(level)


def _hit_favorable_level(direction: float, low_price: float, high_price: float, level: Optional[float]) -> bool:
    if level is None or not np.isfinite(level):
        return False
    if direction > 0:
        return np.isfinite(high_price) and float(high_price) >= float(level)
    return np.isfinite(low_price) and float(low_price) <= float(level)


@dataclass
class ExitEngineConfig:
    initial_stop_mode: str = "none"
    initial_stop_atr_mult: float = 2.0
    atr_period: int = 14
    breakeven_enabled: bool = False
    breakeven_trigger_r: float = 1.0
    partial_take_profit_enabled: bool = False
    partial_take_profit_r: float = 1.5
    partial_take_profit_ratio: float = 0.5
    trailing_mode: str = "none"
    trailing_atr_mult: float = 2.5
    signal_reversal_exit: bool = True
    time_stop_enabled: bool = False
    max_bars_in_trade: int = 20
    allow_same_bar_exit: bool = False
    long_short_symmetry: bool = True
    fixed_stop_loss_pct: Optional[float] = None
    fixed_take_profit_pct: Optional[float] = None
    template_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["initial_stop_mode"] = str(self.initial_stop_mode or "none").lower()
        payload["trailing_mode"] = str(self.trailing_mode or "none").lower()
        payload["atr_period"] = max(1, int(self.atr_period or 14))
        payload["max_bars_in_trade"] = max(1, int(self.max_bars_in_trade or 20))
        payload["initial_stop_atr_mult"] = float(self.initial_stop_atr_mult or 0.0)
        payload["breakeven_trigger_r"] = float(self.breakeven_trigger_r or 0.0)
        payload["partial_take_profit_r"] = float(self.partial_take_profit_r or 0.0)
        payload["partial_take_profit_ratio"] = min(
            1.0,
            max(0.0, float(self.partial_take_profit_ratio or 0.0)),
        )
        payload["trailing_atr_mult"] = float(self.trailing_atr_mult or 0.0)
        payload["fixed_stop_loss_pct"] = _safe_positive(self.fixed_stop_loss_pct)
        payload["fixed_take_profit_pct"] = _safe_positive(self.fixed_take_profit_pct)
        return payload


EXIT_TEMPLATE_PRESETS: Dict[str, Dict[str, Any]] = {
    "ReversalOnly": {
        "initial_stop_mode": "none",
        "breakeven_enabled": False,
        "partial_take_profit_enabled": False,
        "trailing_mode": "none",
        "signal_reversal_exit": True,
        "time_stop_enabled": False,
    },
    "ATRTrail": {
        "initial_stop_mode": "atr",
        "initial_stop_atr_mult": 2.0,
        "breakeven_enabled": True,
        "breakeven_trigger_r": 1.0,
        "partial_take_profit_enabled": False,
        "trailing_mode": "atr",
        "trailing_atr_mult": 2.5,
        "signal_reversal_exit": True,
        "time_stop_enabled": False,
    },
    "PartialPlusATR": {
        "initial_stop_mode": "atr",
        "initial_stop_atr_mult": 2.0,
        "breakeven_enabled": True,
        "breakeven_trigger_r": 1.0,
        "partial_take_profit_enabled": True,
        "partial_take_profit_r": 1.5,
        "partial_take_profit_ratio": 0.5,
        "trailing_mode": "atr",
        "trailing_atr_mult": 2.5,
        "signal_reversal_exit": True,
        "time_stop_enabled": True,
        "max_bars_in_trade": 20,
    },
    "SignalPlusTimeStop": {
        "initial_stop_mode": "atr",
        "initial_stop_atr_mult": 2.0,
        "breakeven_enabled": False,
        "partial_take_profit_enabled": False,
        "trailing_mode": "none",
        "signal_reversal_exit": True,
        "time_stop_enabled": True,
        "max_bars_in_trade": 15,
    },
}


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    bars = df.copy()
    close = pd.to_numeric(bars.get("close"), errors="coerce")
    high = pd.to_numeric(bars.get("high", close), errors="coerce")
    low = pd.to_numeric(bars.get("low", close), errors="coerce")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    span = max(1, int(period or 14))
    atr = true_range.ewm(alpha=1.0 / span, adjust=False, min_periods=span).mean()
    return pd.to_numeric(atr, errors="coerce").ffill()


def resolve_exit_engine_config(
    *,
    template_name: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
    fixed_stop_loss_pct: Optional[float] = None,
    fixed_take_profit_pct: Optional[float] = None,
    allow_same_bar_exit: bool = False,
) -> ExitEngineConfig:
    base: Dict[str, Any] = {"allow_same_bar_exit": bool(allow_same_bar_exit)}
    if template_name:
        base.update(EXIT_TEMPLATE_PRESETS.get(str(template_name), {}))
        base["template_name"] = str(template_name)
    if overrides:
        base.update(dict(overrides))
    if fixed_stop_loss_pct is not None:
        base["fixed_stop_loss_pct"] = fixed_stop_loss_pct
    if fixed_take_profit_pct is not None:
        base["fixed_take_profit_pct"] = fixed_take_profit_pct
    cfg = ExitEngineConfig(**base)
    return ExitEngineConfig(**cfg.to_dict())


@dataclass
class _ActiveTrade:
    trade_id: int
    direction: float
    entry_idx: int
    entry_timestamp: pd.Timestamp
    entry_price: float
    entry_atr: Optional[float]
    remaining_size: float = 1.0
    best_price: Optional[float] = None
    initial_stop_price: Optional[float] = None
    fixed_stop_price: Optional[float] = None
    fixed_take_profit_price: Optional[float] = None
    risk_per_unit: Optional[float] = None
    breakeven_armed: bool = False
    partial_done: bool = False
    partial_exit_count: int = 0
    realized_gross_return: float = 0.0
    mfe: float = 0.0
    mae: float = 0.0
    exit_events: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def side(self) -> str:
        return _direction_label(self.direction)

    def bars_in_trade(self, idx: int) -> int:
        return int(max(0, idx - self.entry_idx + 1))

    def update_excursions(self, *, high_price: float, low_price: float) -> None:
        if self.entry_price <= 0:
            return
        if self.direction > 0:
            if np.isfinite(high_price):
                self.mfe = max(self.mfe, (float(high_price) / self.entry_price) - 1.0)
            if np.isfinite(low_price):
                self.mae = min(self.mae, (float(low_price) / self.entry_price) - 1.0)
        else:
            if np.isfinite(low_price):
                self.mfe = max(self.mfe, 1.0 - (float(low_price) / self.entry_price))
            if np.isfinite(high_price):
                self.mae = min(self.mae, 1.0 - (float(high_price) / self.entry_price))


@dataclass
class ExitEngineResult:
    effective_position: pd.Series
    gross_returns: pd.Series
    turnover: pd.Series
    trade_points: Dict[str, Any]
    trade_stats: Dict[str, Any]
    protective_stats: Dict[str, int]
    exit_reason_breakdown: Dict[str, int]
    exit_events: List[Dict[str, Any]]
    completed_trades: List[Dict[str, Any]]
    config: Dict[str, Any]


class ExitEngine:
    def __init__(self, config: ExitEngineConfig):
        self.config = ExitEngineConfig(**config.to_dict())

    def run(self, df: pd.DataFrame, signal_position: pd.Series) -> ExitEngineResult:
        bars = df.copy()
        bars.index = pd.to_datetime(bars.index)
        bars = bars.sort_index()
        index = bars.index

        close = pd.to_numeric(bars.get("close"), errors="coerce").reindex(index)
        high = pd.to_numeric(bars.get("high", close), errors="coerce").reindex(index).fillna(close)
        low = pd.to_numeric(bars.get("low", close), errors="coerce").reindex(index).fillna(close)

        raw_position = (
            pd.to_numeric(signal_position.reindex(index), errors="coerce")
            .fillna(0.0)
            .map(_signed_state)
            .astype(float)
        )
        raw_prev = raw_position.shift(1).fillna(0.0)
        long_entry_signal = (raw_position > 0) & (raw_prev <= 0)
        short_entry_signal = (raw_position < 0) & (raw_prev >= 0)

        atr_series = compute_atr(bars, period=int(self.config.atr_period or 14))
        atr_for_bar = atr_series.shift(1).ffill()

        effective_position = pd.Series(0.0, index=index, dtype=float)
        gross_returns = pd.Series(0.0, index=index, dtype=float)

        buy_points: List[Dict[str, Any]] = []
        sell_points: List[Dict[str, Any]] = []
        open_points: List[Dict[str, Any]] = []
        close_points: List[Dict[str, Any]] = []
        exit_events: List[Dict[str, Any]] = []
        completed_trades: List[Dict[str, Any]] = []
        exit_reason_counter: Counter[str] = Counter()

        active: Optional[_ActiveTrade] = None
        next_trade_id = 1
        entry_count = 0
        exit_count = 0
        completed_count = 0
        wins = 0

        for idx, ts in enumerate(index):
            px_close = float(close.iloc[idx]) if pd.notna(close.iloc[idx]) else float("nan")
            px_high = float(high.iloc[idx]) if pd.notna(high.iloc[idx]) else px_close
            px_low = float(low.iloc[idx]) if pd.notna(low.iloc[idx]) else px_close
            px_prev_close = (
                float(close.iloc[idx - 1])
                if idx > 0 and pd.notna(close.iloc[idx - 1])
                else px_close
            )

            if not np.isfinite(px_close) or px_close <= 0:
                effective_position.iloc[idx] = (
                    float(active.direction * active.remaining_size) if active is not None else 0.0
                )
                continue

            start_size = float(active.remaining_size) if active is not None else 0.0
            start_direction = float(active.direction) if active is not None else 0.0
            bar_return = 0.0

            if active is not None and start_size > 0:
                active.update_excursions(high_price=px_high, low_price=px_low)
                entry_bar = idx == active.entry_idx
                current_atr = _safe_positive(atr_for_bar.iloc[idx]) if idx < len(atr_for_bar) else None

                stop_price, stop_reason = self._protective_stop(active, current_atr=current_atr)
                fully_closed = False

                if not entry_bar or self.config.allow_same_bar_exit:
                    if _hit_level(start_direction, px_low, px_high, stop_price):
                        bar_return += start_size * _directional_return(start_direction, px_prev_close, float(stop_price))
                        self._record_exit_event(
                            active=active,
                            exit_events=exit_events,
                            close_points=close_points,
                            buy_points=buy_points,
                            sell_points=sell_points,
                            timestamp=ts,
                            exit_price=float(stop_price),
                            reason=stop_reason or "stop",
                            exit_size=start_size,
                            bars_in_trade=active.bars_in_trade(idx),
                        )
                        exit_count += 1
                        exit_reason_counter[str(stop_reason or "stop")] += 1
                        completed_count += 1
                        if active.realized_gross_return > 0:
                            wins += 1
                        completed_trades.append(
                            self._finalize_trade(
                                active=active,
                                bars_in_trade=active.bars_in_trade(idx),
                                exit_timestamp=ts,
                                exit_price=float(stop_price),
                                reason=str(stop_reason or "stop"),
                            )
                        )
                        active = None
                        fully_closed = True

                if active is not None and not fully_closed:
                    if (
                        self.config.fixed_take_profit_pct is not None
                        and active.fixed_take_profit_price is not None
                        and _hit_favorable_level(start_direction, px_low, px_high, active.fixed_take_profit_price)
                    ):
                        bar_return += start_size * _directional_return(start_direction, px_prev_close, active.fixed_take_profit_price)
                        self._record_exit_event(
                            active=active,
                            exit_events=exit_events,
                            close_points=close_points,
                            buy_points=buy_points,
                            sell_points=sell_points,
                            timestamp=ts,
                            exit_price=active.fixed_take_profit_price,
                            reason="take_profit",
                            exit_size=start_size,
                            bars_in_trade=active.bars_in_trade(idx),
                        )
                        exit_count += 1
                        exit_reason_counter["take_profit"] += 1
                        completed_count += 1
                        if active.realized_gross_return > 0:
                            wins += 1
                        completed_trades.append(
                            self._finalize_trade(
                                active=active,
                                bars_in_trade=active.bars_in_trade(idx),
                                exit_timestamp=ts,
                                exit_price=active.fixed_take_profit_price,
                                reason="take_profit",
                            )
                        )
                        active = None
                        fully_closed = True

                if active is not None and not fully_closed:
                    partial_price = self._partial_take_profit_price(active=active)
                    if (
                        partial_price is not None
                        and self.config.partial_take_profit_enabled
                        and not active.partial_done
                        and _hit_favorable_level(start_direction, px_low, px_high, partial_price)
                    ):
                        partial_size = float(
                            min(
                                active.remaining_size,
                                max(
                                    0.0,
                                    active.remaining_size * float(self.config.partial_take_profit_ratio or 0.0),
                                ),
                            )
                        )
                        if partial_size > 0:
                            bar_return += partial_size * _directional_return(start_direction, px_prev_close, partial_price)
                            self._record_exit_event(
                                active=active,
                                exit_events=exit_events,
                                close_points=close_points,
                                buy_points=buy_points,
                                sell_points=sell_points,
                                timestamp=ts,
                                exit_price=partial_price,
                                reason="partial",
                                exit_size=partial_size,
                                bars_in_trade=active.bars_in_trade(idx),
                            )
                            exit_count += 1
                            exit_reason_counter["partial"] += 1
                            active.remaining_size = float(max(0.0, active.remaining_size - partial_size))
                            active.partial_done = True
                            active.partial_exit_count += 1

                if active is not None and active.remaining_size > 0:
                    close_reason = None
                    if self.config.time_stop_enabled and active.bars_in_trade(idx) >= int(self.config.max_bars_in_trade):
                        close_reason = "time_stop"
                    elif self._supports_signal_reversal() and self._raw_support_lost(raw_position.iloc[idx], active.direction):
                        close_reason = "reversal"

                    remaining_size = float(active.remaining_size)
                    if close_reason:
                        bar_return += remaining_size * _directional_return(start_direction, px_prev_close, px_close)
                        self._record_exit_event(
                            active=active,
                            exit_events=exit_events,
                            close_points=close_points,
                            buy_points=buy_points,
                            sell_points=sell_points,
                            timestamp=ts,
                            exit_price=px_close,
                            reason=close_reason,
                            exit_size=remaining_size,
                            bars_in_trade=active.bars_in_trade(idx),
                        )
                        exit_count += 1
                        exit_reason_counter[str(close_reason)] += 1
                        completed_count += 1
                        if active.realized_gross_return > 0:
                            wins += 1
                        completed_trades.append(
                            self._finalize_trade(
                                active=active,
                                bars_in_trade=active.bars_in_trade(idx),
                                exit_timestamp=ts,
                                exit_price=px_close,
                                reason=str(close_reason),
                            )
                        )
                        active = None
                    else:
                        bar_return += remaining_size * _directional_return(start_direction, px_prev_close, px_close)
                        self._update_post_bar_state(active=active, high_price=px_high, low_price=px_low)

            gross_returns.iloc[idx] = float(bar_return)

            if active is None:
                entry_direction = 0.0
                if bool(long_entry_signal.iloc[idx]) and not bool(short_entry_signal.iloc[idx]):
                    entry_direction = 1.0
                elif bool(short_entry_signal.iloc[idx]) and not bool(long_entry_signal.iloc[idx]):
                    entry_direction = -1.0

                if entry_direction != 0.0:
                    active = self._open_trade(
                        trade_id=next_trade_id,
                        direction=entry_direction,
                        idx=idx,
                        timestamp=ts,
                        entry_price=px_close,
                        entry_atr=_safe_positive(atr_series.iloc[idx]) if idx < len(atr_series) else None,
                    )
                    next_trade_id += 1
                    entry_count += 1
                    open_point = {
                        "trade_id": int(active.trade_id),
                        "timestamp": pd.Timestamp(ts).isoformat(),
                        "price": float(px_close),
                        "direction": active.side,
                    }
                    open_points.append(open_point)
                    if active.direction > 0:
                        buy_points.append({"timestamp": open_point["timestamp"], "price": open_point["price"]})
                    else:
                        sell_points.append({"timestamp": open_point["timestamp"], "price": open_point["price"]})

            effective_position.iloc[idx] = float(active.direction * active.remaining_size) if active is not None else 0.0

        turnover = effective_position.diff().abs().fillna(0.0)
        if len(turnover) > 0:
            turnover.iloc[0] = abs(float(effective_position.iloc[0] or 0.0))

        win_rate = (wins / completed_count * 100.0) if completed_count else 0.0
        protective_stats = {
            "forced_stop_exits": int(exit_reason_counter.get("stop", 0)),
            "forced_take_exits": int(exit_reason_counter.get("take_profit", 0)),
        }
        breakdown = {
            "stop": int(exit_reason_counter.get("stop", 0)),
            "take_profit": int(exit_reason_counter.get("take_profit", 0)),
            "trailing": int(exit_reason_counter.get("trailing", 0)),
            "reversal": int(exit_reason_counter.get("reversal", 0)),
            "partial": int(exit_reason_counter.get("partial", 0)),
            "time_stop": int(exit_reason_counter.get("time_stop", 0)),
        }

        return ExitEngineResult(
            effective_position=effective_position,
            gross_returns=gross_returns,
            turnover=turnover,
            trade_points={
                "buy_points": buy_points,
                "sell_points": sell_points,
                "open_points": open_points,
                "close_points": close_points,
                "entries": int(entry_count),
                "exits": int(exit_count),
            },
            trade_stats={
                "entries": int(entry_count),
                "exits": int(exit_count),
                "completed": int(completed_count),
                "win_rate": round(float(win_rate), 2),
            },
            protective_stats=protective_stats,
            exit_reason_breakdown=breakdown,
            exit_events=exit_events,
            completed_trades=completed_trades,
            config=self.config.to_dict(),
        )

    def _open_trade(
        self,
        *,
        trade_id: int,
        direction: float,
        idx: int,
        timestamp: Any,
        entry_price: float,
        entry_atr: Optional[float],
    ) -> _ActiveTrade:
        fixed_stop_price = None
        fixed_take_profit_price = None
        initial_stop_price = None

        if self.config.fixed_stop_loss_pct is not None:
            fixed_stop_price = float(
                entry_price * (1.0 - self.config.fixed_stop_loss_pct)
                if direction > 0
                else entry_price * (1.0 + self.config.fixed_stop_loss_pct)
            )

        if self.config.fixed_take_profit_pct is not None:
            fixed_take_profit_price = float(
                entry_price * (1.0 + self.config.fixed_take_profit_pct)
                if direction > 0
                else entry_price * (1.0 - self.config.fixed_take_profit_pct)
            )

        if str(self.config.initial_stop_mode or "none").lower() == "atr" and entry_atr is not None:
            initial_stop_price = float(
                entry_price - self.config.initial_stop_atr_mult * entry_atr
                if direction > 0
                else entry_price + self.config.initial_stop_atr_mult * entry_atr
            )

        stop_candidates = [
            price
            for price in [fixed_stop_price, initial_stop_price]
            if price is not None and np.isfinite(price)
        ]
        effective_stop = None
        if stop_candidates:
            effective_stop = max(stop_candidates) if direction > 0 else min(stop_candidates)

        risk_per_unit = None
        if effective_stop is not None and np.isfinite(effective_stop):
            risk_candidate = abs(float(entry_price) - float(effective_stop))
            if risk_candidate > 0:
                risk_per_unit = risk_candidate

        return _ActiveTrade(
            trade_id=int(trade_id),
            direction=float(direction),
            entry_idx=int(idx),
            entry_timestamp=pd.Timestamp(timestamp),
            entry_price=float(entry_price),
            entry_atr=entry_atr,
            remaining_size=1.0,
            best_price=float(entry_price),
            initial_stop_price=initial_stop_price,
            fixed_stop_price=fixed_stop_price,
            fixed_take_profit_price=fixed_take_profit_price,
            risk_per_unit=risk_per_unit,
            mfe=0.0,
            mae=0.0,
        )

    def _supports_signal_reversal(self) -> bool:
        return bool(self.config.signal_reversal_exit) or str(self.config.trailing_mode or "none").lower() == "signal_reversal"

    @staticmethod
    def _raw_support_lost(raw_state: Any, active_direction: float) -> bool:
        current = _signed_state(raw_state)
        if active_direction > 0:
            return current <= 0
        return current >= 0

    def _protective_stop(self, active: _ActiveTrade, *, current_atr: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
        candidates: List[Tuple[float, str]] = []

        if active.initial_stop_price is not None and np.isfinite(active.initial_stop_price):
            candidates.append((float(active.initial_stop_price), "stop"))
        if active.fixed_stop_price is not None and np.isfinite(active.fixed_stop_price):
            candidates.append((float(active.fixed_stop_price), "stop"))

        if active.breakeven_armed and np.isfinite(active.entry_price):
            candidates.append((float(active.entry_price), "stop"))

        if str(self.config.trailing_mode or "none").lower() == "atr" and current_atr is not None and active.best_price is not None:
            trailing_stop = (
                float(active.best_price) - float(self.config.trailing_atr_mult) * float(current_atr)
                if active.direction > 0
                else float(active.best_price) + float(self.config.trailing_atr_mult) * float(current_atr)
            )
            if np.isfinite(trailing_stop):
                candidates.append((trailing_stop, "trailing"))

        if not candidates:
            return None, None

        if active.direction > 0:
            price, reason = max(candidates, key=lambda item: item[0])
        else:
            price, reason = min(candidates, key=lambda item: item[0])
        return float(price), str(reason)

    def _partial_take_profit_price(self, *, active: _ActiveTrade) -> Optional[float]:
        if not bool(self.config.partial_take_profit_enabled) or active.partial_done:
            return None
        if active.risk_per_unit is None or active.risk_per_unit <= 0:
            return None
        return float(
            active.entry_price + active.direction * float(self.config.partial_take_profit_r) * float(active.risk_per_unit)
        )

    def _breakeven_trigger_price(self, *, active: _ActiveTrade) -> Optional[float]:
        if not bool(self.config.breakeven_enabled):
            return None
        if active.risk_per_unit is None or active.risk_per_unit <= 0:
            return None
        return float(
            active.entry_price + active.direction * float(self.config.breakeven_trigger_r) * float(active.risk_per_unit)
        )

    def _update_post_bar_state(self, *, active: _ActiveTrade, high_price: float, low_price: float) -> None:
        if active.direction > 0:
            if np.isfinite(high_price):
                active.best_price = float(max(float(active.best_price or active.entry_price), float(high_price)))
        else:
            if np.isfinite(low_price):
                base = float(active.best_price if active.best_price is not None else active.entry_price)
                active.best_price = float(min(base, float(low_price)))

        trigger_price = self._breakeven_trigger_price(active=active)
        if trigger_price is None:
            return
        if active.direction > 0 and np.isfinite(high_price) and float(high_price) >= float(trigger_price):
            active.breakeven_armed = True
        if active.direction < 0 and np.isfinite(low_price) and float(low_price) <= float(trigger_price):
            active.breakeven_armed = True

    def _record_exit_event(
        self,
        *,
        active: _ActiveTrade,
        exit_events: List[Dict[str, Any]],
        close_points: List[Dict[str, Any]],
        buy_points: List[Dict[str, Any]],
        sell_points: List[Dict[str, Any]],
        timestamp: Any,
        exit_price: float,
        reason: str,
        exit_size: float,
        bars_in_trade: int,
    ) -> None:
        size = float(max(0.0, min(active.remaining_size, exit_size)))
        if size <= 0:
            return
        trade_return = _directional_return(active.direction, active.entry_price, exit_price)
        weighted_return = size * trade_return
        remaining_after = max(0.0, float(active.remaining_size) - size)
        risk_return = None
        if active.risk_per_unit is not None and active.entry_price > 0 and active.risk_per_unit > 0:
            risk_return = trade_return / (active.risk_per_unit / active.entry_price)

        event = {
            "trade_id": int(active.trade_id),
            "entry_timestamp": pd.Timestamp(active.entry_timestamp).isoformat(),
            "exit_timestamp": pd.Timestamp(timestamp).isoformat(),
            "direction": active.side,
            "entry_price": float(active.entry_price),
            "exit_price": float(exit_price),
            "size_fraction": round(float(size), 6),
            "gross_return_pct": round(float(weighted_return * 100.0), 6),
            "gross_return_r": round(float(risk_return), 6) if risk_return is not None else None,
            "raw_trade_return_pct": round(float(trade_return * 100.0), 6),
            "bars_in_trade": int(bars_in_trade),
            "reason": str(reason),
            "mfe_pct": round(float(active.mfe * 100.0), 6),
            "mae_pct": round(float(active.mae * 100.0), 6),
            "remaining_fraction": round(float(remaining_after), 6),
            "is_partial": bool(remaining_after > 0),
        }
        exit_events.append(event)
        active.exit_events.append(event)
        active.realized_gross_return += weighted_return

        point = {
            "trade_id": int(active.trade_id),
            "timestamp": pd.Timestamp(timestamp).isoformat(),
            "price": float(exit_price),
            "direction": active.side,
            "reason": "reverse" if str(reason) == "reversal" else str(reason),
            "size_fraction": round(float(size), 6),
        }
        close_points.append(point)
        if active.direction > 0:
            sell_points.append({"timestamp": point["timestamp"], "price": point["price"]})
        else:
            buy_points.append({"timestamp": point["timestamp"], "price": point["price"]})

    def _finalize_trade(
        self,
        *,
        active: _ActiveTrade,
        bars_in_trade: int,
        exit_timestamp: Any,
        exit_price: float,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "trade_id": int(active.trade_id),
            "entry_timestamp": pd.Timestamp(active.entry_timestamp).isoformat(),
            "exit_timestamp": pd.Timestamp(exit_timestamp).isoformat(),
            "direction": active.side,
            "entry_price": float(active.entry_price),
            "exit_price": float(exit_price),
            "gross_return_pct": round(float(active.realized_gross_return * 100.0), 6),
            "bars_in_trade": int(max(1, bars_in_trade)),
            "mfe_pct": round(float(active.mfe * 100.0), 6),
            "mae_pct": round(float(active.mae * 100.0), 6),
            "partial_exit_count": int(active.partial_exit_count),
            "final_exit_reason": str(reason),
            "exit_reasons": [str(item.get("reason") or "") for item in active.exit_events],
        }


def run_exit_engine(
    df: pd.DataFrame,
    signal_position: pd.Series,
    config: ExitEngineConfig,
) -> ExitEngineResult:
    return ExitEngine(config).run(df=df, signal_position=signal_position)
