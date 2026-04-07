"""Risk management module."""
from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from config.settings import settings
from core.strategies import Signal


def _position_manager():
    # Lazy import to avoid circular import during module initialization.
    from core.trading.position_manager import position_manager
    return position_manager


class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskMetrics:
    total_exposure: float = 0.0
    daily_pnl_usd: float = 0.0
    daily_pnl_ratio: float = 0.0
    daily_trades: int = 0
    open_positions: int = 0
    max_drawdown: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    trading_halted: bool = False


_AUTONOMY_PERSISTABLE_KEYS = frozenset(
    {
        "autonomy_daily_stop_buffer_ratio",
        "autonomy_max_drawdown_reduce_only",
        "autonomy_rolling_3d_drawdown_reduce_only",
        "autonomy_rolling_7d_drawdown_reduce_only",
    }
)


class RiskManager:
    """Centralized risk checks for signal/manual execution."""

    def __init__(self):
        # Limits
        self.max_position_size = float(settings.MAX_POSITION_SIZE or 0.1)  # ratio of equity
        self.max_daily_loss_ratio = float(settings.MAX_DAILY_LOSS or 0.02)
        self.max_daily_loss_usd = 0.0
        self.max_daily_trades = 200
        self.max_open_positions = max(1, int(getattr(settings, "MAX_OPEN_POSITIONS", 100) or 100))
        self.max_leverage = 3.0
        self.balance_volatility_alert_pct = 0.12
        autonomy_thresholds = self._default_autonomy_threshold_values()
        self.autonomy_daily_stop_buffer_ratio = float(
            autonomy_thresholds["autonomy_daily_stop_buffer_ratio"]
        )
        self.autonomy_max_drawdown_reduce_only = float(
            autonomy_thresholds["autonomy_max_drawdown_reduce_only"]
        )
        self.autonomy_rolling_3d_drawdown_reduce_only = float(
            autonomy_thresholds["autonomy_rolling_3d_drawdown_reduce_only"]
        )
        self.autonomy_rolling_7d_drawdown_reduce_only = float(
            autonomy_thresholds["autonomy_rolling_7d_drawdown_reduce_only"]
        )
        self._autonomy_overlay_path = Path(
            os.environ.get(
                "AI_AGENT_RISK_CONFIG_PATH",
                str(Path(getattr(settings, "CACHE_PATH", "cache")) / "ai" / "autonomous_agent_risk_config.json"),
            )
        )
        self._load_autonomy_threshold_overlay()

        # Runtime state
        self._daily_trades = 0
        self._daily_realized_pnl = 0.0
        self._daily_start = self._day_start(datetime.now(timezone.utc))
        self._day_start_equity: Optional[float] = None
        self._current_equity: Optional[float] = None
        self._last_equity: Optional[float] = None
        self._current_unrealized_pnl: float = 0.0
        self._equity_curve: List[float] = []
        self._equity_timeline: List[Dict[str, Any]] = []
        self._trade_history: List[Dict[str, Any]] = []
        self._alerts: List[Dict[str, Any]] = []
        self._trading_halted = False
        self._halt_reason = ""
        self._daily_stop_guard_until: Optional[datetime] = None
        self._daily_stop_breach_count = 0
        self._daily_stop_required_breaches_paper = 2
        self._daily_stop_required_breaches_live = 4
        self._risk_scope = "paper"
        self._scope_states: Dict[str, Dict[str, Any]] = {}
        self._scope_states[self._risk_scope] = self._snapshot_runtime_state()

    @staticmethod
    def _day_start(ts: datetime) -> datetime:
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)

    def _default_autonomy_threshold_values(self) -> Dict[str, float]:
        daily_loss_ratio = abs(float(self.max_daily_loss_ratio or 0.0))
        return {
            "autonomy_daily_stop_buffer_ratio": daily_loss_ratio * 0.7 if daily_loss_ratio > 0 else 0.0,
            "autonomy_max_drawdown_reduce_only": max(daily_loss_ratio * 2.5, 0.05),
            "autonomy_rolling_3d_drawdown_reduce_only": max(daily_loss_ratio * 3.0, 0.06),
            "autonomy_rolling_7d_drawdown_reduce_only": max(daily_loss_ratio * 4.5, 0.09),
        }

    def _load_autonomy_threshold_overlay(self) -> None:
        """Load persisted autonomy thresholds without restoring runtime risk state."""
        try:
            if not self._autonomy_overlay_path.exists():
                return
            raw = self._autonomy_overlay_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return
            safe = {key: payload[key] for key in _AUTONOMY_PERSISTABLE_KEYS if key in payload}
            for key, value in safe.items():
                setattr(self, key, float(value))
            self._normalize_autonomy_thresholds()
            logger.info(f"risk_manager: loaded {len(safe)} persisted autonomy thresholds")
        except Exception as exc:
            logger.warning(f"risk_manager: failed to load autonomy threshold overlay: {exc}")

    def _save_autonomy_threshold_overlay(self) -> None:
        """Atomically persist autonomy thresholds so risk governance survives restart."""
        try:
            self._autonomy_overlay_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                key: value
                for key, value in self.get_autonomy_risk_config().items()
                if key in _AUTONOMY_PERSISTABLE_KEYS
            }
            tmp = self._autonomy_overlay_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._autonomy_overlay_path)
        except Exception as exc:
            logger.warning(f"risk_manager: failed to save autonomy threshold overlay: {exc}")

    def _normalize_autonomy_thresholds(self) -> None:
        self.autonomy_daily_stop_buffer_ratio = max(
            0.0,
            min(float(self.autonomy_daily_stop_buffer_ratio or 0.0), 1.0),
        )
        daily_loss_ratio = abs(float(self.max_daily_loss_ratio or 0.0))
        if daily_loss_ratio > 0:
            self.autonomy_daily_stop_buffer_ratio = min(self.autonomy_daily_stop_buffer_ratio, daily_loss_ratio)
        self.autonomy_max_drawdown_reduce_only = max(
            0.0,
            min(float(self.autonomy_max_drawdown_reduce_only or 0.0), 1.0),
        )
        self.autonomy_rolling_3d_drawdown_reduce_only = max(
            0.0,
            min(float(self.autonomy_rolling_3d_drawdown_reduce_only or 0.0), 1.0),
        )
        self.autonomy_rolling_7d_drawdown_reduce_only = max(
            0.0,
            min(float(self.autonomy_rolling_7d_drawdown_reduce_only or 0.0), 1.0),
        )

    def get_autonomy_risk_config(self) -> Dict[str, float]:
        return {
            "autonomy_daily_stop_buffer_ratio": round(float(self.autonomy_daily_stop_buffer_ratio or 0.0), 6),
            "autonomy_max_drawdown_reduce_only": round(float(self.autonomy_max_drawdown_reduce_only or 0.0), 6),
            "autonomy_rolling_3d_drawdown_reduce_only": round(
                float(self.autonomy_rolling_3d_drawdown_reduce_only or 0.0),
                6,
            ),
            "autonomy_rolling_7d_drawdown_reduce_only": round(
                float(self.autonomy_rolling_7d_drawdown_reduce_only or 0.0),
                6,
            ),
        }

    def _get_autonomy_discipline_thresholds(self) -> Dict[str, float]:
        config = self.get_autonomy_risk_config()
        return {
            "daily_stop_buffer_ratio": round(-abs(float(config.get("autonomy_daily_stop_buffer_ratio") or 0.0)), 6),
            "max_drawdown_reduce_only": round(
                float(config.get("autonomy_max_drawdown_reduce_only") or 0.0),
                6,
            ),
            "rolling_3d_drawdown_reduce_only": round(
                float(config.get("autonomy_rolling_3d_drawdown_reduce_only") or 0.0),
                6,
            ),
            "rolling_7d_drawdown_reduce_only": round(
                float(config.get("autonomy_rolling_7d_drawdown_reduce_only") or 0.0),
                6,
            ),
        }

    def _snapshot_runtime_state(self) -> Dict[str, Any]:
        return {
            "daily_trades": int(self._daily_trades),
            "daily_realized_pnl": float(self._daily_realized_pnl),
            "daily_start": self._daily_start,
            "day_start_equity": self._day_start_equity,
            "current_equity": self._current_equity,
            "last_equity": self._last_equity,
            "current_unrealized_pnl": float(self._current_unrealized_pnl or 0.0),
            "equity_curve": list(self._equity_curve),
            "equity_timeline": copy.deepcopy(self._equity_timeline),
            "trade_history": copy.deepcopy(self._trade_history),
            "alerts": copy.deepcopy(self._alerts),
            "trading_halted": bool(self._trading_halted),
            "halt_reason": str(self._halt_reason or ""),
            "daily_stop_guard_until": self._daily_stop_guard_until,
            "daily_stop_breach_count": int(self._daily_stop_breach_count or 0),
        }

    def _restore_runtime_state(self, state: Optional[Dict[str, Any]]) -> None:
        s = state or {}
        self._daily_trades = int(s.get("daily_trades", 0) or 0)
        self._daily_realized_pnl = float(s.get("daily_realized_pnl", 0.0) or 0.0)
        self._daily_start = s.get("daily_start") or self._day_start(datetime.now(timezone.utc))
        self._day_start_equity = s.get("day_start_equity")
        self._current_equity = s.get("current_equity")
        self._last_equity = s.get("last_equity")
        self._current_unrealized_pnl = float(s.get("current_unrealized_pnl", 0.0) or 0.0)
        self._equity_curve = list(s.get("equity_curve") or [])
        self._equity_timeline = list(s.get("equity_timeline") or [])
        self._trade_history = list(s.get("trade_history") or [])
        self._alerts = list(s.get("alerts") or [])
        self._trading_halted = bool(s.get("trading_halted", False))
        self._halt_reason = str(s.get("halt_reason", "") or "")
        self._daily_stop_guard_until = s.get("daily_stop_guard_until")
        self._daily_stop_breach_count = int(s.get("daily_stop_breach_count", 0) or 0)

    def set_account_scope(self, scope: str, reset_baseline: bool = False) -> None:
        """Switch runtime risk state between paper/live to avoid cross-mode contamination."""
        target = "paper" if str(scope or "").lower() == "paper" else "live"
        current = getattr(self, "_risk_scope", "paper")

        if target != current:
            self._scope_states[current] = self._snapshot_runtime_state()
            self._restore_runtime_state(self._scope_states.get(target))
            self._risk_scope = target
            logger.info(f"Risk manager scope switched: {current} -> {target}")

        if reset_baseline:
            self._daily_start = self._day_start(datetime.now(timezone.utc))
            self._daily_trades = 0
            self._daily_realized_pnl = 0.0
            self._alerts.clear()
            self._trading_halted = False
            self._halt_reason = ""
            self._daily_stop_guard_until = datetime.now(timezone.utc) + timedelta(seconds=90)
            self._daily_stop_breach_count = 0
            if self._current_equity and float(self._current_equity) > 0:
                self._day_start_equity = float(self._current_equity)
                self._last_equity = float(self._current_equity)
            else:
                self._day_start_equity = None
                self._last_equity = None
            self._current_unrealized_pnl = 0.0
            self._scope_states[self._risk_scope] = self._snapshot_runtime_state()
            logger.info(f"Risk manager baseline reset for scope={self._risk_scope}")

    def _check_new_day(self) -> None:
        now_day = self._day_start(datetime.now(timezone.utc))
        if now_day <= self._daily_start:
            return

        self._daily_start = now_day
        self._daily_trades = 0
        self._daily_realized_pnl = 0.0
        self._alerts.clear()

        # Start-of-day equity anchors drawdown stop.
        if self._current_equity and self._current_equity > 0:
            self._day_start_equity = self._current_equity
        else:
            self._day_start_equity = None

        self._trading_halted = False
        self._halt_reason = ""
        self._daily_stop_guard_until = datetime.now(timezone.utc) + timedelta(seconds=45)
        self._daily_stop_breach_count = 0
        self._current_unrealized_pnl = 0.0
        logger.info("Risk manager daily counters reset")

    def _add_alert(
        self,
        title: str,
        message: str,
        severity: str = "warning",
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        alert = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "message": message,
            "severity": severity,
            "data": data or {},
        }
        self._alerts.append(alert)
        self._alerts = self._alerts[-200:]

        log_fn = logger.warning if severity in {"warning", "high"} else logger.error
        log_fn(f"[RiskAlert] {title}: {message}")

    def update_equity(
        self,
        total_usd: float,
        day_start_equity: Optional[float] = None,
        current_unrealized_pnl: Optional[float] = None,
    ) -> None:
        """Feed latest account equity to risk manager for drawdown/volatility checks."""
        self._check_new_day()

        equity = float(total_usd or 0.0)
        if equity <= 0:
            return

        if day_start_equity is not None:
            baseline = float(day_start_equity or 0.0)
            if baseline > 0:
                self._day_start_equity = baseline

        if self._day_start_equity is None:
            self._day_start_equity = equity

        if current_unrealized_pnl is not None:
            self._current_unrealized_pnl = float(current_unrealized_pnl or 0.0)

        if self._last_equity and self._last_equity > 0:
            change_ratio = (equity - self._last_equity) / self._last_equity
            if abs(change_ratio) >= self.balance_volatility_alert_pct:
                direction = "上升" if change_ratio > 0 else "下降"
                self._add_alert(
                    title="账户波动预警",
                    message=f"账户权益短时{direction}{abs(change_ratio) * 100:.2f}%",
                    severity="warning",
                    data={
                        "last_equity": round(self._last_equity, 4),
                        "current_equity": round(equity, 4),
                        "change_ratio": round(change_ratio, 6),
                    },
                )

        self._last_equity = equity
        self._current_equity = equity
        self._equity_curve.append(equity)
        self._equity_curve = self._equity_curve[-5000:]
        self._equity_timeline.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "equity": float(equity),
            }
        )
        self._equity_timeline = self._equity_timeline[-5000:]

        self._evaluate_daily_stop()

    def _evaluate_daily_stop(self) -> None:
        if self._daily_stop_guard_until and datetime.now(timezone.utc) < self._daily_stop_guard_until:
            return
        if not self._day_start_equity or self._day_start_equity <= 0 or not self._current_equity:
            return

        daily_pnl_ratio = (self._current_equity - self._day_start_equity) / self._day_start_equity
        daily_pnl_usd = self._current_equity - self._day_start_equity
        daily_realized_pnl = float(self._daily_realized_pnl or 0.0)
        current_unrealized_pnl = float(self._current_unrealized_pnl or 0.0)
        stop_basis_usd = daily_realized_pnl + min(0.0, current_unrealized_pnl)
        stop_basis_ratio = stop_basis_usd / self._day_start_equity if self._day_start_equity > 0 else 0.0

        # Profit or flat trading PnL must never trigger the circuit breaker.
        if stop_basis_usd >= 0:
            self._daily_stop_breach_count = 0
            return

        breach_ratio = stop_basis_ratio <= -abs(self.max_daily_loss_ratio)
        breach_usd = self.max_daily_loss_usd > 0 and stop_basis_usd <= -abs(self.max_daily_loss_usd)
        breached = bool(breach_ratio or breach_usd)
        if (
            breached
            and str(getattr(self, "_risk_scope", "paper")) == "live"
            and self._daily_trades <= 0
            and abs(float(self._daily_realized_pnl or 0.0)) < 1e-9
        ):
            # In live mode, external wallet transfers / manually-held exchange positions can move
            # total equity without going through the system's trade ledger. Those changes should
            # not trip the strategy circuit-breaker.
            self._add_alert(
                title="实盘权益异常波动已忽略",
                message=(
                    f"检测到未归因于系统成交的权益下降 {daily_pnl_ratio * 100:.2f}% "
                    f"({daily_pnl_usd:.2f} USDT)，但真实交易亏损仅 {stop_basis_usd:.2f} USDT，本次不触发熔断"
                ),
                severity="warning",
                data={
                    "daily_pnl_ratio": round(daily_pnl_ratio, 6),
                    "daily_pnl_usd": round(daily_pnl_usd, 4),
                    "stop_basis_usd": round(stop_basis_usd, 4),
                    "stop_basis_ratio": round(stop_basis_ratio, 6),
                    "daily_trades": int(self._daily_trades or 0),
                },
            )
            self._daily_stop_breach_count = 0
            return
        if breached:
            self._daily_stop_breach_count += 1
        else:
            self._daily_stop_breach_count = 0
            return

        required_breaches = (
            self._daily_stop_required_breaches_live
            if str(getattr(self, "_risk_scope", "paper")) == "live"
            else self._daily_stop_required_breaches_paper
        )
        if self._daily_stop_breach_count < max(1, int(required_breaches)):
            return

        if breached and not self._trading_halted:
            self._trading_halted = True
            self._halt_reason = (
                f"触发日内止损，真实交易亏损 {stop_basis_ratio * 100:.2f}% "
                f"({stop_basis_usd:.2f} USDT)"
            )
            self._add_alert(
                title="触发日内风控熔断",
                message=self._halt_reason,
                severity="critical",
                data={
                    "daily_pnl_ratio": round(daily_pnl_ratio, 6),
                    "daily_pnl_usd": round(daily_pnl_usd, 4),
                    "stop_basis_ratio": round(stop_basis_ratio, 6),
                    "stop_basis_usd": round(stop_basis_usd, 4),
                    "current_unrealized_pnl": round(current_unrealized_pnl, 4),
                    "daily_realized_pnl": round(daily_realized_pnl, 4),
                },
            )

    def reset_halt(self) -> None:
        self._trading_halted = False
        self._halt_reason = ""
        # Rebase daily anchor to current equity so stale baseline does not re-trigger instantly.
        if self._current_equity and self._current_equity > 0:
            self._day_start_equity = float(self._current_equity)
            self._last_equity = float(self._current_equity)
            self._daily_realized_pnl = 0.0
        guard_sec = 120 if str(getattr(self, "_risk_scope", "paper")) == "live" else 30
        self._daily_stop_guard_until = datetime.now(timezone.utc) + timedelta(seconds=guard_sec)
        self._daily_stop_breach_count = 0
        self._add_alert(
            title="手动解除熔断",
            message="风控熔断状态已解除，并已重置日内基线",
            severity="warning",
        )

    def clear_runtime_history(self) -> Dict[str, int]:
        """Clear runtime trade/alert/equity history for paper reset."""
        trade_count = len(self._trade_history)
        alert_count = len(self._alerts)
        curve_count = len(self._equity_curve)

        self._trade_history.clear()
        self._alerts.clear()
        self._equity_curve.clear()
        self._equity_timeline.clear()
        self._daily_trades = 0
        self._daily_realized_pnl = 0.0
        self._daily_start = self._day_start(datetime.now(timezone.utc))
        self._day_start_equity = self._current_equity
        self._last_equity = self._current_equity
        self._current_unrealized_pnl = 0.0
        self._trading_halted = False
        self._halt_reason = ""
        self._daily_stop_guard_until = datetime.now(timezone.utc) + timedelta(seconds=30)
        self._daily_stop_breach_count = 0

        return {
            "trade_history_cleared": trade_count,
            "alerts_cleared": alert_count,
            "equity_points_cleared": curve_count,
        }

    async def check_signal(
        self,
        signal: Signal,
        account_equity: Optional[float] = None,
        order_value: Optional[float] = None,
        leverage: float = 1.0,
        strategy_allocation: float = 1.0,
    ) -> bool:
        """Backward-compatible signal check entry."""
        return self.pre_trade_check(
            symbol=signal.symbol,
            side=signal.signal_type.value,
            strategy_name=signal.strategy_name,
            account_equity=account_equity,
            order_value=order_value,
            leverage=leverage,
            strategy_allocation=strategy_allocation,
            allow_close=signal.signal_type.value in {"close_long", "close_short"},
        )

    def pre_trade_check(
        self,
        symbol: str,
        side: str,
        strategy_name: Optional[str],
        account_equity: Optional[float],
        order_value: Optional[float],
        leverage: float = 1.0,
        strategy_allocation: float = 1.0,
        allow_close: bool = False,
    ) -> bool:
        """Return True if an order can pass risk checks."""
        self._check_new_day()

        if self._trading_halted and not allow_close:
            self._add_alert(
                title="交易被阻止",
                message=self._halt_reason or "系统处于熔断状态",
                severity="critical",
                data={"symbol": symbol, "side": side},
            )
            return False

        if self._daily_trades >= self.max_daily_trades and not allow_close:
            self._add_alert(
                title="交易次数超限",
                message=f"当日交易次数已达上限 {self.max_daily_trades}",
                severity="warning",
            )
            return False

        position_manager = _position_manager()
        position_count = position_manager.get_position_count()
        if position_count >= self.max_open_positions and not allow_close:
            self._add_alert(
                title="持仓数超限",
                message=f"当前持仓 {position_count} 超过限制 {self.max_open_positions}",
                severity="warning",
            )
            return False

        if leverage > self.max_leverage:
            self._add_alert(
                title="杠杆超限",
                message=f"请求杠杆 {leverage:.2f}x 超过上限 {self.max_leverage:.2f}x",
                severity="critical",
            )
            return False

        equity = float(account_equity or self._current_equity or 0.0)
        notional = float(order_value or 0.0)
        # Allow tiny float/quote drift when comparing order notional to risk caps.
        epsilon = max(1e-4, float(equity) * 1e-6, 0.05)
        if equity > 0 and notional > 0 and not allow_close:
            single_limit = equity * self.max_position_size
            if notional > single_limit + epsilon:
                self._add_alert(
                    title="单笔仓位超限",
                    message=(
                        f"订单价值 {notional:.2f} USDT 超过单笔上限 {single_limit:.2f} USDT "
                        f"({self.max_position_size * 100:.1f}% 账户权益)"
                    ),
                    severity="critical",
                )
                return False

            if strategy_name:
                allocation = max(0.0, min(float(strategy_allocation or 1.0), 1.0))
                if allocation > 0:
                    allocated_capital = equity * allocation
                    position_manager = _position_manager()
                    current_strategy_exposure = sum(
                        p.value for p in position_manager.get_positions_by_strategy(strategy_name)
                    )
                    if current_strategy_exposure + notional > allocated_capital + epsilon:
                        self._add_alert(
                            title="策略资金分配超限",
                            message=(
                                f"策略 {strategy_name} 当前敞口 {current_strategy_exposure:.2f} + "
                                f"本次 {notional:.2f} 超过分配额度 {allocated_capital:.2f}"
                            ),
                            severity="warning",
                        )
                        return False

        return True

    def record_trade(self, trade: Dict[str, Any]) -> None:
        self._check_new_day()
        self._daily_trades += 1

        pnl = float(trade.get("pnl", 0.0) or 0.0)
        self._daily_realized_pnl += pnl

        self._trade_history.append(
            {
                **trade,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._trade_history = self._trade_history[-2000:]

    def calculate_max_drawdown(self, equity_curve: List[float]) -> float:
        if not equity_curve:
            return 0.0

        peak = equity_curve[0]
        max_drawdown = 0.0
        for v in equity_curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak > 0 else 0.0
            if dd > max_drawdown:
                max_drawdown = dd
        return max_drawdown

    @staticmethod
    def _parse_equity_point_timestamp(raw_value: Any) -> Optional[datetime]:
        text = str(raw_value or "").strip()
        if not text:
            return None
        try:
            ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _equity_window_points(self, *, hours: int) -> List[Dict[str, Any]]:
        if not self._equity_timeline:
            return []
        window_hours = max(1, int(hours or 1))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        points: List[Dict[str, Any]] = []
        for row in self._equity_timeline:
            if not isinstance(row, dict):
                continue
            ts = self._parse_equity_point_timestamp(row.get("timestamp"))
            if ts is None or ts < cutoff:
                continue
            try:
                equity = float(row.get("equity") or 0.0)
            except Exception:
                continue
            if equity <= 0:
                continue
            points.append(
                {
                    "timestamp": ts.isoformat(),
                    "equity": equity,
                }
            )
        return points

    def _drawdown_snapshot_for_points(self, points: List[Dict[str, Any]], *, hours: int) -> Dict[str, Any]:
        equities = [float(row.get("equity") or 0.0) for row in points if float(row.get("equity") or 0.0) > 0]
        if not equities:
            return {
                "hours": int(hours),
                "drawdown": 0.0,
                "peak_equity": None,
                "trough_equity": None,
                "point_count": 0,
                "window_start": None,
                "window_end": None,
            }
        peak = equities[0]
        trough = equities[0]
        max_drawdown = 0.0
        peak_at = peak
        trough_at = trough
        for equity in equities:
            if equity > peak:
                peak = equity
                trough = equity
            if equity < trough:
                trough = equity
            drawdown = (peak - equity) / peak if peak > 0 else 0.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                peak_at = peak
                trough_at = equity
        return {
            "hours": int(hours),
            "drawdown": round(float(max_drawdown), 6),
            "peak_equity": round(float(peak_at), 4),
            "trough_equity": round(float(trough_at), 4),
            "point_count": len(equities),
            "window_start": str(points[0].get("timestamp") or "") if points else None,
            "window_end": str(points[-1].get("timestamp") or "") if points else None,
        }

    def get_rolling_drawdown_snapshot(self, *, hours: int) -> Dict[str, Any]:
        points = self._equity_window_points(hours=hours)
        return self._drawdown_snapshot_for_points(points, hours=hours)

    def get_risk_metrics(self) -> RiskMetrics:
        self._check_new_day()

        position_manager = _position_manager()
        positions = position_manager.get_all_positions()
        total_exposure = float(sum(float(p.value or 0.0) for p in positions))
        open_positions = len(positions)

        day_start = float(self._day_start_equity or 0.0)
        current = float(self._current_equity or 0.0)
        daily_pnl_usd = current - day_start if day_start > 0 else self._daily_realized_pnl
        stop_basis_usd = float(self._daily_realized_pnl or 0.0) + min(0.0, float(self._current_unrealized_pnl or 0.0))
        daily_pnl_ratio = (stop_basis_usd / day_start) if day_start > 0 else 0.0

        if self._trading_halted:
            level = RiskLevel.CRITICAL
        elif daily_pnl_ratio <= -abs(self.max_daily_loss_ratio) * 0.7:
            level = RiskLevel.HIGH
        elif open_positions >= int(self.max_open_positions * 0.7):
            level = RiskLevel.MEDIUM
        else:
            level = RiskLevel.LOW

        return RiskMetrics(
            total_exposure=total_exposure,
            daily_pnl_usd=daily_pnl_usd,
            daily_pnl_ratio=daily_pnl_ratio,
            daily_trades=self._daily_trades,
            open_positions=open_positions,
            max_drawdown=self.calculate_max_drawdown(self._equity_curve),
            risk_level=level,
            trading_halted=self._trading_halted,
        )

    def _build_autonomy_discipline_contract(
        self,
        *,
        daily_stop_basis_ratio: float,
        max_drawdown: float,
        rolling_3d_drawdown: float,
        rolling_7d_drawdown: float,
    ) -> Dict[str, Any]:
        reasons: List[str] = []
        degrade_mode = "normal"
        fresh_entry_allowed = True
        reduce_only = False

        thresholds = self._get_autonomy_discipline_thresholds()
        warning_ratio = float(thresholds.get("daily_stop_buffer_ratio") or 0.0)
        drawdown_reduce_only_threshold = float(thresholds.get("max_drawdown_reduce_only") or 0.0)
        rolling_3d_reduce_only_threshold = float(thresholds.get("rolling_3d_drawdown_reduce_only") or 0.0)
        rolling_7d_reduce_only_threshold = float(thresholds.get("rolling_7d_drawdown_reduce_only") or 0.0)
        if self._trading_halted:
            fresh_entry_allowed = False
            reduce_only = True
            degrade_mode = "halted"
            reasons.append(str(self._halt_reason or "risk_trading_halted"))
        else:
            if warning_ratio < 0 and float(daily_stop_basis_ratio) <= warning_ratio:
                fresh_entry_allowed = False
                reduce_only = True
                degrade_mode = "reduce_only"
                reasons.append(
                    f"daily_stop_buffer_reached({float(daily_stop_basis_ratio):.6f}<={warning_ratio:.6f})"
                )
            if drawdown_reduce_only_threshold > 0 and float(max_drawdown) >= drawdown_reduce_only_threshold:
                fresh_entry_allowed = False
                reduce_only = True
                degrade_mode = "reduce_only"
                reasons.append(
                    f"max_drawdown_limit_exceeded({float(max_drawdown):.6f}>={drawdown_reduce_only_threshold:.6f})"
                )
            if rolling_3d_reduce_only_threshold > 0 and float(rolling_3d_drawdown) >= rolling_3d_reduce_only_threshold:
                fresh_entry_allowed = False
                reduce_only = True
                degrade_mode = "reduce_only"
                reasons.append(
                    f"rolling_3d_drawdown_limit_exceeded({float(rolling_3d_drawdown):.6f}>={rolling_3d_reduce_only_threshold:.6f})"
                )
            if rolling_7d_reduce_only_threshold > 0 and float(rolling_7d_drawdown) >= rolling_7d_reduce_only_threshold:
                fresh_entry_allowed = False
                reduce_only = True
                degrade_mode = "reduce_only"
                reasons.append(
                    f"rolling_7d_drawdown_limit_exceeded({float(rolling_7d_drawdown):.6f}>={rolling_7d_reduce_only_threshold:.6f})"
                )

        return {
            "fresh_entry_allowed": bool(fresh_entry_allowed),
            "reduce_only": bool(reduce_only),
            "degrade_mode": str(degrade_mode),
            "reasons": [item for item in reasons if str(item or "").strip()],
            "thresholds": thresholds,
        }

    def get_recent_alerts(self, limit: int = 20) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 200))
        return list(self._alerts[-limit:])

    def get_trade_history(self, limit: int = 5000) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 0), 50000))
        return list(self._trade_history[-limit:])

    def get_risk_report(self) -> Dict[str, Any]:
        metrics = self.get_risk_metrics()
        autonomy_thresholds = self.get_autonomy_risk_config()
        position_manager = _position_manager()
        current_unrealized_pnl = float(position_manager.get_total_pnl() or 0.0)
        if abs(float(self._current_unrealized_pnl or 0.0)) > 0:
            current_unrealized_pnl = float(self._current_unrealized_pnl or 0.0)
        daily_realized_pnl = float(self._daily_realized_pnl or 0.0)
        daily_total_pnl = float(metrics.daily_pnl_usd or 0.0)
        daily_stop_basis = daily_realized_pnl + min(0.0, current_unrealized_pnl)
        current_equity = float(self._current_equity or 0.0)
        day_start_equity = float(self._day_start_equity or 0.0)
        if current_equity <= 0 and float(self._last_equity or 0.0) > 0:
            current_equity = float(self._last_equity or 0.0)
        if day_start_equity <= 0 and current_equity > 0:
            day_start_equity = current_equity - daily_total_pnl
        daily_total_pnl_ratio = (daily_total_pnl / day_start_equity) if day_start_equity > 0 else 0.0
        daily_stop_basis_ratio = (daily_stop_basis / day_start_equity) if day_start_equity > 0 else 0.0
        # `daily_total_pnl` is equity-based, so the residual may include funding/fees/transfers.
        daily_unrealized_component = daily_total_pnl - daily_realized_pnl
        rolling_3d = self.get_rolling_drawdown_snapshot(hours=24 * 3)
        rolling_7d = self.get_rolling_drawdown_snapshot(hours=24 * 7)
        discipline = self._build_autonomy_discipline_contract(
            daily_stop_basis_ratio=daily_stop_basis_ratio,
            max_drawdown=float(metrics.max_drawdown or 0.0),
            rolling_3d_drawdown=float(rolling_3d.get("drawdown") or 0.0),
            rolling_7d_drawdown=float(rolling_7d.get("drawdown") or 0.0),
        )
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "risk_level": metrics.risk_level.value,
            "trading_halted": metrics.trading_halted,
            "halt_reason": self._halt_reason,
            "discipline": discipline,
            "drawdown": {
                "max_drawdown": round(float(metrics.max_drawdown), 6),
                "rolling_3d": rolling_3d,
                "rolling_7d": rolling_7d,
            },
            "daily_stop_guard_seconds": max(
                0,
                int((self._daily_stop_guard_until - datetime.now(timezone.utc)).total_seconds()),
            )
            if self._daily_stop_guard_until
            else 0,
            "equity": {
                "current": round(current_equity, 4),
                "day_start": round(day_start_equity, 4),
                "daily_pnl_usd": round(float(metrics.daily_pnl_usd), 4),
                "daily_total_pnl_usd": round(daily_total_pnl, 4),
                "daily_realized_pnl_usd": round(daily_realized_pnl, 4),
                "daily_unrealized_component_usd": round(daily_unrealized_component, 4),
                "current_unrealized_pnl_usd": round(current_unrealized_pnl, 4),
                "daily_stop_basis_usd": round(daily_stop_basis, 4),
                "daily_total_pnl_ratio": round(daily_total_pnl_ratio, 6),
                "daily_stop_basis_ratio": round(daily_stop_basis_ratio, 6),
                "daily_pnl_ratio": round(daily_stop_basis_ratio, 6),
                "max_drawdown": round(float(metrics.max_drawdown), 6),
                "rolling_3d_drawdown": round(float(rolling_3d.get("drawdown") or 0.0), 6),
                "rolling_7d_drawdown": round(float(rolling_7d.get("drawdown") or 0.0), 6),
                "pnl_scope_note": "daily_total_pnl_usd为今日权益变化；daily_stop_basis_usd=已实现盈亏+当前浮亏，仅该值用于熔断",
            },
            "limits": {
                "max_position_size": self.max_position_size,
                "max_daily_loss_ratio": self.max_daily_loss_ratio,
                "max_daily_loss_usd": self.max_daily_loss_usd,
                "max_daily_trades": self.max_daily_trades,
                "max_open_positions": self.max_open_positions,
                "max_leverage": self.max_leverage,
                "balance_volatility_alert_pct": self.balance_volatility_alert_pct,
                "autonomy_daily_stop_buffer_ratio": autonomy_thresholds["autonomy_daily_stop_buffer_ratio"],
                "autonomy_max_drawdown_reduce_only": autonomy_thresholds["autonomy_max_drawdown_reduce_only"],
                "autonomy_rolling_3d_drawdown_reduce_only": autonomy_thresholds[
                    "autonomy_rolling_3d_drawdown_reduce_only"
                ],
                "autonomy_rolling_7d_drawdown_reduce_only": autonomy_thresholds[
                    "autonomy_rolling_7d_drawdown_reduce_only"
                ],
                "autonomy_thresholds": autonomy_thresholds,
            },
            "utilization": {
                "daily_trade_utilization": (
                    metrics.daily_trades / self.max_daily_trades if self.max_daily_trades > 0 else 0
                ),
                "position_utilization": (
                    metrics.open_positions / self.max_open_positions if self.max_open_positions > 0 else 0
                ),
            },
            "alerts": self.get_recent_alerts(30),
        }

    def update_parameters(self, params: Dict[str, Any]) -> None:
        mapping = {
            "max_position_size": "max_position_size",
            "max_daily_loss_ratio": "max_daily_loss_ratio",
            "max_daily_loss_usd": "max_daily_loss_usd",
            "max_daily_trades": "max_daily_trades",
            "max_open_positions": "max_open_positions",
            "max_leverage": "max_leverage",
            "balance_volatility_alert_pct": "balance_volatility_alert_pct",
            "autonomy_daily_stop_buffer_ratio": "autonomy_daily_stop_buffer_ratio",
            "autonomy_max_drawdown_reduce_only": "autonomy_max_drawdown_reduce_only",
            "autonomy_rolling_3d_drawdown_reduce_only": "autonomy_rolling_3d_drawdown_reduce_only",
            "autonomy_rolling_7d_drawdown_reduce_only": "autonomy_rolling_7d_drawdown_reduce_only",
        }

        for key, attr in mapping.items():
            if key in params and params[key] is not None:
                setattr(self, attr, float(params[key]))

        autonomy_aliases = {
            "daily_stop_buffer_ratio": "autonomy_daily_stop_buffer_ratio",
            "max_drawdown_reduce_only": "autonomy_max_drawdown_reduce_only",
            "rolling_3d_drawdown_reduce_only": "autonomy_rolling_3d_drawdown_reduce_only",
            "rolling_7d_drawdown_reduce_only": "autonomy_rolling_7d_drawdown_reduce_only",
        }
        for key, attr in autonomy_aliases.items():
            if key in params and params[key] is not None:
                setattr(self, attr, float(params[key]))

        # Backward-compatible names.
        if "max_daily_loss" in params and params["max_daily_loss"] is not None:
            self.max_daily_loss_ratio = float(params["max_daily_loss"])

        if "max_daily_trades" in params and params["max_daily_trades"] is not None:
            self.max_daily_trades = int(float(params["max_daily_trades"]))
        if "max_open_positions" in params and params["max_open_positions"] is not None:
            self.max_open_positions = int(float(params["max_open_positions"]))

        self.max_position_size = max(0.001, min(self.max_position_size, 1.0))
        self.max_daily_loss_ratio = max(0.001, min(self.max_daily_loss_ratio, 1.0))
        self.max_daily_loss_usd = max(0.0, self.max_daily_loss_usd)
        self.max_daily_trades = max(1, int(self.max_daily_trades))
        self.max_open_positions = max(1, int(self.max_open_positions))
        self.max_leverage = max(1.0, float(self.max_leverage))
        self.balance_volatility_alert_pct = max(0.01, min(float(self.balance_volatility_alert_pct), 1.0))
        self._normalize_autonomy_thresholds()
        self._save_autonomy_threshold_overlay()

        logger.info(f"Risk parameters updated: {params}")


risk_manager = RiskManager()
