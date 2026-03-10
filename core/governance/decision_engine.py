from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import select

from config.database import RiskConfig, async_session_maker
from config.settings import settings
from core.governance.audit import GovernanceAuditEvent, new_trace_id, write_audit


@dataclass
class DecisionOutcome:
    allowed: bool
    action: str
    reason: str
    trace_id: str
    decision_mode: str
    reduce_only: bool = False
    metadata: Optional[Dict[str, Any]] = None


class DecisionEngine:
    def __init__(self) -> None:
        self._risk_cfg_cache: Dict[str, Any] = {}
        self._risk_cfg_loaded_at: Optional[datetime] = None
        self._risk_cfg_ttl_sec = 5
        self._lock = asyncio.Lock()

    async def _load_active_risk_config(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        if (
            self._risk_cfg_loaded_at is not None
            and (now - self._risk_cfg_loaded_at).total_seconds() < self._risk_cfg_ttl_sec
            and self._risk_cfg_cache
        ):
            return dict(self._risk_cfg_cache)

        async with self._lock:
            if (
                self._risk_cfg_loaded_at is not None
                and (now - self._risk_cfg_loaded_at).total_seconds() < self._risk_cfg_ttl_sec
                and self._risk_cfg_cache
            ):
                return dict(self._risk_cfg_cache)
            async with async_session_maker() as session:
                result = await session.execute(select(RiskConfig).where(RiskConfig.is_active.is_(True)).limit(1))
                row = result.scalars().first()
                if row is None:
                    rm = _risk_manager()
                    cfg = {
                        "max_leverage": float(rm.max_leverage or 3.0),
                        "max_position_notional_pct": float(rm.max_position_size or 0.1),
                        "max_trade_risk_pct": float(rm.max_position_size or 0.1),
                        "max_daily_drawdown_pct": float(rm.max_daily_loss_ratio or 0.02),
                        "spread_limit_bps": 25.0,
                        "data_staleness_limit_ms": 60_000,
                        "allowed_symbols": [],
                        "allowed_timeframes": [],
                        "reduce_only": False,
                        "kill_switch": False,
                    }
                else:
                    cfg = dict(row.config or {})
            self._risk_cfg_cache = dict(cfg)
            self._risk_cfg_loaded_at = now
            return dict(cfg)

    def _daily_drawdown_blocked(self, cfg: Dict[str, Any]) -> bool:
        report = _risk_manager().get_risk_report()
        equity = dict(report.get("equity") or {})
        daily_pnl_ratio = float(equity.get("daily_pnl_ratio", 0.0) or 0.0)
        dd_limit = abs(float(cfg.get("max_daily_drawdown_pct", 0.02) or 0.02))
        return daily_pnl_ratio <= -dd_limit

    @staticmethod
    def infer_market_regime(
        *,
        adx: Optional[float] = None,
        volatility: Optional[float] = None,
        spread_bps: Optional[float] = None,
        return_shock: Optional[float] = None,
    ) -> str:
        if spread_bps is not None and spread_bps >= 40:
            return "illiquid"
        if return_shock is not None and abs(return_shock) >= 0.04:
            return "shock"
        if volatility is not None and volatility >= 0.03:
            return "high_vol"
        if adx is not None and adx >= 25:
            return "trend"
        return "range"

    async def evaluate_order_intent(
        self,
        *,
        symbol: str,
        side: str,
        leverage: float,
        order_value: float,
        account_equity: float,
        signal_ts: Optional[datetime],
        allow_close: bool = False,
        spread_bps: Optional[float] = None,
        timeframe: Optional[str] = None,
        source: str = "strategy",
    ) -> DecisionOutcome:
        trace_id = new_trace_id()
        mode = str(getattr(settings, "DECISION_MODE", "shadow") or "shadow").lower()
        if not bool(getattr(settings, "GOVERNANCE_ENABLED", True)):
            return DecisionOutcome(
                allowed=True,
                action="allow",
                reason="governance_disabled",
                trace_id=trace_id,
                decision_mode=mode,
                reduce_only=False,
            )

        cfg = await self._load_active_risk_config()
        blocked_reason = ""

        if bool(cfg.get("kill_switch", False)) and not allow_close:
            blocked_reason = "kill_switch_enabled"
        elif bool(cfg.get("reduce_only", False)) and not allow_close:
            blocked_reason = "reduce_only_enabled"
        elif leverage > float(cfg.get("max_leverage", 3.0) or 3.0):
            blocked_reason = "max_leverage_exceeded"
        elif account_equity > 0 and order_value > account_equity * float(cfg.get("max_position_notional_pct", 0.1) or 0.1) and not allow_close:
            blocked_reason = "max_position_notional_pct_exceeded"
        elif account_equity > 0 and order_value > account_equity * float(cfg.get("max_trade_risk_pct", 0.1) or 0.1) and not allow_close:
            blocked_reason = "max_trade_risk_pct_exceeded"
        elif spread_bps is not None and spread_bps > float(cfg.get("spread_limit_bps", 25.0) or 25.0) and not allow_close:
            blocked_reason = "spread_limit_exceeded"
        elif self._daily_drawdown_blocked(cfg) and not allow_close:
            blocked_reason = "max_daily_drawdown_reached"
        else:
            max_staleness = int(cfg.get("data_staleness_limit_ms", 60_000) or 60_000)
            if signal_ts is not None:
                if signal_ts.tzinfo is None:
                    signal_ts = signal_ts.replace(tzinfo=timezone.utc)
                age_ms = int((datetime.now(timezone.utc) - signal_ts).total_seconds() * 1000)
                if age_ms > max_staleness and not allow_close:
                    blocked_reason = "signal_data_stale"

        allowed_symbols = [str(x).upper() for x in (cfg.get("allowed_symbols") or [])]
        if allowed_symbols and str(symbol or "").upper() not in set(allowed_symbols) and not allow_close:
            blocked_reason = "symbol_not_in_allowed_universe"

        allowed_timeframes = [str(x) for x in (cfg.get("allowed_timeframes") or [])]
        if allowed_timeframes and timeframe and str(timeframe) not in set(allowed_timeframes) and not allow_close:
            blocked_reason = "timeframe_not_allowed"

        if blocked_reason:
            outcome = DecisionOutcome(
                allowed=False,
                action="blocked",
                reason=blocked_reason,
                trace_id=trace_id,
                decision_mode=mode,
                reduce_only=bool(cfg.get("reduce_only", False)),
                metadata={"risk_config": cfg, "source": source},
            )
        else:
            action = "shadow_allow" if mode == "shadow" else "allow"
            outcome = DecisionOutcome(
                allowed=True,
                action=action,
                reason="passed",
                trace_id=trace_id,
                decision_mode=mode,
                reduce_only=bool(cfg.get("reduce_only", False)),
                metadata={"risk_config": cfg, "source": source},
            )

        await write_audit(
            GovernanceAuditEvent(
                module="governance.decision",
                action="evaluate_order_intent",
                status="success" if outcome.allowed else "denied",
                actor="system",
                role="SYSTEM",
                trace_id=trace_id,
                input_payload={
                    "symbol": symbol,
                    "side": side,
                    "leverage": leverage,
                    "order_value": order_value,
                    "account_equity": account_equity,
                    "allow_close": allow_close,
                    "spread_bps": spread_bps,
                    "timeframe": timeframe,
                    "source": source,
                },
                output_payload={
                    "allowed": outcome.allowed,
                    "action": outcome.action,
                    "reason": outcome.reason,
                    "decision_mode": outcome.decision_mode,
                },
                payload_json={"risk_config": cfg},
            )
        )
        return outcome


decision_engine = DecisionEngine()


def _risk_manager():
    from core.risk.risk_manager import risk_manager

    return risk_manager
