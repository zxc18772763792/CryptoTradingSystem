"""Risk gate for signal filtering."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class RiskGateConfig:
    alpha_threshold: float = 0.08
    cooldown_min: int = 20
    max_vol: float = 0.12
    max_spread: float = 0.004
    breaker_drawdown: float = 0.15
    pm_features_enable: bool = False
    pm_global_risk_medium: float = 0.45
    pm_global_risk_high: float = 0.65
    pm_geo_halt_minutes: int = 15
    pm_spread_liquidity_min: float = 1.0
    pm_reduce_only_on_high_risk: bool = True
    pm_notional_multiplier_high_risk: float = 0.5
    pm_leverage_multiplier_high_risk: float = 0.4


class RiskGate:
    """Apply spread/vol/cooldown/circuit-breaker checks."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        thresholds = (cfg or {}).get("thresholds") if isinstance(cfg, dict) else {}
        thresholds = thresholds or {}
        pm_cfg = (cfg or {}).get("polymarket") if isinstance(cfg, dict) else {}
        pm_cfg = pm_cfg or {}
        self.config = RiskGateConfig(
            alpha_threshold=float(thresholds.get("alpha_threshold") or 0.08),
            cooldown_min=int(thresholds.get("cooldown_min") or 20),
            max_vol=float(thresholds.get("max_vol") or 0.12),
            max_spread=float(thresholds.get("max_spread") or 0.004),
            breaker_drawdown=float(thresholds.get("breaker_drawdown") or 0.15),
            pm_features_enable=bool(
                pm_cfg.get("enable")
                if pm_cfg.get("enable") is not None
                else str(os.getenv("PM_FEATURES_ENABLE") or "").strip().lower() in {"1", "true", "yes", "on", "y"}
            ),
            pm_global_risk_medium=float(pm_cfg.get("global_risk_medium") or os.getenv("PM_GLOBAL_RISK_MEDIUM") or 0.45),
            pm_global_risk_high=float(pm_cfg.get("global_risk_high") or os.getenv("PM_GLOBAL_RISK_HIGH") or 0.65),
            pm_geo_halt_minutes=int(pm_cfg.get("geo_halt_minutes") or os.getenv("PM_GEO_HALT_MINUTES") or 15),
            pm_spread_liquidity_min=float(pm_cfg.get("spread_liquidity_min") or os.getenv("PM_SPREAD_LIQUIDITY_MIN") or 1.0),
            pm_reduce_only_on_high_risk=bool(pm_cfg.get("reduce_only_on_high_risk", True)),
            pm_notional_multiplier_high_risk=float(pm_cfg.get("notional_multiplier_high_risk") or os.getenv("PM_NOTIONAL_MULTIPLIER_HIGH_RISK") or 0.5),
            pm_leverage_multiplier_high_risk=float(pm_cfg.get("leverage_multiplier_high_risk") or os.getenv("PM_LEVERAGE_MULTIPLIER_HIGH_RISK") or 0.4),
        )
        self._last_active_signal_at: Dict[str, datetime] = {}
        self._breaker_on: bool = False
        self._pm_halt_until: Dict[str, datetime] = {}

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def set_breaker(self, enabled: bool) -> None:
        self._breaker_on = bool(enabled)

    def evaluate(
        self,
        symbol: str,
        proposed_signal: str,
        market_features: Optional[Dict[str, Any]],
        now: Optional[datetime] = None,
    ) -> Tuple[str, List[str]]:
        """Return final signal and blocking reasons."""
        symbol = str(symbol or "").upper()
        signal = str(proposed_signal or "FLAT").upper()
        now = now or datetime.now(timezone.utc)
        features = market_features or {}

        reasons: List[str] = []

        spread = abs(self._to_float(features.get("spread"), 0.0))
        vol_1h = abs(self._to_float(features.get("vol_1h"), 0.0))
        drawdown = abs(self._to_float(features.get("drawdown"), 0.0))
        breaker_flag = bool(features.get("circuit_breaker") or features.get("breaker_on") or self._breaker_on)
        pm_global_risk = self._to_float(features.get("pm_global_risk"), 0.0)
        pm_macro_shock = self._to_float(features.get("pm_macro_shock_sev"), 0.0)
        pm_geo_shock = self._to_float(features.get("pm_geo_shock_sev"), 0.0)
        pm_reg_shock = self._to_float(features.get("pm_reg_shock_sev"), 0.0)
        pm_liquidity_score = self._to_float(features.get("pm_liquidity_score", features.get("pm_price_liquidity")), 0.0)

        if spread > self.config.max_spread:
            reasons.append(f"spread {spread:.5f} > max_spread {self.config.max_spread:.5f}")

        if vol_1h > self.config.max_vol:
            reasons.append(f"vol_1h {vol_1h:.5f} > max_vol {self.config.max_vol:.5f}")

        if breaker_flag:
            reasons.append("circuit breaker is on")

        if drawdown >= self.config.breaker_drawdown:
            reasons.append(
                f"drawdown {drawdown:.4f} >= breaker_drawdown {self.config.breaker_drawdown:.4f}"
            )

        pm_halt_until = self._pm_halt_until.get(symbol)
        if pm_halt_until and pm_halt_until > now:
            reasons.append(f"pm geo halt active until {pm_halt_until.isoformat()}")

        if self.config.pm_features_enable:
            if pm_global_risk >= self.config.pm_global_risk_high and signal != "FLAT":
                reasons.append(
                    f"pm_global_risk {pm_global_risk:.4f} >= high {self.config.pm_global_risk_high:.4f}"
                )
            elif pm_global_risk >= self.config.pm_global_risk_medium and signal != "FLAT":
                reasons.append(
                    f"pm_global_risk elevated {pm_global_risk:.4f} >= medium {self.config.pm_global_risk_medium:.4f}"
                )

            if (
                pm_geo_shock >= self.config.pm_global_risk_medium
                and spread > self.config.max_spread
                and pm_liquidity_score <= self.config.pm_spread_liquidity_min
            ):
                halt_until = now + timedelta(minutes=max(1, int(self.config.pm_geo_halt_minutes)))
                self._pm_halt_until[symbol] = halt_until
                reasons.append(
                    f"geo shock {pm_geo_shock:.4f} + weak liquidity {pm_liquidity_score:.4f} -> halt until {halt_until.isoformat()}"
                )

            if pm_macro_shock > 0:
                reasons.append(f"pm macro shock {pm_macro_shock:.4f}")
            if pm_reg_shock > 0:
                reasons.append(f"pm reg shock {pm_reg_shock:.4f}")

        cooldown = max(0, int(self.config.cooldown_min))
        if signal != "FLAT" and cooldown > 0:
            last_ts = self._last_active_signal_at.get(symbol)
            if last_ts:
                remaining = (last_ts + timedelta(minutes=cooldown) - now).total_seconds()
                if remaining > 0:
                    reasons.append(f"cooldown {remaining:.0f}s remaining")

        if reasons and signal != "FLAT":
            return "FLAT", reasons

        if signal != "FLAT":
            self._last_active_signal_at[symbol] = now

        return signal, reasons
