"""Risk gate for signal filtering."""
from __future__ import annotations

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


class RiskGate:
    """Apply spread/vol/cooldown/circuit-breaker checks."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        thresholds = (cfg or {}).get("thresholds") if isinstance(cfg, dict) else {}
        thresholds = thresholds or {}
        self.config = RiskGateConfig(
            alpha_threshold=float(thresholds.get("alpha_threshold") or 0.08),
            cooldown_min=int(thresholds.get("cooldown_min") or 20),
            max_vol=float(thresholds.get("max_vol") or 0.12),
            max_spread=float(thresholds.get("max_spread") or 0.004),
            breaker_drawdown=float(thresholds.get("breaker_drawdown") or 0.15),
        )
        self._last_active_signal_at: Dict[str, datetime] = {}
        self._breaker_on: bool = False

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
