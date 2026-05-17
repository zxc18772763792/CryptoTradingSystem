"""Three-way signal aggregator.

Combines three independent signal sources with configurable weights:

* **LLM** (weight 0.40) – news/event signal from :func:`signal_engine.generate_signal`
* **ML**  (weight 0.35) – XGBoost directional prediction from :class:`MLSignalModel`
* **Factor** (weight 0.25) – rule-based RSI + EMA trend + momentum score

The final ``AggregatedSignal.direction`` is the weighted vote winner.
``requires_approval`` is ``True`` when confidence is below the
*high_confidence_threshold*, giving operators a chance to review before
execution.

Usage::

    aggregator = SignalAggregator(ml_model_path="models/ml_signal_xgb.json")
    signal = await aggregator.aggregate("BTC/USDT", market_df)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger

from config.settings import settings
from core.ai.ml_signal import MLSignalModel, MLSignalResult, build_feature_frame
from core.ai.risk_gate import RiskGate


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------

@dataclass
class AggregatedSignal:
    symbol: str
    direction: str              # "LONG" | "SHORT" | "FLAT"
    confidence: float           # 0 – 1
    components: Dict[str, Any] = field(default_factory=dict)
    market_context: Dict[str, Any] = field(default_factory=dict)
    requires_approval: bool = True
    blocked_by_risk: bool = False
    risk_reason: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": round(self.confidence, 6),
            "requires_approval": self.requires_approval,
            "blocked_by_risk": self.blocked_by_risk,
            "risk_reason": self.risk_reason,
            "components": self.components,
            "market_context": self.market_context,
            "timestamp": self.timestamp.isoformat(),
        }


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

_DEFAULT_ML_MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "ml_signal_xgb.json"
)


class SignalAggregator:
    """Weighted-vote aggregator over core signals plus derivatives shadow context."""

    WEIGHTS: Dict[str, float] = {"llm": 0.40, "ml": 0.35, "factor": 0.25, "derivatives": 0.20}

    def __init__(
        self,
        ml_model_path: Optional[str] = None,
        high_confidence_threshold: float = 0.70,
        signal_since_minutes: int = 240,
        signal_cfg: Optional[Dict[str, Any]] = None,
    ):
        self._high_conf_threshold = float(high_confidence_threshold)
        self._signal_since_minutes = int(signal_since_minutes)
        self._signal_cfg: Dict[str, Any] = signal_cfg or {}
        self._risk_gate = RiskGate()

        # Lazy-load the ML model
        path = str(ml_model_path or _DEFAULT_ML_MODEL_PATH)
        self._ml_model = MLSignalModel.load_from_path(path)
        if not self._ml_model.is_loaded():
            logger.info(
                "SignalAggregator: ML model not available – ML weight will be zero"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def aggregate(
        self,
        symbol: str,
        market_data: Optional[pd.DataFrame],
        *,
        include_llm: bool = True,
        include_ml: bool = True,
        include_factor: bool = True,
        include_derivatives: bool = True,
    ) -> AggregatedSignal:
        """Compute weighted signal for *symbol* using latest *market_data*."""
        components: Dict[str, Any] = {}
        market_context: Dict[str, Any] = {}
        has_market_data = market_data is not None and not market_data.empty

        # ---- 1. LLM signal ----
        llm_reason = ""
        if include_llm:
            llm_direction, llm_conf = await self._get_llm_signal(symbol, market_data)
            llm_available = bool(llm_direction in {"LONG", "SHORT"} or llm_conf > 0.0)
            if not llm_available:
                llm_reason = "no_recent_events_or_signal_unavailable"
        else:
            llm_direction, llm_conf = "FLAT", 0.0
            llm_available = False
            llm_reason = "disabled_for_fast_scan"
        llm_effective_weight = self.WEIGHTS["llm"] if llm_available else 0.0
        components["llm"] = {
            "direction": llm_direction,
            "confidence": round(llm_conf, 6),
            "weight": self.WEIGHTS["llm"],
            "effective_weight": round(llm_effective_weight, 6),
            "available": llm_available,
            "status": self._component_status(direction=llm_direction, confidence=llm_conf, available=llm_available),
            "reason": llm_reason,
        }

        # ---- 2. ML signal ----
        ml_reason = ""
        if include_ml:
            ml_direction, ml_conf = self._get_ml_signal(symbol, market_data)
            ml_available = bool(self._ml_model.is_loaded() and has_market_data)
            if not ml_available:
                ml_reason = "insufficient_market_data" if self._ml_model.is_loaded() and not has_market_data else "ml_model_unavailable"
        else:
            ml_direction, ml_conf = "FLAT", 0.0
            ml_available = False
            ml_reason = "disabled_for_fast_scan"
        ml_effective_weight = self.WEIGHTS["ml"] if ml_available else 0.0
        components["ml"] = {
            "direction": ml_direction,
            "confidence": round(ml_conf, 6),
            "weight": self.WEIGHTS["ml"],
            "effective_weight": round(ml_effective_weight, 6),
            "available": ml_available,
            "status": self._component_status(direction=ml_direction, confidence=ml_conf, available=ml_available),
            "reason": ml_reason,
        }

        # ---- 3. Factor signal ----
        factor_reason = ""
        if include_factor:
            factor_direction, factor_conf = self._get_factor_signal(market_data)
            factor_available = bool(has_market_data and len(market_data) >= 30)
            if not factor_available:
                factor_reason = "insufficient_market_data"
        else:
            factor_direction, factor_conf = "FLAT", 0.0
            factor_available = False
            factor_reason = "disabled_for_fast_scan"
        factor_effective_weight = self.WEIGHTS["factor"] if factor_available else 0.0
        components["factor"] = {
            "direction": factor_direction,
            "confidence": round(factor_conf, 6),
            "weight": self.WEIGHTS["factor"],
            "effective_weight": round(factor_effective_weight, 6),
            "available": factor_available,
            "status": self._component_status(direction=factor_direction, confidence=factor_conf, available=factor_available),
            "reason": factor_reason,
        }

        # ---- 4. Derivatives / structure signal ----
        derivatives_reason = ""
        derivatives_regime = None
        derivatives_flags: list[str] = []
        derivatives_explain = ""
        derivatives_shadow_only = not bool(getattr(settings, "COINGLASS_LIVE_GATING_ENABLED", False))
        if include_derivatives:
            derivatives_direction, derivatives_conf, derivatives_meta = await self._get_derivatives_signal(symbol)
            derivatives_available = bool(derivatives_meta.get("available"))
            if not derivatives_available:
                derivatives_reason = str(derivatives_meta.get("reason") or "coinglass_snapshot_unavailable")
            derivatives_regime = derivatives_meta.get("regime")
            derivatives_flags = list(derivatives_meta.get("risk_flags") or [])
            derivatives_explain = str(derivatives_meta.get("explain") or "")
            market_context = dict(derivatives_meta.get("context") or {})
        else:
            derivatives_direction, derivatives_conf = "FLAT", 0.0
            derivatives_available = False
            derivatives_reason = "disabled_for_fast_scan"
        if derivatives_available and derivatives_shadow_only and not derivatives_reason:
            derivatives_reason = "shadow_only_context"
        derivatives_effective_weight = (
            self.WEIGHTS["derivatives"] if derivatives_available and not derivatives_shadow_only else 0.0
        )
        components["derivatives"] = {
            "direction": derivatives_direction,
            "confidence": round(derivatives_conf, 6),
            "weight": self.WEIGHTS["derivatives"],
            "effective_weight": round(derivatives_effective_weight, 6),
            "available": derivatives_available,
            "status": self._component_status(direction=derivatives_direction, confidence=derivatives_conf, available=derivatives_available),
            "reason": derivatives_reason,
            "regime": derivatives_regime,
            "risk_flags": derivatives_flags,
            "explain": derivatives_explain,
            "context": market_context,
            "shadow_only": derivatives_shadow_only,
            "confidence_adjustment": 0.0,
        }

        # ---- 5. Weighted vote ----
        direction, confidence = self._weighted_vote(
            [
                (llm_direction, llm_conf, llm_effective_weight),
                (ml_direction, ml_conf, ml_effective_weight),
                (factor_direction, factor_conf, factor_effective_weight),
                (derivatives_direction, derivatives_conf, derivatives_effective_weight),
            ]
        )
        if derivatives_available and derivatives_shadow_only:
            shadow_penalty = self._apply_derivatives_shadow_adjustment(
                direction=direction,
                confidence=confidence,
                market_context=market_context,
                risk_flags=derivatives_flags,
            )
            if shadow_penalty > 0:
                confidence = max(0.0, confidence - shadow_penalty)
                components["derivatives"]["confidence_adjustment"] = round(-shadow_penalty, 6)
                components["derivatives"]["reason"] = "shadow_penalty_applied"

        # ---- 6. Risk-gate filter ----
        blocked, risk_reason = self._apply_risk_gate(symbol, direction, confidence, market_data)
        final_direction = "FLAT" if blocked else direction

        requires_approval = blocked or (
            final_direction in {"LONG", "SHORT"} and confidence < self._high_conf_threshold
        )

        return AggregatedSignal(
            symbol=symbol,
            direction=final_direction,
            confidence=round(confidence, 6),
            components=components,
            market_context=market_context,
            requires_approval=requires_approval,
            blocked_by_risk=blocked,
            risk_reason=risk_reason,
        )

    @staticmethod
    def _component_status(*, direction: str, confidence: float, available: bool) -> str:
        if not available:
            return "unavailable"
        if str(direction or "").upper() in {"LONG", "SHORT"}:
            return "active"
        if float(confidence or 0.0) > 0.0:
            return "neutral"
        return "idle"

    # ------------------------------------------------------------------
    # Sub-signals
    # ------------------------------------------------------------------

    async def _get_llm_signal(
        self,
        symbol: str,
        market_data: Optional[pd.DataFrame],
    ) -> tuple[str, float]:
        """Fetch latest news/event signal from signal_engine."""
        try:
            from core.ai.signal_engine import generate_signal  # noqa: PLC0415

            market_features: Dict[str, Any] = {}
            if market_data is not None and not market_data.empty:
                last = market_data.iloc[-1]
                if "atr" in market_data.columns:
                    market_features["atr"] = float(last.get("atr", 0.0))

            result = await generate_signal(
                symbol=symbol,
                market_features=market_features,
                since_minutes=self._signal_since_minutes,
                cfg=self._signal_cfg,
                risk_gate=None,
            )
            raw_signal = str(result.get("signal") or "FLAT").upper()
            direction = raw_signal if raw_signal in {"LONG", "SHORT"} else "FLAT"
            confidence = float(result.get("confidence") or 0.0)
            return direction, confidence
        except Exception as exc:
            logger.debug(f"SignalAggregator: LLM signal failed for {symbol}: {exc}")
            return "FLAT", 0.0

    def _get_ml_signal(
        self,
        symbol: str,
        market_data: Optional[pd.DataFrame],
    ) -> tuple[str, float]:
        """Run ML model on the feature DataFrame."""
        if not self._ml_model.is_loaded() or market_data is None or market_data.empty:
            return "FLAT", 0.0
        try:
            features = build_feature_frame(market_data)
            result: MLSignalResult = self._ml_model.predict(features, symbol=symbol)
            return result.direction, result.confidence
        except Exception as exc:
            logger.debug(f"SignalAggregator: ML signal failed for {symbol}: {exc}")
            return "FLAT", 0.0

    def _get_factor_signal(
        self,
        market_data: Optional[pd.DataFrame],
    ) -> tuple[str, float]:
        """Rule-based signal using RSI, EMA trend, and momentum."""
        if market_data is None or market_data.empty or len(market_data) < 30:
            return "FLAT", 0.0
        try:
            close = market_data["close"].astype(float)

            # RSI-14
            delta = close.diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.ewm(com=13, adjust=False, min_periods=14).mean()
            avg_loss = loss.ewm(com=13, adjust=False, min_periods=14).mean()
            rs = avg_gain / (avg_loss + 1e-9)
            rsi = float((100.0 - (100.0 / (1.0 + rs))).iloc[-1])

            # EMA trend
            ema_fast = float(close.ewm(span=8, adjust=False).mean().iloc[-1])
            ema_slow = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
            trend_up = ema_fast > ema_slow

            # Momentum-14
            momentum = float(close.pct_change(14).iloc[-1])

            # Scoring: normalise each component to [-1, +1]
            rsi_score = (rsi - 50.0) / 50.0          # +1 = overbought, -1 = oversold
            trend_score = 1.0 if trend_up else -1.0
            mom_score = max(-1.0, min(1.0, momentum * 10.0))

            combined = (
                rsi_score * 0.30
                + trend_score * 0.50
                + mom_score * 0.20
            )

            confidence = min(abs(combined), 1.0)
            if combined > 0.15:
                direction = "LONG"
            elif combined < -0.15:
                direction = "SHORT"
            else:
                direction = "FLAT"

            # F0a: Fear & Greed adjustment (±0.08 confidence boost at extremes)
            try:
                from core.data.sentiment.fear_greed_collector import fear_greed_collector  # noqa: PLC0415
                fg = fear_greed_collector.latest()
                if fg is not None:
                    if fg.is_extreme_fear and direction == "LONG":
                        confidence = min(1.0, confidence + 0.08)
                    elif fg.is_extreme_greed and direction == "SHORT":
                        confidence = min(1.0, confidence + 0.08)
            except (ImportError, AttributeError) as exc:
                logger.debug(f"SignalAggregator: fear/greed adjustment skipped: {exc}")

            return direction, confidence
        except Exception as exc:
            logger.debug(f"SignalAggregator: factor signal failed: {exc}")
            return "FLAT", 0.0

    async def _get_derivatives_signal(self, symbol: str) -> tuple[str, float, Dict[str, Any]]:
        try:
            from core.ai.coinglass_signal import build_coinglass_signal  # noqa: PLC0415

            return await build_coinglass_signal(symbol)
        except Exception as exc:
            logger.debug(f"SignalAggregator: derivatives signal failed for {symbol}: {exc}")
            return "FLAT", 0.0, {"available": False, "reason": "derivatives_signal_failed"}

    # ------------------------------------------------------------------
    # Voting + risk gate
    # ------------------------------------------------------------------

    @staticmethod
    def _weighted_vote(
        signals: list[tuple[str, float, float]],
    ) -> tuple[str, float]:
        """Weighted vote over (direction, confidence, weight) triples."""
        score: Dict[str, float] = {"LONG": 0.0, "SHORT": 0.0, "FLAT": 0.0}
        total_weight = sum(w for _, _, w in signals if w > 0)
        if total_weight <= 0:
            return "FLAT", 0.0

        for direction, confidence, weight in signals:
            if weight <= 0:
                continue
            key = direction if direction in {"LONG", "SHORT"} else "FLAT"
            score[key] += weight * confidence

        if max(score.values()) <= 0:
            return "FLAT", 0.0

        winner = max(score, key=lambda k: score[k])
        raw_confidence = score[winner] / total_weight
        return winner, min(1.0, raw_confidence)

    @staticmethod
    def _apply_derivatives_shadow_adjustment(
        *,
        direction: str,
        confidence: float,
        market_context: Dict[str, Any],
        risk_flags: list[str],
    ) -> float:
        if str(direction or "").upper() not in {"LONG", "SHORT"}:
            return 0.0
        if float(confidence or 0.0) <= 0.0:
            return 0.0

        penalty = 0.0
        normalized_flags = {str(flag or "").strip().lower() for flag in risk_flags}
        crowding_warning = bool(market_context.get("crowding_warning"))
        history_ready = market_context.get("history_ready")
        crowded_long = bool(market_context.get("crowded_long"))
        crowded_short = bool(market_context.get("crowded_short"))
        squeeze_building = bool(market_context.get("squeeze_building"))
        flush_risk = bool(market_context.get("flush_risk"))
        basis_dislocation = bool(market_context.get("basis_dislocation"))
        flow_divergence = bool(market_context.get("flow_divergence"))
        order_flow_confirmed = bool(market_context.get("order_flow_confirmed"))
        funding_zscore = abs(float(market_context.get("funding_zscore") or 0.0))
        history_incomplete = "history_incomplete" in normalized_flags or history_ready is False
        direction_upper = str(direction or "").upper()

        if direction_upper == "LONG":
            if crowded_long or crowding_warning or "crowding_hot" in normalized_flags:
                penalty += 0.10
            if flush_risk or "distribution_risk" in normalized_flags:
                penalty += 0.06
            if basis_dislocation or "basis_dislocation" in normalized_flags:
                penalty += 0.05
            if flow_divergence or "flow_divergence" in normalized_flags:
                penalty += 0.04
            if history_incomplete:
                penalty += 0.03
            if funding_zscore >= 1.75:
                penalty += 0.02
        elif direction_upper == "SHORT":
            if squeeze_building or "squeeze_active" in normalized_flags:
                penalty += 0.10
            if crowded_short:
                penalty += 0.06
            if order_flow_confirmed:
                penalty += 0.06
            if basis_dislocation or "basis_dislocation" in normalized_flags:
                penalty += 0.04
            if flow_divergence or "flow_divergence" in normalized_flags:
                penalty += 0.03
            if history_incomplete:
                penalty += 0.03
            if funding_zscore >= 1.75:
                penalty += 0.02

        return min(float(confidence), penalty)

    def _apply_risk_gate(
        self,
        symbol: str,
        direction: str,
        confidence: float,
        market_data: pd.DataFrame,
    ) -> tuple[bool, str]:
        """Ask the risk gate whether to block the signal."""
        if direction == "FLAT":
            return False, ""
        try:
            market_features: Dict[str, Any] = {}
            if not market_data.empty and "atr" in market_data.columns:
                market_features["atr"] = float(market_data["atr"].iloc[-1])
            # RiskGate.evaluate returns (final_signal, reasons_list)
            final_signal, reasons = self._risk_gate.evaluate(
                symbol=symbol,
                proposed_signal=direction,
                market_features=market_features,
                record_state=False,
            )
            blocked = final_signal == "FLAT" and direction != "FLAT"
            if blocked:
                return True, "; ".join(reasons) if reasons else "blocked by risk gate"
        except Exception as exc:
            logger.debug(f"SignalAggregator: risk gate error: {exc}")
        return False, ""


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_ml_path = os.getenv("ML_SIGNAL_MODEL_PATH", _DEFAULT_ML_MODEL_PATH)
signal_aggregator = SignalAggregator(ml_model_path=_ml_path)
