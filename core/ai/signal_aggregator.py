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

from core.ai.ml_signal import MLSignalModel, MLSignalResult
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
            "timestamp": self.timestamp.isoformat(),
        }


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

_DEFAULT_ML_MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "ml_signal_xgb.json"
)


class SignalAggregator:
    """Weighted-vote aggregator over LLM, ML, and factor signals."""

    WEIGHTS: Dict[str, float] = {"llm": 0.40, "ml": 0.35, "factor": 0.25}

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
        market_data: pd.DataFrame,
    ) -> AggregatedSignal:
        """Compute weighted signal for *symbol* using latest *market_data*."""
        components: Dict[str, Any] = {}

        # ---- 1. LLM signal ----
        llm_direction, llm_conf = await self._get_llm_signal(symbol, market_data)
        components["llm"] = {
            "direction": llm_direction,
            "confidence": round(llm_conf, 6),
            "weight": self.WEIGHTS["llm"],
        }

        # ---- 2. ML signal ----
        ml_direction, ml_conf = self._get_ml_signal(symbol, market_data)
        components["ml"] = {
            "direction": ml_direction,
            "confidence": round(ml_conf, 6),
            "weight": self.WEIGHTS["ml"],
        }

        # ---- 3. Factor signal ----
        factor_direction, factor_conf = self._get_factor_signal(market_data)
        components["factor"] = {
            "direction": factor_direction,
            "confidence": round(factor_conf, 6),
            "weight": self.WEIGHTS["factor"],
        }

        # ---- 4. Weighted vote ----
        direction, confidence = self._weighted_vote(
            [
                (llm_direction, llm_conf, self.WEIGHTS["llm"]),
                (ml_direction, ml_conf, self.WEIGHTS["ml"] if self._ml_model.is_loaded() else 0.0),
                (factor_direction, factor_conf, self.WEIGHTS["factor"]),
            ]
        )

        # ---- 5. Risk-gate filter ----
        blocked, risk_reason = self._apply_risk_gate(symbol, direction, confidence, market_data)

        requires_approval = (
            blocked or confidence < self._high_conf_threshold
        )

        return AggregatedSignal(
            symbol=symbol,
            direction="FLAT" if blocked else direction,
            confidence=round(confidence, 6),
            components=components,
            requires_approval=requires_approval,
            blocked_by_risk=blocked,
            risk_reason=risk_reason,
        )

    # ------------------------------------------------------------------
    # Sub-signals
    # ------------------------------------------------------------------

    async def _get_llm_signal(
        self,
        symbol: str,
        market_data: pd.DataFrame,
    ) -> tuple[str, float]:
        """Fetch latest news/event signal from signal_engine."""
        try:
            from core.ai.signal_engine import generate_signal  # noqa: PLC0415

            market_features: Dict[str, Any] = {}
            if not market_data.empty:
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
        market_data: pd.DataFrame,
    ) -> tuple[str, float]:
        """Run ML model on the feature DataFrame."""
        if not self._ml_model.is_loaded() or market_data.empty:
            return "FLAT", 0.0
        try:
            from scripts.train_ml_signal import compute_features  # noqa: PLC0415

            features = compute_features(market_data)
            result: MLSignalResult = self._ml_model.predict(features, symbol=symbol)
            return result.direction, result.confidence
        except Exception as exc:
            logger.debug(f"SignalAggregator: ML signal failed for {symbol}: {exc}")
            return "FLAT", 0.0

    def _get_factor_signal(
        self,
        market_data: pd.DataFrame,
    ) -> tuple[str, float]:
        """Rule-based signal using RSI, EMA trend, and momentum."""
        if market_data.empty or len(market_data) < 30:
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
                fg = fear_greed_collector._history[0] if fear_greed_collector._history else None
                if fg:
                    if fg.is_extreme_fear and direction == "LONG":
                        confidence = min(1.0, confidence + 0.08)
                    elif fg.is_extreme_greed and direction == "SHORT":
                        confidence = min(1.0, confidence + 0.08)
            except Exception:
                pass

            return direction, confidence
        except Exception as exc:
            logger.debug(f"SignalAggregator: factor signal failed: {exc}")
            return "FLAT", 0.0

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

        winner = max(score, key=lambda k: score[k])
        raw_confidence = score[winner] / total_weight
        return winner, min(1.0, raw_confidence)

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
