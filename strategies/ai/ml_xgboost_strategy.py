"""Live ML strategy backed by the shared XGBoost directional model."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.ai.ml_signal import MLSignalModel, build_feature_frame
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _emit_close_signal(
    *,
    symbol: str,
    price: float,
    strategy_name: str,
    bias: int,
    confidence: float,
) -> Optional[Signal]:
    if bias > 0:
        signal_type = SignalType.CLOSE_LONG
    elif bias < 0:
        signal_type = SignalType.CLOSE_SHORT
    else:
        return None
    return Signal(
        symbol=symbol,
        signal_type=signal_type,
        price=price,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy_name,
        strength=max(0.1, min(1.0, confidence)),
        metadata={"reason": "ml_signal_flat"},
    )


class MLXGBoostStrategy(StrategyBase):
    """ML directional strategy using the shared XGBoost classifier."""

    def __init__(self, name: str = "ML_XGBoost", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "model_path": str(Path(__file__).resolve().parents[2] / "models" / "ml_signal_xgb.json"),
            "threshold": 0.55,
            "neutral_exit_enabled": True,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._model = MLSignalModel.load_from_path(
            str(self.params.get("model_path") or default_params["model_path"]),
            threshold=float(self.params.get("threshold", 0.55)),
        )
        self._last_bias: Dict[str, int] = {}

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or data.empty or len(data) < 50:
            return []

        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"
        current_price = float(data["close"].iloc[-1] or 0.0)
        if current_price <= 0:
            return []

        result = self._model.predict(build_feature_frame(data), symbol=symbol)
        confidence = float(result.confidence or 0.0)
        base_meta = {
            "direction": result.direction,
            "confidence": confidence,
            "long_prob": float(result.long_prob or 0.0),
            "short_prob": float(result.short_prob or 0.0),
            "model_version": str(result.model_version or ""),
        }

        if result.direction == "LONG":
            self._last_bias[symbol] = 1
            return [
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=datetime.now(timezone.utc),
                    strategy_name=self.name,
                    strength=max(0.1, min(1.0, confidence)),
                    stop_loss=current_price * (1 - float(self.params.get("stop_loss_pct", 0.025))),
                    take_profit=current_price * (1 + float(self.params.get("take_profit_pct", 0.06))),
                    metadata=base_meta,
                )
            ]

        if result.direction == "SHORT":
            self._last_bias[symbol] = -1
            return [
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=datetime.now(timezone.utc),
                    strategy_name=self.name,
                    strength=max(0.1, min(1.0, confidence)),
                    stop_loss=current_price * (1 + float(self.params.get("stop_loss_pct", 0.025))),
                    take_profit=current_price * (1 - float(self.params.get("take_profit_pct", 0.06))),
                    metadata=base_meta,
                )
            ]

        if bool(self.params.get("neutral_exit_enabled", True)):
            close_signal = _emit_close_signal(
                symbol=symbol,
                price=current_price,
                strategy_name=self.name,
                bias=int(self._last_bias.get(symbol, 0) or 0),
                confidence=confidence,
            )
            if close_signal is not None:
                self._last_bias.pop(symbol, None)
                return [close_signal]
        return []

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["open", "high", "low", "close", "volume"],
            "min_length": 60,
        }
