from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest

from core.ai.ml_signal import MLSignalModel
from core.risk.risk_manager import RiskManager


def test_allow_close_bypasses_daily_halt_and_leverage_cap_but_not_size_cap(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")
    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: type(
            "PM",
            (),
            {
                "get_position_count": lambda self: 0,
                "get_all_positions": lambda self: [],
                "get_positions_by_strategy": lambda self, _strategy: [],
            },
        )(),
    )
    manager = RiskManager(use_persisted_overlay=False)
    manager.max_leverage = 3.0
    manager.max_position_size = 0.1
    manager._trading_halted = True
    manager._halt_reason = "daily-loss-halt"

    assert manager.pre_trade_check(
        symbol="BTC/USDT",
        side="sell",
        strategy_name="s",
        account_equity=1000.0,
        order_value=50.0,
        leverage=10.0,
        allow_close=True,
    ) is True

    assert manager.pre_trade_check(
        symbol="BTC/USDT",
        side="sell",
        strategy_name="s",
        account_equity=1000.0,
        order_value=500.0,
        leverage=10.0,
        allow_close=True,
    ) is False


class _FakeModel:
    feature_importances_ = np.array([1.0, 0.0])

    def predict_proba(self, _row):
        return np.array([[0.1, 0.9]])


def _loaded_model(feature_names):
    model = MLSignalModel(model_path="unused", threshold=0.55)
    model._model = _FakeModel()
    model._feature_names = list(feature_names)
    return model


def test_ml_signal_missing_feature_returns_flat_without_zero_fill():
    model = _loaded_model(["rsi", "macd"])

    result = model.predict(pd.DataFrame([{"rsi": 55.0}]), symbol="BTC/USDT")

    assert result.direction == "FLAT"
    assert result.confidence == pytest.approx(0.0)


def test_ml_signal_nan_feature_returns_flat_without_zero_fill():
    model = _loaded_model(["rsi", "macd"])

    result = model.predict(pd.DataFrame([{"rsi": 55.0, "macd": np.nan}]), symbol="BTC/USDT")

    assert result.direction == "FLAT"
    assert result.confidence == pytest.approx(0.0)


def test_ml_signal_valid_features_still_predicts():
    model = _loaded_model(["rsi", "macd"])

    result = model.predict(pd.DataFrame([{"rsi": 55.0, "macd": 0.2}]), symbol="BTC/USDT")

    assert result.direction == "LONG"
    assert result.long_prob == pytest.approx(0.9)
