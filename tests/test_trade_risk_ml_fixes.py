from __future__ import annotations

import importlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.ai.ml_signal import MLSignalModel
from core.ai.ml_signal import FEATURE_COLS
from core.ml.pipeline import FEATURE_SET_VERSION
from core.risk.risk_manager import RiskManager


def test_allow_close_bypasses_entry_only_caps_during_daily_loss_halt(monkeypatch):
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
        leverage=2.0,
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
    ) is True

    assert manager.pre_trade_check(
        symbol="BTC/USDT",
        side="sell",
        strategy_name="s",
        account_equity=1000.0,
        order_value=50.0,
        leverage=10.0,
        allow_close=True,
    ) is True


def test_allow_close_does_not_bypass_non_daily_halt_but_bypasses_position_count(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")
    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: type(
            "PM",
            (),
            {
                "get_position_count": lambda self: 1,
                "get_all_positions": lambda self: [],
                "get_positions_by_strategy": lambda self, _strategy: [],
            },
        )(),
    )
    manager = RiskManager(use_persisted_overlay=False)
    manager.max_open_positions = 1
    manager._trading_halted = True
    manager._halt_reason = "manual-kill-switch"

    assert manager.pre_trade_check(
        symbol="BTC/USDT",
        side="sell",
        strategy_name="s",
        account_equity=1000.0,
        order_value=50.0,
        leverage=1.0,
        allow_close=True,
    ) is False

    manager._trading_halted = False

    assert manager.pre_trade_check(
        symbol="BTC/USDT",
        side="sell",
        strategy_name="s",
        account_equity=1000.0,
        order_value=50.0,
        leverage=1.0,
        allow_close=True,
    ) is True


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


def test_ml_signal_load_requires_manifest(monkeypatch, tmp_path: Path):
    model_path = tmp_path / "model.json"
    model_path.write_text("{}", encoding="utf-8")

    class FakeXGB:
        class XGBClassifier:
            def load_model(self, _path):
                raise AssertionError("model should not load without manifest")

    monkeypatch.setitem(importlib.import_module("sys").modules, "xgboost", FakeXGB)

    model = MLSignalModel(str(model_path))
    model.load()

    assert model.is_loaded() is False


def test_ml_signal_load_rejects_manifest_feature_mismatch(monkeypatch, tmp_path: Path):
    model_path = tmp_path / "model.json"
    model_path.write_text("{}", encoding="utf-8")
    model_path.with_suffix(".manifest.json").write_text(
        json.dumps({"feature_set_version": FEATURE_SET_VERSION, "feature_columns": ["rsi"]}),
        encoding="utf-8",
    )

    class FakeXGB:
        class XGBClassifier:
            def load_model(self, _path):
                raise AssertionError("model should not load with stale manifest")

    monkeypatch.setitem(importlib.import_module("sys").modules, "xgboost", FakeXGB)

    model = MLSignalModel(str(model_path))
    model.load()

    assert model.is_loaded() is False


def test_ml_signal_load_accepts_valid_manifest(monkeypatch, tmp_path: Path):
    model_path = tmp_path / "model.json"
    model_path.write_text("{}", encoding="utf-8")
    model_path.with_suffix(".manifest.json").write_text(
        json.dumps({"feature_set_version": FEATURE_SET_VERSION, "feature_columns": FEATURE_COLS}),
        encoding="utf-8",
    )

    class FakeClassifier:
        feature_importances_ = np.ones(len(FEATURE_COLS))

        def load_model(self, _path):
            self.loaded = True

    class FakeXGB:
        XGBClassifier = FakeClassifier

    monkeypatch.setitem(importlib.import_module("sys").modules, "xgboost", FakeXGB)

    model = MLSignalModel(str(model_path))
    model.load()

    assert model.is_loaded() is True
    assert model._feature_names == FEATURE_COLS
