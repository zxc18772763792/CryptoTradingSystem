from __future__ import annotations

import importlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.ai import ml_signal
from core.ai.ml_signal import FEATURE_COLS, MLSignalModel, build_feature_frame, model_usability
from core.ml import pipeline

PASSED = {"passed": True, "reasons": [], "thresholds": {}}


def _ohlcv(rows=300, freq="15min", scale=1.0, seed=0):
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, rows))) * scale
    index = pd.date_range("2026-01-01", periods=rows, freq=freq, tz="UTC")
    return pd.DataFrame({"open": close * (1 + rng.normal(0, 0.002, rows)), "high": close * 1.01,
                         "low": close * 0.99, "close": close, "volume": rng.uniform(1e3, 2e3, rows) / scale},
                        index=index)


def test_v2_features_do_not_depend_on_price_level():
    # The v1 failure: same path, different price level -> opposite signal.
    base = build_feature_frame(_ohlcv())
    scaled = build_feature_frame(_ohlcv(scale=30_000.0))
    pd.testing.assert_frame_equal(base, scaled, check_exact=False, rtol=1e-6, atol=1e-9)
    assert not {"close", "open", "high", "low", "volume", "ema_fast", "bb_upper", "atr", "macd"} & set(FEATURE_COLS)


def _manifest(path: Path, **extra):
    payload = {"feature_set_version": pipeline.FEATURE_SET_VERSION, "feature_columns": FEATURE_COLS, **extra}
    path.with_suffix(".manifest.json").write_text(json.dumps(payload), encoding="utf-8")


class _FakeClassifier:
    feature_importances_ = np.ones(len(FEATURE_COLS))

    def load_model(self, _path):
        pass

    def predict_proba(self, row):
        return np.array([[0.2, 0.8]])


class _FakeXGB:
    XGBClassifier = _FakeClassifier


def _load(tmp_path, monkeypatch, **manifest):
    model_path = tmp_path / "model.json"
    model_path.write_text("{}", encoding="utf-8")
    _manifest(model_path, **manifest)
    monkeypatch.setitem(importlib.import_module("sys").modules, "xgboost", _FakeXGB)
    model = MLSignalModel(str(model_path))
    model.load()
    return model, model_path


def test_loader_refuses_a_model_without_a_passed_gate(tmp_path, monkeypatch):
    model, path = _load(tmp_path, monkeypatch, metrics={"quality_gate": {"passed": False, "reasons": ["f1"]}})
    assert model.is_loaded() is False and "quality gate" in model.load_error
    assert model_usability(str(path))[0] is False
    model, path = _load(tmp_path, monkeypatch)  # a stub sidecar with no gate record at all
    assert model.is_loaded() is False


def test_timeframe_and_single_coin_scope_fail_closed(tmp_path, monkeypatch):
    model, _ = _load(tmp_path, monkeypatch, quality_gate=PASSED, timeframe="1h", symbol="BTC/USDT",
                     training_symbols=["BTC/USDT"], symbol_scope="single")
    assert model.is_loaded()
    hourly = build_feature_frame(_ohlcv(freq="1h")).dropna()
    assert model.predict(hourly, symbol="BTC/USDT").direction == "LONG"
    quarter = build_feature_frame(_ohlcv(freq="15min")).dropna()
    assert model.predict(quarter, symbol="BTC/USDT").direction == "FLAT"
    assert model.last_rejection.startswith("timeframe_mismatch")
    assert model.predict(hourly, symbol="SOL/USDT:USDT").direction == "FLAT"
    assert model.last_rejection.startswith("symbol_outside_single_coin_model")


def test_pooled_model_scores_any_coin_on_its_timeframe(tmp_path, monkeypatch):
    model, path = _load(tmp_path, monkeypatch, quality_gate=PASSED, timeframe="15m", symbol="pooled",
                        training_symbols=["BTC/USDT", "ETH/USDT"], symbol_scope="pooled")
    quarter = build_feature_frame(_ohlcv(freq="15min")).dropna()
    assert model.predict(quarter, symbol="DOGE/USDT").direction == "LONG" and model.last_rejection == ""
    assert model_usability(str(path)) == (True, "")


def test_gate_rejects_one_sided_predictions_overall_and_per_coin():
    good = {"auc": 0.6, "train_samples": 1000, "test_samples": 500, "prediction_threshold": 0.55,
            "classification_report": {"1": {"precision": 0.6, "recall": 0.6, "f1-score": 0.6}}}
    assert pipeline.apply_quality_gate({**good, "test_long_prob": [0.3, 0.7] * 50}).passed
    one_sided = pipeline.apply_quality_gate({**good, "test_long_prob": [0.9] * 95 + [0.3] * 5})
    assert not one_sided.passed and any("one-sided" in r for r in one_sided.reasons)
    per_coin = pipeline.apply_quality_gate({**good, "test_long_prob": [0.3, 0.7] * 50,
                                            "per_symbol_one_side_share": {"DOGE/USDT": 0.97}})
    assert not per_coin.passed and any("DOGE/USDT" in r for r in per_coin.reasons)


def test_pooled_split_is_by_time_with_a_label_purge():
    frames = {f"C{i}/USDT": _ohlcv(rows=400, seed=i) for i in range(3)}
    dataset, symbols = pipeline.build_pooled_dataset(frames, forward_bars=4, min_rows_per_symbol=50)
    split = pipeline.split_pooled_by_time(dataset, test_size=0.25, bar_seconds=900)
    ts = pd.to_datetime(dataset.frame["_ts"], utc=True)
    train_end, test_start = ts.loc[split.X_train.index].max(), ts.loc[split.X_test.index].min()
    assert test_start - train_end > pd.Timedelta(minutes=15 * 4)  # no training label reaches the test period
    assert set(symbols.loc[split.X_test.index]) == set(frames)  # every coin appears in the test period
    # labels never mix coins: each row's label is its own coin's forward move
    one = dataset.frame[dataset.frame["_symbol"] == "C0/USDT"].set_index("_ts")
    fwd = frames["C0/USDT"]["close"].shift(-4) / frames["C0/USDT"]["close"] - 1
    assert (one["_label"] == (fwd.reindex(one.index) > 0).astype(int)).all()


def test_aggregator_treats_a_refused_scope_as_no_vote(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()

    async def _no_llm(symbol, market_data):
        return "FLAT", 0.0

    monkeypatch.setattr(agg, "_get_llm_signal", _no_llm)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_factor_signal", lambda market_data: ("SHORT", 0.64))
    monkeypatch.setattr(agg, "_get_derivatives_signal",
                        lambda symbol: asyncio.sleep(0, result=("FLAT", 0.0, {"available": False, "reason": "test"})))
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    # loaded, but it refused these bars (e.g. 1h bars into a 15m model)
    agg._ml_model = SimpleNamespace(is_loaded=lambda: True, load_error="",
                                    last_rejection="timeframe_mismatch(trained 15m, bars 3600s)")

    result = asyncio.run(agg.aggregate("SOL/USDT", _ohlcv(freq="1h")))

    assert result.components["ml"]["available"] is False
    assert result.components["ml"]["effective_weight"] == pytest.approx(0.0)
    assert result.components["ml"]["reason"].startswith("timeframe_mismatch")
    assert result.direction == "SHORT"  # a refused model must not dilute the other components with a FLAT vote
