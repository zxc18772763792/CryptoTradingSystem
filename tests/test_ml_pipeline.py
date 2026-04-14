from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.ai import ml_signal
from core.ml import pipeline


def _sample_ohlcv(rows: int = 160) -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=rows, freq="H")
    base = np.linspace(100.0, 130.0, rows)
    return pd.DataFrame(
        {
            "open": base + 0.2,
            "high": base + 1.0,
            "low": base - 1.0,
            "close": base + np.sin(np.linspace(0, 8, rows)),
            "volume": np.linspace(1000.0, 2000.0, rows),
        },
        index=index,
    )


def test_diagnose_environment_reports_probe_results(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_find_spec(name: str):
        return object() if name in {"xgboost", "sklearn"} else None

    def fake_import_module(name: str):
        return SimpleNamespace(__version__="9.9.9")

    monkeypatch.delitem(sys.modules, "xgboost", raising=False)
    monkeypatch.delitem(sys.modules, "sklearn", raising=False)
    monkeypatch.setattr(pipeline.importlib.util, "find_spec", fake_find_spec)
    monkeypatch.setattr(pipeline.importlib, "import_module", fake_import_module)

    diagnostics = pipeline.diagnose_environment()

    assert diagnostics.xgboost.available is True
    assert diagnostics.sklearn.available is True
    assert diagnostics.xgboost.version == "9.9.9"


def test_build_dataset_and_split_are_chronological() -> None:
    dataset = pipeline.build_dataset(_sample_ohlcv(), forward_bars=4, min_rows=20)
    split = pipeline.split_dataset(dataset, test_size=0.2)

    assert dataset.feature_columns == pipeline.FEATURE_COLUMNS
    assert split.train_samples > 0
    assert split.test_samples > 0
    assert split.X_train.index.max() < split.X_test.index.min()


def test_quality_gate_rejects_weak_metrics() -> None:
    metrics = {
        "auc": 0.48,
        "train_samples": 40,
        "test_samples": 10,
        "classification_report": {"1": {"precision": 0.2, "recall": 0.1, "f1-score": 0.13}},
    }

    gate = pipeline.apply_quality_gate(metrics)

    assert gate.passed is False
    assert any("auc below gate" in reason for reason in gate.reasons)
    assert any("train sample count too low" in reason for reason in gate.reasons)


def test_build_manifest_and_save_model_artifacts(tmp_path: Path) -> None:
    created_at = pd.Timestamp("2026-04-13T08:00:00Z").to_pydatetime()
    manifest = pipeline.build_manifest(
        model_id="mlsig_btcusdt_1h_20260413T080000000000Z_deadbeef",
        feature_set_version=pipeline.FEATURE_SET_VERSION,
        training_window={
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "lookback_rows": 160,
            "forward_bars": 4,
            "start_at": "2026-01-01T00:00:00",
            "end_at": "2026-01-07T15:00:00",
        },
        symbol="BTC/USDT",
        timeframe="1h",
        metrics={"auc": 0.61},
        created_at=created_at,
        source_commit="deadbeefcafebabe",
        environment={"xgboost": {"available": True}},
    )

    class DummyModel:
        def save_model(self, path: str) -> None:
            Path(path).write_text("dummy-model", encoding="utf-8")

    artifact_dir = pipeline.save_model_artifacts(
        output_root=tmp_path,
        model=DummyModel(),
        manifest=manifest,
        metrics={"auc": 0.61},
        feature_importances={"close": 0.7},
    )

    assert artifact_dir.exists()
    assert (artifact_dir / pipeline.MODEL_FILE_NAME).exists()
    assert (artifact_dir / pipeline.MANIFEST_FILE_NAME).exists()
    assert (artifact_dir / pipeline.METRICS_FILE_NAME).exists()
    assert (artifact_dir / pipeline.FEATURE_IMPORTANCES_FILE_NAME).exists()


def test_inference_feature_builder_matches_training_pipeline() -> None:
    sample = _sample_ohlcv()

    training_features = pipeline.build_feature_frame(sample)
    inference_features = ml_signal.build_feature_frame(sample)

    pd.testing.assert_frame_equal(inference_features, training_features)
    assert ml_signal.FEATURE_COLS == pipeline.FEATURE_COLUMNS
