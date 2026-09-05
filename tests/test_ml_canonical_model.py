from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.ai.ml_signal import MLSignalModel, build_feature_frame


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_canonical_ml_model_loads_and_runs_real_inference():
    model_path = REPO_ROOT / "models" / "ml_signal_xgb.json"
    model = MLSignalModel.load_from_path(str(model_path))

    assert model.is_loaded() is True

    rows = 100
    index = pd.date_range("2026-01-01", periods=rows, freq="1h")
    close = np.linspace(100.0, 120.0, rows) + np.sin(np.arange(rows) / 3.0)
    frame = pd.DataFrame(
        {
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": np.linspace(1000.0, 1200.0, rows),
        },
        index=index,
    )

    result = model.predict(build_feature_frame(frame), symbol="BTC/USDT")

    assert result.direction in {"LONG", "SHORT", "FLAT"}
    assert 0.0 <= result.long_prob <= 1.0
    assert 0.0 <= result.short_prob <= 1.0
    assert result.long_prob + result.short_prob == pytest.approx(1.0)
    assert result.confidence > 0.0
