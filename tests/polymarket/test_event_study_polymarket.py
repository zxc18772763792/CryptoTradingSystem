from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pandas as pd

from scripts.research import event_study_polymarket as study


def test_event_study_outputs_reports(monkeypatch, tmp_path: Path):
    async def fake_load_prices(exchange, symbol, timeframe, days):
        idx = pd.date_range("2026-01-01", periods=20, freq="5min")
        df = pd.DataFrame({
            "close": [100 + i for i in range(20)],
        }, index=idx)
        df["ret_1"] = df["close"].pct_change().shift(-1)
        df["ret_3"] = df["close"].pct_change(3).shift(-3)
        df["ret_6"] = df["close"].pct_change(6).shift(-6)
        df["ret_12"] = df["close"].pct_change(12).shift(-12)
        return df

    async def fake_get_features_range(symbol, since, until, timeframe):
        idx = pd.date_range("2026-01-01", periods=20, freq="5min")
        return [{"ts": ts.to_pydatetime(), "pm_global_risk": 0.7 if i % 4 == 0 else 0.2} for i, ts in enumerate(idx)]

    async def fake_init():
        return None

    async def fake_close():
        return None

    monkeypatch.setattr(study, "_load_prices", fake_load_prices)
    monkeypatch.setattr(study.pm_db, "get_features_range", fake_get_features_range)
    monkeypatch.setattr(study.pm_db, "init_pm_db", fake_init)
    monkeypatch.setattr(study.pm_db, "close_pm_db", fake_close)

    asyncio.run(study._main("BTCUSDT", 5, "5m", tmp_path))

    json_files = list(tmp_path.glob("event_study_polymarket_*.json"))
    md_files = list(tmp_path.glob("event_study_polymarket_*.md"))
    assert json_files and md_files
    payload = json.loads(json_files[0].read_text(encoding="utf-8"))
    assert payload["event_count"] > 0
