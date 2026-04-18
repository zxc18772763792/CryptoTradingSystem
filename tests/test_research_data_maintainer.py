import asyncio
import importlib.util
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from unittest.mock import AsyncMock


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    script_path = REPO_ROOT / "scripts" / "maintain_research_universe_data.py"
    spec = importlib.util.spec_from_file_location("maintain_research_universe_data_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_refresh_recent_1s_symbol_normalizes_naive_latest_timestamp(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "_has_recent_kline", AsyncMock(return_value=False))
    monkeypatch.setattr(
        module.data_storage,
        "load_klines_from_parquet",
        AsyncMock(
            return_value=pd.DataFrame(
                {"close": [1.0]},
                index=pd.DatetimeIndex([pd.Timestamp("2026-04-15 07:00:00")]),
            )
        ),
    )
    monkeypatch.setattr(
        module,
        "download_binance_1s_daily_archive",
        lambda *_args, **_kwargs: SimpleNamespace(total_rows=0, days_processed=0),
    )

    captured = {}

    def fake_fetch(symbol, start_time, end_time, limit=1000, timeout=15):
        captured["symbol"] = symbol
        captured["start_time"] = start_time
        captured["end_time"] = end_time
        ts = pd.DatetimeIndex([pd.Timestamp(start_time).tz_convert("UTC").tz_localize(None)])
        return pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1.0],
            },
            index=ts,
        )

    monkeypatch.setattr(module, "_fetch_binance_public_1s_window_sync", fake_fetch)
    monkeypatch.setattr(module.second_level_backfill_manager, "_save_parts", lambda *_args, **_kwargs: 1)

    result = asyncio.run(module._refresh_recent_1s_symbol("binance", "BTC/USDT", 1))

    assert result["saved_rows"] == 1
    assert captured["symbol"] == "BTC/USDT"
    assert captured["start_time"].tzinfo == timezone.utc
    assert captured["start_time"] <= captured["end_time"]
    assert result["recent_window_start"].endswith("+00:00")
