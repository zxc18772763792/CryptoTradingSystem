from __future__ import annotations

import asyncio
import time

import pandas as pd

from web.api import data as data_api


def test_get_multi_assets_overview_loads_symbol_frames_concurrently(monkeypatch):
    async def fake_load_symbol_df(*, exchange: str, symbol: str, timeframe: str):
        await asyncio.sleep(0.05)
        return pd.DataFrame(
            {
                "close": [100.0, 101.0, 102.0, 103.0],
                "volume": [10.0, 11.0, 12.0, 13.0],
            },
            index=pd.date_range("2026-04-18", periods=4, freq="4h"),
        )

    monkeypatch.setattr(
        data_api,
        "_research_retired_filter",
        lambda **kwargs: (["AAA/USDT", "BBB/USDT", "CCC/USDT"], []),
    )
    monkeypatch.setattr(data_api, "_load_symbol_df", fake_load_symbol_df)

    started_at = time.perf_counter()
    payload = asyncio.run(
        data_api.get_multi_assets_overview(
            exchange="binance",
            symbols="AAA/USDT,BBB/USDT,CCC/USDT",
            timeframe="4h",
            lookback=120,
            exclude_retired=True,
        )
    )
    elapsed = time.perf_counter() - started_at

    assert payload["count"] == 3
    assert len(payload["assets"]) == 3
    assert elapsed < 0.30
