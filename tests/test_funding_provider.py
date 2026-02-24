from pathlib import Path

import pandas as pd

from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider


def test_funding_provider_merge_save_load_align(tmp_path: Path):
    provider = FundingRateProvider(
        FundingProviderConfig(cache_dir=str(tmp_path / "funding_cache"), default_rate=0.0)
    )
    idx = pd.to_datetime(
        [
            "2025-01-01 00:00:00",
            "2025-01-01 08:00:00",
            "2025-01-01 16:00:00",
        ]
    )
    s = pd.Series([0.0001, 0.0002, -0.0001], index=idx)
    provider.merge_series("BTC/USDT", s, save=True)

    p = provider._cache_path("BTC/USDT")
    assert p.exists()

    provider2 = FundingRateProvider(FundingProviderConfig(cache_dir=str(tmp_path / "funding_cache")))
    loaded = provider2.load_local_cache("BTC/USDT", required=True)
    assert len(loaded) == 3

    target_idx = pd.date_range("2025-01-01 00:00:00", periods=6, freq="4h")
    aligned = provider2.align_to_index("BTC/USDT", target_idx)
    assert len(aligned) == 6
    assert float(aligned.iloc[0]) == 0.0001
    assert float(aligned.iloc[1]) == 0.0001
    assert float(aligned.iloc[2]) == 0.0002


def test_attach_to_ohlcv_df_adds_funding_column(tmp_path: Path):
    provider = FundingRateProvider(FundingProviderConfig(cache_dir=str(tmp_path / "funding_cache")))
    fidx = pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 08:00:00"])
    provider.merge_series("ETH/USDT", pd.Series([0.0001, -0.0002], index=fidx), save=False)

    idx = pd.date_range("2025-01-01 00:00:00", periods=6, freq="4h")
    df = pd.DataFrame(
        {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        index=idx,
    )
    out = provider.attach_to_ohlcv_df(df, symbol="ETH/USDT")
    assert "funding_rate" in out.columns
    assert float(out["funding_rate"].iloc[0]) == 0.0001
    assert float(out["funding_rate"].iloc[2]) == -0.0002

