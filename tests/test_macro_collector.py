from __future__ import annotations

import asyncio

import pandas as pd
from unittest.mock import AsyncMock


def _monthly_series(series_name: str, year_ago: float, latest: float) -> pd.Series:
    dates = pd.date_range("2025-03-01", periods=13, freq="MS")
    values = [float(year_ago)] * 12 + [float(latest)]
    return pd.Series(values, index=dates, name=series_name, dtype=float)


def test_load_macro_snapshot_recomputes_legacy_yoy_series(tmp_path, monkeypatch):
    from core.data import macro_collector as module

    monkeypatch.setattr(module, "_CACHE_DIR", tmp_path)

    _monthly_series("CPIAUCSL", 100.0, 110.0).to_frame().to_parquet(tmp_path / "cpi_yoy.parquet")
    _monthly_series("PPIACO", 100.0, 115.0).to_frame().to_parquet(tmp_path / "ppi_yoy.parquet")
    _monthly_series("M1SL", 100.0, 120.0).to_frame().to_parquet(tmp_path / "m1_yoy.parquet")
    _monthly_series("M2SL", 100.0, 108.0).to_frame().to_parquet(tmp_path / "m2_yoy.parquet")
    pd.Series({"2026-03-01": 3.64}, name="FEDFUNDS", dtype=float).to_frame().to_parquet(tmp_path / "fed_rate.parquet")
    pd.Series({"2026-03-01": 1.0}, name="cn_cpi_yoy", dtype=float).to_frame().to_parquet(tmp_path / "cn_cpi_yoy.parquet")
    pd.Series({"2026-03-01": 0.5}, name="cn_ppi_yoy", dtype=float).to_frame().to_parquet(tmp_path / "cn_ppi_yoy.parquet")
    pd.Series({"2026-03-01": 1.2}, name="cn_m1_yoy", dtype=float).to_frame().to_parquet(tmp_path / "cn_m1_yoy.parquet")
    pd.Series({"2026-03-01": 7.4}, name="cn_m2_yoy", dtype=float).to_frame().to_parquet(tmp_path / "cn_m2_yoy.parquet")

    snapshot = module.load_macro_snapshot()

    assert snapshot["fed_rate"] == 3.64
    assert snapshot["cpi_yoy"] == 10.0
    assert snapshot["ppi_yoy"] == 15.0
    assert snapshot["m1_yoy"] == 20.0
    assert snapshot["m2_yoy"] == 8.0
    assert snapshot["ppi_cpi_gap"] == 5.0
    assert snapshot["m1_m2_gap"] == 12.0
    assert snapshot["cn_cpi_yoy"] == 1.0
    assert snapshot["cn_ppi_yoy"] == 0.5
    assert snapshot["cn_m1_yoy"] == 1.2
    assert snapshot["cn_m2_yoy"] == 7.4
    assert snapshot["cn_ppi_cpi_gap"] == -0.5
    assert snapshot["cn_m1_m2_gap"] == -6.2


def test_fetch_fred_macro_persists_scissors_spread(tmp_path, monkeypatch):
    from core.data import macro_collector as module

    monkeypatch.setattr(module, "_CACHE_DIR", tmp_path)

    async def fake_fetch(series_id: str, api_key: str, days: int = 730):
        mapping = {
            "FEDFUNDS": pd.Series({"2026-03-01": 3.50}, name="FEDFUNDS", dtype=float),
            "CPIAUCSL": _monthly_series("CPIAUCSL", 200.0, 212.0),
            "PPIACO": _monthly_series("PPIACO", 200.0, 218.0),
            "M1SL": _monthly_series("M1SL", 300.0, 330.0),
            "M2SL": _monthly_series("M2SL", 300.0, 315.0),
        }
        return mapping.get(series_id)

    monkeypatch.setattr(module, "_fetch_fred_series", fake_fetch)

    result = asyncio.run(module._fetch_fred_macro("demo-key"))

    assert result["fed_rate"] == 3.5
    assert result["cpi_yoy"] == 6.0
    assert result["ppi_yoy"] == 9.0
    assert result["m1_yoy"] == 10.0
    assert result["m2_yoy"] == 5.0
    assert result["ppi_cpi_gap"] == 3.0
    assert result["m1_m2_gap"] == 5.0

    cached_gap = pd.read_parquet(tmp_path / "ppi_cpi_gap.parquet")
    assert float(cached_gap.iloc[-1, 0]) == 3.0
    cached_liquidity_gap = pd.read_parquet(tmp_path / "m1_m2_gap.parquet")
    assert float(cached_liquidity_gap.iloc[-1, 0]) == 5.0


def test_fetch_china_macro_parses_official_sources(tmp_path, monkeypatch):
    from core.data import macro_collector as module

    monkeypatch.setattr(module, "_CACHE_DIR", tmp_path)

    nbs_index = """
    <ul>
      <li><a title="Consumer Price Index in March 2026" href="./202604/t20260413_1963288.html">Consumer Price Index in March 2026</a></li>
      <li><a title="Industrial Producer Price Indexes in March 2026" href="./202604/t20260413_1963289.html">Industrial Producer Price Indexes in March 2026</a></li>
    </ul>
    """
    cpi_page = """
    <html>
      <head><meta name="PubDate" content="2026/04/11 09:30"></head>
      <body>Consumer Price Index -0.7 1.0 0.9</body>
    </html>
    """
    ppi_page = """
    <html>
      <head><meta name="PubDate" content="2026/04/11 09:30"></head>
      <body>I. Producer Price Indexes for Industrial Products 1.0 0.5 -0.6</body>
    </html>
    """
    pboc_index = """
    <ul>
      <li><a href="/en/3688247/3688978/3709137/2026031614261747241/index.html" title="Financial Statistics Report (February 2026)">Financial Statistics Report (February 2026)</a></li>
      <li><a href="/en/3688247/3688978/3709137/2026030216043025426/index.html" title="Financial Statistics Report (January 2026)">Financial Statistics Report (January 2026)</a></li>
    </ul>
    """
    pboc_page = """
    <html>
      <head><meta name="PubDate" content="2026-03-13"></head>
      <body>
        <table>
          <tr><td>M2 Balances</td></tr>
          <tr><td>YOY Growth Rates</td><td>7.0%</td><td>7.4%</td></tr>
        </table>
        <table>
          <tr><td>M1 Balances</td></tr>
          <tr><td>YOY Growth Rates</td><td>-0.3%</td><td>1.2%</td></tr>
        </table>
      </body>
    </html>
    """

    html_map = {
        module._NBS_RELEASE_INDEX_URL: nbs_index,
        "https://www.stats.gov.cn/english/PressRelease/202604/t20260413_1963288.html": cpi_page,
        "https://www.stats.gov.cn/english/PressRelease/202604/t20260413_1963289.html": ppi_page,
        module._PBOC_REPORT_INDEX_URL: pboc_index,
        "https://www.pbc.gov.cn/en/3688247/3688978/3709137/2026031614261747241/index.html": pboc_page,
    }

    monkeypatch.setattr(module, "_request_text", lambda url: html_map.get(url))

    result = asyncio.run(module._fetch_china_macro())

    assert result["cn_cpi_yoy"] == 1.0
    assert result["cn_ppi_yoy"] == 0.5
    assert result["cn_ppi_cpi_gap"] == -0.5
    assert result["cn_m1_yoy"] == 1.2
    assert result["cn_m2_yoy"] == 7.4
    assert result["cn_m1_m2_gap"] == -6.2

    cached_gap = pd.read_parquet(tmp_path / "cn_ppi_cpi_gap.parquet")
    assert float(cached_gap.iloc[-1, 0]) == -0.5
    cached_liquidity_gap = pd.read_parquet(tmp_path / "cn_m1_m2_gap.parquet")
    assert float(cached_liquidity_gap.iloc[-1, 0]) == -6.2


def test_update_macro_cache_uses_yahoo_chart_fallback_for_missing_yfinance(tmp_path, monkeypatch):
    from core.data import macro_collector as module

    monkeypatch.setattr(module, "_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        module,
        "_fetch_yfinance_macro",
        AsyncMock(return_value={"vix": None, "dxy": None, "tnx_10y": None}),
    )
    monkeypatch.setattr(
        module,
        "_fetch_yahoo_chart_macro",
        AsyncMock(return_value={"vix": 21.5, "dxy": 103.2, "tnx_10y": 4.11}),
    )
    async def fake_fetch_china_macro():
        module._write_snapshot_value("cn_cpi_yoy", 1.0)
        module._write_snapshot_value("cn_ppi_yoy", 0.5)
        module._write_snapshot_value("cn_ppi_cpi_gap", -0.5)
        return {"cn_cpi_yoy": 1.0, "cn_ppi_yoy": 0.5, "cn_ppi_cpi_gap": -0.5}

    monkeypatch.setattr(module, "_fetch_china_macro", fake_fetch_china_macro)
    monkeypatch.setattr(module, "_api_key", lambda: "")

    updated = asyncio.run(module.update_macro_cache())
    snapshot = module.load_macro_snapshot()

    assert updated["vix"] == 1
    assert updated["dxy"] == 1
    assert updated["tnx_10y"] == 1
    assert updated["cn_cpi_yoy"] == 1
    assert updated["cn_ppi_yoy"] == 1
    assert updated["cn_ppi_cpi_gap"] == 1
    assert snapshot["vix"] == 21.5
    assert snapshot["dxy"] == 103.2
    assert snapshot["tnx_10y"] == 4.11
    assert snapshot["cn_cpi_yoy"] == 1.0
    assert snapshot["cn_ppi_yoy"] == 0.5
    assert snapshot["cn_ppi_cpi_gap"] == -0.5
