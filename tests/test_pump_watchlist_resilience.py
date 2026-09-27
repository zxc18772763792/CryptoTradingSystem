from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cached_mcaps_rescale_by_price_and_expire(tmp_path, monkeypatch):
    wl = _load("generate_pump_watchlist")
    monkeypatch.setattr(wl, "MCAP_CACHE", tmp_path / "mcap_cache.json")
    monkeypatch.setattr(wl, "_get_json", lambda url, params=None: [{"symbol": "AAAUSDT", "lastPrice": "2.0"}])
    wl._save_mcap_cache({"AAA": 1e7, "GONE": 5e6}, {"AAA": 1.0, "GONE": 1.0})
    mcap, price = wl._cached_mcaps()
    assert mcap == {"AAA": 2e7} and price == {"AAA": 2.0}  # doubled price -> doubled mcap; delisted coin dropped

    stale = {"saved_at": (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(), "mcap": {"AAA": 1e7}, "price": {"AAA": 1.0}}
    (tmp_path / "mcap_cache.json").write_text(json.dumps(stale), encoding="utf-8")
    assert wl._cached_mcaps() == ({}, {})


def test_binance_oi_fallback_needs_archive_for_30d_history(monkeypatch):
    wl = _load("generate_pump_watchlist")
    monkeypatch.setattr(wl.time, "sleep", lambda s: None)
    start = pd.Timestamp("2026-10-01")
    rows = [{"timestamp": int((start + pd.Timedelta(days=i)).timestamp() * 1000), "sumOpenInterestValue": "100"} for i in range(30)]
    monkeypatch.setattr(wl, "_get_json", lambda url, params=None: rows)
    assert wl._binance_oi_fallback("AAAUSDT", None) is None  # 30 days alone cannot give oi_chg_30d
    archived = pd.Series(50.0, index=pd.date_range(start - pd.Timedelta(days=10), periods=12, freq="D"))
    merged = wl._binance_oi_fallback("AAAUSDT", archived)
    assert len(merged) == 40 and merged.loc[start] == 100.0  # Binance wins on overlap, archive fills the past


def test_holder_snapshot_universe_includes_watchlist(tmp_path, monkeypatch):
    oc = _load("snapshot_onchain_features")
    latest = tmp_path / "latest.json"
    latest.write_text(json.dumps({"full_ranking": [{"base": "newcoin"}, {"base": "AAA"}]}), encoding="utf-8")
    monkeypatch.setattr(oc, "WATCHLIST_LATEST", latest)
    bases = oc.universe_bases()
    assert "NEWCOIN" in bases and len(bases) > 50  # manifest coins kept, watchlist coins added
