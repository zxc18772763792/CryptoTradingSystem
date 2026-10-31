from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from core.research import coinglass_monthly_universe as monthly
from core.research import coinglass_daily_store as store
from core.research import retirement, tracker_registry

NOW = datetime(2026, 10, 31, 2, tzinfo=timezone.utc)


class Binance:
    def __init__(self):
        self.calls = 0
        self.symbols = [dict(symbol=f"C{i:02d}USDT", contractType="PERPETUAL", quoteAsset="USDT", status="TRADING") for i in range(40)]

    async def get(self, url):
        self.calls += 1
        return type("Response", (), {"raise_for_status": lambda _: None,
                                     "json": lambda _: {"symbols": self.symbols}})()


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(monthly, "SNAPSHOT_DIR", tmp_path / "universes")
    monkeypatch.setattr(monthly, "_now", lambda: NOW)

    async def counts(perps, as_of):
        return {b: monthly.MIN_BARS for b in perps}

    monkeypatch.setattr(monthly, "history_counts", counts)
    return Binance()


def test_monthly_snapshot_is_immutable_and_next_month_records_changes(env, monkeypatch):
    first = asyncio.run(monthly.refresh(env, NOW))
    original = (monthly.SNAPSHOT_DIR / "2026-10.json").read_bytes()
    env.symbols = env.symbols[1:] + [dict(symbol="NEWUSDT", contractType="PERPETUAL", quoteAsset="USDT", status="TRADING")]
    assert asyncio.run(monthly.refresh(env, NOW)) == first
    assert env.calls == 1
    assert (monthly.SNAPSHOT_DIR / "2026-10.json").read_bytes() == original
    november = datetime(2026, 11, 1, 0, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(monthly, "_now", lambda: november)
    second = asyncio.run(monthly.refresh(env, november))
    assert second["added"] == ["NEW"] and second["removed"] == ["C00"]
    assert second["as_of"] == "2026-11-01T00:00:00+00:00"
    assert second["selected_at"] == november.isoformat()
    assert monthly.load_snapshot(november - timedelta(minutes=1)) == {}
    assert monthly.load_snapshot(NOW) == first
    assert (monthly.SNAPSHOT_DIR / "2026-10.json").read_bytes() == original


def test_missing_month_never_falls_back_and_insufficient_candidates_are_not_published(env, monkeypatch):
    asyncio.run(monthly.refresh(env, NOW))
    november = datetime(2026, 11, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(monthly, "_now", lambda: november)
    assert monthly.members(november) == []
    env.symbols = env.symbols[:10]
    with pytest.raises(ValueError, match="only 10"):
        asyncio.run(monthly.refresh(env, november))
    assert not (monthly.SNAPSHOT_DIR / "2026-11.json").exists()


def test_current_trading_status_and_minimum_history_control_membership(env, monkeypatch):
    env.symbols[0]["status"] = "SETTLING"

    async def counts(perps, as_of):
        assert "C00" not in perps
        return {b: monthly.MIN_BARS - (b == "C01") for b in perps}

    monkeypatch.setattr(monthly, "history_counts", counts)
    snapshot = asyncio.run(monthly.refresh(env, NOW))
    assert len(snapshot["members"]) == 38
    assert not {"C00", "C01"} & snapshot["members"].keys()


def test_history_counts_exclude_duplicates_future_open_and_invalid_prices(monkeypatch, tmp_path):
    from core.data import data_storage
    from scripts import train_ml_signal as trainer

    monkeypatch.setattr(trainer, "_local_symbols", lambda exchange: ["AAA/USDT", "NO/USDT"])

    monkeypatch.setattr(data_storage, "storage_path", tmp_path)
    directory = tmp_path / "binance/AAA_USDT"
    directory.mkdir(parents=True)
    frame = pd.DataFrame({"close": [1, 2, 3, 4, 5, 0, float("nan"), 6]}, index=[
            NOW - timedelta(hours=3), NOW - timedelta(hours=3), NOW - timedelta(hours=2),
            NOW, NOW + timedelta(hours=1), NOW - timedelta(hours=4), NOW - timedelta(hours=5),
            NOW - timedelta(minutes=90)])
    frame.index.name = "timestamp"
    frame.to_parquet(directory / "1h.parquet")
    parts = directory / "1h_parts"
    parts.mkdir()
    frame.iloc[:1].to_parquet(parts / "2026.parquet")  # overlapping partition doesn't inflate history
    assert asyncio.run(monthly.history_counts({"AAA": "AAAUSDT"}, NOW)) == {"AAA": 2}


def test_daily_collection_appends_monthly_coins_after_originals_without_duplicates(env, monkeypatch):
    asyncio.run(monthly.refresh(env, NOW))
    monkeypatch.setattr(store, "universe", lambda: [("C00", "C00USDT")])
    monkeypatch.setattr(store, "WIDE_COINS", False)
    assert store.update_universe() == [(f"C{i:02d}", f"C{i:02d}USDT") for i in range(40)]


def test_no_backtest_reference_is_not_a_zero_return_or_a_decay_benchmark():
    rule = retirement.RULES["coinglass_composite_7d_monthly_v1"]
    assert rule["backtest_mean_pct"] is None
    assert retirement.verdict([1.0] * 12, rule)["verdict"] == "collecting"
    assert retirement.verdict([1.0] * 364, rule)["verdict"] == "confirmed"
    assert retirement.verdict([-1.0] * 364, rule)["verdict"] == "retire"
    spec = next(s for s in tracker_registry.TRACKERS if s["id"] == "coinglass_composite_7d_monthly_v1")
    assert tracker_registry.measurement(spec)["gross_notional_multiplier"] == 2
