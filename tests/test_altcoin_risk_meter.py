"""The radar's one-week risk reading: deciles, touch rates and what counts as finished data."""
from __future__ import annotations

import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import web.api.altcoin as altcoin_api
from core.research import altcoin_risk_meter as meter
from web.api.altcoin import risk as risk_api

DAYS = 480
START = pd.Timestamp("2025-01-01")
LAST_DAY = START + pd.Timedelta(days=DAYS - 1)
AFTER_LAST_DAY = LAST_DAY + pd.Timedelta(days=1, hours=6)


def _bars(ranges: dict[str, float], days: int = DAYS) -> dict[str, pd.DataFrame]:
    """Each coin swings its own fixed percentage around a slow wave (the wave keeps it from looking pegged)."""
    index = pd.date_range(START, periods=days, freq="D")
    wave = 100.0 * (1.0 + 0.08 * np.sin(2.0 * math.pi * np.arange(days) / 60.0))
    close = pd.DataFrame({coin: wave for coin in ranges}, index=index)
    high = pd.DataFrame({coin: wave * (1.0 + swing) for coin, swing in ranges.items()}, index=index)
    low = pd.DataFrame({coin: wave * (1.0 - swing) for coin, swing in ranges.items()}, index=index)
    volume = pd.DataFrame({coin: np.full(days, 1e6) for coin in ranges}, index=index)
    return {"close": close, "high": high, "low": low, "volume": volume}


def _market(extra: dict[str, float] | None = None) -> dict[str, pd.DataFrame]:
    # Swings run from 1% to 50%, so the lowest and highest deciles are far from every level tested.
    ranges = {f"C{i:03d}": 0.01 + 0.49 * i / 99.0 for i in range(100)}
    ranges.update(extra or {})
    return _bars(ranges)


def _level(payload: dict, decile: int) -> dict:
    return next(row for row in payload["calibration"] if row["decile"] == decile)


def test_touch_rates_follow_volatility_on_both_sides():
    payload = meter.compute_risk_meter(_market(), now=AFTER_LAST_DAY)

    assert payload["available"] is True
    calm, wild = _level(payload, 1), _level(payload, 10)
    assert calm["up40"] == 0.0 and calm["dn30"] == 0.0
    assert calm["long"]["3"] == 0.0 and calm["short"]["3"] == 0.0
    assert wild["up40"] == 1.0 and wild["dn30"] == 1.0
    assert wild["long"]["5"] == 1.0 and wild["short"]["5"] == 1.0
    assert calm["group"] == "calm" and wild["group"] == "hottest"
    assert [row["decile"] for row in payload["calibration"]] == list(range(1, 11))


def test_every_coin_is_placed_by_its_own_volatility():
    payload = meter.compute_risk_meter(_market(), now=AFTER_LAST_DAY)
    symbols = payload["symbols"]

    assert symbols["C000"]["decile"] == 1
    assert symbols["C099"]["decile"] == 10
    assert symbols["C099"]["pct"] == 1.0
    assert symbols["C050"]["decile"] in (5, 6)
    assert symbols["C099"]["perp"] == "C099USDT"


def test_calibration_only_uses_finished_weeks():
    payload = meter.compute_risk_meter(_market(), now=AFTER_LAST_DAY)
    method = payload["method"]

    assert payload["as_of"] == str(LAST_DAY.date())
    assert method["calibration_to"] == str((LAST_DAY - pd.Timedelta(days=meter.HOLD_DAYS)).date())
    assert method["calibration_days"] == meter.CAL_WINDOW_DAYS

    # A spike on the last day counts once its day has closed, because it ends the last finished window.
    frames = _market()
    frames["high"].iloc[-1:] = frames["high"].iloc[-1:] * 3.0
    assert _level(meter.compute_risk_meter(frames, now=AFTER_LAST_DAY), 1)["up40"] > 0.0
    # While that day is still forming, nothing is allowed to see it.
    assert _level(meter.compute_risk_meter(frames, now=LAST_DAY), 1)["up40"] == 0.0


def test_todays_forming_bar_is_left_out():
    payload = meter.compute_risk_meter(_market(), now=LAST_DAY + pd.Timedelta(hours=9))
    assert payload["as_of"] == str((LAST_DAY - pd.Timedelta(days=1)).date())


def test_a_day_only_a_few_coins_have_is_not_the_reading_day():
    frames = _market()
    tomorrow = LAST_DAY + pd.Timedelta(days=1)
    for name, frame in frames.items():
        frame.loc[tomorrow] = np.nan
        frame.loc[tomorrow, ["C000", "C001"]] = 100.0
    payload = meter.compute_risk_meter(frames, now=tomorrow + pd.Timedelta(days=2))
    assert payload["as_of"] == str(LAST_DAY.date())


def test_majors_are_out_of_the_universe_but_still_get_a_reading():
    payload = meter.compute_risk_meter(_market({"BTC": 0.01}), now=AFTER_LAST_DAY)
    assert payload["symbols"]["BTC"]["in_universe"] is False
    assert payload["symbols"]["BTC"]["decile"] == 1
    assert payload["symbols"]["C050"]["in_universe"] is True
    assert payload["method"]["universe_today"] == 100


def test_multiplier_contracts_answer_to_both_names():
    payload = meter.compute_risk_meter(_market({"1000PEPE": 0.2, "1MBABYDOGE": 0.2}), now=AFTER_LAST_DAY)
    assert payload["symbols"]["PEPE"] == payload["symbols"]["1000PEPE"]
    assert payload["symbols"]["BABYDOGE"]["perp"] == "1MBABYDOGEUSDT"
    assert meter.strip_multiplier("1INCH") == "1INCH"


def test_delisted_coins_get_no_reading():
    frames = _market({"GONE": 0.2})
    for frame in frames.values():
        frame.loc[frame.index[-30]:, "GONE"] = np.nan
    payload = meter.compute_risk_meter(frames, now=AFTER_LAST_DAY)
    assert "GONE" not in payload["symbols"]


def test_a_young_coin_is_read_on_the_universe_scale():
    frames = _market({"YOUNG": 0.2})
    for frame in frames.values():
        frame.loc[: frame.index[-21], "YOUNG"] = np.nan
    payload = meter.compute_risk_meter(frames, now=AFTER_LAST_DAY)
    assert payload["symbols"]["YOUNG"]["in_universe"] is False
    assert 3 <= payload["symbols"]["YOUNG"]["decile"] <= 5


def test_old_data_is_flagged_stale():
    fresh = meter.compute_risk_meter(_market(), now=AFTER_LAST_DAY)
    old = meter.compute_risk_meter(_market(), now=LAST_DAY + pd.Timedelta(days=10))
    assert fresh["stale"] is False and fresh["age_days"] == pytest.approx(0.25)
    assert old["stale"] is True


def test_aware_and_naive_clocks_agree():
    aware = datetime(2026, 4, 26, 6, tzinfo=timezone.utc)
    frames = _market()
    assert meter.compute_risk_meter(frames, now=aware)["age_days"] == meter.compute_risk_meter(frames, now=aware.replace(tzinfo=None))["age_days"]


def test_too_little_data_is_reported_not_guessed():
    few_coins = _bars({f"C{i}": 0.1 for i in range(10)})
    assert meter.compute_risk_meter(few_coins, now=AFTER_LAST_DAY) == {"available": False, "reason": "not_enough_coins"}
    short = _bars({f"C{i:03d}": 0.1 for i in range(60)}, days=5)
    result = meter.compute_risk_meter(short, now=START + pd.Timedelta(days=30))
    assert result["available"] is False and result["reason"] == "not_enough_history"


def test_load_daily_bars_reads_the_store_layout(tmp_path):
    index = pd.date_range("2026-01-01", periods=3, freq="D", tz="UTC")
    bars = pd.DataFrame({"open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10.0}, index=index)
    for coin in ("AAA", "USDC"):
        folder = tmp_path / f"{coin}_USDT"
        folder.mkdir()
        bars.to_parquet(folder / "1d.parquet")
    (tmp_path / "BBB_USDT").mkdir()  # a coin with no daily file is skipped

    frames = meter.load_daily_bars(tmp_path)

    assert list(frames["close"].columns) == ["AAA"]  # stablecoins never enter
    assert frames["close"].index.tz is None
    assert frames["volume"]["AAA"].iloc[0] == pytest.approx(10.0 * (2.0 + 1.0 + 1.5) / 3.0)


def test_build_with_an_empty_store_is_unavailable(tmp_path):
    assert meter.build_risk_meter(tmp_path) == {"available": False, "reason": "no_local_bars"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(risk_api, "_risk_cache", {"payload": None, "built_at": 0.0})
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    return TestClient(app)


def test_route_builds_once_then_serves_the_cache(client, monkeypatch):
    calls = []

    def build():
        calls.append(1)
        return {"available": True, "as_of": "2026-11-06", "symbols": {}}

    monkeypatch.setattr(risk_api, "build_risk_meter", build)

    first = client.get("/api/altcoin/radar/risk-meter")
    second = client.get("/api/altcoin/radar/risk-meter")

    assert first.status_code == 200 and first.json()["as_of"] == "2026-11-06"
    assert "ts" in second.json()
    assert len(calls) == 1


def test_route_turns_a_failed_build_into_unavailable_and_retries_later(client, monkeypatch):
    def broken():
        raise OSError("disk gone")

    monkeypatch.setattr(risk_api, "build_risk_meter", broken)
    body = client.get("/api/altcoin/radar/risk-meter").json()
    assert body["available"] is False and "disk gone" in body["reason"]

    monkeypatch.setattr(risk_api, "build_risk_meter", lambda: {"available": True, "symbols": {}})
    assert client.get("/api/altcoin/radar/risk-meter").json()["available"] is False  # inside the retry gap
    risk_api._risk_cache["built_at"] -= risk_api._RISK_RETRY_SEC + 1
    assert client.get("/api/altcoin/radar/risk-meter").json()["available"] is True
