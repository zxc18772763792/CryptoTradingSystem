from __future__ import annotations

import pandas as pd
import pytest

from scripts.research_alastar_robustness import evaluate_variant


class Market:
    def __init__(self, prices):
        self.prices = prices

    def frame(self, _symbol, kind="1h"):
        if kind == "1h":
            return self.prices
        return pd.DataFrame({"funding_rate": 0., "mark_price": 100.},
                            index=pd.date_range("2026-01-01", periods=9, freq="8h", tz="UTC"))


def fixture(direction=1):
    prices = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1000.},
                          index=pd.date_range("2026-01-01", periods=60, freq="h", tz="UTC"))
    signal = {"source_id": "s", "symbol": "BTCUSDT", "direction": direction,
              "published_at": "2026-01-01T00:30:00Z"}
    return signal, Market(prices)


def test_added_latency_uses_new_entry_price_and_original_publication():
    signal, market = fixture()
    market.prices.loc["2026-01-01 04:00:00+00:00", ["open", "high", "low", "close"]] = [110., 111., 109., 110.]
    row = evaluate_variant(signal, market, delay_minutes=180)
    assert row["published_at"] == signal["published_at"]
    assert pd.Timestamp(row["entry_at"]) == pd.Timestamp("2026-01-01T04:00:00Z")
    assert row["net_return"] == pytest.approx(100/110 - 1 - .002)


def test_long_stop_caps_ordinary_move_but_does_not_ignore_gap():
    signal, market = fixture()
    market.prices.loc["2026-01-01 03:00:00+00:00", ["low", "open"]] = [80., 90.]
    row = evaluate_variant(signal, market, stop_fraction=.05)
    assert row["stop_triggered"]
    assert row["exit_price"] == 90.
    assert row["net_return"] == pytest.approx(-.102)


def test_short_stop_hits_high_and_uses_worse_opening_gap():
    signal, market = fixture(-1)
    market.prices.loc["2026-01-01 03:00:00+00:00", ["high", "open"]] = [120., 112.]
    row = evaluate_variant(signal, market, stop_fraction=.05)
    assert row["exit_price"] == 112.
    assert row["net_return"] == pytest.approx(-.122)


def test_higher_cost_does_not_change_entry_or_stop_eligibility():
    signal, market = fixture()
    base = evaluate_variant(signal, market, cost_bps=20)
    stressed = evaluate_variant(signal, market, cost_bps=100)
    assert base["entry_at"] == stressed["entry_at"]
    assert stressed["net_return"] - base["net_return"] == pytest.approx(-.008)


def test_missing_candle_after_latency_is_excluded_not_filled():
    signal, market = fixture()
    market.prices = market.prices.drop(pd.Timestamp("2026-01-01T10:00:00Z"))
    assert evaluate_variant(signal, market, delay_minutes=180)["status"] == "price_coverage_gap"


def test_stop_drops_post_stop_funding_and_bounds_intrabar_settlement():
    signal, market = fixture()
    market.prices.loc["2026-01-01 01:00:00+00:00", "low"] = 90.
    prices = market.prices
    funding = pd.DataFrame({"funding_rate": [0., -.01, -.02, 0., 0.], "mark_price": 100.},
                           index=pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00.012Z", "2026-01-01T02:00:00Z",
                                                 "2026-01-01T08:00:00Z", "2026-01-01T16:00:00Z"], format="mixed"))
    class FundingMarket:
        def frame(self, _symbol, kind="1h"):
            return prices if kind == "1h" else funding
    # Entry at01:00; the01:00.012 receipt might occur before/after stop.02:00
    # funding definitely occurs too late. Positive receipts cannot rescue loss.
    row = evaluate_variant(signal, FundingMarket(), stop_fraction=.05)
    assert row["funding_return"] == 0.
    assert row["stop_funding_upper_bound"] == pytest.approx(.01)
    assert row["net_return"] == pytest.approx(-.052)
    assert row["net_stress_40bps"] == pytest.approx(-.054)
    assert "max_adverse_excursion" not in row
