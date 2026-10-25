"""The KOL event study finds a planted effect and does not invent one from noise."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "research_x_kol.py"
spec = importlib.util.spec_from_file_location("research_x_kol", SCRIPT)
study = importlib.util.module_from_spec(spec)
spec.loader.exec_module(study)

AVAILABLE = {"BTC": "BTC_USDT", "ETH": "ETH_USDT", "AAA": "AAA_USDT", "BBB": "BBB_USDT", "CCC": "CCC_USDT",
             "PEPE": "1000PEPE_USDT"}
HOURS = pd.date_range("2026-04-01", periods=24 * 150, freq="h", tz="UTC")


def _walk(seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.004, len(HOURS)))), index=HOURS)


def _prices(plant: float = 0.0, events=()) -> dict:
    prices = {coin: _walk(i) for i, coin in enumerate(["BTC", "AAA", "BBB", "CCC"])}
    for coin, entry, sign in events:
        ramp = np.clip((np.arange(len(HOURS)) - HOURS.get_loc(entry)) / 24.0, 0, 1)
        prices[coin] = prices[coin] * np.exp(sign * plant * ramp)
    return prices


def _records(n: int, seed: int, text: str = "Going long $%s, breakout incoming"):
    """n posts spread over the last 90 days, each 30 minutes before an hourly bar."""
    rng = np.random.default_rng(seed)
    days = rng.choice(np.arange(62, 147), size=n, replace=True)  # inside the last 90 days of the 150
    out, planted = [], []
    for i, day in enumerate(sorted(days)):
        coin = ["AAA", "BBB", "CCC"][int(rng.integers(0, 3))]
        entry = HOURS[int(day) * 24 + int(rng.integers(1, 23))]
        out.append({"handle": "alpha", "author": "alpha", "tweet_id": str(i), "provenance": "backfill",
                    "created_at": (entry - pd.Timedelta(minutes=30)).isoformat(), "text": text % coin})
        planted.append((coin, entry, 1))
    return out, planted


def test_coin_extraction():
    available = AVAILABLE
    assert study.extract_coins("$aaa and $USDT and $nothere, https://x.com/$BBB", available) == ["AAA"]
    assert study.extract_coins("BTC and ETH look heavy, SEC said ETF", available) == ["BTC", "ETH"]
    assert study.extract_coins("比特币 突破, 以太坊 也是; bitcoin again", available) == ["BTC", "ETH"]
    assert study.extract_coins("$PEPE is up", available) == ["PEPE"]
    assert study.extract_coins("nothing tradable here", available) == []


def test_direction_lexicon_cancels_and_handles_chinese():
    assert study.direction("Going long $BTC, breakout") == 1
    assert study.direction("BTC dump incoming, sell") == -1
    assert study.direction("short squeeze: 40M of shorts liquidated") == 1  # the losing side points the other way
    assert study.direction("long liquidations on ETH, longs got rekt") == -1
    assert study.direction("空头爆仓 3 亿") == 1 and study.direction("多头被清算") == -1
    assert study.direction("181,941 traders liquidated") == -1
    assert study.direction("看多 BTC，突破新高") == 1
    assert study.direction("看空，跌破支撑") == -1
    assert study.direction("whale deposited 5,000 ETH to Binance") == -1
    assert study.direction("whale withdrew 5,000 ETH from Binance") == 1
    assert study.direction("ETH is trading sideways") == 0


def test_dedupe_keeps_the_earliest_per_coin_per_day():
    base = pd.Timestamp("2026-08-01T00:10:00Z")
    events = [{"handle": h, "coin": c, "created": base + pd.Timedelta(hours=k)}
              for h, c, k in [("a", "AAA", 0), ("b", "AAA", 3), ("a", "BBB", 4), ("a", "AAA", 30)]]
    assert [(e["handle"], e["coin"]) for e in study.dedupe(events, by_handle=False)] == [
        ("a", "AAA"), ("a", "BBB"), ("a", "AAA")]
    assert len(study.dedupe(events, by_handle=True)) == 4  # b's AAA is its own key; a's AAA reopens after 24h


def test_retweets_replies_and_foreign_authors_are_not_events():
    base = {"handle": "alpha", "author": "alpha", "tweet_id": "1", "created_at": "2026-08-01T00:10:00+00:00",
            "text": "long $AAA", "provenance": "live"}
    ok = study.build_events([base], AVAILABLE)
    assert len(ok) == 1 and ok[0]["entry"] == pd.Timestamp("2026-08-01T01:00:00Z") and ok[0]["dir"] == 1
    for change in ({"is_retweet": True}, {"in_reply_to_user": "other"}, {"author": "other"}, {"created_at": None}):
        assert study.build_events([{**base, **change}], AVAILABLE) == []
    many = study.build_events([{**base, "text": "long $AAA $BBB $CCC $BTC"}], AVAILABLE)
    assert all(e["n_coins"] == 4 for e in many)  # kept as events but excluded from the directional tests


def _run(records, prices, resamples=300):
    return study.run(records, lambda coin: prices[coin], AVAILABLE, resamples=resamples)


def test_a_planted_effect_is_found():
    records, planted = _records(120, seed=1)
    result = _run(records, _prices(plant=0.03, events=planted))
    primary = result["primary"]
    assert primary["n"] >= 60 and primary["mean_net"] > 0.02
    assert primary["p_two_sided"] < 0.01
    assert primary["verdict"]["passes"] is True


def test_short_calls_are_scored_as_entry_minus_exit():
    records, planted = _records(120, seed=2, text="Selling $%s, breakdown incoming, short it")
    result = _run(records, _prices(plant=0.03, events=[(c, e, -1) for c, e, _ in planted]))
    assert result["primary"]["mean_net"] > 0.02 and result["primary"]["p_two_sided"] < 0.01


def test_noise_is_rarely_called_a_signal():
    hits = 0
    trials = 25
    for seed in range(trials):
        records, _ = _records(120, seed=100 + seed)
        result = _run(records, _prices(), resamples=200)
        hits += result["primary"]["p_two_sided"] < 0.05
    assert hits <= 5  # nominal 5%: allow for the discreteness of 25 trials


def test_wrong_way_effect_does_not_pass():
    records, planted = _records(120, seed=3)
    result = _run(records, _prices(plant=0.03, events=[(c, e, -1) for c, e, _ in planted]))
    assert result["primary"]["mean_net"] < 0 and result["primary"]["verdict"]["passes"] is False


def test_costs_come_off_before_the_verdict():
    assert study.cost_of("BTC") == pytest.approx(0.0012) and study.cost_of("AAA") == pytest.approx(0.0020)
    records, planted = _records(120, seed=4)
    prices = _prices(plant=0.03, events=planted)
    events = study.dedupe([e for e in study.build_events(records, AVAILABLE) if e["dir"] and e["n_coins"] <= 3],
                          by_handle=False)
    panel = study.Panel(prices, prices["BTC"], 24, min(e["entry"] for e in events))
    gross = np.mean([e["dir"] * panel.actual(e) for e in events if not np.isnan(panel.actual(e))])
    net = study.evaluate(events, panel, resamples=50)["mean_net"]
    assert net == pytest.approx(gross - 0.0020)  # every event here is an altcoin: 10 bp a side, 20 bp round trip
