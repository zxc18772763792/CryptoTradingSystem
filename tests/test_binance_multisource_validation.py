from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "core" / "research" / "binance_multisource_validation.py"
SPEC = importlib.util.spec_from_file_location("binance_multisource_validation_test", PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def identities() -> pd.DataFrame:
    return MODULE.build_token_identities(
        ["AKEUSDT", "BTWUSDT", "USUSDT", "MUSDT", "INUSDT", "1000PEPEUSDT"],
        [
            {"id": "akedo", "symbol": "ake", "name": "Akedo"},
            {"id": "banana-tape-wall", "symbol": "btw", "name": "Banana Tape Wall"},
            {"id": "bitway", "symbol": "btw", "name": "Bitway"},
            {"id": "pepe", "symbol": "pepe", "name": "Pepe"},
        ],
    )


def test_short_codes_require_explicit_uppercase_token_or_pair() -> None:
    matcher = MODULE.StrictNewsMatcher(identities())
    assert matcher.match("AKE announces a mainnet upgrade") == ["AKEUSDT"]
    assert matcher.match("Binance will list BTWUSDT perpetual") == ["BTWUSDT"]
    assert matcher.match("We are in the US market") == []
    assert matcher.match("M and IN are ordinary abbreviations") == []


def test_news_uses_fetched_time_as_availability_clock() -> None:
    matcher = MODULE.StrictNewsMatcher(identities())
    raw = pd.DataFrame(
        [
            {
                "id": 1,
                "source": "Binance",
                "title": "Binance will list BTWUSDT perpetual",
                "url": "https://www.binance.com/example",
                "published_at": "2026-06-01T00:00:00Z",
                "fetched_at": "2026-06-04T01:00:00Z",
                "content_hash": "a",
            }
        ]
    )
    mapped = MODULE.map_raw_news(raw, matcher)
    assert mapped.iloc[0]["available_at"] == pd.Timestamp("2026-06-04T01:00:00Z")
    assert bool(mapped.iloc[0]["is_listing"])


def test_exchange_transfer_and_unlock_are_supply_flow_risks() -> None:
    transfer = MODULE.classify_news(
        "The AKE dealer transferred tokens to Binance, causing the price to plummet",
        "chaincatcher",
        "",
    )
    unlock = MODULE.classify_news("AKE will unlock tokens next week", "chaincatcher", "")
    assert transfer["is_supply_flow"] and transfer["is_risk"]
    assert unlock["is_supply_flow"] and unlock["is_risk"]


def test_news_cutoff_is_0020_utc_and_rolls_seven_days() -> None:
    rows = pd.DataFrame(
        {
            "symbol": ["AKEUSDT", "AKEUSDT"],
            "date": pd.to_datetime(["2026-07-01", "2026-07-02"], utc=True),
        }
    )
    mentions = pd.DataFrame(
        {
            "news_id": [1, 2],
            "symbol": ["AKEUSDT", "AKEUSDT"],
            "available_at": pd.to_datetime(
                ["2026-07-02T00:10:00Z", "2026-07-02T00:30:00Z"], utc=True
            ),
            "source_key": ["one", "two"],
            "is_official": [False, False],
            "is_listing": [False, False],
            "is_catalyst": [True, True],
            "is_risk": [False, False],
        }
    )
    featured = MODULE.add_news_features(rows, mentions).set_index("date")
    assert featured.loc[pd.Timestamp("2026-07-01", tz="UTC"), "news_count_1d"] == 1
    assert featured.loc[pd.Timestamp("2026-07-02", tz="UTC"), "news_count_1d"] == 1


def test_tvl_is_lagged_one_full_day() -> None:
    raw = pd.DataFrame(
        {
            "symbol": ["TAIKOUSDT"] * 3,
            "source_date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"], utc=True),
            "tvl": [100.0, 110.0, 121.0],
        }
    )
    result = MODULE.build_lagged_tvl_features(raw)
    row = result[result["date"] == pd.Timestamp("2026-01-03", tz="UTC")].iloc[0]
    assert row["log_tvl_lag1d"] == MODULE.np.log1p(110.0)
    assert abs(row["tvl_change_1d_lag1d"] - 0.10) < 1e-12
