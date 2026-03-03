from datetime import datetime, timedelta, timezone

from prediction_markets.polymarket.features import get_features_asof_from_quotes, get_features_range_from_quotes


def _quote(ts_offset_min: int, price: float, category: str = "PRICE"):
    base = datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc)
    return {
        "ts": base + timedelta(minutes=ts_offset_min),
        "market_id": "m1",
        "token_id": "tok1",
        "outcome": "YES",
        "category": category,
        "price": price,
        "midpoint": price,
        "spread": 0.02,
        "depth1": 100.0,
        "depth5": 500.0,
        "relevance_score": 2.0,
        "symbol_weights": {"BTCUSDT": 1.0},
    }


def test_features_range_and_asof():
    quotes = [_quote(i, 0.50 + i * 0.01) for i in range(10)]
    rows = get_features_range_from_quotes(quotes, symbol="BTCUSDT", since=quotes[0]["ts"], until=quotes[-1]["ts"], timeframe="1m")
    assert rows
    snap = get_features_asof_from_quotes(quotes, symbol="BTCUSDT", ts=quotes[-1]["ts"], timeframe="1m")
    assert snap["symbol"] == "BTCUSDT"
    assert "pm_price_signal" in snap
