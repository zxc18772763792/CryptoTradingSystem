from prediction_markets.polymarket.config import load_polymarket_config
from prediction_markets.polymarket.market_resolver import MarketResolver


def test_market_resolver_filters_and_maps_symbols():
    cfg = load_polymarket_config()
    resolver = MarketResolver(cfg)
    event = {
        "id": "evt1",
        "title": "Will BTC hit 100k by June?",
        "description": "Bitcoin price event",
        "markets": [
            {
                "id": "m1",
                "question": "Will BTC hit 100k by June?",
                "description": "BTC target",
                "liquidityClob": 8000,
                "volume24hr": 12000,
                "updatedAt": "2026-03-03T00:00:00Z",
                "outcomes": [
                    {"outcome": "YES", "token_id": "tok_yes"},
                    {"outcome": "NO", "token_id": "tok_no"},
                ],
                "active": True,
                "closed": False,
            }
        ],
    }
    resolved = resolver.resolve(events=[event], keyword_search_hits={"PRICE": []}, latest_quotes=[{"market_id": "m1", "spread": 0.02}])
    assert resolved["markets"]
    market = resolved["markets"][0]
    assert market["category"] == "PRICE"
    price_subs = resolved["subscriptions"]["PRICE"]
    assert len(price_subs) == 2
    assert price_subs[0]["symbol_weights"]["BTCUSDT"] == 1.0
