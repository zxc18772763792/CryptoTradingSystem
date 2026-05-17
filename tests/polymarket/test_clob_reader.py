from __future__ import annotations

import asyncio

import requests

from prediction_markets.polymarket.clob_reader import ClobReader


def test_clob_reader_parses_ws_book_with_millisecond_timestamp():
    reader = ClobReader()
    quotes = reader._quotes_from_ws_payload(
        {
            "event_type": "book",
            "asset_id": "tok_yes",
            "timestamp": "1772469869953",
            "bids": [{"price": "0.48", "size": "10"}, {"price": "0.49", "size": "5"}],
            "asks": [{"price": "0.52", "size": "7"}, {"price": "0.51", "size": "9"}],
        },
        {"tok_yes": {"market_id": "m1", "outcome": "YES"}},
    )

    assert len(quotes) == 1
    quote = quotes[0]
    assert quote["market_id"] == "m1"
    assert quote["bid"] == 0.49
    assert quote["ask"] == 0.51
    assert quote["spread"] == 0.02
    assert quote["ts"].year == 2026


def test_clob_reader_parses_ws_price_change_payload():
    reader = ClobReader()
    quotes = reader._quotes_from_ws_payload(
        {
            "event_type": "price_change",
            "timestamp": "1772469869953",
            "changes": [
                {
                    "asset_id": "tok_no",
                    "price": "0.38",
                    "best_bid": "0.37",
                    "best_ask": "0.39",
                }
            ],
        },
        {"tok_no": {"market_id": "m1", "outcome": "NO"}},
    )

    assert len(quotes) == 1
    assert quotes[0]["outcome"] == "NO"
    assert quotes[0]["price"] == 0.38
    assert quotes[0]["midpoint"] == 0.38
    assert quotes[0]["spread"] == 0.02


def test_get_books_uses_current_payload_then_legacy_fallback():
    client = ClobReader()
    calls = []

    class Response:
        status_code = 422

    async def fake_request(method, path, *, params=None, json_payload=None):
        calls.append(json_payload)
        if isinstance(json_payload, list):
            exc = requests.HTTPError("unprocessable")
            exc.response = Response()
            raise exc
        return {"data": [{"asset_id": "tok_yes", "bids": [], "asks": []}]}

    client._request = fake_request  # type: ignore[method-assign]

    result = asyncio.run(client.get_books(["tok_yes"]))

    assert result["data"][0]["asset_id"] == "tok_yes"
    assert calls[0] == [{"token_id": "tok_yes"}]
    assert calls[1] == {"token_ids": ["tok_yes"]}
