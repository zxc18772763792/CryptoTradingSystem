from __future__ import annotations

import asyncio

from prediction_markets.polymarket.gamma_client import GammaClient


def test_gamma_client_methods_delegate_and_normalize():
    client = GammaClient(base_url="https://example.com")
    calls = []

    async def fake_request(path, *, params=None):
        calls.append((path, params))
        if path == "/events":
            return [{"id": "evt-1"}]
        if path == "/markets":
            return [{"id": "mkt-1"}]
        if path == "/tags":
            return [{"id": 1, "name": "crypto"}]
        if path == "/public-search":
            return {"events": [{"id": "evt-search"}]}
        return []

    client._request = fake_request  # type: ignore[method-assign]

    events = asyncio.run(client.list_events(limit=25, offset=10, search="BTC"))
    markets = asyncio.run(client.list_markets(limit=10, slug="btc"))
    tags = asyncio.run(client.list_tags(limit=20))
    search = asyncio.run(client.search_public("Fed", limit=5))

    assert events == [{"id": "evt-1"}]
    assert markets == [{"id": "mkt-1"}]
    assert tags == [{"id": 1, "name": "crypto"}]
    assert search == [{"id": "evt-search"}]
    assert calls[0][0] == "/events"
    assert calls[0][1]["limit"] == 25
    assert calls[1][0] == "/markets"
    assert calls[1][1]["slug"] == "btc"
    assert calls[3][0] == "/public-search"
    assert calls[3][1]["q"] == "Fed"
