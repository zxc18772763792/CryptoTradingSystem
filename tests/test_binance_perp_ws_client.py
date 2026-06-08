from __future__ import annotations

from core.marketdata.binance_perp_ws_client import BinancePerpWSClient


def test_normalize_event_unwraps_combined_book_ticker():
    payload = BinancePerpWSClient.normalize_event(
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "e": "bookTicker",
                "u": 400900217,
                "s": "BTCUSDT",
                "b": "50000.10",
                "B": "1.25",
                "a": "50000.20",
                "A": "2.5",
                "T": 1700000000000,
                "E": 1700000000100,
            },
        }
    )

    assert payload["type"] == "book_ticker"
    assert payload["stream"] == "btcusdt@bookTicker"
    assert payload["symbol"] == "BTC/USDT"
    assert payload["bid"] == 50000.10
    assert payload["bid_size"] == 1.25
    assert payload["ask"] == 50000.20
    assert payload["ask_size"] == 2.5
    assert payload["sequence"] == 400900217
    assert payload["timestamp"] == "2023-11-14T22:13:20.100000+00:00"


def test_normalize_event_maps_mark_price_update():
    payload = BinancePerpWSClient.normalize_event(
        {
            "e": "markPriceUpdate",
            "E": 1700000000100,
            "s": "ETHUSDT",
            "p": "3000.5",
            "i": "2999.1",
            "P": "3001.2",
            "r": "0.0001",
            "T": 1700006400000,
        }
    )

    assert payload["type"] == "mark_price"
    assert payload["symbol"] == "ETH/USDT"
    assert payload["last"] == 3000.5
    assert payload["mark"] == 3000.5
    assert payload["index"] == 2999.1
    assert payload["estimated_settle_price"] == 3001.2
    assert payload["funding_rate"] == 0.0001
    assert payload["next_funding_time"] == 1700006400000


def test_normalize_event_maps_kline_payload():
    payload = BinancePerpWSClient.normalize_event(
        {
            "e": "kline",
            "E": 1700000000100,
            "s": "BTCUSDT",
            "k": {
                "t": 1700000000000,
                "T": 1700000059999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "49900",
                "c": "50000",
                "h": "50100",
                "l": "49800",
                "v": "12.5",
                "q": "625000",
                "n": 42,
                "x": True,
            },
        }
    )

    assert payload["type"] == "kline"
    assert payload["symbol"] == "BTC/USDT"
    assert payload["timeframe"] == "1m"
    assert payload["open"] == 49900.0
    assert payload["high"] == 50100.0
    assert payload["low"] == 49800.0
    assert payload["close"] == 50000.0
    assert payload["last"] == 50000.0
    assert payload["volume"] == 12.5
    assert payload["quote_volume"] == 625000.0
    assert payload["trade_count"] == 42
    assert payload["is_closed"] is True


def test_normalize_event_maps_depth_update_levels():
    payload = BinancePerpWSClient.normalize_event(
        {
            "e": "depthUpdate",
            "E": 1700000000100,
            "s": "BTCUSDT",
            "U": 100,
            "u": 105,
            "pu": 99,
            "b": [["49999.9", "0.5"], ["bad", "1"]],
            "a": [["50000.1", "0.25"]],
        }
    )

    assert payload["type"] == "depth"
    assert payload["bids"] == [[49999.9, 0.5]]
    assert payload["asks"] == [[50000.1, 0.25]]
    assert payload["first_update_id"] == 100
    assert payload["final_update_id"] == 105
    assert payload["previous_final_update_id"] == 99


def test_normalize_event_keeps_unknown_events_structured():
    raw = {"e": "unknownEvent", "E": 1700000000100, "s": "BTCUSDT", "x": "value"}

    payload = BinancePerpWSClient.normalize_event(raw)

    assert payload["type"] == "unknown"
    assert payload["event_type"] == "unknownEvent"
    assert payload["symbol"] == "BTC/USDT"
    assert payload["raw"] == raw
