from __future__ import annotations

from core.news.eventizer.rules import SymbolMapper, extract_events_rules


def test_rules_extractor_caps_configured_symbol_fanout_to_global_limit() -> None:
    symbols_cfg = {
        "symbols": {
            f"COIN{i}": {"canonical": f"COIN{i}USDT", "base": f"COIN{i}"}
            for i in range(10)
        }
    }
    cfg = {
        "_symbol_mapper": SymbolMapper(symbols_cfg),
        "defaults": {"default_symbol": "COIN0USDT"},
        "rules": [
            {
                "name": "listing",
                "keywords_any": ["listed"],
                "symbols": [f"COIN{i}USDT" for i in range(10)],
                "max_symbols": 20,
                "event_type": "listing",
                "sentiment": 1,
                "impact_score": 0.5,
            }
        ],
    }

    events = extract_events_rules(
        [
            {
                "title": "Exchange listed a basket of tokens",
                "url": "https://example.test/listing",
                "published_at": "2026-05-23T00:00:00+00:00",
            }
        ],
        cfg,
    )

    assert len(events) == 8
    assert [event["symbol"] for event in events] == [f"COIN{i}USDT" for i in range(8)]


def test_rules_extractor_uses_default_symbol_limit_for_invalid_config() -> None:
    symbols_cfg = {
        "symbols": {
            f"COIN{i}": {"canonical": f"COIN{i}USDT", "base": f"COIN{i}"}
            for i in range(5)
        }
    }
    cfg = {
        "_symbol_mapper": SymbolMapper(symbols_cfg),
        "defaults": {},
        "rules": [
            {
                "name": "listing",
                "keywords_any": ["listed"],
                "symbols": [f"COIN{i}USDT" for i in range(5)],
                "max_symbols": "bad",
            }
        ],
    }

    events = extract_events_rules(
        [
            {
                "title": "Exchange listed a basket of tokens",
                "url": "https://example.test/listing",
                "published_at": "2026-05-23T00:00:00+00:00",
            }
        ],
        cfg,
    )

    assert len(events) == 3
