import asyncio

from core.data.binance_alpha import (
    ALPHA_SOURCE_NAME,
    _extract_token_list,
    _load_disk_payload,
    _persist_payload,
    load_alpha_token_catalog,
    alpha_pair_from_id,
    alpha_symbols,
    build_alpha_market_snapshots,
    alpha_trade_symbol_from_id,
)


def test_alpha_ids_use_stable_synthetic_pairs():
    assert alpha_pair_from_id("ALPHA_175") == "ALPHA175/USDT"
    assert alpha_pair_from_id("alpha-001") == "ALPHA001/USDT"


def test_alpha_trade_symbols_preserve_official_underscore():
    assert alpha_trade_symbol_from_id("ALPHA_175") == "ALPHA_175USDT"
    assert alpha_trade_symbol_from_id("alpha_175usdt") == "ALPHA_175USDT"


def test_alpha_symbols_skip_offline_and_deduplicate():
    tokens = [
        {"alphaId": "ALPHA_175", "offline": False},
        {"alphaId": "ALPHA175", "offline": False},
        {"alphaId": "ALPHA_999", "offline": True},
    ]

    assert alpha_symbols(tokens) == ["ALPHA175/USDT"]


def test_alpha_snapshot_preserves_research_metadata():
    tokens = [
        {
            "alphaId": "ALPHA_175",
            "symbol": "NOVA",
            "name": "Nova Protocol",
            "chainId": "56",
            "chainName": "BSC",
            "contractAddress": "0xabc",
            "price": "0.125",
            "percentChange24h": "18.5",
            "volume24h": "1250000",
            "marketCap": "5000000",
            "fdv": "12000000",
            "liquidity": "850000",
            "holders": "4200",
            "totalSupply": "100000000",
            "circulatingSupply": "40000000",
            "count24h": "9100",
            "hotTag": True,
            "score": "87",
            "listingTime": 1720000000000,
        }
    ]

    snapshot = build_alpha_market_snapshots(tokens, timestamp="2026-09-06T00:00:00+00:00")["ALPHA175/USDT"]

    assert snapshot["source_name"] == ALPHA_SOURCE_NAME
    assert snapshot["current_price"] == 0.125
    assert snapshot["price_change_percent_24h"] == 18.5
    assert snapshot["alpha_context"] == {
        "alpha_id": "ALPHA_175",
        "display_symbol": "NOVA",
        "name": "Nova Protocol",
        "chain_id": "56",
        "chain_name": "BSC",
        "contract_address": "0xabc",
        "icon_url": "",
        "liquidity_usd": 850000.0,
        "fdv_usd": 12000000.0,
        "holders": 4200,
        "total_supply": 100000000.0,
        "circulating_supply": 40000000.0,
        "circulating_ratio": 0.4,
        "hot_tag": True,
        "listing_time": 1720000000000,
        "count_24h": 9100,
        "alpha_directory_score": 87,
        "percent_change_24h": 18.5,
        "volume_24h_usd": 1250000.0,
        "market_cap_usd": 5000000.0,
    }


def test_persisted_catalog_round_trips_through_disk_envelope(monkeypatch, tmp_path):
    from core.data import binance_alpha

    monkeypatch.setattr(binance_alpha, "_alpha_root", lambda: tmp_path)
    payload = {
        "source": ALPHA_SOURCE_NAME,
        "updated_at": "2026-09-06T00:00:00+00:00",
        "tokens": [{"alphaId": "ALPHA_175", "symbol": "NOVA"}],
        "count": 1,
        "stale": False,
        "warning": "",
    }

    _persist_payload(payload)
    restored = _load_disk_payload()

    assert restored == payload
    assert _extract_token_list(restored) == payload["tokens"]
    assert _extract_token_list({"data": payload["tokens"]}) == payload["tokens"]


def test_force_refreshes_are_coalesced_within_one_ui_action(monkeypatch):
    from core.data import binance_alpha

    calls = []

    async def fake_fetch_payload():
        calls.append("fetch")
        return {
            "source": ALPHA_SOURCE_NAME,
            "updated_at": "2026-09-06T00:00:00+00:00",
            "tokens": [{"alphaId": "ALPHA_175"}],
            "count": 1,
            "stale": False,
            "warning": "",
        }

    monkeypatch.setattr(binance_alpha, "_fetch_payload", fake_fetch_payload)
    monkeypatch.setattr(binance_alpha, "_persist_payload", lambda payload: None)
    monkeypatch.setattr(binance_alpha, "_load_disk_payload", lambda: None)
    monkeypatch.setattr(binance_alpha, "_alpha_enabled", lambda: True)
    original_cache = dict(binance_alpha._CACHE)
    try:
        binance_alpha._CACHE.update({"payload": None, "stored_at": 0.0})
        binance_alpha._INFLIGHT = None

        first = asyncio.run(load_alpha_token_catalog(refresh=True))
        second = asyncio.run(load_alpha_token_catalog(refresh=True))

        assert first == second
        assert calls == ["fetch"]
    finally:
        binance_alpha._CACHE.clear()
        binance_alpha._CACHE.update(original_cache)
        binance_alpha._INFLIGHT = None


def test_raw_catalog_history_rotates_at_configured_limit(monkeypatch, tmp_path):
    from core.data import binance_alpha

    monkeypatch.setattr(binance_alpha, "_alpha_root", lambda: tmp_path)
    monkeypatch.setattr(binance_alpha, "_history_max_bytes", lambda: 256)
    first = {
        "updated_at": "2026-09-06T00:00:00+00:00",
        "tokens": [{"alphaId": "ALPHA_175", "raw": "a" * 300}],
    }
    second = {
        "updated_at": "2026-09-06T00:05:00+00:00",
        "tokens": [{"alphaId": "ALPHA_175", "raw": "b" * 300}],
    }

    _persist_payload(first)
    _persist_payload(second)

    history = tmp_path / "token_snapshots.jsonl"
    archive = tmp_path / "token_snapshots.jsonl.1"
    assert archive.exists()
    assert '"raw":"a' in archive.read_text(encoding="utf-8")
    assert '"raw":"b' in history.read_text(encoding="utf-8")
