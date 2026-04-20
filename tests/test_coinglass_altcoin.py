from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from core.data import coinglass_altcoin


def test_build_exchange_altcoin_universe_uses_stale_disk_cache_when_refresh_times_out(
    monkeypatch,
    tmp_path,
):
    stale_payload = {
        "exchange": "binance",
        "symbols": ["LINK/USDT", "AAVE/USDT"],
        "count": 2,
        "source": "coinglass_altcoin_universe",
        "board_count": 1,
        "boards": [{"board": "defi", "market_cap_top2": ["LINK/USDT"], "onboard_top2": ["AAVE/USDT"]}],
        "updated_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
    }
    cache_path = tmp_path / "altcoin_universe_binance.json"
    cache_path.write_text(json.dumps(stale_payload, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_CACHE", {})
    monkeypatch.setattr(coinglass_altcoin, "_UNIVERSE_REFRESH_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(coinglass_altcoin, "_universe_cache_path", lambda exchange: cache_path)

    async def slow_market_rows(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {"LINK/USDT": {"symbol": "LINK/USDT", "base_symbol": "LINK", "market_cap_usd": 1_000_000_000}}

    async def slow_onboard_map(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {}

    monkeypatch.setattr(coinglass_altcoin, "load_coinglass_market_snapshots", slow_market_rows)
    monkeypatch.setattr(coinglass_altcoin, "load_exchange_onboard_map", slow_onboard_map)

    payload = asyncio.run(coinglass_altcoin.build_exchange_altcoin_universe("binance"))

    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT"]
    assert payload["stale_fallback"] is True
    assert any("coinglass universe refresh fallback for binance" in item for item in payload["warnings"])
