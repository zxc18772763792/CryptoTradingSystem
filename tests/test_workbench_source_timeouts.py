"""Research-workbench sources that used to time out (2026-09-28 investigation)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import httpx
import pandas as pd
import pytest

from core.utils import dual_transport


class _Client:
    """httpx.AsyncClient stand-in: fails for the transports listed in ``fail``."""

    calls: list = []
    fail: set = set()

    def __init__(self, *args, trust_env=True, **kwargs):
        self.direct = not trust_env

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def get(self, url, params=None):
        _Client.calls.append("direct" if self.direct else "proxy")
        if ("direct" if self.direct else "proxy") in _Client.fail:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, request=httpx.Request("GET", url), text="ok")


@pytest.fixture
def fake_client(monkeypatch):
    _Client.calls, _Client.fail = [], set()
    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    monkeypatch.setattr(dual_transport, "_prefer_direct", {})
    return _Client


def test_measured_hosts_try_direct_first_and_fall_back_to_proxy(fake_client):
    fake_client.fail = {"direct"}
    resp = asyncio.run(dual_transport.get("https://hacked.slowmist.io/", timeout_sec=2))
    assert resp.text == "ok" and fake_client.calls == ["direct", "proxy"]
    fake_client.calls = []
    asyncio.run(dual_transport.get("https://hacked.slowmist.io/", timeout_sec=2))
    assert fake_client.calls == ["proxy"]  # the winner is remembered per host


def test_other_hosts_keep_the_proxy_first(fake_client):
    asyncio.run(dual_transport.get("https://api.coingecko.com/api/v3/global", timeout_sec=2))
    assert fake_client.calls == ["proxy"]


def test_both_transports_failing_raises_the_last_error(fake_client):
    fake_client.fail = {"direct", "proxy"}
    with pytest.raises(httpx.ConnectError):
        asyncio.run(dual_transport.get("https://api.alternative.me/fng/", timeout_sec=2))


def test_multi_asset_overview_reads_only_the_lookback_window(monkeypatch):
    from web.api import data as data_api

    seen = []

    async def fake_load(exchange, symbol, timeframe, start_time=None, end_time=None):
        seen.append(start_time)
        idx = pd.date_range(end=datetime.utcnow(), periods=500, freq="5min")
        return pd.DataFrame({"close": range(1, 501), "volume": 1.0}, index=idx)

    monkeypatch.setattr(data_api, "_load_symbol_df", fake_load)
    monkeypatch.setattr(data_api, "_research_retired_filter", lambda **kw: (kw["requested"], []))
    out = asyncio.run(data_api.get_multi_assets_overview(symbols="BTC/USDT,ETH/USDT", timeframe="5m", lookback=360))
    assert out["count"] == 2
    expected = datetime.utcnow() - timedelta(seconds=300 * 360 * 2 + 86400)
    assert all(abs((s - expected).total_seconds()) < 60 for s in seen)  # not the whole history


def test_onchain_module_waits_through_a_cold_cache(monkeypatch):
    from web.api import research as research_api

    replies = iter([{"served_mode": "bootstrap"}, {"served_mode": "bootstrap"}, {"served_mode": "cache", "ok": True}])
    calls = []

    async def fake_overview(**kwargs):
        calls.append(kwargs["refresh"])
        return next(replies)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(research_api, "get_onchain_overview", fake_overview)
    monkeypatch.setattr(research_api.asyncio, "sleep", no_sleep)
    profile = research_api.ResearchProfile()
    result = asyncio.run(research_api._onchain_overview_for_workbench(profile))
    assert result == {"served_mode": "cache", "ok": True}
    assert calls == [True, False, False]  # later polls never restart the refresh
