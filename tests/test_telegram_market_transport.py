import hashlib
import json
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from scripts.telegram_market_transport import PublicMarketTransport


def transport(tmp_path, replies):
    calls, options = [], []
    class Client:
        def __init__(self, **kwargs):
            options.append(kwargs)
        def get(self, url, params, **kwargs):
            calls.append((url, params))
            reply = replies.pop(0)
            if isinstance(reply, Exception):
                raise reply
            status, payload = reply
            return httpx.Response(status, json=payload, request=httpx.Request("GET", url))
        def close(self):
            calls.append("closed")
    t = PublicMarketTransport(lambda: ("http", "127.0.0.1", 7890, True, "private-user", "private-pass"),
                             client_factory=Client, sleep=lambda _: None, evidence_root=tmp_path)
    return t, calls, options


def test_eof_retry_is_bounded_reuses_client_and_preserves_receipts(tmp_path):
    t, calls, options = transport(tmp_path, [httpx.ConnectError("TLS EOF"), (200, {"serverTime": 10}),
                                           (200, {"serverTime": 20})])
    assert t.get_json("/fapi/v1/time")["serverTime"] == 10
    assert t.get_json("/fapi/v1/time")["serverTime"] == 20
    # Replace the failed pool, then reuse the successful one for the next GET.
    assert len(options) == 2 and len([c for c in calls if c != "closed"]) == 3
    assert options[0]["proxy"] == options[1]["proxy"]
    assert options[0]["verify"].check_hostname
    for p in t.folder.glob("*.json"):
        text = p.read_text(encoding="utf-8")
        assert "private-user" not in text and "private-pass" not in text
        r = json.loads(text)
        if r.get("raw_path"):
            assert hashlib.sha256((p.parent/r["raw_path"]).read_bytes()).hexdigest() == r["raw_sha256"]
    t.close()
    assert calls[-1] == "closed"


@pytest.mark.parametrize("status", [401, 403, 418, 429])
def test_refusal_never_retries_or_changes_route(tmp_path, status):
    t, calls, _ = transport(tmp_path, [(status, {"error": "refused"})])
    with pytest.raises(httpx.HTTPStatusError):
        t.get_json("/fapi/v1/time")
    assert len(calls) == 1


def test_repeated_transport_failure_stops_after_three_attempts(tmp_path):
    t, calls, _ = transport(tmp_path, [httpx.ConnectError("EOF")] * 3)
    with pytest.raises(httpx.ConnectError):
        t.get_json("/fapi/v1/time")
    assert len([c for c in calls if c != "closed"]) == 3 and len(list(t.folder.glob("*.json"))) == 3


def test_account_endpoints_and_secret_parameters_are_rejected_before_network(tmp_path):
    t, calls, _ = transport(tmp_path, [])
    for ep, params in [("/fapi/v1/order", {}), ("/fapi/v1/time", {"api_key": "private-pass"})]:
        with pytest.raises(ValueError):
            t.get_json(ep, params)
    assert not calls and t.folder is None


def test_funding_nonadvancing_page_is_rejected(monkeypatch):
    from scripts import telegram_forward_cycle as wrapper
    monkeypatch.setattr(wrapper, "PAGE", 2)
    monkeypatch.setattr(wrapper, "_original", lambda *a: [{"fundingTime": 1}, {"fundingTime": 2}])
    with pytest.raises(ValueError, match="advance"):
        wrapper.paged_public_json("/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "limit": 10})


def test_collection_fragment_survives_one_disconnect_retry(monkeypatch):
    from scripts import telegram_forward_cycle as w
    source = AsyncMock(side_effect=[asyncio.IncompleteReadError(b"", 8), {"all_sources": "complete"}])
    record = Mock()
    monkeypatch.setattr(w, "_original_collect", source)
    monkeypatch.setattr(w, "_record_collection_retry", record)
    monkeypatch.setattr(w.asyncio, "sleep", AsyncMock())
    store = SimpleNamespace(db=SimpleNamespace(commit=Mock()))
    assert asyncio.run(w.collect_with_retry(store)) == {"all_sources": "complete"}
    assert source.await_count == 2 and store.db.commit.call_count == 1
    record.assert_called_once()


@pytest.mark.parametrize("error", [PermissionError("busy"), RuntimeError("auth refused"), ValueError("invalid source")])
def test_collection_refusal_or_invalid_state_is_never_retried(monkeypatch, error):
    from scripts import telegram_forward_cycle as w
    source = AsyncMock(side_effect=error)
    monkeypatch.setattr(w, "_original_collect", source)
    with pytest.raises(type(error)):
        asyncio.run(w.collect_with_retry(SimpleNamespace()))
    assert source.await_count == 1


def test_repeated_collection_disconnect_stops_after_one_reconnect(monkeypatch):
    from scripts import telegram_forward_cycle as w
    source = AsyncMock(side_effect=asyncio.IncompleteReadError(b"", 8))
    monkeypatch.setattr(w, "_original_collect", source)
    monkeypatch.setattr(w, "_record_collection_retry", Mock())
    monkeypatch.setattr(w.asyncio, "sleep", AsyncMock())
    with pytest.raises(asyncio.IncompleteReadError):
        asyncio.run(w.collect_with_retry(SimpleNamespace(db=SimpleNamespace(commit=Mock()))))
    assert source.await_count == 2


def test_historical_collection_is_not_replayed_by_live_recovery(monkeypatch):
    from scripts import telegram_forward_cycle as w
    source = AsyncMock(side_effect=ConnectionError("offline"))
    monkeypatch.setattr(w, "_original_collect", source)
    with pytest.raises(ConnectionError):
        asyncio.run(w.collect_with_retry(SimpleNamespace(), historical=True))
    assert source.await_count == 1
