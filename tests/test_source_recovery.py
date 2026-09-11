from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest
import requests


def test_optional_unconfigured_sources_are_informational_but_core_failures_remain():
    from core.runtime.operating_mode import validate_operating_mode
    result = validate_operating_mode(source_health={"sources": {
        "optional": {"configured": False, "support_level": "enhancement", "health": "stale", "ready": False, "has_cached_data": True},
        "core": {"configured": False, "support_level": "core", "health": "missing", "ready": False},
        "enabled": {"configured": True, "support_level": "optional", "health": "degraded", "ready": False},
    }})
    rows = {row["code"]: row for row in result.degradations}
    assert rows["source_optional"]["severity"] == "info"
    assert rows["source_optional"]["action_required"] is False
    assert rows["source_core"]["severity"] == "warn"
    assert rows["source_enabled"]["severity"] == "warn"


def test_persisted_options_do_not_reset_data_freshness(tmp_path, monkeypatch):
    from core.data.options_collector import DeribitOptionsCollector, OptionsSnapshot
    collector = DeribitOptionsCollector()
    monkeypatch.setattr(collector, "_PERSIST_DIR", tmp_path)
    old = OptionsSnapshot("BTC", .5, 0., 1., 10, 10, datetime.now(timezone.utc) - timedelta(days=2))
    collector._persist_snapshot(old)
    assert collector.load_cached_snapshot().timestamp == old.timestamp
    fresh = OptionsSnapshot("BTC", .6, .1, .9, 11, 11)
    fetch = AsyncMock(return_value=fresh)
    monkeypatch.setattr(collector, "_fetch_from_api", fetch)
    assert asyncio.run(collector.fetch_snapshot()) == fresh
    fetch.assert_awaited_once()


def test_deribit_uses_windows_system_proxy(monkeypatch):
    import core.data.options_collector as module
    monkeypatch.setattr(module.settings, "HTTP_PROXY", None)
    monkeypatch.setattr(module.settings, "HTTPS_PROXY", None)
    monkeypatch.setattr(module, "getproxies", lambda: {"https": "http://127.0.0.1:9876"})
    monkeypatch.setattr(module, "proxy_bypass", lambda _: False)
    captured = {}
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def get(self, url, **kwargs):
            captured.update(kwargs)
            raise RuntimeError("test ends before network access")
    monkeypatch.setattr(module.aiohttp, "ClientSession", Session)
    asyncio.run(module.DeribitOptionsCollector()._fetch_from_api("BTC"))
    assert captured["proxy"] == "http://127.0.0.1:9876"


@pytest.mark.parametrize("status", [401, 403, 429])
def test_collectors_do_not_immediately_retry_auth_or_rate_limits(status, monkeypatch):
    from core.news.collectors.common import BaseNewsCollector
    collector = BaseNewsCollector()
    response = requests.Response()
    response.status_code = status
    response.url = "https://example.test/news"
    call = Mock(return_value=response)
    monkeypatch.setattr(collector._session, "request", call)
    try:
        with pytest.raises(RuntimeError) as failure:
            collector._request(response.url)
        assert call.call_count == 1
        assert failure.value.__cause__.response.status_code == status
    finally:
        collector.close()


def test_rate_limit_backoff_honors_retry_after_and_auth_pause():
    from core.news.collectors.manager import _retry_cooldown
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "4000"
    error = requests.HTTPError("limited", response=response)
    assert _retry_cooldown(error, 0, 900) == 4000
    assert _retry_cooldown(error, 3, 900) == 7200
    assert _retry_cooldown(RuntimeError("401 unauthorized"), 0, 900) == 3600


def test_success_clears_stale_news_pause(monkeypatch):
    from core.news.storage import db
    row = SimpleNamespace(source="gdelt", cursor_type="ts", cursor_value=None, updated_at=None, last_error="429", error_count=3, success_count=0, failure_count=3, last_success_at=None, paused_until=datetime.now(timezone.utc) + timedelta(hours=1))
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: row)), flush=AsyncMock())
    @asynccontextmanager
    async def scope(): yield session
    monkeypatch.setattr(db, "news_session_scope", scope)
    monkeypatch.setattr(db, "_row_to_state_dict", lambda value: vars(value))
    result = asyncio.run(db.set_source_state("gdelt", mark_success=True))
    assert result["paused_until"] is None
    assert result["last_error"] is None
    assert result["error_count"] == 0


def test_news_breaker_does_not_shorten_provider_pause(monkeypatch):
    from core.news.service import worker
    until = datetime.now(timezone.utc) + timedelta(hours=1)
    monkeypatch.setattr(worker.news_db, "get_source_state", AsyncMock(side_effect=[None, {"error_count": 4, "paused_until": until.isoformat()}]))
    monkeypatch.setattr(worker.news_db, "save_news_raw", AsyncMock(return_value={"inserted": []}))
    monkeypatch.setattr(worker.news_db, "enqueue_llm_tasks", AsyncMock(return_value={}))
    persist = AsyncMock()
    monkeypatch.setattr(worker.news_db, "set_source_state", persist)
    collector = SimpleNamespace(pull_latest_incremental=AsyncMock(return_value={"items": [], "source_stats": {"gdelt": {"errors": ["429"]}}}))
    monkeypatch.setattr(worker, "MultiSourceNewsCollector", lambda _: collector)
    asyncio.run(worker.pull_source_once({}, "gdelt"))
    assert persist.call_args.kwargs["paused_until"] >= until


def test_supplementary_cache_refresh_is_due_only_and_budgeted(monkeypatch):
    from core.data import source_cache_maintenance as module
    from core.data.options_collector import OptionsSnapshot
    monkeypatch.setattr(module.options_collector, "fetch_snapshot", AsyncMock(return_value=OptionsSnapshot("BTC", .5, 0., 1., 10, 10)))
    monkeypatch.setattr(module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(module, "_dataset_due", lambda name, *_: name == "options_info")
    update = AsyncMock(return_value={"updated": [], "errors": []})
    monkeypatch.setattr(module, "update_coinglass_cache", update)
    monkeypatch.setattr(module, "_sync_funding_cache", lambda _: {"rows": 3})
    asyncio.run(module.refresh_source_caches())
    assert update.call_args.kwargs["datasets"] == ["options_info"]
    assert update.call_args.kwargs["manual"] is False
    monkeypatch.setattr(module, "_dataset_due", lambda *_: False)
    update.reset_mock()
    asyncio.run(module.refresh_source_caches())
    update.assert_not_called()


def test_dataset_missing_timestamp_is_due(monkeypatch):
    from core.data import source_cache_maintenance as module
    monkeypatch.setattr(module, "load_dataset_rows_for_symbol", lambda *_: pd.DataFrame({"source_ts": [None]}))
    assert module._dataset_due("options_info", "BTC", 3600)


def test_agent_health_uses_recent_model_failure_without_account_details():
    from web.api.ai_research import _agent_model_runtime_issue
    status = {"running": True, "last_run_at": datetime.now(timezone.utc).isoformat(), "last_diagnostics": {
        "model_feedback": {"raw_error": 'codex_http_403:{"code":"insufficient_user_quota","message":"private-account-detail"}'},
    }}
    assert _agent_model_runtime_issue(status) == "模型供应商额度不足"
    status["last_diagnostics"] = {}
    assert _agent_model_runtime_issue(status) is None
    status["running"] = False
    assert _agent_model_runtime_issue(status) is None
