import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.ops.service import auth
from tests.test_sensitive_api_auth import _api_identity


@pytest.mark.parametrize("method,path,payload", [
    ("post", "/api/strategies/live-one/start", None),
    ("post", "/api/strategies/start-all", None),
    ("put", "/api/strategies/paper-one/runtime-mode", {"runtime_mode": "live", "confirm_live": True}),
    ("put", "/api/strategies/live-one/params", {"params": {"period": 10}}),
])
def test_strategy_operator_cannot_mutate_live(monkeypatch, method, path, payload):
    from web.api import strategies
    monkeypatch.setattr(auth, "resolve_api_key_identity", AsyncMock(return_value=_api_identity("OPERATOR")))
    monkeypatch.setattr(strategies.strategy_manager, "get_strategy_info", lambda name: {"runtime_mode": "live" if name == "live-one" else "paper"})
    monkeypatch.setattr(strategies.strategy_manager, "list_strategies", lambda: [{"runtime_mode": "live"}])
    start = AsyncMock()
    monkeypatch.setattr(strategies.strategy_manager, "start_strategy", start)
    app = FastAPI()
    app.include_router(strategies.router, prefix="/api/strategies")
    response = getattr(TestClient(app), method)(path, json=payload, headers={"X-API-KEY": "dummy"})
    assert response.status_code == 403
    start.assert_not_awaited()


def test_paper_strategy_start_still_allowed_for_operator(monkeypatch):
    from web.api import strategies
    monkeypatch.setattr(auth, "resolve_api_key_identity", AsyncMock(return_value=_api_identity("OPERATOR")))
    monkeypatch.setattr(strategies.strategy_manager, "get_strategy_info", lambda name: {"runtime_mode": "paper"})
    monkeypatch.setattr(strategies.strategy_manager, "start_strategy", AsyncMock(return_value=True))
    monkeypatch.setattr(strategies, "_persist_if_exists", AsyncMock())
    monkeypatch.setattr(strategies, "_schedule_audit_log", lambda **kw: None)
    app = FastAPI()
    app.include_router(strategies.router, prefix="/api/strategies")
    assert TestClient(app).post("/api/strategies/paper-one/start", headers={"X-API-KEY": "dummy"}).status_code == 200


@pytest.mark.parametrize("role,expected", [("AUDITOR", 403), ("ENGINEER", 403), ("OPERATOR", 403), ("RISK_OWNER", 200)])
def test_risk_reset_authorization_and_audit(monkeypatch, role, expected):
    from web.api import risk
    monkeypatch.setattr(auth, "resolve_api_key_identity", AsyncMock(return_value=_api_identity(role)))
    monkeypatch.setattr(risk.circuit_breaker, "reset_portfolio", lambda *args: True)
    audit = AsyncMock()
    monkeypatch.setattr(risk.audit_logger, "log", audit)
    app = FastAPI()
    app.include_router(risk.router, prefix="/api/risk")
    response = TestClient(app).post("/api/risk/circuit-breaker/reset", json={"scope": "portfolio", "confirm": True}, headers={"X-API-KEY": "dummy"})
    assert response.status_code == expected
    if expected == 200:
        assert audit.await_args.kwargs["module"] == "risk"
        assert "target" in audit.await_args.kwargs["details"]
    else:
        audit.assert_not_awaited()


@pytest.mark.parametrize("base,proposed,expanded", [(["BTC"], [], True), (["BTC"], ["ETH"], True), ([], ["BTC"], False), (["BTC", "ETH"], ["ETH"], False)])
def test_allowlist_changes_detect_new_permissions(base, proposed, expanded):
    from core.governance.service import _is_list_expanded
    assert _is_list_expanded(base, proposed) is expanded


def test_profile_write_is_confined_and_cannot_overwrite(tmp_path, monkeypatch):
    from core.ops.service.polymarket_routes import _resolve_profile_output
    from config.settings import settings
    monkeypatch.setattr(settings, "BASE_DIR", tmp_path)
    with pytest.raises(ValueError):
        _resolve_profile_output(".env")
    with pytest.raises(ValueError):
        _resolve_profile_output("core/production.py")
    path = _resolve_profile_output("data/profiles/polymarket/new.json")
    path.parent.mkdir(parents=True)
    path.write_text("preserve", encoding="utf-8")
    with pytest.raises(ValueError):
        _resolve_profile_output(str(path))
    assert path.read_text() == "preserve"


def test_websocket_hello_disconnect_unsubscribes(monkeypatch):
    from web import main
    from starlette.websockets import WebSocketDisconnect
    monkeypatch.setattr(main, "_ws_is_authorized", lambda ws: True)
    queue = asyncio.Queue()
    subscribe, unsubscribe = AsyncMock(return_value=queue), AsyncMock()
    monkeypatch.setattr(main.event_bus, "subscribe", subscribe)
    monkeypatch.setattr(main.event_bus, "unsubscribe", unsubscribe)
    ws = SimpleNamespace(accept=AsyncMock(), send_json=AsyncMock(side_effect=WebSocketDisconnect()))
    asyncio.run(main.websocket_endpoint(ws))
    unsubscribe.assert_awaited_once_with(queue)


def test_counterfactual_rewrite_does_not_lose_concurrent_appends(tmp_path):
    from core.audit.gate_counterfactuals import record_gate_counterfactual, update_gate_counterfactual_outcomes, iter_gate_counterfactuals
    target = tmp_path / "audit.jsonl"
    def append(i):
        record_gate_counterfactual(trace={"trace_id": str(i)}, observed_decision="block", mode="paper", path=target)
    append(0)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(append, i) for i in range(1, 40)]
        futures += [pool.submit(update_gate_counterfactual_outcomes, [{"trace_id": "0", "outcome_ref": "checked"}], path=target) for _ in range(10)]
        for f in futures:
            f.result()
    rows = list(iter_gate_counterfactuals(target))
    assert {r["trace_id"] for r in rows} == {str(i) for i in range(40)}
    assert len(rows) == 40
    assert rows[0]["later_outcome_ref"] == "checked"


def test_replay_next_is_authenticated_post(monkeypatch):
    from web.api import data
    monkeypatch.setenv("OPS_TOKEN", "dummy-required")
    app = FastAPI()
    app.include_router(data.router, prefix="/api/data")
    client = TestClient(app)
    assert client.get("/api/data/replay/test/next").status_code == 405
    assert client.post("/api/data/replay/test/next").status_code == 401


@pytest.mark.parametrize("role,path,payload", [
    ("AUDITOR", "/ops/news/worker_run_once", {}),
    ("AUDITOR", "/ops/news/pull_now", {}),
    ("RESEARCH_LEAD", "/ops/governance/audit/query", {}),
    ("OPERATOR", "/ops/trading/submit_manual_signal", {"symbol": "BTC/USDT", "signal_type": "buy", "price": 100}),
])
def test_ops_secondary_entrypoints_require_capability(monkeypatch, role, path, payload):
    from core.ops.service.api import create_router
    monkeypatch.setenv("OPS_ALLOW_MANUAL_SIGNAL", "true")
    monkeypatch.setattr(auth, "resolve_api_key_identity", AsyncMock(return_value=_api_identity(role)))
    app = FastAPI()
    app.include_router(create_router())
    assert TestClient(app).post(path, json=payload, headers={"X-API-KEY": "dummy"}).status_code == 403
