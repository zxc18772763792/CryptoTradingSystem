"""Offline audit reproduction. AST extraction avoids importing project startup/config.

All connectors, auth identities, persistence, workers and logging are synthetic.
Only artifacts below this report directory are written. No network is used.
"""
from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import secrets
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, Mock

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent


def extracted(relative, names, namespace, *, keep_decorators=False):
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8-sig"))
    selected = []
    for node in tree.body:
        if getattr(node, "name", None) in names:
            if not keep_decorators and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node.decorator_list = []
            selected.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in node.targets):
            selected.append(node)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id in names:
            selected.append(node)
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *selected], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), relative, "exec"), namespace)
    return namespace


@contextlib.asynccontextmanager
async def inert_scope(**kwargs):
    yield {"status": "success", "extra": {}}


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def add(self, row):
        pass

    async def commit(self):
        pass

    async def execute(self, *args):
        return SimpleNamespace(scalar=lambda: 1)


async def run():
    result = {}
    common = dict(globals())
    rbac = extracted("core/governance/rbac.py", {"ROLE_PERMISSIONS", "permission_set_for_role", "has_permission"}, dict(common))
    has_permission = rbac["has_permission"]
    result["role_matrix"] = {r: {p: has_permission(r, p) for p in ["manage_strategies", "approve_live", "approve_risk_change", "manage_news"]} for r in ["OPERATOR", "RESEARCH_LEAD", "AUDITOR"]}

    # Exact route dependency expression, real FastAPI dependency dispatch,
    # but synthetic AUDITOR identity; reset calls a mock circuit breaker.
    async def fake_auth(request: Request):
        request.state.ops_auth = SimpleNamespace(actor="audit_reader", role="AUDITOR")
    cb = SimpleNamespace(reset_portfolio=Mock(return_value=True), reset_strategy=Mock(return_value=True), snapshot=Mock(return_value={"portfolio": {"tripped": False}}))
    actual_log_method = next(n for n in ast.parse((ROOT / "core/audit/audit_logger.py").read_text(encoding="utf-8-sig")).body if isinstance(n, ast.ClassDef) and n.name == "AuditLogger")
    actual_log_method.body = [n for n in actual_log_method.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "log"]
    logger_ns = dict(common)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), actual_log_method], type_ignores=[])), "audit_logger_extract", "exec"), logger_ns)
    audit_log = logger_ns["AuditLogger"]()
    reset_ns = dict(common, router=APIRouter(), circuit_breaker=cb, audit_logger=audit_log, require_sensitive_ops_auth=fake_auth, get_request_auth=lambda request: request.state.ops_auth)
    extracted("web/api/risk.py", {"CircuitBreakerResetRequest", "reset_circuit_breaker"}, reset_ns, keep_decorators=True)
    app = FastAPI()
    app.include_router(reset_ns["router"], prefix="/api/risk")
    response = TestClient(app).post("/api/risk/circuit-breaker/reset", json={"scope": "portfolio", "confirm": True})
    result["auditor_circuit_breaker_reset"] = {"status": response.status_code, "reset_calls": cb.reset_portfolio.call_count, "response": response.json()}
    try:
        await audit_log.log(actor="audit_reader", action="circuit_breaker.portfolio_reset", target="portfolio", details={"changed": True})
    except TypeError as error:
        result["reset_audit_signature_failure"] = str(error)

    # Capture exactly what the endpoint sends to the mode-mutating manager.
    async def fake_permission(request: Request):
        if not has_permission("OPERATOR", "manage_strategies"):
            raise HTTPException(403)
    manager = SimpleNamespace(get_strategy_info=lambda name: {"runtime_mode": "paper"}, set_strategy_runtime_mode=Mock(return_value={"ok": True, "changed": True, "runtime_mode": "live", "previous": "paper"}))
    mode_ns = dict(common, router=APIRouter(), strategy_manager=manager, require_sensitive_ops_permissions=lambda *permissions: fake_permission, _persist_if_exists=AsyncMock(return_value=True), _schedule_audit_log=Mock())
    extracted("web/api/strategies.py", {"StrategyRuntimeModeRequest", "set_strategy_runtime_mode"}, mode_ns, keep_decorators=True)
    mode_app = FastAPI()
    mode_app.include_router(mode_ns["router"], prefix="/api/strategies")
    mode_response = TestClient(mode_app).put("/api/strategies/audit-strategy/runtime-mode", json={"runtime_mode": "live", "confirm_live": True})
    result["operator_strategy_live_mode"] = {"status": mode_response.status_code, "operator_approve_live": has_permission("OPERATOR", "approve_live"), "manager_calls": manager.set_strategy_runtime_mode.call_args_list.__str__()}

    # Compare exact risk scoring semantics and request auto-application.
    service_ns = dict(common, async_session_maker=FakeSession, GovernanceAuditEvent=lambda **kwargs: kwargs, RiskChangeRequest=lambda **kwargs: SimpleNamespace(**kwargs), write_audit=AsyncMock(), new_trace_id=lambda: "synthetic-audit", _activate_risk_config=AsyncMock(return_value={"version": 2}))
    extracted("core/governance/service.py", {"_now", "_normalize_role", "_diff_dict", "_is_list_expanded", "_risk_delta_score", "_is_increase_risk", "request_risk_change"}, service_ns)
    schema_ns = extracted("core/governance/schemas.py", {"RiskConfigPayload"}, dict(common))
    base = schema_ns["RiskConfigPayload"](allowed_symbols=["BTC/USDT"], allowed_timeframes=["1h"]).model_dump()
    service_ns["get_active_risk_config"] = AsyncMock(return_value={"version": 1, "config": base})
    proposed = schema_ns["RiskConfigPayload"](**{**base, "allowed_symbols": [], "allowed_timeframes": []})
    changed = await service_ns["request_risk_change"](SimpleNamespace(actor="operator", role="OPERATOR"), proposed)
    result["allowlist_removed_without_risk_approval"] = {"status": changed["status"], "increase_risk": changed["increase_risk"], "risk_delta_score": changed["risk_delta_score"], "activation_calls": service_ns["_activate_risk_config"].call_count}
    result["allowlist_replaced_risk_score"] = service_ns["_risk_delta_score"](base, {**base, "allowed_symbols": ["ETH/USDT"]})
    rm = SimpleNamespace(max_position_size=0.1, max_daily_loss_ratio=0.02, max_leverage=3, update_parameters=Mock(), reset_halt=Mock(), _trading_halted=True, _halt_reason="daily_loss_limit")
    activate_ns = dict(common, async_session_maker=FakeSession, RiskConfig=MagicMock(), select=lambda *args: MagicMock(), func=MagicMock(), risk_manager=rm, _now=lambda: datetime.now(timezone.utc))
    activate_ns["RiskConfig"].__table__ = MagicMock()
    extracted("core/governance/service.py", {"_activate_risk_config"}, activate_ns)
    await activate_ns["_activate_risk_config"](base_version=1, config={**base, "max_leverage": 2.0}, actor=SimpleNamespace(actor="operator"))
    result["unrelated_reducing_change_resets_runtime_halt"] = {"reset_halt_calls": rm.reset_halt.call_count, "original_reason": "daily_loss_limit", "config_kill_switch": False}

    # No networking or billing: prove readonly auditor dispatches ingestion.
    ops = SimpleNamespace(OpsNewsPullRequest=BaseModel, load_service_config=lambda: {}, IngestRequest=lambda **kwargs: kwargs, run_ingest_pull_now=AsyncMock(return_value={"queued_count": 1}), _ok=lambda data: {"ok": True, "data": data})
    news_ns = dict(common, router=APIRouter(), ops_api=ops, ops_audit_scope=inert_scope, get_request_auth=lambda request: SimpleNamespace(actor="audit_reader", role="AUDITOR", client_ip="127.0.0.1"))
    extracted("core/ops/service/news_routes.py", {"news_pull_now"}, news_ns)
    await news_ns["news_pull_now"](SimpleNamespace(), SimpleNamespace(model_dump=lambda: {"sources": ["synthetic"]}))
    result["auditor_ops_news_ingestion"] = {"manage_news": has_permission("AUDITOR", "manage_news"), "ingest_calls": ops.run_ingest_pull_now.call_count}

    # A disconnect in the initial hello happens before the handler's finally.
    from fastapi import WebSocketDisconnect
    event_bus = SimpleNamespace(subscribe=AsyncMock(return_value=asyncio.Queue()), unsubscribe=AsyncMock())
    ws = SimpleNamespace(accept=AsyncMock(), send_json=AsyncMock(side_effect=WebSocketDisconnect(1006)))
    ws_ns = dict(common, WebSocketDisconnect=WebSocketDisconnect, _ws_is_authorized=lambda request: True, event_bus=event_bus, execution_engine=SimpleNamespace(get_trading_mode=lambda: "paper"))
    extracted("web/main.py", {"websocket_endpoint"}, ws_ns)
    try:
        await ws_ns["websocket_endpoint"](ws)
    except WebSocketDisconnect:
        pass
    result["websocket_hello_disconnect_subscription_leak"] = {"subscribe_calls": event_bus.subscribe.call_count, "unsubscribe_calls": event_bus.unsubscribe.call_count}

    # Paths are sandboxed beneath the audit directory, never the actual repo.
    sandbox = Path(tempfile.mkdtemp(prefix="web_security_sandbox_", dir=OUT)).resolve()
    sandbox.relative_to(OUT.resolve())
    path_ns = dict(common, ops_api=SimpleNamespace(settings=SimpleNamespace(BASE_DIR=sandbox)))
    extracted("core/ops/service/polymarket_routes.py", {"_resolve_under_repo"}, path_ns)
    extracted("prediction_markets/polymarket/paper_strategy.py", {"save_paper_strategy_profile"}, path_ns)
    victim = sandbox / "config" / "existing_config.py"
    victim.parent.mkdir(parents=True)
    victim.write_text("synthetic original configuration", encoding="utf-8")
    accepted_path = path_ns["_resolve_under_repo"]("config/existing_config.py")
    path_ns["save_paper_strategy_profile"]({"kind": "synthetic_audit_profile"}, accepted_path)
    result["profile_overwrites_repo_file"] = {"path_is_inside_audit_sandbox": True, "extension": accepted_path.suffix, "overwritten": victim.read_text(encoding="utf-8").startswith("{")}

    # Demonstrate append lost between outcome read and atomic replace.
    gate_ns = {"__name__": "audit_gate_extract"}
    exec(compile((ROOT / "core/audit/gate_counterfactuals.py").read_text(encoding="utf-8-sig"), "gate_counterfactuals", "exec"), gate_ns)
    audit_path = sandbox / "gate.jsonl"
    trace = lambda ident: {"trace_id": ident, "subject_id": ident, "gates": []}
    gate_ns["record_gate_counterfactual"](trace=trace("old"), observed_decision="block", mode="paper", path=audit_path)
    original_os = gate_ns["os"]
    original_replace = original_os.replace
    def append_then_replace(source, destination):
        gate_ns["record_gate_counterfactual"](trace=trace("new"), observed_decision="block", mode="paper", path=audit_path)
        original_replace(source, destination)
    gate_ns["os"] = SimpleNamespace(getenv=original_os.getenv, fdopen=original_os.fdopen, path=original_os.path, unlink=original_os.unlink, replace=append_then_replace)
    gate_ns["update_gate_counterfactual_outcomes"]([{"trace_id": "old", "status": "checked"}], path=audit_path)
    result["counterfactual_append_lost_at_replace"] = {"remaining_trace_ids": [row["trace_id"] for row in gate_ns["iter_gate_counterfactuals"](audit_path)], "new_record_lost": "new" not in audit_path.read_text(encoding="utf-8")}

    (OUT / "web_security_repro_results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(run())
