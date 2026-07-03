from __future__ import annotations

import os
import sys
import importlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from config.settings import settings

os.environ.setdefault(
    "GATE_COUNTERFACTUAL_AUDIT_PATH",
    str(ROOT / ".pytest_tmp" / "runtime_audit" / "gate_counterfactuals.jsonl"),
)


@pytest.fixture(autouse=True)
def _isolate_runtime_side_effect_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    original_base_dir = settings.BASE_DIR
    monkeypatch.setattr(settings, "CACHE_PATH", tmp_path / "cache", raising=False)
    monkeypatch.setenv(
        "GATE_COUNTERFACTUAL_AUDIT_PATH",
        str(tmp_path / "runtime_audit" / "gate_counterfactuals.jsonl"),
    )

    position_module = sys.modules.get("core.trading.position_manager")
    if position_module is not None:
        manager = getattr(position_module, "position_manager", None)
        if manager is not None:
            manager._storage_root = tmp_path / "cache" / "runtime_state"
            manager.clear_all()

    risk_module = sys.modules.get("core.risk.risk_manager")
    if risk_module is not None:
        manager = getattr(risk_module, "risk_manager", None)
        if manager is not None:
            manager.configure_storage(tmp_path / "cache" / "runtime_state")
            manager.clear_runtime_history()

    account_module = sys.modules.get("core.trading.account_manager")
    if account_module is None:
        monkeypatch.setattr(settings, "BASE_DIR", tmp_path / "app_base", raising=False)
        account_module = importlib.import_module("core.trading.account_manager")
        monkeypatch.setattr(settings, "BASE_DIR", original_base_dir, raising=False)
    if account_module is not None:
        manager = getattr(account_module, "account_manager", None)
        if manager is not None:
            manager._file = tmp_path / "app_base" / "data" / "config" / "accounts.json"
            manager._file.parent.mkdir(parents=True, exist_ok=True)
            manager._accounts = {}
            manager._ensure_default()
            manager._save()

    cb_module = sys.modules.get("core.risk.circuit_breaker")
    if cb_module is not None:
        cb = getattr(cb_module, "circuit_breaker", None)
        if cb is not None:
            cb._store_path = tmp_path / "cache" / "runtime_state" / "circuit_breaker.json"
            with cb._lock:
                cb._portfolio = cb_module._PortfolioState()
                cb._strategies = {}

    ai_module = sys.modules.get("web.api.ai_research")
    if ai_module is not None and hasattr(ai_module, "_reset_operating_mode_cache_for_tests"):
        ai_module._reset_operating_mode_cache_for_tests()
    yield
    position_module = sys.modules.get("core.trading.position_manager")
    if position_module is not None:
        manager = getattr(position_module, "position_manager", None)
        if manager is not None:
            manager.clear_all()
    risk_module = sys.modules.get("core.risk.risk_manager")
    if risk_module is not None:
        manager = getattr(risk_module, "risk_manager", None)
        if manager is not None:
            manager.clear_runtime_history()
    ai_module = sys.modules.get("web.api.ai_research")
    if ai_module is not None and hasattr(ai_module, "_reset_operating_mode_cache_for_tests"):
        ai_module._reset_operating_mode_cache_for_tests()
