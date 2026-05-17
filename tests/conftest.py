from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

os.environ.setdefault(
    "GATE_COUNTERFACTUAL_AUDIT_PATH",
    str(ROOT / ".pytest_tmp" / "runtime_audit" / "gate_counterfactuals.jsonl"),
)


@pytest.fixture(autouse=True)
def _isolate_runtime_side_effect_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "GATE_COUNTERFACTUAL_AUDIT_PATH",
        str(tmp_path / "runtime_audit" / "gate_counterfactuals.jsonl"),
    )

    ai_module = sys.modules.get("web.api.ai_research")
    if ai_module is not None and hasattr(ai_module, "_reset_operating_mode_cache_for_tests"):
        ai_module._reset_operating_mode_cache_for_tests()
    yield
    ai_module = sys.modules.get("web.api.ai_research")
    if ai_module is not None and hasattr(ai_module, "_reset_operating_mode_cache_for_tests"):
        ai_module._reset_operating_mode_cache_for_tests()
