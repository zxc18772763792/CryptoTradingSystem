"""Append-only counterfactual records for gate decisions."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_AUDIT_PATH = Path("data") / "audit" / "gate_counterfactuals.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_gate_counterfactual(
    *,
    trace: Dict[str, Any],
    observed_decision: str,
    mode: str,
    path: Path = DEFAULT_AUDIT_PATH,
    later_outcome_ref: str = "",
) -> Dict[str, Any]:
    root_code = str(trace.get("root_blocker_code") or "")
    gates = list(trace.get("gates") or [])
    root_gate = next((gate for gate in gates if str(gate.get("code") or "") == root_code), None)
    if not root_gate and gates:
        root_gate = gates[0]
    row = {
        "recorded_at": _now_iso(),
        "trace_id": trace.get("trace_id"),
        "subject_type": trace.get("subject_type"),
        "subject_id": trace.get("subject_id"),
        "gate_code": root_code or (root_gate or {}).get("code") or "",
        "observed_decision": observed_decision,
        "counterfactual_decision": (root_gate or {}).get("counterfactual_decision") or "",
        "input_summary": trace.get("input_summary") or {},
        "as_of": trace.get("created_at") or _now_iso(),
        "mode": mode,
        "later_outcome_ref": later_outcome_ref,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
    return row


def iter_gate_counterfactuals(path: Path = DEFAULT_AUDIT_PATH) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def summarize_gate_counterfactuals(path: Path = DEFAULT_AUDIT_PATH, *, limit: Optional[int] = None) -> Dict[str, Any]:
    rows = list(iter_gate_counterfactuals(path))
    if limit is not None:
        rows = rows[-max(0, int(limit)) :]
    gate_counts = Counter(str(row.get("gate_code") or "unknown") for row in rows)
    downgraded = sum(1 for row in rows if str(row.get("counterfactual_decision") or "") and row.get("counterfactual_decision") != row.get("observed_decision"))
    return {
        "path": str(path),
        "total": len(rows),
        "gate_hit_counts": dict(gate_counts),
        "downgrade_rate": round(downgraded / len(rows), 6) if rows else 0.0,
        "missed_alpha_proxy": 0.0,
        "avoided_loss_proxy": 0.0,
        "items": rows[-20:],
    }

