"""Append-only counterfactual records for gate decisions."""
from __future__ import annotations

import json
import os
import tempfile
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional


DEFAULT_AUDIT_PATH = Path("data") / "audit" / "gate_counterfactuals.jsonl"
AUDIT_PATH_ENV = "GATE_COUNTERFACTUAL_AUDIT_PATH"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_audit_path(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path)
    configured = str(os.getenv(AUDIT_PATH_ENV) or "").strip()
    if configured:
        return Path(configured)
    return DEFAULT_AUDIT_PATH


def _iter_jsonl_rows(target: Path) -> Iterator[Dict[str, Any]]:
    if not target.exists():
        return
    with target.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                yield row


def _summary_rows(target: Path, limit: Optional[int]) -> List[Dict[str, Any]]:
    if limit is None:
        return list(_iter_jsonl_rows(target))
    resolved_limit = max(0, int(limit))
    if resolved_limit == 0:
        return []
    rows = deque(_iter_jsonl_rows(target), maxlen=resolved_limit)
    return list(rows)


def record_gate_counterfactual(
    *,
    trace: Dict[str, Any],
    observed_decision: str,
    mode: str,
    path: str | Path | None = None,
    later_outcome_ref: str = "",
) -> Dict[str, Any]:
    target = _resolve_audit_path(path)
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
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
    return row


def iter_gate_counterfactuals(path: str | Path | None = None) -> Iterable[Dict[str, Any]]:
    target = _resolve_audit_path(path)
    return _iter_jsonl_rows(target)


def update_gate_counterfactual_outcomes(
    outcomes: Iterable[Dict[str, Any]],
    *,
    path: str | Path | None = None,
) -> Dict[str, Any]:
    """Backfill later outcomes onto matching counterfactual audit rows.

    Match keys are intentionally simple: ``trace_id`` wins, otherwise
    ``subject_type`` + ``subject_id``. Existing outcome refs are not replaced
    unless ``force`` is truthy in the outcome item.
    """
    target = _resolve_audit_path(path)
    rows = list(iter_gate_counterfactuals(target))
    if not rows:
        return {"updated": 0, "total": 0, "path": str(target)}

    normalized = [dict(item or {}) for item in outcomes or [] if isinstance(item, dict)]
    force_available = any(bool(item.get("force")) for item in normalized)
    by_trace: Dict[str, List[int]] = {}
    by_subject: Dict[tuple[str, str], List[int]] = {}
    by_subject_any_type: Dict[str, List[int]] = {}
    for idx, outcome in enumerate(normalized):
        trace_id = str(outcome.get("trace_id") or "").strip()
        if trace_id:
            by_trace.setdefault(trace_id, []).append(idx)
        subject_id = str(outcome.get("subject_id") or "").strip()
        if not subject_id:
            continue
        subject_type = str(outcome.get("subject_type") or "").strip()
        if subject_type:
            by_subject.setdefault((subject_type, subject_id), []).append(idx)
        else:
            by_subject_any_type.setdefault(subject_id, []).append(idx)

    updated = 0
    for row in rows:
        if row.get("later_outcome_ref") and not force_available:
            continue
        candidate_indexes: List[int] = []
        trace_id = str(row.get("trace_id") or "").strip()
        if trace_id:
            candidate_indexes.extend(by_trace.get(trace_id, []))
        subject_id = str(row.get("subject_id") or "").strip()
        if subject_id:
            subject_type = str(row.get("subject_type") or "").strip()
            if subject_type:
                candidate_indexes.extend(by_subject.get((subject_type, subject_id), []))
            candidate_indexes.extend(by_subject_any_type.get(subject_id, []))

        for outcome_idx in sorted(set(candidate_indexes)):
            outcome = normalized[outcome_idx]
            if row.get("later_outcome_ref") and not bool(outcome.get("force")):
                continue
            row["later_outcome_ref"] = str(
                outcome.get("later_outcome_ref")
                or outcome.get("outcome_ref")
                or outcome.get("status")
                or ""
            )
            row["later_outcome"] = {
                key: value
                for key, value in outcome.items()
                if key not in {"force", "trace_id", "subject_id", "subject_type"}
            }
            row["outcome_recorded_at"] = _now_iso()
            updated += 1
            break

    if updated:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
            os.replace(tmp_name, target)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    return {"updated": updated, "total": len(rows), "path": str(target)}


def summarize_gate_counterfactuals(path: str | Path | None = None, *, limit: Optional[int] = None) -> Dict[str, Any]:
    target = _resolve_audit_path(path)
    rows = _summary_rows(target, limit)
    gate_counts = Counter(str(row.get("gate_code") or "unknown") for row in rows)
    downgraded = sum(1 for row in rows if str(row.get("counterfactual_decision") or "") and row.get("counterfactual_decision") != row.get("observed_decision"))
    outcome_rows = [row for row in rows if row.get("later_outcome_ref")]
    missed_alpha = 0.0
    avoided_loss = 0.0
    for row in outcome_rows:
        outcome = dict(row.get("later_outcome") or {})
        try:
            edge_delta = float(outcome.get("edge_delta") or outcome.get("counterfactual_edge_delta") or 0.0)
        except Exception:
            edge_delta = 0.0
        observed = str(row.get("observed_decision") or "").lower()
        counterfactual = str(row.get("counterfactual_decision") or "").lower()
        if counterfactual and counterfactual != observed:
            if edge_delta > 0:
                missed_alpha += edge_delta
            elif edge_delta < 0:
                avoided_loss += abs(edge_delta)
    return {
        "path": str(target),
        "total": len(rows),
        "gate_hit_counts": dict(gate_counts),
        "downgrade_rate": round(downgraded / len(rows), 6) if rows else 0.0,
        "outcome_linked_count": len(outcome_rows),
        "missed_alpha_proxy": round(missed_alpha, 6),
        "avoided_loss_proxy": round(avoided_loss, 6),
        "items": rows[-20:],
    }
