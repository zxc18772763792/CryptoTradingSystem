"""Helpers for attaching AI research ownership metadata to runtime strategies."""
from __future__ import annotations

from typing import Any, Dict


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def build_ai_research_strategy_metadata(
    candidate: Any,
    *,
    strategy_name: str = "",
    target_mode: str = "",
) -> Dict[str, Any]:
    """Build structured ownership metadata for AI research runtime strategies."""
    metadata = _as_dict(getattr(candidate, "metadata", None))
    runtime_meta = _as_dict(metadata.get("promotion_runtime"))
    promotion = getattr(candidate, "promotion", None)
    constraints = _as_dict(getattr(promotion, "constraints", None))
    lineage = _as_dict(metadata.get("lineage"))

    payload: Dict[str, Any] = {
        "source": "ai_research",
        "source_label": "AI研究",
        "owner_group": "ai_research",
        "registered_from": "candidate_runtime",
        "registered_strategy_name": _safe_text(
            strategy_name
            or metadata.get("registered_strategy_name")
            or runtime_meta.get("registered_strategy_name")
        ),
        "candidate_id": _safe_text(getattr(candidate, "candidate_id", None)),
        "proposal_id": _safe_text(getattr(candidate, "proposal_id", None)),
        "experiment_id": _safe_text(getattr(candidate, "experiment_id", None)),
        "promotion_target": _safe_text(
            getattr(candidate, "promotion_target", None)
            or getattr(promotion, "decision", None)
        ),
        "runtime_mode": _safe_text(target_mode or runtime_meta.get("mode")),
        "search_role": _safe_text(metadata.get("search_role")),
        "research_mode": _safe_text(metadata.get("research_mode")),
        "decision_engine": _safe_text(metadata.get("decision_engine")),
        "strategy_family": _safe_text(metadata.get("strategy_family")),
        "champion_candidate_id": _safe_text(metadata.get("champion_candidate_id")),
        "parent_candidate_id": _safe_text(
            lineage.get("parent_candidate_id") or metadata.get("parent_candidate_id")
        ),
        "parent_proposal_id": _safe_text(
            lineage.get("parent_proposal_id") or metadata.get("parent_proposal_id")
        ),
        "lineage_id": _safe_text(lineage.get("lineage_id")),
    }

    allocation_pct = metadata.get("allocation_pct")
    if allocation_pct is not None:
        payload["allocation_pct"] = allocation_pct
    if constraints:
        payload["promotion_constraints"] = constraints

    return {
        key: value
        for key, value in payload.items()
        if value not in ("", None, {})
    }
