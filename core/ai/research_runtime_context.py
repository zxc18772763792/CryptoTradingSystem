from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from config.settings import settings


_ACTIVE_CANDIDATE_STATUSES = frozenset({"paper_running", "shadow_running", "live_candidate", "live_running"})


def _ai_research_base_dir() -> Path:
    return (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "ai").resolve()


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "/").replace(" ", "")
    if not text:
        return ""
    if "/" in text:
        return text
    for quote in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(quote) and len(text) > len(quote):
            return f"{text[:-len(quote)]}/{quote}"
    return text


def _safe_score(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_ts(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        from datetime import datetime

        return float(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0.0


def _read_registry_rows(path: Path, root_key: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.debug(f"research_runtime_context: failed to parse {path}: {exc}")
        return []
    rows = payload.get(root_key) if isinstance(payload, dict) else []
    return [dict(item) for item in rows or [] if isinstance(item, dict)]


def _load_candidates() -> List[Dict[str, Any]]:
    return _read_registry_rows(_ai_research_base_dir() / "candidates.json", "candidates")


def _load_proposal_map(proposal_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    wanted = {str(item or "").strip() for item in proposal_ids if str(item or "").strip()}
    if not wanted:
        return {}
    rows = _read_registry_rows(_ai_research_base_dir() / "proposals.json", "proposals")
    return {
        str(item.get("proposal_id") or "").strip(): item
        for item in rows
        if str(item.get("proposal_id") or "").strip() in wanted
    }


def _candidate_matches(
    candidate: Dict[str, Any],
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> bool:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    if exchange:
        candidate_exchange = str(metadata.get("exchange") or "").strip().lower()
        if candidate_exchange and candidate_exchange != exchange:
            return False
    if symbol and _normalize_symbol(candidate.get("symbol")) != symbol:
        return False
    if timeframe and str(candidate.get("timeframe") or "").strip() != timeframe:
        return False
    return True


def _validation_payload(summary: Any) -> Dict[str, Any]:
    data = summary if isinstance(summary, dict) else {}
    return {
        "decision": str(data.get("decision") or "").strip(),
        "deployment_score": _safe_score(data.get("deployment_score")),
        "oos_score": _safe_score(data.get("oos_score")),
        "wf_stability": _safe_score(data.get("wf_stability")),
        "robustness_score": _safe_score(data.get("robustness_score")),
        "reasons": list(data.get("reasons") or [])[:3],
    }


def _search_payload(value: Any) -> Dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    return {
        "loop_enabled": bool(data.get("loop_enabled", False)),
        "evaluated_drafts": int(data.get("evaluated_drafts", 0) or 0),
        "accepted_drafts": int(data.get("accepted_drafts", 0) or 0),
        "rejected_drafts": int(data.get("rejected_drafts", 0) or 0),
        "champion_draft_id": str(data.get("champion_draft_id") or "").strip(),
    }


def _lineage_payload(value: Any) -> Dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    return {
        "lineage_id": str(data.get("lineage_id") or "").strip(),
        "parent_proposal_id": str(data.get("parent_proposal_id") or "").strip(),
        "parent_candidate_id": str(data.get("parent_candidate_id") or "").strip(),
        "generation": int(data.get("generation", 0) or 0),
    }


def _candidate_payload(candidate: Dict[str, Any], proposal_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    proposal = proposal_map.get(str(candidate.get("proposal_id") or "").strip(), {})
    proposal = proposal if isinstance(proposal, dict) else {}
    search_summary = metadata.get("search_summary") if isinstance(metadata.get("search_summary"), dict) else proposal.get("search_summary")
    lineage = metadata.get("lineage") if isinstance(metadata.get("lineage"), dict) else proposal.get("lineage")
    return {
        "candidate_id": str(candidate.get("candidate_id") or "").strip(),
        "proposal_id": str(candidate.get("proposal_id") or "").strip(),
        "experiment_id": str(candidate.get("experiment_id") or "").strip(),
        "strategy": str(candidate.get("strategy") or "").strip(),
        "symbol": str(candidate.get("symbol") or "").strip(),
        "timeframe": str(candidate.get("timeframe") or "").strip(),
        "status": str(candidate.get("status") or "").strip(),
        "score": round(_safe_score(candidate.get("score")), 2),
        "promotion_target": str(candidate.get("promotion_target") or "").strip(),
        "exchange": str(metadata.get("exchange") or "").strip().lower(),
        "research_mode": str(metadata.get("research_mode") or proposal.get("research_mode") or "").strip(),
        "search_role": str(metadata.get("search_role") or "").strip(),
        "champion_candidate_id": str(metadata.get("champion_candidate_id") or "").strip(),
        "champion_strategy": str(metadata.get("champion_strategy") or "").strip(),
        "decision_engine": str(metadata.get("decision_engine") or "").strip(),
        "strategy_family": str(metadata.get("strategy_family") or "").strip(),
        "thesis": str(proposal.get("thesis") or metadata.get("llm_rationale") or "").strip(),
        "validation": _validation_payload(candidate.get("validation_summary")),
        "search": _search_payload(search_summary),
        "lineage": _lineage_payload(lineage),
    }


def _candidate_brief(candidate: Dict[str, Any]) -> Dict[str, Any]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return {
        "candidate_id": str(candidate.get("candidate_id") or "").strip(),
        "strategy": str(candidate.get("strategy") or "").strip(),
        "status": str(candidate.get("status") or "").strip(),
        "score": round(_safe_score(candidate.get("score")), 2),
        "search_role": str(metadata.get("search_role") or "").strip(),
        "promotion_target": str(candidate.get("promotion_target") or "").strip(),
    }


def _candidate_rank(candidate: Dict[str, Any], *, prefer_active_first: bool) -> tuple[Any, ...]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    status = str(candidate.get("status") or "").strip()
    search_role = str(metadata.get("search_role") or "").strip().lower()
    champion_candidate_id = str(metadata.get("champion_candidate_id") or "").strip()
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    is_active = status in _ACTIVE_CANDIDATE_STATUSES
    is_champion = search_role == "champion" or champion_candidate_id == candidate_id
    not_filtered = not bool(metadata.get("correlation_filtered"))
    active_rank = 1 if is_active else 0
    champion_rank = 1 if is_champion else 0
    created_rank = _safe_ts(candidate.get("created_at"))
    if prefer_active_first:
        return (active_rank, champion_rank, 1 if not_filtered else 0, _safe_score(candidate.get("score")), created_rank)
    return (champion_rank, active_rank, 1 if not_filtered else 0, _safe_score(candidate.get("score")), created_rank)


def _pick_best(candidates: List[Dict[str, Any]], *, prefer_active_first: bool) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: _candidate_rank(item, prefer_active_first=prefer_active_first), reverse=True)[0]


def resolve_runtime_research_context(
    *,
    exchange: str = "",
    symbol: str = "",
    timeframe: str = "",
    strategy_name: str = "",
) -> Dict[str, Any]:
    exchange_text = str(exchange or "").strip().lower()
    symbol_text = _normalize_symbol(symbol)
    timeframe_text = str(timeframe or "").strip()
    strategy_text = str(strategy_name or "").strip()

    matching = [
        candidate
        for candidate in _load_candidates()
        if _candidate_matches(candidate, exchange=exchange_text, symbol=symbol_text, timeframe=timeframe_text)
    ]
    if not matching:
        return {
            "available": False,
            "exchange": exchange_text,
            "symbol": symbol_text,
            "timeframe": timeframe_text,
            "strategy": strategy_text,
            "candidate_count": 0,
            "selection_reason": "no_matching_candidates",
        }

    proposal_map = _load_proposal_map([str(item.get("proposal_id") or "").strip() for item in matching])
    active_candidates = [
        item for item in matching if str(item.get("status") or "").strip() in _ACTIVE_CANDIDATE_STATUSES
    ]
    strategy_candidates = [
        item for item in matching if strategy_text and str(item.get("strategy") or "").strip() == strategy_text
    ]

    research_champion = _pick_best(matching, prefer_active_first=False)
    active_runtime_candidate = _pick_best(active_candidates, prefer_active_first=True)
    strategy_candidate = _pick_best(strategy_candidates, prefer_active_first=True)

    selected_candidate = None
    selection_reason = "research_champion"
    if strategy_candidate is not None:
        selected_candidate = strategy_candidate
        selection_reason = "strategy_match"
    elif active_runtime_candidate is not None:
        selected_candidate = active_runtime_candidate
        selection_reason = "active_runtime_candidate"
    else:
        selected_candidate = research_champion

    return {
        "available": True,
        "exchange": exchange_text,
        "symbol": symbol_text,
        "timeframe": timeframe_text,
        "strategy": strategy_text,
        "candidate_count": len(matching),
        "selection_reason": selection_reason,
        "selected_candidate": _candidate_payload(selected_candidate, proposal_map) if selected_candidate is not None else {},
        "research_champion": _candidate_payload(research_champion, proposal_map) if research_champion is not None else {},
        "active_runtime_candidate": (
            _candidate_payload(active_runtime_candidate, proposal_map) if active_runtime_candidate is not None else {}
        ),
        "strategy_candidate": _candidate_payload(strategy_candidate, proposal_map) if strategy_candidate is not None else {},
        "matching_candidates": [
            _candidate_brief(item)
            for item in sorted(matching, key=lambda row: _candidate_rank(row, prefer_active_first=False), reverse=True)[:5]
        ],
    }
