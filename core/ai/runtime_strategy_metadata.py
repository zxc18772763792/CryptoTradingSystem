"""Helpers for attaching AI research ownership metadata to runtime strategies."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _first_text(values: Iterable[Any]) -> str:
    for value in values:
        text = _safe_text(value)
        if text:
            return text
    return ""


def _normalize_token(value: Any, *, default: str = "") -> str:
    text = _safe_text(value).lower()
    if not text:
        text = default
    return text.replace("\\", "/")


def build_ai_research_runtime_fingerprint(
    *,
    strategy: Any = "",
    symbol: Any = "",
    timeframe: Any = "",
    target_mode: Any = "",
    exchange: Any = "",
    search_role: Any = "",
) -> str:
    """Return the de-duplication key for one AI research runtime slot.

    Candidate and proposal ids are intentionally excluded. The runtime should
    keep one active champion per strategy/symbol/timeframe/mode slot instead of
    starting every equivalent candidate as its own long-running strategy.
    """
    strategy_token = _normalize_token(strategy, default="strategy")
    symbol_token = _normalize_token(symbol, default="symbol")
    timeframe_token = _normalize_token(timeframe, default="1h")
    mode_token = _normalize_token(target_mode, default="paper")
    if mode_token not in {"paper", "live"}:
        mode_token = "paper" if mode_token in {"shadow", "shadow_virtual"} else mode_token
    exchange_token = _normalize_token(exchange, default="binance")
    role_token = _normalize_token(search_role, default="champion")
    return "|".join(
        [
            "ai_research",
            mode_token,
            role_token,
            exchange_token,
            strategy_token,
            symbol_token,
            timeframe_token,
        ]
    )


def ai_research_runtime_fingerprint_for_candidate(
    candidate: Any,
    *,
    target_mode: Any = "",
    strategy_name: str = "",
) -> str:
    metadata = _as_dict(getattr(candidate, "metadata", None))
    runtime_meta = _as_dict(metadata.get("promotion_runtime"))
    promotion = getattr(candidate, "promotion", None)
    constraints = _as_dict(getattr(promotion, "constraints", None))
    mode = _first_text(
        [
            target_mode,
            runtime_meta.get("mode"),
            metadata.get("runtime_mode"),
            getattr(candidate, "promotion_target", None),
            getattr(promotion, "decision", None),
        ]
    )
    if mode == "shadow":
        mode = "paper"
    return build_ai_research_runtime_fingerprint(
        strategy=_first_text(
            [
                getattr(candidate, "strategy", None),
                metadata.get("strategy_family"),
                strategy_name,
            ]
        ),
        symbol=_first_text(
            [
                getattr(candidate, "symbol", None),
                metadata.get("symbol"),
                metadata.get("primary_symbol"),
            ]
        ),
        timeframe=_first_text(
            [
                getattr(candidate, "timeframe", None),
                metadata.get("timeframe"),
            ]
        ),
        target_mode=mode,
        exchange=_first_text(
            [
                metadata.get("exchange"),
                constraints.get("exchange"),
            ]
        ),
        search_role=metadata.get("search_role"),
    )


def ai_research_runtime_fingerprint_from_strategy_info(info: Dict[str, Any]) -> Optional[str]:
    metadata = _as_dict(info.get("metadata"))
    if metadata.get("runtime_fingerprint"):
        return _safe_text(metadata.get("runtime_fingerprint"))
    if metadata.get("source") != "ai_research" and metadata.get("owner_group") != "ai_research":
        return None
    symbols = list(info.get("symbols") or [])
    params = _as_dict(info.get("params"))
    return build_ai_research_runtime_fingerprint(
        strategy=info.get("strategy_type") or metadata.get("strategy_family") or info.get("name"),
        symbol=symbols[0] if symbols else metadata.get("symbol"),
        timeframe=info.get("timeframe") or metadata.get("timeframe"),
        target_mode=info.get("runtime_mode") or metadata.get("runtime_mode") or metadata.get("promotion_target"),
        exchange=info.get("exchange") or params.get("exchange") or metadata.get("exchange"),
        search_role=metadata.get("search_role"),
    )


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
    payload["runtime_fingerprint"] = ai_research_runtime_fingerprint_for_candidate(
        candidate,
        target_mode=target_mode,
        strategy_name=strategy_name,
    )

    allocation_pct = metadata.get("allocation_pct")
    if allocation_pct is not None:
        payload["allocation_pct"] = allocation_pct
    if constraints:
        payload["promotion_constraints"] = constraints
    if metadata.get("autonomy_handoff_requested"):
        payload["autonomy_handoff_requested"] = True
        payload["autonomy_mode"] = _safe_text(metadata.get("autonomy_mode") or "watch")
    if isinstance(metadata.get("autonomy_watch_scope"), dict):
        payload["autonomy_watch_scope"] = dict(metadata.get("autonomy_watch_scope") or {})

    return {
        key: value
        for key, value in payload.items()
        if value not in ("", None, {})
    }
