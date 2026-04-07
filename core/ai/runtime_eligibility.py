from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from config.settings import settings


_SCHEMA_VERSION = "runtime_eligibility.v1"
_ACTIVE_CANDIDATE_STATUSES = frozenset({"paper_running", "shadow_running", "live_candidate", "live_running"})
_DEFAULT_MAX_AGE_MINUTES = 240


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _runtime_snapshot_path() -> Path:
    return (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "runtime" / "eligibility_snapshot.json").resolve()


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_ts(value: Any) -> float:
    parsed = _safe_parse_iso(value)
    if parsed is None:
        return 0.0
    return float(parsed.timestamp())


def _dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _read_registry_rows(path: Path, root_key: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.debug(f"runtime_eligibility: failed to parse {path}: {exc}")
        return []
    rows = payload.get(root_key) if isinstance(payload, dict) else []
    return [dict(item) for item in rows or [] if isinstance(item, dict)]


def _load_registry_candidates() -> List[Dict[str, Any]]:
    return _read_registry_rows(_ai_research_base_dir() / "candidates.json", "candidates")


def _load_registry_proposal_map(proposal_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    wanted = {str(item or "").strip() for item in proposal_ids if str(item or "").strip()}
    if not wanted:
        return {}
    rows = _read_registry_rows(_ai_research_base_dir() / "proposals.json", "proposals")
    return {
        str(item.get("proposal_id") or "").strip(): item
        for item in rows
        if str(item.get("proposal_id") or "").strip() in wanted
    }


def _validation_payload(summary: Any) -> Dict[str, Any]:
    data = summary if isinstance(summary, dict) else {}
    return {
        "decision": str(data.get("decision") or "").strip(),
        "deployment_score": _safe_float(data.get("deployment_score"), 0.0),
        "oos_score": _safe_float(data.get("oos_score"), 0.0),
        "wf_stability": _safe_float(data.get("wf_stability"), 0.0),
        "robustness_score": _safe_float(data.get("robustness_score"), 0.0),
        "reasons": list(data.get("reasons") or [])[:3],
    }


def _resolve_runtime_mode_cap(status: str, promotion_target: str) -> str:
    status_text = str(status or "").strip()
    target_text = str(promotion_target or "").strip().lower()
    if status_text == "live_running":
        return "live_execute"
    if status_text == "live_candidate":
        return "live_candidate_only"
    if status_text == "paper_running":
        return "paper_execute"
    if status_text == "shadow_running":
        return "shadow"
    if target_text == "live_candidate":
        return "live_candidate_only"
    if target_text == "paper":
        return "paper_execute"
    if target_text == "shadow":
        return "shadow"
    return "observe"


def _resolve_max_age_minutes(candidate: Dict[str, Any], promotion: Dict[str, Any]) -> int:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    constraints = promotion.get("constraints") if isinstance(promotion.get("constraints"), dict) else {}
    raw = (
        metadata.get("runtime_eligibility_max_age_minutes")
        or constraints.get("max_age_minutes")
        or _DEFAULT_MAX_AGE_MINUTES
    )
    value = _safe_int(raw, _DEFAULT_MAX_AGE_MINUTES)
    return max(5, min(10080, value))


def _build_eligibility_record(
    candidate: Dict[str, Any],
    *,
    proposal_map: Dict[str, Dict[str, Any]],
    generated_at: datetime,
) -> Dict[str, Any]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    proposal_id = str(candidate.get("proposal_id") or "").strip()
    proposal = proposal_map.get(proposal_id, {})
    proposal = proposal if isinstance(proposal, dict) else {}
    status = str(candidate.get("status") or "").strip()
    promotion_target = str(candidate.get("promotion_target") or "").strip()
    promotion = candidate.get("promotion") if isinstance(candidate.get("promotion"), dict) else {}
    validation = _validation_payload(candidate.get("validation_summary"))

    runtime_mode_cap = _resolve_runtime_mode_cap(status, promotion_target)
    max_age_minutes = _resolve_max_age_minutes(candidate, promotion)
    expires_at = generated_at + timedelta(minutes=max_age_minutes)
    is_active = status in _ACTIVE_CANDIDATE_STATUSES
    validation_decision = str(validation.get("decision") or "").strip().lower()

    reason_codes: List[str] = []
    if not is_active:
        reason_codes.append("CANDIDATE_NOT_ACTIVE")
    if not validation_decision:
        reason_codes.append("MISSING_VALIDATION_SUMMARY")
    elif validation_decision == "reject":
        reason_codes.append("REJECTED_BY_VALIDATION")
    if runtime_mode_cap == "observe":
        reason_codes.append("RUNTIME_MODE_OBSERVE_ONLY")
    if not promotion_target:
        reason_codes.append("NO_PROMOTION_TARGET")

    eligible_for_autonomy = is_active and validation_decision != "reject" and runtime_mode_cap != "observe"
    if not eligible_for_autonomy:
        reason_codes.append("NOT_ELIGIBLE_FOR_AUTONOMY")

    return {
        "exchange": str(metadata.get("exchange") or "").strip().lower(),
        "symbol": _normalize_symbol(candidate.get("symbol")),
        "timeframe": str(candidate.get("timeframe") or "").strip(),
        "strategy": str(candidate.get("strategy") or "").strip(),
        "candidate_id": str(candidate.get("candidate_id") or "").strip(),
        "proposal_id": proposal_id,
        "experiment_id": str(candidate.get("experiment_id") or "").strip(),
        "status": status,
        "score": round(_safe_float(candidate.get("score"), 0.0), 2),
        "promotion_target": promotion_target,
        "runtime_mode_cap": runtime_mode_cap,
        "eligible_for_autonomy": bool(eligible_for_autonomy),
        "require_live_review": True,
        "max_age_minutes": int(max_age_minutes),
        "generated_at": _iso_utc(generated_at),
        "expires_at": _iso_utc(expires_at),
        "validation": validation,
        "search_role": str(metadata.get("search_role") or "").strip(),
        "champion_candidate_id": str(metadata.get("champion_candidate_id") or "").strip(),
        "champion_strategy": str(metadata.get("champion_strategy") or "").strip(),
        "decision_engine": str(metadata.get("decision_engine") or "").strip(),
        "strategy_family": str(metadata.get("strategy_family") or "").strip(),
        "research_mode": str(metadata.get("research_mode") or proposal.get("research_mode") or "").strip(),
        "thesis": str(proposal.get("thesis") or metadata.get("llm_rationale") or "").strip(),
        "created_at": str(candidate.get("created_at") or "").strip(),
        "reason_codes": _dedupe_keep_order(reason_codes),
    }


def refresh_runtime_eligibility_snapshot(
    *,
    candidates: Optional[List[Dict[str, Any]]] = None,
    proposals: Optional[List[Dict[str, Any]]] = None,
    snapshot_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Rebuild and persist runtime eligibility snapshot from research outputs."""
    generated_at = _now_utc()
    rows = [dict(item) for item in (candidates or _load_registry_candidates()) if isinstance(item, dict)]
    if proposals is None:
        proposal_map = _load_registry_proposal_map([str(item.get("proposal_id") or "").strip() for item in rows])
    else:
        proposal_map = {
            str(item.get("proposal_id") or "").strip(): dict(item)
            for item in proposals
            if isinstance(item, dict) and str(item.get("proposal_id") or "").strip()
        }

    records = [
        _build_eligibility_record(item, proposal_map=proposal_map, generated_at=generated_at)
        for item in rows
    ]
    records.sort(
        key=lambda item: (
            1 if str(item.get("status") or "").strip() in _ACTIVE_CANDIDATE_STATUSES else 0,
            1
            if str(item.get("search_role") or "").strip().lower() == "champion"
            or str(item.get("champion_candidate_id") or "").strip() == str(item.get("candidate_id") or "").strip()
            else 0,
            float(item.get("score") or 0.0),
            _safe_ts(item.get("created_at")),
        ),
        reverse=True,
    )
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": _iso_utc(generated_at),
        "source": "research_registry_refresh",
        "total_records": len(records),
        "records": records,
    }
    path = (snapshot_path or _runtime_snapshot_path()).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    payload["snapshot_path"] = str(path)
    return payload


def load_runtime_eligibility_snapshot(*, snapshot_path: Optional[Path] = None) -> Dict[str, Any]:
    path = (snapshot_path or _runtime_snapshot_path()).resolve()
    if not path.exists():
        return {
            "schema_version": _SCHEMA_VERSION,
            "generated_at": None,
            "source": "runtime_eligibility_snapshot_missing",
            "total_records": 0,
            "records": [],
            "snapshot_path": str(path),
            "reason_codes": ["SNAPSHOT_MISSING"],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"runtime_eligibility: failed to parse snapshot {path}: {exc}")
        return {
            "schema_version": _SCHEMA_VERSION,
            "generated_at": None,
            "source": "runtime_eligibility_snapshot_parse_failed",
            "total_records": 0,
            "records": [],
            "snapshot_path": str(path),
            "reason_codes": ["SNAPSHOT_PARSE_FAILED"],
        }
    rows = payload.get("records") if isinstance(payload, dict) else []
    rows = [dict(item) for item in rows or [] if isinstance(item, dict)]
    return {
        "schema_version": str(payload.get("schema_version") or _SCHEMA_VERSION),
        "generated_at": str(payload.get("generated_at") or "").strip() or None,
        "source": str(payload.get("source") or "runtime_eligibility_snapshot").strip(),
        "total_records": _safe_int(payload.get("total_records"), len(rows)),
        "records": rows,
        "snapshot_path": str(path),
        "reason_codes": [],
    }


def _record_matches(
    record: Dict[str, Any],
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> bool:
    if exchange:
        row_exchange = str(record.get("exchange") or "").strip().lower()
        if row_exchange and row_exchange != exchange:
            return False
    if symbol and _normalize_symbol(record.get("symbol")) != symbol:
        return False
    if timeframe and str(record.get("timeframe") or "").strip() != timeframe:
        return False
    return True


def _record_with_expiry(record: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    out = dict(record)
    reason_codes = list(out.get("reason_codes") or [])
    expires_at = _safe_parse_iso(out.get("expires_at"))
    is_expired = bool(expires_at is not None and expires_at < now)
    out["is_expired"] = is_expired
    eligible = bool(out.get("eligible_for_autonomy"))
    if is_expired:
        reason_codes.append("ELIGIBILITY_EXPIRED")
        eligible = False
    out["eligible_for_autonomy"] = eligible
    out["reason_codes"] = _dedupe_keep_order(reason_codes)
    return out


def _record_rank(record: Dict[str, Any], *, prefer_active_first: bool) -> tuple[Any, ...]:
    status = str(record.get("status") or "").strip()
    search_role = str(record.get("search_role") or "").strip().lower()
    champion_candidate_id = str(record.get("champion_candidate_id") or "").strip()
    candidate_id = str(record.get("candidate_id") or "").strip()
    is_active = status in _ACTIVE_CANDIDATE_STATUSES
    is_champion = search_role == "champion" or champion_candidate_id == candidate_id
    eligible = bool(record.get("eligible_for_autonomy"))
    not_expired = 0 if bool(record.get("is_expired")) else 1
    base = (
        1 if eligible else 0,
        not_expired,
        1 if is_active else 0,
        1 if is_champion else 0,
        _safe_float(record.get("score"), 0.0),
        _safe_ts(record.get("created_at")),
    )
    if prefer_active_first:
        return base
    return (base[0], base[1], base[3], base[2], base[4], base[5])


def _pick_best(records: List[Dict[str, Any]], *, prefer_active_first: bool) -> Optional[Dict[str, Any]]:
    if not records:
        return None
    return sorted(records, key=lambda item: _record_rank(item, prefer_active_first=prefer_active_first), reverse=True)[0]


def _candidate_payload(record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    return {
        "candidate_id": str(record.get("candidate_id") or "").strip(),
        "proposal_id": str(record.get("proposal_id") or "").strip(),
        "experiment_id": str(record.get("experiment_id") or "").strip(),
        "strategy": str(record.get("strategy") or "").strip(),
        "symbol": str(record.get("symbol") or "").strip(),
        "timeframe": str(record.get("timeframe") or "").strip(),
        "status": str(record.get("status") or "").strip(),
        "score": round(_safe_float(record.get("score"), 0.0), 2),
        "promotion_target": str(record.get("promotion_target") or "").strip(),
        "exchange": str(record.get("exchange") or "").strip().lower(),
        "research_mode": str(record.get("research_mode") or "").strip(),
        "search_role": str(record.get("search_role") or "").strip(),
        "champion_candidate_id": str(record.get("champion_candidate_id") or "").strip(),
        "champion_strategy": str(record.get("champion_strategy") or "").strip(),
        "decision_engine": str(record.get("decision_engine") or "").strip(),
        "strategy_family": str(record.get("strategy_family") or "").strip(),
        "thesis": str(record.get("thesis") or "").strip(),
        "validation": dict(record.get("validation") or {}),
        "reason_codes": list(record.get("reason_codes") or []),
    }


def _brief_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "candidate_id": str(record.get("candidate_id") or "").strip(),
        "strategy": str(record.get("strategy") or "").strip(),
        "status": str(record.get("status") or "").strip(),
        "score": round(_safe_float(record.get("score"), 0.0), 2),
        "search_role": str(record.get("search_role") or "").strip(),
        "promotion_target": str(record.get("promotion_target") or "").strip(),
        "eligible_for_autonomy": bool(record.get("eligible_for_autonomy")),
        "is_expired": bool(record.get("is_expired")),
        "reason_codes": list(record.get("reason_codes") or []),
    }


def resolve_runtime_eligibility_context(
    *,
    exchange: str = "",
    symbol: str = "",
    timeframe: str = "",
    strategy_name: str = "",
    snapshot: Optional[Dict[str, Any]] = None,
    auto_refresh_if_missing: bool = True,
) -> Dict[str, Any]:
    exchange_text = str(exchange or "").strip().lower()
    symbol_text = _normalize_symbol(symbol)
    timeframe_text = str(timeframe or "").strip()
    strategy_text = str(strategy_name or "").strip()
    now = _now_utc()

    loaded = dict(snapshot or load_runtime_eligibility_snapshot())
    reason_codes = list(loaded.get("reason_codes") or [])
    source = str(loaded.get("source") or "runtime_eligibility_snapshot").strip()
    records = [dict(item) for item in loaded.get("records", []) if isinstance(item, dict)]
    if auto_refresh_if_missing and not records:
        try:
            refreshed = refresh_runtime_eligibility_snapshot()
            loaded = dict(refreshed)
            source = "runtime_eligibility_snapshot_refreshed"
            records = [dict(item) for item in refreshed.get("records", []) if isinstance(item, dict)]
            reason_codes.append("SNAPSHOT_REFRESHED")
        except Exception as exc:
            logger.warning(f"runtime_eligibility: snapshot refresh failed: {exc}")
            reason_codes.append("SNAPSHOT_REFRESH_FAILED")

    matching = [
        _record_with_expiry(item, now)
        for item in records
        if _record_matches(item, exchange=exchange_text, symbol=symbol_text, timeframe=timeframe_text)
    ]
    if not matching:
        return {
            "available": False,
            "exchange": exchange_text,
            "symbol": symbol_text,
            "timeframe": timeframe_text,
            "strategy": strategy_text,
            "candidate_count": 0,
            "selection_reason": "no_matching_eligibility",
            "reason_codes": _dedupe_keep_order(reason_codes + ["NO_MATCHING_ELIGIBILITY"]),
            "data_source": source,
            "snapshot_generated_at": loaded.get("generated_at"),
            "snapshot_path": loaded.get("snapshot_path"),
            "eligibility_contract": {
                "schema_version": str(loaded.get("schema_version") or _SCHEMA_VERSION),
                "source": source,
                "generated_at": loaded.get("generated_at"),
            },
        }

    strategy_records = [
        item for item in matching if strategy_text and str(item.get("strategy") or "").strip() == strategy_text
    ]
    active_records = [
        item for item in matching if str(item.get("status") or "").strip() in _ACTIVE_CANDIDATE_STATUSES
    ]

    research_champion = _pick_best(matching, prefer_active_first=False)
    active_runtime_candidate = _pick_best(active_records, prefer_active_first=True)
    strategy_candidate = _pick_best(strategy_records, prefer_active_first=True)

    if strategy_candidate is not None:
        selected = strategy_candidate
        selection_reason = "strategy_match"
    elif active_runtime_candidate is not None:
        selected = active_runtime_candidate
        selection_reason = "active_runtime_candidate"
    else:
        selected = research_champion
        selection_reason = "research_champion"

    selected_reason_codes = list(selected.get("reason_codes") or []) if isinstance(selected, dict) else []
    return {
        "available": True,
        "exchange": exchange_text,
        "symbol": symbol_text,
        "timeframe": timeframe_text,
        "strategy": strategy_text,
        "candidate_count": len(matching),
        "selection_reason": selection_reason,
        "reason_codes": _dedupe_keep_order(reason_codes + selected_reason_codes),
        "data_source": source,
        "snapshot_generated_at": loaded.get("generated_at"),
        "snapshot_path": loaded.get("snapshot_path"),
        "eligibility_contract": {
            "schema_version": str(loaded.get("schema_version") or _SCHEMA_VERSION),
            "source": source,
            "generated_at": loaded.get("generated_at"),
        },
        "selected_eligibility": dict(selected or {}),
        "selected_candidate": _candidate_payload(selected),
        "research_champion": _candidate_payload(research_champion),
        "active_runtime_candidate": _candidate_payload(active_runtime_candidate),
        "strategy_candidate": _candidate_payload(strategy_candidate),
        "matching_candidates": [
            _brief_payload(item)
            for item in sorted(matching, key=lambda row: _record_rank(row, prefer_active_first=False), reverse=True)[:5]
        ],
    }

