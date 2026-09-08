"""Strategy persistence helpers for restart recovery."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import select

from config.settings import settings
from config.database import Strategy as StrategyModel
from config.database import async_session_maker
from core.ai.runtime_strategy_metadata import build_ai_research_runtime_fingerprint
from core.strategies.strategy_manager import strategy_manager


def _normalize_restore_mode(value: Any, default: str = "paper") -> str:
    text = str(value or default).strip().lower()
    return "live" if text == "live" else "paper"


def _get_strategy_classes() -> Dict[str, Any]:
    classes: Dict[str, Any] = {}
    try:
        import strategies as strategy_module
        from strategies import ALL_STRATEGIES

        for class_name in ALL_STRATEGIES:
            klass = getattr(strategy_module, class_name, None)
            if klass is not None:
                classes[class_name] = klass
    except Exception as e:
        logger.warning(f"Failed to load strategy classes: {e}")
    return classes


def _build_payload(info: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(info.get("params") or {})
    runtime = dict(info.get("runtime") or {})
    metadata = dict(info.get("metadata") or {}) if isinstance(info.get("metadata"), dict) else {}
    runtime_mode = str(
        info.get("runtime_mode")
        or runtime.get("runtime_mode")
        or metadata.get("runtime_mode")
        or ""
    ).strip().lower()
    if runtime_mode in {"paper", "live"}:
        metadata.setdefault("runtime_mode", runtime_mode)
    return {
        "user_params": params,
        "symbols": list(info.get("symbols") or []),
        "timeframe": str(info.get("timeframe") or "1h"),
        "exchange": str(info.get("exchange") or params.get("exchange") or "gate"),
        "allocation": float(info.get("allocation") or settings.DEFAULT_STRATEGY_ALLOCATION),
        "runtime_limit_minutes": runtime.get("runtime_limit_minutes"),
        "runtime_started_at": runtime.get("started_at"),
        "runtime_mode": runtime_mode if runtime_mode in {"paper", "live"} else None,
        "state": str(info.get("state") or "idle"),
        "metadata": metadata,
    }


def _parse_runtime_anchor(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ai_runtime_fingerprint_from_payload(
    *,
    name: str,
    strategy_type: str,
    payload: Dict[str, Any],
) -> Optional[str]:
    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), dict) else {}
    if metadata.get("runtime_fingerprint"):
        return str(metadata.get("runtime_fingerprint") or "").strip() or None
    if metadata.get("source") != "ai_research" and metadata.get("owner_group") != "ai_research":
        return None
    user_params = dict(payload.get("user_params") or {})
    symbols = list(payload.get("symbols") or [])
    return build_ai_research_runtime_fingerprint(
        strategy=strategy_type or metadata.get("strategy_family") or name,
        symbol=symbols[0] if symbols else metadata.get("symbol"),
        timeframe=payload.get("timeframe") or metadata.get("timeframe"),
        target_mode=payload.get("runtime_mode") or metadata.get("runtime_mode") or metadata.get("promotion_target"),
        exchange=payload.get("exchange") or user_params.get("exchange") or metadata.get("exchange"),
        search_role=metadata.get("search_role"),
    )


def _row_sort_time(row: Any) -> datetime:
    value = getattr(row, "updated_at", None) or getattr(row, "created_at", None)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _research_restore_issue(payload: Dict[str, Any], candidates: Dict[str, Any]) -> Optional[str]:
    metadata = dict(payload.get("metadata") or {})
    if metadata.get("source") != "ai_research" and metadata.get("owner_group") != "ai_research":
        return None
    candidate = candidates.get(str(metadata.get("candidate_id") or ""))
    if candidate is None:
        return "ai_research_candidate_missing"
    if candidate.status not in {"paper_running", "shadow_running", "live_candidate", "live_running"}:
        return "ai_research_candidate_not_running"
    if metadata.get("proposal_id") != candidate.proposal_id or metadata.get("experiment_id") != candidate.experiment_id:
        return "ai_research_lineage_mismatch"
    return None


def _select_ai_runtime_restore_winners(rows: List[Any]) -> Dict[str, str]:
    grouped: Dict[str, List[Any]] = {}
    for row in rows:
        payload = dict(row.params or {})
        state = str(payload.get("state") or ("running" if row.is_active else "stopped")).lower()
        if state != "running":
            continue
        fingerprint = _ai_runtime_fingerprint_from_payload(
            name=str(row.name),
            strategy_type=str(row.type or ""),
            payload=payload,
        )
        if not fingerprint:
            continue
        grouped.setdefault(fingerprint, []).append(row)
    winners: Dict[str, str] = {}
    for fingerprint, candidates in grouped.items():
        winner = sorted(candidates, key=lambda item: (_row_sort_time(item), str(item.name)))[-1]
        winners[fingerprint] = str(winner.name)
    return winners


async def persist_strategy_snapshot(name: str, state_override: Optional[str] = None) -> bool:
    """Persist current strategy manager state for one strategy.

    ``True`` is the durability acknowledgement.  Callers handling a control
    plane mutation must treat ``False`` as a failed mutation instead of
    reporting success based only on the in-memory strategy manager state.
    """
    info = strategy_manager.get_strategy_info(name)
    if not info:
        return False

    payload = _build_payload(info)
    if state_override:
        payload["state"] = state_override

    strategy_type = str(info.get("strategy_type") or "")
    if not strategy_type:
        return False

    try:
        async with async_session_maker() as session:
            result = await session.execute(select(StrategyModel).where(StrategyModel.name == name))
            row = result.scalars().first()
            if row is None:
                row = StrategyModel(name=name, type=strategy_type)

            row.type = strategy_type
            row.params = payload
            row.is_active = payload.get("state") == "running"
            row.description = "strategy_runtime_snapshot"
            session.add(row)
            await session.commit()
        return True
    except Exception as e:
        logger.warning(f"Failed to persist strategy snapshot {name}: {e}")
        return False


async def delete_strategy_snapshot(name: str) -> bool:
    """Delete persisted strategy snapshot."""
    try:
        async with async_session_maker() as session:
            result = await session.execute(select(StrategyModel).where(StrategyModel.name == name))
            row = result.scalars().first()
            if row is None:
                return False
            await session.delete(row)
            await session.commit()
            return True
    except Exception as e:
        logger.warning(f"Failed to delete strategy snapshot {name}: {e}")
        return False


async def restore_strategies_from_db(
    *,
    startup_mode: str = "paper",
    allow_live_restore: bool = False,
) -> Dict[str, Any]:
    """Restore persisted strategies and recover running state."""
    effective_startup_mode = _normalize_restore_mode(startup_mode, default="paper")
    strategy_classes = _get_strategy_classes()
    restored: List[str] = []
    started: List[str] = []
    paused: List[str] = []
    skipped: List[Dict[str, str]] = []

    try:
        async with async_session_maker() as session:
            result = await session.execute(select(StrategyModel).where(StrategyModel.description == "strategy_runtime_snapshot"))
            rows = result.scalars().all()
    except Exception as e:
        logger.warning(f"Failed to load persisted strategies: {e}")
        return {
            "loaded": 0,
            "restored": 0,
            "started": 0,
            "paused": 0,
            "skipped": [{"name": "*", "reason": str(e)}],
        }

    from core.research.experiment_registry import CandidateRegistry

    candidate_path = (Path(settings.DATA_STORAGE_PATH).parent / "research" / "ai" / "candidates.json").resolve()
    try:
        candidates = {candidate.candidate_id: candidate for candidate in CandidateRegistry(candidate_path).list(limit=None)}
    except Exception as exc:
        logger.warning(f"AI candidate registry unavailable during restore: {exc}")
        candidates = {}
    eligible_rows = []
    for row in rows:
        issue = _research_restore_issue(dict(row.params or {}), candidates)
        if issue:
            skipped.append({"name": str(row.name), "reason": issue})
        else:
            eligible_rows.append(row)
    ai_runtime_winners = _select_ai_runtime_restore_winners(eligible_rows)

    for row in eligible_rows:
        name = str(row.name)
        payload = dict(row.params or {})
        strategy_type = str(row.type or "")
        strategy_class = strategy_classes.get(strategy_type)
        if strategy_class is None:
            skipped.append({"name": name, "reason": f"unknown strategy type: {strategy_type}"})
            continue

        user_params = dict(payload.get("user_params") or {})
        exchange = str(payload.get("exchange") or user_params.get("exchange") or "gate")
        user_params.setdefault("exchange", exchange)
        symbols = list(payload.get("symbols") or [])
        timeframe = str(payload.get("timeframe") or "1h")
        allocation = float(payload.get("allocation") or settings.DEFAULT_STRATEGY_ALLOCATION)
        runtime_limit_minutes = payload.get("runtime_limit_minutes")
        runtime_started_at = _parse_runtime_anchor(payload.get("runtime_started_at"))
        state = str(payload.get("state") or ("running" if row.is_active else "stopped")).lower()
        metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), dict) else {}
        runtime_mode = str(payload.get("runtime_mode") or metadata.get("runtime_mode") or "").strip().lower()
        if runtime_mode in {"paper", "live"}:
            metadata.setdefault("runtime_mode", runtime_mode)
        if (
            runtime_mode == "live"
            and effective_startup_mode != "live"
            and not allow_live_restore
        ):
            skipped.append({"name": name, "reason": "live_restore_blocked_in_paper_startup"})
            continue
        ai_runtime_fingerprint = _ai_runtime_fingerprint_from_payload(
            name=name,
            strategy_type=strategy_type,
            payload=payload,
        )
        if ai_runtime_fingerprint:
            metadata.setdefault("runtime_fingerprint", ai_runtime_fingerprint)
        if (
            ai_runtime_fingerprint
            and ai_runtime_fingerprint in ai_runtime_winners
            and ai_runtime_winners.get(ai_runtime_fingerprint) != name
        ):
            skipped.append({"name": name, "reason": "duplicate_ai_research_runtime"})
            await persist_strategy_snapshot(name, state_override="stopped")
            continue

        if strategy_manager.get_strategy(name) is None:
            ok = strategy_manager.register_strategy(
                name=name,
                strategy_class=strategy_class,
                params=user_params,
                symbols=symbols,
                timeframe=timeframe,
                allocation=allocation,
                runtime_limit_minutes=runtime_limit_minutes,
                metadata=metadata,
            )
            if not ok:
                skipped.append({"name": name, "reason": "register_failed"})
                continue

        restored.append(name)

        if state == "running":
            if await strategy_manager.start_strategy(name):
                if runtime_started_at is not None:
                    strategy_manager.restore_strategy_runtime_anchor(name, runtime_started_at)
                started.append(name)
            else:
                skipped.append({"name": name, "reason": "start_failed"})
        elif state == "paused":
            if await strategy_manager.start_strategy(name):
                await strategy_manager.pause_strategy(name)
                paused.append(name)
            else:
                skipped.append({"name": name, "reason": "pause_recover_failed"})

    summary = {
        "loaded": len(rows),
        "restored": len(restored),
        "started": len(started),
        "paused": len(paused),
        "skipped": skipped,
    }
    if restored:
        logger.info(f"Restored strategies: restored={len(restored)}, started={len(started)}, paused={len(paused)}")
    return summary
