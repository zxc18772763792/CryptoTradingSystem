"""Strategy persistence helpers for restart recovery."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import select

from config.database import Strategy as StrategyModel
from config.database import async_session_maker
from core.strategies.strategy_manager import strategy_manager


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
    return {
        "user_params": params,
        "symbols": list(info.get("symbols") or []),
        "timeframe": str(info.get("timeframe") or "1h"),
        "exchange": str(info.get("exchange") or params.get("exchange") or "gate"),
        "allocation": float(info.get("allocation") or 1.0),
        "runtime_limit_minutes": (info.get("runtime") or {}).get("runtime_limit_minutes"),
        "state": str(info.get("state") or "idle"),
    }


async def persist_strategy_snapshot(name: str, state_override: Optional[str] = None) -> bool:
    """Persist current strategy manager state for one strategy."""
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


async def restore_strategies_from_db() -> Dict[str, Any]:
    """Restore persisted strategies and recover running state."""
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

    for row in rows:
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
        allocation = float(payload.get("allocation") or 1.0)
        runtime_limit_minutes = payload.get("runtime_limit_minutes")
        state = str(payload.get("state") or ("running" if row.is_active else "stopped")).lower()

        if strategy_manager.get_strategy(name) is None:
            ok = strategy_manager.register_strategy(
                name=name,
                strategy_class=strategy_class,
                params=user_params,
                symbols=symbols,
                timeframe=timeframe,
                allocation=allocation,
                runtime_limit_minutes=runtime_limit_minutes,
            )
            if not ok:
                skipped.append({"name": name, "reason": "register_failed"})
                continue

        restored.append(name)

        if state == "running":
            if await strategy_manager.start_strategy(name):
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
