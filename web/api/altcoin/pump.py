"""Weekly pump-precursor watchlist radar routes.

Serves the validated weekly cross-sectional model's Top-N list
(data/research/pump_watchlist/latest.json) and triggers an out-of-process
regeneration. The watchlist directory is a module global so tests can point it
at a tmp path (patch web.api.altcoin.pump._PUMP_WATCHLIST_DIR).
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends

from web.api.auth import require_sensitive_ops_permissions

from .helpers import _utcnow

router = APIRouter()

_PUMP_WATCHLIST_DIR = Path(__file__).resolve().parents[3] / "data" / "research" / "pump_watchlist"
_PUMP_WATCHLIST_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "generate_pump_watchlist.py"
_PUMP_WATCHLIST_STALE_DAYS = 8.0
_pump_refresh_state: Dict[str, Any] = {"running": False, "started_at": None, "finished_at": None, "returncode": None, "error": None}
_pump_refresh_lock = asyncio.Lock()


@router.get("/radar/pump-watchlist")
async def get_pump_precursor_watchlist():
    path = _PUMP_WATCHLIST_DIR / "latest.json"
    if not path.exists():
        return {
            "available": False,
            "reason": "not_generated",
            "hint": "python scripts/generate_pump_watchlist.py",
            "refresh": dict(_pump_refresh_state),
            "ts": _utcnow().isoformat(),
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {
            "available": False,
            "reason": f"unreadable:{str(exc)[:80]}",
            "refresh": dict(_pump_refresh_state),
            "ts": _utcnow().isoformat(),
        }
    age_days: Optional[float] = None
    try:
        generated = datetime.fromisoformat(str(payload.get("generated_at")))
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=timezone.utc)
        age_days = (_utcnow() - generated).total_seconds() / 86400.0
    except Exception:
        age_days = None
    # The list is weekly; delisting notices are live. Overlay today's flags so a
    # notice published mid-week shows up without waiting for the next Monday run.
    live_notices: Dict[str, Any] = {}
    try:
        from core.research.exchange_notices import load_flags  # noqa: PLC0415

        flags = load_flags()
        for row in payload.get("top") or []:
            notice = flags.get(str(row.get("base") or "").upper())
            row["exchange_notice"] = notice["kind"] if notice else row.get("exchange_notice")
            if notice:
                live_notices[str(row.get("base"))] = {k: notice.get(k) for k in ("kind", "effective_date", "title")}
    except Exception:  # noqa: BLE001 - annotation only
        pass
    try:
        # Delisting-risk percentile across the whole Binance spot universe (a warning,
        # not an exclusion: 9 of 10 flagged coins are not delisted within 60 days).
        from core.research.delist_risk import load_scores  # noqa: PLC0415

        risk = load_scores()
        for row in payload.get("top") or []:
            entry = risk.get(str(row.get("base") or "").upper())
            row["delist_risk_percentile"] = entry["percentile"] if entry else None
            row["delist_risk_flagged"] = bool(entry and entry["flagged"])
    except Exception:  # noqa: BLE001 - annotation only
        pass
    return {
        "available": True,
        "live_exchange_notices": live_notices,
        "stale": bool(age_days is not None and age_days > _PUMP_WATCHLIST_STALE_DAYS),
        "age_days": None if age_days is None else round(age_days, 2),
        "refresh": dict(_pump_refresh_state),
        "data": payload,
        "ts": _utcnow().isoformat(),
    }


@router.get("/radar/exchange-notices")
async def get_exchange_notices():
    """Coins under an active Binance delisting / futures-delisting / monitoring notice."""
    from core.research.exchange_notices import load_flags  # noqa: PLC0415
    from core.research.exchange_research_runner import status as runner_status  # noqa: PLC0415

    return {"flags": load_flags(), "runner": runner_status(), "ts": _utcnow().isoformat()}


async def _run_pump_watchlist_refresh() -> None:
    _pump_refresh_state.update(
        {"running": True, "started_at": _utcnow().isoformat(), "finished_at": None, "returncode": None, "error": None}
    )
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(_PUMP_WATCHLIST_SCRIPT),
            cwd=str(_PUMP_WATCHLIST_SCRIPT.parents[1]),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        returncode = await process.wait()
        _pump_refresh_state.update({"returncode": int(returncode)})
        if returncode != 0:
            _pump_refresh_state.update({"error": f"exit_{returncode}"})
    except Exception as exc:  # noqa: BLE001
        _pump_refresh_state.update({"error": str(exc)[:160]})
    finally:
        _pump_refresh_state.update({"running": False, "finished_at": _utcnow().isoformat()})


@router.post(
    "/radar/pump-watchlist/refresh",
    dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))],
)
async def refresh_pump_precursor_watchlist():
    async with _pump_refresh_lock:
        if _pump_refresh_state.get("running"):
            return {"success": False, "reason": "already_running", "refresh": dict(_pump_refresh_state)}
        asyncio.create_task(_run_pump_watchlist_refresh())
        return {"success": True, "reason": "started", "refresh": dict(_pump_refresh_state)}
