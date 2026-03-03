from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from core.audit import audit_logger

_LOG_PATH = Path("logs") / "ops_audit.jsonl"
_SENSITIVE_KEYS = {"token", "approval_code", "x-ops-token", "x-ops-approval"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _mask(value: Any) -> str:
    text = str(value or "")
    if len(text) <= 4:
        return "***"
    return f"{text[:2]}***{text[-2:]}"


def sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key or "")
            lower = key_text.strip().lower()
            if lower in _SENSITIVE_KEYS or any(part in lower for part in ["secret", "password", "api_key", "apikey", "approval", "token"]):
                out[key_text] = _mask(item)
            else:
                out[key_text] = sanitize_value(item)
        return out
    if isinstance(value, list):
        return [sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_value(item) for item in value]
    return value


async def write_ops_audit_record(record: Dict[str, Any]) -> None:
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning(f"Failed to write ops audit jsonl: {exc}")

    try:
        await audit_logger.log(
            module="ops",
            action=str(record.get("endpoint") or "ops_action"),
            status=str(record.get("status") or "success"),
            actor=str(record.get("actor") or "openclaw"),
            message=str(record.get("error") or ""),
            details=sanitize_value(record),
        )
    except Exception as exc:
        logger.debug(f"Failed to mirror ops audit to db: {exc}")


@asynccontextmanager
async def ops_audit_scope(
    *,
    actor: str,
    endpoint: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    ip: str = "",
):
    started = _utc_now()
    state: Dict[str, Any] = {
        "status": "success",
        "error": None,
        "result": None,
        "extra": {},
    }
    try:
        yield state
    except Exception as exc:
        state["status"] = state.get("status") or "failed"
        state["error"] = str(exc)
        raise
    finally:
        ended = _utc_now()
        duration_ms = round((ended - started).total_seconds() * 1000.0, 2)
        record = {
            "ts": started.isoformat(),
            "actor": actor or "openclaw",
            "ip": ip or "",
            "endpoint": endpoint,
            "method": method.upper(),
            "params": sanitize_value(params or {}),
            "status": str(state.get("status") or "success"),
            "duration_ms": duration_ms,
            "error": state.get("error"),
        }
        extra = state.get("extra")
        if isinstance(extra, dict) and extra:
            record["extra"] = sanitize_value(extra)
        await write_ops_audit_record(record)
