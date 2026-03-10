from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from loguru import logger

from config.database import AuditRecord, async_session_maker
from config.settings import settings


def _json_hash(payload: Any) -> str:
    try:
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        data = str(payload)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def new_trace_id() -> str:
    return uuid.uuid4().hex


@dataclass
class GovernanceAuditEvent:
    module: str
    action: str
    status: str = "success"
    actor: str = "system"
    role: str = "SYSTEM"
    trace_id: str = ""
    input_payload: Optional[Dict[str, Any]] = None
    output_payload: Optional[Dict[str, Any]] = None
    payload_json: Optional[Dict[str, Any]] = None


async def write_audit(event: GovernanceAuditEvent) -> None:
    trace_id = str(event.trace_id or new_trace_id())
    input_payload = event.input_payload or {}
    output_payload = event.output_payload or {}
    payload = event.payload_json or {}

    if str(getattr(settings, "AUDIT_LEVEL", "full")).lower() != "full":
        payload = {
            "summary": {
                "input_keys": list(input_payload.keys()),
                "output_keys": list(output_payload.keys()),
            }
        }

    row = AuditRecord(
        trace_id=trace_id,
        actor=str(event.actor or "system"),
        role=str(event.role or "SYSTEM"),
        module=str(event.module or "governance"),
        action=str(event.action or "unknown"),
        status=str(event.status or "success"),
        input_hash=_json_hash(input_payload),
        output_hash=_json_hash(output_payload),
        payload_json=payload,
        created_at=datetime.now(timezone.utc),
    )

    try:
        async with async_session_maker() as session:
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning(f"governance audit write failed: {exc}")

