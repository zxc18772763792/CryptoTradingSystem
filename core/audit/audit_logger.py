"""Operation audit logger."""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from loguru import logger
from sqlalchemy import desc, select

from config.database import OperationAudit, async_session_maker


_SENSITIVE_AUDIT_KEYS = {
    "token",
    "access_token",
    "refresh_token",
    "approval_code",
    "authorization",
    "x-api-key",
    "x-ops-token",
    "signature",
    "passphrase",
}
_SENSITIVE_AUDIT_KEY_FRAGMENTS = (
    "secret",
    "password",
    "api_key",
    "apikey",
    "private_key",
)


def _is_sensitive_audit_key(key: Any) -> bool:
    text = str(key or "").strip().lower()
    return text in _SENSITIVE_AUDIT_KEYS or any(fragment in text for fragment in _SENSITIVE_AUDIT_KEY_FRAGMENTS)


def _sanitize_audit_details(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): ("[REDACTED]" if _is_sensitive_audit_key(key) else _sanitize_audit_details(item))
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_audit_details(item) for item in value]
    return value


class AuditLogger:
    def __init__(self) -> None:
        self._background_tasks: Set[asyncio.Task] = set()

    def _consume_background_task_result(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(f"Background audit log task failed: {exc}")

    def schedule(self, **kwargs: Any) -> Optional[asyncio.Task]:
        async def _run() -> None:
            try:
                await self.log(**kwargs)
            except Exception as exc:
                logger.warning(f"Background audit log failed: {exc}")

        coro = _run()
        try:
            task = asyncio.create_task(coro)
        except RuntimeError as exc:
            coro.close()
            logger.warning(f"Failed to schedule audit log: {exc}")
            return None
        if task is None:
            return None
        self._background_tasks.add(task)
        if hasattr(task, "add_done_callback"):
            task.add_done_callback(self._consume_background_task_result)
        return task

    async def drain_background_tasks(
        self,
        *,
        timeout: float = 3.0,
        cancel_pending: bool = False,
    ) -> Dict[str, int]:
        tasks = {task for task in self._background_tasks if not task.done()}
        if not tasks:
            return {"done": 0, "pending": 0, "cancelled": 0}

        done, pending = await asyncio.wait(tasks, timeout=max(0.0, float(timeout or 0.0)))
        cancelled = 0
        if pending and cancel_pending:
            for task in pending:
                task.cancel()
            results = await asyncio.gather(*pending, return_exceptions=True)
            cancelled = sum(1 for item in results if isinstance(item, asyncio.CancelledError))
            pending = {task for task in pending if not task.done()}
        return {"done": len(done), "pending": len(pending), "cancelled": cancelled}

    async def log(
        self,
        module: str,
        action: str,
        status: str = "success",
        actor: str = "system",
        message: str = "",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        row = OperationAudit(
            module=module,
            action=action,
            status=status,
            actor=actor,
            message=message or "",
            details=_sanitize_audit_details(details or {}),
        )
        try:
            async with async_session_maker() as session:
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"Failed to write audit log: {e}")

    async def list_logs(
        self,
        module: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        hours: int = 72,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 2000))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, hours))

        async with async_session_maker() as session:
            stmt = (
                select(OperationAudit)
                .where(OperationAudit.timestamp >= cutoff)
                .order_by(desc(OperationAudit.timestamp))
                .limit(limit)
            )
            if module:
                stmt = stmt.where(OperationAudit.module == module)
            if action:
                stmt = stmt.where(OperationAudit.action == action)
            if status:
                stmt = stmt.where(OperationAudit.status == status)

            result = await session.execute(stmt)
            rows = result.scalars().all()

        return [
            {
                "id": row.id,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "module": row.module,
                "action": row.action,
                "status": row.status,
                "actor": row.actor,
                "message": row.message,
                "details": row.details or {},
            }
            for row in rows
        ]


audit_logger = AuditLogger()

