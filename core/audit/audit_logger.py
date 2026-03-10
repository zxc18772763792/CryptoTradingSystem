"""Operation audit logger."""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import desc, select

from config.database import OperationAudit, async_session_maker


class AuditLogger:
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
            details=details or {},
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

