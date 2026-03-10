"""
Account snapshot persistence for dashboard equity history.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from loguru import logger
from sqlalchemy import delete, select

from config.database import async_session_maker, AccountSnapshot


class AccountSnapshotManager:
    """Persist and query account valuation snapshots."""

    def __init__(self):
        self._last_recorded_at: Optional[datetime] = None
        self._min_interval_seconds: int = 60

    async def record_snapshot(
        self,
        total_usd: float,
        exchanges: Dict[str, Dict[str, Any]],
        mode: str = "paper",
    ) -> None:
        """Store one portfolio-level snapshot plus per-exchange snapshots."""
        now = datetime.now(timezone.utc)
        if (
            self._last_recorded_at
            and (now - self._last_recorded_at).total_seconds() < self._min_interval_seconds
        ):
            return

        rows = [
            AccountSnapshot(
                timestamp=now,
                source="portfolio",
                exchange="all",
                total_usd=float(total_usd),
                mode=mode,
                payload={"exchange_count": len(exchanges)},
            )
        ]

        for exchange_name, exchange_data in exchanges.items():
            rows.append(
                AccountSnapshot(
                    timestamp=now,
                    source="exchange",
                    exchange=exchange_name,
                    total_usd=float(exchange_data.get("total_usd", 0.0) or 0.0),
                    mode=mode,
                    payload={
                        "connected": bool(exchange_data.get("connected", False)),
                        "asset_count": len(exchange_data.get("balances", [])),
                    },
                )
            )

        try:
            async with async_session_maker() as session:
                session.add_all(rows)
                await session.commit()
            self._last_recorded_at = now
        except Exception as e:
            logger.warning(f"Failed to record account snapshot: {e}")

    async def get_history(
        self,
        hours: int = 24,
        exchange: str = "all",
        limit: int = 500,
        mode: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get snapshot history for charting."""
        hours = max(1, hours)
        limit = max(1, min(limit, 5000))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with async_session_maker() as session:
            stmt = (
                select(AccountSnapshot)
                .where(AccountSnapshot.timestamp >= cutoff)
                .where(AccountSnapshot.total_usd > 0)
                .where(
                    AccountSnapshot.source == ("portfolio" if exchange == "all" else "exchange")
                )
                .order_by(AccountSnapshot.timestamp.desc())
                .limit(limit)
            )
            if mode:
                stmt = stmt.where(AccountSnapshot.mode == str(mode))
            if exchange != "all":
                stmt = stmt.where(AccountSnapshot.exchange == exchange)

            result = await session.execute(stmt)
            rows = list(reversed(result.scalars().all()))

        out: List[Dict[str, Any]] = []
        for row in rows:
            total_usd = round(float(row.total_usd or 0.0), 2)
            if total_usd <= 0:
                continue
            out.append(
                {
                    "timestamp": (
                        (row.timestamp.replace(tzinfo=timezone.utc) if row.timestamp and row.timestamp.tzinfo is None else row.timestamp)
                        .astimezone(timezone.utc)
                        .isoformat()
                        if row.timestamp
                        else None
                    ),
                    "exchange": row.exchange,
                    "total_usd": total_usd,
                    "mode": row.mode,
                }
            )
        return out

    async def get_day_start_total(
        self,
        mode: str = "live",
        exchange: str = "all",
        day: Optional[datetime] = None,
    ) -> Optional[float]:
        """Get the earliest recorded total_usd for the given UTC day."""
        anchor = day or datetime.now(timezone.utc)
        day_start = anchor.replace(hour=0, minute=0, second=0, microsecond=0)

        async with async_session_maker() as session:
            stmt = (
                select(AccountSnapshot.total_usd)
                .where(AccountSnapshot.timestamp >= day_start)
                .where(
                    AccountSnapshot.source == ("portfolio" if exchange == "all" else "exchange")
                )
                .where(AccountSnapshot.mode == str(mode))
                .order_by(AccountSnapshot.timestamp.asc())
                .limit(1)
            )
            if exchange != "all":
                stmt = stmt.where(AccountSnapshot.exchange == exchange)

            result = await session.execute(stmt)
            row = result.first()

        if not row:
            return None
        try:
            return float(row[0] or 0.0)
        except Exception:
            return None

    async def clear_history(self, mode: str = "paper") -> int:
        """Delete stored account snapshot rows by mode."""
        async with async_session_maker() as session:
            stmt = delete(AccountSnapshot)
            if mode:
                stmt = stmt.where(AccountSnapshot.mode == str(mode))
            result = await session.execute(stmt)
            await session.commit()
        self._last_recorded_at = None
        return int(result.rowcount or 0)


account_snapshot_manager = AccountSnapshotManager()
