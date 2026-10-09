"""
Account snapshot persistence for dashboard equity history.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from loguru import logger
from sqlalchemy import delete, select

from config.database import async_session_maker, AccountSnapshot

# Rows read for one downsampled range query (newest kept): ~5 months of 1-minute snapshots.
_MAX_RANGE_ROWS = 200_000


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

    @staticmethod
    def _downsample(rows: List[Any], max_points: int) -> List[Any]:
        """Keep the last row of each of `max_points` equal time buckets, plus the very first row."""
        if max_points <= 0 or len(rows) <= max_points:
            return rows
        start = rows[0].timestamp
        width = max((rows[-1].timestamp - start).total_seconds() / max_points, 1e-9)
        kept: List[Any] = []
        last_bucket = None
        for row in rows:
            bucket = min(max_points - 1, int((row.timestamp - start).total_seconds() / width))
            if bucket == last_bucket:
                kept[-1] = row
            else:
                kept.append(row)
                last_bucket = bucket
        if kept[0] is not rows[0]:
            kept.insert(0, rows[0])
        return kept

    async def get_history(
        self,
        hours: int = 24,
        exchange: str = "all",
        limit: int = 500,
        mode: Optional[str] = None,
        max_points: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get snapshot history for charting.

        Without `max_points` this returns the newest `limit` rows inside the window. With it, the
        whole window is returned, downsampled in time to about `max_points` points, so a long range
        keeps its start instead of being cut to the most recent rows.
        """
        hours = max(1, hours)
        limit = max(1, min(limit, 5000))
        if max_points is not None:
            max_points = max(2, min(int(max_points), 5000))
            limit = _MAX_RANGE_ROWS
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
        if max_points is not None:
            rows = self._downsample([row for row in rows if row.timestamp is not None], max_points)

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
