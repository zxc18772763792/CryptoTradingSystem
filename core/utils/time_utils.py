"""Time helpers — single source of truth for "now".

The codebase historically mixed naive ``datetime.now()`` with tz-aware
``datetime.now(timezone.utc)``. Comparing the two raises ``TypeError`` and
silently skews holding-time / PnL / dedup-window math. New code should call
``now_utc()`` instead of ``datetime.now()``.
"""
from datetime import datetime, timezone
from typing import Optional


def now_utc() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def ensure_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Normalize a datetime to tz-aware UTC; naive inputs are assumed UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
