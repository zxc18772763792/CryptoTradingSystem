"""Schemas for the unified market-state surface."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class DataManifestEntry(BaseModel):
    source: str
    scope: str = "symbol"
    as_of: Optional[str] = None
    age_seconds: Optional[float] = None
    ttl_seconds: Optional[float] = None
    freshness: str = "unknown"
    degraded_reason: str = ""


class MarketStateSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: f"market-state-{uuid4().hex[:12]}")
    symbol: str = "BTC/USDT"
    exchange: str = "binance"
    as_of: datetime = Field(default_factory=now_utc)
    regime: str = "low_info_range"
    bias: str = "neutral"
    confidence: float = 0.0
    uncertainty: str = "unknown"
    risk_posture: str = "normal"
    component_votes: Dict[str, Any] = Field(default_factory=dict)
    conflicts: List[str] = Field(default_factory=list)
    data_manifest: List[DataManifestEntry] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")

