"""Supply-event schema with point-in-time visibility checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

from core.structural.context import clamp, parse_utc_datetime, safe_float


SUPPORTED_EVENT_TYPES = {
    "token_unlock",
    "exchange_listing",
    "exchange_delisting",
    "airdrop_claim",
    "staking_unlock",
    "emission_change",
}


@dataclass
class SupplyEvent:
    event_id: str
    symbol: str
    base_asset: str
    event_type: str
    event_time: datetime
    first_seen_at: datetime
    source: str = "manual"
    source_url: str = ""
    unlock_tokens: float = 0.0
    unlock_usd: float = 0.0
    unlock_pct_circ: float = 0.0
    unlock_pct_float: float = 0.0
    recipient_type: str = "unknown"
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.event_id = str(self.event_id or "").strip()
        self.symbol = str(self.symbol or "").strip().upper()
        self.base_asset = str(self.base_asset or "").strip().upper()
        self.event_type = str(self.event_type or "").strip().lower()
        if not self.event_id:
            raise ValueError("SupplyEvent.event_id is required")
        if not self.symbol:
            raise ValueError("SupplyEvent.symbol is required")
        if self.event_type not in SUPPORTED_EVENT_TYPES:
            raise ValueError(f"unsupported supply event type: {self.event_type}")
        if self.event_time is None:
            raise ValueError("SupplyEvent.event_time is required")
        if self.first_seen_at is None:
            raise ValueError("SupplyEvent.first_seen_at is required")
        self.event_time = parse_utc_datetime(self.event_time)
        self.first_seen_at = parse_utc_datetime(self.first_seen_at)
        self.source = str(self.source or "manual").strip().lower()
        self.source_url = str(self.source_url or "").strip()
        self.unlock_tokens = max(0.0, safe_float(self.unlock_tokens))
        self.unlock_usd = max(0.0, safe_float(self.unlock_usd))
        self.unlock_pct_circ = max(0.0, safe_float(self.unlock_pct_circ))
        self.unlock_pct_float = max(0.0, safe_float(self.unlock_pct_float))
        self.recipient_type = str(self.recipient_type or "unknown").strip().lower()
        self.confidence = clamp(self.confidence)
        self.metadata = dict(self.metadata or {})

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SupplyEvent":
        data = dict(payload or {})
        if "event_time" not in data or not data.get("event_time"):
            raise ValueError("SupplyEvent.event_time is required")
        if "first_seen_at" not in data or not data.get("first_seen_at"):
            raise ValueError("SupplyEvent.first_seen_at is required for point-in-time backtests")
        return cls(
            event_id=str(data.get("event_id") or ""),
            symbol=str(data.get("symbol") or ""),
            base_asset=str(data.get("base_asset") or ""),
            event_type=str(data.get("event_type") or ""),
            event_time=parse_utc_datetime(data.get("event_time")),
            first_seen_at=parse_utc_datetime(data.get("first_seen_at")),
            source=str(data.get("source") or "manual"),
            source_url=str(data.get("source_url") or ""),
            unlock_tokens=safe_float(data.get("unlock_tokens")),
            unlock_usd=safe_float(data.get("unlock_usd")),
            unlock_pct_circ=safe_float(data.get("unlock_pct_circ")),
            unlock_pct_float=safe_float(data.get("unlock_pct_float")),
            recipient_type=str(data.get("recipient_type") or "unknown"),
            confidence=safe_float(data.get("confidence")),
            metadata=dict(data.get("metadata") or {}),
        )

    def visible_as_of(self, as_of: datetime) -> bool:
        return self.first_seen_at <= parse_utc_datetime(as_of)

    def hours_to_event(self, as_of: datetime) -> float:
        return (self.event_time - parse_utc_datetime(as_of)).total_seconds() / 3600.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "event_type": self.event_type,
            "event_time": self.event_time.astimezone(timezone.utc).isoformat(),
            "first_seen_at": self.first_seen_at.astimezone(timezone.utc).isoformat(),
            "source": self.source,
            "source_url": self.source_url,
            "unlock_tokens": self.unlock_tokens,
            "unlock_usd": self.unlock_usd,
            "unlock_pct_circ": self.unlock_pct_circ,
            "unlock_pct_float": self.unlock_pct_float,
            "recipient_type": self.recipient_type,
            "confidence": self.confidence,
            "metadata": dict(self.metadata),
        }


def validate_supply_event_payload(payload: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    try:
        SupplyEvent.from_dict(payload)
    except Exception as exc:
        errors.append(str(exc))
    return errors
