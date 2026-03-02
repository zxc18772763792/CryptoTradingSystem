"""News domain models and schemas."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from dateutil import parser as dt_parser
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import JSON, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


EVENT_TYPES = {
    "regulation",
    "exchange",
    "hack",
    "macro",
    "listing",
    "liquidation",
    "etf",
    "institution",
    "tech",
    "other",
}


class NewsBase(DeclarativeBase):
    """Declarative base for news tables."""


class NewsRaw(NewsBase):
    """Raw news pulled from external sources."""

    __tablename__ = "news_raw"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), index=True, nullable=False, default="gdelt")
    title: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    url: Mapped[str] = mapped_column(String(2048), unique=True, index=True, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    published_at: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False, default=datetime.utcnow)
    lang: Mapped[str] = mapped_column(String(32), default="en")
    content_hash: Mapped[str] = mapped_column(String(80), unique=True, index=True, nullable=False)
    symbols: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        Index("ix_news_raw_source_published", "source", "published_at"),
    )


class NewsEvent(NewsBase):
    """Structured news event extracted by LLM/rules."""

    __tablename__ = "news_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(120), unique=True, index=True, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(40), index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    sentiment: Mapped[int] = mapped_column(Integer, nullable=False)
    impact_score: Mapped[float] = mapped_column(Float, nullable=False)
    half_life_min: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    model_source: Mapped[str] = mapped_column(String(16), index=True, default="rules")
    raw_news_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, index=True, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_news_events_symbol_ts", "symbol", "ts"),
        Index("ix_news_events_type_ts", "event_type", "ts"),
    )


class NewsSourceState(NewsBase):
    """Incremental cursor and health state per source."""

    __tablename__ = "news_source_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    cursor_type: Mapped[str] = mapped_column(String(24), nullable=False, default="ts")
    cursor_value: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    paused_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class NewsLLMTask(NewsBase):
    """Async queue for event extraction."""

    __tablename__ = "news_llm_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_news_id: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(64), index=True, nullable=False, default="news")
    status: Mapped[str] = mapped_column(String(24), index=True, nullable=False, default="pending")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_news_llm_tasks_status_priority", "status", "priority"),
    )


def parse_any_datetime(value: Any) -> datetime:
    """Normalize timestamp to timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        dt = dt_parser.isoparse(value)
    else:
        raise ValueError(f"unsupported datetime value: {value!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class EventEvidence(BaseModel):
    """Evidence attached to an event."""

    title: str = ""
    url: str = ""
    source: str = ""
    matched_reason: str = ""


class EventSchema(BaseModel):
    """Strict schema for event extraction."""

    event_id: str
    ts: datetime
    symbol: str
    event_type: str
    sentiment: Literal[-1, 0, 1]
    impact_score: float = Field(ge=0.0, le=1.0)
    half_life_min: int = Field(ge=1, le=10080)
    evidence: EventEvidence

    @field_validator("ts", mode="before")
    @classmethod
    def _validate_ts(cls, value: Any) -> datetime:
        return parse_any_datetime(value)

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        text = str(value or "").strip().upper()
        if not text:
            raise ValueError("symbol is required")
        return text

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        text = str(value or "").strip().lower()
        if text not in EVENT_TYPES:
            raise ValueError(f"event_type must be one of {sorted(EVENT_TYPES)}")
        return text


class SignalRisk(BaseModel):
    stop_loss: float = 0.0
    take_profit: float = 0.0
    invalid_if: str = ""


class SignalSchema(BaseModel):
    """Strict schema for signal output."""

    ts: datetime
    symbol: str
    horizon: Literal["15m", "1h", "4h"]
    signal: Literal["LONG", "SHORT", "FLAT"]
    strength: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    risk: SignalRisk
    explain: List[str] = Field(default_factory=list)
    used_events: List[str] = Field(default_factory=list)
    model_version: str = "glm5_event_rules_v1"

    @field_validator("ts", mode="before")
    @classmethod
    def _validate_ts(cls, value: Any) -> datetime:
        return parse_any_datetime(value)

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        text = str(value or "").strip().upper()
        if not text:
            raise ValueError("symbol is required")
        return text


class PullStats(BaseModel):
    pulled_count: int = 0
    deduped_count: int = 0
    events_count: int = 0
    llm_used: bool = False
    errors: List[str] = Field(default_factory=list)
