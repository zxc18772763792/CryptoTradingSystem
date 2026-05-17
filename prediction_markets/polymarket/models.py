from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class PMBase(DeclarativeBase):
    """Declarative base for Polymarket tables."""


class PMMarket(PMBase):
    __tablename__ = "pm_markets"

    market_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    slug: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    question: Mapped[str] = mapped_column(Text, nullable=False, default="")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    tags_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    outcomes_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    token_ids_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    resolution_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    liquidity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    relevance_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    payload_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        Index("ix_pm_markets_category_active", "category", "active", "closed"),
        Index("ix_pm_markets_slug", "slug"),
    )


class PMQuote(PMBase):
    __tablename__ = "pm_quotes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    outcome: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    bid: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    midpoint: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    spread: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    depth1: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    depth5: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    payload_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        Index("ix_pm_quotes_market_ts", "market_id", "ts"),
        Index("ix_pm_quotes_token_ts", "token_id", "ts"),
    )


class PMSubscription(PMBase):
    __tablename__ = "pm_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    outcome: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    relevance_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    symbol_weights_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    min_liquidity_snapshot: Mapped[float] = mapped_column(Float, default=0.0)
    max_spread_snapshot: Mapped[float] = mapped_column(Float, default=0.0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("market_id", "token_id", "outcome", name="uq_pm_subscription_market_token_outcome"),
    )


class PMAlert(PMBase):
    __tablename__ = "pm_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    metric: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    payload_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)


class PMSourceState(PMBase):
    __tablename__ = "pm_source_state"

    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    cursor_type: Mapped[str] = mapped_column(String(32), nullable=False, default="ts")
    cursor_value: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    last_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    success_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    paused_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class PMPaperAccount(PMBase):
    __tablename__ = "pm_paper_accounts"

    account_id: Mapped[str] = mapped_column(String(64), primary_key=True, default="default")
    initial_cash: Mapped[float] = mapped_column(Float, default=1000.0, nullable=False)
    cash: Mapped[float] = mapped_column(Float, default=1000.0, nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    fees_paid: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class PMPaperOrder(PMBase):
    __tablename__ = "pm_paper_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    account_id: Mapped[str] = mapped_column(String(64), index=True, default="default", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    outcome: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    side: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), default="LIMIT", nullable=False)
    status: Mapped[str] = mapped_column(String(16), index=True, default="OPEN", nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False)
    filled_size: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    avg_fill_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fee_paid: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    reject_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        Index("ix_pm_paper_orders_account_status", "account_id", "status", "created_at"),
        Index("ix_pm_paper_orders_token_status", "token_id", "status"),
    )


class PMPaperFill(PMBase):
    __tablename__ = "pm_paper_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fill_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    order_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    account_id: Mapped[str] = mapped_column(String(64), index=True, default="default", nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    outcome: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    side: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False)
    fee_paid: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    quote_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    payload_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        Index("ix_pm_paper_fills_account_ts", "account_id", "ts"),
        Index("ix_pm_paper_fills_token_ts", "token_id", "ts"),
    )


class PMPaperPosition(PMBase):
    __tablename__ = "pm_paper_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), index=True, default="default", nullable=False)
    market_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    token_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    outcome: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    size: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    avg_price: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    fees_paid: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("account_id", "token_id", name="uq_pm_paper_position_account_token"),
        Index("ix_pm_paper_positions_account_market", "account_id", "market_id"),
    )
