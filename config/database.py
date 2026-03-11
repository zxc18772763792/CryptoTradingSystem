"""
数据库配置模块
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, JSON, UniqueConstraint, event
from datetime import datetime
from typing import AsyncGenerator
import os

from config.settings import settings


class Base(DeclarativeBase):
    """SQLAlchemy 基类"""
    pass


class Kline(Base):
    """K线数据表"""
    __tablename__ = "klines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    quote_volume = Column(Float, default=0)
    trades = Column(Integer, default=0)

    __table_args__ = (
        # 创建复合唯一索引
        {"sqlite_autoincrement": True},
    )


class Trade(Base):
    """交易记录表"""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    strategy = Column(String(50), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # buy/sell
    order_type = Column(String(20), nullable=False)  # market/limit
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    cost = Column(Float, nullable=False)
    fee = Column(Float, default=0)
    fee_currency = Column(String(20), default="")
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    order_id = Column(String(100), default="")
    status = Column(String(20), default="filled")
    notes = Column(Text, default="")


class Position(Base):
    """持仓记录表"""
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    strategy = Column(String(50), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # long/short
    entry_price = Column(Float, nullable=False)
    current_price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    value = Column(Float, nullable=False)
    unrealized_pnl = Column(Float, default=0)
    realized_pnl = Column(Float, default=0)
    leverage = Column(Float, default=1.0)
    liquidation_price = Column(Float, default=0)
    margin = Column(Float, default=0)
    opened_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_open = Column(Boolean, default=True, index=True)


class Strategy(Base):
    """策略配置表"""
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)
    type = Column(String(30), nullable=False)  # technical/quantitative/arbitrage/macro
    description = Column(Text, default="")
    params = Column(JSON, default={})
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SystemLog(Base):
    """系统日志表"""
    __tablename__ = "system_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String(10), nullable=False, index=True)
    module = Column(String(50), nullable=False, index=True)
    message = Column(Text, nullable=False)
    details = Column(JSON, default={})
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


class Signal(Base):
    """交易信号表"""
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    strategy = Column(String(50), nullable=False, index=True)
    signal_type = Column(String(20), nullable=False)  # buy/sell/hold
    price = Column(Float, nullable=False)
    strength = Column(Float, default=1.0)  # 信号强度 0-1
    params = Column(JSON, default={})
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    is_executed = Column(Boolean, default=False, index=True)


class AccountSnapshot(Base):
    """Account valuation snapshots for dashboard history."""
    __tablename__ = "account_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)
    source = Column(String(20), default="portfolio", index=True)  # portfolio/exchange
    exchange = Column(String(20), default="all", index=True)
    total_usd = Column(Float, default=0.0, nullable=False)
    mode = Column(String(20), default="paper", index=True)
    payload = Column(JSON, default={})


class OperationAudit(Base):
    """Operation audit trail for sensitive user/system actions."""
    __tablename__ = "operation_audits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)
    module = Column(String(50), nullable=False, index=True)  # trading/strategy/risk/data/system
    action = Column(String(80), nullable=False, index=True)
    status = Column(String(20), default="success", index=True)  # success/failed
    actor = Column(String(50), default="system", index=True)  # user/system
    message = Column(Text, default="")
    details = Column(JSON, default={})


class NotificationRule(Base):
    """Persistent notification rules."""
    __tablename__ = "notification_rules"

    id = Column(String(64), primary_key=True)
    name = Column(String(120), nullable=False, index=True)
    rule_type = Column(String(50), nullable=False, index=True)
    params = Column(JSON, default={})
    enabled = Column(Boolean, default=True, index=True)
    cooldown_seconds = Column(Integer, default=300)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_triggered_at = Column(DateTime, nullable=True)
    trigger_count = Column(Integer, default=0)


class ApiUser(Base):
    """API key -> role mapping for governance RBAC."""
    __tablename__ = "api_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(80), nullable=False, index=True)
    role = Column(String(40), nullable=False, index=True)
    api_key_hash = Column(String(128), nullable=False, unique=True, index=True)
    is_active = Column(Boolean, default=True, index=True)
    meta_json = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class StrategySpec(Base):
    """Versioned strategy governance object."""
    __tablename__ = "strategy_specs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(String(80), nullable=False, index=True)
    version = Column(Integer, nullable=False, index=True)
    name = Column(String(120), nullable=False, index=True)
    strategy_class = Column(String(120), nullable=False)
    status = Column(String(30), nullable=False, index=True)  # proposed/approved/paper/live/retired
    params = Column(JSON, default={})
    guardrails = Column(JSON, default={})
    metrics = Column(JSON, default={})
    regime = Column(String(40), default="mixed", index=True)
    rollback_to_version = Column(Integer, nullable=True)
    created_by = Column(String(80), default="system", index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("strategy_id", "version", name="uq_strategy_spec_id_ver"),
    )


class StrategyApproval(Base):
    """Approvals for strategy state transitions (including dual-sign for live)."""
    __tablename__ = "strategy_approvals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(String(80), nullable=False, index=True)
    version = Column(Integer, nullable=False, index=True)
    transition = Column(String(60), nullable=False, index=True)  # proposed->approved, paper->live, ...
    approver = Column(String(80), nullable=False, index=True)
    approver_role = Column(String(40), nullable=False, index=True)
    approved = Column(Boolean, default=True, index=True)
    note = Column(Text, default="")
    meta_json = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class RiskConfig(Base):
    """Versioned risk configuration snapshots."""
    __tablename__ = "risk_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Integer, nullable=False, unique=True, index=True)
    config = Column(JSON, default={})
    is_active = Column(Boolean, default=False, index=True)
    created_by = Column(String(80), default="system", index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    activated_at = Column(DateTime, nullable=True)


class RiskChangeRequest(Base):
    """Risk config change request with risk_delta_score and approval state."""
    __tablename__ = "risk_change_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(String(80), nullable=False, unique=True, index=True)
    base_version = Column(Integer, nullable=False)
    proposed_version = Column(Integer, nullable=True, index=True)
    status = Column(String(30), nullable=False, index=True)  # pending/approved/rejected/applied
    requested_by = Column(String(80), nullable=False, index=True)
    requested_role = Column(String(40), nullable=False, index=True)
    approved_by = Column(String(80), nullable=True, index=True)
    approved_role = Column(String(40), nullable=True, index=True)
    risk_delta_score = Column(Float, default=0.0)
    diff = Column(JSON, default={})
    meta_json = Column(JSON, default={})
    reason = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AuditRecord(Base):
    """Full-chain governance/execution audit trail with trace_id and hashes."""
    __tablename__ = "audit_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trace_id = Column(String(80), nullable=False, index=True)
    actor = Column(String(80), default="system", index=True)
    role = Column(String(40), default="SYSTEM", index=True)
    module = Column(String(80), nullable=False, index=True)
    action = Column(String(120), nullable=False, index=True)
    status = Column(String(20), default="success", index=True)
    input_hash = Column(String(80), default="", index=True)
    output_hash = Column(String(80), default="", index=True)
    payload_json = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class AnalyticsMicrostructureSnapshot(Base):
    """Historical microstructure snapshots for research/data diagnostics."""
    __tablename__ = "analytics_microstructure_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(40), nullable=False, index=True)
    source_ok = Column(Boolean, default=True, index=True)
    capture_status = Column(String(20), default="ok", index=True)
    source_error = Column(Text, default="")
    source_name = Column(String(80), default="exchange_public", index=True)
    latency_ms = Column(Integer, default=0)
    ingest_version = Column(String(20), default="v1", index=True)
    spread_bps = Column(Float, default=0.0)
    mid_price = Column(Float, default=0.0)
    order_flow_imbalance = Column(Float, default=0.0)
    buy_ratio = Column(Float, default=0.0)
    sell_ratio = Column(Float, default=0.0)
    large_order_count = Column(Integer, default=0)
    iceberg_candidates = Column(Integer, default=0)
    funding_rate = Column(Float, nullable=True)
    basis_pct = Column(Float, nullable=True)
    payload = Column(JSON, default={})


class AnalyticsCommunitySnapshot(Base):
    """Historical community/announcement/flow snapshots."""
    __tablename__ = "analytics_community_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(40), nullable=False, index=True)
    capture_status = Column(String(20), default="ok", index=True)
    source_error = Column(Text, default="")
    source_name = Column(String(80), default="proxy_layer", index=True)
    latency_ms = Column(Integer, default=0)
    ingest_version = Column(String(20), default="v1", index=True)
    flow_imbalance = Column(Float, default=0.0)
    buy_ratio = Column(Float, default=0.0)
    sell_ratio = Column(Float, default=0.0)
    announcement_count = Column(Integer, default=0)
    security_alert_count = Column(Integer, default=0)
    payload = Column(JSON, default={})


class AnalyticsWhaleSnapshot(Base):
    """Historical whale activity snapshots."""
    __tablename__ = "analytics_whale_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(40), nullable=False, index=True)
    capture_status = Column(String(20), default="ok", index=True)
    source_error = Column(Text, default="")
    source_name = Column(String(80), default="public_chain_proxy", index=True)
    latency_ms = Column(Integer, default=0)
    ingest_version = Column(String(20), default="v1", index=True)
    whale_count = Column(Integer, default=0)
    total_btc = Column(Float, default=0.0)
    max_btc = Column(Float, default=0.0)
    payload = Column(JSON, default={})


class StrategyPerformanceSnapshot(Base):
    """Periodic performance snapshots for live/paper running strategies."""
    __tablename__ = "strategy_performance_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    candidate_id = Column(String(80), nullable=True, index=True)   # AI research candidate ID
    strategy_name = Column(String(120), nullable=False, index=True)
    symbol = Column(String(40), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)
    mode = Column(String(20), default="paper", index=True)          # paper | shadow | live
    # Performance metrics
    total_pnl = Column(Float, default=0.0)
    total_pnl_pct = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    realized_pnl = Column(Float, default=0.0)
    trade_count = Column(Integer, default=0)
    win_count = Column(Integer, default=0)
    loss_count = Column(Integer, default=0)
    win_rate = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    calmar_ratio = Column(Float, nullable=True)
    # Risk context
    cusum_triggered = Column(Boolean, default=False, index=True)
    cusum_low = Column(Float, nullable=True)
    # Extra payload for extensibility
    payload = Column(JSON, default={})

    __table_args__ = (
        UniqueConstraint("strategy_name", "symbol", "timeframe", "snapshot_at", name="uq_perf_snap"),
    )


class AnalyticsHistoryIngestStatus(Base):
    """Last known ingest status per analytics collector."""
    __tablename__ = "analytics_history_ingest_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    collector = Column(String(40), nullable=False, unique=True, index=True)
    exchange = Column(String(20), default="binance", index=True)
    symbol = Column(String(40), default="BTC/USDT", index=True)
    status = Column(String(20), default="idle", index=True)
    error = Column(Text, default="")
    rows_written = Column(Integer, default=0)
    started_at = Column(DateTime, nullable=True, index=True)
    finished_at = Column(DateTime, nullable=True, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    details = Column(JSON, default={})


# 创建异步引擎
try:
    _SQLITE_BUSY_TIMEOUT_SEC = max(3.0, float(os.environ.get("SQLITE_BUSY_TIMEOUT_SEC", "8")))
except Exception:
    _SQLITE_BUSY_TIMEOUT_SEC = 8.0
_SQLITE_CONNECT_ARGS = {"timeout": _SQLITE_BUSY_TIMEOUT_SEC}
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    future=True,
    connect_args=_SQLITE_CONNECT_ARGS,
)


@event.listens_for(engine.sync_engine, "connect")
def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute(f"PRAGMA busy_timeout={int(_SQLITE_BUSY_TIMEOUT_SEC * 1000)}")
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()

# 创建异步会话工厂
async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话"""
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """初始化数据库"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _migrate_analytics_history_schema(conn)


async def close_db():
    """关闭数据库连接"""
    await engine.dispose()


async def _migrate_analytics_history_schema(conn) -> None:
    table_columns = {
        "analytics_microstructure_snapshots": [
            ("capture_status", "TEXT DEFAULT 'ok'"),
            ("source_error", "TEXT DEFAULT ''"),
            ("source_name", "TEXT DEFAULT 'exchange_public'"),
            ("latency_ms", "INTEGER DEFAULT 0"),
            ("ingest_version", "TEXT DEFAULT 'v1'"),
        ],
        "analytics_community_snapshots": [
            ("capture_status", "TEXT DEFAULT 'ok'"),
            ("source_error", "TEXT DEFAULT ''"),
            ("source_name", "TEXT DEFAULT 'proxy_layer'"),
            ("latency_ms", "INTEGER DEFAULT 0"),
            ("ingest_version", "TEXT DEFAULT 'v1'"),
        ],
        "analytics_whale_snapshots": [
            ("capture_status", "TEXT DEFAULT 'ok'"),
            ("source_error", "TEXT DEFAULT ''"),
            ("source_name", "TEXT DEFAULT 'public_chain_proxy'"),
            ("latency_ms", "INTEGER DEFAULT 0"),
            ("ingest_version", "TEXT DEFAULT 'v1'"),
        ],
    }
    for table_name, columns in table_columns.items():
        result = await conn.exec_driver_sql(f"PRAGMA table_info('{table_name}')")
        existing = {str(row[1]) for row in result.fetchall()}
        for column_name, ddl in columns:
            if column_name in existing:
                continue
            await conn.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")

    # Composite indexes for frequent analytics history filters and ordering.
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_analytics_micro_es_ts ON analytics_microstructure_snapshots(exchange, symbol, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_micro_es_status ON analytics_microstructure_snapshots(exchange, symbol, capture_status)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_community_es_ts ON analytics_community_snapshots(exchange, symbol, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_community_es_status ON analytics_community_snapshots(exchange, symbol, capture_status)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_whale_es_ts ON analytics_whale_snapshots(exchange, symbol, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_whale_es_status ON analytics_whale_snapshots(exchange, symbol, capture_status)",
    ]
    for sql in index_statements:
        await conn.exec_driver_sql(sql)
