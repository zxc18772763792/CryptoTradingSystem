"""
数据库配置模块
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, JSON
from datetime import datetime
from typing import AsyncGenerator

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


# 创建异步引擎
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    future=True,
)

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


async def close_db():
    """关闭数据库连接"""
    await engine.dispose()
