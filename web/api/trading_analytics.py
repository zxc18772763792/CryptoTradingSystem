from __future__ import annotations

from typing import Optional

from fastapi import APIRouter

from web.api import trading as trading_api


router = APIRouter()


@router.get("/analytics/overview")
async def get_analytics_overview(
    days: int = 90,
    lookback: int = 240,
    calendar_days: int = 30,
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
):
    return await trading_api.get_analytics_overview(
        days=days,
        lookback=lookback,
        calendar_days=calendar_days,
        exchange=exchange,
        symbol=symbol,
    )


@router.get("/analytics/performance")
async def get_advanced_performance(days: int = 90):
    return await trading_api.get_advanced_performance(days=days)


@router.get("/analytics/risk-dashboard")
async def get_risk_dashboard(lookback: int = 240):
    return await trading_api.get_risk_dashboard(lookback=lookback)


@router.get("/analytics/calendar")
async def get_trading_calendar(days: int = 30):
    return await trading_api.get_trading_calendar(days=days)


@router.get("/analytics/microstructure")
async def get_market_microstructure(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
):
    return await trading_api.get_market_microstructure(exchange=exchange, symbol=symbol, depth_limit=depth_limit)


@router.post("/analytics/behavior/journal")
async def add_behavior_journal(request: trading_api.BehaviorJournalRequest):
    return await trading_api.add_behavior_journal(request)


@router.get("/analytics/behavior/report")
async def get_behavior_report(days: int = 7):
    return await trading_api.get_behavior_report(days=days)


@router.get("/analytics/stoploss/policy")
async def get_stoploss_policy():
    return await trading_api.get_stoploss_policy()


@router.post("/analytics/stoploss/policy")
async def update_stoploss_policy(request: trading_api.StoplossPolicyUpdateRequest):
    return await trading_api.update_stoploss_policy(request)


@router.get("/analytics/equity/rebalance")
async def get_equity_rebalance(
    hours: int = 168,
    target_alloc: str = "BTC:0.4,ETH:0.3,USDT:0.3",
    drift_threshold: float = 0.08,
    monthly_return: float = 0.03,
    months: int = 12,
):
    return await trading_api.get_equity_rebalance(
        hours=hours,
        target_alloc=target_alloc,
        drift_threshold=drift_threshold,
        monthly_return=monthly_return,
        months=months,
    )


@router.get("/analytics/community/overview")
async def get_community_overview(symbol: str = "BTC/USDT", exchange: str = "binance"):
    return await trading_api.get_community_overview(symbol=symbol, exchange=exchange)


@router.post("/analytics/history/collect")
async def collect_analytics_history(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
    collectors: Optional[str] = None,
):
    return await trading_api.collect_analytics_history(
        exchange=exchange,
        symbol=symbol,
        depth_limit=depth_limit,
        collectors=collectors,
    )


@router.get("/analytics/history/health")
async def get_analytics_history_health(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    hours: int = 24 * 7,
    refresh: bool = False,
    depth_limit: int = 80,
):
    return await trading_api.get_analytics_history_health(
        exchange=exchange,
        symbol=symbol,
        hours=hours,
        refresh=refresh,
        depth_limit=depth_limit,
    )


@router.get("/analytics/history/status")
async def get_analytics_history_status(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
):
    return await trading_api.get_analytics_history_status(exchange=exchange, symbol=symbol)


@router.get("/audit")
async def get_audit_logs(
    hours: int = 168,
    limit: int = 100,
    module: Optional[str] = None,
    action: Optional[str] = None,
    status: Optional[str] = None,
):
    return await trading_api.get_audit_logs(
        hours=hours,
        limit=limit,
        module=module,
        action=action,
        status=status,
    )


@router.get("/pnl/heatmap")
async def get_pnl_heatmap(
    days: int = 30,
    bucket: str = "day",
):
    return await trading_api.get_pnl_heatmap(days=days, bucket=bucket)
