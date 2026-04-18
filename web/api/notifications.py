"""Notification API endpoints."""
import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from core.exchanges import exchange_manager
from core.notifications import notification_manager
from core.risk.risk_manager import risk_manager
from core.strategies import strategy_manager
from core.trading import position_manager
from web.api.altcoin import build_altcoin_notification_context
from web.api.auth import require_sensitive_ops_permissions

router = APIRouter()


class NotificationTestRequest(BaseModel):
    title: str = "交易系统测试通知"
    message: str = "这是一条测试通知。"
    channels: List[str] = Field(default_factory=lambda: ["telegram"])


class RuleCreateRequest(BaseModel):
    name: str
    rule_type: str
    params: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    cooldown_seconds: int = 300


class RuleUpdateRequest(BaseModel):
    name: Optional[str] = None
    rule_type: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    cooldown_seconds: Optional[int] = None


class EvaluateRequest(BaseModel):
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    exchange: str = "gate"
    total_usd: Optional[float] = None


async def _load_prices(exchange: str, symbols: List[str]) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return prices

    for symbol in symbols:
        try:
            ticker = await asyncio.wait_for(connector.get_ticker(symbol), timeout=1.5)
            prices[symbol] = float(ticker.last or 0.0)
        except Exception:
            continue
    return prices


async def _build_context(total_usd: Optional[float], prices: Dict[str, float]) -> Dict[str, Any]:
    risk_report = risk_manager.get_risk_report()
    strategy_summary = strategy_manager.get_dashboard_summary(signal_limit=10)
    rules = await notification_manager.list_rules()
    altcoin_context = await build_altcoin_notification_context(rules)
    return {
        "total_usd": float(total_usd or ((risk_report.get("equity") or {}).get("current", 0.0) or 0.0)),
        "prices": prices,
        "risk_report": risk_report,
        "position_count": position_manager.get_position_count(),
        "connected_exchanges": exchange_manager.get_connected_exchanges(),
        "strategy_summary": strategy_summary,
        "altcoin": altcoin_context,
    }


@router.get("/channels")
async def get_channels():
    return {
        "channels": notification_manager.channel_status(),
    }


@router.post("/test", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def test_notification(request: NotificationTestRequest):
    before_events = len(notification_manager.get_events(limit=1000))
    result = await notification_manager.send_message(
        title=request.title,
        message=request.message,
        channels=request.channels,
    )
    all_events = notification_manager.get_events(limit=1000)
    new_events = all_events[before_events:]
    return {
        "success": any(result.values()),
        "result": result,
        "channels": notification_manager.channel_status(),
        "events": new_events[-20:],
    }


@router.get("/rules")
async def list_rules():
    return {
        "rules": await notification_manager.list_rules(),
    }


@router.post("/rules", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def create_rule(request: RuleCreateRequest):
    rule = await notification_manager.add_rule(
        name=request.name,
        rule_type=request.rule_type,
        params=request.params,
        enabled=request.enabled,
        cooldown_seconds=request.cooldown_seconds,
    )
    return {"success": True, "rule": rule}


@router.put("/rules/{rule_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def update_rule(rule_id: str, request: RuleUpdateRequest):
    updated = await notification_manager.update_rule(rule_id, request.model_dump(exclude_none=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"success": True, "rule": updated}


@router.delete("/rules/{rule_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def delete_rule(rule_id: str):
    success = await notification_manager.delete_rule(rule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"success": True}


@router.post("/evaluate", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def evaluate_rules(request: EvaluateRequest):
    prices = await _load_prices(request.exchange, request.symbols)
    context = await _build_context(request.total_usd, prices)
    result = await notification_manager.evaluate_rules(context)
    return {
        "success": True,
        "context": context,
        "result": result,
    }


@router.get("/events")
async def get_events(limit: int = 100):
    return {
        "events": notification_manager.get_events(limit=limit),
    }
