from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends

from web.api.auth import require_sensitive_ops_permissions
from web.api import trading as trading_api


router = APIRouter()


@router.post("/order", response_model=trading_api.OrderResponse, dependencies=[Depends(require_sensitive_ops_permissions("manage_orders"))])
async def create_order(request: trading_api.OrderRequest):
    return await trading_api.create_order(request)


@router.get("/orders")
async def get_orders(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    include_history: bool = True,
    limit: int = 100,
):
    return await trading_api.get_orders(
        symbol=symbol,
        exchange=exchange,
        include_history=include_history,
        limit=limit,
    )


@router.get("/orders/conditional")
async def get_conditional_orders():
    return await trading_api.get_conditional_orders()


@router.delete("/orders/conditional/{conditional_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_orders"))])
async def cancel_conditional_order(conditional_id: str):
    return await trading_api.cancel_conditional_order(conditional_id)


@router.delete("/order/{order_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_orders"))])
async def cancel_order(
    order_id: str,
    symbol: str,
    exchange: str = "binance",
):
    return await trading_api.cancel_order(order_id=order_id, symbol=symbol, exchange=exchange)


@router.delete("/orders", dependencies=[Depends(require_sensitive_ops_permissions("manage_orders"))])
async def cancel_all_orders(
    symbol: Optional[str] = None,
    exchange: str = "binance",
):
    return await trading_api.cancel_all_orders(symbol=symbol, exchange=exchange)
