from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends

from web.api.auth import require_sensitive_ops_permissions
from web.api import trading as trading_api


router = APIRouter()


@router.get("/positions", dependencies=[Depends(require_sensitive_ops_permissions("read_trading_state"))])
async def get_positions(mode: Optional[str] = None):
    return await trading_api.get_positions(mode=mode)


@router.post("/positions/close", dependencies=[Depends(require_sensitive_ops_permissions("close_positions"))])
async def close_position(req: trading_api.PositionCloseRequest):
    return await trading_api.close_position(req)
