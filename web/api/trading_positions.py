from __future__ import annotations

from fastapi import APIRouter, Depends

from web.api.auth import require_sensitive_ops_auth
from web.api import trading as trading_api


router = APIRouter()


@router.get("/positions")
async def get_positions():
    return await trading_api.get_positions()


@router.post("/positions/close", dependencies=[Depends(require_sensitive_ops_auth)])
async def close_position(req: trading_api.PositionCloseRequest):
    return await trading_api.close_position(req)
