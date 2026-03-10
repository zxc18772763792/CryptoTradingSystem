from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from web.api import trading as trading_api


router = APIRouter()


@router.get("/accounts")
async def list_accounts():
    return {"accounts": trading_api.account_manager.list_accounts()}


@router.post("/accounts")
async def create_account(req: trading_api.AccountCreateRequest):
    try:
        item = trading_api.account_manager.create_account(
            account_id=req.account_id,
            name=req.name,
            exchange=req.exchange,
            mode=req.mode,
            parent_account_id=req.parent_account_id,
            enabled=req.enabled,
            metadata=req.metadata,
        )
        return {"success": True, "account": item}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/accounts/{account_id}")
async def update_account(account_id: str, req: trading_api.AccountUpdateRequest):
    payload = req.model_dump(exclude_none=True)
    try:
        item = trading_api.account_manager.update_account(account_id, payload)
        return {"success": True, "account": item}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: str):
    try:
        ok = trading_api.account_manager.delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Account not found")
        return {"success": True, "account_id": account_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/accounts/summary")
async def account_summary():
    positions = [p.to_dict() for p in trading_api.position_manager.get_all_positions()]
    orders = [trading_api._serialize_order(o) for o in trading_api.order_manager.get_recent_orders(limit=1000)]
    agg: Dict[str, Dict[str, Any]] = {}

    for item in trading_api.account_manager.list_accounts():
        aid = item["account_id"]
        agg[aid] = {
            "account": item,
            "positions": 0,
            "position_value": 0.0,
            "unrealized_pnl": 0.0,
            "orders": 0,
        }

    for p in positions:
        aid = p.get("account_id", "main")
        if aid not in agg:
            continue
        agg[aid]["positions"] += 1
        agg[aid]["position_value"] += float(p.get("value") or 0.0)
        agg[aid]["unrealized_pnl"] += float(p.get("unrealized_pnl") or 0.0)

    for o in orders:
        aid = o.get("account_id", "main")
        if aid in agg:
            agg[aid]["orders"] += 1

    return {"accounts": list(agg.values())}
