from __future__ import annotations

import uuid
import math
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.utils import utc_now, parse_ts_any


@dataclass
class PaperRiskLimits:
    initial_cash: float = 1000.0
    max_order_notional: float = 50.0
    max_position_notional: float = 200.0
    max_price: float = 0.99
    min_price: float = 0.01
    fee_rate: float = 0.0
    max_quote_age_seconds: float = 120.0


def _clean_side(side: str) -> str:
    text = str(side or "BUY").strip().upper()
    if text not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    return text


def _clean_price(price: Any) -> float:
    value = float(price)
    if not math.isfinite(value) or value <= 0.0 or value >= 1.0:
        raise ValueError("price must be between 0 and 1")
    return value


def _clean_size(size: Any) -> float:
    value = float(size)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("size must be positive")
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


class PolymarketPaperTrader:
    def __init__(self, account_id: str = "default", limits: Optional[PaperRiskLimits] = None) -> None:
        self.account_id = str(account_id or "default").strip() or "default"
        self.limits = limits or PaperRiskLimits()

    async def ensure_account(self) -> Dict[str, Any]:
        return await pm_db.get_or_create_paper_account(self.account_id, initial_cash=self.limits.initial_cash)

    async def reset(self, initial_cash: Optional[float] = None) -> Dict[str, Any]:
        cash = self.limits.initial_cash if initial_cash is None else float(initial_cash)
        return await pm_db.reset_paper_account(self.account_id, initial_cash=cash)

    async def place_limit(
        self,
        *,
        market_id: str,
        token_id: str,
        outcome: str = "YES",
        side: str = "BUY",
        price: float,
        size: float,
        client_order_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        fill_immediately: bool = True,
    ) -> Dict[str, Any]:
        await self.ensure_account()
        side_norm = _clean_side(side)
        price_f = _clean_price(price)
        size_f = _clean_size(size)
        await self._validate_order_risk(str(token_id or ""), side_norm, price_f, size_f)
        order = await pm_db.create_paper_order(
            {
                "order_id": client_order_id or f"pm-paper-{uuid.uuid4().hex[:20]}",
                "account_id": self.account_id,
                "market_id": str(market_id or ""),
                "token_id": str(token_id or ""),
                "outcome": str(outcome or "YES").upper(),
                "side": side_norm,
                "order_type": "LIMIT",
                "status": "OPEN",
                "price": price_f,
                "size": size_f,
                "payload": _json_safe(metadata or {}),
            },
            risk_limits=asdict(self.limits)
        )
        if fill_immediately:
            filled = await self.try_fill_order(order["order_id"])
            return filled or order
        return order

    async def try_fill_order(self, order_id: str, quote: Optional[Dict[str, Any]] = None,
                             *, simulation_time: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        order = await pm_db.get_paper_order(order_id)
        if order and str(order.get("account_id")) != self.account_id:
            raise ValueError("paper order belongs to another account")
        if not order or order.get("status") not in {"OPEN", "PARTIAL"}:
            return order
        quote = quote or await pm_db.get_latest_quote(str(order.get("token_id") or ""))
        now = parse_ts_any(simulation_time) if simulation_time is not None else utc_now()
        if not quote or not self._quote_is_executable(order, quote, now):
            return order
        fill_price = self._match_price(order, quote)
        if fill_price is None or not math.isfinite(fill_price) or not 0 < fill_price < 1:
            return order
        return await pm_db.fill_paper_order_atomically(
            order_id, account_id=self.account_id, price=fill_price,
            fee_rate=float(self.limits.fee_rate), quote=_json_safe(quote), ts=now,
        )

    def _quote_is_executable(self, order: Dict[str, Any], quote: Dict[str, Any], now: datetime) -> bool:
        if str(quote.get("token_id") or "") != str(order.get("token_id") or ""):
            return False
        if not quote.get("ts") or str((quote.get("payload") or {}).get("source") or "").startswith("gamma"):
            return False
        try:
            age = (parse_ts_any(now) - parse_ts_any(quote["ts"])).total_seconds()
            if not 0 <= age <= self.limits.max_quote_age_seconds:
                return False
            # A midpoint/last-trade price is not an executable side of the book.
            side_price = quote.get("ask" if order.get("side") == "BUY" else "bid")
            return side_price is not None and math.isfinite(float(side_price)) and 0 < float(side_price) < 1
        except (ValueError, TypeError, OverflowError):
            return False

    async def sweep_open_orders(self) -> Dict[str, Any]:
        orders = await pm_db.list_paper_orders(self.account_id, status="ACTIVE", limit=1000)
        filled = []
        for order in orders:
            updated = await self.try_fill_order(order["order_id"])
            if updated and updated.get("status") == "FILLED":
                filled.append(updated)
        return {"checked": len(orders), "filled": len(filled), "items": filled}

    async def cancel(self, order_id: str) -> Dict[str, Any]:
        order = await pm_db.cancel_paper_order(order_id, account_id=self.account_id)
        if not order:
            raise ValueError(f"unknown order_id: {order_id}")
        return order

    async def get_orders(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        return await pm_db.list_paper_orders(self.account_id, status=status, limit=500)

    async def get_positions(self) -> List[Dict[str, Any]]:
        return await pm_db.list_paper_positions(self.account_id)

    async def get_account(self) -> Dict[str, Any]:
        return await self.ensure_account()

    async def get_summary(self, quote_overrides: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        account = await self.ensure_account()
        positions = await pm_db.list_paper_positions(self.account_id)
        orders = await pm_db.list_paper_orders(self.account_id, status="ACTIVE", limit=1000)
        quote_overrides = quote_overrides or {}
        enriched_positions = []
        positions_value = 0.0
        unrealized_pnl = 0.0
        for position in positions:
            token_id = str(position.get("token_id") or "")
            quote = quote_overrides.get(token_id) or await pm_db.get_latest_quote(token_id)
            mark_price = self._mark_price(position, quote)
            size = float(position.get("size") or 0.0)
            avg_price = float(position.get("avg_price") or 0.0)
            value = mark_price * size
            pnl = (mark_price - avg_price) * size
            item = dict(position)
            item.update(
                {
                    "mark_price": round(mark_price, 8),
                    "market_value": round(value, 8),
                    "unrealized_pnl": round(pnl, 8),
                    "quote_ts": quote.get("ts") if quote else None,
                }
            )
            enriched_positions.append(item)
            positions_value += value
            unrealized_pnl += pnl
        reserved_cash = self._reserved_cash(orders)
        reserved_positions = self._reserved_positions(orders)
        cash = float(account.get("cash") or 0.0)
        equity = cash + positions_value
        return {
            "account": account,
            "cash": round(cash, 8),
            "reserved_cash": round(reserved_cash, 8),
            "available_cash": round(max(0.0, cash - reserved_cash), 8),
            "positions_value": round(positions_value, 8),
            "equity": round(equity, 8),
            "realized_pnl": round(float(account.get("realized_pnl") or 0.0), 8),
            "unrealized_pnl": round(unrealized_pnl, 8),
            "fees_paid": round(float(account.get("fees_paid") or 0.0), 8),
            "open_orders": len(orders),
            "reserved_positions": reserved_positions,
            "positions": enriched_positions,
        }

    async def _validate_order_risk(self, token_id: str, side: str, price: float, size: float) -> None:
        if price < self.limits.min_price or price > self.limits.max_price:
            raise ValueError(f"price {price:.4f} outside paper limits")
        notional = price * size
        if notional > self.limits.max_order_notional:
            raise ValueError(f"order notional {notional:.2f} exceeds max_order_notional {self.limits.max_order_notional:.2f}")
        positions = await pm_db.list_paper_positions(self.account_id, include_flat=True)
        current = next((item for item in positions if str(item.get("token_id") or "") == str(token_id or "")), None)
        current_size = float((current or {}).get("size") or 0.0)
        current_avg = float((current or {}).get("avg_price") or 0.0)
        open_orders = await pm_db.list_paper_orders(self.account_id, status="ACTIVE", limit=1000)
        if side == "BUY":
            account = await self.ensure_account()
            fee = notional * max(0.0, float(self.limits.fee_rate or 0.0))
            available_cash = float(account.get("cash") or 0.0) - self._reserved_cash(open_orders)
            if available_cash < notional + fee:
                raise ValueError("insufficient paper cash")
            pending_notional = sum(
                float(o.get("price") or 0.0) * max(0.0, float(o.get("size") or 0.0) - float(o.get("filled_size") or 0.0))
                for o in open_orders if o.get("side") == "BUY" and o.get("token_id") == token_id
            )
            position_notional = current_size * current_avg + pending_notional + notional
            if position_notional > self.limits.max_position_notional:
                raise ValueError(
                    f"position notional {position_notional:.2f} exceeds max_position_notional {self.limits.max_position_notional:.2f}"
                )
        else:
            reserved = self._reserved_positions(open_orders).get(str(token_id or ""), 0.0)
            if size > current_size - reserved + 1e-12:
                raise ValueError("insufficient paper position")

    def _reserved_cash(self, orders: List[Dict[str, Any]]) -> float:
        reserved = 0.0
        fee_rate = max(0.0, float(self.limits.fee_rate or 0.0))
        for order in orders:
            if str(order.get("side") or "").upper() != "BUY":
                continue
            remaining = max(0.0, float(order.get("size") or 0.0) - float(order.get("filled_size") or 0.0))
            reserved += float(order.get("price") or 0.0) * remaining * (1.0 + fee_rate)
        return reserved

    @staticmethod
    def _reserved_positions(orders: List[Dict[str, Any]]) -> Dict[str, float]:
        reserved: Dict[str, float] = {}
        for order in orders:
            if str(order.get("side") or "").upper() != "SELL":
                continue
            token_id = str(order.get("token_id") or "")
            remaining = max(0.0, float(order.get("size") or 0.0) - float(order.get("filled_size") or 0.0))
            reserved[token_id] = reserved.get(token_id, 0.0) + remaining
        return reserved

    @staticmethod
    def _mark_price(position: Dict[str, Any], quote: Optional[Dict[str, Any]]) -> float:
        if not quote:
            return float(position.get("avg_price") or 0.0)
        midpoint = quote.get("midpoint")
        if midpoint is not None:
            return float(midpoint)
        bid = quote.get("bid")
        ask = quote.get("ask")
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
        price = quote.get("price")
        if price is not None:
            return float(price)
        return float(position.get("avg_price") or 0.0)

    @staticmethod
    def _match_price(order: Dict[str, Any], quote: Dict[str, Any]) -> Optional[float]:
        side = str(order.get("side") or "BUY").upper()
        limit_price = float(order.get("price") or 0.0)
        if side == "BUY":
            ask = quote.get("ask")
            candidate = float(ask) if ask is not None else float(quote.get("price") or quote.get("midpoint") or 0.0)
            if candidate > 0 and candidate <= limit_price:
                return candidate
            return None
        bid = quote.get("bid")
        candidate = float(bid) if bid is not None else float(quote.get("price") or quote.get("midpoint") or 0.0)
        if candidate > 0 and candidate >= limit_price:
            return candidate
        return None
