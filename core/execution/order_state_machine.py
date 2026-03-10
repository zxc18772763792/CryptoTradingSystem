"""Order lifecycle state machine for REST submit + WS/REST reconciliation.

Standalone component: can be wired into the existing execution engine later.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class OrderLifecycleState(str, Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    ACKED = "acked"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


TERMINAL_STATES = {
    OrderLifecycleState.FILLED,
    OrderLifecycleState.CANCELED,
    OrderLifecycleState.REJECTED,
    OrderLifecycleState.EXPIRED,
}

STATE_PRECEDENCE = {
    OrderLifecycleState.NEW: 0,
    OrderLifecycleState.SUBMITTED: 1,
    OrderLifecycleState.ACKED: 2,
    OrderLifecycleState.PARTIALLY_FILLED: 3,
    OrderLifecycleState.FILLED: 4,
    OrderLifecycleState.CANCELED: 4,
    OrderLifecycleState.REJECTED: 4,
    OrderLifecycleState.EXPIRED: 4,
    OrderLifecycleState.UNKNOWN: -1,
}


@dataclass
class OrderEvent:
    event_type: str
    timestamp: datetime
    order_id: str
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    status: Optional[str] = None
    filled_qty: Optional[float] = None
    avg_price: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderStateSnapshot:
    order_id: str
    client_order_id: Optional[str]
    exchange_order_id: Optional[str]
    state: OrderLifecycleState
    symbol: str
    side: str
    order_type: str
    qty: float
    filled_qty: float = 0.0
    avg_price: Optional[float] = None
    reduce_only: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_status_text: Optional[str] = None
    history: List[OrderEvent] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def remaining_qty(self) -> float:
        return max(0.0, float(self.qty) - float(self.filled_qty or 0.0))

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


def _state_from_status_text(status: Optional[str]) -> Optional[OrderLifecycleState]:
    s = str(status or "").strip().lower()
    if not s:
        return None
    state_map = {
        "new": OrderLifecycleState.ACKED,
        "open": OrderLifecycleState.ACKED,
        "submitted": OrderLifecycleState.SUBMITTED,
        "acked": OrderLifecycleState.ACKED,
        "partial": OrderLifecycleState.PARTIALLY_FILLED,
        "partially_filled": OrderLifecycleState.PARTIALLY_FILLED,
        "partiallyfilled": OrderLifecycleState.PARTIALLY_FILLED,
        "filled": OrderLifecycleState.FILLED,
        "closed": OrderLifecycleState.FILLED,
        "canceled": OrderLifecycleState.CANCELED,
        "cancelled": OrderLifecycleState.CANCELED,
        "rejected": OrderLifecycleState.REJECTED,
        "expired": OrderLifecycleState.EXPIRED,
    }
    return state_map.get(s)


class OrderStateMachine:
    """Idempotent order tracker with multiple lookup keys and snapshot/restore."""

    def __init__(self) -> None:
        self._orders: Dict[str, OrderStateSnapshot] = {}
        self._client_to_order: Dict[str, str] = {}
        self._exchange_to_order: Dict[str, str] = {}

    def _bind_aliases(self, snap: OrderStateSnapshot) -> None:
        if snap.client_order_id:
            self._client_to_order[str(snap.client_order_id)] = snap.order_id
        if snap.exchange_order_id:
            self._exchange_to_order[str(snap.exchange_order_id)] = snap.order_id

    def _resolve_order_id(
        self,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
    ) -> Optional[str]:
        if order_id and str(order_id) in self._orders:
            return str(order_id)
        if client_order_id and str(client_order_id) in self._client_to_order:
            return self._client_to_order[str(client_order_id)]
        if exchange_order_id and str(exchange_order_id) in self._exchange_to_order:
            return self._exchange_to_order[str(exchange_order_id)]
        return str(order_id) if order_id else None

    def _append_event(self, snap: OrderStateSnapshot, event: OrderEvent) -> None:
        # Deduplicate exact repeats from reconnect/replay flows.
        if snap.history:
            last = snap.history[-1]
            if (
                last.event_type == event.event_type
                and last.status == event.status
                and (last.filled_qty or 0.0) == (event.filled_qty or 0.0)
                and (last.avg_price or 0.0) == (event.avg_price or 0.0)
                and (last.exchange_order_id or "") == (event.exchange_order_id or "")
            ):
                return
        snap.history.append(event)

    def on_submit(
        self,
        order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        qty: float,
        client_order_id: Optional[str] = None,
        raw: Optional[Dict[str, Any]] = None,
        reduce_only: bool = False,
        created_at: Optional[datetime] = None,
    ) -> OrderStateSnapshot:
        now = created_at or _now()
        oid = str(order_id)
        if oid in self._orders:
            snap = self._orders[oid]
            # Idempotent duplicate submit: refresh aliases/metadata only.
            if client_order_id and not snap.client_order_id:
                snap.client_order_id = str(client_order_id)
            snap.extra.update(dict(raw or {}))
            self._bind_aliases(snap)
            return snap

        snap = OrderStateSnapshot(
            order_id=oid,
            client_order_id=str(client_order_id) if client_order_id else None,
            exchange_order_id=None,
            state=OrderLifecycleState.SUBMITTED,
            symbol=str(symbol),
            side=str(side).upper(),
            order_type=str(order_type).lower(),
            qty=max(0.0, float(qty)),
            reduce_only=bool(reduce_only),
            created_at=now,
            updated_at=now,
        )
        if raw:
            snap.extra.update(dict(raw))
        self._append_event(
            snap,
            OrderEvent(
                event_type="submit",
                timestamp=now,
                order_id=oid,
                client_order_id=snap.client_order_id,
                raw=dict(raw or {}),
            ),
        )
        self._orders[oid] = snap
        self._bind_aliases(snap)
        return snap

    def on_exchange_ack(
        self,
        *,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        status: str = "new",
        raw: Optional[Dict[str, Any]] = None,
    ) -> Optional[OrderStateSnapshot]:
        return self.apply_update(
            order_id=order_id,
            status=status,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            raw=raw,
        )

    def apply_update(
        self,
        order_id: Optional[str],
        status: str,
        filled_qty: Optional[float] = None,
        avg_price: Optional[float] = None,
        raw: Optional[Dict[str, Any]] = None,
        *,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> Optional[OrderStateSnapshot]:
        oid = self._resolve_order_id(order_id=order_id, client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if not oid or oid not in self._orders:
            return None

        snap = self._orders[oid]
        now = timestamp or _now()

        if client_order_id and not snap.client_order_id:
            snap.client_order_id = str(client_order_id)
        if exchange_order_id and not snap.exchange_order_id:
            snap.exchange_order_id = str(exchange_order_id)
        self._bind_aliases(snap)

        if filled_qty is not None:
            # Partial updates can arrive out of order. Keep monotonic fill.
            snap.filled_qty = min(max(snap.filled_qty, float(filled_qty)), snap.qty)
        if avg_price is not None:
            snap.avg_price = float(avg_price)
        snap.last_status_text = str(status or "").strip() or snap.last_status_text

        next_state = _state_from_status_text(status)
        if next_state is None:
            next_state = snap.state

        # Terminal state cannot regress. Partial fill upgrades ACK.
        if snap.is_terminal:
            next_state = snap.state
        elif snap.filled_qty >= snap.qty > 0:
            next_state = OrderLifecycleState.FILLED
        elif snap.filled_qty > 0 and next_state in {OrderLifecycleState.SUBMITTED, OrderLifecycleState.ACKED}:
            next_state = OrderLifecycleState.PARTIALLY_FILLED

        if STATE_PRECEDENCE.get(next_state, -1) >= STATE_PRECEDENCE.get(snap.state, -1) or next_state in TERMINAL_STATES:
            snap.state = next_state

        snap.updated_at = now
        if raw:
            snap.extra.update(dict(raw))
        self._append_event(
            snap,
            OrderEvent(
                event_type="update",
                timestamp=now,
                order_id=snap.order_id,
                client_order_id=snap.client_order_id,
                exchange_order_id=snap.exchange_order_id,
                status=status,
                filled_qty=snap.filled_qty if filled_qty is not None else filled_qty,
                avg_price=snap.avg_price if avg_price is not None else avg_price,
                raw=dict(raw or {}),
            ),
        )
        return snap

    def apply_event(self, event: OrderEvent) -> Optional[OrderStateSnapshot]:
        return self.apply_update(
            order_id=event.order_id,
            status=event.status or "",
            filled_qty=event.filled_qty,
            avg_price=event.avg_price,
            raw=event.raw,
            client_order_id=event.client_order_id,
            exchange_order_id=event.exchange_order_id,
            timestamp=event.timestamp,
        )

    def snapshot(self, order_id: Optional[str] = None, *, client_order_id: Optional[str] = None, exchange_order_id: Optional[str] = None) -> Optional[OrderStateSnapshot]:
        oid = self._resolve_order_id(order_id=order_id, client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        return self._orders.get(oid) if oid else None

    def all_open(self) -> List[OrderStateSnapshot]:
        return [s for s in self._orders.values() if not s.is_terminal]

    def all_orders(self) -> List[OrderStateSnapshot]:
        return list(self._orders.values())

    def drop_terminal(self) -> int:
        to_del = [oid for oid, s in self._orders.items() if s.is_terminal]
        for oid in to_del:
            snap = self._orders.pop(oid, None)
            if not snap:
                continue
            if snap.client_order_id:
                self._client_to_order.pop(str(snap.client_order_id), None)
            if snap.exchange_order_id:
                self._exchange_to_order.pop(str(snap.exchange_order_id), None)
        return len(to_del)

    def export_state(self) -> Dict[str, Any]:
        def _event_to_dict(e: OrderEvent) -> Dict[str, Any]:
            d = asdict(e)
            d["timestamp"] = e.timestamp.isoformat() if e.timestamp else None
            return d

        def _snap_to_dict(s: OrderStateSnapshot) -> Dict[str, Any]:
            return {
                "order_id": s.order_id,
                "client_order_id": s.client_order_id,
                "exchange_order_id": s.exchange_order_id,
                "state": s.state.value,
                "symbol": s.symbol,
                "side": s.side,
                "order_type": s.order_type,
                "qty": float(s.qty),
                "filled_qty": float(s.filled_qty),
                "avg_price": None if s.avg_price is None else float(s.avg_price),
                "reduce_only": bool(s.reduce_only),
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "updated_at": s.updated_at.isoformat() if s.updated_at else None,
                "last_status_text": s.last_status_text,
                "extra": dict(s.extra),
                "history": [_event_to_dict(e) for e in s.history],
            }

        return {
            "orders": [_snap_to_dict(s) for s in self._orders.values()],
            "index_client": dict(self._client_to_order),
            "index_exchange": dict(self._exchange_to_order),
        }

    @classmethod
    def from_export(cls, payload: Dict[str, Any]) -> "OrderStateMachine":
        inst = cls()
        for row in list(payload.get("orders") or []):
            try:
                snap = OrderStateSnapshot(
                    order_id=str(row["order_id"]),
                    client_order_id=row.get("client_order_id"),
                    exchange_order_id=row.get("exchange_order_id"),
                    state=OrderLifecycleState(str(row.get("state") or "unknown")),
                    symbol=str(row.get("symbol") or ""),
                    side=str(row.get("side") or ""),
                    order_type=str(row.get("order_type") or ""),
                    qty=float(row.get("qty") or 0.0),
                    filled_qty=float(row.get("filled_qty") or 0.0),
                    avg_price=(None if row.get("avg_price") is None else float(row.get("avg_price"))),
                    reduce_only=bool(row.get("reduce_only", False)),
                    created_at=_coerce_dt(row.get("created_at")),
                    updated_at=_coerce_dt(row.get("updated_at")),
                    last_status_text=row.get("last_status_text"),
                    extra=dict(row.get("extra") or {}),
                    history=[],
                )
                for e in list(row.get("history") or []):
                    snap.history.append(
                        OrderEvent(
                            event_type=str(e.get("event_type") or "update"),
                            timestamp=_coerce_dt(e.get("timestamp")) or _now(),
                            order_id=str(e.get("order_id") or snap.order_id),
                            client_order_id=e.get("client_order_id"),
                            exchange_order_id=e.get("exchange_order_id"),
                            status=e.get("status"),
                            filled_qty=(None if e.get("filled_qty") is None else float(e.get("filled_qty"))),
                            avg_price=(None if e.get("avg_price") is None else float(e.get("avg_price"))),
                            raw=dict(e.get("raw") or {}),
                        )
                    )
                inst._orders[snap.order_id] = snap
                inst._bind_aliases(snap)
            except Exception:
                continue
        return inst

