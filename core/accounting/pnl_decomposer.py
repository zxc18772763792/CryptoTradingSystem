"""PnL decomposition shared by backtest/paper/live."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PnLBreakdown:
    gross_pnl: float = 0.0
    fee: float = 0.0
    slippage_cost: float = 0.0
    funding_pnl: float = 0.0
    net_pnl: float = 0.0


@dataclass
class _PositionLot:
    """A single FIFO lot (partial fill that opened a position)."""
    qty: float
    price: float
    timestamp: datetime
    fee: float = 0.0
    slippage_cost: float = 0.0


@dataclass
class PositionLedger:
    symbol: str
    side: str  # "long" | "short"
    qty: float
    entry_price: float
    opened_at: datetime
    realized: PnLBreakdown = field(default_factory=PnLBreakdown)
    unrealized_gross: float = 0.0
    mark_price: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    # FIFO lots for accurate cost-basis tracking
    _lots: List[_PositionLot] = field(default_factory=list, repr=False)

    def avg_entry_price(self) -> float:
        total_cost = sum(lot.qty * lot.price for lot in self._lots)
        total_qty = sum(lot.qty for lot in self._lots)
        return (total_cost / total_qty) if total_qty else self.entry_price

    def update_unrealized(self, mark: float) -> None:
        self.mark_price = mark
        if not self._lots:
            self.unrealized_gross = 0.0
            return
        total_qty = sum(lot.qty for lot in self._lots)
        avg_price = self.avg_entry_price()
        if self.side == "long":
            self.unrealized_gross = (mark - avg_price) * total_qty
        else:
            self.unrealized_gross = (avg_price - mark) * total_qty


class PnLDecomposer:
    """Unified PnL decomposer for backtest/paper/live modes.

    Tracks positions using FIFO lot accounting. Supports:
    - on_fill: open/close/partial-close fills with fee and slippage
    - on_funding: periodic funding payments (perpetual futures)
    - mark_to_market: update unrealized PnL for all open positions
    """

    def __init__(self) -> None:
        self.positions: Dict[str, PositionLedger] = {}
        self._closed: List[Dict[str, Any]] = []  # closed position records

    # ── public API ──────────────────────────────────────────────────────────

    def on_fill(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        fee: float = 0.0,
        slippage_cost: float = 0.0,
        timestamp: Optional[datetime] = None,
        reduce_only: bool = False,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a fill. Side is 'buy' or 'sell'."""
        if qty <= 0 or price <= 0:
            return
        ts = timestamp or datetime.utcnow()
        side_lower = side.lower()
        pos = self.positions.get(symbol)

        if pos is None and not reduce_only:
            # Open new position
            position_side = "long" if side_lower == "buy" else "short"
            lot = _PositionLot(qty=qty, price=price, timestamp=ts, fee=fee, slippage_cost=slippage_cost)
            self.positions[symbol] = PositionLedger(
                symbol=symbol,
                side=position_side,
                qty=qty,
                entry_price=price,
                opened_at=ts,
                meta=dict(meta or {}),
                _lots=[lot],
            )
            return

        if pos is None:
            # reduce_only but no position — ignore
            return

        # Determine if this fill closes/reduces an existing position
        is_closing = (pos.side == "long" and side_lower == "sell") or \
                     (pos.side == "short" and side_lower == "buy")

        if is_closing:
            self._apply_closing_fill(pos, symbol, qty, price, fee, slippage_cost, ts)
        else:
            # Adding to existing position
            lot = _PositionLot(qty=qty, price=price, timestamp=ts, fee=fee, slippage_cost=slippage_cost)
            pos._lots.append(lot)
            pos.qty = sum(lot.qty for lot in pos._lots)
            pos.entry_price = pos.avg_entry_price()

    def on_funding(
        self,
        symbol: str,
        amount: float,
        timestamp: Optional[datetime] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a funding payment (positive = received, negative = paid)."""
        pos = self.positions.get(symbol)
        if pos is None:
            return
        pos.realized.funding_pnl += amount
        pos.realized.net_pnl += amount

    def mark_to_market(self, marks: Dict[str, float]) -> None:
        """Update unrealized PnL for all open positions given current mark prices."""
        for symbol, mark in marks.items():
            pos = self.positions.get(symbol)
            if pos is not None:
                pos.update_unrealized(float(mark))

    def position_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        p = self.positions.get(symbol)
        if p is None:
            return None
        return {
            "symbol": p.symbol,
            "side": p.side,
            "qty": p.qty,
            "entry_price": p.entry_price,
            "avg_entry_price": p.avg_entry_price(),
            "mark_price": p.mark_price,
            "realized": p.realized.__dict__.copy(),
            "unrealized_gross": p.unrealized_gross,
            "lot_count": len(p._lots),
        }

    def portfolio_breakdown(self) -> Dict[str, float]:
        gross = fee = slip = funding = net = unrealized = 0.0
        for p in self.positions.values():
            gross += p.realized.gross_pnl
            fee += p.realized.fee
            slip += p.realized.slippage_cost
            funding += p.realized.funding_pnl
            net += p.realized.net_pnl
            unrealized += p.unrealized_gross
        # Also include closed positions
        for record in self._closed:
            bd = record.get("realized", {})
            gross += bd.get("gross_pnl", 0.0)
            fee += bd.get("fee", 0.0)
            slip += bd.get("slippage_cost", 0.0)
            funding += bd.get("funding_pnl", 0.0)
            net += bd.get("net_pnl", 0.0)
        return {
            "gross_pnl": gross,
            "fee": fee,
            "slippage_cost": slip,
            "funding_pnl": funding,
            "net_pnl": net,
            "unrealized_gross": unrealized,
        }

    def closed_trades(self) -> List[Dict[str, Any]]:
        return list(self._closed)

    # ── internal helpers ─────────────────────────────────────────────────────

    def _apply_closing_fill(
        self,
        pos: PositionLedger,
        symbol: str,
        qty_close: float,
        close_price: float,
        fee: float,
        slippage_cost: float,
        ts: datetime,
    ) -> None:
        """FIFO lot matching for closing fills."""
        remaining = qty_close
        realized_gross = 0.0
        consumed_fee = 0.0
        consumed_slip = 0.0

        # Distribute closing fee/slip proportionally across consumed qty
        total_close_qty = min(qty_close, sum(lot.qty for lot in pos._lots))

        while remaining > 1e-12 and pos._lots:
            lot = pos._lots[0]
            consume = min(remaining, lot.qty)
            close_fraction = consume / max(total_close_qty, 1e-12)

            if pos.side == "long":
                gross = (close_price - lot.price) * consume
            else:
                gross = (lot.price - close_price) * consume

            realized_gross += gross
            consumed_fee += fee * close_fraction
            consumed_slip += slippage_cost * close_fraction

            if consume >= lot.qty - 1e-12:
                pos._lots.pop(0)
            else:
                lot.qty -= consume
            remaining -= consume

        net = realized_gross - consumed_fee - consumed_slip + pos.realized.funding_pnl

        pos.realized.gross_pnl += realized_gross
        pos.realized.fee += consumed_fee
        pos.realized.slippage_cost += consumed_slip
        pos.realized.net_pnl = pos.realized.gross_pnl - pos.realized.fee - pos.realized.slippage_cost + pos.realized.funding_pnl

        if not pos._lots:
            # Position fully closed — archive and remove
            record = self.position_snapshot(symbol) or {}
            record["closed_at"] = ts.isoformat()
            self._closed.append(record)
            del self.positions[symbol]
        else:
            pos.qty = sum(lot.qty for lot in pos._lots)
            pos.entry_price = pos.avg_entry_price()
