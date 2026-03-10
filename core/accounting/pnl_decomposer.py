"""PnL decomposition skeleton shared by backtest/paper/live."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class PnLBreakdown:
    gross_pnl: float = 0.0
    fee: float = 0.0
    slippage_cost: float = 0.0
    funding_pnl: float = 0.0
    net_pnl: float = 0.0


@dataclass
class PositionLedger:
    symbol: str
    side: str
    qty: float
    entry_price: float
    opened_at: datetime
    realized: PnLBreakdown = field(default_factory=PnLBreakdown)
    unrealized_gross: float = 0.0
    mark_price: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


class PnLDecomposer:
    """TODO: unify pnl fields for backtest/paper/live."""

    def __init__(self):
        self.positions: Dict[str, PositionLedger] = {}

    def on_fill(self, symbol: str, side: str, qty: float, price: float, fee: float = 0.0, slippage_cost: float = 0.0, timestamp: Optional[datetime] = None, reduce_only: bool = False, meta: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError("PnLDecomposer.on_fill not yet implemented")

    def on_funding(self, symbol: str, amount: float, timestamp: Optional[datetime] = None, meta: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError("PnLDecomposer.on_funding not yet implemented")

    def mark_to_market(self, marks: Dict[str, float]) -> None:
        raise NotImplementedError("PnLDecomposer.mark_to_market not yet implemented")

    def position_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        p = self.positions.get(symbol)
        if p is None:
            return None
        return {
            "symbol": p.symbol,
            "side": p.side,
            "qty": p.qty,
            "entry_price": p.entry_price,
            "mark_price": p.mark_price,
            "realized": p.realized.__dict__,
            "unrealized_gross": p.unrealized_gross,
        }

    def portfolio_breakdown(self) -> Dict[str, float]:
        gross = fee = slip = funding = net = 0.0
        for p in self.positions.values():
            gross += p.realized.gross_pnl + p.unrealized_gross
            fee += p.realized.fee
            slip += p.realized.slippage_cost
            funding += p.realized.funding_pnl
            net += p.realized.net_pnl + p.unrealized_gross
        return {"gross_pnl": gross, "fee": fee, "slippage_cost": slip, "funding_pnl": funding, "net_pnl": net}

