from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from prediction_markets.polymarket.paper_trading import PaperRiskLimits, PolymarketPaperTrader


class PolymarketTrader:
    """Trader skeleton. Disabled by default.

    This module is intentionally minimal in v1. It should not be wired into the
    Binance execution engine. Approval-gated enablement happens through Ops API.
    """

    def __init__(self) -> None:
        self.enabled = str(os.getenv("POLY_ENABLE_TRADING") or "").strip().lower() in {"1", "true", "yes", "on", "y"}
        self.mode = str(os.getenv("POLY_TRADING_MODE") or "paper").strip().lower()
        self.paper = PolymarketPaperTrader(
            account_id=os.getenv("POLY_PAPER_ACCOUNT") or "default",
            limits=PaperRiskLimits(
                initial_cash=float(os.getenv("POLY_PAPER_INITIAL_CASH") or 1000.0),
                max_order_notional=float(os.getenv("POLY_PAPER_MAX_ORDER_NOTIONAL") or 50.0),
                max_position_notional=float(os.getenv("POLY_PAPER_MAX_POSITION_NOTIONAL") or 200.0),
                fee_rate=float(os.getenv("POLY_PAPER_FEE_RATE") or 0.0),
            ),
        )

    def _ensure_enabled(self) -> None:
        if not self.enabled:
            raise RuntimeError("Polymarket trading is disabled (POLY_ENABLE_TRADING=false)")

    async def place_limit(self, *args, **kwargs) -> Dict[str, Any]:
        self._ensure_enabled()
        if self.mode == "paper":
            if args:
                raise TypeError("place_limit requires keyword arguments in paper mode")
            return await self.paper.place_limit(**kwargs)
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def cancel(self, order_id: str) -> Dict[str, Any]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.cancel(order_id)
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def get_orders(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.get_orders(status=status)
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def get_positions(self) -> List[Dict[str, Any]]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.get_positions()
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def sweep_open_orders(self) -> Dict[str, Any]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.sweep_open_orders()
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def get_account(self) -> Dict[str, Any]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.get_account()
        raise NotImplementedError("Polymarket live trading is not implemented")

    async def get_summary(self) -> Dict[str, Any]:
        self._ensure_enabled()
        if self.mode == "paper":
            return await self.paper.get_summary()
        raise NotImplementedError("Polymarket live trading is not implemented")
