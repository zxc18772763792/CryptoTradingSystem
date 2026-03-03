from __future__ import annotations

import os
from typing import Any, Dict, List


class PolymarketTrader:
    """Trader skeleton. Disabled by default.

    This module is intentionally minimal in v1. It should not be wired into the
    Binance execution engine. Approval-gated enablement happens through Ops API.
    """

    def __init__(self) -> None:
        self.enabled = str(os.getenv("POLY_ENABLE_TRADING") or "").strip().lower() in {"1", "true", "yes", "on", "y"}

    def _ensure_enabled(self) -> None:
        if not self.enabled:
            raise RuntimeError("Polymarket trading is disabled (POLY_ENABLE_TRADING=false)")

    async def place_limit(self, *args, **kwargs) -> Dict[str, Any]:
        self._ensure_enabled()
        raise NotImplementedError("Polymarket trading skeleton only: place_limit not implemented")

    async def cancel(self, order_id: str) -> Dict[str, Any]:
        self._ensure_enabled()
        raise NotImplementedError("Polymarket trading skeleton only: cancel not implemented")

    async def get_orders(self) -> List[Dict[str, Any]]:
        self._ensure_enabled()
        raise NotImplementedError("Polymarket trading skeleton only: get_orders not implemented")

    async def get_positions(self) -> List[Dict[str, Any]]:
        self._ensure_enabled()
        raise NotImplementedError("Polymarket trading skeleton only: get_positions not implemented")
