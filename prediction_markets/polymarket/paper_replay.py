from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_trading import PaperRiskLimits, PolymarketPaperTrader
from prediction_markets.polymarket.utils import parse_ts_any


@dataclass
class ReplayConfig:
    account_id: str = "replay"
    initial_cash: float = 1000.0
    order_size: float = 10.0
    buy_below: float = 0.45
    sell_above: float = 0.60
    momentum_window: int = 3
    momentum_buy_delta: float = 0.04
    momentum_sell_delta: float = 0.04
    max_order_notional: float = 100.0
    max_position_notional: float = 500.0
    fee_rate: float = 0.0


@dataclass
class ReplayDecision:
    action: str
    price: float
    size: float
    reason: str


class ReplayStrategy(Protocol):
    async def decide(
        self,
        *,
        quote: Dict[str, Any],
        position: Optional[Dict[str, Any]],
        config: ReplayConfig,
    ) -> Optional[ReplayDecision]:
        ...


class ThresholdReplayStrategy:
    async def decide(
        self,
        *,
        quote: Dict[str, Any],
        position: Optional[Dict[str, Any]],
        config: ReplayConfig,
    ) -> Optional[ReplayDecision]:
        bid = quote.get("bid")
        ask = quote.get("ask")
        mid = quote_midpoint(quote)
        if mid <= 0:
            return None
        size = float((position or {}).get("size") or 0.0)
        if size <= 0 and ask is not None and mid <= config.buy_below:
            return ReplayDecision(action="BUY", price=float(ask), size=config.order_size, reason="buy_below")
        if size > 0 and bid is not None and mid >= config.sell_above:
            return ReplayDecision(action="SELL", price=float(bid), size=min(size, config.order_size), reason="sell_above")
        return None


class HoldReplayStrategy:
    async def decide(
        self,
        *,
        quote: Dict[str, Any],
        position: Optional[Dict[str, Any]],
        config: ReplayConfig,
    ) -> Optional[ReplayDecision]:
        return None


class MomentumReplayStrategy:
    def __init__(self) -> None:
        self._history: Dict[str, List[float]] = {}

    async def decide(
        self,
        *,
        quote: Dict[str, Any],
        position: Optional[Dict[str, Any]],
        config: ReplayConfig,
    ) -> Optional[ReplayDecision]:
        token_id = str(quote.get("token_id") or "")
        mid = quote_midpoint(quote)
        if not token_id or mid <= 0:
            return None
        history = self._history.setdefault(token_id, [])
        history.append(mid)
        window = max(1, int(config.momentum_window or 3))
        if len(history) <= window:
            return None
        anchor = history[-window - 1]
        delta = mid - anchor
        size = float((position or {}).get("size") or 0.0)
        ask = quote.get("ask")
        bid = quote.get("bid")
        if size <= 0 and ask is not None and delta >= float(config.momentum_buy_delta or 0.0):
            return ReplayDecision(action="BUY", price=float(ask), size=config.order_size, reason="momentum_buy")
        if size > 0 and bid is not None and delta <= -float(config.momentum_sell_delta or 0.0):
            return ReplayDecision(action="SELL", price=float(bid), size=min(size, config.order_size), reason="momentum_sell")
        return None


def quote_midpoint(quote: Dict[str, Any]) -> float:
    midpoint = quote.get("midpoint")
    if midpoint is not None:
        return float(midpoint)
    bid = quote.get("bid")
    ask = quote.get("ask")
    if bid is not None and ask is not None:
        return (float(bid) + float(ask)) / 2.0
    price = quote.get("price")
    return float(price) if price is not None else 0.0


def build_replay_strategy(name: str) -> ReplayStrategy:
    normalized = str(name or "threshold").strip().lower()
    builders = replay_strategy_registry()
    if normalized not in builders:
        raise ValueError(f"unknown replay strategy: {name}")
    return builders[normalized]()


def replay_strategy_registry() -> Dict[str, Any]:
    return {
        "threshold": ThresholdReplayStrategy,
        "basic": ThresholdReplayStrategy,
        "default": ThresholdReplayStrategy,
        "hold": HoldReplayStrategy,
        "none": HoldReplayStrategy,
        "noop": HoldReplayStrategy,
        "momentum": MomentumReplayStrategy,
    }


def list_replay_strategies() -> List[str]:
    return sorted({"threshold", "hold", "momentum"})


class PolymarketPaperReplay:
    def __init__(self, config: Optional[ReplayConfig] = None, strategy: Optional[ReplayStrategy] = None) -> None:
        self.config = config or ReplayConfig()
        self.strategy = strategy or ThresholdReplayStrategy()
        self.trader = PolymarketPaperTrader(
            account_id=self.config.account_id,
            limits=PaperRiskLimits(
                initial_cash=self.config.initial_cash,
                max_order_notional=self.config.max_order_notional,
                max_position_notional=self.config.max_position_notional,
                fee_rate=self.config.fee_rate,
            ),
        )

    async def run_quotes(self, quotes: List[Dict[str, Any]], *, reset: bool = True) -> Dict[str, Any]:
        items = sorted((dict(item) for item in quotes or []), key=lambda item: parse_ts_any(item.get("ts")))
        if reset:
            await self.trader.reset(self.config.initial_cash)
        last_quotes: Dict[str, Dict[str, Any]] = {}
        decisions: List[Dict[str, Any]] = []
        for quote in items:
            token_id = str(quote.get("token_id") or "")
            if not token_id:
                continue
            last_quotes[token_id] = quote
            decision = await self._step(quote)
            if decision:
                decisions.append(decision)
            await self._sweep_with_quote(quote)
        summary = await self.trader.get_summary(quote_overrides=last_quotes)
        fills = await pm_db.list_paper_fills(self.config.account_id, limit=10000)
        orders = await pm_db.list_paper_orders(self.config.account_id, limit=10000)
        return {
            "account_id": self.config.account_id,
            "quotes_seen": len(items),
            "decisions": decisions,
            "orders_count": len(orders),
            "fills_count": len(fills),
            "summary": summary,
            "fills": fills,
        }

    async def run_token(self, token_id: str, since: datetime, until: datetime, *, reset: bool = True) -> Dict[str, Any]:
        quotes = await pm_db.get_token_quotes(token_id, parse_ts_any(since), parse_ts_any(until))
        return await self.run_quotes(quotes, reset=reset)

    async def _step(self, quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        token_id = str(quote.get("token_id") or "")
        market_id = str(quote.get("market_id") or "")
        outcome = str(quote.get("outcome") or "YES").upper()
        mid = quote_midpoint(quote)
        positions = await pm_db.list_paper_positions(self.config.account_id, include_flat=True)
        position = next((item for item in positions if str(item.get("token_id") or "") == token_id), None)
        decision = await self.strategy.decide(quote=quote, position=position, config=self.config)
        if not decision:
            return None
        order = await self.trader.place_limit(
            market_id=market_id,
            token_id=token_id,
            outcome=outcome,
            side=decision.action,
            price=decision.price,
            size=decision.size,
            metadata={"replay": True, "signal": decision.reason, "quote_ts": str(quote.get("ts") or "")},
            fill_immediately=False,
        )
        order = await self.trader.try_fill_order(order["order_id"], quote=quote, simulation_time=parse_ts_any(quote["ts"]))
        return {"ts": quote.get("ts"), "action": decision.action, "reason": decision.reason, "midpoint": mid, "order": order}

    async def _sweep_with_quote(self, quote: Dict[str, Any]) -> Dict[str, Any]:
        token_id = str(quote.get("token_id") or "")
        if not token_id:
            return {"checked": 0, "filled": 0, "items": []}
        orders = await pm_db.list_paper_orders(self.config.account_id, status="ACTIVE", limit=1000)
        filled = []
        checked = 0
        for order in orders:
            if str(order.get("token_id") or "") != token_id:
                continue
            checked += 1
            updated = await self.trader.try_fill_order(order["order_id"], quote=quote, simulation_time=parse_ts_any(quote["ts"]))
            if updated and updated.get("status") == "FILLED":
                filled.append(updated)
        return {"checked": checked, "filled": len(filled), "items": filled}
