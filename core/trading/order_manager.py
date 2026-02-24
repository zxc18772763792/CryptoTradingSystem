"""
Order management module.
"""
from datetime import datetime
import math
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

from loguru import logger

from config.settings import settings
from core.exchanges import exchange_manager
from core.exchanges.base_exchange import Order, OrderSide, OrderType, OrderStatus
from core.utils.asset_valuation import STABLE_COINS, build_currency_usd_quotes


class OrderSource(Enum):
    MANUAL = "manual"
    STRATEGY = "strategy"
    API = "api"
    SYSTEM = "system"


@dataclass
class OrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    amount: float
    price: Optional[float] = None
    exchange: str = "binance"
    strategy: Optional[str] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    trigger_price: Optional[float] = None
    account_id: str = "main"
    order_mode: str = "normal"  # normal/iceberg/twap/vwap/conditional
    iceberg_parts: int = 1
    algo_slices: int = 1
    algo_interval_sec: int = 0
    reduce_only: bool = False
    params: Dict[str, Any] = field(default_factory=dict)


class OrderManager:
    def __init__(self):
        self._orders: Dict[str, Order] = {}
        self._pending_orders: Dict[str, OrderRequest] = {}
        self._order_callbacks: List[callable] = []
        self._order_meta: Dict[str, Dict[str, Any]] = {}
        self._paper_trading: bool = True
        self._paper_order_seq: int = 0

    @staticmethod
    def _request_meta(request: OrderRequest) -> Dict[str, Any]:
        return {
            "strategy": request.strategy,
            "account_id": request.account_id,
            "order_mode": request.order_mode,
            "stop_loss": request.stop_loss,
            "take_profit": request.take_profit,
            "trailing_stop_pct": request.trailing_stop_pct,
            "trailing_stop_distance": request.trailing_stop_distance,
            "trigger_price": request.trigger_price,
            "iceberg_parts": request.iceberg_parts,
            "algo_slices": request.algo_slices,
            "algo_interval_sec": request.algo_interval_sec,
            "reduce_only": request.reduce_only,
            "params": request.params or {},
        }

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        text = str(symbol or "").strip().upper()
        if "/" in text:
            left, right = text.split("/", 1)
            return left.strip(), right.strip()
        return text, "USDT"

    @staticmethod
    def _safe_nonnegative_float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            if math.isnan(out) or math.isinf(out):
                return float(default)
            return max(0.0, out)
        except Exception:
            return float(default)

    def _resolve_paper_cost_params(self, request: OrderRequest) -> tuple[float, float]:
        params = dict(request.params or {})
        fee_rate = self._safe_nonnegative_float(
            params.get("paper_fee_rate", params.get("fee_rate", settings.PAPER_FEE_RATE)),
            float(settings.PAPER_FEE_RATE or 0.0),
        )
        slippage_bps = self._safe_nonnegative_float(
            params.get("paper_slippage_bps", params.get("slippage_bps", settings.PAPER_SLIPPAGE_BPS)),
            float(settings.PAPER_SLIPPAGE_BPS or 0.0),
        )
        return min(fee_rate, 1.0), min(slippage_bps, 10000.0)

    def set_paper_trading(self, enabled: bool) -> None:
        self._paper_trading = enabled
        logger.info(f"Paper trading mode: {enabled}")

    def register_callback(self, callback: callable) -> None:
        self._order_callbacks.append(callback)

    async def _notify_callbacks(self, order: Order, event: str) -> None:
        for callback in self._order_callbacks:
            try:
                await callback(order, event)
            except Exception as e:
                logger.error(f"Order callback error: {e}")

    async def create_order(self, request: OrderRequest) -> Optional[Order]:
        if self._paper_trading:
            return await self._create_paper_order(request)
        return await self._create_real_order(request)

    def _next_paper_order_id(self) -> str:
        self._paper_order_seq = (self._paper_order_seq + 1) % 1000000
        return f"paper_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{self._paper_order_seq:06d}"

    def _next_rejected_order_id(self) -> str:
        self._paper_order_seq = (self._paper_order_seq + 1) % 1000000
        return f"rejected_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{self._paper_order_seq:06d}"

    async def _create_paper_order(self, request: OrderRequest) -> Order:
        order_id = self._next_paper_order_id()
        fill_price = float(request.price or 0.0)

        if fill_price <= 0:
            connector = exchange_manager.get_exchange(request.exchange)
            if connector:
                try:
                    ticker = await connector.get_ticker(request.symbol)
                    fill_price = float(ticker.last or 0.0)
                except Exception as e:
                    logger.warning(
                        f"[PAPER] Failed to fetch ticker for {request.symbol} "
                        f"on {request.exchange}: {e}"
                    )
                if fill_price <= 0:
                    try:
                        base, quote = self._split_symbol(request.symbol)
                        quotes = await build_currency_usd_quotes(
                            connector=connector,
                            currencies=[base, quote],
                            timeout_sec=1.2,
                            max_parallel=2,
                        )
                        base_usd = float(quotes.get(base, 1.0 if base in STABLE_COINS else 0.0) or 0.0)
                        quote_usd = float(quotes.get(quote, 1.0 if quote in STABLE_COINS else 0.0) or 0.0)
                        if base_usd > 0 and quote_usd > 0:
                            fill_price = base_usd / quote_usd
                    except Exception as e:
                        logger.debug(
                            f"[PAPER] quote fallback failed for {request.symbol} "
                            f"on {request.exchange}: {e}"
                        )

        reference_price = float(fill_price or 0.0)
        fee_rate, slippage_bps = self._resolve_paper_cost_params(request)
        slippage_rate = float(slippage_bps or 0.0) / 10000.0
        if reference_price > 0 and slippage_rate > 0:
            if request.side == OrderSide.BUY:
                fill_price = reference_price * (1.0 + slippage_rate)
            else:
                fill_price = reference_price * max(0.0, 1.0 - slippage_rate)
            if fill_price <= 0:
                fill_price = reference_price

        amount = float(request.amount or 0.0)
        notional_usd = abs(amount * float(fill_price or 0.0))
        fee_usd = notional_usd * fee_rate if notional_usd > 0 else 0.0
        slippage_cost_usd = abs(float(fill_price or 0.0) - reference_price) * abs(amount)

        order = Order(
            id=order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.order_type,
            price=fill_price,
            amount=amount,
            filled=amount,
            remaining=0,
            cost=amount * fill_price,
            status=OrderStatus.CLOSED,
            timestamp=datetime.now(),
            exchange=request.exchange,
        )

        self._orders[order_id] = order
        meta = self._request_meta(request)
        meta.update(
            {
                "paper": True,
                "paper_reference_price": round(reference_price, 8) if reference_price > 0 else 0.0,
                "paper_fee_rate": round(fee_rate, 8),
                "paper_fee_usd": round(fee_usd, 8),
                "paper_slippage_bps": round(slippage_bps, 4),
                "paper_slippage_rate": round(slippage_rate, 8),
                "paper_slippage_cost_usd": round(slippage_cost_usd, 8),
                "paper_notional_usd": round(notional_usd, 8),
            }
        )
        self._order_meta[order_id] = meta

        logger.info(
            f"[PAPER] Order created: {order_id} "
            f"{request.side.value} {amount} {request.symbol} @ {fill_price} "
            f"(ref={reference_price}, slip={slippage_bps}bps, fee={fee_usd:.6f})"
        )

        await self._notify_callbacks(order, "created")
        return order

    async def _create_real_order(self, request: OrderRequest) -> Optional[Order]:
        exchange = exchange_manager.get_exchange(request.exchange)
        if not exchange:
            logger.error(f"Exchange not found: {request.exchange}")
            return None

        try:
            params = dict(request.params or {})
            if request.stop_loss is not None:
                params.setdefault("stopLossPrice", float(request.stop_loss))
                params.setdefault("stopPrice", float(request.stop_loss))
            if request.take_profit is not None:
                params.setdefault("takeProfitPrice", float(request.take_profit))
            if request.trigger_price is not None:
                params.setdefault("triggerPrice", float(request.trigger_price))
            if request.reduce_only:
                params.setdefault("reduceOnly", True)

            order = await exchange.create_order(
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                amount=request.amount,
                price=request.price,
                params=params,
            )

            self._orders[order.id] = order
            self._order_meta[order.id] = self._request_meta(request)
            logger.info(
                f"Order created: {order.id} "
                f"{request.side.value} {request.amount} {request.symbol} @ {request.price}"
            )

            await self._notify_callbacks(order, "created")
            return order
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            return None

    async def record_rejected_order(
        self,
        request: OrderRequest,
        reason: str,
        price: Optional[float] = None,
    ) -> Order:
        order_id = self._next_rejected_order_id()
        reject_price = float(price if price and price > 0 else request.price or 0.0)
        amount = float(request.amount or 0.0)

        order = Order(
            id=order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.order_type,
            price=reject_price,
            amount=amount,
            filled=0.0,
            remaining=amount,
            cost=amount * reject_price,
            status=OrderStatus.REJECTED,
            timestamp=datetime.now(),
            exchange=request.exchange,
        )

        meta = self._request_meta(request)
        meta.update(
            {
                "rejected": True,
                "reject_reason": str(reason or "unknown"),
            }
        )

        self._orders[order_id] = order
        self._order_meta[order_id] = meta

        logger.warning(
            f"[ORDER_REJECTED] {order_id} {request.side.value} {amount} {request.symbol} "
            f"@ {reject_price} reason={reason}"
        )
        await self._notify_callbacks(order, "rejected")
        return order

    async def cancel_order(
        self,
        order_id: str,
        symbol: str,
        exchange: str = "binance",
    ) -> bool:
        if self._paper_trading:
            return await self._cancel_paper_order(order_id)

        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return False

        try:
            success = await connector.cancel_order(order_id, symbol)
            if success and order_id in self._orders:
                self._orders[order_id].status = OrderStatus.CANCELED
                await self._notify_callbacks(self._orders[order_id], "canceled")
            return success
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def _cancel_paper_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id].status = OrderStatus.CANCELED
            logger.info(f"[PAPER] Order cancelled: {order_id}")
            await self._notify_callbacks(self._orders[order_id], "canceled")
            return True
        return False

    async def get_order(
        self,
        order_id: str,
        symbol: str,
        exchange: str = "binance",
    ) -> Optional[Order]:
        if self._paper_trading:
            return self._orders.get(order_id)

        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return None

        try:
            order = await connector.get_order(order_id, symbol)
            self._orders[order_id] = order
            return order
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None

    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> List[Order]:
        if self._paper_trading:
            return [
                o for o in self._orders.values()
                if o.status == OrderStatus.OPEN
                and (symbol is None or o.symbol == symbol)
                and (exchange is None or o.exchange == exchange)
            ]

        if exchange is None:
            return []

        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return []

        try:
            orders = await connector.get_open_orders(symbol)
            for order in orders:
                self._orders[order.id] = order
            return orders
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    def get_recent_orders(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 100,
    ) -> List[Order]:
        orders = [
            o for o in self._orders.values()
            if (symbol is None or o.symbol == symbol)
            and (exchange is None or o.exchange == exchange)
        ]
        orders.sort(key=lambda o: o.timestamp or datetime.min, reverse=True)
        return orders[: max(1, limit)]

    async def cancel_all_orders(
        self,
        symbol: Optional[str] = None,
        exchange: str = "binance",
    ) -> int:
        orders = await self.get_open_orders(symbol, exchange)
        cancelled = 0
        for order in orders:
            if await self.cancel_order(order.id, order.symbol, exchange):
                cancelled += 1
        return cancelled

    def get_order_by_id(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_order_metadata(self, order_id: str) -> Dict[str, Any]:
        return dict(self._order_meta.get(order_id) or {})

    def get_all_orders(self) -> List[Order]:
        return list(self._orders.values())

    def get_orders_by_strategy(self, strategy: str) -> List[Order]:
        return [
            o for o in self._orders.values()
            if getattr(o, "strategy", None) == strategy
        ]

    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        return [o for o in self._orders.values() if o.symbol == symbol]

    def get_stats(self) -> Dict[str, int]:
        orders = list(self._orders.values())
        return {
            "total_orders": len(orders),
            "open_orders": len([o for o in orders if o.status == OrderStatus.OPEN]),
            "closed_orders": len([o for o in orders if o.status == OrderStatus.CLOSED]),
            "canceled_orders": len([o for o in orders if o.status == OrderStatus.CANCELED]),
            "buy_orders": len([o for o in orders if o.side == OrderSide.BUY]),
            "sell_orders": len([o for o in orders if o.side == OrderSide.SELL]),
        }

    def clear_paper_history(self) -> Dict[str, int]:
        """Clear in-memory paper-trading order history."""
        total = len(self._orders)
        meta_total = len(self._order_meta)
        pending_total = len(self._pending_orders)
        self._orders.clear()
        self._order_meta.clear()
        self._pending_orders.clear()
        self._paper_order_seq = 0
        return {
            "orders_cleared": total,
            "metadata_cleared": meta_total,
            "pending_cleared": pending_total,
        }


order_manager = OrderManager()
