
"""Trading execution engine."""
from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config.settings import settings
from core.exchanges import exchange_manager
from core.risk.risk_manager import risk_manager
from core.strategies import Signal, SignalType
from core.strategies.strategy_manager import strategy_manager
from core.trading.account_manager import account_manager
from core.trading.order_manager import OrderRequest, OrderSide, OrderType, order_manager
from core.trading.position_manager import PositionSide, position_manager
from core.utils.asset_valuation import STABLE_COINS, build_currency_usd_quotes


@dataclass
class ConditionalManualOrder:
    conditional_id: str
    created_at: str
    exchange: str
    symbol: str
    side: str
    order_type: str
    amount: float
    price: Optional[float]
    leverage: float
    stop_loss: Optional[float]
    take_profit: Optional[float]
    trailing_stop_pct: Optional[float]
    trailing_stop_distance: Optional[float]
    trigger_price: float
    account_id: str
    strategy: str = "manual"
    reduce_only: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conditional_id": self.conditional_id,
            "created_at": self.created_at,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "amount": self.amount,
            "price": self.price,
            "leverage": self.leverage,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "trailing_stop_pct": self.trailing_stop_pct,
            "trailing_stop_distance": self.trailing_stop_distance,
            "trigger_price": self.trigger_price,
            "account_id": self.account_id,
            "strategy": self.strategy,
            "reduce_only": self.reduce_only,
        }


class ExecutionEngine:
    def __init__(self):
        self._running: bool = False
        self._signal_queue: asyncio.Queue = asyncio.Queue()
        self._queue_task: Optional[asyncio.Task] = None
        self._execution_callbacks: List[callable] = []
        self._paper_trading: bool = True

        self._cached_equity: float = 0.0
        self._equity_updated_at: Optional[datetime] = None
        self._equity_cache_seconds = 45
        self._asset_unit_usd_cache: Dict[str, Dict[str, float]] = {}
        self._paper_equity_anchor: float = 0.0
        self._paper_total_fees_usd: float = 0.0
        self._paper_fee_applied_orders: set[str] = set()
        self._signal_diagnostics: Dict[str, Any] = {
            "submitted": 0,
            "executed": 0,
            "skipped_zero_qty": 0,
            "risk_rejected": 0,
            "order_failed": 0,
            "order_timeout": 0,
            "exceptions": 0,
            "last_signal": None,
            "last_result": None,
            "last_updated_at": None,
        }

        self._conditional_orders: Dict[str, ConditionalManualOrder] = {}
        self._conditional_seq = 0
        self._last_bg_check_at: Optional[datetime] = None
        self._bg_check_interval_seconds = 2.0

    def set_paper_trading(self, enabled: bool) -> None:
        self._paper_trading = bool(enabled)
        settings.TRADING_MODE = "paper" if self._paper_trading else "live"
        order_manager.set_paper_trading(self._paper_trading)
        risk_manager.set_account_scope("paper" if self._paper_trading else "live", reset_baseline=True)
        if self._paper_trading and self._paper_equity_anchor < 100:
            report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
            seed = float(self._cached_equity or 0.0)
            if report_eq > 0:
                seed = max(seed, report_eq)
            self._paper_equity_anchor = max(seed, float(getattr(settings, "PAPER_INITIAL_EQUITY", 10000.0) or 10000.0))
        logger.info(f"Execution engine paper trading: {self._paper_trading}")

    def get_trading_mode(self) -> str:
        return "paper" if self._paper_trading else "live"

    def is_paper_mode(self) -> bool:
        return self._paper_trading

    def register_callback(self, callback: callable) -> None:
        self._execution_callbacks.append(callback)

    async def _notify_callbacks(self, event: str, data: Any) -> None:
        for callback in self._execution_callbacks:
            try:
                await callback(event, data)
            except Exception as e:
                logger.error(f"Execution callback error: {e}")

    async def submit_signal(self, signal: Signal) -> bool:
        self._signal_diagnostics["submitted"] = int(self._signal_diagnostics.get("submitted", 0)) + 1
        self._signal_diagnostics["last_signal"] = {
            "strategy": signal.strategy_name,
            "symbol": signal.symbol,
            "signal_type": signal.signal_type.value,
            "price": float(signal.price or 0.0),
            "strength": float(signal.strength or 0.0),
            "timestamp": signal.timestamp.isoformat() if signal.timestamp else datetime.utcnow().isoformat(),
        }
        self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
        if self._running:
            await self._ensure_queue_worker()
        if self._queue_task and not self._queue_task.done():
            await self._signal_queue.put(signal)
            logger.debug(
                f"Signal queued: {signal.signal_type.value} {signal.symbol} "
                f"(queue_size={self._signal_queue.qsize()})"
            )
            return True

        logger.warning(
            f"Signal queue worker unavailable, execute inline: "
            f"{signal.signal_type.value} {signal.symbol}"
        )
        asyncio.create_task(self.execute_signal(signal))
        return True

    async def _ensure_queue_worker(self) -> None:
        if not self._running:
            return
        if self._queue_task and not self._queue_task.done():
            return
        self._queue_task = asyncio.create_task(
            self._process_signal_queue(),
            name="execution_signal_queue",
        )
        logger.warning("Execution signal queue worker started/restarted")

    def is_queue_worker_alive(self) -> bool:
        return bool(self._queue_task and not self._queue_task.done())

    async def _estimate_asset_usd(self, connector: Any, currency: str, total: float) -> float:
        if total <= 0:
            return 0.0
        if currency in STABLE_COINS:
            return float(total)
        try:
            quotes = await build_currency_usd_quotes(
                connector=connector,
                currencies=[currency],
                timeout_sec=1.6,
            )
            return float(total) * float(quotes.get(str(currency or "").upper(), 0.0) or 0.0)
        except Exception:
            return 0.0

    async def _refresh_equity(self) -> float:
        total_usd = 0.0
        has_unpriced_assets = False
        report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
        for exchange_name in exchange_manager.get_connected_exchanges():
            connector = exchange_manager.get_exchange(exchange_name)
            if not connector:
                continue
            try:
                if (not self._paper_trading) and str(exchange_name).lower() == "binance":
                    try:
                        # Reuse the web trading API's fast Binance wallet snapshot logic so live
                        # strategy sizing does not depend on the slower generic CCXT balance path.
                        from web.api.trading import _fetch_binance_live_wallet_snapshot_fast

                        snap = await asyncio.wait_for(
                            _fetch_binance_live_wallet_snapshot_fast(),
                            timeout=8.5,
                        )
                        snap_total = float((snap or {}).get("total_usd") or 0.0)
                        if snap_total > 0:
                            total_usd += snap_total
                            continue
                    except Exception as e:
                        logger.debug(f"Fast Binance equity snapshot unavailable in execution engine: {e}")

                balances = await connector.get_balance()
                unit_cache = self._asset_unit_usd_cache.setdefault(exchange_name, {})

                currencies = []
                for b in balances:
                    ccy = str(b.currency or "").upper()
                    total = float(b.total or 0.0)
                    if total <= 0 or ccy in STABLE_COINS:
                        continue
                    currencies.append(ccy)

                live_quotes = await build_currency_usd_quotes(
                    connector=connector,
                    currencies=currencies,
                    timeout_sec=1.6,
                    max_parallel=2,
                )
                for ccy, unit in live_quotes.items():
                    if unit and float(unit) > 0:
                        unit_cache[ccy] = float(unit)

                unpriced_assets = 0
                for b in balances:
                    ccy = str(b.currency or "").upper()
                    total = float(b.total or 0.0)
                    if total <= 0:
                        continue
                    unit_usd = 1.0 if ccy in STABLE_COINS else float(live_quotes.get(ccy, 0.0) or 0.0)
                    if unit_usd <= 0 and ccy not in STABLE_COINS:
                        unit_usd = float(unit_cache.get(ccy, 0.0) or 0.0)
                    if unit_usd <= 0:
                        unpriced_assets += 1
                        continue
                    total_usd += float(total) * float(unit_usd)

                if unpriced_assets > 0:
                    logger.debug(f"Equity valuation fallback on {exchange_name}: unpriced_assets={unpriced_assets}")
                    has_unpriced_assets = True
            except Exception as e:
                logger.debug(f"Failed to estimate equity on {exchange_name}: {e}")

        if total_usd > 0:
            candidate = float(total_usd)
            if (
                (not self._paper_trading)
                and report_eq > 100
                and candidate < report_eq * 0.35
            ):
                logger.warning(
                    f"Skip suspicious low live equity snapshot in execution engine: "
                    f"candidate={candidate:.4f}, report_eq={report_eq:.4f}"
                )
                candidate = float(report_eq)
            if (
                has_unpriced_assets
                and float(self._cached_equity or 0.0) > 0
                and candidate < float(self._cached_equity) * 0.6
            ):
                logger.warning(
                    f"Skip abnormal equity drop in execution engine: "
                    f"cached={self._cached_equity:.4f}, new={candidate:.4f}"
                )
                candidate = float(self._cached_equity)
            # Avoid overwriting a stable cached value with transient tiny estimates.
            if self._paper_trading and candidate < 100 and float(self._cached_equity or 0.0) >= 100:
                candidate = float(self._cached_equity)
            self._cached_equity = candidate
            self._equity_updated_at = datetime.utcnow()
            risk_manager.update_equity(self._cached_equity)

        return float(self._cached_equity or 0.0)

    async def _get_account_equity(self, force: bool = False) -> float:
        report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
        if self._paper_trading:
            return await self.get_account_equity_snapshot(force=force)

        now = datetime.utcnow()
        cached_eq = float(self._cached_equity or 0.0)
        if not force:
            if report_eq > 100:
                if report_eq > cached_eq:
                    self._cached_equity = report_eq
                    self._equity_updated_at = now
                return float(report_eq)
            if (
                cached_eq > 100
                and self._equity_updated_at
                and (now - self._equity_updated_at).total_seconds() < max(30, int(self._equity_cache_seconds))
            ):
                return float(cached_eq)

        if (
            not force
            and self._equity_updated_at
            and (now - self._equity_updated_at).total_seconds() < self._equity_cache_seconds
        ):
            eq = cached_eq
        else:
            eq = await self._refresh_equity()

        if eq <= 0:
            if report_eq > 0:
                eq = report_eq
            elif cached_eq > 0:
                eq = cached_eq
        elif (not self._paper_trading) and report_eq > 100 and eq < report_eq * 0.35:
            eq = report_eq

        if self._paper_trading and eq < 100:
            eq = max(eq, float(getattr(settings, "PAPER_INITIAL_EQUITY", 10000.0) or 10000.0))

        return float(eq)

    async def get_account_equity_snapshot(self, force: bool = False) -> float:
        """Public equity snapshot for dashboard/risk alignment."""
        if not self._paper_trading:
            return await self._get_account_equity(force=force)

        if self._paper_equity_anchor < 100:
            report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
            seed = float(self._paper_equity_anchor or 0.0)
            seed = max(seed, float(self._cached_equity or 0.0))
            if report_eq > 0:
                seed = max(seed, report_eq)
            self._paper_equity_anchor = max(seed, float(getattr(settings, "PAPER_INITIAL_EQUITY", 10000.0) or 10000.0))

        realized = float(position_manager.get_total_realized_pnl() or 0.0)
        unrealized = float(position_manager.get_total_pnl() or 0.0)
        raw_mark_equity = float(
            self._paper_equity_anchor + realized + unrealized - float(self._paper_total_fees_usd or 0.0)
        )
        if raw_mark_equity <= 0:
            logger.warning(
                "Paper equity dropped below zero; clamp to floor for risk visibility. "
                f"anchor={self._paper_equity_anchor:.4f}, realized={realized:.4f}, "
                f"unrealized={unrealized:.4f}, fees={float(self._paper_total_fees_usd or 0.0):.4f}"
            )
            mark_equity = max(1.0, float(self._paper_equity_anchor) * 0.001)
        else:
            mark_equity = raw_mark_equity

        self._cached_equity = float(mark_equity)
        self._equity_updated_at = datetime.utcnow()
        risk_manager.update_equity(self._cached_equity)
        return float(self._cached_equity)

    async def _resolve_price(self, exchange: str, symbol: str, preferred_price: Optional[float] = None) -> float:
        if preferred_price and preferred_price > 0:
            return float(preferred_price)
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return 0.0
        try:
            ticker = await connector.get_ticker(symbol)
            return float(ticker.last or 0.0)
        except Exception:
            return 0.0

    async def _resolve_order_context(
        self,
        exchange: str,
        symbol: str,
        quantity: float,
        preferred_price: Optional[float],
    ) -> Tuple[float, float]:
        price = await self._resolve_price(exchange, symbol, preferred_price)
        order_value = float(quantity or 0.0) * float(price or 0.0)
        return float(price or 0.0), float(order_value or 0.0)

    @staticmethod
    def _safe_nonnegative_float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            if out < 0:
                return 0.0
            return out
        except Exception:
            return float(default)

    @staticmethod
    def _floor_to_decimals(value: float, decimals: int = 8) -> float:
        if decimals < 0:
            decimals = 0
        factor = 10 ** decimals
        return math.floor(float(value) * factor) / factor

    @staticmethod
    def _ceil_to_decimals(value: float, decimals: int = 8) -> float:
        if decimals < 0:
            decimals = 0
        factor = 10 ** decimals
        return math.ceil(float(value) * factor) / factor

    async def _get_exchange_amount_rules(self, exchange: str, symbol: str) -> Tuple[float, int]:
        connector = exchange_manager.get_exchange(exchange)
        client = getattr(connector, "_client", None) if connector else None
        min_amount = 0.0
        decimals = 8
        if not client:
            return min_amount, decimals
        try:
            market = client.market(symbol)
        except Exception:
            market = None
        if isinstance(market, dict):
            try:
                min_amount = max(0.0, float((((market.get("limits") or {}).get("amount") or {}).get("min")) or 0.0))
            except Exception:
                min_amount = 0.0
            try:
                precision = (market.get("precision") or {}).get("amount")
                if precision is not None:
                    if isinstance(precision, int):
                        decimals = max(0, int(precision))
                    else:
                        precision_value = float(precision)
                        if precision_value >= 1:
                            decimals = 0
                        elif precision_value > 0:
                            decimals = max(0, int(round(-math.log10(precision_value))))
            except Exception:
                decimals = 8
        return min_amount, decimals

    def _consume_paper_order_cost(self, order_id: Optional[str]) -> Dict[str, float]:
        if not self._paper_trading:
            return {"fee_usd": 0.0, "slippage_cost_usd": 0.0}
        oid = str(order_id or "").strip()
        if not oid or oid in self._paper_fee_applied_orders:
            return {"fee_usd": 0.0, "slippage_cost_usd": 0.0}
        meta = order_manager.get_order_metadata(oid)
        fee_usd = self._safe_nonnegative_float(meta.get("paper_fee_usd"), 0.0)
        slippage_cost_usd = self._safe_nonnegative_float(meta.get("paper_slippage_cost_usd"), 0.0)
        self._paper_fee_applied_orders.add(oid)
        if fee_usd > 0:
            self._paper_total_fees_usd += float(fee_usd)
        return {
            "fee_usd": float(fee_usd),
            "slippage_cost_usd": float(slippage_cost_usd),
        }

    def _build_reject_reason(self) -> str:
        report = risk_manager.get_risk_report()
        halt_reason = str(report.get("halt_reason") or "").strip()
        if halt_reason:
            return halt_reason
        alerts = report.get("alerts") or []
        if alerts:
            last = alerts[-1]
            title = str(last.get("title") or "").strip()
            msg = str(last.get("message") or "").strip()
            if title and msg:
                return f"{title}: {msg}"
            if msg:
                return msg
            if title:
                return title
        return "风控规则拒绝"

    async def _calculate_quantity(
        self,
        signal: Signal,
        exchange: str,
        account_equity: Optional[float],
        strategy_allocation: float,
    ) -> float:
        price = await self._resolve_price(exchange, signal.symbol, signal.price)
        if price <= 0:
            return 0.0
        configured_min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
        risk_buffer = 0.998

        if signal.quantity is not None:
            qty = max(0.0, float(signal.quantity))
            if qty <= 0:
                return 0.0
            # Unless explicitly requested, avoid sending dust-size strategy orders.
            if not bool((signal.metadata or {}).get("respect_quantity", False)):
                qty = max(qty, min_notional / price)
            return max(0.0, self._floor_to_decimals(qty, 8))

        equity = float(account_equity or 0.0)
        if equity <= 0:
            # Conservative fallback for unknown equity in paper mode.
            return max(0.0, round(max(10.0, min_notional) / price, 8))

        strength = float(signal.strength or 1.0)
        strength = max(0.1, min(strength, 1.0))

        single_cap = equity * float(risk_manager.max_position_size or 0.1)
        alloc_ratio = max(0.0, min(float(strategy_allocation or 0.0), 1.0))
        alloc_cap = equity * alloc_ratio if alloc_ratio > 0 else single_cap
        market_type = str((signal.metadata or {}).get("market_type") or "").strip().lower()
        if not market_type and signal.strategy_name:
            strategy = strategy_manager.get_strategy(signal.strategy_name)
            market_type = str((getattr(strategy, "params", {}) or {}).get("market_type") or "").strip().lower()
        is_binance_futures = str(exchange or "").lower() == "binance" and market_type in {
            "future", "futures", "swap", "contract", "perp", "perpetual"
        }
        exchange_min_notional = 100.0 if is_binance_futures else 10.0

        current_strategy_exposure = 0.0
        if signal.strategy_name:
            current_strategy_exposure = sum(
                float(p.value or 0.0)
                for p in position_manager.get_positions_by_strategy(signal.strategy_name)
            )

        remaining_alloc_cap = max(0.0, alloc_cap - current_strategy_exposure)
        if alloc_ratio > 0 and remaining_alloc_cap <= 0:
            return 0.0
        effective_min_notional = configured_min_notional
        if alloc_ratio > 0:
            effective_min_notional = min(
                configured_min_notional,
                max(exchange_min_notional, remaining_alloc_cap * 0.98),
            )
        else:
            effective_min_notional = min(
                configured_min_notional,
                max(exchange_min_notional, single_cap * 0.98),
            )
        effective_min_notional = max(exchange_min_notional, effective_min_notional)
        if alloc_ratio > 0 and remaining_alloc_cap < effective_min_notional:
            logger.debug(
                f"Skip tiny order for {signal.strategy_name or 'unknown'}: "
                f"remaining allocation {remaining_alloc_cap:.4f} < min_notional {effective_min_notional:.4f}"
            )
            return 0.0

        target_notional = min(single_cap, remaining_alloc_cap if alloc_ratio > 0 else single_cap)
        target_notional *= strength
        target_notional = max(effective_min_notional, target_notional)
        buffered_cap = single_cap * risk_buffer
        if alloc_ratio > 0:
            buffered_cap = min(buffered_cap, remaining_alloc_cap * risk_buffer)
            if buffered_cap < effective_min_notional:
                return 0.0
            target_notional = min(target_notional, buffered_cap)
        else:
            target_notional = min(target_notional, buffered_cap)
        if target_notional <= 0:
            return 0.0

        qty = target_notional / price
        min_amount, amount_decimals = await self._get_exchange_amount_rules(exchange, signal.symbol)
        if min_amount > 0 and qty < min_amount:
            min_amount_notional = min_amount * price
            effective_cap = max(0.0, buffered_cap)
            if alloc_ratio > 0:
                effective_cap = min(effective_cap, max(0.0, remaining_alloc_cap))
            if effective_cap <= 0 or min_amount_notional > (effective_cap + max(0.05, effective_cap * 0.01)):
                logger.info(
                    f"Skip strategy order below exchange min amount: strategy={signal.strategy_name} "
                    f"symbol={signal.symbol} qty={qty:.8f} min_amount={min_amount:.8f} "
                    f"required_notional={min_amount_notional:.4f} cap={effective_cap:.4f}"
                )
                return 0.0
            qty = float(min_amount)

        floored_qty = max(0.0, self._floor_to_decimals(qty, amount_decimals))
        if floored_qty <= 0:
            return 0.0

        floored_notional = floored_qty * price
        if floored_notional + 1e-9 >= effective_min_notional:
            return floored_qty

        required_qty = self._ceil_to_decimals(effective_min_notional / price, amount_decimals)
        if required_qty <= 0:
            return floored_qty

        required_notional = required_qty * price
        effective_cap = max(0.0, buffered_cap)
        if alloc_ratio > 0:
            effective_cap = min(effective_cap, max(0.0, remaining_alloc_cap))

        if required_notional <= effective_cap + max(0.05, effective_cap * 0.01):
            logger.info(
                f"Adjust quantity upward to satisfy min notional: strategy={signal.strategy_name} "
                f"symbol={signal.symbol} qty={floored_qty:.8f}->{required_qty:.8f} "
                f"notional={floored_notional:.4f}->{required_notional:.4f} "
                f"cap={effective_cap:.4f}"
            )
            return required_qty

        logger.info(
            f"Skip strategy order after precision floor broke min notional: strategy={signal.strategy_name} "
            f"symbol={signal.symbol} floored_qty={floored_qty:.8f} "
            f"required_qty={required_qty:.8f} required_notional={required_notional:.4f} "
            f"cap={effective_cap:.4f}"
        )
        return 0.0

    def _resolve_strategy_trade_policy(
        self,
        strategy_name: Optional[str],
        exchange: str,
    ) -> Dict[str, Any]:
        strategy = strategy_manager.get_strategy(strategy_name) if strategy_name else None
        params = dict(getattr(strategy, "params", {}) or {})

        market_type = str(params.get("market_type") or "").strip().lower()
        if not market_type:
            connector = exchange_manager.get_exchange(exchange)
            market_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").strip().lower()
        if not market_type:
            market_type = "futures" if self._paper_trading else "spot"

        is_derivatives = market_type in {"future", "futures", "swap", "contract", "perp", "perpetual"}
        default_allow_short = True if self._paper_trading else is_derivatives

        allow_long = bool(params.get("allow_long", True))
        allow_short = bool(params.get("allow_short", default_allow_short))
        reverse_on_signal = bool(params.get("reverse_on_signal", True))
        allow_pyramiding = bool(params.get("allow_pyramiding", False))

        return {
            "market_type": market_type,
            "allow_long": allow_long,
            "allow_short": allow_short,
            "reverse_on_signal": reverse_on_signal,
            "allow_pyramiding": allow_pyramiding,
        }
    async def execute_signal(self, signal: Signal) -> Optional[Dict[str, Any]]:
        try:
            if signal.signal_type == SignalType.CLOSE_LONG:
                return await self._close_position(signal, PositionSide.LONG)
            if signal.signal_type == SignalType.CLOSE_SHORT:
                return await self._close_position(signal, PositionSide.SHORT)

            if signal.signal_type == SignalType.BUY:
                side = OrderSide.BUY
                position_side = PositionSide.LONG
            elif signal.signal_type == SignalType.SELL:
                side = OrderSide.SELL
                position_side = PositionSide.SHORT
            else:
                return None

            account_id = str(signal.metadata.get("account_id", "main"))
            exchange = account_manager.resolve_exchange(account_id, str(signal.metadata.get("exchange", "binance")))
            leverage = float(signal.metadata.get("leverage", 1.0) or 1.0)
            trade_policy = self._resolve_strategy_trade_policy(signal.strategy_name, exchange)
            existing_position = position_manager.get_position(exchange, signal.symbol, account_id=account_id)

            if side == OrderSide.BUY and not bool(trade_policy.get("allow_long", True)):
                if existing_position and existing_position.side == PositionSide.SHORT:
                    close_signal = Signal(
                        symbol=signal.symbol,
                        signal_type=SignalType.CLOSE_SHORT,
                        price=signal.price,
                        timestamp=signal.timestamp,
                        strategy_name=signal.strategy_name,
                        strength=signal.strength,
                        quantity=signal.quantity,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        metadata=dict(signal.metadata or {}),
                    )
                    return await self._close_position(close_signal, PositionSide.SHORT)
                return None

            if side == OrderSide.SELL and not bool(trade_policy.get("allow_short", True)):
                if existing_position and existing_position.side == PositionSide.LONG:
                    close_signal = Signal(
                        symbol=signal.symbol,
                        signal_type=SignalType.CLOSE_LONG,
                        price=signal.price,
                        timestamp=signal.timestamp,
                        strategy_name=signal.strategy_name,
                        strength=signal.strength,
                        quantity=signal.quantity,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        metadata=dict(signal.metadata or {}),
                    )
                    return await self._close_position(close_signal, PositionSide.LONG)
                return None

            if existing_position:
                same_direction = (
                    (side == OrderSide.BUY and existing_position.side == PositionSide.LONG)
                    or (side == OrderSide.SELL and existing_position.side == PositionSide.SHORT)
                )
                if same_direction and not bool(trade_policy.get("allow_pyramiding", False)):
                    return None

            if bool(trade_policy.get("reverse_on_signal", True)) and existing_position:
                need_reverse = (
                    (side == OrderSide.BUY and existing_position.side == PositionSide.SHORT)
                    or (side == OrderSide.SELL and existing_position.side == PositionSide.LONG)
                )
                if need_reverse:
                    close_signal = Signal(
                        symbol=signal.symbol,
                        signal_type=(
                            SignalType.CLOSE_SHORT
                            if side == OrderSide.BUY
                            else SignalType.CLOSE_LONG
                        ),
                        price=signal.price,
                        timestamp=signal.timestamp,
                        strategy_name=signal.strategy_name,
                        strength=signal.strength,
                        quantity=signal.quantity,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        metadata=dict(signal.metadata or {}),
                    )
                    await self._close_position(
                        close_signal,
                        PositionSide.SHORT if side == OrderSide.BUY else PositionSide.LONG,
                    )
                    existing_position = position_manager.get_position(exchange, signal.symbol, account_id=account_id)
                    if existing_position and existing_position.side != position_side:
                        return None

            logger.info(
                f"Executing strategy signal: strategy={signal.strategy_name} "
                f"symbol={signal.symbol} type={signal.signal_type.value}"
            )
            try:
                account_equity = await asyncio.wait_for(self._get_account_equity(), timeout=12.0)
            except asyncio.TimeoutError:
                report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
                cached_eq = float(self._cached_equity or 0.0)
                fallback_eq = max(report_eq, cached_eq, 1000.0 if not self._paper_trading else 0.0)
                if fallback_eq <= 0:
                    raise
                account_equity = float(fallback_eq)
                logger.warning(
                    f"Strategy equity snapshot timed out, fallback used: "
                    f"strategy={signal.strategy_name} equity={account_equity:.4f}"
                )
            strategy_allocation = strategy_manager.get_strategy_allocation(signal.strategy_name)

            qty = await self._calculate_quantity(
                signal=signal,
                exchange=exchange,
                account_equity=account_equity,
                strategy_allocation=strategy_allocation,
            )
            if qty <= 0:
                self._signal_diagnostics["skipped_zero_qty"] = int(self._signal_diagnostics.get("skipped_zero_qty", 0)) + 1
                self._signal_diagnostics["last_result"] = {
                    "status": "skipped_zero_qty",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "equity": float(account_equity or 0.0),
                    "allocation": float(strategy_allocation or 0.0),
                }
                self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
                logger.info(
                    f"Skip strategy order due to zero quantity: strategy={signal.strategy_name} "
                    f"symbol={signal.symbol} exchange={exchange} equity={account_equity} "
                    f"allocation={strategy_allocation}"
                )
                return None

            quote_price, order_value = await asyncio.wait_for(
                self._resolve_order_context(exchange, signal.symbol, qty, signal.price),
                timeout=8.0,
            )

            req = OrderRequest(
                symbol=signal.symbol,
                side=side,
                order_type=OrderType.MARKET if signal.quantity is None else OrderType.LIMIT,
                amount=qty,
                price=signal.price,
                exchange=exchange,
                strategy=signal.strategy_name,
                account_id=account_id,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                trailing_stop_pct=(
                    float(signal.metadata.get("trailing_stop_pct"))
                    if signal.metadata.get("trailing_stop_pct") is not None
                    else None
                ),
                trailing_stop_distance=(
                    float(signal.metadata.get("trailing_stop_distance"))
                    if signal.metadata.get("trailing_stop_distance") is not None
                    else None
                ),
                params={
                    "leverage": leverage,
                    "market_type": str(trade_policy.get("market_type") or ""),
                },
            )

            # Treat opposite-side action against existing position as close/reduce.
            # This avoids blocking legitimate close actions when max-open-position limit is reached.
            closes_existing = bool(
                existing_position
                and (
                    (side == OrderSide.BUY and existing_position.side == PositionSide.SHORT)
                    or (side == OrderSide.SELL and existing_position.side == PositionSide.LONG)
                )
            )

            if not risk_manager.pre_trade_check(
                symbol=signal.symbol,
                side=side.value,
                strategy_name=signal.strategy_name,
                account_equity=account_equity,
                order_value=order_value,
                leverage=leverage,
                strategy_allocation=strategy_allocation,
                allow_close=closes_existing,
            ):
                self._signal_diagnostics["risk_rejected"] = int(self._signal_diagnostics.get("risk_rejected", 0)) + 1
                reason = self._build_reject_reason()
                self._signal_diagnostics["last_result"] = {
                    "status": "risk_rejected",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "reason": reason,
                    "order_value": float(order_value or 0.0),
                }
                self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
                rejected = await order_manager.record_rejected_order(
                    request=req,
                    reason=reason,
                    price=quote_price or signal.price,
                )
                await self._notify_callbacks(
                    "risk_rejected",
                    {
                        "type": "strategy_signal",
                        "symbol": signal.symbol,
                        "strategy": signal.strategy_name,
                        "reason": reason,
                        "order_id": rejected.id,
                    },
                )
                return None
            try:
                order = await asyncio.wait_for(order_manager.create_order(req), timeout=20.0)
            except asyncio.TimeoutError:
                fail_reason = "策略下单超时"
                self._signal_diagnostics["order_timeout"] = int(self._signal_diagnostics.get("order_timeout", 0)) + 1
                self._signal_diagnostics["last_result"] = {
                    "status": "order_timeout",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "account_id": account_id,
                }
                self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
                await order_manager.record_rejected_order(
                    request=req,
                    reason=fail_reason,
                    price=quote_price or signal.price,
                )
                logger.error(
                    f"Strategy order timeout: strategy={signal.strategy_name} "
                    f"symbol={signal.symbol} exchange={exchange} account_id={account_id}"
                )
                return None
            if not order:
                fail_reason = str(order_manager.get_last_error() or "").strip() or "下单执行失败"
                self._signal_diagnostics["order_failed"] = int(self._signal_diagnostics.get("order_failed", 0)) + 1
                self._signal_diagnostics["last_result"] = {
                    "status": "order_failed",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "account_id": account_id,
                    "reason": fail_reason,
                }
                self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
                await order_manager.record_rejected_order(
                    request=req,
                    reason=fail_reason,
                    price=quote_price or signal.price,
                )
                logger.error(
                    f"Strategy order failed: strategy={signal.strategy_name} "
                    f"symbol={signal.symbol} exchange={exchange} account_id={account_id} "
                    f"reason={fail_reason}"
                )
                return None

            paper_cost = self._consume_paper_order_cost(order.id)
            fee_usd = float(paper_cost.get("fee_usd", 0.0) or 0.0)
            slippage_cost_usd = float(paper_cost.get("slippage_cost_usd", 0.0) or 0.0)
            fill_price = float(order.price or signal.price or quote_price or 0.0)
            trade_pnl = 0.0
            current_position = position_manager.get_position(exchange, signal.symbol, account_id=account_id)

            def _merge_protection_settings() -> None:
                if not current_position:
                    return
                if req.stop_loss is not None:
                    current_position.stop_loss = float(req.stop_loss)
                if req.take_profit is not None:
                    current_position.take_profit = float(req.take_profit)
                if req.trailing_stop_pct is not None:
                    pct = max(0.0, float(req.trailing_stop_pct))
                    current_position.trailing_stop_pct = pct if pct > 0 else None
                    if pct > 0:
                        current_position.trailing_stop_distance = None
                elif req.trailing_stop_distance is not None:
                    dist = max(0.0, float(req.trailing_stop_distance))
                    current_position.trailing_stop_distance = dist if dist > 0 else None
                    if dist > 0:
                        current_position.trailing_stop_pct = None

            if side == OrderSide.BUY:
                if current_position and current_position.side == PositionSide.LONG:
                    total_qty = current_position.quantity + qty
                    if total_qty > 0:
                        current_position.entry_price = (
                            (current_position.entry_price * current_position.quantity)
                            + (fill_price * qty)
                        ) / total_qty
                    current_position.quantity = total_qty
                    current_position.margin = (
                        current_position.entry_price * current_position.quantity
                    ) / max(1e-9, float(current_position.leverage or leverage or 1.0))
                    _merge_protection_settings()
                    current_position.update_price(fill_price)
                else:
                    position_manager.open_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        side=position_side,
                        entry_price=fill_price,
                        quantity=qty,
                        leverage=leverage,
                        strategy=signal.strategy_name,
                        account_id=account_id,
                        stop_loss=req.stop_loss,
                        take_profit=req.take_profit,
                        trailing_stop_pct=req.trailing_stop_pct,
                        trailing_stop_distance=req.trailing_stop_distance,
                        metadata={"source": "strategy"},
                    )
            else:
                if current_position and current_position.side == PositionSide.SHORT:
                    total_qty = current_position.quantity + qty
                    if total_qty > 0:
                        current_position.entry_price = (
                            (current_position.entry_price * current_position.quantity)
                            + (fill_price * qty)
                        ) / total_qty
                    current_position.quantity = total_qty
                    current_position.margin = (
                        current_position.entry_price * current_position.quantity
                    ) / max(1e-9, float(current_position.leverage or leverage or 1.0))
                    _merge_protection_settings()
                    current_position.update_price(fill_price)
                else:
                    position_manager.open_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        side=position_side,
                        entry_price=fill_price,
                        quantity=qty,
                        leverage=leverage,
                        strategy=signal.strategy_name,
                        account_id=account_id,
                        stop_loss=req.stop_loss,
                        take_profit=req.take_profit,
                        trailing_stop_pct=req.trailing_stop_pct,
                        trailing_stop_distance=req.trailing_stop_distance,
                        metadata={"source": "strategy"},
                    )

            trade_pnl -= fee_usd
            risk_manager.record_trade(
                {
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "strategy": signal.strategy_name,
                    "side": side.value,
                    "notional": float(qty * fill_price),
                    "pnl": trade_pnl,
                    "fee_usd": fee_usd,
                    "slippage_cost_usd": slippage_cost_usd,
                }
            )

            result = {
                "signal": signal.to_dict(),
                "order": {
                    "id": order.id,
                    "status": order.status.value,
                    "price": order.price,
                    "amount": order.amount,
                    "filled": order.filled,
                    "fee_usd": fee_usd,
                    "slippage_cost_usd": slippage_cost_usd,
                },
                "timestamp": datetime.now().isoformat(),
            }
            self._signal_diagnostics["executed"] = int(self._signal_diagnostics.get("executed", 0)) + 1
            self._signal_diagnostics["last_result"] = {
                "status": "executed",
                "strategy": signal.strategy_name,
                "symbol": signal.symbol,
                "exchange": exchange,
                "order_id": order.id,
                "amount": float(order.amount or 0.0),
                "filled": float(order.filled or 0.0),
                "price": float(order.price or 0.0),
            }
            self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
            await self._notify_callbacks("order_executed", result)
            return result
        except Exception as e:
            self._signal_diagnostics["exceptions"] = int(self._signal_diagnostics.get("exceptions", 0)) + 1
            self._signal_diagnostics["last_result"] = {
                "status": "exception",
                "strategy": signal.strategy_name,
                "symbol": signal.symbol,
                "reason": str(e),
            }
            self._signal_diagnostics["last_updated_at"] = datetime.utcnow().isoformat()
            logger.error(f"Failed to execute signal: {e}")
            return None

    async def _close_position(self, signal: Signal, position_side: PositionSide) -> Optional[Dict[str, Any]]:
        account_id = str(signal.metadata.get("account_id", "main"))
        exchange = account_manager.resolve_exchange(account_id, str(signal.metadata.get("exchange", "binance")))
        position = position_manager.get_position(exchange, signal.symbol, account_id=account_id)
        if not position or position.side != position_side:
            return None

        close_qty = float(position.quantity or 0.0)
        if close_qty <= 0:
            return None

        close_side = OrderSide.SELL if position_side == PositionSide.LONG else OrderSide.BUY
        quote_price, order_value = await self._resolve_order_context(
            exchange=exchange,
            symbol=signal.symbol,
            quantity=close_qty,
            preferred_price=signal.price,
        )

        account_equity = await self._get_account_equity()
        if not risk_manager.pre_trade_check(
            symbol=signal.symbol,
            side=close_side.value,
            strategy_name=signal.strategy_name,
            account_equity=account_equity,
            order_value=order_value,
            leverage=float(position.leverage or 1.0),
            strategy_allocation=strategy_manager.get_strategy_allocation(signal.strategy_name),
            allow_close=True,
        ):
            reason = self._build_reject_reason()
            request = OrderRequest(
                symbol=signal.symbol,
                side=close_side,
                order_type=OrderType.MARKET,
                amount=close_qty,
                price=quote_price if quote_price > 0 else None,
                exchange=exchange,
                strategy=signal.strategy_name,
                account_id=account_id,
                reduce_only=True,
                params={"close_reason": signal.signal_type.value, "leverage": float(position.leverage or 1.0)},
            )
            await order_manager.record_rejected_order(
                request=request,
                reason=reason or "平仓被风控拒绝",
                price=quote_price if quote_price > 0 else signal.price,
            )
            return None

        close_request = OrderRequest(
            symbol=signal.symbol,
            side=close_side,
            order_type=OrderType.MARKET,
            amount=close_qty,
            price=signal.price if float(signal.price or 0.0) > 0 else None,
            exchange=exchange,
            strategy=signal.strategy_name,
            account_id=account_id,
            reduce_only=True,
            params={"close_reason": signal.signal_type.value, "leverage": float(position.leverage or 1.0)},
        )
        close_order = await order_manager.create_order(close_request)
        if not close_order:
            await order_manager.record_rejected_order(
                request=close_request,
                reason="平仓下单失败",
                price=quote_price if quote_price > 0 else signal.price,
            )
            return None

        paper_cost = self._consume_paper_order_cost(close_order.id)
        fee_usd = float(paper_cost.get("fee_usd", 0.0) or 0.0)
        slippage_cost_usd = float(paper_cost.get("slippage_cost_usd", 0.0) or 0.0)
        close_price = float(close_order.price or signal.price or quote_price or 0.0)
        closed = position_manager.close_position(
            exchange=exchange,
            symbol=signal.symbol,
            close_price=close_price,
            quantity=close_qty,
            account_id=account_id,
        )
        if not closed:
            return None

        risk_manager.record_trade(
            {
                "symbol": signal.symbol,
                "exchange": exchange,
                "strategy": signal.strategy_name,
                "side": signal.signal_type.value,
                "pnl": float(closed.realized_pnl or 0.0) - fee_usd,
                "notional": float(close_price * close_qty),
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
            }
        )

        result = {
            "action": "close_position",
            "symbol": signal.symbol,
            "side": position_side.value,
            "close_price": close_price,
            "pnl": float(closed.realized_pnl or 0.0) - fee_usd,
            "fee_usd": fee_usd,
            "slippage_cost_usd": slippage_cost_usd,
            "account_id": account_id,
            "order": {
                "id": close_order.id,
                "status": close_order.status.value,
                "price": close_order.price,
                "amount": close_order.amount,
                "filled": close_order.filled,
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
            },
        }
        await self._notify_callbacks("order_executed", result)
        return result

    def _new_conditional_id(self) -> str:
        self._conditional_seq += 1
        return f"cond_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{self._conditional_seq:04d}"
    async def _queue_conditional_order(
        self,
        exchange: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float],
        leverage: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        trailing_stop_pct: Optional[float],
        trailing_stop_distance: Optional[float],
        trigger_price: float,
        account_id: str,
        strategy: str,
        reduce_only: bool,
    ) -> Dict[str, Any]:
        cid = self._new_conditional_id()
        self._conditional_orders[cid] = ConditionalManualOrder(
            conditional_id=cid,
            created_at=datetime.utcnow().isoformat(),
            exchange=exchange,
            symbol=symbol,
            side=side,
            order_type=order_type,
            amount=float(amount),
            price=float(price) if price is not None else None,
            leverage=float(leverage),
            stop_loss=float(stop_loss) if stop_loss is not None else None,
            take_profit=float(take_profit) if take_profit is not None else None,
            trailing_stop_pct=float(trailing_stop_pct) if trailing_stop_pct is not None else None,
            trailing_stop_distance=float(trailing_stop_distance) if trailing_stop_distance is not None else None,
            trigger_price=float(trigger_price),
            account_id=account_id,
            strategy=strategy,
            reduce_only=bool(reduce_only),
        )
        payload = {
            "conditional_id": cid,
            "status": "queued",
            "exchange": exchange,
            "symbol": symbol,
            "side": side,
            "trigger_price": float(trigger_price),
            "amount": float(amount),
            "account_id": account_id,
        }
        await self._notify_callbacks("conditional_queued", payload)
        return payload

    async def _execute_manual_order_single(
        self,
        exchange: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float],
        leverage: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        trailing_stop_pct: Optional[float],
        trailing_stop_distance: Optional[float],
        trigger_price: Optional[float],
        order_mode: str,
        iceberg_parts: int,
        algo_slices: int,
        algo_interval_sec: int,
        account_id: str,
        reduce_only: bool,
        strategy: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        side_lower = str(side).lower()
        is_sell = side_lower == "sell"
        raw_amount = float(amount or 0.0)
        if raw_amount <= 0:
            return None

        if not account_manager.is_enabled(account_id):
            return None

        existing_position = position_manager.get_position(exchange, symbol, account_id=account_id)
        closes_existing = bool(
            (is_sell and existing_position and existing_position.side == PositionSide.LONG)
            or ((not is_sell) and existing_position and existing_position.side == PositionSide.SHORT)
        )

        if reduce_only and not closes_existing:
            return None

        exec_amount = raw_amount
        if reduce_only and existing_position and closes_existing:
            exec_amount = min(exec_amount, float(existing_position.quantity or 0.0))
            if exec_amount <= 0:
                return None

        quote_price, order_value = await self._resolve_order_context(exchange, symbol, exec_amount, price)
        account_equity = await self._get_account_equity()

        if not risk_manager.pre_trade_check(
            symbol=symbol,
            side=side_lower,
            strategy_name=strategy,
            account_equity=account_equity,
            order_value=order_value,
            leverage=leverage,
            strategy_allocation=1.0,
            allow_close=closes_existing,
        ):
            return None

        request = OrderRequest(
            symbol=symbol,
            side=OrderSide.BUY if side_lower == "buy" else OrderSide.SELL,
            order_type=OrderType.MARKET if str(order_type).lower() == "market" else OrderType.LIMIT,
            amount=exec_amount,
            price=price,
            exchange=exchange,
            strategy=strategy,
            account_id=account_id,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pct=trailing_stop_pct,
            trailing_stop_distance=trailing_stop_distance,
            trigger_price=trigger_price,
            order_mode=str(order_mode or "normal").lower(),
            iceberg_parts=max(1, int(iceberg_parts or 1)),
            algo_slices=max(1, int(algo_slices or 1)),
            algo_interval_sec=max(0, int(algo_interval_sec or 0)),
            reduce_only=reduce_only,
            params=dict(params or {}, leverage=float(leverage)),
        )
        order = await order_manager.create_order(request)
        if not order:
            return None

        paper_cost = self._consume_paper_order_cost(order.id)
        fee_usd = float(paper_cost.get("fee_usd", 0.0) or 0.0)
        slippage_cost_usd = float(paper_cost.get("slippage_cost_usd", 0.0) or 0.0)
        fill_price = float(order.price or price or quote_price or 0.0)
        trade_pnl = 0.0

        def _merge_protection_settings() -> None:
            if not existing_position:
                return
            if stop_loss is not None:
                existing_position.stop_loss = float(stop_loss)
            if take_profit is not None:
                existing_position.take_profit = float(take_profit)
            if trailing_stop_pct is not None:
                pct = max(0.0, float(trailing_stop_pct))
                existing_position.trailing_stop_pct = pct if pct > 0 else None
                if pct > 0:
                    existing_position.trailing_stop_distance = None
            elif trailing_stop_distance is not None:
                dist = max(0.0, float(trailing_stop_distance))
                existing_position.trailing_stop_distance = dist if dist > 0 else None
                if dist > 0:
                    existing_position.trailing_stop_pct = None

        if side_lower == "buy":
            if existing_position and existing_position.side == PositionSide.LONG:
                total_qty = existing_position.quantity + exec_amount
                if total_qty > 0:
                    existing_position.entry_price = (
                        (existing_position.entry_price * existing_position.quantity)
                        + (fill_price * exec_amount)
                    ) / total_qty
                existing_position.quantity = total_qty
                existing_position.margin = (
                    existing_position.entry_price * existing_position.quantity
                ) / max(1e-9, float(existing_position.leverage or leverage or 1.0))
                _merge_protection_settings()
                existing_position.update_price(fill_price)
            elif existing_position and existing_position.side == PositionSide.SHORT:
                close_qty = min(exec_amount, float(existing_position.quantity or 0.0))
                prev_realized = float(existing_position.realized_pnl or 0.0)
                closed = position_manager.close_position(
                    exchange=exchange,
                    symbol=symbol,
                    close_price=fill_price,
                    quantity=close_qty,
                    account_id=account_id,
                )
                if closed:
                    trade_pnl += float(closed.realized_pnl or 0.0) - prev_realized
                remaining = max(0.0, exec_amount - close_qty)
                if remaining > 0 and not reduce_only:
                    position_manager.open_position(
                        exchange=exchange,
                        symbol=symbol,
                        side=PositionSide.LONG,
                        entry_price=fill_price,
                        quantity=remaining,
                        leverage=leverage,
                        strategy=strategy,
                        account_id=account_id,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        trailing_stop_pct=trailing_stop_pct,
                        trailing_stop_distance=trailing_stop_distance,
                        metadata={"source": "manual", "reduce_only": reduce_only},
                    )
            else:
                position_manager.open_position(
                    exchange=exchange,
                    symbol=symbol,
                    side=PositionSide.LONG,
                    entry_price=fill_price,
                    quantity=exec_amount,
                    leverage=leverage,
                    strategy=strategy,
                    account_id=account_id,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop_pct=trailing_stop_pct,
                    trailing_stop_distance=trailing_stop_distance,
                    metadata={"source": "manual", "reduce_only": reduce_only},
                )
        else:
            if existing_position and existing_position.side == PositionSide.LONG:
                close_qty = min(exec_amount, float(existing_position.quantity or 0.0))
                prev_realized = float(existing_position.realized_pnl or 0.0)
                closed = position_manager.close_position(
                    exchange=exchange,
                    symbol=symbol,
                    close_price=fill_price,
                    quantity=close_qty,
                    account_id=account_id,
                )
                if closed:
                    trade_pnl += float(closed.realized_pnl or 0.0) - prev_realized
                remaining = max(0.0, exec_amount - close_qty)
                if remaining > 0 and not reduce_only:
                    position_manager.open_position(
                        exchange=exchange,
                        symbol=symbol,
                        side=PositionSide.SHORT,
                        entry_price=fill_price,
                        quantity=remaining,
                        leverage=leverage,
                        strategy=strategy,
                        account_id=account_id,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        trailing_stop_pct=trailing_stop_pct,
                        trailing_stop_distance=trailing_stop_distance,
                        metadata={"source": "manual", "reduce_only": reduce_only},
                    )
            elif existing_position and existing_position.side == PositionSide.SHORT:
                total_qty = existing_position.quantity + exec_amount
                if total_qty > 0:
                    existing_position.entry_price = (
                        (existing_position.entry_price * existing_position.quantity)
                        + (fill_price * exec_amount)
                    ) / total_qty
                existing_position.quantity = total_qty
                existing_position.margin = (
                    existing_position.entry_price * existing_position.quantity
                ) / max(1e-9, float(existing_position.leverage or leverage or 1.0))
                _merge_protection_settings()
                existing_position.update_price(fill_price)
            else:
                position_manager.open_position(
                    exchange=exchange,
                    symbol=symbol,
                    side=PositionSide.SHORT,
                    entry_price=fill_price,
                    quantity=exec_amount,
                    leverage=leverage,
                    strategy=strategy,
                    account_id=account_id,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop_pct=trailing_stop_pct,
                    trailing_stop_distance=trailing_stop_distance,
                    metadata={"source": "manual", "reduce_only": reduce_only},
                )

        trade_pnl -= fee_usd
        risk_manager.record_trade(
            {
                "symbol": symbol,
                "exchange": exchange,
                "strategy": strategy,
                "side": side_lower,
                "notional": float(exec_amount * fill_price),
                "pnl": trade_pnl,
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
            }
        )

        result = {
            "order_id": order.id,
            "status": order.status.value,
            "price": fill_price,
            "amount": order.amount,
            "filled": order.filled,
            "exchange": exchange,
            "symbol": symbol,
            "side": side_lower,
            "account_id": account_id,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop_pct": trailing_stop_pct,
            "trailing_stop_distance": trailing_stop_distance,
            "trigger_price": trigger_price,
            "order_mode": request.order_mode,
            "reduce_only": reduce_only,
            "fee_usd": fee_usd,
            "slippage_cost_usd": slippage_cost_usd,
        }
        await self._notify_callbacks("manual_order_executed", result)
        return result

    async def execute_manual_order(
        self,
        exchange: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float] = None,
        leverage: float = 1.0,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        trailing_stop_distance: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_mode: str = "normal",
        iceberg_parts: int = 1,
        algo_slices: int = 1,
        algo_interval_sec: int = 0,
        account_id: str = "main",
        reduce_only: bool = False,
        strategy: str = "manual",
    ) -> Optional[Dict[str, Any]]:
        mode = str(order_mode or "normal").lower()
        if mode not in {"normal", "conditional", "iceberg", "twap", "vwap"}:
            mode = "normal"
        if str(side).lower() not in {"buy", "sell"}:
            return None
        if str(order_type).lower() not in {"market", "limit"}:
            return None
        if float(amount or 0.0) <= 0:
            return None
        if float(leverage or 0.0) <= 0:
            return None
        account_id = str(account_id or "main")
        exchange = str(exchange or "").strip().lower()
        if not exchange:
            exchange = account_manager.resolve_exchange(account_id, "binance")

        if mode == "conditional":
            if trigger_price is None or trigger_price <= 0:
                return None
            last_price = await self._resolve_price(exchange, symbol, price)
            trigger_hit = (
                (str(side).lower() == "buy" and last_price >= float(trigger_price))
                or (str(side).lower() == "sell" and last_price <= float(trigger_price))
            )
            if not trigger_hit:
                return await self._queue_conditional_order(
                    exchange=exchange,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    amount=amount,
                    price=price,
                    leverage=leverage,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop_pct=trailing_stop_pct,
                    trailing_stop_distance=trailing_stop_distance,
                    trigger_price=float(trigger_price),
                    account_id=account_id,
                    strategy=strategy,
                    reduce_only=reduce_only,
                )

        if mode in {"iceberg", "twap", "vwap"}:
            parts = int(iceberg_parts if mode == "iceberg" else algo_slices)
            parts = max(1, min(parts, 50))
            if mode == "vwap":
                base = [float(i + 1) for i in range(parts)]
                s = sum(base) or 1.0
                chunks = [float(amount) * (x / s) for x in base]
            else:
                chunk = float(amount) / float(parts)
                chunks = [chunk] * parts

            child = []
            for idx, chunk_amount in enumerate(chunks):
                if chunk_amount <= 0:
                    continue
                item = await self._execute_manual_order_single(
                    exchange=exchange,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    amount=chunk_amount,
                    price=price,
                    leverage=leverage,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop_pct=trailing_stop_pct,
                    trailing_stop_distance=trailing_stop_distance,
                    trigger_price=trigger_price,
                    order_mode=mode,
                    iceberg_parts=int(iceberg_parts or 1),
                    algo_slices=int(algo_slices or 1),
                    algo_interval_sec=int(algo_interval_sec or 0),
                    account_id=account_id,
                    reduce_only=reduce_only,
                    strategy=strategy,
                    params={
                        "algo_mode": mode,
                        "algo_part": idx + 1,
                        "algo_parts": parts,
                    },
                )
                if item:
                    child.append(item)
                if mode == "twap" and idx < len(chunks) - 1 and int(algo_interval_sec or 0) > 0:
                    await asyncio.sleep(max(0, int(algo_interval_sec)))

            if not child:
                return None

            filled = sum(float(x.get("filled") or x.get("amount") or 0.0) for x in child)
            notional = sum(float(x.get("filled") or x.get("amount") or 0.0) * float(x.get("price") or 0.0) for x in child)
            avg_price = (notional / filled) if filled > 0 else 0.0
            merged = {
                "order_id": f"{mode}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
                "status": "closed",
                "price": avg_price,
                "amount": float(amount),
                "filled": filled,
                "exchange": exchange,
                "symbol": symbol,
                "side": str(side).lower(),
                "account_id": account_id,
                "order_mode": mode,
                "child_orders": [x.get("order_id") for x in child],
            }
            await self._notify_callbacks("algo_order_executed", merged)
            return merged

        return await self._execute_manual_order_single(
            exchange=exchange,
            symbol=symbol,
            side=side,
            order_type=order_type,
            amount=amount,
            price=price,
            leverage=leverage,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pct=trailing_stop_pct,
            trailing_stop_distance=trailing_stop_distance,
            trigger_price=trigger_price,
            order_mode=mode,
            iceberg_parts=int(iceberg_parts or 1),
            algo_slices=int(algo_slices or 1),
            algo_interval_sec=int(algo_interval_sec or 0),
            account_id=account_id,
            reduce_only=reduce_only,
            strategy=strategy,
            params={
                "trigger_price": trigger_price,
                "order_mode": mode,
                "iceberg_parts": int(iceberg_parts or 1),
                "algo_slices": int(algo_slices or 1),
                "algo_interval_sec": int(algo_interval_sec or 0),
            },
        )
    async def _execute_protective_close(self, exchange: str, symbol: str, account_id: str, price: float, reason: str) -> None:
        pos = position_manager.get_position(exchange, symbol, account_id=account_id)
        if not pos:
            return
        close_side = "sell" if pos.side == PositionSide.LONG else "buy"
        result = await self._execute_manual_order_single(
            exchange=exchange,
            symbol=symbol,
            side=close_side,
            order_type="market",
            amount=pos.quantity,
            price=price,
            leverage=pos.leverage,
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id=account_id,
            reduce_only=True,
            strategy=pos.strategy or "risk",
            params={"close_reason": reason},
        )
        if result:
            await self._notify_callbacks(
                "protective_close",
                {
                    "reason": reason,
                    "symbol": symbol,
                    "exchange": exchange,
                    "account_id": account_id,
                    "close_price": price,
                    "order_id": result.get("order_id"),
                },
            )

    async def _check_protective_orders(self) -> None:
        positions = position_manager.get_all_positions()
        if not positions:
            return

        prices: Dict[Tuple[str, str], float] = {}
        for pos in positions:
            key = (pos.exchange, pos.symbol)
            if key not in prices:
                px = await self._resolve_price(pos.exchange, pos.symbol)
                if px > 0:
                    prices[key] = px

        for pos in positions:
            px = prices.get((pos.exchange, pos.symbol))
            if not px:
                continue
            position_manager.update_position_price(
                exchange=pos.exchange,
                symbol=pos.symbol,
                current_price=px,
                account_id=pos.account_id,
            )

            reason = None
            if pos.side == PositionSide.LONG:
                if pos.stop_loss is not None and px <= float(pos.stop_loss):
                    reason = "stop_loss"
                elif pos.trailing_stop_price is not None and px <= float(pos.trailing_stop_price):
                    reason = "trailing_stop"
                elif pos.take_profit is not None and px >= float(pos.take_profit):
                    reason = "take_profit"
            else:
                if pos.stop_loss is not None and px >= float(pos.stop_loss):
                    reason = "stop_loss"
                elif pos.trailing_stop_price is not None and px >= float(pos.trailing_stop_price):
                    reason = "trailing_stop"
                elif pos.take_profit is not None and px <= float(pos.take_profit):
                    reason = "take_profit"

            if reason:
                await self._execute_protective_close(pos.exchange, pos.symbol, pos.account_id, px, reason)

    async def _check_conditional_orders(self) -> None:
        if not self._conditional_orders:
            return

        for cid in list(self._conditional_orders.keys()):
            cond = self._conditional_orders.get(cid)
            if not cond:
                continue
            current = await self._resolve_price(cond.exchange, cond.symbol, cond.price)
            if current <= 0:
                continue
            hit = (cond.side == "buy" and current >= cond.trigger_price) or (cond.side == "sell" and current <= cond.trigger_price)
            if not hit:
                continue

            result = await self._execute_manual_order_single(
                exchange=cond.exchange,
                symbol=cond.symbol,
                side=cond.side,
                order_type=cond.order_type,
                amount=cond.amount,
                price=cond.price,
                leverage=cond.leverage,
                stop_loss=cond.stop_loss,
                take_profit=cond.take_profit,
                trailing_stop_pct=cond.trailing_stop_pct,
                trailing_stop_distance=cond.trailing_stop_distance,
                trigger_price=cond.trigger_price,
                order_mode="conditional",
                iceberg_parts=1,
                algo_slices=1,
                algo_interval_sec=0,
                account_id=cond.account_id,
                reduce_only=cond.reduce_only,
                strategy=cond.strategy,
                params={"conditional_id": cid, "trigger_price": cond.trigger_price},
            )
            if result:
                await self._notify_callbacks(
                    "conditional_triggered",
                    {
                        "conditional_id": cid,
                        "order_id": result.get("order_id"),
                        "symbol": cond.symbol,
                        "exchange": cond.exchange,
                        "trigger_price": cond.trigger_price,
                        "triggered_price": current,
                    },
                )
                self._conditional_orders.pop(cid, None)

    async def _background_tick(self) -> None:
        now = datetime.utcnow()
        if self._last_bg_check_at and (now - self._last_bg_check_at).total_seconds() < self._bg_check_interval_seconds:
            return
        self._last_bg_check_at = now
        await self._check_conditional_orders()
        await self._check_protective_orders()

    async def _process_signal_queue(self) -> None:
        while self._running:
            try:
                signal = await asyncio.wait_for(self._signal_queue.get(), timeout=1.0)
                await self.execute_signal(signal)
            except asyncio.TimeoutError:
                await self._background_tick()
            except Exception as e:
                logger.error(f"Signal processing error: {e}")

    async def _prime_live_equity(self) -> None:
        if self._paper_trading:
            return
        try:
            await asyncio.wait_for(self._refresh_equity(), timeout=30.0)
            logger.info(f"Live equity primed: {float(self._cached_equity or 0.0):.4f}")
        except Exception as e:
            logger.warning(f"Live equity prime skipped: {e}")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._paper_trading = settings.TRADING_MODE == "paper"
        order_manager.set_paper_trading(self._paper_trading)
        risk_manager.set_account_scope("paper" if self._paper_trading else "live", reset_baseline=False)
        await self._ensure_queue_worker()
        if not self._paper_trading:
            asyncio.create_task(self._prime_live_equity(), name="execution_prime_live_equity")
        logger.info(f"Execution engine started (paper trading: {self._paper_trading})")

    async def stop(self) -> None:
        self._running = False
        self._conditional_orders.clear()
        if self._queue_task and not self._queue_task.done():
            self._queue_task.cancel()
            try:
                await self._queue_task
            except asyncio.CancelledError:
                pass
        self._queue_task = None
        for exchange in exchange_manager.get_connected_exchanges():
            await order_manager.cancel_all_orders(exchange=exchange)
        logger.info("Execution engine stopped")

    def list_conditional_orders(self) -> List[Dict[str, Any]]:
        return [o.to_dict() for o in self._conditional_orders.values()]

    def cancel_conditional_order(self, conditional_id: str) -> bool:
        if conditional_id in self._conditional_orders:
            del self._conditional_orders[conditional_id]
            return True
        return False

    def clear_paper_runtime(self) -> Dict[str, int]:
        """Clear pending conditional/signal runtime state for paper reset."""
        conditional_count = len(self._conditional_orders)
        fee_count = len(self._paper_fee_applied_orders)
        fee_total = float(self._paper_total_fees_usd or 0.0)
        self._conditional_orders.clear()
        queue_cleared = 0
        while not self._signal_queue.empty():
            try:
                self._signal_queue.get_nowait()
                queue_cleared += 1
            except Exception:
                break
        self._last_bg_check_at = None
        self._paper_total_fees_usd = 0.0
        self._paper_fee_applied_orders.clear()
        if self._paper_trading:
            report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
            if report_eq > 0:
                self._paper_equity_anchor = max(
                    report_eq,
                    float(getattr(settings, "PAPER_INITIAL_EQUITY", 10000.0) or 10000.0),
                )
        return {
            "conditional_orders_cleared": conditional_count,
            "queued_signals_cleared": queue_cleared,
            "paper_fee_orders_cleared": fee_count,
            "paper_fee_total_cleared": round(fee_total, 8),
        }

    @property
    def is_running(self) -> bool:
        return self._running

    def get_queue_size(self) -> int:
        return self._signal_queue.qsize()

    def get_signal_diagnostics(self) -> Dict[str, Any]:
        return dict(self._signal_diagnostics or {})


execution_engine = ExecutionEngine()
