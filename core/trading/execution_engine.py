
"""Trading execution engine."""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config.settings import settings
from core.ai.live_decision_router import live_decision_router
from core.audit import audit_logger
from core.exchanges import exchange_manager
from core.governance.audit import GovernanceAuditEvent, write_audit
from core.governance.decision_engine import decision_engine
from core.risk.risk_manager import risk_manager
from core.runtime import runtime_state
from core.strategies import Signal, SignalType
from core.strategies.strategy_manager import strategy_manager
from core.trading.account_manager import account_manager
from core.trading.order_manager import OrderRequest, OrderSide, OrderType, order_manager
from core.trading.position_manager import PositionSide, position_manager
from core.utils.asset_valuation import STABLE_COINS, build_currency_usd_quotes

_AUTONOMOUS_PROFIT_MANAGEMENT_DEFAULTS = {
    "profit_protect_enabled": True,
    "profit_protect_trigger_pct": 0.0035,
    "profit_protect_lock_pct": 0.0012,
    "partial_take_profit_enabled": True,
    "partial_take_profit_trigger_pct": 0.0060,
    "partial_take_profit_fraction": 0.5,
    "post_partial_trailing_stop_pct": 0.0025,
    "outage_protection_enabled": True,
    "outage_tight_trailing_stop_pct": 0.0015,
}
_PROFIT_MANAGEMENT_STATE_KEYS = (
    "profit_protect_armed",
    "profit_protect_armed_at",
    "partial_take_profit_done",
    "partial_take_profit_done_at",
    "partial_take_profit_order_id",
    "partial_take_profit_price",
    "partial_take_profit_skip_reason",
    "outage_protection_armed",
    "outage_protection_armed_at",
    "outage_protection_last_applied_at",
    "outage_protection_reason",
    "profit_management_last_event",
)


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
        self._signal_queue: Optional[asyncio.Queue] = None
        self._signal_queue_loop: Optional[asyncio.AbstractEventLoop] = None
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
            "ai_rejected": 0,
            "ai_reduce_only_rejected": 0,
            "ai_review_bypassed": 0,
            "order_failed": 0,
            "order_timeout": 0,
            "exceptions": 0,
            "last_signal": None,
            "last_result": None,
            "last_ai_review_result": None,
            "last_updated_at": None,
        }

        self._conditional_orders: Dict[str, ConditionalManualOrder] = {}
        self._conditional_seq = 0
        self._last_bg_check_at: Optional[datetime] = None
        self._bg_check_interval_seconds = 2.0
        self._last_live_reconcile_at: Optional[datetime] = None
        self._live_reconcile_interval_seconds = 12.0
        self._live_reconcile_grace_seconds = 20.0
        self._live_reconcile_absence_counts: Dict[Tuple[str, str, str, str], int] = {}
        self._live_reconcile_absence_threshold = 3
        self._live_reconcile_absence_min_age_seconds = 10.0 * 60.0
        self._real_order_timeout_seconds = 30.0
        self._live_review_root = Path("./data/cache/live_review")
        self._live_trade_journal_path = self._live_review_root / "strategy_trade_journal.jsonl"
        self._live_trade_counts_path = self._live_review_root / "strategy_trade_counts.json"
        self._live_strategy_trade_counts: Dict[str, int] = self._load_live_trade_counts()
        self._live_review_lock = asyncio.Lock()

    def _load_live_trade_counts(self) -> Dict[str, int]:
        try:
            if not self._live_trade_counts_path.exists():
                return {}
            raw = json.loads(self._live_trade_counts_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return {}
            counts: Dict[str, int] = {}
            for key, value in raw.items():
                name = str(key or "").strip()
                if not name:
                    continue
                try:
                    counts[name] = max(0, int(value))
                except Exception:
                    continue
            return counts
        except Exception as exc:
            logger.debug(f"load live trade counts failed: {exc}")
            return {}

    def _persist_live_trade_counts(self) -> None:
        try:
            self._live_review_root.mkdir(parents=True, exist_ok=True)
            tmp = self._live_trade_counts_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(self._live_strategy_trade_counts, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self._live_trade_counts_path)
        except Exception as exc:
            logger.warning(f"persist live trade counts failed: {exc}")

    @staticmethod
    def _signal_to_dict_safe(signal: Signal) -> Dict[str, Any]:
        try:
            payload = signal.to_dict()
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {
            "symbol": str(getattr(signal, "symbol", "") or ""),
            "signal_type": str(getattr(getattr(signal, "signal_type", None), "value", "") or ""),
            "price": float(getattr(signal, "price", 0.0) or 0.0),
            "timestamp": (
                getattr(signal, "timestamp", None).isoformat()
                if getattr(signal, "timestamp", None) is not None
                else datetime.now(timezone.utc).isoformat()
            ),
            "strategy_name": str(getattr(signal, "strategy_name", "") or ""),
            "strength": float(getattr(signal, "strength", 0.0) or 0.0),
            "quantity": getattr(signal, "quantity", None),
            "stop_loss": getattr(signal, "stop_loss", None),
            "take_profit": getattr(signal, "take_profit", None),
            "metadata": dict(getattr(signal, "metadata", {}) or {}),
        }

    async def _record_live_strategy_trade(
        self,
        *,
        signal: Signal,
        exchange: str,
        account_id: str,
        side: str,
        quantity: float,
        fill_price: float,
        order_id: Optional[str],
        order_status: Optional[str],
        pnl: float,
        fee_usd: float,
        slippage_cost_usd: float,
        action: str,
        gross_pnl_usd: Optional[float] = None,
        net_pnl_usd: Optional[float] = None,
    ) -> None:
        if self._paper_trading:
            return

        strategy = str(getattr(signal, "strategy_name", "") or "").strip() or "unknown"
        symbol = str(getattr(signal, "symbol", "") or "").strip()
        ts = datetime.now(timezone.utc)
        signal_payload = self._signal_to_dict_safe(signal)
        signal_type = str(signal_payload.get("signal_type") or side or "").strip().lower()
        resolved_fee_usd = self._safe_nonnegative_float(fee_usd, 0.0)
        resolved_slippage_cost_usd = self._safe_nonnegative_float(slippage_cost_usd, 0.0)
        resolved_gross_pnl_usd = self._safe_float(
            gross_pnl_usd,
            self._safe_float(pnl, 0.0) + resolved_fee_usd,
        )
        resolved_net_pnl_usd = self._safe_float(
            net_pnl_usd,
            self._safe_float(pnl, 0.0),
        )
        resolved_cost_usd = resolved_fee_usd + resolved_slippage_cost_usd

        entry: Dict[str, Any]
        async with self._live_review_lock:
            strategy_count = int(self._live_strategy_trade_counts.get(strategy, 0)) + 1
            self._live_strategy_trade_counts[strategy] = strategy_count
            self._persist_live_trade_counts()
            self._live_review_root.mkdir(parents=True, exist_ok=True)
            entry = {
                "timestamp": ts.isoformat(),
                "mode": "live",
                "action": str(action or "trade"),
                "strategy": strategy,
                "strategy_trade_count": strategy_count,
                "exchange": str(exchange or "").strip().lower(),
                "account_id": str(account_id or "main"),
                "symbol": symbol,
                "side": str(side or "").lower(),
                "signal_type": signal_type,
                "quantity": float(quantity or 0.0),
                "fill_price": float(fill_price or 0.0),
                "notional": float(quantity or 0.0) * float(fill_price or 0.0),
                "order_id": str(order_id or ""),
                "order_status": str(order_status or ""),
                "pnl": float(resolved_net_pnl_usd),
                "gross_pnl_usd": float(resolved_gross_pnl_usd),
                "fee_usd": float(resolved_fee_usd),
                "slippage_cost_usd": float(resolved_slippage_cost_usd),
                "cost_usd": float(resolved_cost_usd),
                "signal": signal_payload,
            }
            if net_pnl_usd is not None:
                entry["net_pnl_usd"] = float(resolved_net_pnl_usd)
            with self._live_trade_journal_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

        try:
            await audit_logger.log(
                module="trading.live_review",
                action="strategy_trade",
                status="success",
                message=f"{strategy} {symbol} {signal_type}",
                details=entry,
            )
        except Exception as exc:
            logger.debug(f"record live strategy audit skipped: {exc}")

    def get_live_trade_review(
        self,
        *,
        limit: int = 200,
        strategy: Optional[str] = None,
        hours: int = 24 * 7,
    ) -> Dict[str, Any]:
        size = max(1, min(int(limit or 200), 2000))
        lookback_hours = max(1, min(int(hours or 24 * 7), 24 * 365))
        strategy_filter = str(strategy or "").strip()
        cutoff = datetime.now(timezone.utc).timestamp() - lookback_hours * 3600

        items: List[Dict[str, Any]] = []
        if self._live_trade_journal_path.exists():
            try:
                with self._live_trade_journal_path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        text = str(line or "").strip()
                        if not text:
                            continue
                        try:
                            row = json.loads(text)
                        except Exception:
                            continue
                        if not isinstance(row, dict):
                            continue
                        if strategy_filter and str(row.get("strategy") or "") != strategy_filter:
                            continue
                        ts_raw = str(row.get("timestamp") or "")
                        try:
                            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                            if ts.timestamp() < cutoff:
                                continue
                        except Exception:
                            continue
                        items.append(row)
            except Exception as exc:
                logger.debug(f"read live trade review failed: {exc}")
        items = items[-size:]

        strategy_counts = dict(self._live_strategy_trade_counts or {})
        if strategy_filter:
            strategy_counts = {strategy_filter: int(strategy_counts.get(strategy_filter, 0))}

        summary = self._build_live_trade_review_summary(items)
        return {
            "mode": "live",
            "hours": lookback_hours,
            "limit": size,
            "strategy": strategy_filter or None,
            "count": len(items),
            "strategy_trade_counts": strategy_counts,
            "summary": summary,
            "items": items,
        }

    def set_paper_trading(self, enabled: bool, *, sync_runtime_state: bool = True) -> None:
        self._paper_trading = bool(enabled)
        mode = "paper" if self._paper_trading else "live"
        order_manager.set_paper_trading(self._paper_trading)
        position_manager.set_scope(mode)
        risk_manager.set_account_scope(mode, reset_baseline=True)
        if sync_runtime_state:
            runtime_state.initialize_mode(mode, reason="execution_engine.set_paper_trading")
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

    def _ensure_signal_queue(self) -> asyncio.Queue:
        loop = asyncio.get_running_loop()
        if self._signal_queue is not None and self._signal_queue_loop is loop:
            return self._signal_queue

        dropped = 0
        if self._signal_queue is not None:
            with contextlib.suppress(Exception):
                dropped = int(self._signal_queue.qsize())
        if dropped > 0:
            logger.warning(
                "Execution signal queue rebound to current event loop; "
                f"dropped {dropped} pending signal(s)"
            )

        self._signal_queue = asyncio.Queue()
        self._signal_queue_loop = loop
        return self._signal_queue

    async def submit_signal(self, signal: Signal) -> bool:
        self._signal_diagnostics["submitted"] = int(self._signal_diagnostics.get("submitted", 0)) + 1
        self._signal_diagnostics["last_signal"] = {
            "strategy": signal.strategy_name,
            "symbol": signal.symbol,
            "signal_type": signal.signal_type.value,
            "price": float(signal.price or 0.0),
            "strength": float(signal.strength or 0.0),
            "timestamp": signal.timestamp.isoformat() if signal.timestamp else datetime.now(timezone.utc).isoformat(),
        }
        self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
        if self._running:
            await self._ensure_queue_worker()
        queue = self._ensure_signal_queue()
        if self._queue_task and not self._queue_task.done():
            await queue.put(signal)
            logger.debug(
                f"Signal queued: {signal.signal_type.value} {signal.symbol} "
                f"(queue_size={queue.qsize()})"
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
        queue = self._ensure_signal_queue()
        if self._queue_task and not self._queue_task.done():
            task_loop = getattr(self._queue_task, "_loop", None)
            if task_loop is self._signal_queue_loop:
                return
            with contextlib.suppress(Exception):
                self._queue_task.cancel()
            self._queue_task = None
            logger.warning("Execution signal queue worker rebound to current event loop")
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
            self._equity_updated_at = datetime.now(timezone.utc)
            risk_manager.update_equity(self._cached_equity)
            runtime_state.update_equity_snapshot(self._cached_equity, updated_at=self._equity_updated_at)

        return float(self._cached_equity or 0.0)

    async def _get_account_equity(self, force: bool = False) -> float:
        report_eq = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
        if self._paper_trading:
            return await self.get_account_equity_snapshot(force=force)

        now = datetime.now(timezone.utc)
        cached_eq = float(self._cached_equity or 0.0)
        if not force:
            if report_eq > 100:
                if report_eq > cached_eq:
                    self._cached_equity = report_eq
                    self._equity_updated_at = now
                    runtime_state.update_equity_snapshot(self._cached_equity, updated_at=self._equity_updated_at)
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
        self._equity_updated_at = datetime.now(timezone.utc)
        risk_manager.update_equity(self._cached_equity)
        runtime_state.update_equity_snapshot(self._cached_equity, updated_at=self._equity_updated_at)
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
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except Exception:
            return float(default)
        if not math.isfinite(out):
            return float(default)
        return out

    @staticmethod
    def _safe_ratio(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except Exception:
            return float(default)
        if not math.isfinite(out):
            return float(default)
        return max(0.0, min(out, 1.0))

    @classmethod
    def _live_trade_row_net_pnl_usd(cls, row: Dict[str, Any]) -> float:
        if row.get("net_pnl_usd") is not None:
            return cls._safe_float(row.get("net_pnl_usd"), 0.0)
        recorded = cls._safe_float(row.get("pnl"), 0.0)
        slippage_cost_usd = cls._safe_nonnegative_float(row.get("slippage_cost_usd"), 0.0)
        return recorded - slippage_cost_usd

    @classmethod
    def _live_trade_row_gross_pnl_usd(cls, row: Dict[str, Any]) -> float:
        if row.get("gross_pnl_usd") is not None:
            return cls._safe_float(row.get("gross_pnl_usd"), 0.0)
        if row.get("net_pnl_usd") is not None:
            return (
                cls._safe_float(row.get("net_pnl_usd"), 0.0)
                + cls._safe_nonnegative_float(row.get("fee_usd"), 0.0)
                + cls._safe_nonnegative_float(row.get("slippage_cost_usd"), 0.0)
            )
        return cls._safe_float(row.get("pnl"), 0.0) + cls._safe_nonnegative_float(row.get("fee_usd"), 0.0)

    @classmethod
    def _live_trade_row_position_side(cls, row: Dict[str, Any]) -> str:
        action = str(row.get("action") or "").strip().lower()
        side = str(row.get("side") or "").strip().lower()
        if action == "close":
            return "long" if side in {"sell", "short"} else "short"
        return "long" if side in {"buy", "long"} else "short"

    @classmethod
    def _build_live_trade_review_summary(cls, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {
            "trade_count": len(items),
            "entry_count": 0,
            "close_count": 0,
            "winning_close_count": 0,
            "losing_close_count": 0,
            "gross_pnl_usd": 0.0,
            "fee_usd": 0.0,
            "slippage_cost_usd": 0.0,
            "cost_usd": 0.0,
            "net_pnl_usd": 0.0,
            "win_rate": None,
            "profit_factor": None,
            "avg_holding_minutes": None,
            "latest_trade_at": None,
            "dominant_symbol": None,
        }
        if not items:
            return summary

        symbol_counter: Counter[str] = Counter()
        holding_minutes: List[float] = []
        open_stacks: Dict[Tuple[str, str], List[datetime]] = {}
        positive_close_pnl = 0.0
        negative_close_pnl = 0.0

        for row in items:
            symbol = str(row.get("symbol") or "").strip()
            action = str(row.get("action") or "").strip().lower()
            symbol_counter[symbol] += 1
            summary["latest_trade_at"] = row.get("timestamp") or summary["latest_trade_at"]

            fee_usd = cls._safe_nonnegative_float(row.get("fee_usd"), 0.0)
            slippage_cost_usd = cls._safe_nonnegative_float(row.get("slippage_cost_usd"), 0.0)
            gross_pnl_usd = cls._live_trade_row_gross_pnl_usd(row)
            net_pnl_usd = cls._live_trade_row_net_pnl_usd(row)
            summary["gross_pnl_usd"] += gross_pnl_usd
            summary["fee_usd"] += fee_usd
            summary["slippage_cost_usd"] += slippage_cost_usd
            summary["cost_usd"] += fee_usd + slippage_cost_usd
            summary["net_pnl_usd"] += net_pnl_usd

            position_side = cls._live_trade_row_position_side(row)
            stack_key = (str(symbol or "").strip().upper().replace("-", "/"), position_side)
            trade_dt: Optional[datetime] = None
            ts_raw = str(row.get("timestamp") or "").strip()
            if ts_raw:
                with contextlib.suppress(Exception):
                    trade_dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    if trade_dt.tzinfo is None:
                        trade_dt = trade_dt.replace(tzinfo=timezone.utc)

            if action == "close":
                summary["close_count"] += 1
                if net_pnl_usd > 0:
                    summary["winning_close_count"] += 1
                    positive_close_pnl += net_pnl_usd
                elif net_pnl_usd < 0:
                    summary["losing_close_count"] += 1
                    negative_close_pnl += abs(net_pnl_usd)
                stack = open_stacks.get(stack_key) or []
                if stack and trade_dt is not None:
                    opened_at = stack.pop()
                    holding_minutes.append(max(0.0, (trade_dt - opened_at).total_seconds()) / 60.0)
            else:
                summary["entry_count"] += 1
                if trade_dt is not None:
                    open_stacks.setdefault(stack_key, []).append(trade_dt)

        if summary["close_count"] > 0:
            summary["win_rate"] = round(
                float(summary["winning_close_count"]) / float(summary["close_count"]),
                6,
            )
        if positive_close_pnl > 0 and negative_close_pnl > 0:
            summary["profit_factor"] = round(positive_close_pnl / negative_close_pnl, 6)
        elif positive_close_pnl > 0 and negative_close_pnl <= 0:
            summary["profit_factor"] = None
        if holding_minutes:
            summary["avg_holding_minutes"] = round(sum(holding_minutes) / len(holding_minutes), 4)
        if symbol_counter:
            summary["dominant_symbol"] = symbol_counter.most_common(1)[0][0]

        for key in ("gross_pnl_usd", "fee_usd", "slippage_cost_usd", "cost_usd", "net_pnl_usd"):
            summary[key] = round(float(summary[key]), 4)
        return summary

    def get_strategy_position_cap_notional(
        self,
        *,
        account_equity: Optional[float],
        strategy_allocation: float,
    ) -> float:
        equity = self._safe_nonnegative_float(account_equity, 0.0)
        if equity <= 0:
            return 0.0
        single_cap = equity * float(risk_manager.max_position_size or 0.1)
        alloc_ratio = self._safe_ratio(strategy_allocation, 0.0)
        alloc_cap = equity * alloc_ratio if alloc_ratio > 0 else single_cap
        return max(0.0, min(single_cap, alloc_cap if alloc_ratio > 0 else single_cap))

    def _resolve_position_notional(self, position: Optional[Any], *, fallback_price: Optional[float] = None) -> float:
        if position is None:
            return 0.0
        value = self._safe_nonnegative_float(getattr(position, "value", 0.0), 0.0)
        if value > 0:
            return value
        qty = self._safe_nonnegative_float(getattr(position, "quantity", 0.0), 0.0)
        if qty <= 0:
            return 0.0
        for raw_price in (
            getattr(position, "current_price", 0.0),
            getattr(position, "entry_price", 0.0),
            fallback_price,
        ):
            price = self._safe_nonnegative_float(raw_price, 0.0)
            if price > 0:
                return qty * price
        return 0.0

    def _pair_group_exposure(
        self,
        *,
        strategy_name: Optional[str],
        pair_group_id: str,
        symbol: str,
        fallback_price: Optional[float] = None,
    ) -> Tuple[float, float]:
        if not strategy_name or not pair_group_id:
            return 0.0, 0.0
        target_symbol = str(symbol or "").strip().upper()
        group_exposure = 0.0
        leg_exposure = 0.0
        for position in position_manager.get_positions_by_strategy(strategy_name):
            metadata = dict(getattr(position, "metadata", {}) or {})
            if str(metadata.get("pair_group_id") or "").strip() != pair_group_id:
                continue
            notional = self._resolve_position_notional(position, fallback_price=fallback_price)
            if notional <= 0:
                continue
            group_exposure += notional
            if str(getattr(position, "symbol", "") or "").strip().upper() == target_symbol:
                leg_exposure += notional
        return float(group_exposure), float(leg_exposure)

    @staticmethod
    def _safe_positive_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            out = float(value)
            if out <= 0:
                return default
            return out
        except Exception:
            return default

    @staticmethod
    def _safe_protective_pct(value: Any) -> Optional[float]:
        try:
            out = float(value)
            if 0 < out < 1:
                return out
            return None
        except Exception:
            return None

    @staticmethod
    def _merge_position_metadata(
        current: Optional[Dict[str, Any]],
        updates: Optional[Dict[str, Any]],
        *,
        source: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        reset_profit_management_state: bool = False,
    ) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        if isinstance(current, dict):
            merged.update(current)
        if reset_profit_management_state:
            for key in _PROFIT_MANAGEMENT_STATE_KEYS:
                merged.pop(key, None)
        if isinstance(updates, dict):
            merged.update(dict(updates))
        if source is not None:
            merged["source"] = source
        if reduce_only is not None:
            merged["reduce_only"] = bool(reduce_only)
        return merged

    def _effective_profit_management_metadata(self, position: Any) -> Dict[str, Any]:
        metadata = dict(getattr(position, "metadata", {}) or {})
        source = str(metadata.get("source") or "").strip().lower()
        strategy = str(getattr(position, "strategy", "") or "").strip().lower()
        is_autonomous_position = bool(
            metadata.get("agent_provider")
            or metadata.get("agent_model")
            or source == "ai_autonomous_agent"
            or strategy == "ai_autonomousagent"
        )
        if not is_autonomous_position:
            return metadata
        changed = False
        for key, value in _AUTONOMOUS_PROFIT_MANAGEMENT_DEFAULTS.items():
            if metadata.get(key) is None:
                metadata[key] = value
                changed = True
        if changed:
            position.metadata = metadata
        return metadata

    @staticmethod
    def _position_profit_pct(position: Any) -> float:
        try:
            return float(getattr(position, "unrealized_pnl_pct", 0.0) or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _is_more_protective_stop(
        side: Any,
        candidate_stop: Optional[float],
        current_stop: Optional[float],
    ) -> bool:
        if candidate_stop is None or float(candidate_stop) <= 0:
            return False
        if current_stop is None or float(current_stop) <= 0:
            return True
        if side == PositionSide.SHORT:
            return float(candidate_stop) < float(current_stop) - 1e-12
        return float(candidate_stop) > float(current_stop) + 1e-12

    @staticmethod
    def _is_tighter_trailing_pct(
        candidate_pct: Optional[float],
        current_pct: Optional[float],
    ) -> bool:
        if candidate_pct is None or float(candidate_pct) <= 0:
            return False
        if current_pct is None or float(current_pct) <= 0:
            return True
        return float(candidate_pct) < float(current_pct) - 1e-12

    def _entry_lock_stop_price(self, position: Any, lock_pct: Any) -> Optional[float]:
        pct = self._safe_protective_pct(lock_pct)
        entry_price = self._safe_positive_float(getattr(position, "entry_price", None))
        if pct is None or entry_price is None:
            return None
        if getattr(position, "side", None) == PositionSide.SHORT:
            return entry_price * (1.0 - pct)
        return entry_price * (1.0 + pct)

    def _apply_position_stop_loss(
        self,
        position: Any,
        *,
        stop_price: Optional[float],
        metadata_flag: Optional[str] = None,
        event: Optional[str] = None,
    ) -> bool:
        if not self._is_more_protective_stop(
            getattr(position, "side", None),
            stop_price,
            getattr(position, "stop_loss", None),
        ):
            return False
        position.stop_loss = float(stop_price)
        metadata = dict(getattr(position, "metadata", {}) or {})
        now_iso = datetime.now(timezone.utc).isoformat()
        if metadata_flag:
            metadata[metadata_flag] = True
            metadata[f"{metadata_flag}_at"] = now_iso
        if event:
            metadata["profit_management_last_event"] = event
        position.metadata = metadata
        return True

    def _apply_position_trailing_pct(
        self,
        position: Any,
        *,
        trailing_pct: Any,
        current_price: Optional[float] = None,
        event: Optional[str] = None,
    ) -> bool:
        pct = self._safe_protective_pct(trailing_pct)
        current_pct = self._safe_protective_pct(getattr(position, "trailing_stop_pct", None))
        if pct is None or not self._is_tighter_trailing_pct(pct, current_pct):
            return False
        position.trailing_stop_pct = float(pct)
        position.trailing_stop_distance = None
        refresh_price = self._safe_positive_float(
            current_price,
            self._safe_positive_float(getattr(position, "current_price", None)),
        )
        if refresh_price is not None:
            position.update_price(refresh_price)
        metadata = dict(getattr(position, "metadata", {}) or {})
        if event:
            metadata["profit_management_last_event"] = event
        position.metadata = metadata
        return True

    def _calculate_partial_take_profit_quantity(
        self,
        position: Any,
        *,
        current_price: float,
        fraction: Any,
    ) -> Tuple[float, str]:
        quantity = self._safe_nonnegative_float(getattr(position, "quantity", 0.0), 0.0)
        price = self._safe_nonnegative_float(current_price, 0.0)
        if quantity <= 0 or price <= 0:
            return 0.0, "invalid_position"
        raw_fraction = self._safe_nonnegative_float(fraction, 0.5)
        partial_fraction = min(0.95, max(0.05, float(raw_fraction or 0.5)))
        close_qty = quantity * partial_fraction
        remaining_qty = max(0.0, quantity - close_qty)
        if close_qty <= 0 or remaining_qty <= 0:
            return 0.0, "invalid_partial_fraction"
        min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
        close_notional = close_qty * price
        remaining_notional = remaining_qty * price
        if close_notional + 1e-9 < min_notional:
            return 0.0, "partial_notional_below_min"
        if remaining_notional + 1e-9 < min_notional:
            return 0.0, "remaining_notional_below_min"
        return float(close_qty), ""

    async def _execute_position_partial_take_profit(
        self,
        position: Any,
        *,
        current_price: float,
    ) -> Dict[str, Any]:
        metadata = self._effective_profit_management_metadata(position)
        close_qty, skip_reason = self._calculate_partial_take_profit_quantity(
            position,
            current_price=current_price,
            fraction=metadata.get("partial_take_profit_fraction"),
        )
        if close_qty <= 0:
            metadata["partial_take_profit_skip_reason"] = skip_reason or "partial_unavailable"
            position.metadata = metadata
            return {"applied": False, "reason": metadata["partial_take_profit_skip_reason"]}

        close_side = "sell" if getattr(position, "side", None) == PositionSide.LONG else "buy"
        result = await self._execute_manual_order_single(
            exchange=str(getattr(position, "exchange", "") or ""),
            symbol=str(getattr(position, "symbol", "") or ""),
            side=close_side,
            order_type="market",
            amount=float(close_qty),
            price=float(current_price),
            leverage=float(getattr(position, "leverage", 1.0) or 1.0),
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id=str(getattr(position, "account_id", "main") or "main"),
            reduce_only=True,
            strategy=str(getattr(position, "strategy", "") or "risk"),
            params={"close_reason": "partial_take_profit", "profit_management": True},
        )
        if not result:
            metadata["partial_take_profit_skip_reason"] = "execution_rejected"
            position.metadata = metadata
            return {"applied": False, "reason": "execution_rejected"}

        refreshed = position_manager.get_position(
            str(getattr(position, "exchange", "") or ""),
            str(getattr(position, "symbol", "") or ""),
            account_id=str(getattr(position, "account_id", "main") or "main"),
        )
        if refreshed is None:
            return {"applied": False, "reason": "position_closed"}
        refreshed.update_price(float(current_price))
        refreshed_metadata = self._effective_profit_management_metadata(refreshed)
        refreshed_metadata["partial_take_profit_done"] = True
        refreshed_metadata["partial_take_profit_done_at"] = datetime.now(timezone.utc).isoformat()
        refreshed_metadata["partial_take_profit_order_id"] = str(result.get("order_id") or "")
        refreshed_metadata["partial_take_profit_price"] = float(current_price)
        refreshed_metadata.pop("partial_take_profit_skip_reason", None)
        refreshed_metadata["profit_management_last_event"] = "partial_take_profit"
        refreshed.metadata = refreshed_metadata
        refreshed.take_profit = None
        trailing_pct = self._safe_protective_pct(refreshed_metadata.get("post_partial_trailing_stop_pct"))
        if trailing_pct is not None:
            self._apply_position_trailing_pct(
                refreshed,
                trailing_pct=trailing_pct,
                current_price=current_price,
                event="post_partial_trailing",
            )
        logger.info(
            "Profit management partial take profit executed "
            f"symbol={refreshed.symbol} exchange={refreshed.exchange} "
            f"account_id={refreshed.account_id} qty={close_qty:.8f} price={float(current_price):.8f}"
        )
        await self._notify_callbacks(
            "profit_management_partial_take_profit",
            {
                "symbol": refreshed.symbol,
                "exchange": refreshed.exchange,
                "account_id": refreshed.account_id,
                "price": float(current_price),
                "close_qty": float(close_qty),
                "order_id": result.get("order_id"),
            },
        )
        return {
            "applied": True,
            "order_id": result.get("order_id"),
            "position": refreshed,
        }

    async def _apply_position_profit_management(self, position: Any, current_price: float) -> Any:
        metadata = self._effective_profit_management_metadata(position)
        if not metadata:
            return position

        profit_pct = self._position_profit_pct(position)
        if profit_pct <= 0:
            return position

        partial_trigger_pct = self._safe_protective_pct(metadata.get("partial_take_profit_trigger_pct"))
        if (
            bool(metadata.get("partial_take_profit_enabled"))
            and not bool(metadata.get("partial_take_profit_done"))
            and partial_trigger_pct is not None
            and profit_pct >= partial_trigger_pct
        ):
            partial_result = await self._execute_position_partial_take_profit(
                position,
                current_price=current_price,
            )
            refreshed_position = partial_result.get("position")
            if refreshed_position is not None:
                position = refreshed_position
                metadata = self._effective_profit_management_metadata(position)
                profit_pct = self._position_profit_pct(position)

        profit_trigger_pct = self._safe_protective_pct(metadata.get("profit_protect_trigger_pct"))
        if (
            bool(metadata.get("profit_protect_enabled"))
            and profit_trigger_pct is not None
            and profit_pct >= profit_trigger_pct
        ):
            lock_stop = self._entry_lock_stop_price(position, metadata.get("profit_protect_lock_pct"))
            if self._apply_position_stop_loss(
                position,
                stop_price=lock_stop,
                metadata_flag="profit_protect_armed",
                event="profit_protect",
            ):
                logger.info(
                    "Profit protect armed "
                    f"symbol={position.symbol} exchange={position.exchange} "
                    f"account_id={position.account_id} stop_loss={float(position.stop_loss or 0.0):.8f}"
                )

        if bool(metadata.get("partial_take_profit_done")):
            trailing_pct = self._safe_protective_pct(metadata.get("post_partial_trailing_stop_pct"))
            if self._apply_position_trailing_pct(
                position,
                trailing_pct=trailing_pct,
                current_price=current_price,
                event="post_partial_trailing",
            ):
                logger.info(
                    "Post-partial trailing armed "
                    f"symbol={position.symbol} exchange={position.exchange} "
                    f"account_id={position.account_id} trailing_stop_pct={float(position.trailing_stop_pct or 0.0):.6f}"
                )

        return position_manager.get_position(
            str(getattr(position, "exchange", "") or ""),
            str(getattr(position, "symbol", "") or ""),
            account_id=str(getattr(position, "account_id", "main") or "main"),
        ) or position

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
        total_cost_usd = fee_usd + slippage_cost_usd
        if total_cost_usd > 0:
            self._paper_total_fees_usd += float(total_cost_usd)
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
        equity = float(account_equity or 0.0)
        alloc_ratio = self._safe_ratio(strategy_allocation, 0.0)
        apply_total_exposure_cap = bool((signal.metadata or {}).get("apply_total_exposure_cap"))
        signal_total_exposure_ratio = self._safe_ratio(
            (signal.metadata or {}).get("max_total_exposure_ratio"),
            0.0,
        )
        effective_alloc_ratio = alloc_ratio
        if apply_total_exposure_cap and signal_total_exposure_ratio > 0:
            effective_alloc_ratio = (
                min(alloc_ratio, signal_total_exposure_ratio)
                if alloc_ratio > 0
                else signal_total_exposure_ratio
            )
        position_cap_notional = self.get_strategy_position_cap_notional(
            account_equity=account_equity,
            strategy_allocation=strategy_allocation,
        )
        same_direction_limit_ratio = self._safe_ratio(
            (signal.metadata or {}).get("same_direction_max_exposure_ratio"),
            0.0,
        )
        same_direction_existing_notional = self._safe_nonnegative_float(
            (signal.metadata or {}).get("same_direction_existing_notional"),
            0.0,
        )
        same_direction_limit_notional = 0.0
        same_direction_remaining_cap = 0.0
        if same_direction_limit_ratio > 0 and position_cap_notional > 0:
            same_direction_limit_notional = position_cap_notional * same_direction_limit_ratio
            same_direction_remaining_cap = max(0.0, same_direction_limit_notional - same_direction_existing_notional)

        if signal.quantity is not None:
            qty = max(0.0, float(signal.quantity))
            if qty <= 0:
                return 0.0
            # Unless explicitly requested, avoid sending dust-size strategy orders.
            if not bool((signal.metadata or {}).get("respect_quantity", False)):
                qty = max(qty, configured_min_notional / price)
            if same_direction_limit_notional > 0:
                if same_direction_remaining_cap <= 0:
                    return 0.0
                qty = min(qty, same_direction_remaining_cap / price)
            return max(0.0, self._floor_to_decimals(qty, 8))

        if equity <= 0:
            # Conservative fallback for unknown equity in paper mode.
            return max(0.0, round(max(10.0, configured_min_notional) / price, 8))

        strength = float(signal.strength or 1.0)
        strength = max(0.1, min(strength, 1.0))

        single_cap = equity * float(risk_manager.max_position_size or 0.1)
        alloc_cap = equity * effective_alloc_ratio if effective_alloc_ratio > 0 else single_cap
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

        pair_group_id = str((signal.metadata or {}).get("pair_group_id") or "").strip()
        pair_quantity_scale = self._safe_nonnegative_float(
            (signal.metadata or {}).get("pair_quantity_scale"),
            0.0,
        )
        pair_unit_notional = self._safe_nonnegative_float(
            (signal.metadata or {}).get("pair_unit_notional"),
            0.0,
        )
        pair_leg_fraction = self._safe_ratio(
            (signal.metadata or {}).get("pair_leg_notional_fraction"),
            0.0,
        )
        pair_min_leg_fraction = self._safe_ratio(
            (signal.metadata or {}).get("pair_min_leg_notional_fraction"),
            0.0,
        )
        current_pair_group_exposure = 0.0
        current_pair_leg_exposure = 0.0
        if pair_group_id and pair_quantity_scale > 0 and pair_unit_notional > 0 and pair_leg_fraction > 0:
            current_pair_group_exposure, current_pair_leg_exposure = self._pair_group_exposure(
                strategy_name=signal.strategy_name,
                pair_group_id=pair_group_id,
                symbol=signal.symbol,
                fallback_price=price,
            )

        remaining_alloc_cap = max(0.0, alloc_cap - current_strategy_exposure)
        if effective_alloc_ratio > 0 and remaining_alloc_cap <= 0:
            return 0.0
        effective_min_notional = configured_min_notional
        if effective_alloc_ratio > 0:
            effective_min_notional = min(
                configured_min_notional,
                max(exchange_min_notional, remaining_alloc_cap * 0.98),
            )
        else:
            effective_min_notional = min(
                configured_min_notional,
                max(exchange_min_notional, single_cap * 0.98),
            )
        if same_direction_limit_notional > 0:
            if same_direction_remaining_cap <= 0:
                return 0.0
            effective_min_notional = min(
                effective_min_notional,
                max(exchange_min_notional, same_direction_remaining_cap * 0.98),
            )
        effective_min_notional = max(exchange_min_notional, effective_min_notional)
        if effective_alloc_ratio > 0 and remaining_alloc_cap < effective_min_notional:
            logger.debug(
                f"Skip tiny order for {signal.strategy_name or 'unknown'}: "
                f"remaining allocation {remaining_alloc_cap:.4f} < min_notional {effective_min_notional:.4f}"
            )
            return 0.0

        if pair_group_id and pair_quantity_scale > 0 and pair_unit_notional > 0 and pair_leg_fraction > 0:
            group_other_exposure = max(0.0, current_strategy_exposure - current_pair_group_exposure)
            pair_effective_cap = single_cap
            if effective_alloc_ratio > 0:
                pair_effective_cap = min(pair_effective_cap, max(0.0, alloc_cap - group_other_exposure))
            pair_min_fraction = max(1e-6, float(pair_min_leg_fraction or pair_leg_fraction))
            pair_effective_min_notional = max(
                configured_min_notional,
                exchange_min_notional / pair_min_fraction,
            )
            pair_buffered_cap = pair_effective_cap * risk_buffer
            if same_direction_limit_notional > 0:
                pair_buffered_cap = min(pair_buffered_cap, current_pair_leg_exposure + same_direction_remaining_cap)
            if pair_buffered_cap < pair_effective_min_notional:
                logger.info(
                    f"Skip pair leg due to insufficient pair notional cap: strategy={signal.strategy_name} "
                    f"symbol={signal.symbol} pair_group={pair_group_id} cap={pair_buffered_cap:.4f} "
                    f"required={pair_effective_min_notional:.4f}"
                )
                return 0.0

            target_pair_notional = max(pair_effective_min_notional, pair_effective_cap * strength)
            target_pair_notional = min(target_pair_notional, pair_buffered_cap)
            desired_leg_notional = target_pair_notional * pair_leg_fraction
            remaining_leg_notional = max(0.0, desired_leg_notional - current_pair_leg_exposure)
            if remaining_leg_notional <= 0:
                return 0.0

            qty = remaining_leg_notional / price
            min_amount, amount_decimals = await self._get_exchange_amount_rules(exchange, signal.symbol)
            if min_amount > 0 and qty < min_amount:
                required_leg_notional = min_amount * price
                required_pair_notional = required_leg_notional / max(pair_leg_fraction, 1e-6)
                if required_pair_notional > pair_effective_cap + max(0.05, pair_effective_cap * 0.01):
                    logger.info(
                        f"Skip pair leg below exchange min amount: strategy={signal.strategy_name} "
                        f"symbol={signal.symbol} qty={qty:.8f} min_amount={min_amount:.8f} "
                        f"pair_cap={pair_effective_cap:.4f}"
                    )
                    return 0.0
                qty = float(min_amount)

            floored_qty = max(0.0, self._floor_to_decimals(qty, amount_decimals))
            if floored_qty <= 0:
                return 0.0

            floored_notional = floored_qty * price
            if floored_notional + 1e-9 < exchange_min_notional:
                required_qty = self._ceil_to_decimals(exchange_min_notional / price, amount_decimals)
                required_leg_notional = required_qty * price
                required_pair_notional = required_leg_notional / max(pair_leg_fraction, 1e-6)
                if required_pair_notional > pair_effective_cap + max(0.05, pair_effective_cap * 0.01):
                    logger.info(
                        f"Skip pair leg after precision floor broke exchange min notional: "
                        f"strategy={signal.strategy_name} symbol={signal.symbol} "
                        f"floored_qty={floored_qty:.8f} required_qty={required_qty:.8f} "
                        f"pair_cap={pair_effective_cap:.4f}"
                    )
                    return 0.0
                floored_qty = required_qty
            return floored_qty

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
        if same_direction_limit_notional > 0:
            buffered_cap = min(buffered_cap, same_direction_remaining_cap * risk_buffer)
            if buffered_cap < effective_min_notional:
                return 0.0
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
        strategy_type = str(getattr(getattr(strategy, "__class__", None), "__name__", "") or "")

        market_type = str(params.get("market_type") or "").strip().lower()
        if not market_type and strategy_type == "PairsTradingStrategy":
            market_type = "future"
        if not market_type:
            connector = exchange_manager.get_exchange(exchange)
            market_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").strip().lower()
        if not market_type:
            market_type = "futures" if self._paper_trading else "spot"

        is_derivatives = market_type in {"future", "futures", "swap", "contract", "perp", "perpetual"}
        default_allow_short = True if self._paper_trading else is_derivatives
        if strategy_type == "PairsTradingStrategy":
            default_allow_short = True

        allow_long = bool(params.get("allow_long", True))
        allow_short = bool(params.get("allow_short", default_allow_short))
        reverse_on_signal = bool(params.get("reverse_on_signal", True))
        allow_pyramiding = bool(params.get("allow_pyramiding", False))
        stop_loss_pct = self._safe_protective_pct(params.get("stop_loss_pct"))
        take_profit_pct = self._safe_protective_pct(params.get("take_profit_pct"))

        return {
            "market_type": market_type,
            "allow_long": allow_long,
            "allow_short": allow_short,
            "reverse_on_signal": reverse_on_signal,
            "allow_pyramiding": allow_pyramiding,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
        }

    def _normalize_protection_levels(
        self,
        *,
        side: OrderSide,
        entry_price: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
    ) -> Tuple[Optional[float], Optional[float]]:
        sl = self._safe_positive_float(stop_loss)
        tp = self._safe_positive_float(take_profit)
        px = float(entry_price or 0.0)
        if px <= 0:
            return sl, tp

        eps = max(1e-8, px * 1e-8)
        if side == OrderSide.BUY:
            if sl is not None and sl >= px - eps:
                sl = None
            if tp is not None and tp <= px + eps:
                tp = None
        elif side == OrderSide.SELL:
            if sl is not None and sl <= px + eps:
                sl = None
            if tp is not None and tp >= px - eps:
                tp = None
        return sl, tp

    def _resolve_signal_protection_pcts(
        self,
        *,
        signal: Signal,
        trade_policy: Dict[str, Any],
    ) -> Tuple[Optional[float], Optional[float]]:
        meta = dict(signal.metadata or {})
        signal_sl_pct = self._safe_protective_pct(meta.get("stop_loss_pct"))
        signal_tp_pct = self._safe_protective_pct(meta.get("take_profit_pct"))
        policy_sl_pct = self._safe_protective_pct(trade_policy.get("stop_loss_pct"))
        policy_tp_pct = self._safe_protective_pct(trade_policy.get("take_profit_pct"))
        default_sl_pct = self._safe_protective_pct(
            getattr(settings, "STRATEGY_DEFAULT_STOP_LOSS_PCT", 0.03)
        )
        default_tp_pct = self._safe_protective_pct(
            getattr(settings, "STRATEGY_DEFAULT_TAKE_PROFIT_PCT", 0.06)
        )

        sl_pct = (
            signal_sl_pct
            if signal_sl_pct is not None
            else (policy_sl_pct if policy_sl_pct is not None else default_sl_pct)
        )
        tp_pct = (
            signal_tp_pct
            if signal_tp_pct is not None
            else (policy_tp_pct if policy_tp_pct is not None else default_tp_pct)
        )
        return sl_pct, tp_pct

    def _ensure_signal_protection_levels(
        self,
        *,
        signal: Signal,
        side: OrderSide,
        entry_price: float,
        trade_policy: Dict[str, Any],
    ) -> Tuple[Optional[float], Optional[float]]:
        stop_loss, take_profit = self._normalize_protection_levels(
            side=side,
            entry_price=float(entry_price or 0.0),
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
        )
        sl_pct, tp_pct = self._resolve_signal_protection_pcts(
            signal=signal,
            trade_policy=trade_policy,
        )

        px = float(entry_price or 0.0)
        auto_stop = False
        auto_take = False
        if px > 0:
            if stop_loss is None and sl_pct is not None:
                stop_loss = px * (1.0 - sl_pct) if side == OrderSide.BUY else px * (1.0 + sl_pct)
                auto_stop = True
            if take_profit is None and tp_pct is not None:
                take_profit = px * (1.0 + tp_pct) if side == OrderSide.BUY else px * (1.0 - tp_pct)
                auto_take = True

            stop_loss, take_profit = self._normalize_protection_levels(
                side=side,
                entry_price=px,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )

        if auto_stop or auto_take:
            logger.warning(
                "Auto-injected protective levels for strategy signal: "
                f"strategy={signal.strategy_name} symbol={signal.symbol} side={side.value} "
                f"entry={px:.8f} stop_loss={stop_loss} take_profit={take_profit}"
            )
        return stop_loss, take_profit

    @staticmethod
    def _canonical_symbol(symbol: str) -> str:
        raw = str(symbol or "").strip().upper()
        if not raw:
            return ""
        if ":" in raw:
            raw = raw.split(":", 1)[0]
        if "_" in raw and "/" not in raw:
            left, right = raw.split("_", 1)
            raw = f"{left}/{right}"
        if raw.endswith("USDT") and "/" not in raw and len(raw) > 4:
            raw = f"{raw[:-4]}/USDT"
        return raw

    async def _get_exchange_position_snapshot(
        self,
        *,
        exchange: str,
        symbol: str,
        preferred_side: Optional[PositionSide] = None,
    ) -> Optional[Any]:
        if self._paper_trading:
            return None
        connector = exchange_manager.get_exchange(exchange)
        if connector is None:
            return None
        default_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").strip().lower()
        if default_type not in {"future", "futures", "swap", "contract", "perp", "perpetual"}:
            return None

        target_symbol = self._canonical_symbol(symbol)
        if not target_symbol:
            return None

        try:
            positions = await asyncio.wait_for(connector.get_positions(), timeout=8.0)
        except Exception as e:
            logger.debug(f"Failed to query exchange position snapshot for {exchange} {symbol}: {e}")
            return None

        preferred_match: Optional[Any] = None
        fallback_match: Optional[Any] = None
        preferred_side_text = str(getattr(preferred_side, "value", "") or "").strip().lower()

        for pos in positions or []:
            symbol_raw = str((pos.get("symbol") if isinstance(pos, dict) else getattr(pos, "symbol", "")) or "")
            symbol_key = self._canonical_symbol(symbol_raw)
            if symbol_key != target_symbol:
                continue

            amount = float((pos.get("amount") if isinstance(pos, dict) else getattr(pos, "amount", 0.0)) or 0.0)
            if abs(amount) <= 1e-12:
                amount = float((pos.get("quantity") if isinstance(pos, dict) else getattr(pos, "quantity", 0.0)) or 0.0)
            if abs(amount) <= 1e-12:
                continue

            side_text = str((pos.get("side") if isinstance(pos, dict) else getattr(pos, "side", "")) or "").strip().lower()
            if side_text not in {"long", "short"}:
                side_text = "short" if amount < 0 else "long"
            if side_text not in {"long", "short"}:
                continue

            entry_price = float((pos.get("entry_price") if isinstance(pos, dict) else getattr(pos, "entry_price", 0.0)) or 0.0)
            current_price = float((pos.get("current_price") if isinstance(pos, dict) else getattr(pos, "current_price", 0.0)) or 0.0)
            if current_price <= 0:
                current_price = float((pos.get("markPrice") if isinstance(pos, dict) else getattr(pos, "markPrice", 0.0)) or 0.0)
            if current_price <= 0:
                current_price = entry_price
            quantity = abs(amount)
            leverage = float((pos.get("leverage") if isinstance(pos, dict) else getattr(pos, "leverage", 1.0)) or 1.0)
            unrealized_pnl = float(
                (pos.get("unrealized_pnl") if isinstance(pos, dict) else getattr(pos, "unrealized_pnl", 0.0))
                or (pos.get("unrealizedPnl") if isinstance(pos, dict) else getattr(pos, "unrealizedPnl", 0.0))
                or 0.0
            )

            snapshot = SimpleNamespace(
                symbol=target_symbol,
                exchange=exchange,
                side=PositionSide.LONG if side_text == "long" else PositionSide.SHORT,
                entry_price=entry_price,
                current_price=current_price,
                quantity=quantity,
                value=current_price * quantity,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=0.0,
                leverage=leverage,
                margin=0.0,
                strategy=None,
                account_id="exchange_live",
                stop_loss=None,
                take_profit=None,
                trailing_stop_pct=None,
                trailing_stop_distance=None,
                metadata={"source": "exchange_live", "synced_from_exchange": exchange},
                update_price=lambda price: None,
            )
            if side_text == preferred_side_text:
                preferred_match = snapshot
                break
            if fallback_match is None:
                fallback_match = snapshot

        return preferred_match or fallback_match

    async def _resolve_existing_position(
        self,
        *,
        exchange: str,
        symbol: str,
        account_id: str,
        preferred_side: Optional[PositionSide] = None,
    ) -> Optional[Any]:
        exact = position_manager.get_position(exchange, symbol, account_id=account_id)
        if exact is not None:
            return exact
        return await self._get_exchange_position_snapshot(
            exchange=exchange,
            symbol=symbol,
            preferred_side=preferred_side,
        )

    async def _exchange_has_side_position(
        self,
        *,
        exchange: str,
        symbol: str,
        side: PositionSide,
        min_qty: float = 1e-12,
    ) -> Tuple[bool, bool]:
        connector = exchange_manager.get_exchange(exchange)
        if connector is None:
            return False, False
        try:
            positions = await asyncio.wait_for(connector.get_positions(), timeout=8.0)
        except Exception as e:
            logger.warning(f"Failed to query exchange positions for reconciliation: {e}")
            return False, False
        target_symbol = self._canonical_symbol(symbol)
        target_side = "long" if side == PositionSide.LONG else "short"
        for pos in positions or []:
            pos_symbol = self._canonical_symbol(
                str((pos.get("symbol") if isinstance(pos, dict) else getattr(pos, "symbol", "")) or "")
            )
            if pos_symbol != target_symbol:
                continue
            pos_side = str((pos.get("side") if isinstance(pos, dict) else getattr(pos, "side", "")) or "").strip().lower()
            pos_qty = abs(float((pos.get("amount") if isinstance(pos, dict) else getattr(pos, "amount", 0.0)) or 0.0))
            if pos_side == target_side and pos_qty > max(1e-12, float(min_qty or 0.0)):
                return True, True
        return False, True

    @staticmethod
    def _is_reduce_only_rejected(error_text: str) -> bool:
        text = str(error_text or "").lower()
        return ("reduceonly order is rejected" in text) or ("\"code\":-2022" in text) or ("code:-2022" in text)

    async def _reconcile_local_positions_with_exchange(self) -> None:
        """In live mode, drop stale local positions that no longer exist on exchange."""
        if self._paper_trading:
            return
        local_positions = list(position_manager.get_all_positions())
        if not local_positions:
            return

        now = datetime.now(timezone.utc)
        if (
            self._last_live_reconcile_at
            and (now - self._last_live_reconcile_at).total_seconds() < self._live_reconcile_interval_seconds
        ):
            return
        self._last_live_reconcile_at = now

        now_ts = datetime.now().timestamp()
        grouped: Dict[str, List[Any]] = {}
        active_local_keys: set[Tuple[str, str, str, str]] = set()
        for pos in local_positions:
            exchange_name = str(getattr(pos, "exchange", "") or "").strip().lower()
            if not exchange_name:
                continue
            local_symbol = self._canonical_symbol(str(getattr(pos, "symbol", "") or ""))
            local_side = str(getattr(getattr(pos, "side", None), "value", "") or "").strip().lower()
            account_id = str(getattr(pos, "account_id", "main") or "main")
            if local_symbol and local_side in {"long", "short"}:
                active_local_keys.add((account_id, exchange_name, local_symbol, local_side))
            grouped.setdefault(exchange_name, []).append(pos)

        for exchange_name, positions in grouped.items():
            connector = exchange_manager.get_exchange(exchange_name)
            if not connector:
                continue
            default_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").lower()
            if default_type not in {"future", "swap"}:
                continue

            try:
                exchange_positions = await asyncio.wait_for(connector.get_positions(), timeout=7.5)
            except Exception as e:
                logger.debug(f"Skip local position reconcile for {exchange_name}: {e}")
                continue

            exchange_side_keys: set[Tuple[str, str]] = set()
            for ex_pos in exchange_positions or []:
                symbol_raw = str((ex_pos.get("symbol") if isinstance(ex_pos, dict) else getattr(ex_pos, "symbol", "")) or "")
                symbol_key = self._canonical_symbol(symbol_raw)
                if not symbol_key:
                    continue
                amount = float((ex_pos.get("amount") if isinstance(ex_pos, dict) else getattr(ex_pos, "amount", 0.0)) or 0.0)
                if abs(amount) <= 1e-12:
                    continue
                side = str((ex_pos.get("side") if isinstance(ex_pos, dict) else getattr(ex_pos, "side", "")) or "").strip().lower()
                if not side:
                    side = "short" if amount < 0 else "long"
                if side not in {"long", "short"}:
                    continue
                exchange_side_keys.add((symbol_key, side))

            for local_pos in positions:
                metadata = getattr(local_pos, "metadata", {}) or {}
                source = str(metadata.get("source") or "").strip().lower()
                account_id = str(getattr(local_pos, "account_id", "main") or "main")
                local_symbol = self._canonical_symbol(str(getattr(local_pos, "symbol", "") or ""))
                local_side = str(getattr(getattr(local_pos, "side", None), "value", "") or "").strip().lower()
                if not local_symbol or local_side not in {"long", "short"}:
                    continue
                position_key = (account_id, exchange_name, local_symbol, local_side)
                if source == "exchange_live":
                    self._live_reconcile_absence_counts.pop(position_key, None)
                    continue
                age: Optional[float] = None
                local_updated_at = getattr(local_pos, "updated_at", None)
                if isinstance(local_updated_at, datetime):
                    age = max(0.0, now_ts - float(local_updated_at.timestamp()))
                    if age < self._live_reconcile_grace_seconds:
                        self._live_reconcile_absence_counts.pop(position_key, None)
                        continue

                if (local_symbol, local_side) in exchange_side_keys:
                    self._live_reconcile_absence_counts.pop(position_key, None)
                    continue

                absence_count = int(self._live_reconcile_absence_counts.get(position_key, 0) or 0) + 1
                self._live_reconcile_absence_counts[position_key] = absence_count
                min_age = max(
                    float(self._live_reconcile_grace_seconds),
                    float(self._live_reconcile_absence_min_age_seconds),
                )
                if age is not None and age < min_age:
                    continue
                if absence_count < max(1, int(self._live_reconcile_absence_threshold)):
                    continue

                close_price = float(
                    getattr(local_pos, "current_price", 0.0)
                    or getattr(local_pos, "entry_price", 0.0)
                    or 0.0
                )
                if close_price <= 0:
                    close_price = float(getattr(local_pos, "entry_price", 0.0) or 0.0)

                closed = position_manager.close_position(
                    exchange=exchange_name,
                    symbol=str(getattr(local_pos, "symbol", "") or ""),
                    close_price=close_price,
                    quantity=float(getattr(local_pos, "quantity", 0.0) or 0.0),
                    account_id=str(getattr(local_pos, "account_id", "main") or "main"),
                )
                if not closed:
                    continue
                self._live_reconcile_absence_counts.pop(position_key, None)
                logger.warning(
                    "Reconciled stale local position from exchange snapshot: "
                    f"exchange={exchange_name} symbol={closed.symbol} side={closed.side.value} "
                    f"account_id={closed.account_id} misses={absence_count} age_sec={age if age is not None else 'na'}"
                )
                await self._notify_callbacks(
                    "position_reconciled",
                    {
                        "exchange": exchange_name,
                        "symbol": closed.symbol,
                        "side": closed.side.value,
                        "account_id": closed.account_id,
                        "close_price": close_price,
                        "reason": "exchange_flat_manual_close",
                    },
                )

        stale_keys = [
            key
            for key in list(self._live_reconcile_absence_counts.keys())
            if key not in active_local_keys
        ]
        for key in stale_keys:
            self._live_reconcile_absence_counts.pop(key, None)

    async def _evaluate_live_ai_decision(
        self,
        *,
        signal: Signal,
        side: OrderSide,
        exchange: str,
        account_id: str,
        leverage: float,
        account_equity: float,
        order_value: float,
        quote_price: float,
        existing_position: Any,
        trade_policy: Dict[str, Any],
    ) -> Dict[str, Any]:
        position_payload: Dict[str, Any] = {}
        if existing_position is not None:
            try:
                position_payload = {
                    "side": str(getattr(existing_position, "side", None).value),
                    "quantity": float(getattr(existing_position, "quantity", 0.0) or 0.0),
                    "entry_price": float(getattr(existing_position, "entry_price", 0.0) or 0.0),
                    "unrealized_pnl": float(getattr(existing_position, "unrealized_pnl", 0.0) or 0.0),
                }
            except Exception:
                position_payload = {}

        metadata = dict(signal.metadata or {})
        source = str(metadata.get("source") or "").strip().lower()
        if bool(metadata.get("skip_live_decision_review")):
            self._signal_diagnostics["ai_review_bypassed"] = int(
                self._signal_diagnostics.get("ai_review_bypassed", 0)
            ) + 1
            return {
                "enabled": False,
                "applied": False,
                "mode": "bypass",
                "provider": "",
                "model": "",
                "action": "allow",
                "allowed": True,
                "reason": "ai_live_decision_bypassed_by_metadata_flag",
                "bypass_reason": "metadata_skip_live_decision_review",
                "bypass_source": source,
                "confidence": 1.0,
                "latency_ms": 0,
                "research_context": {},
            }
        metadata.update(
            {
                "exchange": exchange,
                "account_id": account_id,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
            }
        )

        return await live_decision_router.evaluate_signal(
            trading_mode=self.get_trading_mode(),
            strategy=str(signal.strategy_name or ""),
            symbol=str(signal.symbol or ""),
            signal_type=str(getattr(signal.signal_type, "value", side.value) or side.value),
            signal_strength=float(signal.strength or 0.0),
            price=float(quote_price or signal.price or 0.0),
            account_equity=float(account_equity or 0.0),
            order_value=float(order_value or 0.0),
            leverage=float(leverage or 1.0),
            timeframe=str(metadata.get("timeframe") or ""),
            existing_position=position_payload,
            trade_policy=dict(trade_policy or {}),
            metadata=metadata,
        )

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
            existing_position = await self._resolve_existing_position(
                exchange=exchange,
                symbol=signal.symbol,
                account_id=account_id,
                preferred_side=position_side,
            )
            if bool((signal.metadata or {}).get("close_only")):
                if not existing_position:
                    return None
                if side == OrderSide.BUY and existing_position.side == PositionSide.SHORT:
                    close_signal = Signal(
                        symbol=signal.symbol,
                        signal_type=SignalType.CLOSE_SHORT,
                        price=signal.price,
                        timestamp=signal.timestamp,
                        strategy_name=signal.strategy_name,
                        strength=signal.strength,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        metadata=dict(signal.metadata or {}),
                    )
                    return await self._close_position(close_signal, PositionSide.SHORT)
                if side == OrderSide.SELL and existing_position.side == PositionSide.LONG:
                    close_signal = Signal(
                        symbol=signal.symbol,
                        signal_type=SignalType.CLOSE_LONG,
                        price=signal.price,
                        timestamp=signal.timestamp,
                        strategy_name=signal.strategy_name,
                        strength=signal.strength,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        metadata=dict(signal.metadata or {}),
                    )
                    return await self._close_position(close_signal, PositionSide.LONG)
                return None
            same_direction = False
            same_direction_source = ""
            same_direction_limit_ratio = 0.0

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
                    same_direction_source = str((getattr(existing_position, "metadata", {}) or {}).get("source") or "local")
                    same_direction_limit_ratio = self._safe_ratio(
                        (signal.metadata or {}).get("same_direction_max_exposure_ratio"),
                        0.0,
                    )
                    if same_direction_limit_ratio <= 0:
                        reason = f"same_direction_existing_position_no_pyramiding(source={same_direction_source})"
                        self._signal_diagnostics["last_result"] = {
                            "status": "existing_position_blocked",
                            "strategy": signal.strategy_name,
                            "symbol": signal.symbol,
                            "exchange": exchange,
                            "reason": reason,
                            "position_side": str(getattr(existing_position.side, "value", "") or ""),
                            "position_source": same_direction_source,
                        }
                        self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                        logger.info(
                            f"Skip strategy signal due to existing same-direction position without pyramiding: "
                            f"strategy={signal.strategy_name} symbol={signal.symbol} exchange={exchange} "
                            f"side={side.value} source={same_direction_source}"
                        )
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
                    existing_position = await self._resolve_existing_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        account_id=account_id,
                        preferred_side=position_side,
                    )
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
            if same_direction and same_direction_limit_ratio > 0 and existing_position is not None:
                position_cap_notional = self.get_strategy_position_cap_notional(
                    account_equity=account_equity,
                    strategy_allocation=strategy_allocation,
                )
                if position_cap_notional <= 0:
                    reason = "same_direction_exposure_cap_unavailable"
                    self._signal_diagnostics["last_result"] = {
                        "status": "existing_position_blocked",
                        "strategy": signal.strategy_name,
                        "symbol": signal.symbol,
                        "exchange": exchange,
                        "reason": reason,
                        "position_side": str(getattr(existing_position.side, "value", "") or ""),
                        "position_source": same_direction_source or "local",
                    }
                    self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                    logger.info(
                        f"Skip strategy signal because same-direction exposure cap is unavailable: "
                        f"strategy={signal.strategy_name} symbol={signal.symbol} exchange={exchange}"
                    )
                    return None
                existing_notional = self._resolve_position_notional(existing_position, fallback_price=signal.price)
                exposure_limit_notional = position_cap_notional * same_direction_limit_ratio
                if existing_notional + 1e-9 >= exposure_limit_notional:
                    reason = (
                        "same_direction_exposure_limit_reached"
                        f"(current={existing_notional:.4f},limit={exposure_limit_notional:.4f},"
                        f"ratio={same_direction_limit_ratio:.3f},source={same_direction_source or 'local'})"
                    )
                    self._signal_diagnostics["last_result"] = {
                        "status": "existing_position_blocked",
                        "strategy": signal.strategy_name,
                        "symbol": signal.symbol,
                        "exchange": exchange,
                        "reason": reason,
                        "position_side": str(getattr(existing_position.side, "value", "") or ""),
                        "position_source": same_direction_source or "local",
                        "position_notional": float(existing_notional),
                        "exposure_limit_notional": float(exposure_limit_notional),
                        "exposure_limit_ratio": float(same_direction_limit_ratio),
                    }
                    self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                    logger.info(
                        f"Skip strategy signal because same-direction exposure limit is reached: "
                        f"strategy={signal.strategy_name} symbol={signal.symbol} exchange={exchange} "
                        f"current={existing_notional:.4f} limit={exposure_limit_notional:.4f} "
                        f"ratio={same_direction_limit_ratio:.3f}"
                    )
                    return None
                signal.metadata = dict(signal.metadata or {})
                signal.metadata["same_direction_max_exposure_ratio"] = float(same_direction_limit_ratio)
                signal.metadata["same_direction_existing_notional"] = float(existing_notional)
                signal.metadata["same_direction_position_cap_notional"] = float(position_cap_notional)
                signal.metadata["same_direction_limit_notional"] = float(exposure_limit_notional)

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
                self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
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
            level_price = float(quote_price or signal.price or 0.0)
            resolved_stop_loss, resolved_take_profit = self._ensure_signal_protection_levels(
                signal=signal,
                side=side,
                entry_price=level_price,
                trade_policy=trade_policy,
            )
            signal.stop_loss = resolved_stop_loss
            signal.take_profit = resolved_take_profit

            requested_order_type = str((signal.metadata or {}).get("order_type") or "").strip().lower()
            strategy_order_type = OrderType.MARKET
            if requested_order_type == OrderType.LIMIT.value:
                strategy_order_type = OrderType.LIMIT

            req = OrderRequest(
                symbol=signal.symbol,
                side=side,
                order_type=strategy_order_type,
                amount=qty,
                price=signal.price,
                exchange=exchange,
                strategy=signal.strategy_name,
                account_id=account_id,
                stop_loss=resolved_stop_loss,
                take_profit=resolved_take_profit,
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

            ai_decision = await self._evaluate_live_ai_decision(
                signal=signal,
                side=side,
                exchange=exchange,
                account_id=account_id,
                leverage=leverage,
                account_equity=float(account_equity or 0.0),
                order_value=float(order_value or 0.0),
                quote_price=float(quote_price or 0.0),
                existing_position=existing_position,
                trade_policy=trade_policy,
            )
            req.params["ai_live_decision"] = ai_decision
            ai_action = str(ai_decision.get("action") or "").lower()
            ai_applied = bool(ai_decision.get("applied"))
            if ai_action == "block" and ai_applied:
                self._signal_diagnostics["ai_rejected"] = int(self._signal_diagnostics.get("ai_rejected", 0)) + 1
                reason = (
                    f"AI决策拦截({ai_decision.get('provider')}/{ai_decision.get('model')}): "
                    f"{ai_decision.get('reason')}"
                )
                ai_review_result = {
                    "status": "ai_rejected",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "reason": reason,
                    "ai_decision": ai_decision,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                self._signal_diagnostics["last_result"] = dict(ai_review_result)
                self._signal_diagnostics["last_ai_review_result"] = dict(ai_review_result)
                self._signal_diagnostics["last_updated_at"] = str(ai_review_result["ts"])
                rejected = await order_manager.record_rejected_order(
                    request=req,
                    reason=reason,
                    price=quote_price or signal.price,
                )
                await self._notify_callbacks(
                    "ai_decision_rejected",
                    {
                        "type": "strategy_signal",
                        "symbol": signal.symbol,
                        "strategy": signal.strategy_name,
                        "reason": reason,
                        "order_id": rejected.id,
                        "ai_decision": ai_decision,
                    },
                )
                await write_audit(
                    GovernanceAuditEvent(
                        module="trading.execution",
                        action="ai_live_decision_rejected",
                        status="denied",
                        actor="system",
                        role="SYSTEM",
                        input_payload={
                            "symbol": signal.symbol,
                            "strategy": signal.strategy_name,
                            "side": side.value,
                            "order_value": float(order_value or 0.0),
                        },
                        output_payload={"reason": reason, "order_id": rejected.id, "ai_decision": ai_decision},
                    )
                )
                return None
            if ai_action == "reduce_only" and ai_applied:
                req.reduce_only = True
                req.params["ai_reduce_only"] = True
                req.params["ai_reduce_only_reason"] = str(ai_decision.get("reason") or "")
                if not closes_existing or existing_position is None:
                    self._signal_diagnostics["ai_reduce_only_rejected"] = int(
                        self._signal_diagnostics.get("ai_reduce_only_rejected", 0)
                    ) + 1
                    reason = (
                        f"AI仅允许减仓({ai_decision.get('provider')}/{ai_decision.get('model')}): "
                        f"{ai_decision.get('reason')}"
                    )
                    ai_review_result = {
                        "status": "ai_reduce_only_rejected",
                        "strategy": signal.strategy_name,
                        "symbol": signal.symbol,
                        "exchange": exchange,
                        "reason": reason,
                        "ai_decision": ai_decision,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                    self._signal_diagnostics["last_result"] = dict(ai_review_result)
                    self._signal_diagnostics["last_ai_review_result"] = dict(ai_review_result)
                    self._signal_diagnostics["last_updated_at"] = str(ai_review_result["ts"])
                    rejected = await order_manager.record_rejected_order(
                        request=req,
                        reason=reason,
                        price=quote_price or signal.price,
                    )
                    await self._notify_callbacks(
                        "ai_decision_reduce_only_rejected",
                        {
                            "type": "strategy_signal",
                            "symbol": signal.symbol,
                            "strategy": signal.strategy_name,
                            "reason": reason,
                            "order_id": rejected.id,
                            "ai_decision": ai_decision,
                        },
                    )
                    await write_audit(
                        GovernanceAuditEvent(
                            module="trading.execution",
                            action="ai_live_decision_reduce_only_rejected",
                            status="denied",
                            actor="system",
                            role="SYSTEM",
                            input_payload={
                                "symbol": signal.symbol,
                                "strategy": signal.strategy_name,
                                "side": side.value,
                                "order_value": float(order_value or 0.0),
                            },
                            output_payload={"reason": reason, "order_id": rejected.id, "ai_decision": ai_decision},
                        )
                    )
                    return None

                existing_qty = float(getattr(existing_position, "quantity", 0.0) or 0.0)
                if existing_qty <= 0:
                    return None
                if float(req.amount or 0.0) > existing_qty:
                    prev_amount = max(float(req.amount or 0.0), 1e-12)
                    req.amount = existing_qty
                    qty = existing_qty
                    order_value = float(order_value or 0.0) * (existing_qty / prev_amount)
                    self._signal_diagnostics["ai_reduce_only_adjusted"] = int(
                        self._signal_diagnostics.get("ai_reduce_only_adjusted", 0)
                    ) + 1
                    req.params["ai_reduce_only_adjusted_amount"] = existing_qty

            governance_check = await decision_engine.evaluate_order_intent(
                symbol=signal.symbol,
                side=side.value,
                leverage=leverage,
                order_value=float(order_value or 0.0),
                account_equity=float(account_equity or 0.0),
                signal_ts=signal.timestamp,
                allow_close=closes_existing,
                spread_bps=None,
                timeframe=str(signal.metadata.get("timeframe") or ""),
                source="strategy_signal",
            )
            req.params["trace_id"] = governance_check.trace_id
            if not governance_check.allowed:
                self._signal_diagnostics["risk_rejected"] = int(self._signal_diagnostics.get("risk_rejected", 0)) + 1
                reason = f"治理风控拦截: {governance_check.reason}"
                self._signal_diagnostics["last_result"] = {
                    "status": "governance_rejected",
                    "strategy": signal.strategy_name,
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "reason": reason,
                    "trace_id": governance_check.trace_id,
                }
                self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                rejected = await order_manager.record_rejected_order(
                    request=req,
                    reason=reason,
                    price=quote_price or signal.price,
                )
                await self._notify_callbacks(
                    "governance_rejected",
                    {
                        "type": "strategy_signal",
                        "symbol": signal.symbol,
                        "strategy": signal.strategy_name,
                        "reason": reason,
                        "order_id": rejected.id,
                        "trace_id": governance_check.trace_id,
                    },
                )
                await write_audit(
                    GovernanceAuditEvent(
                        module="trading.execution",
                        action="order_intent_rejected",
                        status="denied",
                        actor="system",
                        role="SYSTEM",
                        trace_id=governance_check.trace_id,
                        input_payload={
                            "symbol": signal.symbol,
                            "strategy": signal.strategy_name,
                            "side": side.value,
                            "order_value": float(order_value or 0.0),
                        },
                        output_payload={"reason": reason, "order_id": rejected.id},
                    )
                )
                return None

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
                self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
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
                order = await asyncio.wait_for(order_manager.create_order(req), timeout=float(self._real_order_timeout_seconds))
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
                self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
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
                self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
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
            # For live orders, read fee from the exchange order object
            if not self._paper_trading and fee_usd <= 0:
                order_fee = float(getattr(order, "fee", 0.0) or 0.0)
                if order_fee > 0:
                    fee_usd = order_fee
            fill_price = float(order.price or signal.price or quote_price or 0.0)
            trade_pnl = 0.0
            current_position = position_manager.get_position(exchange, signal.symbol, account_id=account_id)
            if fill_price > 0 and (req.stop_loss is None or req.take_profit is None):
                fill_stop_loss, fill_take_profit = self._ensure_signal_protection_levels(
                    signal=signal,
                    side=side,
                    entry_price=fill_price,
                    trade_policy=trade_policy,
                )
                if req.stop_loss is None:
                    req.stop_loss = fill_stop_loss
                if req.take_profit is None:
                    req.take_profit = fill_take_profit
                signal.stop_loss = req.stop_loss
                signal.take_profit = req.take_profit

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
                current_position.metadata = self._merge_position_metadata(
                    getattr(current_position, "metadata", {}) or {},
                    dict(signal.metadata or {}),
                    source="strategy",
                    reduce_only=bool(req.reduce_only),
                    reset_profit_management_state=not bool(req.reduce_only),
                )

            exec_amount = float(order.filled or qty or 0.0)
            if side == OrderSide.BUY:
                if current_position and current_position.side == PositionSide.LONG:
                    total_qty = current_position.quantity + exec_amount
                    if total_qty > 0:
                        current_position.entry_price = (
                            (current_position.entry_price * current_position.quantity)
                            + (fill_price * exec_amount)
                        ) / total_qty
                    current_position.quantity = total_qty
                    current_position.margin = (
                        current_position.entry_price * current_position.quantity
                    ) / max(1e-9, float(current_position.leverage or leverage or 1.0))
                    _merge_protection_settings()
                    current_position.update_price(fill_price)
                elif current_position and current_position.side == PositionSide.SHORT:
                    close_qty = min(exec_amount, float(current_position.quantity or 0.0))
                    prev_realized = float(current_position.realized_pnl or 0.0)
                    closed = position_manager.close_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        close_price=fill_price,
                        quantity=close_qty,
                        account_id=account_id,
                    )
                    if closed:
                        trade_pnl += float(closed.realized_pnl or 0.0) - prev_realized
                    remaining = max(0.0, exec_amount - close_qty)
                    if remaining > 0 and not req.reduce_only:
                        position_manager.open_position(
                            exchange=exchange,
                            symbol=signal.symbol,
                            side=PositionSide.LONG,
                            entry_price=fill_price,
                            quantity=remaining,
                            leverage=leverage,
                            strategy=signal.strategy_name,
                            account_id=account_id,
                            stop_loss=req.stop_loss,
                            take_profit=req.take_profit,
                            trailing_stop_pct=req.trailing_stop_pct,
                            trailing_stop_distance=req.trailing_stop_distance,
                            metadata=self._merge_position_metadata(
                                None,
                                dict(signal.metadata or {}),
                                source="strategy",
                                reduce_only=bool(req.reduce_only),
                                reset_profit_management_state=True,
                            ),
                        )
                else:
                    position_manager.open_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        side=position_side,
                        entry_price=fill_price,
                        quantity=exec_amount,
                        leverage=leverage,
                        strategy=signal.strategy_name,
                        account_id=account_id,
                        stop_loss=req.stop_loss,
                        take_profit=req.take_profit,
                        trailing_stop_pct=req.trailing_stop_pct,
                        trailing_stop_distance=req.trailing_stop_distance,
                        metadata=self._merge_position_metadata(
                            None,
                            dict(signal.metadata or {}),
                            source="strategy",
                            reduce_only=bool(req.reduce_only),
                            reset_profit_management_state=True,
                        ),
                    )
            else:
                if current_position and current_position.side == PositionSide.LONG:
                    close_qty = min(exec_amount, float(current_position.quantity or 0.0))
                    prev_realized = float(current_position.realized_pnl or 0.0)
                    closed = position_manager.close_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        close_price=fill_price,
                        quantity=close_qty,
                        account_id=account_id,
                    )
                    if closed:
                        trade_pnl += float(closed.realized_pnl or 0.0) - prev_realized
                    remaining = max(0.0, exec_amount - close_qty)
                    if remaining > 0 and not req.reduce_only:
                        position_manager.open_position(
                            exchange=exchange,
                            symbol=signal.symbol,
                            side=PositionSide.SHORT,
                            entry_price=fill_price,
                            quantity=remaining,
                            leverage=leverage,
                            strategy=signal.strategy_name,
                            account_id=account_id,
                            stop_loss=req.stop_loss,
                            take_profit=req.take_profit,
                            trailing_stop_pct=req.trailing_stop_pct,
                            trailing_stop_distance=req.trailing_stop_distance,
                            metadata=self._merge_position_metadata(
                                None,
                                dict(signal.metadata or {}),
                                source="strategy",
                                reduce_only=bool(req.reduce_only),
                                reset_profit_management_state=True,
                            ),
                        )
                elif current_position and current_position.side == PositionSide.SHORT:
                    total_qty = current_position.quantity + exec_amount
                    if total_qty > 0:
                        current_position.entry_price = (
                            (current_position.entry_price * current_position.quantity)
                            + (fill_price * exec_amount)
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
                        quantity=exec_amount,
                        leverage=leverage,
                        strategy=signal.strategy_name,
                        account_id=account_id,
                        stop_loss=req.stop_loss,
                        take_profit=req.take_profit,
                        trailing_stop_pct=req.trailing_stop_pct,
                        trailing_stop_distance=req.trailing_stop_distance,
                        metadata=self._merge_position_metadata(
                            None,
                            dict(signal.metadata or {}),
                            source="strategy",
                            reduce_only=bool(req.reduce_only),
                            reset_profit_management_state=True,
                        ),
                    )

            gross_trade_pnl = float(trade_pnl or 0.0)
            net_trade_pnl = gross_trade_pnl - fee_usd - slippage_cost_usd
            risk_manager.record_trade(
                {
                    "symbol": signal.symbol,
                    "exchange": exchange,
                    "strategy": signal.strategy_name,
                    "side": side.value,
                    "signal_type": signal.signal_type.value,
                    "fill_price": float(fill_price or 0.0),
                    "quantity": float(exec_amount or 0.0),
                    "notional": float(exec_amount * fill_price),
                    "pnl": net_trade_pnl,
                    "fee_usd": fee_usd,
                    "slippage_cost_usd": slippage_cost_usd,
                    "order_id": order.id,
                    "strength": float(signal.strength or 0.0),
                    "stop_loss": signal.stop_loss,
                    "take_profit": signal.take_profit,
                    "action": "open_or_add",
                }
            )
            await self._record_live_strategy_trade(
                signal=signal,
                exchange=exchange,
                account_id=account_id,
                side=side.value,
                quantity=exec_amount,
                fill_price=float(fill_price or 0.0),
                order_id=order.id,
                order_status=order.status.value,
                pnl=float(net_trade_pnl or 0.0),
                fee_usd=fee_usd,
                slippage_cost_usd=slippage_cost_usd,
                gross_pnl_usd=float(gross_trade_pnl or 0.0),
                net_pnl_usd=float(net_trade_pnl or 0.0),
                action="open_or_add",
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
            self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
            await self._notify_callbacks("order_executed", result)
            await write_audit(
                GovernanceAuditEvent(
                    module="trading.execution",
                    action="order_executed",
                    status="success",
                    actor="system",
                    role="SYSTEM",
                    trace_id=str(req.params.get("trace_id") or ""),
                    input_payload={
                        "symbol": signal.symbol,
                        "strategy": signal.strategy_name,
                        "side": side.value,
                        "qty": exec_amount,
                        "price": float(fill_price),
                    },
                    output_payload={
                        "order_id": order.id,
                        "status": order.status.value,
                        "filled": float(order.filled or 0.0),
                    },
                    payload_json={
                        "fee_usd": fee_usd,
                        "slippage_cost_usd": slippage_cost_usd,
                        "account_id": account_id,
                    },
                )
            )
            return result
        except Exception as e:
            self._signal_diagnostics["exceptions"] = int(self._signal_diagnostics.get("exceptions", 0)) + 1
            self._signal_diagnostics["last_result"] = {
                "status": "exception",
                "strategy": signal.strategy_name,
                "symbol": signal.symbol,
                "reason": str(e),
            }
            self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
            logger.error(f"Failed to execute signal: {e}")
            return None

    async def _close_position(self, signal: Signal, position_side: PositionSide) -> Optional[Dict[str, Any]]:
        account_id = str(signal.metadata.get("account_id", "main"))
        exchange = account_manager.resolve_exchange(account_id, str(signal.metadata.get("exchange", "binance")))
        trade_policy = self._resolve_strategy_trade_policy(signal.strategy_name, exchange)
        position = await self._resolve_existing_position(
            exchange=exchange,
            symbol=signal.symbol,
            account_id=account_id,
            preferred_side=position_side,
        )
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
                params={
                    "close_reason": signal.signal_type.value,
                    "leverage": float(position.leverage or 1.0),
                    "market_type": str(trade_policy.get("market_type") or ""),
                },
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
            params={
                "close_reason": signal.signal_type.value,
                "leverage": float(position.leverage or 1.0),
                "market_type": str(trade_policy.get("market_type") or ""),
            },
        )
        close_order = await order_manager.create_order(close_request)
        if not close_order:
            last_error = str(order_manager.get_last_error() or "")
            if self._is_reduce_only_rejected(last_error):
                has_exchange_pos, checked = await self._exchange_has_side_position(
                    exchange=exchange,
                    symbol=signal.symbol,
                    side=position_side,
                    min_qty=close_qty * 0.5,
                )
                if checked and not has_exchange_pos:
                    close_price = float(quote_price or signal.price or position.current_price or position.entry_price or 0.0)
                    if close_price <= 0:
                        close_price = float(position.entry_price or 0.0)
                    closed = position_manager.close_position(
                        exchange=exchange,
                        symbol=signal.symbol,
                        close_price=close_price,
                        quantity=close_qty,
                        account_id=account_id,
                    )
                    if closed:
                        logger.warning(
                            f"Reconciled local-only position after reduce-only rejection: "
                            f"strategy={signal.strategy_name} symbol={signal.symbol} side={position_side.value} "
                            f"account_id={account_id} error={last_error}"
                        )
                        result = {
                            "action": "close_position_reconciled",
                            "symbol": signal.symbol,
                            "side": position_side.value,
                            "close_price": close_price,
                            "quantity": float(close_qty or 0.0),
                            "pnl": float(closed.realized_pnl or 0.0),
                            "fee_usd": 0.0,
                            "slippage_cost_usd": 0.0,
                            "exchange": exchange,
                            "account_id": account_id,
                            "strategy": signal.strategy_name,
                            "signal": signal.to_dict(),
                            "order": None,
                            "reason": "exchange_no_position_reduce_only_rejected",
                            "timestamp": datetime.now().isoformat(),
                        }
                        self._signal_diagnostics["last_result"] = {
                            "status": "reconciled",
                            "strategy": signal.strategy_name,
                            "symbol": signal.symbol,
                            "exchange": exchange,
                            "reason": "reduce_only_rejected_exchange_flat",
                        }
                        self._signal_diagnostics["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                        await self._notify_callbacks("order_executed", result)
                        return result
            reject_reason = "平仓下单失败"
            if last_error:
                reject_reason = f"{reject_reason}: {last_error}"
            await order_manager.record_rejected_order(
                request=close_request,
                reason=reject_reason,
                price=quote_price if quote_price > 0 else signal.price,
            )
            return None

        paper_cost = self._consume_paper_order_cost(close_order.id)
        fee_usd = float(paper_cost.get("fee_usd", 0.0) or 0.0)
        slippage_cost_usd = float(paper_cost.get("slippage_cost_usd", 0.0) or 0.0)
        # For live orders, read fee from the exchange order object
        if not self._paper_trading and fee_usd <= 0:
            order_fee = float(getattr(close_order, "fee", 0.0) or 0.0)
            if order_fee > 0:
                fee_usd = order_fee
        close_price = float(close_order.price or signal.price or quote_price or 0.0)
        closed = position_manager.close_position(
            exchange=exchange,
            symbol=signal.symbol,
            close_price=close_price,
            quantity=close_qty,
            account_id=account_id,
        )
        if not closed:
            source = str((getattr(position, "metadata", {}) or {}).get("source") or "").strip().lower()
            if source != "exchange_live":
                return None
            gross_pnl = 0.0
            entry_price = float(getattr(position, "entry_price", 0.0) or 0.0)
            if entry_price > 0 and close_qty > 0:
                if position_side == PositionSide.LONG:
                    gross_pnl = (close_price - entry_price) * close_qty
                else:
                    gross_pnl = (entry_price - close_price) * close_qty
            closed = SimpleNamespace(realized_pnl=gross_pnl)

        risk_manager.record_trade(
            {
                "symbol": signal.symbol,
                "exchange": exchange,
                "strategy": signal.strategy_name,
                "side": signal.signal_type.value,
                "signal_type": signal.signal_type.value,
                "fill_price": float(close_price or 0.0),
                "quantity": float(close_qty or 0.0),
                "pnl": float(closed.realized_pnl or 0.0) - fee_usd - slippage_cost_usd,
                "notional": float(close_price * close_qty),
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
                "order_id": close_order.id,
                "strength": float(signal.strength or 0.0),
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "action": "close",
            }
        )
        gross_close_pnl = float(closed.realized_pnl or 0.0)
        close_pnl = gross_close_pnl - fee_usd - slippage_cost_usd
        await self._record_live_strategy_trade(
            signal=signal,
            exchange=exchange,
            account_id=account_id,
            side=close_side.value,
            quantity=float(close_order.filled or close_qty or 0.0),
            fill_price=float(close_price or 0.0),
            order_id=close_order.id,
            order_status=close_order.status.value,
            pnl=close_pnl,
            fee_usd=fee_usd,
            slippage_cost_usd=slippage_cost_usd,
            gross_pnl_usd=gross_close_pnl,
            net_pnl_usd=close_pnl,
            action="close",
        )

        result = {
            "action": "close_position",
            "symbol": signal.symbol,
            "side": position_side.value,
            "close_price": close_price,
            "quantity": float(close_qty or 0.0),
            "pnl": close_pnl,
            "fee_usd": fee_usd,
            "slippage_cost_usd": slippage_cost_usd,
            "exchange": exchange,
            "account_id": account_id,
            "strategy": signal.strategy_name,
            "signal": signal.to_dict(),
            "order": {
                "id": close_order.id,
                "status": close_order.status.value,
                "price": close_order.price,
                "amount": close_order.amount,
                "filled": close_order.filled,
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
            },
            "timestamp": datetime.now().isoformat(),
        }
        try:
            await audit_logger.log(
                module="trading",
                action="trade_close",
                status="success",
                message=f"closed {signal.symbol} {position_side.value}",
                details={
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "symbol": signal.symbol,
                    "strategy": signal.strategy_name,
                    "exchange": exchange,
                    "side": position_side.value,
                    "pnl": float(result.get("pnl") or 0.0),
                    "fee_usd": float(result.get("fee_usd") or 0.0),
                    "slippage_cost_usd": float(result.get("slippage_cost_usd") or 0.0),
                    "close_price": float(result.get("close_price") or 0.0),
                    "quantity": float(close_qty or 0.0),
                    "notional": float(close_price * close_qty),
                    "account_id": account_id,
                },
            )
        except Exception as audit_err:
            logger.debug(f"trade_close audit log skipped: {audit_err}")
        await self._notify_callbacks("order_executed", result)
        return result

    def _new_conditional_id(self) -> str:
        self._conditional_seq += 1
        return f"cond_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{self._conditional_seq:04d}"
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
            created_at=datetime.now(timezone.utc).isoformat(),
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
        governance_check = await decision_engine.evaluate_order_intent(
            symbol=symbol,
            side=side_lower,
            leverage=float(leverage or 1.0),
            order_value=float(order_value or 0.0),
            account_equity=float(account_equity or 0.0),
            signal_ts=datetime.now(timezone.utc),
            allow_close=closes_existing,
            spread_bps=None,
            timeframe=None,
            source="manual_order",
        )
        if not governance_check.allowed:
            return None

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
            params=dict(params or {}, leverage=float(leverage), trace_id=governance_check.trace_id),
        )
        order = await order_manager.create_order(request)
        if not order:
            return None

        paper_cost = self._consume_paper_order_cost(order.id)
        fee_usd = float(paper_cost.get("fee_usd", 0.0) or 0.0)
        slippage_cost_usd = float(paper_cost.get("slippage_cost_usd", 0.0) or 0.0)
        # For live orders, read fee from the exchange order object
        if not self._paper_trading and fee_usd <= 0:
            order_fee = float(getattr(order, "fee", 0.0) or 0.0)
            if order_fee > 0:
                fee_usd = order_fee
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
            existing_position.metadata = self._merge_position_metadata(
                getattr(existing_position, "metadata", {}) or {},
                None,
                source="manual",
                reduce_only=reduce_only,
                reset_profit_management_state=not bool(reduce_only),
            )

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
                        metadata=self._merge_position_metadata(
                            None,
                            None,
                            source="manual",
                            reduce_only=reduce_only,
                            reset_profit_management_state=True,
                        ),
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
                    metadata=self._merge_position_metadata(
                        None,
                        None,
                        source="manual",
                        reduce_only=reduce_only,
                        reset_profit_management_state=True,
                    ),
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
                        metadata=self._merge_position_metadata(
                            None,
                            None,
                            source="manual",
                            reduce_only=reduce_only,
                            reset_profit_management_state=True,
                        ),
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
                    metadata=self._merge_position_metadata(
                        None,
                        None,
                        source="manual",
                        reduce_only=reduce_only,
                        reset_profit_management_state=True,
                    ),
                )

        gross_trade_pnl = float(trade_pnl or 0.0)
        net_trade_pnl = gross_trade_pnl - fee_usd - slippage_cost_usd
        risk_manager.record_trade(
            {
                "symbol": symbol,
                "exchange": exchange,
                "strategy": strategy,
                "side": side_lower,
                "signal_type": side_lower,
                "fill_price": float(fill_price or 0.0),
                "quantity": float(exec_amount or 0.0),
                "notional": float(exec_amount * fill_price),
                "pnl": net_trade_pnl,
                "fee_usd": fee_usd,
                "slippage_cost_usd": slippage_cost_usd,
                "order_id": order.id,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "action": "manual_order",
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
                "order_id": f"{mode}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}",
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

    async def tighten_profitable_position_protection(
        self,
        *,
        exchange: str,
        symbol: str,
        account_id: str = "main",
        current_price: Optional[float] = None,
        reason: str = "model_feedback_outage",
    ) -> Dict[str, Any]:
        position = position_manager.get_position(exchange, symbol, account_id=account_id)
        if position is None:
            return {"applied": False, "reason": "no_local_position"}

        price = self._safe_nonnegative_float(current_price, 0.0)
        if price <= 0:
            price = await self._resolve_price(exchange, symbol, getattr(position, "current_price", None))
        if price <= 0:
            price = self._safe_nonnegative_float(getattr(position, "current_price", 0.0), 0.0)
        if price <= 0:
            return {"applied": False, "reason": "price_unavailable"}

        position_manager.update_position_price(
            exchange=exchange,
            symbol=symbol,
            current_price=price,
            account_id=account_id,
        )
        position = position_manager.get_position(exchange, symbol, account_id=account_id) or position
        profit_pct = self._position_profit_pct(position)
        if profit_pct <= 0:
            return {"applied": False, "reason": "position_not_profitable", "profit_pct": float(profit_pct)}

        metadata = self._effective_profit_management_metadata(position)
        if metadata.get("outage_protection_enabled") is False:
            return {"applied": False, "reason": "outage_protection_disabled", "profit_pct": float(profit_pct)}

        base_lock_pct = self._safe_protective_pct(metadata.get("profit_protect_lock_pct"))
        if base_lock_pct is None:
            base_lock_pct = min(0.01, max(0.0005, profit_pct * 0.6))
        dynamic_lock_pct = min(float(base_lock_pct), max(0.0002, profit_pct * 0.75))

        base_trailing_pct = self._safe_protective_pct(metadata.get("outage_tight_trailing_stop_pct"))
        if base_trailing_pct is None:
            base_trailing_pct = self._safe_protective_pct(metadata.get("post_partial_trailing_stop_pct"))
        if base_trailing_pct is None:
            base_trailing_pct = min(0.01, max(0.001, profit_pct * 0.5))
        dynamic_trailing_pct = min(float(base_trailing_pct), max(0.0005, profit_pct * 0.5))

        applied_fields: List[str] = []
        lock_stop = self._entry_lock_stop_price(position, dynamic_lock_pct)
        if self._apply_position_stop_loss(
            position,
            stop_price=lock_stop,
            metadata_flag="outage_protection_armed",
            event=reason,
        ):
            applied_fields.append("stop_loss")
        if self._apply_position_trailing_pct(
            position,
            trailing_pct=dynamic_trailing_pct,
            current_price=price,
            event=reason,
        ):
            applied_fields.append("trailing_stop_pct")

        if applied_fields:
            metadata = dict(getattr(position, "metadata", {}) or {})
            now_iso = datetime.now(timezone.utc).isoformat()
            metadata["outage_protection_armed"] = True
            metadata["outage_protection_armed_at"] = metadata.get("outage_protection_armed_at") or now_iso
            metadata["outage_protection_last_applied_at"] = now_iso
            metadata["outage_protection_reason"] = reason
            position.metadata = metadata
            logger.warning(
                "Outage protection tightened profitable position "
                f"symbol={symbol} exchange={exchange} account_id={account_id} "
                f"profit_pct={profit_pct:.4%} fields={','.join(applied_fields)}"
            )
            await self._notify_callbacks(
                "profit_management_outage_protection",
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "account_id": account_id,
                    "profit_pct": float(profit_pct),
                    "applied_fields": list(applied_fields),
                    "reason": reason,
                },
            )
            return {
                "applied": True,
                "reason": "outage_protection_armed",
                "profit_pct": float(profit_pct),
                "applied_fields": list(applied_fields),
                "stop_loss": float(position.stop_loss or 0.0) if position.stop_loss is not None else None,
                "trailing_stop_pct": float(position.trailing_stop_pct or 0.0)
                if position.trailing_stop_pct is not None
                else None,
            }

        return {
            "applied": False,
            "reason": "already_protected",
            "profit_pct": float(profit_pct),
            "stop_loss": float(position.stop_loss or 0.0) if position.stop_loss is not None else None,
            "trailing_stop_pct": float(position.trailing_stop_pct or 0.0)
            if position.trailing_stop_pct is not None
            else None,
        }

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
            updated_position = position_manager.update_position_price(
                exchange=pos.exchange,
                symbol=pos.symbol,
                current_price=px,
                account_id=pos.account_id,
            )
            pos = updated_position or pos
            pos = await self._apply_position_profit_management(pos, px)
            pos = position_manager.get_position(pos.exchange, pos.symbol, account_id=pos.account_id) or pos

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
        now = datetime.now(timezone.utc)
        if self._last_bg_check_at and (now - self._last_bg_check_at).total_seconds() < self._bg_check_interval_seconds:
            return
        self._last_bg_check_at = now
        await self._reconcile_local_positions_with_exchange()
        await self._check_conditional_orders()
        await self._check_protective_orders()

    async def _process_signal_queue(self) -> None:
        queue = self._ensure_signal_queue()
        while self._running:
            try:
                signal = await asyncio.wait_for(queue.get(), timeout=1.0)
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
        self._paper_trading = runtime_state.is_paper_mode()
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
        queue = self._signal_queue
        if queue is not None:
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue_cleared += 1
                except Exception:
                    break
        self._last_bg_check_at = None
        self._last_live_reconcile_at = None
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
        queue = self._signal_queue
        if queue is None:
            return 0
        return queue.qsize()

    def get_signal_diagnostics(self) -> Dict[str, Any]:
        return dict(self._signal_diagnostics or {})

    async def prime_live_equity(self) -> None:
        await self._prime_live_equity()


execution_engine = ExecutionEngine()
