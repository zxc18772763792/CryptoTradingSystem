"""
Order management module.
"""
import asyncio
import re
import time
import uuid
import json
import hashlib
from datetime import datetime, timezone
import math
from typing import Any, Awaitable, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

from loguru import logger

from config.settings import settings
from core.exchanges.exchange_manager import exchange_manager
from core.governance.decision_engine import decision_engine
from core.marketdata.runtime_price_provider import get_realtime_price
from core.risk.risk_manager import risk_manager
from core.exchanges.base_exchange import Order, OrderSide, OrderType, OrderStatus
from core.trading.binance_rest import (
    binance_ccxt_symbol,
    binance_market_symbol,
    binance_signed_request,
)
from core.utils.asset_valuation import STABLE_COINS, build_currency_usd_quotes

# A live tick further than this from the caller's price is more likely a symbol/scale mismatch
# (e.g. a 1000x contract) than a real move; the paper fill then keeps the request price.
_PAPER_LIVE_PRICE_MAX_DEVIATION = 0.15


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
    # Binance newClientOrderId allows A-Za-z0-9_-.|, max length 36.
    _CLIENT_ORDER_ID_MAX_LEN = 36
    _CLIENT_ORDER_ID_TTL_SEC = 60.0
    _CLIENT_ORDER_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9_\-\.]")

    def __init__(self):
        self._orders: Dict[str, Order] = {}
        self._pending_orders: Dict[str, OrderRequest] = {}
        self._order_callbacks: List[Callable[[Order, str], Awaitable[None]]] = []
        self._order_meta: Dict[str, Dict[str, Any]] = {}
        self._paper_trading: bool = True
        self._paper_order_seq: int = 0
        self._last_error: str = ""
        # client_order_id -> created_at monotonic ts; 60s TTL for idempotency dedup.
        self._client_order_ids: Dict[str, float] = {}
        self._client_order_seq: int = 0
        self._client_order_lock = asyncio.Lock()

    @staticmethod
    def _normalize_mode(value: Any, default: str = "paper") -> str:
        text = str(value or default).strip().lower()
        return "live" if text == "live" else "paper"

    def _cache_live_order(self, order: Order, metadata: Dict[str, Any]) -> Order:
        account_id = str(metadata.get("account_id") or "main")
        identity = json.dumps([account_id, str(order.exchange).lower(), order.symbol, str(order.id)], separators=(",", ":"))
        key = "live_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        order.account_id = account_id
        order.cache_key = key
        self._orders[key] = order
        self._order_meta[key] = {**self._order_meta.get(key, {}), **metadata,
                                 "account_id": account_id, "mode": "live", "exchange_order_id": str(order.id)}
        return order

    def _order_key(self, order_id: str, *, account_id: Optional[str] = None,
                   exchange: Optional[str] = None, symbol: Optional[str] = None) -> Optional[str]:
        matches = []
        candidates = [(order_id, self._orders[order_id])] if order_id in self._orders else self._orders.items()
        for key, order in candidates:
            if order_id not in {key, str(order.id)}:
                continue
            meta = self._order_meta.get(key, {})
            if account_id is not None and str(meta.get("account_id") or "main") != str(account_id):
                continue
            if exchange and str(order.exchange).lower() != str(exchange).lower():
                continue
            if symbol and str(order.symbol).split(":")[0] != str(symbol).split(":")[0]:
                continue
            matches.append(key)
        # Never select a different account based on insertion order.
        return matches[0] if len(matches) == 1 else None

    def _resolve_request_mode(self, request: OrderRequest) -> str:
        fallback_mode = "paper" if self._paper_trading else "live"
        try:
            from core.trading.account_manager import account_manager

            params = dict(request.params or {})
            explicit_mode = params.get("trading_mode") or params.get("runtime_mode") or params.get("mode")
            if explicit_mode in {"paper", "live"}:
                return self._normalize_mode(explicit_mode)
            account_mode = account_manager.get_account_mode(
                request.account_id,
                default=fallback_mode,
            )
            if account_mode in {"paper", "live"}:
                return self._normalize_mode(account_mode)
        except Exception:
            pass
        return fallback_mode

    def _resolve_operation_mode(
        self,
        *,
        order_id: Optional[str] = None,
        trading_mode: Optional[str] = None,
    ) -> str:
        """Resolve query/cancel routing without relying on mutable global mode.

        Persisted order metadata is authoritative for an existing order.  The
        explicit mode is used for collection queries and for exchange orders
        that have not yet been cached locally.
        """
        if order_id:
            order_id = self._order_key(order_id) or order_id
            stored_mode = str((self._order_meta.get(order_id) or {}).get("mode") or "").strip().lower()
            if stored_mode in {"paper", "live"}:
                return stored_mode
        explicit_mode = str(trading_mode or "").strip().lower()
        if explicit_mode in {"paper", "live"}:
            return explicit_mode
        return "paper" if self._paper_trading else "live"

    def _operation_mode_conflicts(
        self,
        order_id: str,
        trading_mode: Optional[str],
    ) -> bool:
        requested = str(trading_mode or "").strip().lower()
        order_id = self._order_key(order_id) or order_id
        stored = str((self._order_meta.get(order_id) or {}).get("mode") or "").strip().lower()
        return requested in {"paper", "live"} and stored in {"paper", "live"} and requested != stored

    def _request_meta(self, request: OrderRequest) -> Dict[str, Any]:
        resolved_mode = self._resolve_request_mode(request)
        return {
            "strategy": request.strategy,
            "account_id": request.account_id,
            "mode": resolved_mode,
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

    @staticmethod
    def _ts_sort_key(ts: Any) -> datetime:
        """Coerce a possibly-naive order timestamp into a tz-aware UTC key."""
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts
        return datetime.min.replace(tzinfo=timezone.utc)

    @staticmethod
    def _normalize_leverage(value: Any, default: int = 1) -> int:
        try:
            lev = float(value)
            if math.isnan(lev) or math.isinf(lev):
                lev = float(default)
        except Exception:
            lev = float(default)
        # Binance USDT-M futures leverage is an integer between 1 and 125.
        return int(max(1, min(125, round(lev))))

    @staticmethod
    def _resolve_cached_exchange(exchange_name: str, account_id: Optional[str] = None):
        getter = getattr(exchange_manager, "get_exchange")
        try:
            return getter(exchange_name, account_id=account_id)
        except TypeError:
            return getter(exchange_name)

    @staticmethod
    async def _ensure_exchange_connector(exchange_name: str, account_id: Optional[str] = None):
        ensure = getattr(exchange_manager, "ensure_exchange", None)
        if callable(ensure):
            try:
                return await ensure(exchange_name, account_id=account_id)
            except TypeError:
                return await ensure(exchange_name)
        return OrderManager._resolve_cached_exchange(exchange_name, account_id=account_id)

    async def _sync_binance_futures_leverage(
        self,
        symbol: str,
        leverage: Any,
        *,
        account_id: str = "main",
    ) -> bool:
        target_leverage = self._normalize_leverage(leverage, default=1)
        try:
            market_symbol = str(binance_market_symbol(symbol) or "").strip().upper()
            if not market_symbol:
                self._last_error = f"binance leverage sync failed: invalid symbol {symbol!r}"
                return False
            await binance_signed_request(
                "POST",
                "/fapi/v1/leverage",
                host="fapi",
                params={
                    "symbol": market_symbol,
                    "leverage": target_leverage,
                },
                timeout_sec=5.0,
                account_id=account_id,
            )
            logger.info(
                f"Binance futures leverage synced: symbol={market_symbol} "
                f"target={target_leverage}x account_id={account_id}"
            )
            return True
        except Exception as e:
            self._last_error = (
                f"binance leverage sync failed: symbol={symbol} "
                f"target={target_leverage}x error={e}"
            )
            logger.error(self._last_error)
            return False

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
        if self._paper_trading == enabled:
            return
        self._paper_trading = enabled
        logger.info(f"Paper trading mode: {enabled}")

    def register_callback(self, callback: Callable[[Order, str], Awaitable[None]]) -> None:
        self._order_callbacks.append(callback)

    async def _notify_callbacks(self, order: Order, event: str) -> None:
        for callback in self._order_callbacks:
            try:
                await callback(order, event)
            except Exception as e:
                logger.error(f"Order callback error: {e}")

    async def create_order(self, request: OrderRequest) -> Optional[Order]:
        self._last_error = ""
        self._evict_terminal_orders()
        if self._resolve_request_mode(request) == "paper":
            return await self._create_paper_order(request)
        return await self._create_real_order(request)

    def get_last_error(self) -> str:
        return str(self._last_error or "")

    async def _evaluate_order_governance(
        self,
        request: OrderRequest,
        *,
        order_value: float,
        params: Dict[str, Any],
        source: str,
    ):
        request_mode = self._resolve_request_mode(request)
        try:
            report = risk_manager.get_risk_report(scope=request_mode)
        except TypeError:
            # Preserve compatibility with lightweight test/extension stubs
            # that implement the historical zero-argument method.
            report = risk_manager.get_risk_report()
        equity = float((report.get("equity") or {}).get("current") or 0.0)
        return await decision_engine.evaluate_order_intent(
            symbol=request.symbol,
            side=request.side.value,
            leverage=float(params.get("leverage", 1.0) or 1.0),
            order_value=float(order_value or 0.0),
            account_equity=float(equity or 0.0),
            signal_ts=datetime.now(timezone.utc),
            allow_close=bool(request.reduce_only),
            spread_bps=None,
            timeframe=str(params.get("timeframe") or ""),
            source=source,
        )

    def _governance_rejection_reason(
        self,
        governance_check,
        request: OrderRequest,
    ) -> str:
        if governance_check is None:
            return ""
        if not governance_check.allowed:
            return f"governance blocked: {governance_check.reason}"
        if governance_check.reduce_only and not bool(request.reduce_only):
            return "governance reduce_only enabled"
        return ""

    @staticmethod
    def _governance_already_checked(params: Dict[str, Any]) -> bool:
        return bool(params.get("governance_prechecked") and params.get("trace_id"))

    def _next_paper_order_id(self) -> str:
        # uuid4 avoids collisions when multiple processes share runtime state
        # while still being readable enough for logs.
        self._paper_order_seq = (self._paper_order_seq + 1) % 1000000
        return f"paper_{uuid.uuid4().hex}"

    def _next_rejected_order_id(self) -> str:
        self._paper_order_seq = (self._paper_order_seq + 1) % 1000000
        return f"rejected_{uuid.uuid4().hex}"

    def _prune_expired_client_order_ids(self, now: Optional[float] = None) -> None:
        ts_now = float(now if now is not None else time.monotonic())
        expired = [
            cid for cid, born in self._client_order_ids.items()
            if ts_now - born > self._CLIENT_ORDER_ID_TTL_SEC
        ]
        for cid in expired:
            self._client_order_ids.pop(cid, None)

    def _sanitize_client_order_prefix(self, strategy: Optional[str]) -> str:
        text = self._CLIENT_ORDER_ID_SAFE_RE.sub("", str(strategy or "sys"))
        text = text[:8] or "sys"
        return text

    async def _allocate_client_order_id(self, strategy: Optional[str]) -> str:
        """Generate a fresh newClientOrderId and reserve it for 60s idempotency.

        Format: <strategy[:8]>-<ts_ms>-<seq>, truncated to 36 chars to fit
        Binance's spot/futures clientOrderId limits. The seq counter ensures
        sub-millisecond collisions cannot reuse an in-flight id.
        """
        async with self._client_order_lock:
            self._prune_expired_client_order_ids()
            prefix = self._sanitize_client_order_prefix(strategy)
            now_ms = int(time.time() * 1000)
            for _ in range(64):
                self._client_order_seq = (self._client_order_seq + 1) % 1_000_000
                candidate = f"{prefix}-{now_ms}-{self._client_order_seq:06d}"
                if len(candidate) > self._CLIENT_ORDER_ID_MAX_LEN:
                    # Trim the prefix first to keep the unique suffix intact.
                    overflow = len(candidate) - self._CLIENT_ORDER_ID_MAX_LEN
                    trimmed_prefix = prefix[: max(1, len(prefix) - overflow)]
                    candidate = f"{trimmed_prefix}-{now_ms}-{self._client_order_seq:06d}"
                    candidate = candidate[: self._CLIENT_ORDER_ID_MAX_LEN]
                if candidate not in self._client_order_ids:
                    self._client_order_ids[candidate] = time.monotonic()
                    return candidate
                # Collision (extremely unlikely): rotate the timestamp slightly.
                now_ms += 1
            # Fall back to a uuid suffix if every seq slot is taken in the
            # same millisecond — should not happen, but stay safe.
            fallback = f"{prefix}-{uuid.uuid4().hex}"[: self._CLIENT_ORDER_ID_MAX_LEN]
            self._client_order_ids[fallback] = time.monotonic()
            return fallback

    def _is_client_order_id_active(self, client_order_id: Optional[str]) -> bool:
        """Return True if a clientOrderId is already in flight within TTL."""
        if not client_order_id:
            return False
        self._prune_expired_client_order_ids()
        return str(client_order_id) in self._client_order_ids

    def _release_client_order_id(self, client_order_id: Optional[str]) -> None:
        """Drop a reservation immediately (only safe when the order was rejected
        before execution — see _is_ambiguous_submit_error)."""
        if not client_order_id:
            return
        self._client_order_ids.pop(str(client_order_id), None)

    @staticmethod
    def _is_ambiguous_submit_error(error: Exception) -> bool:
        """True when a submit failure leaves the order's exchange state UNKNOWN.

        Network/timeout failures may mean the request reached the exchange and
        filled even though we never saw the response. In that case the
        clientOrderId MUST stay reserved so a blind retry cannot create a second
        fill (the exchange also rejects a duplicate id). Definitive
        pre-execution rejections (bad params, insufficient funds, throttle,
        auth) return False — those never executed, so the id is safe to release.
        """
        if isinstance(error, (asyncio.TimeoutError, TimeoutError, ConnectionError)):
            return True
        ambiguous_names = {
            "RequestTimeout",
            "NetworkError",
            "ExchangeNotAvailable",
            "ReadTimeout",
            "ReadTimeoutError",
            "ConnectTimeout",
            "ConnectionError",
        }
        for cls in type(error).mro():
            if cls.__name__ in ambiguous_names:
                return True
        message = str(error or "").lower()
        return any(
            token in message
            for token in (
                "timeout",
                "timed out",
                "temporarily unavailable",
                "server disconnected",
                "connection reset",
                "connection aborted",
            )
        )

    async def _paper_live_price(self, request: OrderRequest, connector: Any) -> Any:
        """Fresh market price for a paper fill (hub tick, else REST ticker); None when unavailable."""
        try:
            price_read = await get_realtime_price(
                request.exchange,
                request.symbol,
                connector=connector,
                max_age_sec=float(getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0) or 10.0),
                allow_rest_fallback=True,
            )
        except Exception as e:
            logger.warning(
                f"[PAPER] Failed to fetch ticker for {request.symbol} "
                f"on {request.exchange}: {e}"
            )
            return None
        return price_read if price_read.ok else None

    async def _create_paper_order(self, request: OrderRequest) -> Order:
        order_id = self._next_paper_order_id()
        requested_price = float(request.price or 0.0)
        fill_price = requested_price
        fill_source = "request" if requested_price > 0 else "none"
        live_read = None

        # A market order fills at the market, not at the price the caller carried (often the close
        # of a bar minutes old): take a fresh tick and keep the request price only as a fallback.
        if request.order_type == OrderType.MARKET or fill_price <= 0:
            connector = self._resolve_cached_exchange(request.exchange, account_id=request.account_id)
            live_read = await self._paper_live_price(request, connector)
            live_price = float(live_read.price) if live_read is not None else 0.0
            if live_price > 0 and requested_price > 0 and (
                abs(live_price / requested_price - 1.0) > _PAPER_LIVE_PRICE_MAX_DEVIATION
            ):
                fill_source = "request_live_implausible"
                logger.warning(
                    f"[PAPER] live price {live_price} for {request.symbol} is more than "
                    f"{_PAPER_LIVE_PRICE_MAX_DEVIATION:.0%} from the request price {requested_price}; "
                    f"filling at the request price"
                )
            elif live_price > 0:
                fill_price = live_price
                fill_source = "live"
            elif requested_price > 0:
                fill_source = "request_fallback"
            if fill_price <= 0 and connector:
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
                        fill_source = "usd_quotes"
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
        params = dict(request.params or {})
        params.setdefault("leverage", 1.0)
        governance_check = None
        rejection_reason = ""
        if not self._governance_already_checked(params):
            governance_check = await self._evaluate_order_governance(
                request,
                order_value=notional_usd,
                params=params,
                source="order_manager_paper_submit",
            )
            rejection_reason = self._governance_rejection_reason(governance_check, request)
        if rejection_reason:
            self._last_error = rejection_reason
            rejected = await self.record_rejected_order(
                request,
                reason=rejection_reason,
                price=fill_price,
            )
            self._order_meta.setdefault(rejected.id, {}).update(
                {
                    "governance_trace_id": governance_check.trace_id,
                    "governance_action": getattr(governance_check, "action", ""),
                    "governance_reason": getattr(governance_check, "reason", ""),
                }
            )
            return rejected

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
            timestamp=datetime.now(timezone.utc),
            exchange=request.exchange,
        )

        self._orders[order_id] = order
        meta = self._request_meta(request)
        meta.update(
            {
                "paper": True,
                "paper_reference_price": round(reference_price, 8) if reference_price > 0 else 0.0,
                "paper_fill_source": fill_source,
                "paper_requested_price": round(requested_price, 8),
                # signed: + means the caller's price sat above the live market at submit time
                "paper_request_vs_live_bps": (
                    round((requested_price / float(live_read.price) - 1.0) * 10000.0, 2)
                    if live_read is not None and requested_price > 0
                    else None
                ),
                "paper_live_price_source": str(live_read.source) if live_read is not None else "",
                "paper_live_price_age_ms": live_read.age_ms if live_read is not None else None,
                "paper_live_bid": live_read.bid if live_read is not None else None,
                "paper_live_ask": live_read.ask if live_read is not None else None,
                "paper_fee_rate": round(fee_rate, 8),
                "paper_fee_usd": round(fee_usd, 8),
                "paper_slippage_bps": round(slippage_bps, 4),
                "paper_slippage_rate": round(slippage_rate, 8),
                "paper_slippage_cost_usd": round(slippage_cost_usd, 8),
                "paper_notional_usd": round(notional_usd, 8),
                "governance_trace_id": (
                    getattr(governance_check, "trace_id", None)
                    or params.get("trace_id")
                    or ""
                ),
                "governance_action": (
                    getattr(governance_check, "action", None)
                    or ("prechecked" if self._governance_already_checked(params) else "")
                ),
                "governance_reason": getattr(governance_check, "reason", ""),
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
        exchange = await self._ensure_exchange_connector(request.exchange, account_id=request.account_id)
        if not exchange:
            self._last_error = (
                f"exchange connector unavailable: exchange={request.exchange} "
                f"account_id={request.account_id}"
            )
            logger.error(self._last_error)
            return None

        client_order_id: Optional[str] = None
        try:
            params = dict(request.params or {})
            requested_leverage = self._normalize_leverage(params.get("leverage", 1.0), default=1)
            params["leverage"] = float(requested_leverage)
            post_only = bool(params.get("post_only") or params.get("postOnly"))
            if post_only:
                params["postOnly"] = True

            # Idempotency: inject a fresh newClientOrderId for every live submit and
            # keep it reserved for 60s. If the caller supplied one (e.g. retry of a
            # known order), reuse it but reject if still in-flight to prevent
            # accidental double-fills from upstream retries.
            supplied_coid = (
                params.get("newClientOrderId")
                or params.get("clientOrderId")
                or params.get("client_order_id")
            )
            if supplied_coid:
                supplied_coid_str = str(supplied_coid)
                if self._is_client_order_id_active(supplied_coid_str):
                    self._last_error = (
                        f"duplicate client_order_id detected within idempotency window: "
                        f"{supplied_coid_str}"
                    )
                    logger.warning(self._last_error)
                    return None
                client_order_id = supplied_coid_str
                self._client_order_ids[client_order_id] = time.monotonic()
            else:
                client_order_id = await self._allocate_client_order_id(request.strategy)
                if request.params is None:
                    request.params = {}
                request.params["newClientOrderId"] = client_order_id
                request.params["clientOrderId"] = client_order_id
            params["newClientOrderId"] = client_order_id
            params["clientOrderId"] = client_order_id
            order_price = request.price
            if request.order_type == OrderType.MARKET:
                order_price = None
            # Market orders carry no limit price — estimate notional from the
            # latest ticker so governance/risk size checks are not bypassed
            # (order_value=0 would fail-open through every cap).
            valuation_price = float(request.price or 0.0)
            if valuation_price <= 0:
                fail_closed = bool(
                    str(getattr(settings, "MARKET_WS_MODE", "off") or "off").strip().lower() == "strategy_primary"
                    and bool(getattr(settings, "MARKET_WS_FAIL_CLOSED_FOR_LIVE", True))
                )
                try:
                    price_read = await get_realtime_price(
                        request.exchange,
                        request.symbol,
                        connector=exchange,
                        max_age_sec=float(getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0) or 10.0),
                        allow_rest_fallback=True,
                        fail_closed=fail_closed,
                    )
                    valuation_price = float(price_read.price or 0.0) if price_read.ok else 0.0
                except Exception as e:
                    if fail_closed:
                        raise
                    logger.warning(
                        f"order_manager: failed to resolve market price for "
                        f"{request.symbol}: {e}"
                    )
            order_value = abs(float(request.amount or 0.0) * valuation_price)
            governance_check = await self._evaluate_order_governance(
                request,
                order_value=order_value,
                params=params,
                source="order_manager_real_submit",
            )
            rejection_reason = self._governance_rejection_reason(governance_check, request)
            if rejection_reason:
                self._last_error = rejection_reason
                return None
            params.setdefault("trace_id", governance_check.trace_id)
            # Binance/major CEX normal MARKET/LIMIT endpoints reject stop-loss / take-profit
            # attachment params (e.g. -4120). Keep these for true trigger/conditional orders only.
            allow_trigger_params = (
                request.order_mode in {"conditional"}
                or request.order_type
                in {
                    OrderType.STOP_LOSS,
                    OrderType.STOP_LOSS_LIMIT,
                    OrderType.TAKE_PROFIT,
                    OrderType.TAKE_PROFIT_LIMIT,
                }
            )
            if allow_trigger_params:
                if request.stop_loss is not None:
                    params.setdefault("stopLossPrice", float(request.stop_loss))
                    params.setdefault("stopPrice", float(request.stop_loss))
                if request.take_profit is not None:
                    params.setdefault("takeProfitPrice", float(request.take_profit))
                if request.trigger_price is not None:
                    params.setdefault("triggerPrice", float(request.trigger_price))
            if request.reduce_only:
                params.setdefault("reduceOnly", True)

            market_type = str(params.get("market_type") or "").strip().lower()
            binance_sandbox_futures = (
                str(request.exchange or "").lower() == "binance"
                and market_type in {"future", "futures", "swap", "perp", "perpetual", "contract"}
                and bool(getattr(getattr(exchange, "config", None), "sandbox", False))
            )
            if binance_sandbox_futures and not bool(request.reduce_only):
                # The production fast REST leverage endpoint is unavailable in sandbox.
                client = await exchange._ensure_client()
                await client.set_leverage(requested_leverage, request.symbol)
                params.pop("leverage", None)
            is_binance_futures = (
                str(request.exchange or "").lower() == "binance"
                and market_type in {"future", "futures", "swap", "perp", "perpetual", "contract"}
                and not bool(getattr(getattr(exchange, "config", None), "sandbox", False))
                and str(request.symbol).split(":")[0].upper().endswith(("/USDT", "/USDC"))
            )
            if is_binance_futures and not bool(request.reduce_only):
                synced = await self._sync_binance_futures_leverage(
                    symbol=request.symbol,
                    leverage=requested_leverage,
                    account_id=request.account_id,
                )
                if not synced:
                    return None

            if (
                is_binance_futures
                and request.order_type in {OrderType.MARKET, OrderType.LIMIT}
            ):
                try:
                    # Binance rejects prices/quantities that are not aligned to
                    # the symbol tick/step size (-4014 "Price not increased by
                    # tick size", -1111 precision). The ccxt client carries the
                    # loaded market filters, so use it to snap to precision.
                    fmt_client = getattr(exchange, "_client", None)

                    def _fmt(method: str, value: float) -> float:
                        if fmt_client is None:
                            return float(value)
                        try:
                            return float(
                                getattr(fmt_client, method)(request.symbol, value)
                            )
                        except Exception:
                            return float(value)

                    payload_amount = _fmt("amount_to_precision", request.amount)
                    raw_payload: Dict[str, Any] = {
                        "symbol": binance_market_symbol(request.symbol),
                        "side": request.side.value.upper(),
                        "type": request.order_type.value.upper(),
                        "quantity": payload_amount,
                        "newOrderRespType": "RESULT",
                        "newClientOrderId": client_order_id,
                    }
                    if order_price is not None and request.order_type == OrderType.LIMIT:
                        raw_payload["price"] = _fmt("price_to_precision", float(order_price))
                        raw_payload["timeInForce"] = "GTX" if post_only else "GTC"
                    if request.reduce_only:
                        raw_payload["reduceOnly"] = "true"
                    raw_order = await binance_signed_request(
                        "POST",
                        "/fapi/v1/order",
                        host="fapi",
                        params=raw_payload,
                        timeout_sec=8.0,
                        account_id=request.account_id,
                    )
                    status_text = str((raw_order or {}).get("status") or "").upper()
                    status_map = {
                        "NEW": OrderStatus.OPEN,
                        "PARTIALLY_FILLED": OrderStatus.OPEN,
                        "FILLED": OrderStatus.CLOSED,
                        "CANCELED": OrderStatus.CANCELED,
                        "CANCELLED": OrderStatus.CANCELED,
                        "EXPIRED": OrderStatus.EXPIRED,
                        "REJECTED": OrderStatus.REJECTED,
                    }
                    fill_price = self._safe_nonnegative_float(
                        (raw_order or {}).get("avgPrice"),
                        self._safe_nonnegative_float((raw_order or {}).get("price"), 0.0),
                    )
                    amount = self._safe_nonnegative_float((raw_order or {}).get("origQty"), request.amount)
                    filled = self._safe_nonnegative_float((raw_order or {}).get("executedQty"), 0.0)
                    order = Order(
                        id=str((raw_order or {}).get("orderId") or ""),
                        symbol=binance_ccxt_symbol(str((raw_order or {}).get("symbol") or ""), futures=True),
                        side=request.side,
                        type=request.order_type,
                        price=float(fill_price or 0.0),
                        amount=float(amount or 0.0),
                        filled=float(filled or 0.0),
                        remaining=max(0.0, float(amount or 0.0) - float(filled or 0.0)),
                        cost=self._safe_nonnegative_float((raw_order or {}).get("cumQuote"), float(fill_price or 0.0) * float(filled or 0.0)),
                        status=status_map.get(status_text, OrderStatus.OPEN),
                        timestamp=datetime.fromtimestamp(
                            self._safe_nonnegative_float((raw_order or {}).get("updateTime"), 0.0) / 1000.0,
                            tz=timezone.utc,
                        ) if self._safe_nonnegative_float((raw_order or {}).get("updateTime"), 0.0) > 0 else datetime.now(timezone.utc),
                        exchange=request.exchange,
                    )
                    meta_payload = self._request_meta(request)
                    meta_payload["client_order_id"] = client_order_id
                    self._cache_live_order(order, meta_payload)
                    logger.info(
                        f"Fast Binance futures order created: {order.id} "
                        f"{request.side.value} {request.amount} {request.symbol} "
                        f"@ {order_price} lev={requested_leverage}x"
                    )
                    await self._notify_callbacks(order, "created")
                    return order
                except Exception as fast_err:
                    if self._is_ambiguous_submit_error(fast_err):
                        logger.warning(
                            f"Fast Binance futures order path failed ambiguously; "
                            f"skipping ccxt fallback because exchange state is unknown: {fast_err}"
                        )
                        raise
                    logger.warning(
                        f"Fast Binance futures order path failed definitively, "
                        f"fallback to ccxt: {fast_err}"
                    )

            order = await exchange.create_order(
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                amount=request.amount,
                price=order_price,
                params=params,
            )

            meta_payload = self._request_meta(request)
            meta_payload["client_order_id"] = client_order_id
            self._cache_live_order(order, meta_payload)
            logger.info(
                f"Order created: {order.id} "
                f"{request.side.value} {request.amount} {request.symbol} "
                f"@ {order_price} lev={requested_leverage}x"
            )

            await self._notify_callbacks(order, "created")
            return order
        except Exception as e:
            self._last_error = str(e)
            # Idempotency on failure:
            #  • Ambiguous network/timeout → the order MAY have reached the
            #    exchange and filled. KEEP the clientOrderId reserved so a blind
            #    retry is blocked (the exchange also rejects a duplicate id).
            #    This prevents double-fills; the reservation auto-expires after
            #    the TTL, by which point the order should be reconciled.
            #  • Definitive pre-execution rejection → the order never executed,
            #    so release the id for honest retries.
            if self._is_ambiguous_submit_error(e):
                logger.critical(
                    f"order submit AMBIGUOUS — exchange state UNKNOWN; holding "
                    f"client_order_id={client_order_id} reserved for "
                    f"{self._CLIENT_ORDER_ID_TTL_SEC:.0f}s, reconcile before retry. "
                    f"exchange={request.exchange} symbol={request.symbol} "
                    f"side={request.side.value} amount={request.amount} error={e}"
                )
            else:
                self._release_client_order_id(client_order_id)
            logger.error(
                f"Failed to create order: exchange={request.exchange} symbol={request.symbol} "
                f"type={request.order_type.value} side={request.side.value} amount={request.amount} "
                f"price={order_price} error={e}"
            )
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
            timestamp=datetime.now(timezone.utc),
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
        self, order_id: str, symbol: str, exchange: str = "binance",
        trading_mode: Optional[str] = None, account_id: Optional[str] = None,
    ) -> bool:
        key = self._order_key(order_id, account_id=account_id, exchange=exchange, symbol=symbol)
        if key is None or self._operation_mode_conflicts(key, trading_mode):
            logger.error("Refused unknown, ambiguous or mode-conflicting order cancellation")
            return False
        if self._resolve_operation_mode(order_id=key, trading_mode=trading_mode) == "paper":
            return await self._cancel_paper_order(key)
        order = self._orders[key]
        connector = self._resolve_cached_exchange(exchange, account_id=self._order_meta[key].get("account_id"))
        if not connector:
            return False
        try:
            success = await connector.cancel_order(order.id, order.symbol)
            if success:
                order.status = OrderStatus.CANCELED
                await self._notify_callbacks(order, "canceled")
            return success
        except Exception as exc:
            logger.error(f"Failed to cancel order: {exc}")
            return False

    async def _cancel_paper_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id].status = OrderStatus.CANCELED
            logger.info(f"[PAPER] Order cancelled: {order_id}")
            await self._notify_callbacks(self._orders[order_id], "canceled")
            return True
        return False

    async def get_order(
        self, order_id: str, symbol: str, exchange: str = "binance",
        trading_mode: Optional[str] = None, account_id: Optional[str] = None,
    ) -> Optional[Order]:
        key = self._order_key(order_id, account_id=account_id, exchange=exchange, symbol=symbol)
        if key is None or self._operation_mode_conflicts(key, trading_mode):
            return None
        if self._resolve_operation_mode(order_id=key, trading_mode=trading_mode) == "paper":
            return self._orders.get(key)
        cached = self._orders[key]
        metadata = self._order_meta[key]
        connector = self._resolve_cached_exchange(exchange, account_id=metadata.get("account_id"))
        if not connector:
            return None
        try:
            order = await connector.get_order(cached.id, cached.symbol)
            if order is not None:
                if not order.exchange:
                    order.exchange = exchange
                self._cache_live_order(order, metadata)
            return order
        except Exception as exc:
            logger.error(f"Failed to get order: {exc}")
            return None

    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        trading_mode: Optional[str] = None,
    ) -> List[Order]:
        resolved_mode = self._resolve_operation_mode(trading_mode=trading_mode)
        if resolved_mode == "paper":
            return [
                o for o in self._orders.values()
                if o.status == OrderStatus.OPEN
                and self._resolve_operation_mode(order_id=o.cache_key or o.id) == "paper"
                and (symbol is None or o.symbol == symbol)
                and (exchange is None or o.exchange == exchange)
            ]

        if exchange is None:
            exchanges = exchange_manager.get_connected_exchanges()
            if not exchanges:
                return []

            async def _fetch_open_orders(ex_name: str) -> List[Order]:
                connector = self._resolve_cached_exchange(ex_name)
                if not connector:
                    return []
                try:
                    rows = await asyncio.wait_for(connector.get_open_orders(symbol), timeout=7.0)
                    for row in rows:
                        if not getattr(row, "exchange", ""):
                            row.exchange = ex_name
                        self._cache_live_order(row, {"mode": "live", "account_id": "main"})
                    return rows
                except Exception as ex:
                    logger.warning(f"Failed to get open orders from {ex_name}: {ex}")
                    return []

            result = await asyncio.gather(
                *[_fetch_open_orders(ex_name) for ex_name in exchanges],
                return_exceptions=False,
            )
            merged: List[Order] = []
            seen = set()
            for rows in result:
                for row in rows:
                    key = (str(getattr(row, "exchange", "")).lower(), str(getattr(row, "id", "")))
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(row)
            merged.sort(key=lambda x: self._ts_sort_key(x.timestamp), reverse=True)
            return merged

        connector = self._resolve_cached_exchange(exchange)
        if not connector:
            return []

        try:
            orders = await connector.get_open_orders(symbol)
            for order in orders:
                if not order.exchange:
                    order.exchange = exchange
                self._cache_live_order(order, {"mode": "live", "account_id": "main"})
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
        orders.sort(key=lambda o: self._ts_sort_key(o.timestamp), reverse=True)
        return orders[: max(1, limit)]

    async def cancel_all_orders(
        self,
        symbol: Optional[str] = None,
        exchange: str = "binance",
        trading_mode: Optional[str] = None,
    ) -> int:
        resolved_mode = self._resolve_operation_mode(trading_mode=trading_mode)
        orders = await self.get_open_orders(symbol, exchange, trading_mode=resolved_mode)
        cancelled = 0
        for order in orders:
            if await self.cancel_order(
                order.cache_key or order.id,
                order.symbol,
                exchange,
                trading_mode=resolved_mode,
            ):
                cancelled += 1
        return cancelled

    def get_order_by_id(self, order_id: str, **scope: Any) -> Optional[Order]:
        return self._orders.get(self._order_key(order_id, **scope))

    def get_order_metadata(self, order_id: str, **scope: Any) -> Dict[str, Any]:
        return dict(self._order_meta.get(self._order_key(order_id, **scope)) or {})

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

    def _evict_terminal_orders(self, max_keep: int = 5000) -> None:
        """Bound in-memory order history so live trading cannot grow it without
        bound (clear_paper_history only clears *paper* orders, so live orders
        otherwise accumulate forever -> eventual OOM). Once the total exceeds
        max_keep, drop the OLDEST terminal orders (closed/canceled/expired/
        rejected). OPEN orders are always retained regardless of count.
        """
        total = len(self._orders)
        if total <= max_keep:
            return
        terminal = {
            OrderStatus.CLOSED,
            OrderStatus.CANCELED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
        }
        overflow = total - max_keep
        removed = 0
        # dict preserves insertion order -> iterate oldest-first.
        for order_id in list(self._orders.keys()):
            if removed >= overflow:
                break
            order = self._orders.get(order_id)
            if getattr(order, "status", None) in terminal:
                self._orders.pop(order_id, None)
                self._order_meta.pop(order_id, None)
                removed += 1
        if removed:
            logger.debug(
                f"Evicted {removed} terminal order(s) from in-memory history (cap={max_keep})"
            )

    def clear_paper_history(self, mode: str = "paper") -> Dict[str, int]:
        """Clear in-memory order history for the requested runtime mode."""
        target = self._normalize_mode(mode, default="paper")
        order_ids_to_remove = {
            order_id
            for order_id, meta in self._order_meta.items()
            if self._normalize_mode(meta.get("mode"), default="paper") == target
        }
        pending_ids_to_remove = {
            order_id
            for order_id, request in self._pending_orders.items()
            if self._resolve_request_mode(request) == target
        }
        total = len(order_ids_to_remove)
        meta_total = len(order_ids_to_remove)
        pending_total = len(pending_ids_to_remove)
        for order_id in order_ids_to_remove:
            self._orders.pop(order_id, None)
            self._order_meta.pop(order_id, None)
        for order_id in pending_ids_to_remove:
            self._pending_orders.pop(order_id, None)
        if target == "paper":
            self._paper_order_seq = 0
        return {
            "orders_cleared": total,
            "metadata_cleared": meta_total,
            "pending_cleared": pending_total,
        }


order_manager = OrderManager()
