"""持仓管理模块。"""
import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from config.settings import settings


class PositionSide(Enum):
    """持仓方向。"""

    LONG = "long"
    SHORT = "short"
    BOTH = "both"


class AmbiguousPositionError(RuntimeError):
    """Raised when a close/update request matches more than one position."""


@dataclass
class Position:
    """单个持仓信息。"""

    symbol: str
    exchange: str
    side: PositionSide
    entry_price: float
    current_price: float
    quantity: float
    value: float
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    realized_pnl: float = 0.0
    leverage: float = 1.0
    margin: float = 0.0
    liquidation_price: Optional[float] = None
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    strategy: Optional[str] = None
    account_id: str = "main"
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    trailing_stop_price: Optional[float] = None
    highest_price: Optional[float] = None
    lowest_price: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def cost_usd(self) -> float:
        """Return known execution cost attached to this position, if any."""
        meta = self.metadata if isinstance(self.metadata, dict) else {}
        fee = self._safe_float(meta.get("fee_usd"), 0.0)
        slippage = self._safe_float(meta.get("slippage_cost_usd"), 0.0)
        cost = self._safe_float(meta.get("cost_usd"), fee + slippage)
        if cost <= 0:
            cost = fee + slippage
        return max(0.0, float(cost or 0.0))

    def gross_realized_pnl(self) -> float:
        """PositionManager keeps realized_pnl as gross price PnL for execution compatibility."""
        return float(self.realized_pnl or 0.0)

    def net_realized_pnl(self) -> float:
        return self.gross_realized_pnl() - self.cost_usd()

    def update_price(self, current_price: float) -> None:
        """更新价格与浮动盈亏。"""
        self.current_price = float(current_price)
        self.updated_at = datetime.now(timezone.utc)

        if self.entry_price <= 0:
            self.unrealized_pnl = 0.0
            self.unrealized_pnl_pct = 0.0
            self.value = self.current_price * self.quantity
            return

        if self.side == PositionSide.LONG:
            self.unrealized_pnl = (self.current_price - self.entry_price) * self.quantity
            self.unrealized_pnl_pct = (self.current_price - self.entry_price) / self.entry_price
        else:
            self.unrealized_pnl = (self.entry_price - self.current_price) * self.quantity
            self.unrealized_pnl_pct = (self.entry_price - self.current_price) / self.entry_price

        self.value = self.current_price * self.quantity

        if self.highest_price is None:
            self.highest_price = self.current_price
        if self.lowest_price is None:
            self.lowest_price = self.current_price
        self.highest_price = max(float(self.highest_price), self.current_price)
        self.lowest_price = min(float(self.lowest_price), self.current_price)

        # Update trailing stop according to best favorable price.
        if self.side == PositionSide.LONG:
            if self.trailing_stop_pct is not None:
                self.trailing_stop_price = self.highest_price * (1 - float(self.trailing_stop_pct))
            elif self.trailing_stop_distance is not None:
                self.trailing_stop_price = self.highest_price - float(self.trailing_stop_distance)
        else:
            if self.trailing_stop_pct is not None:
                self.trailing_stop_price = self.lowest_price * (1 + float(self.trailing_stop_pct))
            elif self.trailing_stop_distance is not None:
                self.trailing_stop_price = self.lowest_price + float(self.trailing_stop_distance)

    def to_dict(self) -> Dict[str, Any]:
        """序列化。"""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "quantity": self.quantity,
            "value": self.value,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "realized_pnl": self.realized_pnl,
            "gross_realized_pnl": self.gross_realized_pnl(),
            "net_realized_pnl": self.net_realized_pnl(),
            "cost_usd": self.cost_usd(),
            "leverage": self.leverage,
            "margin": self.margin,
            "opened_at": self.opened_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "strategy": self.strategy,
            "account_id": self.account_id,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "trailing_stop_pct": self.trailing_stop_pct,
            "trailing_stop_distance": self.trailing_stop_distance,
            "trailing_stop_price": self.trailing_stop_price,
            "highest_price": self.highest_price,
            "lowest_price": self.lowest_price,
            "metadata": self.metadata,
        }


class PositionManager:
    """持仓管理器。"""

    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._position_history: List[Position] = []
        self._position_history_limit = max(
            100,
            int(getattr(settings, "POSITION_HISTORY_LIMIT", 5000) or 5000),
        )
        self._callbacks: List[Any] = []
        self._scope = self._normalize_scope(getattr(settings, "TRADING_MODE", "paper"))
        self._storage_root = Path(getattr(settings, "CACHE_PATH", "cache")) / "runtime_state"
        self._persist_throttle_seconds = 2.0
        self._last_persist_at = 0.0
        self._dirty = False
        self._last_close_error = ""
        self._restore_scope_state(self._load_scope_state(self._scope))

    def get_last_close_error(self) -> str:
        """Most recent close_position failure reason (empty if last close was clean)."""
        return str(self._last_close_error or "")

    @staticmethod
    def _normalize_scope(scope: str) -> str:
        return "live" if str(scope or "").strip().lower() == "live" else "paper"

    @staticmethod
    def _normalize_strategy(strategy: Any) -> str:
        return str(strategy or "").strip()

    @staticmethod
    def _parse_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def _coerce_side(value: Any) -> PositionSide:
        text = str(getattr(value, "value", value) or "").strip().lower()
        if text == PositionSide.SHORT.value:
            return PositionSide.SHORT
        if text == PositionSide.BOTH.value:
            return PositionSide.BOTH
        return PositionSide.LONG

    @classmethod
    def _json_safe_value(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {str(k): cls._json_safe_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._json_safe_value(v) for v in value]
        if hasattr(value, "isoformat") and callable(getattr(value, "isoformat")):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        return str(value)

    def _scope_state_path(self, scope: Optional[str] = None) -> Path:
        normalized = self._normalize_scope(scope or self._scope)
        return self._storage_root / f"positions_{normalized}.json"

    def _position_to_state(self, position: Position) -> Dict[str, Any]:
        return {
            "symbol": str(position.symbol or ""),
            "exchange": str(position.exchange or ""),
            "side": position.side.value,
            "entry_price": float(position.entry_price or 0.0),
            "current_price": float(position.current_price or 0.0),
            "quantity": float(position.quantity or 0.0),
            "value": float(position.value or 0.0),
            "unrealized_pnl": float(position.unrealized_pnl or 0.0),
            "unrealized_pnl_pct": float(position.unrealized_pnl_pct or 0.0),
            "realized_pnl": float(position.realized_pnl or 0.0),
            "leverage": float(position.leverage or 1.0),
            "margin": float(position.margin or 0.0),
            "liquidation_price": float(position.liquidation_price) if position.liquidation_price is not None else None,
            "opened_at": position.opened_at.isoformat() if hasattr(position.opened_at, "isoformat") else str(position.opened_at),
            "updated_at": position.updated_at.isoformat() if hasattr(position.updated_at, "isoformat") else str(position.updated_at),
            "strategy": position.strategy,
            "account_id": str(position.account_id or "main"),
            "stop_loss": float(position.stop_loss) if position.stop_loss is not None else None,
            "take_profit": float(position.take_profit) if position.take_profit is not None else None,
            "trailing_stop_pct": float(position.trailing_stop_pct) if position.trailing_stop_pct is not None else None,
            "trailing_stop_distance": float(position.trailing_stop_distance) if position.trailing_stop_distance is not None else None,
            "trailing_stop_price": float(position.trailing_stop_price) if position.trailing_stop_price is not None else None,
            "highest_price": float(position.highest_price) if position.highest_price is not None else None,
            "lowest_price": float(position.lowest_price) if position.lowest_price is not None else None,
            "metadata": self._json_safe_value(position.metadata or {}),
        }

    def _position_from_state(self, payload: Dict[str, Any]) -> Optional[Position]:
        if not isinstance(payload, dict):
            return None
        try:
            position = Position(
                symbol=str(payload.get("symbol") or ""),
                exchange=str(payload.get("exchange") or ""),
                side=self._coerce_side(payload.get("side")),
                entry_price=float(payload.get("entry_price") or 0.0),
                current_price=float(payload.get("current_price") or payload.get("entry_price") or 0.0),
                quantity=float(payload.get("quantity") or 0.0),
                value=float(payload.get("value") or 0.0),
                unrealized_pnl=float(payload.get("unrealized_pnl") or 0.0),
                unrealized_pnl_pct=float(payload.get("unrealized_pnl_pct") or 0.0),
                realized_pnl=float(payload.get("realized_pnl") or 0.0),
                leverage=float(payload.get("leverage") or 1.0),
                margin=float(payload.get("margin") or 0.0),
                liquidation_price=float(payload["liquidation_price"]) if payload.get("liquidation_price") is not None else None,
                opened_at=self._parse_datetime(payload.get("opened_at")),
                updated_at=self._parse_datetime(payload.get("updated_at")),
                strategy=str(payload.get("strategy") or "") or None,
                account_id=str(payload.get("account_id") or "main"),
                stop_loss=float(payload["stop_loss"]) if payload.get("stop_loss") is not None else None,
                take_profit=float(payload["take_profit"]) if payload.get("take_profit") is not None else None,
                trailing_stop_pct=float(payload["trailing_stop_pct"]) if payload.get("trailing_stop_pct") is not None else None,
                trailing_stop_distance=float(payload["trailing_stop_distance"]) if payload.get("trailing_stop_distance") is not None else None,
                trailing_stop_price=float(payload["trailing_stop_price"]) if payload.get("trailing_stop_price") is not None else None,
                highest_price=float(payload["highest_price"]) if payload.get("highest_price") is not None else None,
                lowest_price=float(payload["lowest_price"]) if payload.get("lowest_price") is not None else None,
                metadata=dict(payload.get("metadata") or {}),
            )
            if not self._is_restorable_position(position):
                return None
            return position
        except Exception as e:
            logger.warning(f"Failed to restore persisted position: {e}")
            return None

    @staticmethod
    def _is_restorable_position(position: Position) -> bool:
        account_id = str(position.account_id or "").strip()
        strategy = str(position.strategy or "").strip()
        metadata = dict(position.metadata or {})
        source = str(metadata.get("source") or "").strip().lower()
        if account_id == "acct_A" and strategy == "stub" and source in {"", "strategy"}:
            logger.warning(
                "Skipped persisted test stub position: "
                f"exchange={position.exchange} symbol={position.symbol} account_id={account_id}"
            )
            return False
        return True

    def _snapshot_scope_state(self) -> Dict[str, Any]:
        return {
            "scope": self._scope,
            "open_positions": [self._position_to_state(pos) for pos in self._positions.values()],
            "closed_positions": [self._position_to_state(pos) for pos in self._position_history],
        }

    def _restore_scope_state(self, payload: Optional[Dict[str, Any]]) -> None:
        self._positions = {}
        self._position_history = []
        if not isinstance(payload, dict):
            self._dirty = False
            return

        for row in payload.get("open_positions") or []:
            position = self._position_from_state(row)
            if position is None:
                continue
            self._positions[
                self._make_key(position.exchange, position.symbol, position.account_id, position.strategy)
            ] = position

        for row in payload.get("closed_positions") or []:
            position = self._position_from_state(row)
            if position is None:
                continue
            self._position_history.append(position)
        self._trim_position_history()

        self._dirty = False

    def _read_scope_positions(self, scope: Optional[str] = None) -> List[Position]:
        normalized = self._normalize_scope(scope or self._scope)
        if normalized == self._scope:
            return list(self._positions.values())

        payload = self._load_scope_state(normalized)
        if not isinstance(payload, dict):
            return []

        positions: List[Position] = []
        for row in payload.get("open_positions") or []:
            position = self._position_from_state(row)
            if position is not None:
                positions.append(position)
        return positions

    def _read_scope_closed_positions(self, scope: Optional[str] = None) -> List[Position]:
        normalized = self._normalize_scope(scope or self._scope)
        if normalized == self._scope:
            return list(self._position_history)

        payload = self._load_scope_state(normalized)
        if not isinstance(payload, dict):
            return []

        positions: List[Position] = []
        for row in payload.get("closed_positions") or []:
            position = self._position_from_state(row)
            if position is not None:
                positions.append(position)
        return positions

    def _load_scope_state(self, scope: Optional[str] = None) -> Optional[Dict[str, Any]]:
        path = self._scope_state_path(scope)
        try:
            if not path.exists():
                return None
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
        except Exception as e:
            logger.warning(f"Failed to load persisted positions for scope={scope or self._scope}: {e}")
            return None

    def _persist_scope_state(self, *, force: bool = False) -> None:
        if not self._dirty and not force:
            return
        now = time.monotonic()
        if not force and (now - self._last_persist_at) < self._persist_throttle_seconds:
            return
        try:
            path = self._scope_state_path(self._scope)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(self._snapshot_scope_state(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(path)
            self._last_persist_at = now
            self._dirty = False
        except Exception as e:
            logger.warning(f"Failed to persist positions for scope={self._scope}: {e}")

    def _trim_position_history(self) -> None:
        if len(self._position_history) > self._position_history_limit:
            self._position_history = self._position_history[-self._position_history_limit:]

    def set_scope(self, scope: str) -> None:
        target = self._normalize_scope(scope)
        if target == self._scope:
            if not self._positions and not self._position_history:
                self._restore_scope_state(self._load_scope_state(target))
            return
        self._dirty = True
        self._persist_scope_state(force=True)
        self._scope = target
        self._last_persist_at = 0.0
        self._restore_scope_state(self._load_scope_state(target))

    def get_scope(self) -> str:
        return str(self._scope or "paper")

    def _make_key(
        self,
        exchange: str,
        symbol: str,
        account_id: str = "main",
        strategy: Optional[str] = None,
    ) -> str:
        strategy_key = self._normalize_strategy(strategy) or "__default__"
        return f"{account_id}:{exchange}_{symbol}::{strategy_key}"

    def _matching_items(
        self,
        exchange: str,
        symbol: str,
        *,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> List[tuple[str, Position]]:
        target_account = str(account_id or "main") if account_id is not None else None
        strategy_filter_enabled = strategy is not None
        target_strategy = self._normalize_strategy(strategy) if strategy_filter_enabled else ""
        matches: List[tuple[str, Position]] = []
        for key, position in self._positions.items():
            if position.exchange != exchange or position.symbol != symbol:
                continue
            if target_account is not None and str(position.account_id or "main") != target_account:
                continue
            if strategy_filter_enabled and self._normalize_strategy(position.strategy) != target_strategy:
                continue
            matches.append((key, position))
        return matches

    def _match_key(
        self,
        exchange: str,
        symbol: str,
        *,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> Optional[str]:
        matches = self._matching_items(
            exchange,
            symbol,
            account_id=account_id,
            strategy=strategy,
        )
        if len(matches) == 1:
            return matches[0][0]
        return None

    def register_callback(self, callback: Any) -> None:
        self._callbacks.append(callback)

    async def _notify_callbacks(self, position: Position, event: str) -> None:
        for callback in self._callbacks:
            try:
                await callback(position, event)
            except Exception as e:
                logger.error(f"Position callback error: {e}")

    def open_position(
        self,
        exchange: str,
        symbol: str,
        side: PositionSide,
        entry_price: float,
        quantity: float,
        leverage: float = 1.0,
        strategy: Optional[str] = None,
        account_id: str = "main",
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        trailing_stop_distance: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Position:
        strategy_key = self._normalize_strategy(strategy) or None
        key = self._make_key(exchange, symbol, account_id, strategy_key)
        qty = float(quantity)
        price = float(entry_price)
        lev = max(1e-9, float(leverage or 1.0))
        trail_pct = None if trailing_stop_pct is None else max(0.0, float(trailing_stop_pct))
        trail_dist = None if trailing_stop_distance is None else max(0.0, float(trailing_stop_distance))

        position = Position(
            symbol=symbol,
            exchange=exchange,
            side=side,
            entry_price=price,
            current_price=price,
            quantity=qty,
            value=price * qty,
            leverage=lev,
            margin=(price * qty) / lev,
            strategy=strategy_key,
            account_id=account_id or "main",
            stop_loss=float(stop_loss) if stop_loss is not None else None,
            take_profit=float(take_profit) if take_profit is not None else None,
            trailing_stop_pct=trail_pct if trail_pct and trail_pct > 0 else None,
            trailing_stop_distance=trail_dist if trail_dist and trail_dist > 0 else None,
            trailing_stop_price=None,
            highest_price=price,
            lowest_price=price,
            metadata=metadata or {},
        )
        position.update_price(price)

        self._positions[key] = position
        self._dirty = True
        self._persist_scope_state()
        logger.info(
            f"Position opened: {symbol} {side.value} "
            f"{qty} @ {price} (leverage: {lev}x)"
        )

        try:
            asyncio.get_running_loop()
            asyncio.create_task(self._notify_callbacks(position, "opened"))
        except RuntimeError:
            pass
        return position

    def close_position(
        self,
        exchange: str,
        symbol: str,
        close_price: float,
        quantity: Optional[float] = None,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> Optional[Position]:
        matches = self._matching_items(
            exchange,
            symbol,
            account_id=account_id,
            strategy=strategy,
        )
        if not matches:
            msg = (
                f"No position found for {exchange}_{symbol} "
                f"account={account_id or '*'} strategy={strategy!r}"
            )
            logger.warning(msg)
            self._last_close_error = msg
            return None
        if len(matches) > 1:
            # Do NOT swallow this silently: an unclosable position is a
            # risk event. Surface it via error log + queryable last-error so
            # the caller / health checks can react instead of assuming success.
            msg = (
                f"Ambiguous position close for {exchange}_{symbol} "
                f"account={account_id or '*'} strategy={strategy!r}: "
                f"{len(matches)} matches — caller must disambiguate by "
                f"account_id/strategy"
            )
            logger.error(msg)
            self._last_close_error = msg
            raise AmbiguousPositionError(msg)
        self._last_close_error = ""
        key, position = matches[0]
        if not position:
            logger.warning(f"No position found for {exchange}_{symbol}")
            return None

        close_px = float(close_price)

        if quantity is not None and 0 < float(quantity) < position.quantity:
            closing_qty = float(quantity)
            origin_qty = position.quantity

            position.update_price(close_px)
            realized_piece = position.unrealized_pnl * (closing_qty / origin_qty)

            position.realized_pnl += realized_piece
            position.quantity = origin_qty - closing_qty
            position.value = close_px * position.quantity
            position.updated_at = datetime.now(timezone.utc)

            logger.info(
                f"Position partially closed: {symbol} "
                f"{closing_qty}/{origin_qty} @ {close_px}, "
                f"realized PnL: {realized_piece:.2f}"
            )

            try:
                asyncio.get_running_loop()
                asyncio.create_task(self._notify_callbacks(position, "partial_close"))
            except RuntimeError:
                pass
            self._dirty = True
            self._persist_scope_state()
            return position

        position.update_price(close_px)
        position.realized_pnl += position.unrealized_pnl

        logger.info(
            f"Position closed: {symbol} @ {close_px}, "
            f"PnL: {position.realized_pnl:.2f}"
        )

        self._position_history.append(position)
        self._trim_position_history()
        del self._positions[key]
        self._dirty = True
        self._persist_scope_state()

        try:
            asyncio.get_running_loop()
            asyncio.create_task(self._notify_callbacks(position, "closed"))
        except RuntimeError:
            pass
        return position

    def update_position_price(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> Optional[Position]:
        key = self._match_key(exchange, symbol, account_id=account_id, strategy=strategy)
        if not key:
            return None
        position = self._positions.get(key)
        if position:
            position.update_price(current_price)
            self._dirty = True
            # Price ticks are high-frequency: rely on the 2s throttle instead of
            # forcing a synchronous full-state disk write on every update.
            self._persist_scope_state()
        return position

    def flush(self) -> None:
        """Force-persist current scope state immediately, bypassing the throttle.

        Call before reading back persisted state in tests or before graceful shutdown.
        """
        self._persist_scope_state(force=True)

    def update_all_prices(self, prices: Dict[str, Dict[str, float]]) -> None:
        updated = False
        for position in self._positions.values():
            exchange_prices = prices.get(position.exchange, {})
            if position.symbol in exchange_prices:
                position.update_price(exchange_prices[position.symbol])
                updated = True
        if updated:
            self._dirty = True
            # High-frequency batch price refresh: throttled persist, not forced.
            self._persist_scope_state()

    def get_position(
        self,
        exchange: str,
        symbol: str,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> Optional[Position]:
        key = self._match_key(exchange, symbol, account_id=account_id, strategy=strategy)
        return self._positions.get(key) if key else None

    def get_positions(
        self,
        exchange: str,
        symbol: str,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> List[Position]:
        return [
            position
            for _, position in self._matching_items(
                exchange,
                symbol,
                account_id=account_id,
                strategy=strategy,
            )
        ]

    def get_all_positions(self, scope: Optional[str] = None) -> List[Position]:
        return self._read_scope_positions(scope)

    def get_positions_by_exchange(self, exchange: str, scope: Optional[str] = None) -> List[Position]:
        return [p for p in self._read_scope_positions(scope) if p.exchange == exchange]

    def get_positions_by_strategy(self, strategy: str, scope: Optional[str] = None) -> List[Position]:
        target_strategy = self._normalize_strategy(strategy)
        return [
            p for p in self._read_scope_positions(scope)
            if self._normalize_strategy(p.strategy) == target_strategy
        ]

    def get_total_value(self) -> float:
        return sum(p.value for p in self._positions.values())

    def get_total_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self._positions.values())

    def get_total_realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self._position_history)

    def get_closed_positions(
        self,
        limit: Optional[int] = None,
        scope: Optional[str] = None,
    ) -> List[Position]:
        """返回历史平仓记录。"""
        rows = self._read_scope_closed_positions(scope)
        if limit is None or limit <= 0:
            return rows
        return rows[-limit:]

    def has_position(
        self,
        exchange: str,
        symbol: str,
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> bool:
        return self.get_position(exchange, symbol, account_id=account_id, strategy=strategy) is not None

    def get_position_count(self) -> int:
        return len(self._positions)

    def get_stats(self) -> Dict[str, Any]:
        positions = list(self._positions.values())
        return {
            "position_count": len(positions),
            "total_value": self.get_total_value(),
            "total_unrealized_pnl": self.get_total_pnl(),
            "total_realized_pnl": self.get_total_realized_pnl(),
            "long_positions": len([p for p in positions if p.side == PositionSide.LONG]),
            "short_positions": len([p for p in positions if p.side == PositionSide.SHORT]),
            "winning_positions": len([p for p in positions if p.unrealized_pnl > 0]),
            "losing_positions": len([p for p in positions if p.unrealized_pnl < 0]),
        }

    def get_exposure_by_symbol(self) -> Dict[str, float]:
        exposure: Dict[str, float] = {}
        for position in self._positions.values():
            exposure[position.symbol] = exposure.get(position.symbol, 0.0) + position.value
        return exposure

    def close_all_positions(
        self,
        prices: Dict[str, Dict[str, float]],
        account_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> List[Position]:
        closed: List[Position] = []
        for position in list(self._positions.values()):
            if account_id and position.account_id != account_id:
                continue
            if strategy is not None and self._normalize_strategy(position.strategy) != self._normalize_strategy(strategy):
                continue
            exchange_prices = prices.get(position.exchange, {})
            if position.symbol not in exchange_prices:
                continue
            result = self.close_position(
                exchange=position.exchange,
                symbol=position.symbol,
                close_price=exchange_prices[position.symbol],
                account_id=position.account_id,
                strategy=self._normalize_strategy(position.strategy),
            )
            if result:
                closed.append(result)
        return closed

    def clear_all(self) -> Dict[str, int]:
        """Clear in-memory open/closed position records."""
        open_count = len(self._positions)
        closed_count = len(self._position_history)
        self._positions.clear()
        self._position_history.clear()
        self._dirty = True
        self._persist_scope_state(force=True)
        return {
            "open_positions_cleared": open_count,
            "closed_positions_cleared": closed_count,
        }

    def clear_scope(self, scope: str) -> Dict[str, int]:
        target = self._normalize_scope(scope)
        if target == self._scope:
            return self.clear_all()

        payload = self._load_scope_state(target) or {
            "scope": target,
            "open_positions": [],
            "closed_positions": [],
        }
        open_count = len(list(payload.get("open_positions") or []))
        closed_count = len(list(payload.get("closed_positions") or []))
        path = self._scope_state_path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {"scope": target, "open_positions": [], "closed_positions": []},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        tmp.replace(path)
        return {
            "open_positions_cleared": open_count,
            "closed_positions_cleared": closed_count,
        }


position_manager = PositionManager()
