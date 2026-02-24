"""持仓管理模块。"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from loguru import logger


class PositionSide(Enum):
    """持仓方向。"""

    LONG = "long"
    SHORT = "short"
    BOTH = "both"


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
    opened_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
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

    def update_price(self, current_price: float) -> None:
        """更新价格与浮动盈亏。"""
        self.current_price = float(current_price)
        self.updated_at = datetime.now()

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
        self._callbacks: List[Any] = []

    def _make_key(self, exchange: str, symbol: str, account_id: str = "main") -> str:
        return f"{account_id}:{exchange}_{symbol}"

    def _match_key(self, exchange: str, symbol: str, account_id: Optional[str] = None) -> Optional[str]:
        if account_id:
            key = self._make_key(exchange, symbol, account_id)
            return key if key in self._positions else None
        for key, position in self._positions.items():
            if position.exchange == exchange and position.symbol == symbol:
                return key
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
        key = self._make_key(exchange, symbol, account_id)
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
            strategy=strategy,
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
        logger.info(
            f"Position opened: {symbol} {side.value} "
            f"{qty} @ {price} (leverage: {lev}x)"
        )

        asyncio.create_task(self._notify_callbacks(position, "opened"))
        return position

    def close_position(
        self,
        exchange: str,
        symbol: str,
        close_price: float,
        quantity: Optional[float] = None,
        account_id: Optional[str] = None,
    ) -> Optional[Position]:
        key = self._match_key(exchange, symbol, account_id=account_id)
        if not key:
            logger.warning(f"No position found for {exchange}_{symbol} account={account_id or '*'}")
            return None
        position = self._positions.get(key)
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
            position.updated_at = datetime.now()

            logger.info(
                f"Position partially closed: {symbol} "
                f"{closing_qty}/{origin_qty} @ {close_px}, "
                f"realized PnL: {realized_piece:.2f}"
            )

            asyncio.create_task(self._notify_callbacks(position, "partial_close"))
            return position

        position.update_price(close_px)
        position.realized_pnl += position.unrealized_pnl

        logger.info(
            f"Position closed: {symbol} @ {close_px}, "
            f"PnL: {position.realized_pnl:.2f}"
        )

        self._position_history.append(position)
        del self._positions[key]

        asyncio.create_task(self._notify_callbacks(position, "closed"))
        return position

    def update_position_price(
        self,
        exchange: str,
        symbol: str,
        current_price: float,
        account_id: Optional[str] = None,
    ) -> Optional[Position]:
        key = self._match_key(exchange, symbol, account_id=account_id)
        if not key:
            return None
        position = self._positions.get(key)
        if position:
            position.update_price(current_price)
        return position

    def update_all_prices(self, prices: Dict[str, Dict[str, float]]) -> None:
        for position in self._positions.values():
            exchange_prices = prices.get(position.exchange, {})
            if position.symbol in exchange_prices:
                position.update_price(exchange_prices[position.symbol])

    def get_position(self, exchange: str, symbol: str, account_id: Optional[str] = None) -> Optional[Position]:
        key = self._match_key(exchange, symbol, account_id=account_id)
        return self._positions.get(key) if key else None

    def get_all_positions(self) -> List[Position]:
        return list(self._positions.values())

    def get_positions_by_exchange(self, exchange: str) -> List[Position]:
        return [p for p in self._positions.values() if p.exchange == exchange]

    def get_positions_by_strategy(self, strategy: str) -> List[Position]:
        return [p for p in self._positions.values() if p.strategy == strategy]

    def get_total_value(self) -> float:
        return sum(p.value for p in self._positions.values())

    def get_total_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self._positions.values())

    def get_total_realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self._position_history)

    def get_closed_positions(self, limit: Optional[int] = None) -> List[Position]:
        """返回历史平仓记录。"""
        if limit is None or limit <= 0:
            return list(self._position_history)
        return self._position_history[-limit:]

    def has_position(self, exchange: str, symbol: str, account_id: Optional[str] = None) -> bool:
        return self.get_position(exchange, symbol, account_id=account_id) is not None

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
    ) -> List[Position]:
        closed: List[Position] = []
        for position in list(self._positions.values()):
            if account_id and position.account_id != account_id:
                continue
            exchange_prices = prices.get(position.exchange, {})
            if position.symbol not in exchange_prices:
                continue
            result = self.close_position(
                exchange=position.exchange,
                symbol=position.symbol,
                close_price=exchange_prices[position.symbol],
                account_id=position.account_id,
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
        return {
            "open_positions_cleared": open_count,
            "closed_positions_cleared": closed_count,
        }


position_manager = PositionManager()
