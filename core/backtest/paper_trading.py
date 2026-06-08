"""
Paper trading module.
"""

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from loguru import logger

from config.settings import settings
from core.exchanges.exchange_manager import exchange_manager
from core.marketdata.runtime_price_provider import get_realtime_price
from core.strategies import Signal, StrategyBase
from core.trading.execution_engine import execution_engine
from core.trading.position_manager import position_manager


def _log_task_failure(task: "asyncio.Task[Any]", *, context: str) -> None:
    """Safely log task failures from a done callback.

    The callback must never raise, and it must treat cancellation as a normal
    shutdown path. Probing ``task.exception()`` is wrapped so that a callback
    implementation bug does not hide the underlying task failure.
    """

    if task.cancelled():
        return

    exc = None
    try:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
    except Exception as probe_error:
        logger.error(f"{context} done callback failed while probing task state: {probe_error!r}")
        return

    if exc is not None:
        logger.error(f"{context} exited with an error: {exc!r}")


class PaperTradingEngine:
    """Paper trading execution engine."""

    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self._capital = initial_capital
        self._running = False
        self._strategies: List[StrategyBase] = []
        self._callbacks: List[Callable] = []
        self._trade_history: List[Dict[str, Any]] = []
        self._main_task: asyncio.Task | None = None

    def add_strategy(self, strategy: StrategyBase) -> None:
        """Register a strategy for simulation."""
        self._strategies.append(strategy)
        logger.info(f"Strategy added: {strategy.name}")

    def remove_strategy(self, strategy_name: str) -> None:
        """Remove a strategy by name."""
        self._strategies = [s for s in self._strategies if s.name != strategy_name]

    def register_callback(self, callback: Callable) -> None:
        """Register an async callback for paper trading events."""
        self._callbacks.append(callback)

    async def _notify_callbacks(self, event: str, data: Any) -> None:
        """Notify registered callbacks."""
        for callback in self._callbacks:
            try:
                await callback(event, data)
            except Exception as e:
                logger.error(f"Callback error: {e}")

    async def start(self) -> None:
        """Start the paper trading engine."""
        if self._running:
            return

        self._running = True

        for strategy in self._strategies:
            strategy.initialize()
            strategy.start()

        execution_engine.set_paper_trading(True)
        await execution_engine.start()

        logger.info(f"Paper trading started with capital: ${self._capital:,.2f}")
        self._main_task = asyncio.create_task(self._main_loop())
        self._main_task.add_done_callback(lambda task: _log_task_failure(task, context="Paper trading loop"))

    async def stop(self) -> None:
        """Stop the paper trading engine."""
        self._running = False

        if self._main_task and not self._main_task.done():
            self._main_task.cancel()

        for strategy in self._strategies:
            strategy.stop()

        await execution_engine.stop()
        logger.info("Paper trading stopped")

    async def _main_loop(self) -> None:
        """Main background loop."""
        while self._running:
            try:
                await self._update_prices()
                await self._run_strategies()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Paper trading loop error: {e}")
                await asyncio.sleep(5)

    async def _update_prices(self) -> Dict[str, Dict[str, float]]:
        """Refresh a small live price snapshot for simulated positions."""
        prices: Dict[str, Dict[str, float]] = {}

        for exchange_name in exchange_manager.get_connected_exchanges():
            exchange = exchange_manager.get_exchange(exchange_name)
            if not exchange:
                continue

            symbols = exchange_manager.get_supported_symbols(exchange_name)
            for symbol in symbols[:5]:
                try:
                    result = await get_realtime_price(
                        exchange_name,
                        symbol,
                        connector=exchange,
                        allow_rest_fallback=True,
                        max_age_sec=float(getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0) or 10.0),
                        rest_timeout_sec=3.0,
                    )
                    if result.ok and result.price is not None:
                        prices.setdefault(exchange_name, {})[symbol] = float(result.price)
                except Exception as e:
                    logger.warning(f"Failed to get ticker for {symbol}: {e}")

        if prices:
            position_manager.update_all_prices(prices)
        return prices

    async def _run_strategies(self) -> None:
        """No-op: signal generation is handled by strategy_manager, not this engine.

        PaperTradingEngine receives pre-generated signals via process_signal().
        This loop exists so the tick scheduler has a hook to call, but live signal
        dispatch does NOT flow through here — do not add strategy.generate_signals()
        calls here without also wiring up market data routing.
        """
        for strategy in self._strategies:
            if not strategy.is_running:
                continue
            try:
                pass
            except Exception as e:
                logger.error(f"Strategy {strategy.name} error: {e}")

    async def process_signal(self, signal: Signal) -> None:
        """Execute a simulated signal and record the result."""
        result = await execution_engine.execute_signal(signal)

        if result:
            self._trade_history.append(
                {
                    **result,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._notify_callbacks("trade", result)

    def get_capital(self) -> float:
        """Return current simulated cash."""
        return self._capital

    def get_total_value(self) -> float:
        """Return cash plus marked-to-market position value."""
        positions_value = position_manager.get_total_value()
        return self._capital + positions_value

    def get_pnl(self) -> float:
        """Return net profit and loss."""
        return self.get_total_value() - self.initial_capital

    def get_pnl_pct(self) -> float:
        """Return net profit ratio."""
        return self.get_pnl() / self.initial_capital

    def get_trade_count(self) -> int:
        """Return number of simulated trades."""
        return len(self._trade_history)

    def get_stats(self) -> Dict[str, Any]:
        """Return a compact paper trading snapshot."""
        return {
            "initial_capital": self.initial_capital,
            "current_capital": self._capital,
            "total_value": self.get_total_value(),
            "pnl": self.get_pnl(),
            "pnl_pct": self.get_pnl_pct(),
            "trade_count": self.get_trade_count(),
            "position_count": position_manager.get_position_count(),
            "running": self._running,
        }

    def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return recent simulated trade history."""
        return self._trade_history[-limit:]

    def reset(self) -> None:
        """Reset paper trading balances and clear local positions."""
        self._capital = self.initial_capital
        self._trade_history.clear()

        for position in list(position_manager.get_all_positions()):
            position_manager.close_position(
                str(position.exchange or ""),
                str(position.symbol or ""),
                0,
                account_id=str(position.account_id or "main"),
                strategy=str(position.strategy or ""),
            )

        logger.info("Paper trading reset")


class RealTimeSimulator:
    """Replay historical prices as a simple real-time feed."""

    def __init__(self):
        self._running = False
        self._price_feeds: Dict[str, float] = {}
        self._callbacks: List[Callable] = []

    async def simulate_from_data(
        self,
        data: Dict[str, List[float]],
        speed: float = 1.0,
    ) -> None:
        """
        Replay price sequences.

        Args:
            data: Mapping like ``{symbol: [price1, price2, ...]}``.
            speed: Playback speed where ``1.0`` is real time.
        """
        self._running = True

        max_len = max(len(prices) for prices in data.values())
        for i in range(max_len):
            if not self._running:
                break

            for symbol, prices in data.items():
                if i < len(prices):
                    self._price_feeds[symbol] = prices[i]

            await self._notify_price_update()
            await asyncio.sleep(1.0 / speed)

    def register_price_callback(self, callback: Callable) -> None:
        """Register an async callback for simulated price updates."""
        self._callbacks.append(callback)

    async def _notify_price_update(self) -> None:
        """Fan out the latest simulated prices."""
        for callback in self._callbacks:
            try:
                await callback(self._price_feeds.copy())
            except Exception as e:
                logger.error(f"Price callback error: {e}")

    def stop(self) -> None:
        """Stop playback."""
        self._running = False


paper_trading_engine = PaperTradingEngine()
realtime_simulator = RealTimeSimulator()
