"""
Paper trading module.
"""

import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List

from loguru import logger

from core.exchanges import exchange_manager
from core.strategies import Signal, StrategyBase
from core.trading.execution_engine import execution_engine
from core.trading.position_manager import position_manager


class PaperTradingEngine:
    """Paper trading execution engine."""

    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self._capital = initial_capital
        self._running = False
        self._strategies: List[StrategyBase] = []
        self._callbacks: List[Callable] = []
        self._trade_history: List[Dict[str, Any]] = []

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
        asyncio.create_task(self._main_loop())

    async def stop(self) -> None:
        """Stop the paper trading engine."""
        self._running = False

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
                    ticker = await exchange.get_ticker(symbol)
                    prices.setdefault(exchange_name, {})[symbol] = ticker.last
                except Exception as e:
                    logger.warning(f"Failed to get ticker for {symbol}: {e}")

        if prices:
            position_manager.update_all_prices(prices)
        return prices

    async def _run_strategies(self) -> None:
        """Run strategy hooks for the current tick."""
        for strategy in self._strategies:
            if not strategy.is_running:
                continue

            try:
                # Strategy-specific data gathering is intentionally left to the
                # strategy implementation and surrounding orchestration.
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
                    "timestamp": datetime.now().isoformat(),
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
