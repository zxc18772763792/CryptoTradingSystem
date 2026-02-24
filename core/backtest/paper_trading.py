"""
模拟交易模块
"""
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
from loguru import logger

from core.strategies import StrategyBase, Signal
from core.trading.execution_engine import execution_engine
from core.trading.position_manager import position_manager
from core.exchanges import exchange_manager, Ticker


class PaperTradingEngine:
    """模拟交易引擎"""

    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self._capital = initial_capital
        self._running = False
        self._strategies: List[StrategyBase] = []
        self._callbacks: List[Callable] = []
        self._trade_history: List[Dict] = []

    def add_strategy(self, strategy: StrategyBase) -> None:
        """添加策略"""
        self._strategies.append(strategy)
        logger.info(f"Strategy added: {strategy.name}")

    def remove_strategy(self, strategy_name: str) -> None:
        """移除策略"""
        self._strategies = [s for s in self._strategies if s.name != strategy_name]

    def register_callback(self, callback: Callable) -> None:
        """注册回调"""
        self._callbacks.append(callback)

    async def _notify_callbacks(self, event: str, data: Any) -> None:
        """通知回调"""
        for callback in self._callbacks:
            try:
                await callback(event, data)
            except Exception as e:
                logger.error(f"Callback error: {e}")

    async def start(self) -> None:
        """启动模拟交易"""
        if self._running:
            return

        self._running = True

        # 初始化所有策略
        for strategy in self._strategies:
            strategy.initialize()
            strategy.start()

        # 设置执行引擎为模拟模式
        execution_engine.set_paper_trading(True)
        await execution_engine.start()

        logger.info(f"Paper trading started with capital: ${self._capital:,.2f}")

        # 启动主循环
        asyncio.create_task(self._main_loop())

    async def stop(self) -> None:
        """停止模拟交易"""
        self._running = False

        # 停止所有策略
        for strategy in self._strategies:
            strategy.stop()

        await execution_engine.stop()
        logger.info("Paper trading stopped")

    async def _main_loop(self) -> None:
        """主循环"""
        while self._running:
            try:
                # 更新价格
                await self._update_prices()

                # 运行策略
                await self._run_strategies()

                # 等待
                await asyncio.sleep(60)  # 每分钟执行一次

            except Exception as e:
                logger.error(f"Paper trading loop error: {e}")
                await asyncio.sleep(5)

    async def _update_prices(self) -> None:
        """更新价格"""
        prices = {}

        for exchange_name in exchange_manager.get_connected_exchanges():
            exchange = exchange_manager.get_exchange(exchange_name)
            if not exchange:
                continue

            symbols = exchange_manager.get_supported_symbols(exchange_name)

            for symbol in symbols[:5]:  # 限制数量避免请求过多
                try:
                    ticker = await exchange.get_ticker(symbol)
                    if exchange_name not in prices:
                        prices[exchange_name] = {}
                    prices[exchange_name][symbol] = ticker.last

                    # 更新持仓
                    position_manager.update_position_price(
                        exchange_name, symbol, ticker.last
                    )

                except Exception as e:
                    logger.warning(f"Failed to get ticker for {symbol}: {e}")

        return prices

    async def _run_strategies(self) -> None:
        """运行策略"""
        for strategy in self._strategies:
            if not strategy.is_running:
                continue

            try:
                # 获取策略需要的数据
                # 这里简化处理，实际应该根据策略配置获取数据
                pass

            except Exception as e:
                logger.error(f"Strategy {strategy.name} error: {e}")

    async def process_signal(self, signal: Signal) -> None:
        """处理信号"""
        result = await execution_engine.execute_signal(signal)

        if result:
            self._trade_history.append({
                **result,
                "timestamp": datetime.now().isoformat(),
            })

            await self._notify_callbacks("trade", result)

    def get_capital(self) -> float:
        """获取当前资金"""
        return self._capital

    def get_total_value(self) -> float:
        """获取总价值（资金+持仓）"""
        positions_value = position_manager.get_total_value()
        return self._capital + positions_value

    def get_pnl(self) -> float:
        """获取总盈亏"""
        return self.get_total_value() - self.initial_capital

    def get_pnl_pct(self) -> float:
        """获取总盈亏百分比"""
        return self.get_pnl() / self.initial_capital

    def get_trade_count(self) -> int:
        """获取交易次数"""
        return len(self._trade_history)

    def get_stats(self) -> Dict:
        """获取统计信息"""
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

    def get_trade_history(self, limit: int = 100) -> List[Dict]:
        """获取交易历史"""
        return self._trade_history[-limit:]

    def reset(self) -> None:
        """重置模拟交易"""
        self._capital = self.initial_capital
        self._trade_history.clear()

        # 清除所有持仓
        for exchange in exchange_manager.get_connected_exchanges():
            symbols = exchange_manager.get_supported_symbols(exchange)
            for symbol in symbols:
                position_manager.close_position(exchange, symbol, 0)

        logger.info("Paper trading reset")


class RealTimeSimulator:
    """实时模拟器（用于策略测试）"""

    def __init__(self):
        self._running = False
        self._price_feeds: Dict[str, float] = {}
        self._callbacks: List[Callable] = []

    async def simulate_from_data(
        self,
        data: Dict[str, List],
        speed: float = 1.0,
    ) -> None:
        """
        从历史数据模拟

        Args:
            data: {symbol: [price1, price2, ...]}
            speed: 模拟速度（1.0 = 实时，10.0 = 10倍速）
        """
        self._running = True

        # 获取最大长度
        max_len = max(len(prices) for prices in data.values())

        for i in range(max_len):
            if not self._running:
                break

            # 更新价格
            for symbol, prices in data.items():
                if i < len(prices):
                    self._price_feeds[symbol] = prices[i]

            # 通知回调
            await self._notify_price_update()

            # 等待
            await asyncio.sleep(1.0 / speed)

    def register_price_callback(self, callback: Callable) -> None:
        """注册价格更新回调"""
        self._callbacks.append(callback)

    async def _notify_price_update(self) -> None:
        """通知价格更新"""
        for callback in self._callbacks:
            try:
                await callback(self._price_feeds.copy())
            except Exception as e:
                logger.error(f"Price callback error: {e}")

    def stop(self) -> None:
        """停止模拟"""
        self._running = False


# 全局实例
paper_trading_engine = PaperTradingEngine()
realtime_simulator = RealTimeSimulator()
