"""
数据采集器模块
支持从多个交易所采集K线、行情等数据
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum
from loguru import logger

from core.exchanges import Kline, Ticker
from core.exchanges.exchange_manager import exchange_manager
from config.settings import settings


def _utc_now() -> datetime:
    """tz-aware UTC now — matches Kline.timestamp (tz-aware) used elsewhere on `task.last_collected`."""
    return datetime.now(timezone.utc)


class DataType(Enum):
    """数据类型"""
    KLINE = "kline"
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    TRADE = "trade"
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"
    FEAR_GREED = "fear_greed"


@dataclass
class CollectionTask:
    """采集任务"""
    exchange: str
    symbol: str
    data_type: DataType
    timeframe: Optional[str] = None
    interval: int = 60  # 采集间隔（秒）
    last_collected: Optional[datetime] = None


class DataCollector:
    """数据采集器"""

    def __init__(self):
        self._tasks: Dict[str, CollectionTask] = {}
        self._running: bool = False
        self._callbacks: Dict[DataType, List[callable]] = {
            DataType.KLINE: [],
            DataType.TICKER: [],
            DataType.ORDERBOOK: [],
            DataType.TRADE: [],
            DataType.FUNDING_RATE: [],
            DataType.OPEN_INTEREST: [],
            DataType.FEAR_GREED: [],
        }
        self._collected_data: Dict[str, List[Any]] = {}
        self._callback_failures: int = 0
        self._callback_failure_counts: Dict[DataType, int] = {
            data_type: 0 for data_type in DataType
        }
        self._last_callback_error: Optional[Dict[str, Any]] = None

        # 子采集器实例
        self._funding_rate_collector = None
        self._fear_greed_collector = None

        # The collection loop task. Saved as an attribute (not just a local) so
        # the asyncio event loop won't garbage-collect it mid-execution — see
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        # ("Important: Save a reference to the result of this function...").
        self._loop_task: Optional[asyncio.Task] = None

    def add_task(
        self,
        exchange: str,
        symbol: str,
        data_type: DataType,
        timeframe: Optional[str] = None,
        interval: int = 60,
    ) -> str:
        """
        添加采集任务

        Args:
            exchange: 交易所名称
            symbol: 交易对
            data_type: 数据类型
            timeframe: 时间框架（仅K线需要）
            interval: 采集间隔（秒）

        Returns:
            任务ID
        """
        task_id = f"{exchange}_{symbol}_{data_type.value}_{timeframe or ''}"
        task = CollectionTask(
            exchange=exchange,
            symbol=symbol,
            data_type=data_type,
            timeframe=timeframe,
            interval=interval,
        )
        self._tasks[task_id] = task
        self._collected_data[task_id] = []

        logger.info(f"Added collection task: {task_id}")
        return task_id

    def remove_task(self, task_id: str) -> bool:
        """移除采集任务"""
        if task_id in self._tasks:
            del self._tasks[task_id]
            if task_id in self._collected_data:
                del self._collected_data[task_id]
            logger.info(f"Removed collection task: {task_id}")
            return True
        return False

    def register_callback(self, data_type: DataType, callback: callable) -> None:
        """注册数据回调函数"""
        self._callbacks[data_type].append(callback)

    async def _collect_kline(self, task: CollectionTask) -> List[Kline]:
        """采集K线数据"""
        exchange = exchange_manager.get_exchange(task.exchange)
        if not exchange:
            logger.warning(f"Exchange not found: {task.exchange}")
            return []

        try:
            since = task.last_collected or _utc_now() - timedelta(days=1)
            klines = await exchange.get_klines(
                symbol=task.symbol,
                timeframe=task.timeframe,
                since=since,
                limit=settings.MAX_CANDLES_PER_REQUEST,
            )

            if klines:
                task.last_collected = klines[-1].timestamp

            return klines

        except Exception as e:
            logger.error(f"Failed to collect kline data: {e}")
            return []

    async def _collect_ticker(self, task: CollectionTask) -> Optional[Ticker]:
        """采集行情数据"""
        exchange = exchange_manager.get_exchange(task.exchange)
        if not exchange:
            return None

        try:
            ticker = await exchange.get_ticker(task.symbol)
            task.last_collected = _utc_now()
            return ticker

        except Exception as e:
            logger.error(f"Failed to collect ticker data: {e}")
            return None

    async def _collect_orderbook(self, task: CollectionTask) -> Optional[Dict]:
        """采集订单簿数据"""
        exchange = exchange_manager.get_exchange(task.exchange)
        if not exchange:
            return None

        try:
            orderbook = await exchange.get_order_book(task.symbol)
            task.last_collected = _utc_now()
            return orderbook

        except Exception as e:
            logger.error(f"Failed to collect orderbook data: {e}")
            return None
    
    async def _collect_funding_rate(self, task: CollectionTask) -> Optional[Dict]:
        """采集资金费率数据"""
        try:
            from core.data.coinglass_client import coinglass_enabled

            if coinglass_enabled():
                from core.data.coinglass_feature_builder import build_coinglass_overview_payload

                overview = await build_coinglass_overview_payload(
                    symbol=task.symbol,
                    refresh=True,
                    manual=False,
                )
                snapshot = dict((overview or {}).get("snapshot") or {})
                funding_rate = snapshot.get("funding_rate") or snapshot.get("funding_rate_oi_weighted")
                if funding_rate is not None:
                    task.last_collected = _utc_now()
                    return {
                        "source": "coinglass_cache",
                        "symbol": task.symbol,
                        "funding_rate": float(funding_rate),
                        "timestamp": snapshot.get("timestamp"),
                        "freshness_sec": (overview or {}).get("freshness_sec"),
                    }

            # 懒加载采集器
            if self._funding_rate_collector is None:
                from core.data.funding_rate_collector import FundingRateCollector
                self._funding_rate_collector = FundingRateCollector()
            
            # 获取所有交易所的资金费率
            rates = await self._funding_rate_collector.fetch_all(task.symbol)
            task.last_collected = _utc_now()
            return rates

        except Exception as e:
            logger.error(f"Failed to collect funding rate data: {e}")
            return None
    
    async def _collect_fear_greed(self, task: CollectionTask) -> Optional[Dict]:
        """采集恐惧贪婪指数"""
        try:
            # 懒加载采集器
            if self._fear_greed_collector is None:
                from core.data.sentiment.fear_greed_collector import FearGreedCollector
                self._fear_greed_collector = FearGreedCollector()
            
            index = await self._fear_greed_collector.fetch_current()
            task.last_collected = _utc_now()
            return index

        except Exception as e:
            logger.error(f"Failed to collect fear & greed index: {e}")
            return None

    async def _process_task(self, task_id: str) -> None:
        """处理单个采集任务"""
        task = self._tasks.get(task_id)
        if not task:
            return

        data = None

        if task.data_type == DataType.KLINE:
            data = await self._collect_kline(task)
        elif task.data_type == DataType.TICKER:
            data = await self._collect_ticker(task)
        elif task.data_type == DataType.ORDERBOOK:
            data = await self._collect_orderbook(task)
        elif task.data_type == DataType.FUNDING_RATE:
            data = await self._collect_funding_rate(task)
        elif task.data_type == DataType.FEAR_GREED:
            data = await self._collect_fear_greed(task)

        if data:
            # 存储数据
            if isinstance(data, list):
                self._collected_data[task_id].extend(data)
            else:
                self._collected_data[task_id].append(data)

            # 触发回调
            for callback in self._callbacks[task.data_type]:
                try:
                    await callback(task, data)
                except Exception as e:
                    self._callback_failures += 1
                    self._callback_failure_counts[task.data_type] = (
                        int(self._callback_failure_counts.get(task.data_type, 0)) + 1
                    )
                    self._last_callback_error = {
                        "task_id": task_id,
                        "data_type": task.data_type.value,
                        "error": str(e),
                        "timestamp": _utc_now().isoformat(),
                    }
                    logger.error(f"Callback error: {e}")

    async def _run_collection_loop(self) -> None:
        """运行采集循环"""
        while self._running:
            tasks_to_run = []

            for task_id, task in self._tasks.items():
                # 检查是否需要采集
                if task.last_collected is None:
                    tasks_to_run.append(self._process_task(task_id))
                else:
                    elapsed = (_utc_now() - task.last_collected).total_seconds()
                    if elapsed >= task.interval:
                        tasks_to_run.append(self._process_task(task_id))

            if tasks_to_run:
                await asyncio.gather(*tasks_to_run, return_exceptions=True)

            await asyncio.sleep(1)

    async def start(self) -> None:
        """启动数据采集"""
        if self._running:
            return

        self._running = True
        logger.info("Data collector started")

        # 启动采集循环 — keep a reference so GC doesn't kill the loop.
        self._loop_task = asyncio.create_task(
            self._run_collection_loop(), name="data_collector_loop"
        )

    async def stop(self) -> None:
        """停止数据采集"""
        self._running = False
        task = self._loop_task
        self._loop_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                # CancelledError is expected; any other exception is the loop's
                # exit error which we already logged. Don't propagate here so
                # the rest of shutdown can proceed.
                pass
        logger.info("Data collector stopped")

    def get_collected_data(self, task_id: str) -> List[Any]:
        """获取采集的数据"""
        return self._collected_data.get(task_id, [])

    def clear_collected_data(self, task_id: str) -> None:
        """清除已采集的数据"""
        if task_id in self._collected_data:
            self._collected_data[task_id] = []

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running

    @property
    def task_count(self) -> int:
        """任务数量"""
        return len(self._tasks)

    def get_task_info(self, task_id: str) -> Optional[Dict]:
        """获取任务信息"""
        task = self._tasks.get(task_id)
        if task:
            return {
                "exchange": task.exchange,
                "symbol": task.symbol,
                "data_type": task.data_type.value,
                "timeframe": task.timeframe,
                "interval": task.interval,
                "last_collected": task.last_collected.isoformat() if task.last_collected else None,
            }
        return None

    def list_tasks(self) -> List[Dict]:
        """列出所有任务"""
        return [
            self.get_task_info(task_id)
            for task_id in self._tasks
        ]

    def get_diagnostics(self) -> Dict[str, Any]:
        return {
            "callback_failures": int(self._callback_failures),
            "callback_failures_by_type": {
                data_type.value: int(count)
                for data_type, count in self._callback_failure_counts.items()
            },
            "last_callback_error": dict(self._last_callback_error)
            if self._last_callback_error
            else None,
        }


# 全局数据采集器实例
data_collector = DataCollector()
