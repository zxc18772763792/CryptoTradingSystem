"""
历史数据管理模块
负责批量下载、更新和管理历史数据
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from loguru import logger

from config.settings import settings
from core.exchanges import exchange_manager, Kline
from core.data.data_storage import data_storage
from core.data.data_processor import data_processor


@dataclass
class DownloadProgress:
    """下载进度"""
    exchange: str
    symbol: str
    timeframe: str
    total_candles: int
    downloaded_candles: int
    start_time: datetime
    end_time: datetime
    current_time: datetime
    is_complete: bool = False


class HistoricalDataManager:
    """历史数据管理器"""

    def __init__(self):
        self._download_tasks: Dict[str, DownloadProgress] = {}

    async def download_historical_klines(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        save_to_parquet: bool = True,
        progress_callback: Optional[callable] = None,
    ) -> List[Kline]:
        """
        下载历史K线数据

        Args:
            exchange: 交易所名称
            symbol: 交易对
            timeframe: 时间框架
            start_time: 开始时间
            end_time: 结束时间
            save_to_parquet: 是否保存到Parquet文件
            progress_callback: 进度回调函数

        Returns:
            K线数据列表
        """
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            logger.error(f"Exchange not found: {exchange}")
            return []

        # 设置默认时间范围
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(days=365)  # 默认下载1年数据

        # 创建任务ID
        task_id = f"{exchange}_{symbol}_{timeframe}"

        # 初始化进度
        self._download_tasks[task_id] = DownloadProgress(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            total_candles=0,
            downloaded_candles=0,
            start_time=start_time,
            end_time=end_time,
            current_time=start_time,
        )

        all_klines = []
        current_time = start_time

        logger.info(f"Starting download: {task_id} from {start_time} to {end_time}")

        while current_time < end_time:
            try:
                # 获取数据
                klines = await connector.get_klines(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=current_time,
                    limit=settings.MAX_CANDLES_PER_REQUEST,
                )

                if not klines:
                    break

                # 更新进度
                self._download_tasks[task_id].downloaded_candles += len(klines)
                self._download_tasks[task_id].current_time = klines[-1].timestamp

                # 过滤超出时间范围的数据
                filtered_klines = [
                    k for k in klines
                    if start_time <= k.timestamp <= end_time
                ]
                all_klines.extend(filtered_klines)

                # 更新下一次请求的起始时间
                current_time = klines[-1].timestamp + timedelta(milliseconds=1)

                # 回调进度
                if progress_callback:
                    await progress_callback(self._download_tasks[task_id])

                logger.debug(
                    f"Downloaded {len(klines)} candles, "
                    f"total: {len(all_klines)}, "
                    f"current: {klines[-1].timestamp}"
                )

                # 避免请求过快
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Download error: {e}")
                await asyncio.sleep(5)
                continue

        # 去重和排序
        unique_klines = self._deduplicate_klines(all_klines)

        # 保存数据
        if save_to_parquet and unique_klines:
            await data_storage.save_klines_to_parquet(
                unique_klines,
                exchange,
                symbol,
                timeframe,
            )

        # 标记完成
        self._download_tasks[task_id].is_complete = True
        self._download_tasks[task_id].total_candles = len(unique_klines)

        logger.info(
            f"Download complete: {task_id}, "
            f"total candles: {len(unique_klines)}"
        )

        return unique_klines

    def _deduplicate_klines(self, klines: List[Kline]) -> List[Kline]:
        """去重K线数据"""
        seen = set()
        unique = []

        for kline in klines:
            key = kline.timestamp.isoformat()
            if key not in seen:
                seen.add(key)
                unique.append(kline)

        return sorted(unique, key=lambda x: x.timestamp)

    async def download_multiple_symbols(
        self,
        exchange: str,
        symbols: List[str],
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, List[Kline]]:
        """批量下载多个交易对的数据"""
        results = {}

        for symbol in symbols:
            try:
                klines = await self.download_historical_klines(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=end_time,
                )
                results[symbol] = klines

                # 避免请求过快
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Failed to download {symbol}: {e}")
                results[symbol] = []

        return results

    async def update_historical_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> int:
        """
        更新历史数据（增量更新）

        Args:
            exchange: 交易所名称
            symbol: 交易对
            timeframe: 时间框架

        Returns:
            新增的K线数量
        """
        # 获取本地最新数据时间
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        if df.empty:
            # 没有历史数据，下载全部
            start_time = None
        else:
            # 从最新数据开始更新
            start_time = df.index.max() + timedelta(milliseconds=1)

        # 下载新数据
        new_klines = await self.download_historical_klines(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=datetime.now(),
        )

        return len(new_klines)

    async def update_all_data(
        self,
        exchanges: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, int]]:
        """
        更新所有数据

        Args:
            exchanges: 要更新的交易所列表，None表示全部

        Returns:
            更新结果统计
        """
        if exchanges is None:
            exchanges = exchange_manager.get_connected_exchanges()

        results = {}

        for exchange in exchanges:
            symbols = exchange_manager.get_supported_symbols(exchange)
            timeframes = ["1h", "4h", "1d"]  # 默认更新的时间框架

            results[exchange] = {}

            for symbol in symbols:
                for timeframe in timeframes:
                    try:
                        count = await self.update_historical_data(
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                        )
                        results[exchange][f"{symbol}_{timeframe}"] = count

                    except Exception as e:
                        logger.error(f"Update failed: {exchange} {symbol} {timeframe}: {e}")
                        results[exchange][f"{symbol}_{timeframe}"] = 0

        return results

    def get_download_progress(self, task_id: str) -> Optional[DownloadProgress]:
        """获取下载进度"""
        return self._download_tasks.get(task_id)

    def list_download_tasks(self) -> List[Dict]:
        """列出所有下载任务"""
        return [
            {
                "task_id": task_id,
                "exchange": task.exchange,
                "symbol": task.symbol,
                "timeframe": task.timeframe,
                "downloaded": task.downloaded_candles,
                "is_complete": task.is_complete,
            }
            for task_id, task in self._download_tasks.items()
        ]

    async def get_data_coverage(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Dict:
        """获取数据覆盖情况"""
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        if df.empty:
            return {
                "has_data": False,
                "start": None,
                "end": None,
                "count": 0,
                "days": 0,
            }

        return {
            "has_data": True,
            "start": df.index.min().isoformat(),
            "end": df.index.max().isoformat(),
            "count": len(df),
            "days": (df.index.max() - df.index.min()).days,
        }


# 全局历史数据管理器实例
historical_data_manager = HistoricalDataManager()
