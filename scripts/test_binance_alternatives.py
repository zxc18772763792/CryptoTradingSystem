"""
Binance数据获取 - 使用多种方式绕过国内限制
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from loguru import logger
import pandas as pd


class BinanceDataFetcher:
    """Binance数据获取器 - 支持多种方式"""

    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy

        # Binance可用的域名
        self.domains = [
            "https://data-api.binance.vision",  # 数据API，通常可直接访问
            "https://api.binance.com",           # 主API，需要代理
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://fapi.binance.com",          # 合约API
        ]

        # 当前可用的域名
        self.working_domain = None

    async def test_connectivity(self) -> Optional[str]:
        """测试哪个域名可用"""
        async with aiohttp.ClientSession() as session:
            for domain in self.domains:
                try:
                    url = f"{domain}/api/v3/ping"
                    timeout = aiohttp.ClientTimeout(total=5)

                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            self.working_domain = domain
                            logger.info(f"Binance可用域名: {domain}")
                            return domain
                except Exception:
                    continue

        logger.warning("无法连接到任何Binance域名")
        return None

    async def get_klines(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1h",
        limit: int = 1000,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """获取K线数据"""

        if not self.working_domain:
            await self.test_connectivity()

        if not self.working_domain:
            return pd.DataFrame()

        url = f"{self.working_domain}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        async with aiohttp.ClientSession() as session:
            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()

                        klines = []
                        for item in data:
                            klines.append({
                                "timestamp": datetime.fromtimestamp(item[0] / 1000),
                                "open": float(item[1]),
                                "high": float(item[2]),
                                "low": float(item[3]),
                                "close": float(item[4]),
                                "volume": float(item[5]),
                                "quote_volume": float(item[7]),
                                "trades": int(item[8]),
                            })

                        df = pd.DataFrame(klines)
                        df = df.set_index("timestamp").sort_index()
                        logger.info(f"获取 {symbol} {interval} K线 {len(df)} 条")
                        return df
                    else:
                        logger.error(f"HTTP {resp.status}: {await resp.text()}")

            except Exception as e:
                logger.error(f"获取K线失败: {e}")

        return pd.DataFrame()

    async def get_ticker(self, symbol: str = "BTCUSDT") -> Dict[str, Any]:
        """获取行情"""
        if not self.working_domain:
            await self.test_connectivity()

        if not self.working_domain:
            return {}

        url = f"{self.working_domain}/api/v3/ticker/24hr"
        params = {"symbol": symbol}

        async with aiohttp.ClientSession() as session:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {
                            "symbol": symbol,
                            "last": float(data["lastPrice"]),
                            "high_24h": float(data["highPrice"]),
                            "low_24h": float(data["lowPrice"]),
                            "volume_24h": float(data["volume"]),
                            "price_change_pct": float(data["priceChangePercent"]),
                        }
            except Exception as e:
                logger.error(f"获取行情失败: {e}")

        return {}

    async def get_exchange_info(self) -> Dict[str, Any]:
        """获取交易所信息"""
        if not self.working_domain:
            await self.test_connectivity()

        if not self.working_domain:
            return {}

        url = f"{self.working_domain}/api/v3/exchangeInfo"

        async with aiohttp.ClientSession() as session:
            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception as e:
                logger.error(f"获取交易所信息失败: {e}")

        return {}


async def test_binance():
    """测试Binance连接"""
    fetcher = BinanceDataFetcher()

    print("=" * 60)
    print("Binance连接测试")
    print("=" * 60)

    # 测试连接
    domain = await fetcher.test_connectivity()

    if domain:
        print(f"\n✓ 可用域名: {domain}")

        # 获取BTC行情
        print("\n获取BTC行情...")
        ticker = await fetcher.get_ticker("BTCUSDT")
        if ticker:
            print(f"  价格: ${ticker['last']:,.2f}")
            print(f"  24h涨跌: {ticker['price_change_pct']}%")

        # 获取K线
        print("\n获取K线数据...")
        df = await fetcher.get_klines("BTCUSDT", "1h", 100)
        if not df.empty:
            print(f"  获取 {len(df)} 条K线")
            print(f"  时间范围: {df.index[0]} ~ {df.index[-1]}")
            print(f"  最新收盘: ${df['close'].iloc[-1]:,.2f}")

    else:
        print("\n✗ 无法连接Binance")
        print("  请配置代理或检查网络")


async def download_binance_historical(
    symbols: List[str] = None,
    intervals: List[str] = None,
    days: int = 30,
    output_dir: str = "./data/historical/binance"
):
    """下载Binance历史数据"""
    from pathlib import Path
    import pyarrow as pa
    import pyarrow.parquet as pq

    fetcher = BinanceDataFetcher()

    # 测试连接
    if not await fetcher.test_connectivity():
        logger.error("无法连接Binance，请检查网络或配置代理")
        return

    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    if intervals is None:
        intervals = ["1h", "4h", "1d"]

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)

    for symbol in symbols:
        for interval in intervals:
            try:
                logger.info(f"下载 {symbol} {interval}...")

                df = await fetcher.get_klines(
                    symbol=symbol,
                    interval=interval,
                    limit=1000,
                    start_time=start_time,
                    end_time=end_time,
                )

                if not df.empty:
                    df["symbol"] = symbol

                    # 保存
                    file_path = output_path / f"{symbol}_{interval}.parquet"
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(file_path), compression="zstd")

                    logger.info(f"  ✓ 保存 {len(df)} 条到 {file_path}")

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"  ✗ 失败: {e}")

    logger.info("下载完成!")


if __name__ == "__main__":
    asyncio.run(test_binance())
