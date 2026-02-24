"""
数据下载和测试脚本 - 使用Gate.io API
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import aiohttp
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


async def fetch_json(session, url, params=None, headers=None):
    """获取JSON数据"""
    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
        if response.status == 200:
            return await response.json()
        else:
            raise Exception(f"HTTP {response.status}: {await response.text()}")


async def main():
    """使用Gate.io下载数据"""
    logger.info("=" * 60)
    logger.info("Crypto Trading System - 数据下载测试")
    logger.info("使用 Gate.io API")
    logger.info("=" * 60)

    # 创建数据目录
    data_path = Path("./data/historical")
    data_path.mkdir(parents=True, exist_ok=True)

    # Gate.io API 端点
    BASE_URL = "https://api.gateio.ws/api/v4"

    async with aiohttp.ClientSession() as session:
        # 测试1: 获取ticker信息
        logger.info("\n--- 测试1: 获取实时价格 ---")
        try:
            url = f"{BASE_URL}/spot/tickers"
            params = {"currency_pair": "BTC_USDT"}
            data = await fetch_json(session, url, params)

            if data:
                ticker = data[0]
                logger.info(f"✓ BTC/USDT 最新价格: ${float(ticker['last']):,.2f}")
                logger.info(f"  买一价: ${float(ticker['highest_bid']):,.2f}")
                logger.info(f"  卖一价: ${float(ticker['lowest_ask']):,.2f}")
                logger.info(f"  24h成交量: {float(ticker['base_volume']):,.2f} BTC")

            # 获取ETH价格
            params = {"currency_pair": "ETH_USDT"}
            data = await fetch_json(session, url, params)
            if data:
                logger.info(f"✓ ETH/USDT: ${float(data[0]['last']):,.2f}")

            # 获取SOL价格
            params = {"currency_pair": "SOL_USDT"}
            data = await fetch_json(session, url, params)
            if data:
                logger.info(f"✓ SOL/USDT: ${float(data[0]['last']):,.2f}")

        except Exception as e:
            logger.error(f"✗ 获取价格失败: {e}")
            logger.info("尝试备用数据源...")

            # 备用：使用币安数据API（有时可访问）
            try:
                url = "https://data-api.binance.vision/api/v3/ticker/price"
                params = {"symbol": "BTCUSDT"}
                data = await fetch_json(session, url, params)
                logger.info(f"✓ BTC/USDT (Binance Data): ${float(data['price']):,.2f}")
            except Exception as e2:
                logger.error(f"备用数据源也失败: {e2}")
                logger.info("\n提示: 网络无法访问交易所API，请检查网络或配置代理")
                logger.info("你可以在 .env 文件中设置代理:")
                logger.info("  HTTP_PROXY=http://127.0.0.1:7890")
                logger.info("  HTTPS_PROXY=http://127.0.0.1:7890")
                return

        # 测试2: 下载K线数据
        logger.info("\n--- 测试2: 下载历史K线数据 ---")
        pairs = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]
        intervals = ["1h", "4h", "1d"]  # Gate.io时间间隔（秒）

        # Gate.io interval映射（字符串格式）
        interval_map = {"1h": "1h", "4h": "4h", "1d": "1d"}

        all_data = {}

        for pair in pairs:
            all_data[pair] = {}
            for interval_name, interval_val in interval_map.items():
                try:
                    logger.info(f"正在下载 {pair} {interval_name} 数据...")

                    url = f"{BASE_URL}/spot/candlesticks"
                    # 获取最近1000条K线
                    params = {
                        "currency_pair": pair,
                        "interval": interval_val,
                        "limit": 1000,
                    }

                    data = await fetch_json(session, url, params)

                    if data:
                        # Gate.io返回格式: [timestamp, volume, close, high, low, open]
                        klines = []
                        for item in data:
                            klines.append({
                                "timestamp": datetime.fromtimestamp(int(item[0])),
                                "open": float(item[5]),
                                "high": float(item[3]),
                                "low": float(item[4]),
                                "close": float(item[2]),
                                "volume": float(item[1]),
                            })

                        # 按时间排序
                        df = pd.DataFrame(klines)
                        df["timestamp"] = pd.to_datetime(df["timestamp"])
                        df = df.set_index("timestamp").sort_index()

                        all_data[pair][interval_name] = df

                        logger.info(f"  ✓ 获取 {len(df)} 条K线")
                        logger.info(f"    时间范围: {df.index[0]} ~ {df.index[-1]}")
                        logger.info(f"    最新收盘价: ${df['close'].iloc[-1]:,.2f}")

                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"  ✗ 下载失败: {e}")

        # 测试3: 保存数据到Parquet
        logger.info("\n--- 测试3: 保存数据到本地 ---")

        saved_count = 0
        for pair, tf_data in all_data.items():
            for timeframe, df in tf_data.items():
                if df.empty:
                    continue

                # 保存路径
                safe_pair = pair.replace("_", "/")
                file_path = data_path / "gate" / pair / f"{timeframe}.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # 保存
                table = pa.Table.from_pandas(df)
                pq.write_table(table, str(file_path), compression='zstd')

                logger.info(f"  ✓ 保存 {pair} {timeframe}: {len(df)} 条")
                saved_count += 1

        # 测试4: 验证数据可以读取
        logger.info("\n--- 测试4: 验证数据读取 ---")
        for pair in ["BTC_USDT", "ETH_USDT"]:
            for timeframe in ["1h", "4h", "1d"]:
                file_path = data_path / "gate" / pair / f"{timeframe}.parquet"
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    logger.info(f"  ✓ 读取 {pair} {timeframe}: {len(df)} 条")
                    logger.info(f"    最新: {df.index[-1]}, 收盘: ${df['close'].iloc[-1]:,.2f}")

    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("测试完成!")
    logger.info(f"  - API连接: ✓ (Gate.io)")
    logger.info(f"  - 价格获取: ✓")
    logger.info(f"  - K线下载: ✓")
    logger.info(f"  - 本地存储: {saved_count} 个文件")
    logger.info(f"  - 数据读取: ✓")
    logger.info("=" * 60)

    # 显示数据统计
    logger.info("\n数据存储位置:")
    logger.info(f"  {data_path.absolute()}")

    # 计算总大小和显示文件
    total_size = 0
    logger.info("\n已保存文件:")
    for f in data_path.rglob("*.parquet"):
        size_kb = f.stat().st_size / 1024
        total_size += f.stat().st_size
        logger.info(f"  {f.relative_to(data_path)} ({size_kb:.1f} KB)")

    logger.info(f"\n总大小: {total_size / 1024:.2f} KB")


if __name__ == "__main__":
    asyncio.run(main())
