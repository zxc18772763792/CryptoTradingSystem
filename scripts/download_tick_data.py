"""
高频数据下载脚本 - 下载秒级/分钟级数据
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import aiohttp

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


async def fetch_json(session, url, params=None, timeout=30):
    """获取JSON数据"""
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
        if response.status == 200:
            return await response.json()
        else:
            raise Exception(f"HTTP {response.status}: {await response.text()}")


async def download_high_freq_data():
    """下载高频数据"""
    logger.info("=" * 60)
    logger.info("高频数据下载 - Gate.io")
    logger.info("=" * 60)

    data_path = Path("./data/historical")
    data_path.mkdir(parents=True, exist_ok=True)

    BASE_URL = "https://api.gateio.ws/api/v4"

    # 高频时间框架
    # 注意: Gate.io支持10s, 但30s不支持
    timeframes = ["10s", "1m", "5m", "15m", "30m"]
    pairs = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]

    # 高频数据只保留较短期限
    limits = {
        "10s": 1000,   # 约2.7小时
        "1m": 1000,    # 约16小时
        "5m": 1000,    # 约3.5天
        "15m": 1000,   # 约10天
        "30m": 1000,   # 约20天
    }

    all_data = {}

    async with aiohttp.ClientSession() as session:
        for pair in pairs:
            all_data[pair] = {}

            for tf in timeframes:
                try:
                    logger.info(f"下载 {pair} {tf} 数据...")

                    url = f"{BASE_URL}/spot/candlesticks"
                    params = {
                        "currency_pair": pair,
                        "interval": tf,
                        "limit": limits.get(tf, 1000),
                    }

                    data = await fetch_json(session, url, params, timeout=30)

                    if data:
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

                        df = pd.DataFrame(klines)
                        df["timestamp"] = pd.to_datetime(df["timestamp"])
                        df = df.set_index("timestamp").sort_index()

                        all_data[pair][tf] = df

                        logger.info(f"  ✓ 获取 {len(df)} 条 {tf} K线")
                        logger.info(f"    时间范围: {df.index[0]} ~ {df.index[-1]}")

                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"  ✗ 下载失败: {e}")

        # 保存数据
        logger.info("\n保存高频数据...")

        for pair, tf_data in all_data.items():
            for timeframe, df in tf_data.items():
                if df.empty:
                    continue

                file_path = data_path / "gate" / pair / f"{timeframe}.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)

                table = pa.Table.from_pandas(df)
                pq.write_table(table, str(file_path), compression='zstd')

                logger.info(f"  ✓ 保存 {pair} {timeframe}: {len(df)} 条")

    logger.info("\n高频数据下载完成!")


async def get_realtime_tick(session, symbol="BTC_USDT"):
    """获取实时tick数据"""
    BASE_URL = "https://api.gateio.ws/api/v4"

    # 获取最近交易
    url = f"{BASE_URL}/spot/trades"
    params = {"currency_pair": symbol, "limit": 100}

    data = await fetch_json(session, url, params)

    if data:
        trades = []
        for trade in data:
            trades.append({
                "id": trade["id"],
                "timestamp": datetime.fromtimestamp(trade["create_time_ms"] / 1000),
                "price": float(trade["price"]),
                "amount": float(trade["amount"]),
                "side": "buy" if trade["side"] == "sell" else "sell",  # 注意：API返回的是taker方向
            })

        df = pd.DataFrame(trades)
        df = df.set_index("timestamp").sort_index()
        return df

    return pd.DataFrame()


async def stream_realtime_trades(symbol="BTC_USDT", duration_seconds=60):
    """实时交易流 (轮询方式)"""
    logger.info(f"开始实时交易流监控: {symbol}")
    logger.info(f"持续时间: {duration_seconds}秒")

    BASE_URL = "https://api.gateio.ws/api/v4"
    all_trades = []
    last_id = None

    async with aiohttp.ClientSession() as session:
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < duration_seconds:
            try:
                url = f"{BASE_URL}/spot/trades"
                params = {"currency_pair": symbol, "limit": 100}

                if last_id:
                    params["last_id"] = last_id

                data = await fetch_json(session, url, params, timeout=10)

                if data:
                    for trade in data:
                        trade_data = {
                            "timestamp": datetime.fromtimestamp(trade["create_time_ms"] / 1000),
                            "price": float(trade["price"]),
                            "amount": float(trade["amount"]),
                            "side": "buy" if trade["side"] == "sell" else "sell",
                        }
                        all_trades.append(trade_data)
                        last_id = trade["id"]

                    # 显示最新交易
                    latest = data[-1]
                    logger.info(
                        f"  {latest['price']} | "
                        f"{latest['amount']} | "
                        f"{'🟢' if latest['side'] == 'sell' else '🔴'}"
                    )

            except Exception as e:
                logger.error(f"获取交易数据失败: {e}")

            await asyncio.sleep(1)  # 每秒轮询一次

    if all_trades:
        df = pd.DataFrame(all_trades)
        df = df.set_index("timestamp").sort_index()
        logger.info(f"\n收集到 {len(df)} 条交易记录")
        return df

    return pd.DataFrame()


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="高频数据下载")
    parser.add_argument("--mode", choices=["download", "stream", "tick"], default="download")
    parser.add_argument("--symbol", default="BTC_USDT", help="交易对")
    parser.add_argument("--duration", type=int, default=60, help="流持续时间(秒)")

    args = parser.parse_args()

    if args.mode == "download":
        await download_high_freq_data()
    elif args.mode == "stream":
        await stream_realtime_trades(args.symbol, args.duration)
    elif args.mode == "tick":
        async with aiohttp.ClientSession() as session:
            df = await get_realtime_tick(session, args.symbol)
            if not df.empty:
                logger.info(f"\n最近100条交易:")
                print(df)


if __name__ == "__main__":
    asyncio.run(main())
