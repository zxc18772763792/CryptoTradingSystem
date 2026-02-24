"""Prepare second-level historical data with resumable backfill tasks."""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data import second_level_backfill_manager
from core.exchanges import exchange_manager


def _parse_csv(value: str) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _normalize_symbol(value: str) -> str:
    raw = value.strip().upper()
    if "/" in raw:
        return raw
    if "_" in raw:
        left, right = raw.split("_", 1)
        return f"{left}/{right}"
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


def _format_task(task: dict) -> str:
    progress = float(task.get("progress_ratio", 0.0)) * 100
    return (
        f"{task.get('task_id')} | status={task.get('status')} | "
        f"progress={progress:.2f}% ({task.get('completed_days', 0)}/{task.get('total_days', 0)}d) | "
        f"bars={task.get('total_bars', 0)} | trades={task.get('total_trades', 0)} | "
        f"error={task.get('last_error', '')[:120]}"
    )


async def _wait_tasks(task_ids: List[str], poll_seconds: int, max_hours: float) -> None:
    start_ts = datetime.utcnow()
    while True:
        tasks = [second_level_backfill_manager.get_task(task_id) for task_id in task_ids]
        tasks = [task for task in tasks if task]
        if not tasks:
            logger.error("任务列表为空，停止等待。")
            return

        logger.info("------ 秒级回填进度 ------")
        for task in tasks:
            logger.info(_format_task(task))

        done_states = {"completed", "stopped", "failed"}
        if all(task.get("status") in done_states for task in tasks):
            logger.info("所有任务已结束。")
            return

        if max_hours > 0:
            elapsed_hours = (datetime.utcnow() - start_ts).total_seconds() / 3600.0
            if elapsed_hours >= max_hours:
                logger.warning(f"达到最大运行时长 {max_hours} 小时，停止等待。")
                return

        await asyncio.sleep(max(2, poll_seconds))


async def main() -> None:
    parser = argparse.ArgumentParser(description="准备过去N天的秒级数据（支持断点续跑）")
    parser.add_argument("--exchanges", default="binance,gate", help="交易所列表，逗号分隔")
    parser.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="交易对列表，逗号分隔")
    parser.add_argument("--days", type=int, default=365, help="回填天数")
    parser.add_argument("--window-days", type=int, default=1, help="每个任务窗口天数")
    parser.add_argument("--poll-seconds", type=int, default=20, help="进度轮询间隔秒")
    parser.add_argument("--max-hours", type=float, default=0.0, help="最大等待小时数，0表示不限")
    parser.add_argument("--no-wait", action="store_true", help="仅启动任务，不等待完成")
    parser.add_argument("--log-file", default="", help="日志文件路径")
    args = parser.parse_args()

    if args.log_file:
        logger.add(args.log_file, rotation="100 MB", retention="14 days", encoding="utf-8")

    exchanges = [ex.lower() for ex in _parse_csv(args.exchanges)]
    symbols = [_normalize_symbol(item) for item in _parse_csv(args.symbols)]
    days = max(1, min(1200, int(args.days)))
    window_days = max(1, min(7, int(args.window_days)))

    logger.info(f"准备秒级数据: exchanges={exchanges} symbols={symbols} days={days}")
    await exchange_manager.initialize(exchanges)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    task_ids: List[str] = []
    for exchange in exchanges:
        for symbol in symbols:
            result = second_level_backfill_manager.start_task(
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                window_days=window_days,
            )
            task_ids.append(result["task_id"])
            logger.info(
                f"启动任务: {result['task_id']} started={result.get('started')} "
                f"status={result.get('task', {}).get('status')}"
            )

    if not args.no_wait:
        await _wait_tasks(task_ids, poll_seconds=args.poll_seconds, max_hours=float(args.max_hours))
    else:
        logger.warning("当前为 no-wait 模式，脚本结束后后台协程会终止。建议使用默认等待模式。")

    await exchange_manager.close_all()
    logger.info("秒级数据准备流程结束。")


if __name__ == "__main__":
    asyncio.run(main())

