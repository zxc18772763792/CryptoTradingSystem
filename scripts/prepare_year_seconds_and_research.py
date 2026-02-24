"""Prepare 1-year second-level data and run strategy research."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data import download_binance_1s_daily_archive, second_level_backfill_manager  # noqa: E402
from core.exchanges import exchange_manager  # noqa: E402
from core.research import ResearchConfig, run_strategy_research  # noqa: E402


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


async def _run_gate_backfill(symbol: str, days: int, max_hours: float, poll_seconds: int) -> Dict:
    await exchange_manager.initialize(["gate"])

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    task = second_level_backfill_manager.start_task(
        exchange="gate",
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        window_days=1,
    )
    task_id = task["task_id"]
    logger.info(f"Gate 秒级回填任务已启动: {task_id}")

    started = datetime.utcnow()
    while True:
        state = second_level_backfill_manager.get_task(task_id) or {}
        status = state.get("status")
        logger.info(
            f"Gate backfill status={status} progress={state.get('progress_ratio', 0):.2%} "
            f"bars={state.get('total_bars', 0)} error={state.get('last_error', '')[:120]}"
        )
        if status in {"completed", "failed", "stopped"}:
            await exchange_manager.close_all()
            return state

        if max_hours > 0:
            elapsed = (datetime.utcnow() - started).total_seconds() / 3600.0
            if elapsed >= max_hours:
                second_level_backfill_manager.stop_task(task_id)
                await exchange_manager.close_all()
                return second_level_backfill_manager.get_task(task_id) or {}

        await asyncio.sleep(max(2, poll_seconds))


async def main() -> None:
    parser = argparse.ArgumentParser(description="准备1年秒级数据并执行策略研究")
    parser.add_argument("--symbol", default="BTC/USDT", help="交易对")
    parser.add_argument("--days", type=int, default=365, help="数据天数")
    parser.add_argument("--initial-capital", type=float, default=10000.0, help="研究初始资金")
    parser.add_argument("--commission-rate", type=float, default=0.0004, help="单边手续费率")
    parser.add_argument("--slippage-bps", type=float, default=2.0, help="单边滑点(bps)")
    parser.add_argument("--include-gate", action="store_true", help="额外尝试 Gate 秒级回填")
    parser.add_argument("--gate-max-hours", type=float, default=0.0, help="Gate 最长等待小时")
    parser.add_argument("--poll-seconds", type=int, default=20, help="Gate 回填轮询秒")
    parser.add_argument("--output-dir", default="", help="研究结果输出目录")
    parser.add_argument("--log-file", default="", help="日志文件路径")
    args = parser.parse_args()

    if args.log_file:
        logger.add(args.log_file, rotation="100 MB", retention="14 days", encoding="utf-8")

    symbol = _normalize_symbol(args.symbol)
    days = max(1, min(1200, int(args.days)))
    end_date = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)

    logger.info(f"Step1/3 下载 Binance 1s 历史包: symbol={symbol} days={days}")
    archive_stats = download_binance_1s_daily_archive(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        skip_existing=True,
    )
    logger.info(f"Binance 下载统计: {archive_stats.to_dict()}")

    gate_state: Dict = {}
    if args.include_gate:
        logger.info("Step2/3 启动 Gate 秒级回填")
        gate_state = await _run_gate_backfill(
            symbol=symbol,
            days=days,
            max_hours=max(0.0, float(args.gate_max_hours)),
            poll_seconds=max(2, int(args.poll_seconds)),
        )
    else:
        logger.info("Step2/3 跳过 Gate 秒级回填（未开启 --include-gate）")

    logger.info("Step3/3 执行 Binance 秒级策略研究")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else ResearchConfig().output_dir
    research_result = await run_strategy_research(
        ResearchConfig(
            exchange="binance",
            symbol=symbol,
            days=days,
            initial_capital=max(10.0, float(args.initial_capital)),
            commission_rate=max(0.0, float(args.commission_rate)),
            slippage_bps=max(0.0, float(args.slippage_bps)),
            output_dir=output_dir,
        )
    )

    summary = {
        "archive": archive_stats.to_dict(),
        "gate_backfill": gate_state,
        "research": research_result,
    }
    logger.info("流程完成")
    logger.info(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
