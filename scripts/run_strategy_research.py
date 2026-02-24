"""Run strategy research on prepared second-level data."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import List

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.research import (  # noqa: E402
    SUPPORTED_RESEARCH_TIMEFRAMES,
    ResearchConfig,
    run_strategy_research,
)


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


async def main() -> None:
    parser = argparse.ArgumentParser(description="秒级数据策略研究")
    parser.add_argument("--exchange", default="binance", help="交易所")
    parser.add_argument("--symbol", default="BTC/USDT", help="交易对")
    parser.add_argument("--days", type=int, default=365, help="研究样本天数")
    parser.add_argument("--initial-capital", type=float, default=10000.0, help="初始资金")
    parser.add_argument(
        "--timeframes",
        default="1s,5s,10s,30s,1m,5m,15m,30m,1h",
        help=f"研究周期，逗号分隔，可选: {','.join(SUPPORTED_RESEARCH_TIMEFRAMES)}",
    )
    parser.add_argument(
        "--strategies",
        default=(
            "MAStrategy,EMAStrategy,RSIStrategy,RSIDivergenceStrategy,MACDStrategy,MACDHistogramStrategy,"
            "BollingerBandsStrategy,BollingerSqueezeStrategy,MeanReversionStrategy,BollingerMeanReversionStrategy,"
            "MomentumStrategy,TrendFollowingStrategy,PairsTradingStrategy,DonchianBreakoutStrategy,StochasticStrategy,"
            "ADXTrendStrategy,VWAPReversionStrategy,MarketSentimentStrategy,SocialSentimentStrategy,FundFlowStrategy,"
            "WhaleActivityStrategy"
        ),
        help="策略列表，逗号分隔",
    )
    parser.add_argument("--min-rows", type=int, default=300, help="每个周期最少K线数量")
    parser.add_argument("--commission-rate", type=float, default=0.0004, help="单边手续费率，如 0.0004=0.04%")
    parser.add_argument("--slippage-bps", type=float, default=2.0, help="单边滑点，基点 bps")
    parser.add_argument("--output-dir", default="", help="输出目录")
    args = parser.parse_args()

    symbol = _normalize_symbol(args.symbol)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None

    config = ResearchConfig(
        exchange=args.exchange.strip().lower(),
        symbol=symbol,
        days=max(1, int(args.days)),
        initial_capital=max(10.0, float(args.initial_capital)),
        timeframes=_parse_csv(args.timeframes),
        strategies=_parse_csv(args.strategies),
        min_rows_per_timeframe=max(80, int(args.min_rows)),
        commission_rate=max(0.0, float(args.commission_rate)),
        slippage_bps=max(0.0, float(args.slippage_bps)),
        output_dir=output_dir if output_dir else ResearchConfig().output_dir,
    )

    logger.info(
        f"启动策略研究: exchange={config.exchange} symbol={config.symbol} "
        f"days={config.days} timeframes={config.timeframes} strategies={len(config.strategies)}"
    )

    result = await run_strategy_research(config)
    logger.info("策略研究完成")
    logger.info(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
