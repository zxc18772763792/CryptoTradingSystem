"""Run a timed paper-trading strategy session and save a report."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

import strategies as strategy_module
from config.database import close_db, init_db
from config.settings import settings
from core.notifications import notification_manager
from core.risk.risk_manager import risk_manager
from core.strategies import strategy_manager, strategy_health_monitor
from core.trading.execution_engine import execution_engine
from core.trading.order_manager import order_manager
from core.trading.position_manager import position_manager
from core.data import data_storage
from core.exchanges import exchange_manager


DEFAULT_STRATEGIES = [
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "MACDStrategy",
    "BollingerBandsStrategy",
    "MeanReversionStrategy",
    "MomentumStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
]


def _safe_name(raw: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in raw)


def _snapshot_payload() -> Dict:
    summary = strategy_manager.get_dashboard_summary(signal_limit=20)
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "running_count": summary.get("running_count", 0),
        "stale_running_count": summary.get("stale_running_count", 0),
        "recent_signals_count": len(summary.get("recent_signals", [])),
        "runtime": summary.get("runtime", {}),
        "risk": risk_manager.get_risk_report(),
        "orders": order_manager.get_stats(),
        "positions": position_manager.get_stats(),
    }


async def _send_email_summary(email: str, title: str, message: str) -> Dict:
    settings.EMAIL_RECEIVER = email
    return await notification_manager.send_message(title=title, message=message, channels=["email"])


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run paper trading session")
    parser.add_argument("--hours", type=float, default=4.0, help="session duration in hours")
    parser.add_argument("--exchange", default="binance", help="exchange for strategies")
    parser.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="symbols csv")
    parser.add_argument("--timeframe", default="1m", help="strategy timeframe")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES), help="strategy classes csv")
    parser.add_argument("--sample-seconds", type=int, default=30, help="snapshot interval")
    parser.add_argument("--email", default="", help="email to send summary")
    parser.add_argument("--report-dir", default="data/research", help="report output directory")
    parser.add_argument("--log-file", default="logs/paper_session_4h.log", help="log file path")
    args = parser.parse_args()

    if args.log_file:
        logger.add(args.log_file, rotation="100 MB", retention="14 days", encoding="utf-8")

    hours = max(0.1, float(args.hours))
    duration_seconds = int(hours * 3600)
    sample_seconds = max(5, int(args.sample_seconds))
    exchange = args.exchange.strip().lower()
    symbols = [s.strip().upper().replace("_", "/") for s in args.symbols.split(",") if s.strip()]
    strategy_names = [s.strip() for s in args.strategies.split(",") if s.strip()]
    report_dir = Path(args.report_dir).resolve()
    report_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Paper session start: hours={hours} exchange={exchange} "
        f"timeframe={args.timeframe} symbols={symbols} strategies={len(strategy_names)}"
    )

    await init_db()
    await data_storage.initialize()
    await exchange_manager.initialize([exchange])

    execution_engine.set_paper_trading(True)
    await execution_engine.start()
    await strategy_health_monitor.start()
    strategy_manager.register_signal_callback(execution_engine.submit_signal)

    registered: List[str] = []
    for name in strategy_names:
        cls = getattr(strategy_module, name, None)
        if cls is None:
            logger.warning(f"Skip unavailable strategy class: {name}")
            continue
        instance_name = f"paper_{_safe_name(name)}_{int(datetime.utcnow().timestamp())}"
        ok = strategy_manager.register_strategy(
            name=instance_name,
            strategy_class=cls,
            params={"exchange": exchange},
            symbols=symbols,
            timeframe=args.timeframe,
            allocation=0.08,
        )
        if ok:
            registered.append(instance_name)
        await asyncio.sleep(0.01)

    if not registered:
        raise RuntimeError("No strategy registered for paper session")

    for name in registered:
        await strategy_manager.start_strategy(name)

    started_at = datetime.utcnow()
    snapshots: List[Dict] = []
    while True:
        now = datetime.utcnow()
        elapsed = int((now - started_at).total_seconds())
        if elapsed >= duration_seconds:
            break
        snap = _snapshot_payload()
        snap["elapsed_seconds"] = elapsed
        snapshots.append(snap)
        logger.info(
            f"paper_session tick elapsed={elapsed}s running={snap.get('running_count')} "
            f"signals={snap.get('recent_signals_count')} orders={snap.get('orders', {}).get('total_orders', 0)}"
        )
        await asyncio.sleep(sample_seconds)

    for name in registered:
        await strategy_manager.stop_strategy(name)
        strategy_manager.unregister_strategy(name)

    await strategy_health_monitor.stop()
    await execution_engine.stop()
    await exchange_manager.close_all()
    await data_storage.close()
    await close_db()

    ended_at = datetime.utcnow()
    result = {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": int((ended_at - started_at).total_seconds()),
        "exchange": exchange,
        "symbols": symbols,
        "timeframe": args.timeframe,
        "strategies": registered,
        "snapshots": snapshots,
        "final": _snapshot_payload() if snapshots else {},
    }

    report_path = report_dir / f"paper_session_{started_at.strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Paper session report saved: {report_path}")

    if args.email:
        title = "Paper Session Completed"
        message = (
            f"DurationSeconds: {result['duration_seconds']}\n"
            f"Exchange: {exchange}\n"
            f"Symbols: {', '.join(symbols)}\n"
            f"StrategyCount: {len(registered)}\n"
            f"Report: {report_path}"
        )
        email_result = await _send_email_summary(args.email, title, message)
        logger.info(f"Email summary result: {email_result}")


if __name__ == "__main__":
    asyncio.run(main())
