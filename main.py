"""Crypto Trading System entry point."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import uvicorn
from loguru import logger

# Ensure project root on sys.path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings


@dataclass(frozen=True)
class RunConfig:
    """Runtime configuration derived from settings + CLI args."""

    mode: Literal["web", "cli"]
    trading_mode: Literal["paper", "live"]
    web_host: str
    web_port: int
    data_storage_path: Path
    cache_path: Path
    log_path: Path
    log_level: str
    log_rotation: str
    log_retention: str


def build_run_config(args: argparse.Namespace) -> RunConfig:
    """Build immutable runtime config without mutating global settings."""
    runtime_settings = settings.model_copy(update={"TRADING_MODE": args.trading_mode})
    return RunConfig(
        mode=args.mode,
        trading_mode=runtime_settings.TRADING_MODE,
        web_host=runtime_settings.WEB_HOST,
        web_port=int(runtime_settings.WEB_PORT),
        data_storage_path=runtime_settings.DATA_STORAGE_PATH,
        cache_path=runtime_settings.CACHE_PATH,
        log_path=runtime_settings.LOG_PATH,
        log_level=runtime_settings.LOG_LEVEL,
        log_rotation=runtime_settings.LOG_ROTATION,
        log_retention=runtime_settings.LOG_RETENTION,
    )


def ensure_runtime_directories(run_config: RunConfig) -> None:
    """Create runtime directories during startup (not import time)."""
    for path in (run_config.log_path, run_config.data_storage_path, run_config.cache_path):
        path.mkdir(parents=True, exist_ok=True)


def setup_logging(run_config: RunConfig) -> None:
    """Configure stdout and file logging sinks."""
    logger.remove()

    logger.add(
        sys.stdout,
        level=run_config.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>",
    )

    logger.add(
        run_config.log_path / "trading_{time:YYYY-MM-DD}.log",
        rotation=run_config.log_rotation,
        retention=run_config.log_retention,
        level=run_config.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )

    logger.info("Logging configured")


async def run_cli(run_config: RunConfig) -> None:
    """Run CLI mode."""
    from config.database import init_db
    from core.data import data_storage
    from core.exchanges import exchange_manager

    logger.info(f"Starting CLI mode with trading mode: {run_config.trading_mode}")

    await init_db()
    await data_storage.initialize()
    await exchange_manager.initialize()

    logger.info("System initialized")
    logger.info(f"Connected exchanges: {exchange_manager.get_connected_exchanges()}")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await exchange_manager.close_all()
        await data_storage.close()


def run_web(run_config: RunConfig) -> None:
    """Run existing trading web service."""
    os.environ["TRADING_MODE"] = run_config.trading_mode
    logger.info(
        f"Starting Web server on {run_config.web_host}:{run_config.web_port} "
        f"(trading_mode={run_config.trading_mode})"
    )
    uvicorn.run(
        "web.main:app",
        host=run_config.web_host,
        port=run_config.web_port,
        reload=False,
        log_level="info",
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Crypto Trading System")
    parser.add_argument(
        "--mode",
        choices=["web", "cli"],
        default="web",
        help="Run mode (web or cli)",
    )
    parser.add_argument(
        "--trading-mode",
        choices=["paper", "live"],
        default=settings.TRADING_MODE,
        help="Trading mode (paper or live)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_config = build_run_config(args)

    ensure_runtime_directories(run_config)
    setup_logging(run_config)

    logger.info("=" * 60)
    logger.info("Crypto Trading System")
    logger.info(f"Run Mode: {run_config.mode}")
    logger.info(f"Trading Mode: {run_config.trading_mode}")
    logger.info(f"Data Path: {run_config.data_storage_path}")
    logger.info("=" * 60)

    if run_config.mode == "web":
        run_web(run_config)
    else:
        asyncio.run(run_cli(run_config))


if __name__ == "__main__":
    main()
