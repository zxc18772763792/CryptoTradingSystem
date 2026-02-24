"""
交易所管理器
统一管理所有交易所连接
"""
from dataclasses import replace
from typing import Optional, Dict, List, Any
from loguru import logger

from config.exchanges import ExchangeConfig, EXCHANGE_CONFIGS, ExchangeType
from config.settings import settings
from core.exchanges.base_exchange import BaseExchange
from core.exchanges.binance_connector import BinanceConnector
from core.exchanges.okx_connector import OKXConnector
from core.exchanges.gate_connector import GateConnector
from core.exchanges.bybit_connector import BybitConnector
try:
    from core.exchanges.dex_connectors import (
        UniswapConnector,
        SushiSwapConnector,
        PancakeSwapConnector,
    )
except Exception:  # pragma: no cover - optional dependency
    UniswapConnector = None
    SushiSwapConnector = None
    PancakeSwapConnector = None


class ExchangeManager:
    """交易所管理器"""

    def __init__(self):
        self._exchanges: Dict[str, BaseExchange] = {}
        self._connected: bool = False

    @staticmethod
    def _resolve_default_type(name: str, fallback: str) -> str:
        mapping = {
            "binance": str(getattr(settings, "BINANCE_DEFAULT_TYPE", fallback) or fallback),
            "okx": str(getattr(settings, "OKX_DEFAULT_TYPE", fallback) or fallback),
            "gate": str(getattr(settings, "GATE_DEFAULT_TYPE", fallback) or fallback),
            "bybit": str(getattr(settings, "BYBIT_DEFAULT_TYPE", fallback) or fallback),
        }
        resolved = str(mapping.get(name, fallback) or fallback).strip().lower()
        aliases = {
            "futures": "future",
            "perp": "swap",
            "perpetual": "swap",
        }
        resolved = aliases.get(resolved, resolved)
        if resolved not in {"spot", "future", "swap", "margin"}:
            return str(fallback or "spot").strip().lower() or "spot"
        return resolved

    async def initialize(self, exchange_names: Optional[List[str]] = None) -> bool:
        """
        初始化交易所连接

        Args:
            exchange_names: 要初始化的交易所列表，None表示初始化所有

        Returns:
            是否初始化成功
        """
        if exchange_names is None:
            exchange_names = ["gate", "binance"]
            if settings.OKX_API_KEY and settings.OKX_API_SECRET:
                exchange_names.append("okx")
            if settings.BYBIT_API_KEY and settings.BYBIT_API_SECRET:
                exchange_names.append("bybit")

        success_count = 0

        for name in exchange_names:
            base_config = EXCHANGE_CONFIGS.get(name)
            if not base_config:
                logger.warning(f"Unknown exchange: {name}")
                continue
            runtime_default_type = self._resolve_default_type(name, base_config.default_type)
            config = replace(base_config, default_type=runtime_default_type)

            try:
                connector = await self._create_connector(name, config)
                if connector:
                    self._exchanges[name] = connector
                    success_count += 1
            except Exception as e:
                logger.error(f"Failed to initialize {name}: {e}")

        self._connected = success_count > 0
        logger.info(f"Exchange manager initialized: {success_count}/{len(exchange_names)} exchanges connected")
        return self._connected

    async def _create_connector(
        self,
        name: str,
        config: ExchangeConfig,
    ) -> Optional[BaseExchange]:
        """创建交易所连接器"""
        connectors = {
            "binance": BinanceConnector,
            "okx": OKXConnector,
            "gate": GateConnector,
            "bybit": BybitConnector,
        }

        connector_class = connectors.get(name)
        if not connector_class:
            logger.warning(f"No connector for exchange: {name}")
            return None

        connector = connector_class(config)
        try:
            if await connector.connect():
                return connector
        except Exception as e:
            logger.error(f"Connector {name} connect error: {e}")
            try:
                await connector.disconnect()
            except Exception:
                pass
            return None

        # Best-effort cleanup for partially initialized async clients.
        try:
            await connector.disconnect()
        except Exception:
            pass
        return None

    async def add_dex(self, dex_name: str, chain: str = "ethereum") -> bool:
        """添加DEX交易所"""
        config = ExchangeConfig(
            name=dex_name,
            exchange_type=ExchangeType.DEX,
        )

        dex_connectors = {
            "uniswap": UniswapConnector,
            "sushiswap": SushiSwapConnector,
            "pancakeswap": PancakeSwapConnector,
        }

        connector_class = dex_connectors.get(dex_name)
        if not connector_class:
            logger.warning(f"Unknown DEX: {dex_name}")
            return False

        connector = connector_class(config)
        if await connector.connect():
            self._exchanges[dex_name] = connector
            logger.info(f"DEX {dex_name} added successfully")
            return True

        return False

    def get_exchange(self, name: str) -> Optional[BaseExchange]:
        """获取交易所连接器"""
        return self._exchanges.get(name)

    def get_all_exchanges(self) -> Dict[str, BaseExchange]:
        """获取所有交易所"""
        return self._exchanges

    def get_connected_exchanges(self) -> List[str]:
        """获取已连接的交易所列表"""
        return [
            name for name, exchange in self._exchanges.items()
            if exchange.is_connected
        ]

    async def close_all(self) -> None:
        """关闭所有连接"""
        for name, exchange in self._exchanges.items():
            try:
                await exchange.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting {name}: {e}")

        self._exchanges.clear()
        self._connected = False
        logger.info("All exchanges disconnected")

    async def health_check(self) -> Dict[str, bool]:
        """健康检查所有交易所"""
        results = {}
        for name, exchange in self._exchanges.items():
            try:
                results[name] = await exchange.health_check()
            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                results[name] = False

        return results

    def get_supported_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所支持的交易对"""
        exchange = self._exchanges.get(exchange_name)
        if exchange:
            return exchange.config.supported_symbols
        return []

    def get_supported_timeframes(self, exchange_name: str) -> List[str]:
        """获取交易所支持的时间框架"""
        exchange = self._exchanges.get(exchange_name)
        if exchange:
            return exchange.config.supported_timeframes
        return []

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self._connected


# 全局交易所管理器实例
exchange_manager = ExchangeManager()
