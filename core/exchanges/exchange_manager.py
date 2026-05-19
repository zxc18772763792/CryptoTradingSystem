"""
Exchange connector manager with optional account-scoped isolation.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import replace
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config.exchanges import EXCHANGE_CONFIGS, ExchangeConfig, ExchangeType
from config.settings import settings
from core.exchanges.base_exchange import BaseExchange
from core.exchanges.binance_connector import BinanceConnector
from core.exchanges.bybit_connector import BybitConnector
from core.exchanges.gate_connector import GateConnector
from core.exchanges.okx_connector import OKXConnector
try:
    from core.exchanges.dex_connectors import (
        PancakeSwapConnector,
        SushiSwapConnector,
        UniswapConnector,
    )
except Exception:  # pragma: no cover - optional dependency
    PancakeSwapConnector = None
    SushiSwapConnector = None
    UniswapConnector = None


class _LazyAccountManagerProxy:
    def __getattr__(self, name: str):
        from core.trading.account_manager import account_manager as live_account_manager

        return getattr(live_account_manager, name)


account_manager = _LazyAccountManagerProxy()


class ExchangeManager:
    """Manage shared and account-scoped exchange connectors."""

    def __init__(self):
        self._exchanges: Dict[str, BaseExchange] = {}
        self._account_exchanges: Dict[str, Dict[str, BaseExchange]] = {}
        self._connected: bool = False

    @staticmethod
    def _account_manager():
        return account_manager

    @staticmethod
    def _normalize_account_id(account_id: Optional[str]) -> Optional[str]:
        text = str(account_id or "").strip()
        return text or None

    def _target_store(self, account_id: Optional[str]) -> Dict[str, BaseExchange]:
        aid = self._normalize_account_id(account_id)
        if not aid:
            return self._exchanges
        return self._account_exchanges.setdefault(aid, {})

    @staticmethod
    def _resolve_default_type(name: str, fallback: str) -> str:
        mapping = {
            "binance": str(getattr(settings, "BINANCE_DEFAULT_TYPE", fallback) or fallback),
            "okx": str(getattr(settings, "OKX_DEFAULT_TYPE", fallback) or fallback),
            "gate": str(getattr(settings, "GATE_DEFAULT_TYPE", fallback) or fallback),
            "bybit": str(getattr(settings, "BYBIT_DEFAULT_TYPE", fallback) or fallback),
        }
        resolved = str(mapping.get(name, fallback) or fallback).strip().lower()
        return ExchangeManager._normalize_default_type(resolved, fallback)

    @staticmethod
    def _normalize_default_type(value: str, fallback: str) -> str:
        resolved = str(value or fallback).strip().lower()
        aliases = {
            "futures": "future",
            "perp": "swap",
            "perpetual": "swap",
        }
        resolved = aliases.get(resolved, resolved)
        if resolved not in {"spot", "future", "swap", "margin"}:
            return str(fallback or "spot").strip().lower() or "spot"
        return resolved

    @staticmethod
    def _startup_connect_timeout_sec() -> Optional[float]:
        try:
            timeout = float(getattr(settings, "EXCHANGE_STARTUP_CONNECT_TIMEOUT_SEC", 18.0) or 0.0)
        except Exception:
            timeout = 18.0
        if timeout <= 0:
            return None
        return timeout

    def _resolve_exchange_config(
        self,
        name: str,
        *,
        account_id: Optional[str] = None,
    ) -> Optional[ExchangeConfig]:
        base_config = EXCHANGE_CONFIGS.get(name)
        if not base_config:
            logger.warning(f"Unknown exchange: {name}")
            return None

        runtime_default_type = self._resolve_default_type(name, base_config.default_type)
        config = replace(base_config, default_type=runtime_default_type)

        aid = self._normalize_account_id(account_id)
        if not aid:
            return config

        account_manager = self._account_manager()
        credentials = account_manager.get_exchange_credentials(aid, name)
        requires_isolation = account_manager.requires_live_connector_isolation(aid)
        api_key = str(credentials.get("api_key") or "").strip()
        api_secret = str(credentials.get("api_secret") or "").strip()
        if requires_isolation and (not api_key or not api_secret):
            logger.warning(
                f"Exchange connector isolation requires dedicated credentials: "
                f"account_id={aid} exchange={name}"
            )
            return None

        override_default_type = str(credentials.get("default_type") or "").strip()
        return replace(
            config,
            api_key=api_key or config.api_key,
            api_secret=api_secret or config.api_secret,
            passphrase=str(credentials.get("passphrase") or config.passphrase or "").strip() or config.passphrase,
            sandbox=bool(credentials.get("sandbox", config.sandbox)),
            default_type=self._normalize_default_type(override_default_type, config.default_type),
            proxy=str(credentials.get("proxy") or config.proxy or "").strip() or config.proxy,
        )

    async def initialize(
        self,
        exchange_names: Optional[List[str]] = None,
        *,
        account_id: Optional[str] = None,
    ) -> bool:
        """
        Initialize shared or account-scoped exchange connectors.

        When ``account_id`` is omitted, the manager keeps the existing shared
        connector behavior. Passing an ``account_id`` creates a dedicated
        connector pool for that account.
        """
        if exchange_names is None:
            exchange_names = ["gate", "binance"]
            if settings.OKX_API_KEY and settings.OKX_API_SECRET:
                exchange_names.append("okx")
            if settings.BYBIT_API_KEY and settings.BYBIT_API_SECRET:
                exchange_names.append("bybit")

        target_store = self._target_store(account_id)
        exchange_specs: List[Tuple[str, ExchangeConfig]] = []
        success_count = 0

        for name in exchange_names:
            existing = target_store.get(name)
            if existing is not None and existing.is_connected:
                success_count += 1
                continue
            config = self._resolve_exchange_config(name, account_id=account_id)
            if not config:
                continue
            exchange_specs.append((name, config))

        connect_timeout_sec = self._startup_connect_timeout_sec()
        started_at = time.perf_counter()
        results: List[Optional[BaseExchange] | BaseException] = []
        if exchange_specs:
            tasks = [
                asyncio.create_task(
                    self._create_connector(name, config, timeout_sec=connect_timeout_sec),
                    name=f"exchange_init::{account_id or 'shared'}::{name}",
                )
                for name, config in exchange_specs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for (name, _config), result in zip(exchange_specs, results):
            if isinstance(result, BaseException):
                logger.error(f"Failed to initialize {name}: {result}")
                continue
            if result:
                existing = target_store.get(name)
                if existing is not None and existing is not result:
                    with contextlib.suppress(Exception):
                        await existing.disconnect()
                target_store[name] = result
                success_count += 1

        self._connected = any(exchange.is_connected for exchange in self._exchanges.values())
        elapsed_sec = time.perf_counter() - started_at
        scope = f" account={account_id}" if account_id else ""
        logger.info(
            f"Exchange manager initialized: {success_count}/{len(exchange_names)} exchanges connected "
            f"in {elapsed_sec:.2f}s{scope}"
        )
        if account_id:
            return any(exchange.is_connected for exchange in target_store.values())
        return self._connected

    async def _create_connector(
        self,
        name: str,
        config: ExchangeConfig,
        *,
        timeout_sec: Optional[float] = None,
    ) -> Optional[BaseExchange]:
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
        started_at = time.perf_counter()
        try:
            connect_coro = connector.connect()
            connected = (
                await asyncio.wait_for(connect_coro, timeout=timeout_sec)
                if timeout_sec is not None
                else await connect_coro
            )
            if connected:
                logger.info(
                    f"Connector {name} connected in {time.perf_counter() - started_at:.2f}s"
                )
                return connector
            logger.warning(
                f"Connector {name} unavailable after {time.perf_counter() - started_at:.2f}s"
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Connector {name} connect timed out after {time.perf_counter() - started_at:.2f}s"
                + (
                    f" (limit={float(timeout_sec):.1f}s)"
                    if timeout_sec is not None
                    else ""
                )
            )
        except Exception as e:
            logger.error(f"Connector {name} connect error: {e}")

        try:
            await connector.disconnect()
        except Exception:
            with contextlib.suppress(Exception):
                client = getattr(connector, "_client", None)
                if client and hasattr(client, "close"):
                    await client.close()
        return None

    async def ensure_exchange(
        self,
        name: str,
        *,
        account_id: Optional[str] = None,
    ) -> Optional[BaseExchange]:
        try:
            connector = self.get_exchange(name, account_id=account_id)
        except TypeError:
            connector = self.get_exchange(name)
        if connector is not None and bool(getattr(connector, "is_connected", True)):
            return connector

        if connector is not None:
            ok = await self.reconnect_exchange(name, account_id=account_id, timeout_sec=20.0)
        else:
            ok = await self.initialize([name], account_id=account_id)
        if not ok:
            return None
        try:
            return self.get_exchange(name, account_id=account_id)
        except TypeError:
            return self.get_exchange(name)

    async def add_dex(self, dex_name: str, chain: str = "ethereum") -> bool:
        del chain
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

    def get_exchange(
        self,
        name: str,
        account_id: Optional[str] = None,
    ) -> Optional[BaseExchange]:
        aid = self._normalize_account_id(account_id)
        if aid:
            connector = self._account_exchanges.get(aid, {}).get(name)
            if connector is not None:
                return connector
            account_manager = self._account_manager()
            if account_manager.requires_live_connector_isolation(aid):
                return None
        return self._exchanges.get(name)

    def get_all_exchanges(self) -> Dict[str, BaseExchange]:
        return self._exchanges

    def get_connected_exchanges(self, account_id: Optional[str] = None) -> List[str]:
        store = self._target_store(account_id)
        return [name for name, exchange in store.items() if exchange.is_connected]

    async def close_all(self, *, account_id: Optional[str] = None) -> None:
        if account_id:
            target_store = self._account_exchanges.pop(str(account_id), {})
            for name, exchange in target_store.items():
                try:
                    await exchange.disconnect()
                except Exception as e:
                    logger.error(f"Error disconnecting {name} for {account_id}: {e}")
            return

        stores = [self._exchanges, *self._account_exchanges.values()]
        for store in stores:
            for name, exchange in store.items():
                try:
                    await exchange.disconnect()
                except Exception as e:
                    logger.error(f"Error disconnecting {name}: {e}")

        self._exchanges.clear()
        self._account_exchanges.clear()
        self._connected = False
        logger.info("All exchanges disconnected")

    async def health_check(self, account_id: Optional[str] = None) -> Dict[str, bool]:
        results = {}
        store = self._target_store(account_id)
        for name, exchange in store.items():
            try:
                results[name] = await exchange.health_check()
            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                results[name] = False

        return results

    async def reconnect_exchange(
        self,
        name: str,
        *,
        account_id: Optional[str] = None,
        timeout_sec: Optional[float] = 20.0,
    ) -> bool:
        connector = self.get_exchange(name, account_id=account_id)
        if connector is not None:
            try:
                ok = await asyncio.wait_for(connector.connect(), timeout=timeout_sec)
                if ok:
                    logger.info(f"exchange_manager: {name} reconnected (fast path)")
                    self._connected = any(e.is_connected for e in self._exchanges.values())
                    return True
            except Exception as exc:
                logger.warning(f"exchange_manager: {name} fast-path reconnect failed: {exc}")

        config = self._resolve_exchange_config(name, account_id=account_id)
        if config is None:
            logger.warning(f"exchange_manager: no config for {name}, cannot reconnect")
            return False

        fresh = await self._create_connector(name, config, timeout_sec=timeout_sec)
        if fresh:
            self._target_store(account_id)[name] = fresh
            self._connected = any(e.is_connected for e in self._exchanges.values())
            logger.info(f"exchange_manager: {name} reconnected (fresh connector)")
            return True

        logger.warning(f"exchange_manager: {name} reconnect failed")
        return False

    def get_supported_symbols(self, exchange_name: str, account_id: Optional[str] = None) -> List[str]:
        exchange = self.get_exchange(exchange_name, account_id=account_id)
        if exchange:
            return exchange.config.supported_symbols
        return []

    def get_supported_timeframes(self, exchange_name: str, account_id: Optional[str] = None) -> List[str]:
        exchange = self.get_exchange(exchange_name, account_id=account_id)
        if exchange:
            return exchange.config.supported_timeframes
        return []

    @property
    def is_connected(self) -> bool:
        return self._connected


exchange_manager = ExchangeManager()
