"""Data API."""
import asyncio
import contextlib
import copy
import hashlib
import json
import math
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from loguru import logger

from config.settings import settings
from config.strategy_registry import get_strategy_defaults, get_strategy_recommended_symbols
from web.api.auth import require_sensitive_ops_permissions
from core.data import (
    candidate_symbol_dirs,
    canonical_symbol_dir,
    data_collector,
    data_storage,
    normalize_symbol,
    symbol_from_storage_dirname,
    historical_data_manager,
    second_level_backfill_manager,
    download_binance_1s_daily_archive,
)
from core.data.data_storage import _normalize_parquet_frame_index
from core.data.coinglass_altcoin import (
    build_exchange_altcoin_universe,
    is_alt_candidate_symbol,
    load_cached_exchange_altcoin_universe,
)
from core.data.coinglass_client import (
    CoinglassBudgetExceeded,
    CoinglassClient,
    CoinglassError,
    coinglass_enabled,
    load_coinglass_cached_source_snapshot,
    load_dataset_rows_for_symbol,
    normalize_dataset_response,
)
from core.data.coinglass_onchain import (
    fetch_coinglass_exchange_chain_transfers,
    fetch_coinglass_whale_transfers,
    fetch_exchange_balance_snapshot,
    fetch_spot_netflow_summary,
    summarize_exchange_flows,
)
from core.data.factor_library import FACTOR_CATALOG, build_factor_library
from core.data.coinglass_feature_builder import build_coinglass_overview_payload
from core.data.coinglass_registry import get_coinglass_manifest
from core.exchanges.exchange_manager import exchange_manager
from core.runtime import runtime_state
from web.api.backtest import (
    _build_fama_backtest_components,
    _load_backtest_inputs,
    _run_backtest_core,
    get_backtest_strategy_info,
)

try:
    from core.data.funding_rate_collector import FundingRateCollector
except Exception:  # pragma: no cover - optional integration
    FundingRateCollector = None

try:
    from core.data.sentiment.fear_greed_collector import FearGreedCollector
except Exception:  # pragma: no cover - optional integration
    FearGreedCollector = None

router = APIRouter()
_RESEARCH_SYMBOLS_TIMEOUT_SEC = 8.0


_SUB_MINUTE_TIMEFRAMES = {"1s", "5s", "10s", "30s"}
_RESAMPLE_RULES = {
    # Use modern pandas frequency aliases. 'T' (minute) and 'H' (hour) were
    # deprecated in pandas 2.2 and now emit FutureWarnings.
    "1s": "1s",
    "5s": "5s",
    "10s": "10s",
    "30s": "30s",
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
    "1w": "1W",
    "1M": "1MS",
}
_REPLAY_SESSIONS: Dict[str, Dict[str, Any]] = {}
_RESEARCH_COVERAGE_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "df": None}
_DOWNLOAD_TASKS: Dict[str, Dict[str, Any]] = {}
_DOWNLOAD_BACKGROUND_TASKS: Dict[str, asyncio.Task[None]] = {}
_DOWNLOAD_TASK_SEMAPHORE: Optional[asyncio.Semaphore] = None
_DOWNLOAD_TASK_SEMAPHORE_LOOP_ID: Optional[int] = None
_LIVE_CACHE_REFRESH_TASKS: Dict[str, asyncio.Task[None]] = {}
_ONCHAIN_OVERVIEW_CACHE: Dict[str, Dict[str, Any]] = {}
_ONCHAIN_OVERVIEW_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_FACTOR_LIBRARY_CACHE: Dict[str, Dict[str, Any]] = {}
_FACTOR_LIBRARY_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_FACTOR_LIBRARY_REFRESH_META: Dict[str, Dict[str, Any]] = {}
_FAMA_CACHE: Dict[str, Dict[str, Any]] = {}
_FAMA_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_HEALTH_EXACT_SCAN_ROW_LIMIT = 250000
_HEALTH_EXACT_SCAN_FILE_LIMIT = 32
_HEALTH_GAP_PREVIEW_LIMIT = 8
_HEALTH_FAST_SCAN_RELAXED_TIMEFRAMES = {"1s", "5s", "10s", "30s"}
_HEALTH_FAST_SCAN_MIN_EXPECTED_BARS = 20000
_HEALTH_FAST_SCAN_DENSITY_THRESHOLD = 0.35
_ONCHAIN_OVERVIEW_CACHE_TTL_SEC = 180.0
_ONCHAIN_OVERVIEW_CACHE_STALE_SEC = 1800.0
_FACTOR_CACHE_TTL_SEC = 300.0
_FACTOR_CACHE_STALE_SEC = 1800.0
_FACTOR_LIBRARY_BOOTSTRAP_WAIT_SEC = 3.0
_FACTOR_LIBRARY_PENDING_RETRY_SEC = 5.0
_PAIR_SCAN_ALLOWED_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d"}
_PAIR_SCAN_DEFAULT_LOOKBACK = {
    "1m": 1440,
    "5m": 1440,
    "15m": 960,
    "1h": 720,
    "4h": 540,
    "1d": 365,
}
_PAIR_SCAN_MAX_SYMBOLS = 24
_PAIR_SCAN_MAX_ROWS = 20
_ARBITRAGE_READY_MIN_BARS_PER_LEG = 1000
_ARBITRAGE_READY_MIN_OVERLAP_BARS = 500
_ARBITRAGE_COST_PASS_MAX = 30.0
_ARBITRAGE_COST_WARN_MAX = 60.0
_ARBITRAGE_MAX_UNIVERSE_SYMBOLS = 12
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RESEARCH_PAYLOAD_CACHE_DIR = _PROJECT_ROOT / "data" / "cache" / "research_payloads"
_RESEARCH_UNIVERSE_TASK_NAME = "CryptoTradingSystem_ResearchUniverseRefresh"
_RESEARCH_UNIVERSE_SUMMARY_PATH = _PROJECT_ROOT / "data" / "research" / "research_universe_incremental_latest.json"
_RESEARCH_UNIVERSE_LOG_PATH = _PROJECT_ROOT / "logs" / "research_universe_refresh.log"
_REPLAY_SESSION_TTL_SEC = 30 * 60.0
_REPLAY_SESSION_MAX_ACTIVE = 12


def _cancel_pending_task(task: Optional[asyncio.Task[Any]]) -> bool:
    if task is None or task.done():
        return False
    with contextlib.suppress(Exception):
        task.cancel()
    return True


def _cancel_pending_task_map(tasks: Dict[str, asyncio.Task[Any]]) -> int:
    cancelled = 0
    for task in list(tasks.values()):
        if _cancel_pending_task(task):
            cancelled += 1
    tasks.clear()
    return cancelled


def _pending_tasks_count(tasks: Dict[str, asyncio.Task[Any]]) -> int:
    return sum(1 for task in tasks.values() if task is not None and not task.done())


def _live_cache_refresh_key(exchange: str, symbol: str, timeframe: str) -> str:
    return "|".join(
        [
            str(exchange or "").strip().lower(),
            str(symbol or "").strip().upper(),
            str(timeframe or "").strip(),
        ]
    )


def _schedule_live_cache_refresh(
    refresh_key: str,
    build_coro: Callable[[], Awaitable[None]],
) -> bool:
    existing = _LIVE_CACHE_REFRESH_TASKS.get(refresh_key)
    if existing is not None and not existing.done():
        return False

    task = asyncio.create_task(build_coro())
    _LIVE_CACHE_REFRESH_TASKS[refresh_key] = task

    def _cleanup(done_task: asyncio.Task[None], *, tracked_key: str = refresh_key) -> None:
        if _LIVE_CACHE_REFRESH_TASKS.get(tracked_key) is done_task:
            _LIVE_CACHE_REFRESH_TASKS.pop(tracked_key, None)

    task.add_done_callback(_cleanup)
    return True


def _clear_data_api_runtime_caches() -> Dict[str, Any]:
    global _DOWNLOAD_TASK_SEMAPHORE
    global _DOWNLOAD_TASK_SEMAPHORE_LOOP_ID

    replay_sessions = len(_REPLAY_SESSIONS)
    download_tasks = len(_DOWNLOAD_TASKS)
    completed_download_tasks = sum(
        1
        for task in _DOWNLOAD_TASKS.values()
        if str(task.get("status") or "").strip().lower()
        in {"completed", "failed", "cancelled"}
    )
    active_download_tasks = max(0, download_tasks - completed_download_tasks)
    onchain_entries = len(_ONCHAIN_OVERVIEW_CACHE)
    factor_entries = len(_FACTOR_LIBRARY_CACHE)
    fama_entries = len(_FAMA_CACHE)
    research_coverage_loaded = bool(
        isinstance(_RESEARCH_COVERAGE_CACHE.get("df"), pd.DataFrame)
        and not _RESEARCH_COVERAGE_CACHE["df"].empty
    )
    download_background_tasks_cancelled = _cancel_pending_task_map(_DOWNLOAD_BACKGROUND_TASKS)
    live_cache_refresh_tasks_cancelled = _cancel_pending_task_map(_LIVE_CACHE_REFRESH_TASKS)
    onchain_refresh_tasks_cancelled = _cancel_pending_task_map(_ONCHAIN_OVERVIEW_REFRESH_TASKS)
    factor_refresh_tasks_cancelled = _cancel_pending_task_map(_FACTOR_LIBRARY_REFRESH_TASKS)
    fama_refresh_tasks_cancelled = _cancel_pending_task_map(_FAMA_REFRESH_TASKS)

    _REPLAY_SESSIONS.clear()
    _DOWNLOAD_TASKS.clear()
    _DOWNLOAD_TASK_SEMAPHORE = None
    _DOWNLOAD_TASK_SEMAPHORE_LOOP_ID = None
    _ONCHAIN_OVERVIEW_CACHE.clear()
    _FACTOR_LIBRARY_CACHE.clear()
    _FACTOR_LIBRARY_REFRESH_META.clear()
    _FAMA_CACHE.clear()
    _RESEARCH_COVERAGE_CACHE.update({"path": None, "mtime": None, "df": None})

    return {
        "replay_sessions_cleared": replay_sessions,
        "download_tasks_cleared": download_tasks,
        "active_download_tasks_cleared": active_download_tasks,
        "completed_download_tasks_cleared": completed_download_tasks,
        "onchain_cache_entries_cleared": onchain_entries,
        "factor_cache_entries_cleared": factor_entries,
        "fama_cache_entries_cleared": fama_entries,
        "research_coverage_loaded": research_coverage_loaded,
        "download_background_tasks_cancelled": download_background_tasks_cancelled,
        "live_cache_refresh_tasks_cancelled": live_cache_refresh_tasks_cancelled,
        "onchain_refresh_tasks_cancelled": onchain_refresh_tasks_cancelled,
        "factor_refresh_tasks_cancelled": factor_refresh_tasks_cancelled,
        "fama_refresh_tasks_cancelled": fama_refresh_tasks_cancelled,
    }


def _inspect_data_api_runtime_caches() -> Dict[str, Any]:
    coverage_df = _RESEARCH_COVERAGE_CACHE.get("df")
    coverage_rows = int(len(coverage_df.index)) if isinstance(coverage_df, pd.DataFrame) else 0
    active_download_tasks = sum(
        1
        for task in _DOWNLOAD_TASKS.values()
        if str(task.get("status") or "").strip().lower() not in {"completed", "failed", "cancelled"}
    )
    return {
        "replay_sessions": len(_REPLAY_SESSIONS),
        "download_tasks": len(_DOWNLOAD_TASKS),
        "active_download_tasks": active_download_tasks,
        "download_background_tasks": _pending_tasks_count(_DOWNLOAD_BACKGROUND_TASKS),
        "live_cache_refresh_tasks": _pending_tasks_count(_LIVE_CACHE_REFRESH_TASKS),
        "onchain_cache_entries": len(_ONCHAIN_OVERVIEW_CACHE),
        "onchain_refresh_tasks": _pending_tasks_count(_ONCHAIN_OVERVIEW_REFRESH_TASKS),
        "factor_cache_entries": len(_FACTOR_LIBRARY_CACHE),
        "factor_refresh_tasks": _pending_tasks_count(_FACTOR_LIBRARY_REFRESH_TASKS),
        "factor_refresh_meta_entries": len(_FACTOR_LIBRARY_REFRESH_META),
        "fama_cache_entries": len(_FAMA_CACHE),
        "fama_refresh_tasks": _pending_tasks_count(_FAMA_REFRESH_TASKS),
        "research_coverage_path": _RESEARCH_COVERAGE_CACHE.get("path"),
        "research_coverage_mtime": _RESEARCH_COVERAGE_CACHE.get("mtime"),
        "research_coverage_rows": coverage_rows,
        "download_semaphore_initialized": _DOWNLOAD_TASK_SEMAPHORE is not None,
    }


runtime_state.register_cache(
    "web_api_data",
    clear=_clear_data_api_runtime_caches,
    inspect=_inspect_data_api_runtime_caches,
    scope="global",
)


class KlineRequest(BaseModel):
    exchange: str
    symbol: str
    timeframe: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 500


class ReplayStartRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    timeframe: str = "1m"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    window: int = 300
    speed: float = 1.0


class BatchDownloadRequest(BaseModel):
    exchange: str = "binance"
    symbols: List[str] = Field(default_factory=list)
    timeframe: str = "1h"
    days: int = 365
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    background: bool = True


def _timeframe_seconds(timeframe: str) -> int:
    tf = timeframe or "1m"
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except Exception:
        return 60

    if unit == "s":
        return max(1, value)
    if unit == "m":
        return max(1, value * 60)
    if unit == "h":
        return max(1, value * 3600)
    if unit == "d":
        return max(1, value * 86400)
    if unit == "w":
        return max(1, value * 7 * 86400)
    if unit == "M":
        return max(1, value * 30 * 86400)
    return 60


def _normalize_query_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    # Parquet indexes are tz-naive UTC (see _normalize_parquet_frame_index).
    # Convert incoming tz-aware query bounds to UTC before stripping tz so the
    # data page filters on the same clock as the strategy/backtest pipeline.
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _normalize_kline_frame_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API-loaded kline indexes to UTC-naive before time filters."""
    return _normalize_parquet_frame_index(df)


def _safe_iso_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert(None)
        return ts.to_pydatetime().replace(tzinfo=None).isoformat()
    except Exception:
        text = str(value or "").strip()
        return text or None


def _to_utc_iso(value: Any) -> str:
    """Serialize a pandas Timestamp / datetime as an unambiguous UTC ISO string.

    Parquet indexes in this project are tz-naive UTC (see
    ``_normalize_parquet_frame_index``). Returning a naive ISO such as
    ``"2026-05-21T05:00:00"`` to the browser is dangerous — JavaScript
    ``new Date(...)`` interprets naive ISO as **local time**, visually
    shifting every bar by the user's timezone offset and breaking the
    candlestick chart on the data page (see ``static/js/app.js`` ``klineToDate``
    / ``loadMoreLeftByViewport``). This helper appends an explicit ``Z``
    suffix so both sides agree on the moment in time.

    Accepts naive (assumed-UTC), tz-aware, or string inputs; returns ``""``
    if the value is null/NaT/unparseable so callers don't need to guard.
    """
    if value is None:
        return ""
    try:
        ts = pd.Timestamp(value)
    except Exception:
        text = str(value or "").strip()
        return text or ""
    if pd.isna(ts):
        return ""
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    iso = ts.isoformat()
    # ``pd.Timestamp(...).isoformat()`` produces ``...+00:00`` for UTC; canonicalize
    # to the ``Z`` form for shorter wire payload and consistent log eyeballing.
    if iso.endswith("+00:00"):
        return iso[:-6] + "Z"
    return iso


def _utc_iso(dt: Optional[datetime] = None) -> str:
    current = dt or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.isoformat().replace("+00:00", "Z")


def _error_text(err: Any) -> str:
    if err is None:
        return ""
    text = str(err).strip()
    return text or type(err).__name__


def _symbol_base_asset(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    main = raw.split(":")[0]
    if "/" in main:
        return main.split("/")[0]
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if main.endswith(suffix) and len(main) > len(suffix):
            return main[: -len(suffix)]
    return main


def _chain_context(
    display_name: str,
    *,
    lookup_chain: Optional[str] = None,
    family: Optional[str] = None,
    matched_by: str = "manual",
) -> Dict[str, Any]:
    display = str(display_name or "").strip() or "Unknown"
    lookup = str(lookup_chain or "").strip() or None
    normalized_family = (
        str(family or display)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )
    return {
        "display_name": display,
        "lookup_chain": lookup,
        "family": normalized_family,
        "matched_by": str(matched_by or "manual").strip().lower() or "manual",
        "tvl_supported": bool(lookup),
        "cache_key": f"{display}|{lookup or '-'}",
    }


_ONCHAIN_CHAIN_ALIAS_CONTEXTS: Dict[str, Dict[str, Any]] = {
    "ethereum": _chain_context("Ethereum", lookup_chain="Ethereum", family="ethereum"),
    "eth": _chain_context("Ethereum", lookup_chain="Ethereum", family="ethereum"),
    "bsc": _chain_context("BSC", lookup_chain="BSC", family="bsc"),
    "bnb": _chain_context("BSC", lookup_chain="BSC", family="bsc"),
    "bnb chain": _chain_context("BSC", lookup_chain="BSC", family="bsc"),
    "bnb smart chain": _chain_context("BSC", lookup_chain="BSC", family="bsc"),
    "bitcoin": _chain_context("Bitcoin", lookup_chain="Bitcoin", family="bitcoin"),
    "btc": _chain_context("Bitcoin", lookup_chain="Bitcoin", family="bitcoin"),
    "brc20": _chain_context("BRC-20", lookup_chain="Bitcoin", family="brc20"),
    "brc-20": _chain_context("BRC-20", lookup_chain="Bitcoin", family="brc20"),
    "ordinals": _chain_context("Ordinals", lookup_chain="Bitcoin", family="ordinals"),
    "runes": _chain_context("Runes", lookup_chain="Bitcoin", family="runes"),
    "solana": _chain_context("Solana", lookup_chain="Solana", family="solana"),
    "sol": _chain_context("Solana", lookup_chain="Solana", family="solana"),
    "base": _chain_context("Base", lookup_chain="Base", family="base"),
    "tron": _chain_context("Tron", lookup_chain="Tron", family="tron"),
    "trx": _chain_context("Tron", lookup_chain="Tron", family="tron"),
    "polygon": _chain_context("Polygon", lookup_chain="Polygon", family="polygon"),
    "matic": _chain_context("Polygon", lookup_chain="Polygon", family="polygon"),
    "arbitrum": _chain_context("Arbitrum", lookup_chain="Arbitrum", family="arbitrum"),
    "optimism": _chain_context("Optimism", lookup_chain="Optimism", family="optimism"),
    "avalanche": _chain_context("Avalanche", lookup_chain="Avalanche", family="avalanche"),
    "avax": _chain_context("Avalanche", lookup_chain="Avalanche", family="avalanche"),
    "xrpl": _chain_context("XRPL", lookup_chain="XRPL", family="xrpl"),
    "xrp": _chain_context("XRPL", lookup_chain="XRPL", family="xrpl"),
    "cardano": _chain_context("Cardano", lookup_chain="Cardano", family="cardano"),
    "ada": _chain_context("Cardano", lookup_chain="Cardano", family="cardano"),
    "doge": _chain_context("Doge", lookup_chain="Doge", family="doge"),
    "dogecoin": _chain_context("Doge", lookup_chain="Doge", family="doge"),
    "polkadot": _chain_context("Polkadot", lookup_chain=None, family="polkadot"),
    "dot": _chain_context("Polkadot", lookup_chain=None, family="polkadot"),
    "litecoin": _chain_context("Litecoin", lookup_chain="Litecoin", family="litecoin"),
    "ltc": _chain_context("Litecoin", lookup_chain="Litecoin", family="litecoin"),
    "bch": _chain_context("Bitcoincash", lookup_chain="Bitcoincash", family="bitcoincash"),
    "bitcoincash": _chain_context("Bitcoincash", lookup_chain="Bitcoincash", family="bitcoincash"),
    "etc": _chain_context("EthereumClassic", lookup_chain="EthereumClassic", family="ethereum_classic"),
    "ethereum classic": _chain_context("EthereumClassic", lookup_chain="EthereumClassic", family="ethereum_classic"),
    "ethereumclassic": _chain_context("EthereumClassic", lookup_chain="EthereumClassic", family="ethereum_classic"),
    "cosmos": _chain_context("CosmosHub", lookup_chain="CosmosHub", family="cosmoshub"),
    "atom": _chain_context("CosmosHub", lookup_chain="CosmosHub", family="cosmoshub"),
    "cosmoshub": _chain_context("CosmosHub", lookup_chain="CosmosHub", family="cosmoshub"),
    "near": _chain_context("Near", lookup_chain="Near", family="near"),
    "aptos": _chain_context("Aptos", lookup_chain="Aptos", family="aptos"),
    "apt": _chain_context("Aptos", lookup_chain="Aptos", family="aptos"),
    "sui": _chain_context("Sui", lookup_chain="Sui", family="sui"),
    "injective": _chain_context("Injective", lookup_chain="Injective", family="injective"),
    "inj": _chain_context("Injective", lookup_chain="Injective", family="injective"),
    "filecoin": _chain_context("Filecoin", lookup_chain="Filecoin", family="filecoin"),
    "fil": _chain_context("Filecoin", lookup_chain="Filecoin", family="filecoin"),
    "hedera": _chain_context("Hedera", lookup_chain="Hedera", family="hedera"),
    "hbar": _chain_context("Hedera", lookup_chain="Hedera", family="hedera"),
    "icp": _chain_context("ICP", lookup_chain="ICP", family="icp"),
    "internet computer": _chain_context("ICP", lookup_chain="ICP", family="icp"),
    "ton": _chain_context("TON", lookup_chain="TON", family="ton"),
    "thorchain": _chain_context("Thorchain", lookup_chain="Thorchain", family="thorchain"),
    "rune": _chain_context("Thorchain", lookup_chain="Thorchain", family="thorchain"),
    "starknet": _chain_context("Starknet", lookup_chain="Starknet", family="starknet"),
    "strk": _chain_context("Starknet", lookup_chain="Starknet", family="starknet"),
    "manta": _chain_context("Manta", lookup_chain="Manta", family="manta"),
    "zetachain": _chain_context("ZetaChain", lookup_chain="ZetaChain", family="zetachain"),
    "zeta": _chain_context("ZetaChain", lookup_chain="ZetaChain", family="zetachain"),
    "ronin": _chain_context("Ronin", lookup_chain="Ronin", family="ronin"),
    "bittensor": _chain_context("Bittensor", lookup_chain="Bittensor", family="bittensor"),
    "tao": _chain_context("Bittensor", lookup_chain="Bittensor", family="bittensor"),
    "akash": _chain_context("Akash", lookup_chain=None, family="akash"),
    "akt": _chain_context("Akash", lookup_chain=None, family="akash"),
    "okt": _chain_context("OKTChain", lookup_chain="OKTChain", family="oktchain"),
    "oktchain": _chain_context("OKTChain", lookup_chain="OKTChain", family="oktchain"),
    "gatelayer": _chain_context("GateLayer", lookup_chain="GateLayer", family="gatelayer"),
    "gate": _chain_context("GateLayer", lookup_chain="GateLayer", family="gatelayer"),
}


_ONCHAIN_SYMBOL_CHAIN_KEYS: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "bsc",
    "SOL": "solana",
    "XRP": "xrpl",
    "ADA": "cardano",
    "DOGE": "doge",
    "TRX": "tron",
    "LINK": "ethereum",
    "AVAX": "avalanche",
    "DOT": "polkadot",
    "POL": "polygon",
    "MATIC": "polygon",
    "LTC": "litecoin",
    "BCH": "bitcoincash",
    "ETC": "ethereumclassic",
    "ATOM": "cosmoshub",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "SUI": "sui",
    "INJ": "injective",
    "RUNE": "thorchain",
    "AAVE": "ethereum",
    "MKR": "ethereum",
    "UNI": "ethereum",
    "FIL": "filecoin",
    "HBAR": "hedera",
    "ICP": "icp",
    "TON": "ton",
    "ORDI": "brc-20",
    "SATS": "brc-20",
    "RATS": "brc-20",
    "PEPE": "ethereum",
    "FLOKI": "bsc",
    "BONK": "solana",
    "WIF": "solana",
    "BOME": "solana",
    "MEME": "ethereum",
    "NEIRO": "ethereum",
    "TURBO": "ethereum",
    "TAO": "bittensor",
    "FET": "ethereum",
    "AGIX": "ethereum",
    "RNDR": "ethereum",
    "RENDER": "solana",
    "AKT": "akash",
    "OCEAN": "ethereum",
    "GMX": "arbitrum",
    "GNS": "polygon",
    "JOE": "avalanche",
    "AXS": "ronin",
    "SAND": "ethereum",
    "MANA": "ethereum",
    "GALA": "ethereum",
    "IMX": "ethereum",
    "PYTH": "solana",
    "JTO": "solana",
    "W": "solana",
    "JUP": "solana",
    "BGB": "ethereum",
    "GT": "gatelayer",
    "OKB": "oktchain",
    "STRK": "starknet",
    "MANTA": "manta",
    "ALT": "ethereum",
    "ZETA": "zetachain",
    "CAKE": "bsc",
}


def resolve_onchain_chain_context(
    symbol: str,
    chain: Optional[str] = None,
) -> Dict[str, Any]:
    requested = str(chain or "").strip()
    normalized = requested.lower()
    if requested and normalized not in {"auto", "default"}:
        direct = _ONCHAIN_CHAIN_ALIAS_CONTEXTS.get(normalized)
        if direct:
            resolved = dict(direct)
            resolved["matched_by"] = "chain_override"
            resolved["requested_chain"] = requested
            resolved["symbol_base"] = _symbol_base_asset(symbol)
            return resolved
        display = requested
        return {
            **_chain_context(
                display,
                lookup_chain=requested,
                family=normalized or display,
                matched_by="chain_override_raw",
            ),
            "requested_chain": requested,
            "symbol_base": _symbol_base_asset(symbol),
        }

    base_asset = _symbol_base_asset(symbol)
    alias_key = _ONCHAIN_SYMBOL_CHAIN_KEYS.get(base_asset)
    if alias_key:
        direct = _ONCHAIN_CHAIN_ALIAS_CONTEXTS.get(alias_key)
        if direct:
            resolved = dict(direct)
            resolved["matched_by"] = "symbol_map"
            resolved["symbol_base"] = base_asset
            return resolved

    unresolved_display = base_asset or "Unknown"
    return {
        **_chain_context(
            unresolved_display,
            lookup_chain=None,
            family="unresolved",
            matched_by="unresolved_symbol",
        ),
        "symbol_base": base_asset,
    }


def _has_snapshot_values(snapshot: Dict[str, Any]) -> bool:
    if not isinstance(snapshot, dict):
        return False
    for value in snapshot.values():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                return True
            continue
        if isinstance(value, (int, float)):
            if math.isfinite(float(value)):
                return True
            continue
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        return True
    return False


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _download_task_progress_defaults() -> Dict[str, Any]:
    return {
        "downloaded_candles": 0,
        "estimated_total_candles": 0,
        "total_candles": 0,
        "progress_pct": 0.0,
        "pages_fetched": 0,
        "retry_count": 0,
        "consecutive_errors": 0,
        "current_time": None,
        "heartbeat_at": None,
        "updated_at": None,
        "last_success_at": None,
        "last_error": "",
        "status_message": "",
        "progress": {
            "downloaded_candles": 0,
            "estimated_total_candles": 0,
            "total_candles": 0,
            "progress_pct": 0.0,
            "pages_fetched": 0,
            "retry_count": 0,
            "consecutive_errors": 0,
            "current_time": None,
            "heartbeat_at": None,
            "updated_at": None,
            "last_success_at": None,
            "last_error": "",
            "status": "pending",
            "message": "",
            "is_complete": False,
        },
    }


def _apply_download_progress(task: Dict[str, Any], progress: Any) -> None:
    if not isinstance(task, dict) or progress is None:
        return

    progress_pct = float(getattr(progress, "progress_pct", 0.0) or 0.0)
    progress_payload = {
        "downloaded_candles": _safe_int(getattr(progress, "downloaded_candles", 0), 0),
        "estimated_total_candles": _safe_int(getattr(progress, "estimated_total_candles", 0), 0),
        "total_candles": _safe_int(getattr(progress, "total_candles", 0), 0),
        "progress_pct": round(progress_pct, 2),
        "pages_fetched": _safe_int(getattr(progress, "pages_fetched", 0), 0),
        "retry_count": _safe_int(getattr(progress, "retry_count", 0), 0),
        "consecutive_errors": _safe_int(getattr(progress, "consecutive_errors", 0), 0),
        "current_time": _safe_iso_timestamp(getattr(progress, "current_time", None)),
        "heartbeat_at": _safe_iso_timestamp(getattr(progress, "updated_at", None)),
        "updated_at": _safe_iso_timestamp(getattr(progress, "updated_at", None)),
        "last_success_at": _safe_iso_timestamp(getattr(progress, "last_success_at", None)),
        "last_error": str(getattr(progress, "last_error", "") or "").strip(),
        "status": str(getattr(progress, "status", "") or "").strip() or str(task.get("status") or "pending"),
        "message": str(getattr(progress, "message", "") or "").strip(),
        "is_complete": bool(getattr(progress, "is_complete", False)),
        "started_at": _safe_iso_timestamp(getattr(progress, "started_at", None)),
        "finished_at": _safe_iso_timestamp(getattr(progress, "finished_at", None)),
    }

    task["downloaded_candles"] = progress_payload["downloaded_candles"]
    task["estimated_total_candles"] = progress_payload["estimated_total_candles"]
    task["total_candles"] = progress_payload["total_candles"]
    task["progress_pct"] = progress_payload["progress_pct"]
    task["pages_fetched"] = progress_payload["pages_fetched"]
    task["retry_count"] = progress_payload["retry_count"]
    task["consecutive_errors"] = progress_payload["consecutive_errors"]
    task["current_time"] = progress_payload["current_time"]
    task["heartbeat_at"] = progress_payload["heartbeat_at"]
    task["updated_at"] = progress_payload["updated_at"]
    task["last_success_at"] = progress_payload["last_success_at"]
    task["last_error"] = progress_payload["last_error"]
    task["status_message"] = progress_payload["message"]
    task["progress"] = progress_payload


def _touch_download_task_progress(task: Dict[str, Any]) -> None:
    if not isinstance(task, dict):
        return
    progress = task.get("progress")
    if not isinstance(progress, dict):
        progress = {}
        task["progress"] = progress
    progress["status"] = str(task.get("status") or progress.get("status") or "pending")
    progress["message"] = str(task.get("status_message") or progress.get("message") or "")
    progress["last_error"] = str(task.get("last_error") or progress.get("last_error") or "")
    progress["progress_pct"] = float(task.get("progress_pct") or progress.get("progress_pct") or 0.0)
    progress["downloaded_candles"] = _safe_int(task.get("downloaded_candles"), progress.get("downloaded_candles") or 0)
    progress["estimated_total_candles"] = _safe_int(task.get("estimated_total_candles"), progress.get("estimated_total_candles") or 0)
    progress["total_candles"] = _safe_int(task.get("total_candles"), progress.get("total_candles") or 0)
    progress["pages_fetched"] = _safe_int(task.get("pages_fetched"), progress.get("pages_fetched") or 0)
    progress["retry_count"] = _safe_int(task.get("retry_count"), progress.get("retry_count") or 0)
    progress["consecutive_errors"] = _safe_int(task.get("consecutive_errors"), progress.get("consecutive_errors") or 0)
    progress["current_time"] = task.get("current_time") or progress.get("current_time")
    progress["updated_at"] = task.get("updated_at") or progress.get("updated_at")
    progress["heartbeat_at"] = task.get("heartbeat_at") or progress.get("heartbeat_at")
    progress["last_success_at"] = task.get("last_success_at") or progress.get("last_success_at")
    progress["is_complete"] = str(task.get("status") or "").strip() == "completed"


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return default
    if not math.isfinite(out):
        return default
    return out


def _load_premium_external_snapshot() -> Dict[str, Any]:
    """Load optional premium source snapshots from local cache (no remote requests)."""
    sources: Dict[str, Dict[str, Any]] = {}

    def _append_source(
        name: str,
        snapshot: Dict[str, Any],
        key_configured: Optional[bool],
        *,
        has_cached_data_override: Optional[bool] = None,
    ) -> None:
        has_cached_data = (
            bool(has_cached_data_override)
            if has_cached_data_override is not None
            else _has_snapshot_values(snapshot)
        )
        available = bool(has_cached_data or key_configured is True)
        sources[name] = {
            "available": available,
            "has_cached_data": has_cached_data,
            "key_configured": bool(key_configured) if key_configured is not None else None,
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
        }

    try:
        from core.data.glassnode_collector import load_glassnode_snapshot, _api_key as _gn_key  # noqa: PLC0415

        _append_source("glassnode", load_glassnode_snapshot() or {}, bool(_gn_key()))
    except Exception as exc:
        sources["glassnode"] = {"available": False, "has_cached_data": False, "key_configured": False, "error": _error_text(exc), "snapshot": {}}

    try:
        from core.data.cryptoquant_collector import load_cryptoquant_snapshot, _api_key as _cq_key  # noqa: PLC0415

        _append_source("cryptoquant", load_cryptoquant_snapshot() or {}, bool(_cq_key()))
    except Exception as exc:
        sources["cryptoquant"] = {"available": False, "has_cached_data": False, "key_configured": False, "error": _error_text(exc), "snapshot": {}}

    try:
        from core.data.nansen_collector import load_nansen_snapshot, _api_key as _ns_key  # noqa: PLC0415

        _append_source("nansen", load_nansen_snapshot() or {}, bool(_ns_key()))
    except Exception as exc:
        sources["nansen"] = {"available": False, "has_cached_data": False, "key_configured": False, "error": _error_text(exc), "snapshot": {}}

    try:
        from core.data.kaiko_collector import load_kaiko_snapshot, _api_key as _kk_key  # noqa: PLC0415

        _append_source("kaiko", load_kaiko_snapshot() or {}, bool(_kk_key()))
    except Exception as exc:
        sources["kaiko"] = {"available": False, "has_cached_data": False, "key_configured": False, "error": _error_text(exc), "snapshot": {}}

    try:
        coinglass_snapshot = load_coinglass_cached_source_snapshot()
        _append_source(
            "coinglass",
            coinglass_snapshot,
            bool(coinglass_snapshot.get("key_configured")),
            has_cached_data_override=bool(coinglass_snapshot.get("has_cached_data")),
        )
    except Exception as exc:
        sources["coinglass"] = {"available": False, "has_cached_data": False, "key_configured": False, "error": _error_text(exc), "snapshot": {}}

    total_sources = len(sources)
    configured_keys = sum(1 for source in sources.values() if source.get("key_configured") is True)
    cached_sources = sum(1 for source in sources.values() if source.get("has_cached_data"))
    available_sources = sum(1 for source in sources.values() if source.get("available"))

    return {
        "sources": sources,
        "summary": {
            "total_sources": total_sources,
            "configured_keys": configured_keys,
            "cached_sources": cached_sources,
            "available_sources": available_sources,
            "active_sources": [name for name, source in sources.items() if source.get("has_cached_data")],
        },
    }


def _onchain_overview_cache_key(exchange: str, symbol: str, whale_threshold_btc: float, chain: str) -> str:
    return "|".join(
        [
            str(exchange or "binance").strip().lower(),
            str(symbol or "BTC/USDT").strip().upper(),
            f"{float(whale_threshold_btc or 0.0):.4f}",
            str(chain or "auto").strip(),
        ]
    )


def _research_payload_cache_key(prefix: str, **kwargs: Any) -> str:
    ordered = [prefix]
    for key in sorted(kwargs.keys()):
        value = kwargs[key]
        if isinstance(value, (list, tuple, set)):
            text = ",".join(str(item) for item in value)
        else:
            text = str(value)
        ordered.append(f"{key}={text}")
    return "|".join(ordered)


def _clone_jsonable(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return copy.deepcopy(payload)
    except Exception:
        return dict(payload or {})


def _coerce_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert(None)
        return ts.tz_localize(None) if getattr(ts, "tzinfo", None) is not None else ts
    except Exception:
        return None


def _estimate_expected_bars(start_ts: Any, end_ts: Any, timeframe: str) -> Optional[int]:
    start = _coerce_timestamp(start_ts)
    end = _coerce_timestamp(end_ts)
    if start is None or end is None or end < start:
        return None
    step_seconds = _timeframe_seconds(timeframe)
    if step_seconds <= 0:
        return None
    total_seconds = (end.to_pydatetime() - start.to_pydatetime()).total_seconds()
    return max(1, int(total_seconds // step_seconds) + 1)


_COINGLASS_PRICE_HISTORY_TIMEFRAMES = {"1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"}


def _datetime_to_epoch_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    current = value
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return int(current.timestamp() * 1000)


def _coinglass_row_float(row: Dict[str, Any], *keys: str) -> Optional[float]:
    lowered = {"".join(ch for ch in str(key or "").lower() if ch.isalnum()): value for key, value in dict(row or {}).items()}
    for key in keys:
        candidate = "".join(ch for ch in str(key or "").lower() if ch.isalnum())
        if candidate not in lowered:
            continue
        try:
            value = float(lowered[candidate])
        except Exception:
            continue
        if math.isfinite(value):
            return value
    return None


def _coinglass_row_timestamp(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    for key in ("t", "ts", "time", "timestamp", "date"):
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            if isinstance(value, (int, float)):
                numeric = float(value)
                if numeric > 1_000_000_000_000:
                    numeric = numeric / 1000.0
                ts = pd.Timestamp(datetime.fromtimestamp(numeric, tz=timezone.utc))
            else:
                ts = pd.Timestamp(value)
            if pd.isna(ts):
                continue
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.tz_convert(None)
            return ts.tz_localize(None) if getattr(ts, "tzinfo", None) is not None else ts
        except Exception:
            continue
    return None


def _coinglass_rows_to_price_df(rows: List[Dict[str, Any]], start_time: datetime, end_time: datetime) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for row in rows or []:
        ts = _coinglass_row_timestamp(row)
        if ts is None:
            continue
        ts_dt = ts.to_pydatetime().replace(tzinfo=None)
        if ts_dt < start_time or ts_dt > end_time:
            continue
        open_value = _coinglass_row_float(row, "open", "o")
        high_value = _coinglass_row_float(row, "high", "h")
        low_value = _coinglass_row_float(row, "low", "l")
        close_value = _coinglass_row_float(row, "close", "c")
        if None in {open_value, high_value, low_value, close_value}:
            continue
        volume_value = _coinglass_row_float(
            row,
            "volume",
            "v",
            "vol",
            "base_volume",
            "baseVolume",
            "amount",
            "amount_usd",
        )
        records.append(
            {
                "timestamp": ts_dt,
                "open": float(open_value),
                "high": float(high_value),
                "low": float(low_value),
                "close": float(close_value),
                "volume": float(volume_value or 0.0),
            }
        )

    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame(records)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    frame = frame.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    frame = frame.set_index("timestamp")
    return frame[["open", "high", "low", "close", "volume"]]


async def _emit_download_progress_message(
    progress_callback: Optional[Any],
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    start_time: datetime,
    end_time: datetime,
    message: str,
    status: str = "running",
    downloaded_candles: int = 0,
    estimated_total_candles: int = 0,
    total_candles: int = 0,
    progress_pct: float = 0.0,
    pages_fetched: int = 0,
    retry_count: int = 0,
    consecutive_errors: int = 0,
    current_time: Optional[datetime] = None,
    last_success_at: Optional[datetime] = None,
    last_error: str = "",
    is_complete: bool = False,
) -> None:
    if not progress_callback:
        return
    now = datetime.now()
    payload = SimpleNamespace(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        downloaded_candles=int(max(0, downloaded_candles)),
        estimated_total_candles=int(max(0, estimated_total_candles)),
        total_candles=int(max(0, total_candles)),
        progress_pct=float(max(0.0, progress_pct)),
        pages_fetched=int(max(0, pages_fetched)),
        retry_count=int(max(0, retry_count)),
        consecutive_errors=int(max(0, consecutive_errors)),
        current_time=current_time or start_time,
        updated_at=now,
        last_success_at=last_success_at,
        last_error=str(last_error or ""),
        status=str(status or "running"),
        message=str(message or ""),
        is_complete=bool(is_complete),
        started_at=now,
        finished_at=now if is_complete or status in {"completed", "failed"} else None,
    )
    result = progress_callback(payload)
    if asyncio.iscoroutine(result):
        await result


def _copy_progress_snapshot(progress: Any, **overrides: Any) -> SimpleNamespace:
    data = {
        "exchange": getattr(progress, "exchange", ""),
        "symbol": getattr(progress, "symbol", ""),
        "timeframe": getattr(progress, "timeframe", ""),
        "downloaded_candles": getattr(progress, "downloaded_candles", 0),
        "estimated_total_candles": getattr(progress, "estimated_total_candles", 0),
        "total_candles": getattr(progress, "total_candles", 0),
        "progress_pct": getattr(progress, "progress_pct", 0.0),
        "pages_fetched": getattr(progress, "pages_fetched", 0),
        "retry_count": getattr(progress, "retry_count", 0),
        "consecutive_errors": getattr(progress, "consecutive_errors", 0),
        "current_time": getattr(progress, "current_time", None),
        "updated_at": getattr(progress, "updated_at", None),
        "last_success_at": getattr(progress, "last_success_at", None),
        "last_error": getattr(progress, "last_error", ""),
        "status": getattr(progress, "status", "running"),
        "message": getattr(progress, "message", ""),
        "is_complete": getattr(progress, "is_complete", False),
        "started_at": getattr(progress, "started_at", None),
        "finished_at": getattr(progress, "finished_at", None),
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _scan_parquet_files(file_paths: List[Path]) -> Dict[str, Any]:
    rows = 0
    size_bytes = 0
    modified_at = None
    start_ts = None
    end_ts = None
    read_errors: List[str] = []

    for file_path in file_paths:
        try:
            stat = file_path.stat()
            size_bytes += int(stat.st_size)
            if modified_at is None or stat.st_mtime > modified_at:
                modified_at = stat.st_mtime
        except Exception:
            pass

        try:
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            rows += int(metadata.num_rows or 0)
            names = list(getattr(metadata.schema, "names", []) or [])
            ts_index = None
            for candidate in ("timestamp", "__index_level_0__", "index"):
                if candidate in names:
                    ts_index = names.index(candidate)
                    break
            if ts_index is None:
                continue
            for group_idx in range(int(metadata.num_row_groups or 0)):
                column = metadata.row_group(group_idx).column(ts_index)
                stats = getattr(column, "statistics", None)
                if not stats or not getattr(stats, "has_min_max", False):
                    continue
                local_start = _coerce_timestamp(getattr(stats, "min", None))
                local_end = _coerce_timestamp(getattr(stats, "max", None))
                if local_start is not None and (start_ts is None or local_start < start_ts):
                    start_ts = local_start
                if local_end is not None and (end_ts is None or local_end > end_ts):
                    end_ts = local_end
        except Exception as e:
            read_errors.append(f"{file_path.name}: {e}")

    return {
        "rows": rows,
        "size_bytes": size_bytes,
        "modified_at": datetime.fromtimestamp(modified_at).isoformat() if modified_at else None,
        "start": _safe_iso_timestamp(start_ts),
        "end": _safe_iso_timestamp(end_ts),
        "read_errors": read_errors[:5],
    }


def _summarize_backup_batches(backup_root: Path) -> Dict[str, Any]:
    if not backup_root.exists():
        return {"count": 0, "symbol_dirs": 0, "recent": []}

    batches: List[Dict[str, Any]] = []
    symbol_dir_total = 0
    for batch_dir in sorted([p for p in backup_root.iterdir() if p.is_dir()], reverse=True):
        symbol_dirs = [p for p in batch_dir.rglob("*") if p.is_dir() and any(p.glob("*.parquet"))]
        symbol_dir_total += len(symbol_dirs)
        batches.append(
            {
                "batch": batch_dir.name,
                "symbol_dirs": len(symbol_dirs),
                "path": str(batch_dir),
            }
        )
    return {"count": len(batches), "symbol_dirs": symbol_dir_total, "recent": batches[:5]}


def _normalize_symbol_alias(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s in {"MATIC/USDT", "MATICUSDT"}:
        return "POL/USDT"
    if s in {"RNDR/USDT", "RNDRUSDT"}:
        return "RENDER/USDT"
    return s


def _load_research_coverage_df() -> pd.DataFrame:
    try:
        path = Path(settings.DATA_STORAGE_PATH).parent / "research" / "universe30_coverage.csv"
        cache = _RESEARCH_COVERAGE_CACHE
        mtime = path.stat().st_mtime if path.exists() else None
        if cache.get("path") == str(path) and cache.get("mtime") == mtime and isinstance(cache.get("df"), pd.DataFrame):
            return cache["df"].copy()
        if not path.exists():
            cache.update({"path": str(path), "mtime": None, "df": pd.DataFrame()})
            return pd.DataFrame()
        df = pd.read_csv(path)
        for col in ("symbol", "timeframe"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
        for col in ("is_stale", "retired_like"):
            if col in df.columns:
                df[col] = df[col].map(lambda x: str(x).strip().lower() in {"1", "true", "yes", "y"})
        cache.update({"path": str(path), "mtime": mtime, "df": df.copy()})
        return df
    except Exception as e:
        logger.warning(f"load research coverage failed: {e}")
        return pd.DataFrame()


def _research_retired_filter(
    exchange: str,
    timeframe: str,
    requested: List[str],
    exclude_retired: bool,
) -> tuple[List[str], List[str]]:
    norm_requested = [_normalize_symbol_alias(s) for s in requested]
    if not exclude_retired:
        return norm_requested, []
    if str(exchange or "").lower() != "binance":
        return norm_requested, []

    cov = _load_research_coverage_df()
    if cov.empty or "symbol" not in cov.columns or "timeframe" not in cov.columns or "retired_like" not in cov.columns:
        return norm_requested, []

    tf = str(timeframe or "").lower().upper()
    retired = cov[
        (cov["timeframe"].astype(str).str.upper() == tf) &
        (cov["retired_like"] == True)
    ]
    retired_set = set(retired["symbol"].astype(str).str.upper().tolist())
    filtered: List[str] = []
    excluded: List[str] = []
    seen = set()
    for sym in norm_requested:
        k = str(sym or "").strip().upper()
        if not k or k in seen:
            continue
        seen.add(k)
        if k in retired_set:
            excluded.append(k)
        else:
            filtered.append(k)
    return filtered, excluded


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df

    rule = _RESAMPLE_RULES.get(timeframe)
    if not rule:
        return pd.DataFrame()

    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()

    ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    volume = src[["volume"]].resample(rule).sum()
    merged = pd.concat([ohlc, volume], axis=1)
    merged = merged.dropna(subset=["open", "high", "low", "close"])
    return merged


def _parquet_path(exchange: str, symbol: str, timeframe: str) -> Path:
    return canonical_symbol_dir(Path(settings.DATA_STORAGE_PATH), exchange, symbol) / f"{timeframe}.parquet"


async def _save_df_to_parquet(exchange: str, symbol: str, timeframe: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    target = _parquet_path(exchange, symbol, timeframe)
    target.parent.mkdir(parents=True, exist_ok=True)

    merged = _normalize_kline_frame_for_compare(df)
    merged = merged.sort_index()

    for symbol_root in candidate_symbol_dirs(Path(settings.DATA_STORAGE_PATH), exchange, symbol):
        existing_path = symbol_root / f"{timeframe}.parquet"
        if not existing_path.exists():
            continue
        existing = pd.read_parquet(existing_path)
        existing = _normalize_kline_frame_for_compare(existing)
        merged = pd.concat([existing, merged])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    merged.to_parquet(target)


async def _safe_exchange_call(
    exchange: str,
    method_name: str,
    *args,
    retries: int = 2,
    **kwargs,
):
    last_error = None

    for attempt in range(retries + 1):
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            try:
                await exchange_manager.initialize([exchange])
                connector = exchange_manager.get_exchange(exchange)
            except Exception as e:
                last_error = e

        if not connector:
            await asyncio.sleep(0.2)
            continue

        method = getattr(connector, method_name, None)
        if not callable(method):
            raise RuntimeError(f"{exchange} connector has no method {method_name}")

        try:
            return await method(*args, **kwargs)
        except Exception as e:
            last_error = e
            logger.warning(
                f"[{exchange}] {method_name} failed (attempt {attempt + 1}/{retries + 1}): {e}"
            )

            # Refresh the shared connector in place without first tearing it down,
            # so concurrent readers keep their current client until a new one is ready.
            try:
                await connector.connect()
            except Exception:
                pass
            await asyncio.sleep(0.4 * (attempt + 1))

    raise last_error or RuntimeError(f"{exchange}.{method_name} failed")


async def _fetch_public_trades(
    exchange: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 1000,
    max_batches: int = 80,
    max_trades: int = 120000,
) -> List[Dict[str, Any]]:
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        try:
            await exchange_manager.initialize([exchange])
            connector = exchange_manager.get_exchange(exchange)
        except Exception:
            connector = None

    if not connector:
        return []

    client = getattr(connector, "_client", None)
    fetch_trades = getattr(client, "fetch_trades", None)
    if not callable(fetch_trades):
        return []

    since_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    loops = 0
    all_trades: List[Dict[str, Any]] = []

    while since_ms < end_ms and loops < max(1, int(max_batches)):
        loops += 1
        batch = await fetch_trades(symbol, since=since_ms, limit=limit)
        if not batch:
            break

        valid = [t for t in batch if t.get("timestamp") and t["timestamp"] <= end_ms]
        if valid:
            all_trades.extend(valid)
            if len(all_trades) >= max(1, int(max_trades)):
                all_trades = all_trades[: max(1, int(max_trades))]
                break

        last_ts = batch[-1].get("timestamp")
        if not last_ts or last_ts <= since_ms:
            break

        since_ms = last_ts + 1
        if len(batch) < limit:
            break

    return all_trades


async def _fetch_binance_public_klines(symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
    tf_map = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
        "1w": "1w",
        "1M": "1M",
    }
    interval = tf_map.get(str(timeframe or "").strip())
    if not interval:
        return pd.DataFrame()

    clean_symbol = str(symbol or "").split(":")[0].replace("/", "").upper()
    if not clean_symbol:
        return pd.DataFrame()

    req_limit = max(10, min(int(limit or 500), 1000))
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": clean_symbol, "interval": interval, "limit": req_limit}
    async with httpx.AsyncClient(timeout=8.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        payload = res.json()

    rows: List[Dict[str, Any]] = []
    for item in payload or []:
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts = item[0]
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )

    if not rows:
        return pd.DataFrame()
    return _normalize_kline_frame_for_compare(pd.DataFrame(rows).set_index("timestamp").sort_index())


def _trades_to_ohlcv(trades: List[Dict[str, Any]], timeframe: str) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()

    rule = _RESAMPLE_RULES.get(timeframe)
    if not rule:
        return pd.DataFrame()

    rows = []
    for t in trades:
        ts = t.get("timestamp")
        price = t.get("price")
        amount = t.get("amount")
        if ts is None or price is None or amount is None:
            continue
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(float(ts) / 1000, tz=timezone.utc),
                "price": float(price),
                "amount": float(amount),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("timestamp").sort_index()
    ohlc = df["price"].resample(rule).ohlc()
    volume = df["amount"].resample(rule).sum().rename("volume")
    kdf = pd.concat([ohlc, volume], axis=1).dropna()
    kdf.columns = ["open", "high", "low", "close", "volume"]
    return kdf


async def _load_local_or_aggregate(
    exchange: str,
    symbol: str,
    timeframe: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    df = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    if not df.empty:
        return df

    if timeframe in {"1w", "1M"}:
        pad_start = (start_time - timedelta(days=35)) if start_time else None
        base = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe="1d",
            start_time=pad_start,
            end_time=end_time,
        )
        if base.empty:
            for alt in ["gate", "binance"]:
                if alt == exchange:
                    continue
                base = await data_storage.load_klines_from_parquet(
                    exchange=alt,
                    symbol=symbol,
                    timeframe="1d",
                    start_time=pad_start,
                    end_time=end_time,
                )
                if not base.empty:
                    exchange = alt
                    break

        if not base.empty:
            agg = _resample_ohlcv(base, timeframe)
            if start_time:
                agg = agg[agg.index >= start_time]
            if end_time:
                agg = agg[agg.index <= end_time]
            if not agg.empty:
                await _save_df_to_parquet(exchange, symbol, timeframe, agg)
            return agg

    if timeframe in {"5s", "10s", "30s"}:
        pad_seconds = max(1, _timeframe_seconds(timeframe))
        base_start = (start_time - timedelta(seconds=pad_seconds)) if start_time else None
        base = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe="1s",
            start_time=base_start,
            end_time=end_time,
        )
        if not base.empty:
            agg = _resample_ohlcv(base, timeframe)
            if start_time:
                agg = agg[agg.index >= start_time]
            if end_time:
                agg = agg[agg.index <= end_time]
            if not agg.empty:
                await _save_df_to_parquet(exchange, symbol, timeframe, agg)
            return agg

    return pd.DataFrame()


def _validate_ohlcv(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "rows": 0,
            "invalid_rows": 0,
            "duplicate_rows": 0,
            "invalid_ratio": 0.0,
        }

    check_df = df[["open", "high", "low", "close", "volume"]].copy()

    invalid = (
        (check_df[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (check_df["high"] < check_df[["open", "close", "low"]].max(axis=1))
        | (check_df["low"] > check_df[["open", "close", "high"]].min(axis=1))
        | (check_df["volume"] < 0)
    )

    duplicate_rows = int(check_df.index.duplicated().sum())
    invalid_rows = int(invalid.sum())
    rows = int(len(check_df))

    return {
        "rows": rows,
        "invalid_rows": invalid_rows,
        "duplicate_rows": duplicate_rows,
        "invalid_ratio": round((invalid_rows / rows), 6) if rows > 0 else 0.0,
    }


def _detect_missing_bars(df: pd.DataFrame, timeframe: str, max_preview: int = 2000) -> Dict[str, Any]:
    if df.empty:
        return {"missing_count": 0, "missing_preview": []}

    freq = _RESAMPLE_RULES.get(timeframe)
    if not freq:
        return {"missing_count": 0, "missing_preview": []}

    idx = pd.to_datetime(df.index).sort_values()
    full_range = pd.date_range(start=idx.min(), end=idx.max(), freq=freq)
    missing = full_range.difference(idx)

    preview = [ts.isoformat() for ts in missing[:max_preview]]
    return {
        "missing_count": int(len(missing)),
        "missing_preview": preview,
    }


def _fill_missing_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df

    freq = _RESAMPLE_RULES.get(timeframe)
    if not freq:
        return df

    src = df.copy().sort_index()
    src.index = pd.to_datetime(src.index)

    full_index = pd.date_range(start=src.index.min(), end=src.index.max(), freq=freq)
    merged = src.reindex(full_index)

    close_ref = merged["close"].ffill()
    merged["open"] = merged["open"].fillna(close_ref)
    merged["high"] = merged["high"].fillna(merged[["open", "close"]].max(axis=1))
    merged["low"] = merged["low"].fillna(merged[["open", "close"]].min(axis=1))
    merged["close"] = merged["close"].fillna(close_ref)
    merged["volume"] = merged["volume"].fillna(0.0)

    merged = merged.dropna(subset=["open", "high", "low", "close"])
    return merged


def _new_replay_id(payload: Dict[str, Any]) -> str:
    raw = (
        f"{payload.get('exchange')}|{payload.get('symbol')}|{payload.get('timeframe')}|"
        f"{datetime.now(timezone.utc).isoformat()}|{len(_REPLAY_SESSIONS)}"
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _touch_replay_session(
    session: Dict[str, Any],
    *,
    now_monotonic: Optional[float] = None,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    current_monotonic = float(
        now_monotonic if now_monotonic is not None else time.monotonic()
    )
    current_utc = now_utc or datetime.now(timezone.utc)
    session["last_access_monotonic"] = current_monotonic
    session["last_accessed_at"] = current_utc.isoformat()
    return session


def _prune_replay_sessions(
    *,
    now_monotonic: Optional[float] = None,
    keep_id: Optional[str] = None,
) -> Dict[str, List[str]]:
    current_monotonic = float(
        now_monotonic if now_monotonic is not None else time.monotonic()
    )
    keep_text = str(keep_id or "").strip()
    expired_removed: List[str] = []
    overflow_removed: List[str] = []

    for replay_id, session in list(_REPLAY_SESSIONS.items()):
        last_access = _safe_float(session.get("last_access_monotonic"))
        created = _safe_float(session.get("created_monotonic"), 0.0) or 0.0
        touched_at = last_access if last_access is not None else created
        if replay_id != keep_text and (current_monotonic - touched_at) > _REPLAY_SESSION_TTL_SEC:
            _REPLAY_SESSIONS.pop(replay_id, None)
            expired_removed.append(replay_id)

    active_limit = max(1, int(_REPLAY_SESSION_MAX_ACTIVE))
    if len(_REPLAY_SESSIONS) > active_limit:
        sortable: List[tuple[float, str]] = []
        for replay_id, session in _REPLAY_SESSIONS.items():
            if replay_id == keep_text:
                continue
            last_access = _safe_float(session.get("last_access_monotonic"))
            created = _safe_float(session.get("created_monotonic"), 0.0) or 0.0
            touched_at = last_access if last_access is not None else created
            sortable.append((float(touched_at), replay_id))
        sortable.sort()
        overflow = max(0, len(_REPLAY_SESSIONS) - active_limit)
        for _, replay_id in sortable[:overflow]:
            if replay_id == keep_text:
                continue
            if replay_id in _REPLAY_SESSIONS:
                _REPLAY_SESSIONS.pop(replay_id, None)
                overflow_removed.append(replay_id)

    return {
        "expired_removed": expired_removed,
        "overflow_removed": overflow_removed,
    }


def _get_replay_session(replay_id: str) -> Optional[Dict[str, Any]]:
    _prune_replay_sessions()
    session = _REPLAY_SESSIONS.get(replay_id)
    if not session:
        return None
    return _touch_replay_session(session)


def _new_download_task_id(payload: Dict[str, Any]) -> str:
    raw = (
        f"{payload.get('exchange')}|{payload.get('symbol')}|{payload.get('timeframe')}|"
        f"{payload.get('start_time')}|{payload.get('end_time')}|{time.time()}|{len(_DOWNLOAD_TASKS)}"
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _run_local_command(args: List[str], timeout_sec: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(_PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_sec,
    )


def _run_powershell_json(command: str, timeout_sec: int = 30) -> Dict[str, Any]:
    result = _run_local_command(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
        timeout_sec=timeout_sec,
    )
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0:
        raise RuntimeError(stderr or stdout or "PowerShell command failed")
    if not stdout:
        return {}
    return dict(json.loads(stdout))


def _read_research_universe_summary() -> Dict[str, Any]:
    path = _RESEARCH_UNIVERSE_SUMMARY_PATH
    if not path.exists():
        return {"exists": False, "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "exists": True,
            "path": str(path),
            "updated_at": _safe_iso_timestamp(datetime.fromtimestamp(path.stat().st_mtime)),
            "error": str(exc),
        }
    return {
        "exists": True,
        "path": str(path),
        "updated_at": _safe_iso_timestamp(datetime.fromtimestamp(path.stat().st_mtime)),
        "status": payload.get("status") or "unknown",
        "timestamp": payload.get("timestamp"),
        "timeframes": payload.get("timeframes") or [],
        "seconds_symbols": payload.get("seconds_symbols") or [],
        "seconds_days": int(payload.get("seconds_days") or 0),
        "selected_symbol_count": len(payload.get("selected_symbols") or []),
        "failures_count": int(payload.get("failures_count") or 0),
        "downloaded_rows_total": int(payload.get("downloaded_rows_total") or 0),
        "idle_seconds": payload.get("idle_seconds") or {},
    }


def _map_research_task_state_label(state: str) -> str:
    key = str(state or "").strip().lower()
    return {
        "running": "运行中",
        "ready": "待命",
        "queued": "排队中",
        "disabled": "已禁用",
        "unknown": "未知",
    }.get(key, state or "未知")


def _get_research_universe_refresh_status_sync() -> Dict[str, Any]:
    task_name = _RESEARCH_UNIVERSE_TASK_NAME
    try:
        task_payload = _run_powershell_json(
            (
                "$task = Get-ScheduledTask -TaskName '{0}' -ErrorAction SilentlyContinue;"
                "if (-not $task) {{ [pscustomobject]@{{exists=$false; task_name='{0}'}} | ConvertTo-Json -Compress; exit 0 }};"
                "$info = Get-ScheduledTaskInfo -TaskName '{0}' -ErrorAction SilentlyContinue;"
                "$nextRun = $null; if ($info -and $info.NextRunTime -and $info.NextRunTime.Year -gt 1900) {{ $nextRun = $info.NextRunTime.ToString('o') }};"
                "$lastRun = $null; if ($info -and $info.LastRunTime -and $info.LastRunTime.Year -gt 1900) {{ $lastRun = $info.LastRunTime.ToString('o') }};"
                "[pscustomobject]@{{"
                "exists=$true;"
                "task_name='{0}';"
                "state=[string]$task.State;"
                "next_run_time=$nextRun;"
                "last_run_time=$lastRun;"
                "last_task_result=if($info){{$info.LastTaskResult}}else{{$null}}"
                "}} | ConvertTo-Json -Compress"
            ).format(task_name),
            timeout_sec=20,
        )
    except Exception as exc:
        task_payload = {
            "exists": False,
            "task_name": task_name,
            "error": str(exc),
        }

    state = str(task_payload.get("state") or "unknown")
    task_payload["state_label"] = _map_research_task_state_label(state)
    task_payload["log_path"] = str(_RESEARCH_UNIVERSE_LOG_PATH)
    if _RESEARCH_UNIVERSE_LOG_PATH.exists():
        task_payload["log_updated_at"] = _safe_iso_timestamp(
            datetime.fromtimestamp(_RESEARCH_UNIVERSE_LOG_PATH.stat().st_mtime)
        )

    return {
        "task": task_payload,
        "summary": _read_research_universe_summary(),
    }


async def _trigger_research_universe_refresh_start(
    exchange: str = "binance",
    timeframes: str = "1m,5m,15m,1h",
    days: int = 90,
    overlap_bars: int = 48,
) -> Dict[str, Any]:
    ensure_script = _PROJECT_ROOT / "scripts" / "ensure_research_universe_refresh_task.ps1"
    if not ensure_script.exists():
        raise RuntimeError(f"Ensure script not found: {ensure_script}")

    result = await asyncio.to_thread(
        _run_local_command,
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ensure_script),
            "-EnvName",
            "crypto_trading",
            "-Exchange",
            str(exchange or "binance").strip() or "binance",
            "-Timeframes",
            str(timeframes or "1m,5m,15m,1h").strip() or "1m,5m,15m,1h",
            "-Days",
            str(max(7, int(days or 90))),
            "-OverlapBars",
            str(max(8, int(overlap_bars or 48))),
            "-StartNow",
            "-Quiet",
        ],
        90,
    )
    if result.returncode != 0:
        message = str(result.stderr or result.stdout or "").strip() or "研究币池增量刷新启动失败"
        raise RuntimeError(message)
    payload = _get_research_universe_refresh_status_sync()
    payload["accepted"] = True
    payload["message"] = "研究币池增量追平已触发"
    return payload


def _new_download_batch_id(
    exchange: str,
    timeframe: str,
    symbols: List[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> str:
    raw = (
        f"{exchange}|{timeframe}|{','.join(symbols)}|{start_time}|{end_time}|"
        f"{datetime.now(timezone.utc).isoformat()}"
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _download_task_concurrency() -> int:
    raw_value = int(getattr(settings, "DATA_DOWNLOAD_MAX_CONCURRENT_TASKS", 2) or 2)
    return max(1, min(raw_value, 8))


def _download_task_retention() -> int:
    raw_value = int(getattr(settings, "DATA_DOWNLOAD_TASK_RETENTION", 400) or 400)
    return max(100, min(raw_value, 2000))


def _download_task_timeout_sec(payload: Optional[Dict[str, Any]] = None) -> float:
    raw_value = float(getattr(settings, "DATA_DOWNLOAD_TASK_TIMEOUT_SEC", 1800) or 1800)
    return max(60.0, min(raw_value, 6 * 3600.0))


def _mark_download_task_failed(task: Dict[str, Any], message: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    task["status"] = "failed"
    task["error"] = message
    task["last_error"] = message
    task["status_message"] = message
    task["updated_at"] = now
    task["heartbeat_at"] = now
    if not task.get("finished_at"):
        task["finished_at"] = now
    if not isinstance(task.get("result"), dict):
        task["result"] = {
            "exchange": task.get("exchange"),
            "symbol": task.get("symbol"),
            "timeframe": task.get("timeframe"),
            "count": _safe_int(task.get("downloaded_candles"), 0),
            "error": message,
            "start": task.get("start_time"),
            "end": task.get("end_time"),
        }
    else:
        task["result"]["error"] = task["result"].get("error") or message
    _touch_download_task_progress(task)


def _sync_download_task_liveness() -> None:
    for task_id, task in list(_DOWNLOAD_TASKS.items()):
        status = str(task.get("status") or "").strip().lower()
        if status not in {"pending", "running"}:
            continue
        background_task = _DOWNLOAD_BACKGROUND_TASKS.get(task_id)
        if background_task is None:
            _mark_download_task_failed(task, "Download task worker is missing")
            continue
        if not background_task.done():
            continue
        message = "Download task worker stopped before reporting a result"
        if background_task.cancelled():
            message = "Download task worker was cancelled"
        else:
            with contextlib.suppress(Exception):
                exc = background_task.exception()
                if exc:
                    message = str(exc)
        _mark_download_task_failed(task, message)


def _get_download_task_semaphore() -> asyncio.Semaphore:
    global _DOWNLOAD_TASK_SEMAPHORE
    global _DOWNLOAD_TASK_SEMAPHORE_LOOP_ID
    loop = asyncio.get_running_loop()
    loop_id = id(loop)
    if _DOWNLOAD_TASK_SEMAPHORE is None or _DOWNLOAD_TASK_SEMAPHORE_LOOP_ID != loop_id:
        _DOWNLOAD_TASK_SEMAPHORE = asyncio.Semaphore(_download_task_concurrency())
        _DOWNLOAD_TASK_SEMAPHORE_LOOP_ID = loop_id
    return _DOWNLOAD_TASK_SEMAPHORE


def _prune_download_tasks() -> None:
    keep_limit = _download_task_retention()
    if len(_DOWNLOAD_TASKS) <= keep_limit:
        return
    removable: List[tuple[str, str]] = []
    for task_id, task in _DOWNLOAD_TASKS.items():
        status = str(task.get("status") or "").strip().lower()
        if status not in {"completed", "failed", "cancelled"}:
            continue
        removable.append((str(task.get("created_at") or ""), task_id))
    removable.sort()
    for _, task_id in removable[: max(0, len(_DOWNLOAD_TASKS) - keep_limit)]:
        _DOWNLOAD_TASKS.pop(task_id, None)


async def _load_symbol_df(
    exchange: str,
    symbol: str,
    timeframe: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    start_time = _normalize_query_datetime(start_time)
    end_time = _normalize_query_datetime(end_time)
    df = await _load_local_or_aggregate(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    df = _normalize_kline_frame_for_compare(df)
    if start_time:
        df = df[df.index >= start_time]
    if end_time:
        df = df[df.index <= end_time]
    return df


def _build_chain_tvl_unavailable_payload(
    chain_context: Dict[str, Any],
    *,
    error: str = "",
) -> Dict[str, Any]:
    return {
        "chain": str(chain_context.get("display_name") or "Unknown"),
        "lookup_chain": chain_context.get("lookup_chain"),
        "family": chain_context.get("family"),
        "matched_by": chain_context.get("matched_by"),
        "available": False,
        "latest_tvl": None,
        "change_1d_pct": 0.0,
        "change_7d_pct": 0.0,
        "series": [],
        "error": error or None,
    }


async def _fetch_defillama_chain_tvl(
    chain: str = "Ethereum",
    *,
    display_chain: Optional[str] = None,
    chain_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resolved_context = dict(chain_context or {})
    if not resolved_context:
        resolved_context = _chain_context(
            display_chain or chain,
            lookup_chain=chain,
            matched_by="defillama_lookup",
        )
    display_name = str(
        display_chain
        or resolved_context.get("display_name")
        or chain
        or "Unknown"
    ).strip() or "Unknown"
    lookup_chain = str(
        resolved_context.get("lookup_chain") or chain or ""
    ).strip()
    if not lookup_chain:
        return _build_chain_tvl_unavailable_payload(
            {**resolved_context, "display_name": display_name},
            error="chain_tvl_not_supported",
        )

    url = f"https://api.llama.fi/v2/historicalChainTvl/{lookup_chain}"
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            res = await client.get(url)
            res.raise_for_status()
            rows = res.json() or []
    except Exception as e:
        return _build_chain_tvl_unavailable_payload(
            {
                **resolved_context,
                "display_name": display_name,
                "lookup_chain": lookup_chain,
            },
            error=str(e),
        )

    if not isinstance(rows, list) or not rows:
        return _build_chain_tvl_unavailable_payload(
            {
                **resolved_context,
                "display_name": display_name,
                "lookup_chain": lookup_chain,
            },
            error="empty_chain_tvl",
        )

    data = []
    for row in rows:
        ts = int(row.get("date") or 0)
        tvl = float(row.get("tvl") or 0.0)
        if ts <= 0:
            continue
        # ``utcfromtimestamp`` produces a tz-naive datetime; without ``Z`` the
        # browser would interpret this as local time and shift the TVL chart by
        # the user's timezone offset. See _to_utc_iso() for the full rationale.
        data.append(
            {
                "timestamp": _to_utc_iso(datetime.utcfromtimestamp(ts)),
                "tvl": tvl,
            }
        )

    if not data:
        return _build_chain_tvl_unavailable_payload(
            {
                **resolved_context,
                "display_name": display_name,
                "lookup_chain": lookup_chain,
            },
            error="empty_chain_tvl",
        )

    latest = data[-1]["tvl"]
    prev_1d = data[-2]["tvl"] if len(data) >= 2 else latest
    prev_7d = data[-8]["tvl"] if len(data) >= 8 else prev_1d
    chg_1d = ((latest - prev_1d) / prev_1d * 100) if prev_1d > 0 else 0.0
    chg_7d = ((latest - prev_7d) / prev_7d * 100) if prev_7d > 0 else 0.0
    return {
        "chain": display_name,
        "lookup_chain": lookup_chain,
        "family": resolved_context.get("family"),
        "matched_by": resolved_context.get("matched_by"),
        "available": True,
        "latest_tvl": round(latest, 2),
        "change_1d_pct": round(chg_1d, 4),
        "change_7d_pct": round(chg_7d, 4),
        "series": data[-180:],
    }


async def _fetch_btc_whale_unconfirmed(min_btc: float = 10.0) -> Dict[str, Any]:
    try:
        # Fetch tx + BTC price concurrently to avoid serial latency blowing through API timeout.
        async with httpx.AsyncClient(timeout=6.5) as client:
            tx_res, px_res = await asyncio.gather(
                client.get("https://blockchain.info/unconfirmed-transactions?format=json"),
                client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
            )
            tx_res.raise_for_status()
            px_res.raise_for_status()
            tx_json = tx_res.json() or {}
            px_json = px_res.json() or {}
    except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
        return {"available": False, "error": f"whale_timeout:{type(exc).__name__}", "count": 0, "transactions": []}
    except Exception as e:
        return {"available": False, "error": str(e), "count": 0, "transactions": []}

    btc_price = float(px_json.get("price") or 0.0)
    txs = tx_json.get("txs") or []
    candidates = []
    for tx in txs[:500]:
        out_value_satoshi = sum(float(v.get("value", 0.0) or 0.0) for v in (tx.get("out") or []))
        btc_amount = out_value_satoshi / 1e8
        candidates.append(
            {
                "hash": tx.get("hash"),
                "btc": round(btc_amount, 6),
                "usd_estimate": round(btc_amount * btc_price, 2) if btc_price > 0 else None,
                "timestamp": _to_utc_iso(
                    datetime.utcfromtimestamp(int(tx.get("time") or 0))
                ) if tx.get("time") else None,
            }
        )
    requested_threshold = float(max(1.0, min_btc))
    effective_threshold = requested_threshold
    whales = [item for item in candidates if float(item.get("btc") or 0.0) >= effective_threshold]
    if not whales and requested_threshold > 10.0:
        effective_threshold = 10.0
        whales = [item for item in candidates if float(item.get("btc") or 0.0) >= effective_threshold]
    whales.sort(key=lambda x: float(x.get("btc") or 0.0), reverse=True)
    return {
        "available": True,
        "btc_price": btc_price,
        "threshold_btc": float(effective_threshold),
        "requested_threshold_btc": float(requested_threshold),
        "count": len(whales),
        "transactions": whales[:50],
    }


async def _fetch_whale_activity(symbol: str, min_btc: float = 10.0) -> Dict[str, Any]:
    if coinglass_enabled():
        try:
            whale_payload, exchange_chain_payload = await asyncio.gather(
                fetch_coinglass_whale_transfers(symbol=symbol, min_btc=min_btc),
                fetch_coinglass_exchange_chain_transfers(symbol=symbol, min_btc=min_btc),
                return_exceptions=True,
            )
            payloads = [
                item
                for item in (whale_payload, exchange_chain_payload)
                if isinstance(item, dict) and item.get("available")
            ]
            if payloads:
                transactions: List[Dict[str, Any]] = []
                btc_price = None
                source_names: List[str] = []
                for payload in payloads:
                    transactions.extend(list(payload.get("transactions") or []))
                    btc_price = btc_price or payload.get("btc_price")
                    source_name = str(payload.get("source_name") or "").strip()
                    if source_name:
                        source_names.append(source_name)
                transactions.sort(
                    key=lambda item: _safe_float(item.get("amount_usd") or item.get("usd_estimate") or item.get("btc")),
                    reverse=True,
                )
                exchange_transactions = [
                    item
                    for item in transactions
                    if str((item or {}).get("provider") or "") == "coinglass_exchange_chain_tx"
                ]
                return {
                    "available": True,
                    "btc_price": btc_price,
                    "threshold_btc": float(min_btc),
                    "requested_threshold_btc": float(min_btc),
                    "count": len(transactions),
                    "transactions": transactions[:50],
                    "exchange_flow_summary": summarize_exchange_flows(exchange_transactions),
                    "source": "+".join(source_names) or "coinglass_whale_transfer",
                }
        except Exception as exc:
            logger.debug(f"coinglass whale activity unavailable: {exc}")

    fallback = await _fetch_btc_whale_unconfirmed(min_btc=min_btc)
    fallback["source"] = "blockchain_info_fallback"
    return fallback


async def _fetch_multi_exchange_funding(symbol: str) -> Dict[str, Any]:
    coinglass_payload = await _fetch_coinglass_multi_exchange_funding(symbol)
    if bool(coinglass_payload.get("available")):
        return coinglass_payload

    if FundingRateCollector is None:
        return {"available": False, "count": 0, "rates": {}, "error": "funding_collector_unavailable"}

    try:
        async with FundingRateCollector(timeout=8) as collector:
            rates = await collector.fetch_all(symbol)
    except Exception as exc:
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": _error_text(exc),
        }

    if not rates:
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": "no_exchange_data",
        }

    normalized: Dict[str, Dict[str, Any]] = {}
    values: List[float] = []
    for exchange, row in rates.items():
        try:
            value = float(getattr(row, "funding_rate", 0.0))
            values.append(value)
            normalized[str(exchange)] = {
                "symbol": str(getattr(row, "symbol", "") or ""),
                "funding_rate": value,
                "funding_rate_pct": round(value * 100.0, 6),
                "funding_time": (
                    getattr(row, "funding_time", None).isoformat()
                    if getattr(row, "funding_time", None)
                    else None
                ),
                "source": "exchange_public_fallback",
            }
        except Exception:
            continue

    if not normalized:
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": "parse_empty",
        }

    spread_rate = (max(values) - min(values)) if values else 0.0
    mean_rate = (sum(values) / len(values)) if values else 0.0
    return {
        "available": True,
        "symbol": symbol,
        "count": len(normalized),
        "rates": normalized,
        "mean_rate": round(mean_rate, 10),
        "mean_rate_pct": round(mean_rate * 100.0, 6),
        "spread_rate": round(spread_rate, 10),
        "spread_rate_pct": round(spread_rate * 100.0, 6),
        "max_abs_rate_pct": round(max(abs(v) for v in values) * 100.0, 6) if values else 0.0,
        "timestamp": _utc_iso(),
        "source": "exchange_public_fallback",
        "fallback_reason": coinglass_payload.get("error") or "coinglass_unavailable",
    }


async def _fetch_coinglass_multi_exchange_funding(symbol: str) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": "coinglass_disabled",
            "source": "coinglass_cache",
        }

    try:
        overview = dict(
            await build_coinglass_overview_payload(
                symbol=symbol,
                refresh=False,
                manual=False,
            )
            or {}
        )
        if not overview.get("available") and overview.get("key_configured"):
            overview = dict(
                await build_coinglass_overview_payload(
                    symbol=symbol,
                    refresh=True,
                    manual=False,
                )
                or overview
            )
    except Exception as exc:
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": _error_text(exc),
            "source": "coinglass_cache",
        }

    rates: Dict[str, Dict[str, Any]] = {}
    values: List[float] = []
    frame = load_dataset_rows_for_symbol("funding_rate_exchange_list", symbol)
    if not frame.empty and "payload_json" in frame.columns:
        latest_request_key = str(frame.iloc[-1].get("request_key") or "")
        if latest_request_key and "request_key" in frame.columns:
            frame = frame[frame["request_key"].astype(str) == latest_request_key]
        for _, row in frame.iterrows():
            try:
                payload = json.loads(str(row.get("payload_json") or "{}"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            exchange_name = str(
                payload.get("exchange")
                or payload.get("exchange_name")
                or row.get("exchange")
                or ""
            ).strip()
            value = _safe_float(
                payload.get("funding_rate")
                or payload.get("fundingRate")
                or payload.get("rate"),
                None,
            )
            if not exchange_name or value is None:
                continue
            values.append(float(value))
            rates[exchange_name.lower()] = {
                "symbol": str(payload.get("symbol") or symbol),
                "funding_rate": float(value),
                "funding_rate_pct": round(float(value) * 100.0, 6),
                "funding_time": payload.get("funding_time")
                or payload.get("fundingTime")
                or row.get("source_ts"),
                "source": "coinglass_cache",
            }

    snapshot = dict((overview.get("snapshot") or {}))
    snapshot_payload = dict(snapshot.get("payload") or {})
    aggregate_value = _safe_float(
        snapshot.get("funding_rate")
        or snapshot.get("funding_rate_oi_weighted")
        or snapshot_payload.get("funding_rate"),
        None,
    )
    if aggregate_value is not None and "aggregate" not in rates:
        values.append(float(aggregate_value))
        rates["aggregate"] = {
            "symbol": symbol,
            "funding_rate": float(aggregate_value),
            "funding_rate_pct": round(float(aggregate_value) * 100.0, 6),
            "funding_time": snapshot.get("timestamp"),
            "source": "coinglass_cache",
        }

    if not rates:
        return {
            "available": False,
            "count": 0,
            "rates": {},
            "symbol": symbol,
            "error": overview.get("degraded_reason") or "coinglass_cache_empty",
            "source": "coinglass_cache",
        }

    spread_rate = (max(values) - min(values)) if values else 0.0
    mean_rate = (sum(values) / len(values)) if values else 0.0
    return {
        "available": True,
        "symbol": symbol,
        "count": len(rates),
        "rates": rates,
        "mean_rate": round(mean_rate, 10),
        "mean_rate_pct": round(mean_rate * 100.0, 6),
        "spread_rate": round(spread_rate, 10),
        "spread_rate_pct": round(spread_rate * 100.0, 6),
        "max_abs_rate_pct": round(max(abs(v) for v in values) * 100.0, 6) if values else 0.0,
        "timestamp": _utc_iso(),
        "source": "coinglass_cache",
        "freshness_sec": overview.get("freshness_sec"),
        "degraded_reason": overview.get("degraded_reason"),
    }


async def _fetch_fear_greed_snapshot() -> Dict[str, Any]:
    if FearGreedCollector is None:
        return {"available": False, "error": "fear_greed_collector_unavailable"}

    try:
        async with FearGreedCollector(timeout=8) as collector:
            current = await collector.fetch_current()
    except Exception as exc:
        return {"available": False, "error": _error_text(exc)}

    if not current:
        return {"available": False, "error": "empty_response"}

    return {
        "available": True,
        "value": int(getattr(current, "value", 0)),
        "classification": str(getattr(current, "classification", "") or ""),
        "signal": str(getattr(current, "signal", "") or ""),
        "signal_strength": round(float(getattr(current, "signal_strength", 0.0) or 0.0), 4),
        "timestamp": (
            getattr(current, "timestamp", None).isoformat()
            if getattr(current, "timestamp", None)
            else None
        ),
        "time_until_update": getattr(current, "time_until_update", None),
        "source": "alternative.me",
    }


def _calc_trade_imbalance_proxy(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}

    buy_volume = 0.0
    sell_volume = 0.0
    for t in trades:
        amount = float(t.get("amount") or 0.0)
        side = str(t.get("side") or "").lower()
        if side == "buy":
            buy_volume += amount
        elif side == "sell":
            sell_volume += amount
        else:
            if bool(t.get("takerOrMaker")):
                sell_volume += amount
            else:
                buy_volume += amount
    total = buy_volume + sell_volume
    imbalance = ((buy_volume - sell_volume) / total) if total > 0 else 0.0
    return {
        "count": len(trades),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(imbalance, 6),
    }


def _build_onchain_component_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    flow = dict(payload.get("exchange_flow_proxy") or {})
    tvl = dict(payload.get("defi_tvl") or {})
    whales = dict(payload.get("whale_activity") or {})
    exchange_balance = dict(payload.get("exchange_balance") or {})
    funding_multi = dict(payload.get("funding_rate_multi_source") or {})
    fear_greed = dict(payload.get("fear_greed_index") or {})
    premium_external = dict(payload.get("premium_external") or {})
    premium_summary = dict(premium_external.get("summary") or {})
    premium_sources = dict(premium_external.get("sources") or {})
    configured_keys = _safe_int(premium_summary.get("configured_keys"), 0)
    cached_sources = _safe_int(premium_summary.get("cached_sources"), 0)
    total_sources = _safe_int(premium_summary.get("total_sources"), len(premium_sources))
    premium_ok = cached_sources > 0 or configured_keys == 0
    return {
        "exchange_flow_proxy": {
            "status": "ok" if flow.get("available") else "degraded",
            "source": flow.get("source") or "exchange_public_trades",
            "error": flow.get("error"),
        },
        "exchange_balance": {
            "status": "ok" if exchange_balance.get("available") else "degraded",
            "source": exchange_balance.get("source") or "coinglass_exchange_balance",
            "error": exchange_balance.get("error"),
        },
        "defi_tvl": {
            "status": "ok" if tvl.get("available") else "degraded",
            "source": "defillama",
            "error": tvl.get("error"),
        },
        "whale_activity": {
            "status": "ok" if whales.get("available") else "degraded",
            "source": whales.get("source") or "coinglass_whale_transfer",
            "error": whales.get("error"),
        },
        "funding_rate_multi_source": {
            "status": "ok" if funding_multi.get("available") else "degraded",
            "source": funding_multi.get("source") or "coinglass_cache",
            "error": funding_multi.get("error"),
        },
        "fear_greed_index": {
            "status": "ok" if fear_greed.get("available") else "degraded",
            "source": "alternative.me",
            "error": fear_greed.get("error"),
        },
        "premium_external": {
            "status": "ok" if premium_ok else "degraded",
            "source": "coinglass+optional_cached_external",
            "detail": f"cached={cached_sources}/{max(total_sources, 0)} key={configured_keys}",
            "error": None if premium_ok else "premium_sources_configured_but_cache_empty",
        },
    }


async def _compute_onchain_overview(
    exchange: str,
    symbol: str,
    whale_threshold_btc: float,
    chain: str,
) -> Dict[str, Any]:
    started_at = time.monotonic()
    chain_context = resolve_onchain_chain_context(symbol, chain)
    premium_external = _load_premium_external_snapshot()
    connector = exchange_manager.get_exchange(exchange)
    if connector is None:
        imbalance = {
            "available": False,
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
            "error": f"exchange_not_connected:{exchange}",
        }
    else:
        imbalance = {
            "available": False,
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
            "error": "live_flow_proxy_disabled_for_fast_path",
        }

    if chain_context.get("tvl_supported") and chain_context.get("lookup_chain"):
        tvl_task = asyncio.create_task(
            asyncio.wait_for(
                _fetch_defillama_chain_tvl(
                    chain=str(chain_context.get("lookup_chain") or ""),
                    display_chain=str(chain_context.get("display_name") or ""),
                    chain_context=chain_context,
                ),
                timeout=6.0,
            )
        )
    else:
        tvl_task = asyncio.create_task(
            asyncio.sleep(
                0,
                result=_build_chain_tvl_unavailable_payload(
                    chain_context,
                    error="chain_tvl_not_supported",
                ),
            )
        )
    whale_task = asyncio.create_task(
        asyncio.wait_for(
            _fetch_whale_activity(
                symbol=symbol,
                min_btc=max(1.0, whale_threshold_btc),
            ),
            timeout=8.0,
        )
    )
    spot_flow_task = asyncio.create_task(
        asyncio.wait_for(fetch_spot_netflow_summary(symbol=symbol), timeout=7.0)
    )
    balance_task = asyncio.create_task(
        asyncio.wait_for(fetch_exchange_balance_snapshot(symbol=symbol), timeout=7.0)
    )
    funding_task = asyncio.create_task(asyncio.wait_for(_fetch_multi_exchange_funding(symbol=symbol), timeout=6.5))
    fear_greed_task = asyncio.create_task(asyncio.wait_for(_fetch_fear_greed_snapshot(), timeout=6.5))

    tvl_result, whale_result, spot_flow_result, balance_result, funding_result, fear_greed_result = await asyncio.gather(
        tvl_task,
        whale_task,
        spot_flow_task,
        balance_task,
        funding_task,
        fear_greed_task,
        return_exceptions=True,
    )
    tvl = (
        tvl_result
        if isinstance(tvl_result, dict)
        else _build_chain_tvl_unavailable_payload(
            chain_context,
            error=_error_text(tvl_result),
        )
    )
    whales = (
        whale_result
        if isinstance(whale_result, dict)
        else {"available": False, "error": _error_text(whale_result), "count": 0, "transactions": []}
    )
    spot_flow = (
        spot_flow_result
        if isinstance(spot_flow_result, dict)
        else {"available": False, "error": _error_text(spot_flow_result), "source": "coinglass_spot_netflow"}
    )
    exchange_balance = (
        balance_result
        if isinstance(balance_result, dict)
        else {"available": False, "error": _error_text(balance_result), "source": "coinglass_exchange_balance"}
    )
    if not spot_flow.get("available"):
        whale_flow = dict((whales or {}).get("exchange_flow_summary") or {})
        if whale_flow.get("available"):
            spot_flow = {
                **whale_flow,
                "source": whale_flow.get("source") or "coinglass_exchange_chain_tx",
                "fallback_source": "whale_activity_exchange_chain",
            }
    if not spot_flow.get("available"):
        spot_flow = imbalance
    funding_multi = (
        funding_result
        if isinstance(funding_result, dict)
        else {"available": False, "error": _error_text(funding_result), "count": 0, "rates": {}, "symbol": symbol}
    )
    fear_greed = (
        fear_greed_result
        if isinstance(fear_greed_result, dict)
        else {"available": False, "error": _error_text(fear_greed_result)}
    )
    payload = {
        "symbol": symbol,
        "exchange": exchange,
        "window_hours": 4,
        "chain_context": chain_context,
        "exchange_flow_proxy": spot_flow,
        "exchange_balance": exchange_balance,
        "defi_tvl": tvl,
        "whale_activity": whales,
        "funding_rate_multi_source": funding_multi,
        "fear_greed_index": fear_greed,
        "premium_external": premium_external,
        "generated_at": _utc_iso(),
        "latency_ms": int((time.monotonic() - started_at) * 1000),
    }
    payload["component_status"] = _build_onchain_component_status(payload)
    payload["degraded"] = any(v.get("status") != "ok" for v in payload["component_status"].values())
    return payload


async def _refresh_onchain_overview_cache(
    cache_key: str,
    *,
    exchange: str,
    symbol: str,
    whale_threshold_btc: float,
    chain: str,
) -> None:
    try:
        payload = await _compute_onchain_overview(
            exchange=exchange,
            symbol=symbol,
            whale_threshold_btc=whale_threshold_btc,
            chain=chain,
        )
        _ONCHAIN_OVERVIEW_CACHE[cache_key] = {
            "created_monotonic": time.monotonic(),
            "payload": payload,
        }
    except Exception as e:
        logger.warning(f"onchain overview refresh failed {cache_key}: {e}")
        cached = dict(_ONCHAIN_OVERVIEW_CACHE.get(cache_key) or {})
        if cached:
            payload = dict(cached.get("payload") or {})
            payload["refresh_error"] = str(e)
            payload["refresh_failed_at"] = _utc_iso()
            cached["payload"] = payload
            _ONCHAIN_OVERVIEW_CACHE[cache_key] = cached
    finally:
        _ONCHAIN_OVERVIEW_REFRESH_TASKS.pop(cache_key, None)


def _prepare_cached_onchain_payload(cache_key: str, refresh: bool = False) -> Optional[Dict[str, Any]]:
    cached = dict(_ONCHAIN_OVERVIEW_CACHE.get(cache_key) or {})
    payload = dict(cached.get("payload") or {})
    if not payload:
        return None
    age_sec = max(0.0, time.monotonic() - float(cached.get("created_monotonic") or 0.0))
    payload["cached"] = True
    payload["cache_age_sec"] = round(age_sec, 3)
    payload["refreshing"] = cache_key in _ONCHAIN_OVERVIEW_REFRESH_TASKS
    payload["stale"] = age_sec > _ONCHAIN_OVERVIEW_CACHE_TTL_SEC
    payload["stale_too_long"] = age_sec > _ONCHAIN_OVERVIEW_CACHE_STALE_SEC
    payload["served_mode"] = "cache_refresh" if refresh else "cache"
    return payload


def _build_onchain_placeholder(
    *,
    exchange: str,
    symbol: str,
    whale_threshold_btc: float,
    chain: str,
    reason: str = "后台刷新中",
) -> Dict[str, Any]:
    chain_context = resolve_onchain_chain_context(symbol, chain)
    payload = {
        "symbol": symbol,
        "exchange": exchange,
        "window_hours": 4,
        "chain_context": chain_context,
        "exchange_flow_proxy": {
            "available": False,
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
            "error": reason,
        },
        "defi_tvl": _build_chain_tvl_unavailable_payload(
            chain_context,
            error=reason,
        ),
        "exchange_balance": {
            "available": False,
            "source": "coinglass_exchange_balance",
            "symbol": symbol,
            "exchange_balance_btc": None,
            "exchange_balance_usd": None,
            "exchange_balance_change_24h": None,
            "exchange_balance_change_7d": None,
            "stablecoin_exchange_balance_usd": None,
            "stablecoin_netflow_usd": None,
            "exchange_reserve_pressure_score": None,
            "onchain_activity_score": None,
            "error": reason,
        },
        "whale_activity": {
            "available": False,
            "btc_price": None,
            "threshold_btc": float(max(1.0, whale_threshold_btc)),
            "count": 0,
            "transactions": [],
            "error": reason,
        },
        "funding_rate_multi_source": {
            "available": False,
            "symbol": symbol,
            "count": 0,
            "rates": {},
            "mean_rate": None,
            "mean_rate_pct": None,
            "spread_rate": None,
            "spread_rate_pct": None,
            "max_abs_rate_pct": None,
            "error": reason,
        },
        "fear_greed_index": {
            "available": False,
            "value": None,
            "classification": "",
            "signal": "",
            "signal_strength": None,
            "timestamp": None,
            "source": "alternative.me",
            "error": reason,
        },
        "premium_external": {
            "sources": {
                "glassnode": {"available": False, "has_cached_data": False, "key_configured": False, "snapshot": {}},
                "cryptoquant": {"available": False, "has_cached_data": False, "key_configured": False, "snapshot": {}},
                "nansen": {"available": False, "has_cached_data": False, "key_configured": False, "snapshot": {}},
                "kaiko": {"available": False, "has_cached_data": False, "key_configured": False, "snapshot": {}},
            },
            "summary": {
                "total_sources": 4,
                "configured_keys": 0,
                "cached_sources": 0,
                "available_sources": 0,
                "active_sources": [],
            },
        },
        "generated_at": _utc_iso(),
        "latency_ms": 0,
        "degraded": True,
        "cached": False,
        "cache_age_sec": 0.0,
        "refreshing": True,
        "stale": False,
        "stale_too_long": False,
        "served_mode": "bootstrap",
    }
    payload["component_status"] = _build_onchain_component_status(payload)
    return payload


def _prepare_research_cached_payload(
    cache_store: Dict[str, Dict[str, Any]],
    task_store: Dict[str, asyncio.Task],
    cache_key: str,
    *,
    refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    cached = dict(cache_store.get(cache_key) or {})
    payload = dict(cached.get("payload") or {})
    if not payload:
        return None
    age_sec = max(0.0, time.monotonic() - float(cached.get("created_monotonic") or 0.0))
    payload["cached"] = True
    payload["cache_age_sec"] = round(age_sec, 3)
    payload["refreshing"] = cache_key in task_store
    payload["stale"] = age_sec > _FACTOR_CACHE_TTL_SEC
    payload["stale_too_long"] = age_sec > _FACTOR_CACHE_STALE_SEC
    payload["served_mode"] = "cache_refresh" if refresh else "cache"
    return payload


def _store_research_cached_payload(
    cache_store: Dict[str, Dict[str, Any]],
    cache_key: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    cache_store[cache_key] = {
        "created_monotonic": time.monotonic(),
        "payload": _clone_jsonable(payload),
    }
    return payload


def _prime_research_cached_payload(
    cache_store: Dict[str, Dict[str, Any]],
    cache_key: str,
    payload: Dict[str, Any],
    *,
    age_sec: float = 0.0,
) -> Dict[str, Any]:
    cache_store[cache_key] = {
        "created_monotonic": max(0.0, time.monotonic() - max(0.0, float(age_sec or 0.0))),
        "payload": _clone_jsonable(payload),
    }
    return payload


def _research_payload_disk_cache_path(prefix: str, cache_key: str) -> Path:
    digest = hashlib.sha1(str(cache_key or "").encode("utf-8")).hexdigest()
    return _RESEARCH_PAYLOAD_CACHE_DIR / str(prefix or "generic") / f"{digest}.json"


def _load_research_disk_cached_payload(
    prefix: str,
    cache_key: str,
    *,
    max_age_sec: float = 86400.0,
) -> Optional[Dict[str, Any]]:
    path = _research_payload_disk_cache_path(prefix, cache_key)
    if not path.exists():
        return None
    try:
        age_sec = max(0.0, time.time() - float(path.stat().st_mtime))
    except Exception:
        age_sec = 0.0
    if age_sec > max(60.0, float(max_age_sec or 0.0)):
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    payload = dict(payload)
    payload["cached"] = True
    payload["cache_age_sec"] = round(age_sec, 3)
    payload["refreshing"] = False
    payload["stale"] = age_sec > _FACTOR_CACHE_TTL_SEC
    payload["stale_too_long"] = age_sec > _FACTOR_CACHE_STALE_SEC
    payload["served_mode"] = "disk_cache"
    payload["cache_source"] = "disk"
    return payload


def _store_research_disk_cached_payload(prefix: str, cache_key: str, payload: Dict[str, Any]) -> None:
    path = _research_payload_disk_cache_path(prefix, cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = _clone_jsonable(payload)
    body["stored_at_utc"] = _utc_iso()
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_factor_library_placeholder(
    *,
    exchange: str,
    timeframe: str,
    symbols_requested: List[str],
    exclude_retired: bool,
    excluded_symbols: Optional[List[str]] = None,
    reason: str = "因子库正在后台计算",
) -> Dict[str, Any]:
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_effective": 0,
        "symbols_requested": list(symbols_requested or []),
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": list(excluded_symbols or []),
            "requested_after_filter": list(symbols_requested or []),
        },
        "symbols_used": [],
        "points": 0,
        "factors": [],
        "catalog": FACTOR_CATALOG,
        "universe_size": 0,
        "universe_quality": "empty",
        "warnings": [str(reason)],
        "diagnostics": {},
        "window_config": {},
        "latest": {},
        "mean_24": {},
        "std_24": {},
        "correlation": {},
        "series": [],
        "asset_scores": [],
        "error": str(reason),
        "degraded": True,
        "cached": False,
        "cache_age_sec": 0.0,
        "refreshing": False,
        "served_mode": "fallback",
    }


def _estimate_factor_library_expected_sec(*, timeframe: str, symbol_count: int, lookback: int) -> float:
    tf = str(timeframe or "1h").lower()
    tf_scale = {
        "1m": 1.25,
        "5m": 1.15,
        "15m": 1.0,
        "1h": 1.1,
        "4h": 1.15,
        "1d": 1.2,
    }.get(tf, 1.1)
    return max(
        8.0,
        min(
            90.0,
            (6.0 + max(1, int(symbol_count or 0)) * 2.0 + max(120, int(lookback or 0)) / 70.0) * tf_scale,
        ),
    )


def _build_factor_library_pending_message(
    *,
    timeframe: str,
    symbol_count: int,
    lookback: int,
    elapsed_sec: float,
    expected_sec: float,
) -> str:
    return (
        f"因子库首次计算中：{str(timeframe or '1h').upper()} / {int(symbol_count or 0)} 币 / "
        f"lookback {int(lookback or 0)}，已等待 {max(0.0, float(elapsed_sec or 0.0)):.1f} 秒，"
        f"通常需要约 {max(1.0, float(expected_sec or 0.0)):.0f} 秒。"
    )


def _build_factor_library_pending_placeholder(
    *,
    exchange: str,
    timeframe: str,
    symbols_requested: List[str],
    exclude_retired: bool,
    refresh_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = dict(refresh_meta or {})
    symbol_count = max(1, int(meta.get("symbol_count") or len(symbols_requested or []) or 1))
    lookback = max(120, int(meta.get("lookback") or 0) or 120)
    elapsed_sec = max(0.0, float(meta.get("elapsed_sec") or 0.0))
    expected_sec = _estimate_factor_library_expected_sec(
        timeframe=str(meta.get("timeframe") or timeframe or "1h"),
        symbol_count=symbol_count,
        lookback=lookback,
    )
    message = _build_factor_library_pending_message(
        timeframe=str(meta.get("timeframe") or timeframe or "1h"),
        symbol_count=symbol_count,
        lookback=lookback,
        elapsed_sec=elapsed_sec,
        expected_sec=expected_sec,
    )
    payload = _build_factor_library_placeholder(
        exchange=exchange,
        timeframe=timeframe,
        symbols_requested=symbols_requested,
        exclude_retired=exclude_retired,
        reason=message,
    )
    payload.update(
        {
            "refreshing": True,
            "served_mode": "bootstrap",
            "pending": True,
            "pending_stage": "bootstrap",
            "pending_since": meta.get("started_at"),
            "pending_elapsed_sec": round(elapsed_sec, 3),
            "pending_expected_sec": round(expected_sec, 3),
            "retry_after_sec": float(_FACTOR_LIBRARY_PENDING_RETRY_SEC),
            "cache_source": "none",
        }
    )
    return payload


def _augment_factor_library_refreshing_payload(
    payload: Dict[str, Any],
    *,
    refresh_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = dict(refresh_meta or {})
    updated = _clone_jsonable(payload)
    cache_age_sec = round(float(updated.get("cache_age_sec") or 0.0), 3)
    updated["refreshing"] = True
    updated["served_mode"] = "cache_refresh"
    updated["pending"] = True
    updated["pending_stage"] = "refreshing"
    updated["pending_since"] = meta.get("started_at")
    updated["pending_elapsed_sec"] = round(float(meta.get("elapsed_sec") or 0.0), 3)
    updated["retry_after_sec"] = float(_FACTOR_LIBRARY_PENDING_RETRY_SEC)
    updated.setdefault("cache_source", "memory")

    cache_label = "磁盘缓存" if updated.get("cache_source") == "disk" else "缓存"
    note = f"当前展示的是 {cache_age_sec:.0f} 秒前的{cache_label}，后台仍在刷新因子库。"
    warnings = [str(item) for item in list(updated.get("warnings") or []) if str(item).strip()]
    if note not in warnings:
        warnings.insert(0, note)
    updated["warnings"] = warnings
    updated["error"] = ""
    return updated


def _build_fama_placeholder(
    *,
    exchange: str,
    timeframe: str,
    symbols_requested: List[str],
    exclude_retired: bool,
    excluded_symbols: Optional[List[str]] = None,
    reason: str = "Fama 因子正在后台计算",
) -> Dict[str, Any]:
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "symbols_requested": list(symbols_requested or []),
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": list(excluded_symbols or []),
            "requested_after_filter": list(symbols_requested or []),
        },
        "symbols_used": [],
        "points": 0,
        "universe_size": 0,
        "universe_quality": "empty",
        "latest": {"MKT": 0.0, "SMB": 0.0, "HML": 0.0, "MOM": 0.0, "RMW": 0.0, "CMA": 0.0, "VOL": 0.0},
        "mean_24": {"MKT": 0.0, "SMB": 0.0, "HML": 0.0, "MOM": 0.0, "RMW": 0.0, "CMA": 0.0, "VOL": 0.0},
        "std_24": {"MKT": 0.0, "SMB": 0.0, "HML": 0.0, "MOM": 0.0, "RMW": 0.0, "CMA": 0.0, "VOL": 0.0},
        "series": [],
        "warnings": [str(reason)],
        "error": str(reason),
        "degraded": True,
        "cached": False,
        "cache_age_sec": 0.0,
        "refreshing": False,
        "served_mode": "fallback",
    }


def _factor_series_from_returns(returns_df: pd.DataFrame) -> pd.DataFrame:
    if returns_df.empty:
        return pd.DataFrame()
    x = returns_df.dropna(how="all")
    if x.empty:
        return pd.DataFrame()

    mkt = x.mean(axis=1)
    vol = x.rolling(48, min_periods=12).std().mean(axis=1)
    mom = (1 + x).rolling(48, min_periods=12).apply(np.prod, raw=True) - 1
    mom_cross = mom.mean(axis=1)

    # Fama-like SMB proxy: low-liquidity basket - high-liquidity basket.
    avg_abs = x.abs().rolling(48, min_periods=12).mean().iloc[-1].sort_values()
    if len(avg_abs) >= 2:
        cut = max(1, len(avg_abs) // 3)
        low = list(avg_abs.head(cut).index)
        high = list(avg_abs.tail(cut).index)
        smb = x[low].mean(axis=1) - x[high].mean(axis=1)
    else:
        smb = pd.Series(0.0, index=x.index)

    factors = pd.DataFrame(
        {
            "MKT": mkt.fillna(0.0),
            "SMB": smb.fillna(0.0),
            "MOM": mom_cross.fillna(0.0),
            "VOL": vol.fillna(0.0),
        },
        index=x.index,
    )
    return factors


def _normalize_symbol_folder(folder_name: str) -> str:
    return symbol_from_storage_dirname(folder_name)


def _discover_local_symbols(exchange: str, timeframe: str, max_count: int = 200) -> List[str]:
    root = Path(settings.DATA_STORAGE_PATH) / str(exchange or "").lower()
    if not root.exists() or not root.is_dir():
        return []

    out: List[str] = []
    for sym_dir in root.iterdir():
        if not sym_dir.is_dir():
            continue
        tf_file = sym_dir / f"{timeframe}.parquet"
        tf_parts = sym_dir / f"{timeframe}_parts"
        if not tf_file.exists() and not tf_parts.exists():
            continue
        sym = _normalize_symbol_folder(sym_dir.name)
        if sym:
            out.append(sym)
        if len(out) >= max(1, int(max_count)):
            break
    return sorted(set(out))


def _expand_symbols_with_local(
    exchange: str,
    timeframe: str,
    requested: List[str],
    min_symbols: int,
    max_symbols: int,
    excluded_symbols: Optional[set[str]] = None,
) -> List[str]:
    out: List[str] = []
    seen = set()
    excluded = {str(s).strip().upper() for s in (excluded_symbols or set()) if str(s).strip()}
    for sym in requested:
        key = _normalize_symbol_alias(sym)
        if not key or key in seen:
            continue
        if key in excluded:
            continue
        out.append(key)
        seen.add(key)

    if len(out) < int(min_symbols):
        local = _discover_local_symbols(exchange=exchange, timeframe=timeframe, max_count=max_symbols * 2)
        for sym in local:
            key = _normalize_symbol_alias(sym)
            if not key or key in seen:
                continue
            if key in excluded:
                continue
            out.append(key)
            seen.add(key)
            if len(out) >= int(max_symbols):
                break

    return out[: max(1, int(max_symbols))]


def _latest_partition_end_time(exchange: str, symbol: str, timeframe: str) -> Optional[datetime]:
    for sym_dir in candidate_symbol_dirs(Path(settings.DATA_STORAGE_PATH), exchange, symbol):
        parts_dir = sym_dir / f"{timeframe}_parts"
        if not parts_dir.exists() or not parts_dir.is_dir():
            continue
        files = sorted(parts_dir.glob("*.parquet"), reverse=True)
        for path in files:
            try:
                day = pd.Timestamp(path.stem).to_pydatetime()
                return day + timedelta(days=1)
            except Exception:
                continue
    return None


async def _build_factor_input_frames(
    exchange: str,
    symbol_list: List[str],
    timeframe: str,
    lookback: int,
) -> tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    approx_bars = max(120, int(lookback))
    tf_seconds = max(1, _timeframe_seconds(timeframe))
    # Bound disk scan cost for high-frequency data by querying a recent window first.
    window_seconds = max(3600, min(approx_bars * tf_seconds * 2, 366 * 24 * 3600))
    common_end_candidates: Dict[str, Optional[datetime]] = {
        sym: _latest_partition_end_time(exchange=exchange, symbol=sym, timeframe=timeframe) for sym in symbol_list
    }
    known_ends = [ts for ts in common_end_candidates.values() if ts is not None]
    common_end = min(known_ends) if known_ends else None
    query_start = (common_end - timedelta(seconds=window_seconds)) if common_end else None

    close_map: Dict[str, pd.Series] = {}
    volume_map: Dict[str, pd.Series] = {}

    for sym in symbol_list:
        df = await _load_symbol_df(
            exchange=exchange,
            symbol=sym,
            timeframe=timeframe,
            start_time=query_start,
            end_time=common_end,
        )
        if df.empty and timeframe not in _SUB_MINUTE_TIMEFRAMES:
            # Fallback to full scan for symbols whose shared window has no local cache.
            df = await _load_symbol_df(exchange=exchange, symbol=sym, timeframe=timeframe, end_time=common_end)
        if df.empty:
            continue
        tail = df.tail(max(120, int(lookback)))
        close = pd.to_numeric(tail.get("close"), errors="coerce")
        volume = pd.to_numeric(tail.get("volume"), errors="coerce")
        if close is None or volume is None:
            continue
        close = close.dropna()
        volume = volume.reindex(close.index).fillna(0.0)
        if len(close) < 30:
            continue
        close_map[sym] = close[~close.index.duplicated(keep="last")].sort_index()
        volume_map[sym] = volume[~volume.index.duplicated(keep="last")].sort_index()

    if not close_map:
        return pd.DataFrame(), pd.DataFrame(), []

    used = [c for c in close_map.keys() if c in volume_map]
    if not used:
        return pd.DataFrame(), pd.DataFrame(), []

    common_index = None
    for sym in used:
        idx = close_map[sym].index.intersection(volume_map[sym].index)
        common_index = idx if common_index is None else common_index.intersection(idx)

    if common_index is None or len(common_index) < 30:
        close_df = pd.DataFrame(close_map).sort_index()
        volume_df = pd.DataFrame(volume_map).reindex(close_df.index).sort_index().fillna(0.0)
        coverage_threshold = max(2, int(np.ceil(len(used) * 0.6)))
        eligible = close_df.notna().sum(axis=1) >= coverage_threshold
        close_df = close_df.loc[eligible].dropna(axis=1, how="all")
        volume_df = volume_df.reindex(close_df.index)[close_df.columns].fillna(0.0)
        used = list(close_df.columns)
    else:
        common_index = common_index.sort_values()
        close_df = pd.DataFrame({sym: close_map[sym].reindex(common_index) for sym in used}, index=common_index)
        volume_df = pd.DataFrame({sym: volume_map[sym].reindex(common_index) for sym in used}, index=common_index).fillna(0.0)

    close_df = close_df[used].dropna(how="all")
    volume_df = volume_df[used].reindex(close_df.index).fillna(0.0)
    return close_df, volume_df, used


async def _compute_factor_library_payload(
    *,
    exchange: str,
    symbols_requested: List[str],
    timeframe: str,
    lookback: int,
    quantile: float,
    series_limit: int,
    exclude_retired: bool,
) -> Dict[str, Any]:
    requested, excluded_retired = _research_retired_filter(
        exchange=exchange,
        timeframe=timeframe,
        requested=symbols_requested,
        exclude_retired=exclude_retired,
    )
    symbol_list = _expand_symbols_with_local(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        min_symbols=4,
        max_symbols=30,
        excluded_symbols=set(excluded_retired),
    )
    close_df, volume_df, used = await _build_factor_input_frames(
        exchange=exchange,
        symbol_list=symbol_list,
        timeframe=timeframe,
        lookback=lookback,
    )
    if close_df.empty or len(used) < 2:
        return _build_factor_library_placeholder(
            exchange=exchange,
            timeframe=timeframe,
            symbols_requested=symbols_requested,
            exclude_retired=exclude_retired,
            excluded_symbols=excluded_retired,
            reason="可用于多因子计算的数据不足（至少2个币种）",
        )

    result = await asyncio.to_thread(
        build_factor_library,
        close_df=close_df,
        volume_df=volume_df,
        quantile=float(quantile),
        timeframe=timeframe,
    )
    factors = result.factors
    if factors.empty:
        return _build_factor_library_placeholder(
            exchange=exchange,
            timeframe=timeframe,
            symbols_requested=symbols_requested,
            exclude_retired=exclude_retired,
            excluded_symbols=excluded_retired,
            reason="多因子计算失败",
        )

    latest = factors.iloc[-1].to_dict()
    mean_24 = factors.tail(min(24, len(factors))).mean().to_dict()
    std_24 = factors.tail(min(24, len(factors))).std().fillna(0.0).to_dict()
    corr = factors.corr().round(4).fillna(0.0).to_dict()

    series: List[Dict[str, Any]] = []
    tail = factors.tail(max(30, min(int(series_limit), 800)))
    for idx, row in tail.iterrows():
        payload = {"timestamp": _to_utc_iso(idx)}
        for col in factors.columns:
            payload[col] = round(float(row.get(col, 0.0)), 10)
        series.append(payload)

    asset_scores = []
    if not result.asset_scores.empty:
        for _, row in result.asset_scores.head(60).iterrows():
            asset_scores.append(
                {
                    "symbol": str(row.get("symbol")),
                    "score": round(float(row.get("score", 0.0)), 6),
                    "momentum": round(float(row.get("momentum", 0.0)), 6),
                    "value": round(float(row.get("value", 0.0)), 6),
                    "value_hml": round(float(row.get("value_hml", 0.0)), 6),
                    "quality": round(float(row.get("quality", 0.0)), 6),
                    "profitability": round(float(row.get("profitability", 0.0)), 6),
                    "investment": round(float(row.get("investment", 0.0)), 6),
                    "low_vol": round(float(row.get("low_vol", 0.0)), 6),
                    "liquidity": round(float(row.get("liquidity", 0.0)), 6),
                    "low_beta": round(float(row.get("low_beta", 0.0)), 6),
                    "size": round(float(row.get("size", 0.0)), 6),
                }
            )

    warnings: List[str] = []
    if len(used) < 4:
        warnings.append("当前可用币种较少（<4），横截面因子稳定性有限，建议补充更多币种历史数据。")
    diagnostics = dict(getattr(result, "diagnostics", {}) or {})
    warnings.extend(
        str(item)
        for item in list(diagnostics.get("warnings") or [])
        if str(item).strip()
    )

    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_effective": lookback,
        "symbols_requested": symbols_requested,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_after_filter": requested,
        },
        "symbols_used": used,
        "points": int(len(factors)),
        "factors": list(factors.columns),
        "catalog": FACTOR_CATALOG,
        "universe_size": len(used),
        "universe_quality": "low" if len(used) < 4 else "normal",
        "warnings": warnings,
        "diagnostics": diagnostics,
        "window_config": dict(diagnostics.get("window_config") or {}),
        "latest": {k: round(float(v), 10) for k, v in latest.items()},
        "mean_24": {k: round(float(v), 10) for k, v in mean_24.items()},
        "std_24": {k: round(float(v), 10) for k, v in std_24.items()},
        "correlation": corr,
        "series": series,
        "asset_scores": asset_scores,
        "degraded": bool(warnings),
        "error": "",
        "served_mode": "live",
    }


async def _compute_fama_payload(
    *,
    exchange: str,
    symbols_requested: List[str],
    timeframe: str,
    lookback: int,
    exclude_retired: bool,
) -> Dict[str, Any]:
    requested, excluded_retired = _research_retired_filter(
        exchange=exchange,
        timeframe=timeframe,
        requested=symbols_requested,
        exclude_retired=exclude_retired,
    )
    symbol_list = _expand_symbols_with_local(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        min_symbols=2,
        max_symbols=24,
        excluded_symbols=set(excluded_retired),
    )
    close_df, volume_df, used = await _build_factor_input_frames(
        exchange=exchange,
        symbol_list=symbol_list,
        timeframe=timeframe,
        lookback=int(lookback),
    )
    if close_df.empty or len(used) < 2:
        return _build_fama_placeholder(
            exchange=exchange,
            timeframe=timeframe,
            symbols_requested=symbols_requested,
            exclude_retired=exclude_retired,
            excluded_symbols=excluded_retired,
            reason="可用于因子计算的数据不足",
        )

    if close_df.empty or volume_df.empty:
        return _build_fama_placeholder(
            exchange=exchange,
            timeframe=timeframe,
            symbols_requested=symbols_requested,
            exclude_retired=exclude_retired,
            excluded_symbols=excluded_retired,
            reason="Fama 输入为空，无法计算风格因子",
        )

    factor_result = await asyncio.to_thread(
        build_factor_library,
        close_df=close_df,
        volume_df=volume_df,
        quantile=0.3,
        timeframe=timeframe,
    )
    factors = factor_result.factors.copy()
    if factors.empty:
        return _build_fama_placeholder(
            exchange=exchange,
            timeframe=timeframe,
            symbols_requested=symbols_requested,
            exclude_retired=exclude_retired,
            excluded_symbols=excluded_retired,
            reason="Fama 风格因子计算失败",
        )

    wanted = ["MKT", "SMB", "HML", "MOM", "RMW", "CMA", "VOL"]
    for col in wanted:
        if col not in factors.columns:
            factors[col] = 0.0
    factors = factors[wanted].fillna(0.0)

    latest = factors.iloc[-1].to_dict()
    mean_24 = factors.tail(min(24, len(factors))).mean().to_dict()
    std_24 = factors.tail(min(24, len(factors))).std().fillna(0.0).to_dict()

    out_series = []
    for idx, row in factors.tail(400).iterrows():
        out_series.append(
            {
                "timestamp": _to_utc_iso(idx),
                "MKT": round(float(row.get("MKT", 0.0)), 8),
                "SMB": round(float(row.get("SMB", 0.0)), 8),
                "HML": round(float(row.get("HML", 0.0)), 8),
                "MOM": round(float(row.get("MOM", 0.0)), 8),
                "RMW": round(float(row.get("RMW", 0.0)), 8),
                "CMA": round(float(row.get("CMA", 0.0)), 8),
                "VOL": round(float(row.get("VOL", 0.0)), 8),
            }
        )

    warnings: List[str] = []
    if len(used) < 4:
        warnings.append("当前可用币种较少（<4），Fama 风格因子稳定性有限。")

    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "symbols_requested": symbols_requested,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_after_filter": requested,
        },
        "symbols_used": used,
        "points": len(factors),
        "universe_size": len(used),
        "universe_quality": "low" if len(used) < 4 else "normal",
        "latest": {k: round(float(v), 8) for k, v in latest.items()},
        "mean_24": {k: round(float(v), 8) for k, v in mean_24.items()},
        "std_24": {k: round(float(v), 8) for k, v in std_24.items()},
        "series": out_series,
        "warnings": warnings,
        "degraded": bool(warnings),
        "error": "",
        "served_mode": "live",
    }


async def _refresh_factor_library_cache(
    cache_key: str,
    *,
    exchange: str,
    symbols_requested: List[str],
    timeframe: str,
    lookback: int,
    quantile: float,
    series_limit: int,
    exclude_retired: bool,
) -> None:
    try:
        payload = await _compute_factor_library_payload(
            exchange=exchange,
            symbols_requested=symbols_requested,
            timeframe=timeframe,
            lookback=lookback,
            quantile=quantile,
            series_limit=series_limit,
            exclude_retired=exclude_retired,
        )
        _store_research_cached_payload(_FACTOR_LIBRARY_CACHE, cache_key, payload)
        _store_research_disk_cached_payload("factor_library", cache_key, payload)
    except Exception as e:
        logger.warning(f"factor library refresh failed {cache_key}: {e}")
        cached = dict(_FACTOR_LIBRARY_CACHE.get(cache_key) or {})
        if cached:
            payload = dict(cached.get("payload") or {})
            payload["refresh_error"] = str(e)
            payload["refresh_failed_at"] = _utc_iso()
            cached["payload"] = payload
            _FACTOR_LIBRARY_CACHE[cache_key] = cached
    finally:
        _FACTOR_LIBRARY_REFRESH_TASKS.pop(cache_key, None)
        _FACTOR_LIBRARY_REFRESH_META.pop(cache_key, None)


async def _refresh_fama_cache(
    cache_key: str,
    *,
    exchange: str,
    symbols_requested: List[str],
    timeframe: str,
    lookback: int,
    exclude_retired: bool,
) -> None:
    try:
        payload = await _compute_fama_payload(
            exchange=exchange,
            symbols_requested=symbols_requested,
            timeframe=timeframe,
            lookback=lookback,
            exclude_retired=exclude_retired,
        )
        _store_research_cached_payload(_FAMA_CACHE, cache_key, payload)
    except Exception as e:
        logger.warning(f"fama refresh failed {cache_key}: {e}")
        cached = dict(_FAMA_CACHE.get(cache_key) or {})
        if cached:
            payload = dict(cached.get("payload") or {})
            payload["refresh_error"] = str(e)
            payload["refresh_failed_at"] = _utc_iso()
            cached["payload"] = payload
            _FAMA_CACHE[cache_key] = cached
    finally:
        _FAMA_REFRESH_TASKS.pop(cache_key, None)


def _factor_library_refresh_meta(cache_key: str) -> Dict[str, Any]:
    meta = dict(_FACTOR_LIBRARY_REFRESH_META.get(cache_key) or {})
    started_monotonic = meta.get("started_monotonic")
    if started_monotonic is not None:
        meta["elapsed_sec"] = max(0.0, time.monotonic() - float(started_monotonic))
    else:
        meta["elapsed_sec"] = 0.0
    return meta


@router.get("/klines")
async def get_klines(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    limit: int = 500,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    align: str = "tail",
):
    start_time = _normalize_query_datetime(start_time)
    end_time = _normalize_query_datetime(end_time)
    limit = max(10, min(limit, 5000))
    requested_exchange = str(exchange or "").lower() or "binance"
    align_mode = str(align or "tail").lower()
    candidates = [requested_exchange] + [
        ex for ex in ["binance", "gate", "okx"] if ex != requested_exchange
    ]
    actual_exchange = requested_exchange
    df = pd.DataFrame()

    load_start = start_time
    load_end = end_time
    if load_start is None and align_mode != "head":
        effective_end = load_end or datetime.now()
        seconds = _timeframe_seconds(timeframe)
        # UI tail reads only need a bounded recent window; otherwise partitioned
        # parquet loads may scan the entire history before trimming to `limit`.
        lookback_seconds = max(limit * seconds * 4, seconds * 240)
        if timeframe in _SUB_MINUTE_TIMEFRAMES:
            lookback_seconds = max(900, min(lookback_seconds, 6 * 3600))
        load_start = effective_end - timedelta(seconds=lookback_seconds)

    async def _fetch_live_df(ex_name: str, live_limit: int) -> pd.DataFrame:
        if timeframe in _SUB_MINUTE_TIMEFRAMES:
            effective_limit = max(120, min(int(live_limit or 0), 480))
            seconds = _timeframe_seconds(timeframe)
            q_end_time = end_time or datetime.now()
            q_start_time = start_time
            if q_start_time is None:
                window_seconds = max(effective_limit * seconds, 300)
                window_seconds = min(window_seconds, 7200)
                q_start_time = q_end_time - timedelta(seconds=window_seconds)
            batch_cap = max(12, min(40, int(effective_limit // 15) + 8))
            trade_cap = max(6000, min(60000, int(effective_limit * 80)))
            try:
                trades = await asyncio.wait_for(
                    _fetch_public_trades(
                        ex_name,
                        symbol,
                        q_start_time,
                        q_end_time,
                        limit=1000,
                        max_batches=batch_cap,
                        max_trades=trade_cap,
                    ),
                    timeout=8.0,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                raise TimeoutError(f"{ex_name} public trades fetch timeout/cancelled") from e
            return _trades_to_ohlcv(trades, timeframe)

        try:
            klines = await asyncio.wait_for(
                _safe_exchange_call(ex_name, "get_klines", symbol, timeframe, limit=live_limit),
                timeout=8.0,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            raise TimeoutError(f"{ex_name} get_klines timeout/cancelled") from e
        if not klines:
            return pd.DataFrame()
        live_df = pd.DataFrame(
            [
                {
                    "timestamp": k.timestamp,
                    "open": k.open,
                    "high": k.high,
                    "low": k.low,
                    "close": k.close,
                    "volume": k.volume,
                }
                for k in klines
            ]
        ).set_index("timestamp")
        return _normalize_kline_frame_for_compare(live_df)

    # Prefer local data first, and fallback across exchanges.
    for ex in candidates:
        local_df = await _load_local_or_aggregate(
            exchange=ex,
            symbol=symbol,
            timeframe=timeframe,
            start_time=load_start,
            end_time=load_end,
        )
        if local_df.empty:
            continue
        df = local_df
        actual_exchange = ex
        break

    # If no local data, fetch live and persist.
    if df.empty:
        last_error: Optional[str] = None
        for ex in candidates:
            try:
                live_df = await _fetch_live_df(ex, live_limit=limit)
                if live_df.empty:
                    continue
                df = _normalize_kline_frame_for_compare(live_df)
                actual_exchange = ex
                await _save_df_to_parquet(actual_exchange, symbol, timeframe, df)
                break
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                last_error = f"{ex} live fetch timeout/cancelled: {e}"
            except Exception as e:
                last_error = str(e)
        if df.empty and last_error:
            return {
                "exchange": requested_exchange,
                "actual_exchange": requested_exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "data": [],
                "error": last_error,
            }
    else:
        # Merge latest live bars when requesting near-now window to keep chart realtime.
        if end_time is None:
            live_refresh_key = _live_cache_refresh_key(actual_exchange, symbol, timeframe)

            async def _refresh_cache_from_live(ex_name: str, live_limit: int) -> None:
                try:
                    fresh_df = await _fetch_live_df(ex_name, live_limit=live_limit)
                    if not fresh_df.empty:
                        await _save_df_to_parquet(ex_name, symbol, timeframe, fresh_df)
                except Exception as refresh_err:
                    logger.debug(f"background live refresh skipped: {refresh_err}")

            live_limit = max(120, min(limit, 240 if timeframe in _SUB_MINUTE_TIMEFRAMES else 1200))
            quick_timeout = 1.2 if timeframe in _SUB_MINUTE_TIMEFRAMES else 2.5
            stale_seconds = 0.0
            stale_threshold = max(90.0, _timeframe_seconds(timeframe) * 3.0)
            if not df.empty:
                last_local_ts = pd.to_datetime(df.index.max())
                current_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                stale_seconds = max(
                    0.0,
                    (
                        current_utc_naive
                        - last_local_ts.to_pydatetime().replace(tzinfo=None)
                    ).total_seconds(),
                )
            # Data-page first paint should not be held hostage by slow exchange
            # refreshes when we already have enough local candles to render.
            # Schedule a refresh in the background and let the next poll merge
            # fresh bars.
            enough_local_rows = len(df.index) >= max(10, min(limit, 300))
            should_block_for_refresh = stale_seconds > stale_threshold and not enough_local_rows
            if stale_seconds > stale_threshold and enough_local_rows:
                _schedule_live_cache_refresh(
                    live_refresh_key,
                    lambda: _refresh_cache_from_live(actual_exchange, live_limit),
                )
            used_public_fallback = False
            if should_block_for_refresh and actual_exchange == "binance" and timeframe not in _SUB_MINUTE_TIMEFRAMES:
                try:
                    public_df = await asyncio.wait_for(
                        _fetch_binance_public_klines(
                            symbol=symbol,
                            timeframe=timeframe,
                            limit=live_limit,
                        ),
                        timeout=3.0,
                    )
                    if not public_df.empty:
                        await _save_df_to_parquet(actual_exchange, symbol, timeframe, public_df)
                        df = pd.concat([df, public_df])
                        df = _normalize_kline_frame_for_compare(df)
                        df = df[~df.index.duplicated(keep="last")].sort_index()
                        used_public_fallback = True
                except Exception as public_err:
                    logger.debug(f"public kline fallback skipped: {public_err}")
            if should_block_for_refresh and not used_public_fallback:
                try:
                    live_df = await asyncio.wait_for(
                        _fetch_live_df(actual_exchange, live_limit=live_limit),
                        timeout=quick_timeout,
                    )
                    if not live_df.empty:
                        await _save_df_to_parquet(actual_exchange, symbol, timeframe, live_df)
                        df = pd.concat([df, live_df])
                        df = _normalize_kline_frame_for_compare(df)
                        df = df[~df.index.duplicated(keep="last")].sort_index()
                except (asyncio.TimeoutError, asyncio.CancelledError) as live_err:
                    logger.debug(f"live refresh timeout/cancelled: {live_err}")
                    _schedule_live_cache_refresh(
                        live_refresh_key,
                        lambda: _refresh_cache_from_live(actual_exchange, live_limit),
                    )
                except Exception as live_err:
                    logger.debug(f"live refresh skipped: {live_err}")
                    _schedule_live_cache_refresh(
                        live_refresh_key,
                        lambda: _refresh_cache_from_live(actual_exchange, live_limit),
                    )

    if df.empty:
        return {
            "exchange": requested_exchange,
            "actual_exchange": actual_exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "data": [],
            "message": "无可用K线数据",
        }

    if start_time:
        df = df[df.index >= start_time]
    if end_time:
        df = df[df.index <= end_time]

    if df.empty:
        return {
            "exchange": requested_exchange,
            "actual_exchange": actual_exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "data": [],
            "message": "指定时间范围内无可用K线数据",
        }

    if align_mode == "head":
        df = df.head(limit)
    else:
        if start_time and not end_time:
            df = df.head(limit)
        else:
            df = df.tail(limit)

    return {
        "exchange": requested_exchange,
        "actual_exchange": actual_exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "data": [
            {
                "timestamp": _to_utc_iso(idx),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
            for idx, row in df.iterrows()
        ],
    }


@router.get("/ticker")
async def get_ticker(exchange: str, symbol: str):
    try:
        ticker = await _safe_exchange_call(exchange, "get_ticker", symbol)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"交易所未连接或行情拉取失败: {e}")

    return {
        "exchange": exchange,
        "symbol": symbol,
        "last": ticker.last,
        "bid": ticker.bid,
        "ask": ticker.ask,
        "high_24h": ticker.high_24h,
        "low_24h": ticker.low_24h,
        "volume_24h": ticker.volume_24h,
        "timestamp": ticker.timestamp.isoformat(),
    }


@router.get("/tickers")
async def get_tickers(exchange: str):
    symbols = exchange_manager.get_supported_symbols(exchange)
    tickers = []

    for symbol in symbols[:20]:
        try:
            ticker = await _safe_exchange_call(exchange, "get_ticker", symbol)
            tickers.append(
                {
                    "symbol": symbol,
                    "last": ticker.last,
                    "change_24h": (ticker.last - ticker.low_24h) / ticker.low_24h if ticker.low_24h > 0 else 0,
                    "volume_24h": ticker.volume_24h,
                }
            )
        except Exception:
            continue

    return {"exchange": exchange, "tickers": tickers}


async def run_download_historical_data(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    days: int = 365,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    progress_callback: Optional[Any] = None,
):
    symbol = _normalize_symbol_alias(normalize_symbol(symbol) or str(symbol or "").strip())
    start_time = _normalize_query_datetime(start_time)
    end_time = _normalize_query_datetime(end_time)
    if end_time is None:
        end_time = datetime.now()
    if start_time is None:
        start_time = end_time - timedelta(days=days)
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    span_days = max(0.0, (end_time - start_time).total_seconds() / 86400.0)

    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        for alt_exchange in ["gate", "binance"]:
            connector = exchange_manager.get_exchange(alt_exchange)
            if connector:
                exchange = alt_exchange
                break

    if not connector:
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": 0,
            "error": "没有可用的交易所连接",
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
        }

    async def _download_with_native_source(source_exchange: str) -> Dict[str, Any]:
        estimated_total = _estimate_expected_bars(start_time, end_time, timeframe) or 0
        await _emit_download_progress_message(
            progress_callback,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            message=(
                f"开始尝试 {source_exchange} 原生K线源"
                if source_exchange == exchange
                else f"{exchange} 原生下载失败，切换到 {source_exchange} K线源兜底"
            ),
            status="running",
            estimated_total_candles=estimated_total,
        )

        async def _native_progress(progress: Any) -> None:
            if not progress_callback:
                return
            prefix = f"数据源 {source_exchange}"
            message = str(getattr(progress, "message", "") or "").strip()
            proxied = _copy_progress_snapshot(
                progress,
                exchange=exchange,
                message=f"{prefix} | {message}" if message else prefix,
            )
            result = progress_callback(proxied)
            if asyncio.iscoroutine(result):
                await result

        klines = await historical_data_manager.download_historical_klines(
            exchange=source_exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            save_to_parquet=False,
            progress_callback=_native_progress,
        )
        if not klines:
            raise RuntimeError(f"{source_exchange} 未返回有效K线")

        await data_storage.save_klines_to_parquet(
            klines,
            exchange,
            symbol,
            timeframe,
        )
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(klines),
            "start": klines[0].timestamp.isoformat(),
            "end": klines[-1].timestamp.isoformat(),
            "source": "native",
            "source_exchange": source_exchange,
            "message": (
                "历史K线下载完成"
                if source_exchange == exchange
                else f"{exchange} 原生不可用，已使用 {source_exchange} K线源补齐并写回本地"
            ),
        }

    async def _download_with_coinglass_source(source_exchange: str) -> Dict[str, Any]:
        if str(timeframe or "").strip().lower() not in _COINGLASS_PRICE_HISTORY_TIMEFRAMES:
            raise RuntimeError(f"Coinglass 价格历史暂不支持 {timeframe}")
        if not coinglass_enabled():
            raise RuntimeError("Coinglass 未启用或缺少 API Key")

        manifest = get_coinglass_manifest("price_history")
        if manifest is None:
            raise RuntimeError("Coinglass price_history manifest 未注册")

        estimated_total = _estimate_expected_bars(start_time, end_time, timeframe) or 0
        start_ms = _datetime_to_epoch_ms(start_time)
        end_ms = _datetime_to_epoch_ms(end_time)
        if start_ms is None or end_ms is None:
            raise RuntimeError("Coinglass 时间范围无效")

        all_rows: List[Dict[str, Any]] = []
        cursor_ms = start_ms
        pages_fetched = 0
        last_success_at: Optional[datetime] = None
        request_limit = max(1, min(int(settings.MAX_CANDLES_PER_REQUEST or 1000), 1000))

        await _emit_download_progress_message(
            progress_callback,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            message=f"Binance/Gate 原生K线失败，切换到 Coinglass {source_exchange} 价格历史",
            status="running",
            estimated_total_candles=estimated_total,
        )

        async with CoinglassClient() as client:
            while cursor_ms <= end_ms:
                pages_fetched += 1
                response = await client.request_dataset(
                    manifest,
                    symbol=symbol,
                    exchange=source_exchange,
                    interval=timeframe,
                    limit=request_limit,
                    start_time=cursor_ms,
                    end_time=end_ms,
                    manual=False,
                )
                normalized = normalize_dataset_response(
                    dataset="price_history",
                    request_meta=response.get("params") or {},
                    response_payload=response.get("payload"),
                )
                rows = list(normalized.get("rows") or [])
                if not rows:
                    if pages_fetched == 1:
                        raise RuntimeError(normalized.get("error") or f"Coinglass {source_exchange} 未返回价格历史")
                    break

                all_rows.extend(rows)
                page_df = _coinglass_rows_to_price_df(rows, start_time, end_time)
                if page_df.empty:
                    if pages_fetched == 1:
                        raise RuntimeError(f"Coinglass {source_exchange} 返回了数据，但无法解析为OHLC")
                    break

                last_ts = page_df.index.max()
                if pd.isna(last_ts):
                    raise RuntimeError(f"Coinglass {source_exchange} 返回了空时间戳")
                next_cursor_ms = int(last_ts.to_pydatetime().replace(tzinfo=timezone.utc).timestamp() * 1000) + 1
                if next_cursor_ms <= cursor_ms:
                    raise RuntimeError(f"Coinglass {source_exchange} 返回重复时间戳，下载无法推进")

                cursor_ms = next_cursor_ms
                last_success_at = datetime.now()
                progress_pct = 0.0
                if estimated_total > 0:
                    progress_pct = min(99.5 if cursor_ms <= end_ms else 100.0, (len(all_rows) / estimated_total) * 100.0)
                await _emit_download_progress_message(
                    progress_callback,
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=end_time,
                    message=f"Coinglass {source_exchange} 第 {pages_fetched} 页已返回 {len(page_df)} 根价格K线",
                    status="running",
                    downloaded_candles=len(all_rows),
                    estimated_total_candles=estimated_total,
                    progress_pct=progress_pct,
                    pages_fetched=pages_fetched,
                    current_time=last_ts.to_pydatetime().replace(tzinfo=None),
                    last_success_at=last_success_at,
                )

                if len(rows) < request_limit or cursor_ms > end_ms:
                    break

        frame = _coinglass_rows_to_price_df(all_rows, start_time, end_time)
        if frame.empty:
            raise RuntimeError(f"Coinglass {source_exchange} 未返回可保存的价格K线")

        await _save_df_to_parquet(exchange, symbol, timeframe, frame)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(len(frame)),
            "start": frame.index.min().isoformat(),
            "end": frame.index.max().isoformat(),
            "source": "coinglass",
            "source_exchange": source_exchange,
            "message": f"{exchange} 与 Gate 原生K线不可用，已使用 Coinglass {source_exchange} 价格历史补齐",
        }

    if timeframe in _SUB_MINUTE_TIMEFRAMES:
        if span_days > 7:
            if exchange == "binance" and timeframe == "1s":
                start_date = start_time.date()
                end_date = (end_time - timedelta(days=1)).date()
                if end_date < start_date:
                    end_date = start_date
                stats = await asyncio.to_thread(
                    download_binance_1s_daily_archive,
                    symbol,
                    start_date,
                    end_date,
                    True,
                )
                return {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "count": int(stats.total_rows),
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "message": "已从 Binance 历史包下载秒级数据",
                    "archive_stats": stats.to_dict(),
                }

            task = second_level_backfill_manager.start_task(
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                window_days=1,
            )
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "async_task": task,
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "message": "秒级数据跨度超过7天，已切换为后台分片回填任务",
            }

        trades = await _fetch_public_trades(exchange, symbol, start_time, end_time)
        kdf = _trades_to_ohlcv(trades, timeframe)
        if kdf.empty:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "error": "未获取到逐笔成交，无法生成子分钟K线",
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            }

        await _save_df_to_parquet(exchange, symbol, timeframe, kdf)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(len(kdf)),
            "start": kdf.index[0].isoformat(),
            "end": kdf.index[-1].isoformat(),
            "message": "子分钟K线已从逐笔成交聚合完成",
        }

    if timeframe in {"1w", "1M"}:
        await historical_data_manager.download_historical_klines(
            exchange=exchange,
            symbol=symbol,
            timeframe="1d",
            start_time=start_time,
            end_time=end_time,
            progress_callback=progress_callback,
        )
        day_df = await data_storage.load_klines_from_parquet(exchange=exchange, symbol=symbol, timeframe="1d")
        agg_df = _resample_ohlcv(day_df, timeframe)
        if agg_df.empty:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "error": "日线数据不足，无法聚合周/月线",
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            }

        await _save_df_to_parquet(exchange, symbol, timeframe, agg_df)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(len(agg_df)),
            "start": agg_df.index[0].isoformat(),
            "end": agg_df.index[-1].isoformat(),
            "message": "周/月线已聚合生成",
        }

    source_attempts: List[str] = []
    for candidate in [exchange, "binance", "gate"]:
        normalized_exchange = str(candidate or "").strip().lower()
        if not normalized_exchange or normalized_exchange in source_attempts:
            continue
        source_attempts.append(normalized_exchange)

    source_errors: List[str] = []

    for source_exchange in source_attempts:
        if exchange_manager.get_exchange(source_exchange) is None:
            source_errors.append(f"{source_exchange}: connector_unavailable")
            continue
        try:
            return await _download_with_native_source(source_exchange)
        except Exception as exc:
            source_errors.append(f"{source_exchange}: {exc}")
            logger.warning(f"native historical download failed source={source_exchange} symbol={symbol} timeframe={timeframe}: {exc}")

    coinglass_errors: List[str] = []
    for source_exchange in source_attempts:
        try:
            return await _download_with_coinglass_source(source_exchange)
        except (CoinglassBudgetExceeded, CoinglassError, RuntimeError) as exc:
            coinglass_errors.append(f"{source_exchange}: {exc}")
            logger.warning(f"coinglass historical fallback failed source={source_exchange} symbol={symbol} timeframe={timeframe}: {exc}")

    combined_errors = " | ".join(source_errors + coinglass_errors) or "unknown_download_failure"
    return {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "count": 0,
        "error": combined_errors,
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
    }


async def _run_download_task(task_id: str, payload: Dict[str, Any]) -> None:
    task = _DOWNLOAD_TASKS.get(task_id)
    if not task:
        return
    semaphore = _get_download_task_semaphore()
    try:
        async with semaphore:
            task["error"] = None
            task["status"] = "running"
            task["started_at"] = datetime.now(timezone.utc).isoformat()
            task["updated_at"] = task["started_at"]
            task["heartbeat_at"] = task["started_at"]
            task["status_message"] = "任务已开始，正在下载历史K线"
            _touch_download_task_progress(task)

            async def _progress_callback(progress: Any) -> None:
                live_task = _DOWNLOAD_TASKS.get(task_id)
                if not live_task:
                    return
                _apply_download_progress(live_task, progress)

            timeout_sec = _download_task_timeout_sec(payload)
            result = await asyncio.wait_for(
                run_download_historical_data(
                    exchange=str(payload.get("exchange") or "binance"),
                    symbol=str(payload.get("symbol") or "BTC/USDT"),
                    timeframe=str(payload.get("timeframe") or "1h"),
                    days=int(payload.get("days") or 365),
                    start_time=payload.get("start_time"),
                    end_time=payload.get("end_time"),
                    progress_callback=_progress_callback,
                ),
                timeout=timeout_sec,
            )
            task["result"] = result
            result_error = ""
            if isinstance(result, dict):
                result_error = str(result.get("error") or "").strip()
                if result.get("count") is not None:
                    task["total_candles"] = _safe_int(result.get("count"), task.get("total_candles") or 0)
                    task["downloaded_candles"] = _safe_int(result.get("count"), task.get("downloaded_candles") or 0)
                if str(result.get("message") or "").strip():
                    task["status_message"] = str(result.get("message") or "").strip()
            if result_error:
                task["status"] = "failed"
                task["error"] = result_error
                task["last_error"] = result_error
            else:
                task["status"] = "completed"
                task["error"] = None
                task["progress_pct"] = 100.0
                task["heartbeat_at"] = datetime.now(timezone.utc).isoformat()
                task["updated_at"] = task["heartbeat_at"]
            _touch_download_task_progress(task)
    except asyncio.TimeoutError:
        timeout_sec = _download_task_timeout_sec(payload)
        timeout_display_sec = max(1, int(math.ceil(timeout_sec)))
        timeout_message = f"Download task timed out after {timeout_display_sec} seconds"
        result = {
            "exchange": str(payload.get("exchange") or task.get("exchange") or "binance"),
            "symbol": str(payload.get("symbol") or task.get("symbol") or "BTC/USDT"),
            "timeframe": str(payload.get("timeframe") or task.get("timeframe") or "1h"),
            "count": _safe_int(task.get("downloaded_candles"), 0),
            "error": timeout_message,
            "timeout_sec": timeout_display_sec,
            "start": task.get("start_time"),
            "end": task.get("end_time"),
        }
        task["result"] = result
        task["status"] = "failed"
        task["error"] = timeout_message
        task["last_error"] = timeout_message
        task["status_message"] = timeout_message
        task["updated_at"] = datetime.now(timezone.utc).isoformat()
        task["heartbeat_at"] = task["updated_at"]
        _touch_download_task_progress(task)
    except Exception as e:
        task["status"] = "failed"
        task["error"] = str(e)
        task["last_error"] = str(e)
        task["status_message"] = str(e)
        task["updated_at"] = datetime.now(timezone.utc).isoformat()
        task["heartbeat_at"] = task["updated_at"]
        _touch_download_task_progress(task)
    finally:
        task["finished_at"] = datetime.now(timezone.utc).isoformat()
        task["updated_at"] = task["finished_at"]
        task["heartbeat_at"] = task["finished_at"]
        _touch_download_task_progress(task)
        current_background_task = _DOWNLOAD_BACKGROUND_TASKS.get(task_id)
        if current_background_task is asyncio.current_task():
            _DOWNLOAD_BACKGROUND_TASKS.pop(task_id, None)
        _prune_download_tasks()


def _normalize_download_symbol_list(symbols: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in symbols or []:
        normalized = _normalize_symbol_alias(normalize_symbol(str(raw or "").strip()))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _queue_download_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    _prune_download_tasks()
    task_id = _new_download_task_id(payload)
    start_time = payload.get("start_time")
    end_time = payload.get("end_time")
    task_record = {
        "task_id": task_id,
        "status": "pending",
        "batch_id": str(payload.get("batch_id") or ""),
        "exchange": str(payload.get("exchange") or "binance"),
        "symbol": _normalize_symbol_alias(normalize_symbol(str(payload.get("symbol") or "BTC/USDT"))),
        "timeframe": str(payload.get("timeframe") or "1h"),
        "days": int(payload.get("days") or 0),
        "timeout_sec": int(_download_task_timeout_sec(payload)),
        "start_time": start_time.isoformat() if isinstance(start_time, datetime) else (str(start_time) if start_time else None),
        "end_time": end_time.isoformat() if isinstance(end_time, datetime) else (str(end_time) if end_time else None),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }
    task_record.update(_download_task_progress_defaults())
    task_record["status_message"] = "任务排队中，等待下载槽位"
    task_record["progress"]["message"] = task_record["status_message"]
    _DOWNLOAD_TASKS[task_id] = task_record
    background_task = asyncio.create_task(_run_download_task(task_id, payload))
    _DOWNLOAD_BACKGROUND_TASKS[task_id] = background_task

    def _cleanup_background_task(done_task: asyncio.Task[None], *, tracked_task_id: str = task_id) -> None:
        if _DOWNLOAD_BACKGROUND_TASKS.get(tracked_task_id) is done_task:
            _DOWNLOAD_BACKGROUND_TASKS.pop(tracked_task_id, None)

    background_task.add_done_callback(_cleanup_background_task)
    return task_record


def _should_queue_single_download(
    *,
    requested_background: Optional[bool],
    timeframe: str,
    span_days: float,
) -> bool:
    if requested_background is not None:
        return bool(requested_background)
    return not (str(timeframe or "") in {"1h", "4h", "1d"} and span_days <= 30)


@router.post("/download", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def download_historical_data(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    days: int = 365,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    background: Optional[bool] = None,
):
    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "days": days,
        "start_time": start_time,
        "end_time": end_time,
    }
    span_ref_end = _normalize_query_datetime(end_time) or datetime.now()
    span_ref_start = _normalize_query_datetime(start_time) or (span_ref_end - timedelta(days=max(1, int(days or 1))))
    span_days = max(0.0, (span_ref_end - span_ref_start).total_seconds() / 86400.0)
    should_background = _should_queue_single_download(
        requested_background=background,
        timeframe=timeframe,
        span_days=span_days,
    )
    if not should_background:
        return await run_download_historical_data(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            days=days,
            start_time=start_time,
            end_time=end_time,
        )

    task_record = _queue_download_task(payload)
    return {
        "queued": True,
        "task_id": task_record["task_id"],
        "status": "pending",
        "message": "历史数据下载已转为后台任务",
        "async_task": task_record,
        "task": task_record,
    }


@router.post("/download/batch", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def download_historical_data_batch(req: BatchDownloadRequest):
    exchange = str(req.exchange or "binance").strip().lower() or "binance"
    timeframe = str(req.timeframe or "1h").strip() or "1h"
    symbols = _normalize_download_symbol_list(list(req.symbols or []))
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")
    if len(symbols) > 100:
        raise HTTPException(status_code=400, detail="单次批量下载最多支持 100 个 symbols")

    start_time = _normalize_query_datetime(req.start_time)
    end_time = _normalize_query_datetime(req.end_time)
    days = max(1, int(req.days or 365))
    if not bool(req.background) and len(symbols) == 1:
        result = await run_download_historical_data(
            exchange=exchange,
            symbol=symbols[0],
            timeframe=timeframe,
            days=days,
            start_time=start_time,
            end_time=end_time,
        )
        return {
            "queued": False,
            "message": "单个历史下载已直接执行",
            "exchange": exchange,
            "timeframe": timeframe,
            "days": days,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "symbols": symbols,
            "task_count": 0,
            "task_ids": [],
            "tasks": [],
            "results": [result],
        }

    batch_id = _new_download_batch_id(exchange, timeframe, symbols, start_time, end_time)
    tasks: List[Dict[str, Any]] = []
    for symbol in symbols:
        payload = {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "days": days,
            "start_time": start_time,
            "end_time": end_time,
            "batch_id": batch_id,
        }
        tasks.append(_queue_download_task(payload))

    return {
        "queued": True,
        "message": f"已创建 {len(tasks)} 个历史下载任务",
        "batch_id": batch_id,
        "exchange": exchange,
        "timeframe": timeframe,
        "days": days,
        "start_time": start_time.isoformat() if start_time else None,
        "end_time": end_time.isoformat() if end_time else None,
        "symbols": symbols,
        "task_count": len(tasks),
        "task_ids": [str(item.get("task_id") or "") for item in tasks],
        "tasks": tasks,
    }


@router.get("/integrity/check")
async def check_data_integrity(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
):
    df = await _load_local_or_aggregate(exchange=exchange, symbol=symbol, timeframe=timeframe)
    if df.empty:
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "ok": False,
            "message": "无本地数据",
            "quality": _validate_ohlcv(df),
            "missing": {"missing_count": 0, "missing_preview": []},
        }

    quality = _validate_ohlcv(df)
    missing = _detect_missing_bars(df, timeframe)
    ok = quality["invalid_rows"] == 0 and quality["duplicate_rows"] == 0

    return {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "ok": bool(ok),
        "quality": quality,
        "missing": missing,
        "rows": int(len(df)),
        "start": df.index.min().isoformat(),
        "end": df.index.max().isoformat(),
    }


@router.post("/integrity/repair", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def repair_data_integrity(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
):
    df = await _load_local_or_aggregate(exchange=exchange, symbol=symbol, timeframe=timeframe)
    if df.empty:
        raise HTTPException(status_code=404, detail="无本地数据可修复")

    before_missing = _detect_missing_bars(df, timeframe)
    cleaned = df.copy()
    cleaned = cleaned[~cleaned.index.duplicated(keep="last")].sort_index()
    cleaned = _fill_missing_bars(cleaned, timeframe)

    await _save_df_to_parquet(exchange, symbol, timeframe, cleaned)

    after_missing = _detect_missing_bars(cleaned, timeframe)
    return {
        "success": True,
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "before": {
            "rows": int(len(df)),
            "missing": before_missing,
            "quality": _validate_ohlcv(df),
        },
        "after": {
            "rows": int(len(cleaned)),
            "missing": after_missing,
            "quality": _validate_ohlcv(cleaned),
        },
    }


@router.get("/cross-validate")
async def cross_validate_data(
    symbol: str,
    timeframe: str = "1h",
    primary_exchange: str = "binance",
    secondary_exchange: str = "gate",
    limit: int = 500,
):
    primary = await _load_local_or_aggregate(primary_exchange, symbol, timeframe)
    secondary = await _load_local_or_aggregate(secondary_exchange, symbol, timeframe)

    if primary.empty or secondary.empty:
        raise HTTPException(status_code=404, detail="两路数据至少一路缺失")

    p = primary.tail(limit)[["close", "volume"]].copy()
    s = secondary.tail(limit)[["close", "volume"]].copy()

    merged = p.join(s, how="inner", lsuffix="_p", rsuffix="_s")
    if merged.empty:
        raise HTTPException(status_code=400, detail="两路数据无重合时间区间")

    close_diff_pct = (merged["close_p"] - merged["close_s"]).abs() / merged["close_p"].replace(0, pd.NA)
    volume_diff_pct = (merged["volume_p"] - merged["volume_s"]).abs() / merged["volume_p"].replace(0, pd.NA)

    close_diff_pct = close_diff_pct.fillna(0)
    volume_diff_pct = volume_diff_pct.fillna(0)

    result = {
        "symbol": symbol,
        "timeframe": timeframe,
        "primary_exchange": primary_exchange,
        "secondary_exchange": secondary_exchange,
        "overlap_bars": int(len(merged)),
        "close_diff": {
            "mean_pct": round(float(close_diff_pct.mean() * 100), 6),
            "max_pct": round(float(close_diff_pct.max() * 100), 6),
            "p95_pct": round(float(close_diff_pct.quantile(0.95) * 100), 6),
        },
        "volume_diff": {
            "mean_pct": round(float(volume_diff_pct.mean() * 100), 6),
            "max_pct": round(float(volume_diff_pct.max() * 100), 6),
            "p95_pct": round(float(volume_diff_pct.quantile(0.95) * 100), 6),
        },
    }

    result["is_consistent"] = result["close_diff"]["mean_pct"] < 1.0
    return result


@router.post("/reconnect", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def reconnect_exchange(exchange: str):
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        ok = await exchange_manager.initialize([exchange])
        return {
            "exchange": exchange,
            "connected": bool(ok),
            "message": "交易所连接已初始化" if ok else "初始化失败",
        }

    try:
        await connector.disconnect()
    except Exception:
        pass

    ok = await connector.connect()
    return {
        "exchange": exchange,
        "connected": bool(ok),
        "message": "重连成功" if ok else "重连失败",
    }


@router.get("/coverage")
async def get_data_coverage(exchange: str, symbol: str, timeframe: str = "1h"):
    return await historical_data_manager.get_data_coverage(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )


@router.get("/storage/stats")
async def get_storage_stats():
    return await data_storage.get_storage_stats()


@router.get("/storage/health")
async def get_storage_health(exact: bool = False):
    storage_root = Path(settings.DATA_STORAGE_PATH)
    backup_root = Path(settings.CACHE_PATH) / "symbol_dir_backups"

    datasets: List[Dict[str, Any]] = []
    exchange_rows: List[Dict[str, Any]] = []
    duplicate_symbol_dirs: List[Dict[str, Any]] = []
    unique_symbols = set()
    unique_timeframes = set()
    total_active_files = 0
    total_partition_files = 0
    total_corrupt_files = 0
    total_size_bytes = 0
    datasets_with_issues = 0
    exact_scan_count = 0
    fast_scan_count = 0
    suppressed_gap_datasets = 0

    if storage_root.exists():
        for exchange_dir in sorted([p for p in storage_root.iterdir() if p.is_dir()]):
            exchange_name = exchange_dir.name
            symbol_buckets: Dict[str, List[Path]] = {}
            for symbol_dir in sorted([p for p in exchange_dir.iterdir() if p.is_dir()]):
                normalized_symbol = symbol_from_storage_dirname(symbol_dir.name)
                if not normalized_symbol:
                    continue
                symbol_buckets.setdefault(normalized_symbol, []).append(symbol_dir)

            exchange_dataset_rows: List[Dict[str, Any]] = []
            exchange_size_bytes = 0
            exchange_corrupt_files = 0
            exchange_partition_files = 0
            exchange_active_files = 0

            for normalized_symbol, symbol_dirs in sorted(symbol_buckets.items()):
                unique_symbols.add((exchange_name, normalized_symbol))
                if len(symbol_dirs) > 1:
                    duplicate_symbol_dirs.append(
                        {
                            "exchange": exchange_name,
                            "symbol": normalized_symbol,
                            "directories": [p.name for p in symbol_dirs],
                        }
                    )

                timeframe_map: Dict[str, Dict[str, Any]] = {}
                for symbol_dir in symbol_dirs:
                    for file_path in sorted(symbol_dir.glob("*.parquet")):
                        timeframe = file_path.name.split(".corrupt_", 1)[0].replace(".parquet", "")
                        if not timeframe:
                            continue
                        bucket = timeframe_map.setdefault(
                            timeframe,
                            {"single_files": [], "partition_files": [], "corrupt_files": 0},
                        )
                        if ".corrupt_" in file_path.name:
                            bucket["corrupt_files"] += 1
                            total_corrupt_files += 1
                            exchange_corrupt_files += 1
                            continue
                        bucket["single_files"].append(file_path)
                        total_active_files += 1
                        exchange_active_files += 1

                    for part_dir in sorted([p for p in symbol_dir.iterdir() if p.is_dir() and p.name.endswith("_parts")]):
                        timeframe = part_dir.name[:-6]
                        if not timeframe:
                            continue
                        bucket = timeframe_map.setdefault(
                            timeframe,
                            {"single_files": [], "partition_files": [], "corrupt_files": 0},
                        )
                        for file_path in sorted(part_dir.glob("*.parquet")):
                            if ".corrupt_" in file_path.name:
                                bucket["corrupt_files"] += 1
                                total_corrupt_files += 1
                                exchange_corrupt_files += 1
                                continue
                            bucket["partition_files"].append(file_path)
                            total_partition_files += 1
                            exchange_partition_files += 1

                for timeframe, bucket in sorted(timeframe_map.items()):
                    physical_files = [*bucket["single_files"], *bucket["partition_files"]]
                    if not physical_files:
                        continue
                    unique_timeframes.add(timeframe)

                    scan = _scan_parquet_files(physical_files)
                    total_size_bytes += int(scan["size_bytes"] or 0)
                    exchange_size_bytes += int(scan["size_bytes"] or 0)
                    scan_mode = "fast"
                    gap_count = None
                    gap_preview: List[str] = []
                    gap_issue_suppressed = False
                    scan_note = None
                    coverage_ratio = None
                    logical_rows = int(scan["rows"] or 0)
                    start_at = scan.get("start")
                    end_at = scan.get("end")

                    should_exact_scan = (
                        bool(exact)
                        and timeframe not in _SUB_MINUTE_TIMEFRAMES
                        and logical_rows <= _HEALTH_EXACT_SCAN_ROW_LIMIT
                        and len(physical_files) <= _HEALTH_EXACT_SCAN_FILE_LIMIT
                    )
                    if should_exact_scan:
                        try:
                            df = await data_storage.load_klines_from_parquet(
                                exchange=exchange_name,
                                symbol=normalized_symbol,
                                timeframe=timeframe,
                            )
                            logical_rows = int(len(df))
                            if not df.empty:
                                start_at = _safe_iso_timestamp(df.index.min())
                                end_at = _safe_iso_timestamp(df.index.max())
                                missing = _detect_missing_bars(df, timeframe, max_preview=_HEALTH_GAP_PREVIEW_LIMIT)
                                gap_count = int(missing.get("missing_count") or 0)
                                gap_preview = list(missing.get("missing_preview") or [])[:_HEALTH_GAP_PREVIEW_LIMIT]
                            else:
                                gap_count = 0
                            scan_mode = "exact"
                            exact_scan_count += 1
                        except Exception as e:
                            scan["read_errors"] = [*(scan.get("read_errors") or []), f"exact_scan: {e}"][:5]

                    if scan_mode != "exact":
                        fast_scan_count += 1
                        expected_bars = _estimate_expected_bars(start_at, end_at, timeframe)
                        if expected_bars is not None:
                            coverage_ratio = round(
                                min(1.0, max(0.0, (float(logical_rows) / float(expected_bars)) if expected_bars > 0 else 0.0)),
                                6,
                            )
                            estimated_gap_count = max(0, int(expected_bars) - int(logical_rows))
                            if (
                                timeframe in _HEALTH_FAST_SCAN_RELAXED_TIMEFRAMES
                                and expected_bars >= _HEALTH_FAST_SCAN_MIN_EXPECTED_BARS
                                and coverage_ratio < _HEALTH_FAST_SCAN_DENSITY_THRESHOLD
                            ):
                                gap_count = 0
                                gap_issue_suppressed = True
                                suppressed_gap_datasets += 1
                                scan_note = (
                                    f"快扫样本稀疏，当前覆盖率约 {coverage_ratio:.1%}，缺口告警已抑制；"
                                    "如需精查建议按更短时间窗口下载。"
                                )
                            else:
                                gap_count = estimated_gap_count

                    source_type = (
                        "mixed"
                        if bucket["single_files"] and bucket["partition_files"]
                        else ("partitioned" if bucket["partition_files"] else "single")
                    )
                    issues: List[str] = []
                    if len(symbol_dirs) > 1:
                        issues.append("重复目录")
                    if int(bucket["corrupt_files"] or 0) > 0:
                        issues.append("存在损坏文件")
                    if int(gap_count or 0) > 0 and not gap_issue_suppressed:
                        issues.append("存在缺口")
                    if scan.get("read_errors"):
                        issues.append("元数据读取异常")
                    if issues:
                        datasets_with_issues += 1

                    row = {
                        "exchange": exchange_name,
                        "symbol": normalized_symbol,
                        "timeframe": timeframe,
                        "source_type": source_type,
                        "rows": logical_rows,
                        "physical_rows": int(scan["rows"] or 0),
                        "start": start_at,
                        "end": end_at,
                        "gap_count": int(gap_count or 0),
                        "gap_preview": gap_preview,
                        "coverage_ratio": coverage_ratio,
                        "scan_note": scan_note,
                        "gap_issue_suppressed": gap_issue_suppressed,
                        "corrupt_files": int(bucket["corrupt_files"] or 0),
                        "active_files": len(physical_files),
                        "partition_files": len(bucket["partition_files"]),
                        "duplicate_dirs": len(symbol_dirs) - 1,
                        "directories": [p.name for p in symbol_dirs],
                        "size_mb": round((int(scan["size_bytes"] or 0) / (1024 * 1024)), 2),
                        "modified_at": scan.get("modified_at"),
                        "scan_mode": scan_mode,
                        "issues": issues,
                        "read_errors": scan.get("read_errors") or [],
                        "recommended_action": (
                            "redownload"
                            if int(bucket["corrupt_files"] or 0) > 0
                            else ("repair" if int(gap_count or 0) > 0 and not gap_issue_suppressed else None)
                        ),
                    }
                    datasets.append(row)
                    exchange_dataset_rows.append(row)

            exchange_rows.append(
                {
                    "exchange": exchange_name,
                    "dataset_count": len(exchange_dataset_rows),
                    "symbol_count": len({row["symbol"] for row in exchange_dataset_rows}),
                    "timeframe_count": len({row["timeframe"] for row in exchange_dataset_rows}),
                    "files": exchange_active_files,
                    "partition_files": exchange_partition_files,
                    "corrupt_files": exchange_corrupt_files,
                    "size_mb": round(exchange_size_bytes / (1024 * 1024), 2),
                    "issue_count": sum(1 for row in exchange_dataset_rows if row["issues"]),
                    "latest_modified_at": max(
                        [row["modified_at"] for row in exchange_dataset_rows if row.get("modified_at")],
                        default=None,
                    ),
                }
            )

    backup_summary = _summarize_backup_batches(backup_root)
    datasets.sort(
        key=lambda row: (
            -len(row.get("issues") or []),
            -int(row.get("gap_count") or 0),
            -int(row.get("corrupt_files") or 0),
            str(row.get("exchange") or ""),
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
        )
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "exchange_count": len(exchange_rows),
            "dataset_count": len(datasets),
            "symbol_count": len(unique_symbols),
            "timeframe_count": len(unique_timeframes),
            "active_files": total_active_files,
            "partition_files": total_partition_files,
            "corrupt_files": total_corrupt_files,
            "duplicate_symbol_buckets": len(duplicate_symbol_dirs),
            "datasets_with_issues": datasets_with_issues,
            "backup_batches": int(backup_summary["count"]),
            "backup_symbol_dirs": int(backup_summary["symbol_dirs"]),
            "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
            "exact_scan_count": exact_scan_count,
            "fast_scan_count": fast_scan_count,
            "suppressed_gap_datasets": suppressed_gap_datasets,
            "scan_profile": "exact" if exact else "fast",
        },
        "exchanges": exchange_rows,
        "duplicates": duplicate_symbol_dirs,
        "backups": backup_summary,
        "datasets": datasets,
    }


@router.get("/available")
async def get_available_data():
    storage_path = Path(settings.DATA_STORAGE_PATH)
    available = []
    seen = set()

    if storage_path.exists():
        for exchange_dir in storage_path.iterdir():
            if not exchange_dir.is_dir():
                continue
            for symbol_dir in exchange_dir.iterdir():
                if not symbol_dir.is_dir():
                    continue
                for file in symbol_dir.glob("*.parquet"):
                    if ".corrupt_" in file.name:
                        continue
                    normalized_symbol = symbol_from_storage_dirname(symbol_dir.name)
                    if not normalized_symbol:
                        continue
                    key = (exchange_dir.name, normalized_symbol, file.stem)
                    if key in seen:
                        continue
                    seen.add(key)
                    available.append(
                        {
                            "exchange": exchange_dir.name,
                            "symbol": normalized_symbol,
                            "timeframe": file.stem,
                        }
                    )

    return {"available": available, "count": len(available)}


@router.get("/symbols")
async def get_data_symbols(exchange: str = "binance"):
    def _normalize_symbol_choice(raw: str) -> Optional[str]:
        text = normalize_symbol(raw)
        if not text:
            return None
        if "/" not in text:
            return None
        base, quote = [part.strip() for part in text.split("/", 1)]
        if not base or not quote:
            return None
        if quote not in {"USDT", "USD", "USDC"}:
            return None
        return f"{base}/{quote}"

    def _build_symbol_payload(exchange_name: str, preferred: List[str]) -> Dict[str, Any]:
        configured = exchange_manager.get_supported_symbols(exchange_name) or []
        local_symbols = sorted(
            {
                str(item.get("symbol") or "").strip()
                for item in available_rows
                if str(item.get("exchange") or "").strip().lower() == exchange_name and str(item.get("symbol") or "").strip()
            }
        )
        merged_raw = {
            normalized
            for x in [*preferred, *configured, *local_symbols]
            for normalized in [_normalize_symbol_choice(str(x).strip())]
            if normalized
        }
        preferred_rank = {sym: idx for idx, sym in enumerate(preferred)}
        merged = sorted(
            merged_raw,
            key=lambda sym: (preferred_rank.get(sym, 9999), sym),
        )
        return {
            "exchange": exchange_name,
            "symbols": merged,
            "configured_count": len(configured),
            "local_count": len(local_symbols),
            "count": len(merged),
        }

    exchange_name = str(exchange or "binance").strip().lower() or "binance"
    available_rows = (await get_available_data()).get("available") or []
    preferred = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT",
        "ADA/USDT", "TRX/USDT", "TON/USDT", "LINK/USDT", "AVAX/USDT", "DOT/USDT", "POL/USDT",
        "LTC/USDT", "BCH/USDT", "ETC/USDT", "ATOM/USDT", "NEAR/USDT", "APT/USDT",
        "ARB/USDT", "OP/USDT", "SUI/USDT", "INJ/USDT", "RUNE/USDT", "AAVE/USDT",
        "MKR/USDT", "UNI/USDT", "FIL/USDT", "HBAR/USDT", "ICP/USDT",
    ]
    return _build_symbol_payload(exchange_name, preferred)


_RESEARCH_MAJOR_SYMBOL_FALLBACK = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "TRX/USDT",
    "TON/USDT",
    "LINK/USDT",
]


def _merge_research_symbol_lists(*groups: Any, limit: int = 60) -> List[str]:
    merged: List[str] = []
    seen: set[str] = set()
    max_items = max(1, int(limit or 60))
    for group in groups:
        for raw in list(group or []):
            symbol = str(raw or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            merged.append(symbol)
            if len(merged) >= max_items:
                return merged
    return merged


def _filter_altcoin_research_symbols(
    symbols: List[str],
    *,
    excluded_major_symbols: Optional[List[str]] = None,
    limit: int = 60,
) -> List[str]:
    excluded = {
        str(item or "").strip().upper()
        for item in list(excluded_major_symbols or [])
        if str(item or "").strip()
    }
    out: List[str] = []
    seen: set[str] = set()
    for raw in list(symbols or []):
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen or symbol in excluded:
            continue
        if not is_alt_candidate_symbol(symbol):
            continue
        seen.add(symbol)
        out.append(symbol)
        if len(out) >= max(1, int(limit or 60)):
            break
    return out


def _detach_research_symbols_task(task: asyncio.Task[Any], *, label: str) -> None:
    def _consume_result(done_task: asyncio.Task[Any]) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            exc = done_task.exception()
            if exc is not None:
                logger.debug(f"{label} background task finished after fallback: {exc}")

    task.add_done_callback(_consume_result)


async def _build_research_symbols_fallback(
    *,
    exchange: str,
    warning: str,
    source: str = "research_universe_fallback",
) -> Dict[str, Any]:
    cached_universe = load_cached_exchange_altcoin_universe(exchange, allow_stale=True)
    if cached_universe and list(cached_universe.get("symbols") or []):
        data = dict(cached_universe)
        data["fallback_source"] = "coinglass_altcoin_universe_cache"
        data["stale_fallback"] = True
    else:
        data = await get_data_symbols(exchange=exchange)
        data["fallback_source"] = "data_symbols"

    symbols = list(data.get("symbols") or [])
    data["source"] = source
    data["warning"] = warning
    data["primary_symbol"] = str((symbols or ["BTC/USDT"])[0] or "BTC/USDT")
    data["default_count"] = min(30, len(symbols))
    data["count"] = len(symbols)
    return data


@router.get("/research/symbols")
async def get_research_symbols(exchange: str = "binance", include_major: bool = True):
    universe_task: asyncio.Task[Dict[str, Any]] = asyncio.create_task(
        build_exchange_altcoin_universe(exchange=exchange)
    )
    try:
        done, _ = await asyncio.wait(
            {universe_task},
            timeout=_RESEARCH_SYMBOLS_TIMEOUT_SEC,
        )
        if universe_task not in done:
            logger.warning(
                "research_symbols: coinglass altcoin universe timed out after %.1fs",
                _RESEARCH_SYMBOLS_TIMEOUT_SEC,
            )
            universe_task.cancel()
            _detach_research_symbols_task(
                universe_task,
                label=f"research_symbols:{exchange}",
            )
            data = await _build_research_symbols_fallback(
                exchange=exchange,
                warning=(
                    f"coinglass altcoin universe timed out after "
                    f"{_RESEARCH_SYMBOLS_TIMEOUT_SEC:.1f}s"
                ),
            )
        else:
            data = await universe_task
    except asyncio.TimeoutError:
        logger.warning(
            "research_symbols: coinglass altcoin universe timed out after %.1fs",
            _RESEARCH_SYMBOLS_TIMEOUT_SEC,
        )
        if not universe_task.done():
            universe_task.cancel()
            _detach_research_symbols_task(
                universe_task,
                label=f"research_symbols:{exchange}",
            )
        data = await _build_research_symbols_fallback(
            exchange=exchange,
            warning=(
                f"coinglass altcoin universe timed out after "
                f"{_RESEARCH_SYMBOLS_TIMEOUT_SEC:.1f}s"
            ),
        )
    except Exception as exc:
        logger.warning(
            f"research_symbols: coinglass altcoin universe failed: {exc}"
        )
        if not universe_task.done():
            universe_task.cancel()
            _detach_research_symbols_task(
                universe_task,
                label=f"research_symbols:{exchange}",
            )
        data = await _build_research_symbols_fallback(
            exchange=exchange,
            warning=str(exc),
        )

    if not data.get("symbols"):
        fallback = await get_data_symbols(exchange=exchange)
        fallback["source"] = "research_universe_fallback_empty"
        if not bool(include_major):
            fallback["symbols"] = _filter_altcoin_research_symbols(
                list(fallback.get("symbols") or []),
                excluded_major_symbols=list(fallback.get("excluded_major_symbols") or []),
            )
            fallback["count"] = len(fallback.get("symbols") or [])
        fallback["primary_symbol"] = str((fallback.get("symbols") or ["BTC/USDT"])[0] or "BTC/USDT")
        fallback["default_count"] = min(30, len(fallback.get("symbols") or []))
        fallback["include_major"] = bool(include_major)
        fallback["symbol_scope"] = "research_with_benchmarks" if bool(include_major) else "altcoin_only"
        return fallback

    major_symbols = list(data.get("major_market_cap_symbols") or [])
    if not major_symbols:
        excluded = {
            str(item or "").strip().upper()
            for item in list(data.get("excluded_major_symbols") or [])
            if str(item or "").strip()
        }
        if excluded:
            major_symbols = [symbol for symbol in _RESEARCH_MAJOR_SYMBOL_FALLBACK if symbol in excluded]
        elif not bool(include_major):
            major_symbols = [
                symbol
                for symbol in _RESEARCH_MAJOR_SYMBOL_FALLBACK
                if not is_alt_candidate_symbol(symbol)
            ]
        else:
            major_symbols = list(_RESEARCH_MAJOR_SYMBOL_FALLBACK)
    if bool(include_major):
        merged_symbols = _merge_research_symbol_lists(major_symbols, data.get("symbols") or [])
    else:
        merged_symbols = _filter_altcoin_research_symbols(
            list(data.get("symbols") or []),
            excluded_major_symbols=list(data.get("excluded_major_symbols") or []) + major_symbols,
        )
    data["major_market_cap_symbols"] = _merge_research_symbol_lists(major_symbols, limit=10)
    data["symbols"] = merged_symbols
    data["count"] = len(merged_symbols)
    data["primary_symbol"] = str((merged_symbols or ["BTC/USDT"])[0] or "BTC/USDT")
    data["default_count"] = min(30, len(merged_symbols))
    data["include_major"] = bool(include_major)
    data["symbol_scope"] = "research_with_benchmarks" if bool(include_major) else "altcoin_only"
    return data


@router.get("/research/refresh/status")
async def get_research_refresh_status():
    return await asyncio.to_thread(_get_research_universe_refresh_status_sync)


@router.post("/research/refresh/start", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def start_research_refresh(
    exchange: str = "binance",
    timeframes: str = "1m,5m,15m,1h",
    days: int = 90,
    overlap_bars: int = 48,
):
    try:
        return await _trigger_research_universe_refresh_start(
            exchange=exchange,
            timeframes=timeframes,
            days=days,
            overlap_bars=overlap_bars,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def _pair_scan_default_lookback(timeframe: str) -> int:
    tf = str(timeframe or "1h").strip().lower() or "1h"
    return int(_PAIR_SCAN_DEFAULT_LOOKBACK.get(tf, 720))


def _pair_scan_min_overlap(lookback: int) -> int:
    return max(80, min(int(max(lookback, 1) // 2), 240))


def _pair_scan_score_range(value: float, low: float, high: float) -> float:
    if not math.isfinite(float(value)):
        return 0.0
    if high <= low:
        return 0.0
    clipped = max(low, min(float(value), high))
    return max(0.0, min((clipped - low) / (high - low), 1.0))


def _pair_scan_half_life(spread: pd.Series) -> Optional[float]:
    if spread is None or len(spread) < 20:
        return None
    series = pd.to_numeric(spread, errors="coerce").dropna()
    if len(series) < 20:
        return None
    lagged = series.shift(1).dropna()
    delta = series.diff().dropna()
    aligned = pd.concat([lagged.rename("lagged"), delta.rename("delta")], axis=1).dropna()
    if len(aligned) < 20:
        return None
    x = (aligned["lagged"] - aligned["lagged"].mean()).values.reshape(-1, 1)
    y = aligned["delta"].values
    if len(x) < 20:
        return None
    try:
        coef, *_ = np.linalg.lstsq(x, y, rcond=None)
        beta = float(coef[0]) if len(coef) else 0.0
    except Exception:
        return None
    if not math.isfinite(beta) or beta >= 0:
        return None
    half_life = math.log(2.0) / abs(beta)
    if not math.isfinite(half_life) or half_life <= 0:
        return None
    return float(half_life)


def _pair_scan_signal_bias(current_z: float, entry_z: float = 2.0, exit_z: float = 0.6) -> str:
    if not math.isfinite(current_z):
        return "unknown"
    if current_z <= -abs(entry_z):
        return "long_spread_bias"
    if current_z >= abs(entry_z):
        return "short_spread_bias"
    if abs(current_z) <= abs(exit_z):
        return "balanced"
    return "watch"


def _pair_scan_relationship(level_corr: float, return_corr: float) -> str:
    corr = return_corr if math.isfinite(return_corr) else level_corr
    if not math.isfinite(corr):
        return "unknown"
    return "negative_corr" if corr < 0 else "positive_corr"


def _pair_scan_entry_snapshot(signal_bias: str) -> Dict[str, Any]:
    key = str(signal_bias or "").strip()
    if key == "long_spread_bias":
        return {
            "entry_state": "long_spread",
            "entry_ready": True,
            "blocked_reasons": [],
        }
    if key == "short_spread_bias":
        return {
            "entry_state": "short_spread",
            "entry_ready": True,
            "blocked_reasons": [],
        }
    if key == "balanced":
        return {
            "entry_state": "no_trade",
            "entry_ready": False,
            "blocked_reasons": ["当前 z-score 已回归到平衡区，缺少新的价差边际"],
        }
    if key == "watch":
        return {
            "entry_state": "watch",
            "entry_ready": False,
            "blocked_reasons": ["当前 z-score 尚未进入开仓区间"],
        }
    return {
        "entry_state": "watch",
        "entry_ready": False,
        "blocked_reasons": ["当前缺少有效信号，暂不建议执行"],
    }


def _pair_scan_pair_metrics(
    symbol1: str,
    symbol2: str,
    close1: pd.Series,
    close2: pd.Series,
    lookback: int,
) -> Optional[Dict[str, Any]]:
    min_overlap = _pair_scan_min_overlap(lookback)
    merged = pd.concat(
        [
            pd.to_numeric(close1, errors="coerce").rename("close1"),
            pd.to_numeric(close2, errors="coerce").rename("close2"),
        ],
        axis=1,
    ).dropna()
    if len(merged) < min_overlap:
        return None

    merged = merged[~merged.index.duplicated(keep="last")].sort_index().tail(max(lookback, min_overlap))
    if len(merged) < min_overlap:
        return None

    price1 = merged["close1"].astype(float)
    price2 = merged["close2"].astype(float)
    if (price1 <= 0).any() or (price2 <= 0).any():
        return None

    log1 = np.log(price1)
    log2 = np.log(price2)
    level_corr = float(log1.corr(log2)) if len(log1) >= 3 else float("nan")

    returns = pd.concat(
        [log1.diff().rename("ret1"), log2.diff().rename("ret2")],
        axis=1,
    ).dropna()
    if len(returns) < max(20, min_overlap // 4):
        return None
    return_corr = float(returns["ret1"].corr(returns["ret2"])) if len(returns) >= 3 else float("nan")
    if not math.isfinite(level_corr) or not math.isfinite(return_corr):
        return None
    if abs(level_corr) < 0.55 or abs(return_corr) < 0.15:
        return None
    if level_corr * return_corr < 0:
        return None

    try:
        centered_x = (price2 - float(price2.mean())).values.reshape(-1, 1)
        centered_y = (price1 - float(price1.mean())).values
        coef, *_ = np.linalg.lstsq(centered_x, centered_y, rcond=None)
        hedge_ratio = float(coef[0]) if len(coef) else 1.0
    except Exception:
        hedge_ratio = 1.0
    if not math.isfinite(hedge_ratio) or abs(hedge_ratio) < 0.05:
        return None
    hedge_ratio = max(-5.0, min(hedge_ratio, 5.0))

    spread = price1 - hedge_ratio * price2
    spread = spread.dropna()
    if len(spread) < min_overlap:
        return None
    spread_std = float(spread.std(ddof=0))
    if not math.isfinite(spread_std) or spread_std <= 0:
        return None

    z_window = max(48, min(max(60, int(lookback // 6)), 180, len(spread) - 1))
    if z_window < 30:
        return None
    spread_mean = spread.rolling(z_window, min_periods=z_window).mean()
    spread_sigma = spread.rolling(z_window, min_periods=z_window).std().replace(0, np.nan)
    z_score = ((spread - spread_mean) / spread_sigma).dropna()
    if len(z_score) < 5:
        return None

    current_z = float(z_score.iloc[-1])
    current_abs_z = abs(current_z)
    half_life = _pair_scan_half_life(spread.tail(max(z_window, min_overlap)))
    zero_crossings = int(((z_score.shift(1) * z_score) < 0).sum())
    crossing_rate = float(zero_crossings / max(len(z_score), 1))
    coverage_ratio = min(float(len(merged)) / float(max(lookback, 1)), 1.0)

    corr_score = 0.65 * _pair_scan_score_range(abs(level_corr), 0.55, 0.95) + 0.35 * _pair_scan_score_range(abs(return_corr), 0.15, 0.75)
    if half_life is None:
        half_life_score = 0.0
    else:
        ideal_cap = max(12.0, min(float(z_window) / 3.0, 96.0))
        if half_life <= 2.0:
            half_life_score = max(0.35, half_life / 2.0)
        elif half_life <= ideal_cap:
            half_life_score = 1.0
        else:
            half_life_score = max(0.0, 1.0 - ((half_life - ideal_cap) / max(ideal_cap * 2.0, 1.0)))
    crossing_score = _pair_scan_score_range(crossing_rate, 0.01, 0.08)
    opportunity_score = _pair_scan_score_range(current_abs_z, 0.35, 2.4)
    hedge_score = max(0.0, 1.0 - min(abs(math.log(max(abs(hedge_ratio), 1e-9))), math.log(5.0)) / math.log(5.0))
    relationship = _pair_scan_relationship(level_corr, return_corr)
    total_score = 100.0 * (
        0.38 * corr_score
        + 0.22 * half_life_score
        + 0.15 * crossing_score
        + 0.15 * opportunity_score
        + 0.10 * max(coverage_ratio, hedge_score * 0.7)
    )

    return {
        "primary_symbol": symbol1,
        "pair_symbol": symbol2,
        "score": round(float(total_score), 2),
        "level_corr": round(float(level_corr), 4),
        "return_corr": round(float(return_corr), 4),
        "hedge_ratio": round(float(hedge_ratio), 4),
        "correlation_regime": relationship,
        "current_z_score": round(float(current_z), 4),
        "current_abs_z": round(float(current_abs_z), 4),
        "half_life_bars": round(float(half_life), 2) if half_life is not None else None,
        "crossing_rate": round(float(crossing_rate), 4),
        "overlap_bars": int(len(merged)),
        "lookback_period": int(lookback),
        "signal_window_bars": int(z_window),
        "signal_bias": _pair_scan_signal_bias(current_z),
        **_pair_scan_entry_snapshot(_pair_scan_signal_bias(current_z)),
    }


def _normalize_symbol_list(raw_symbols: List[Any], max_count: int = _ARBITRAGE_MAX_UNIVERSE_SYMBOLS) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in raw_symbols or []:
        symbol = normalize_symbol(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
        if len(out) >= max(1, int(max_count)):
            break
    return out


def _build_arbitrage_readiness_params(
    strategy: str,
    exchange: str,
    timeframe: str,
    symbol: str,
    pair_symbol: str,
    universe_symbols: List[str],
    venues: List[str],
    lookback: int,
) -> Dict[str, Any]:
    strategy_name = str(strategy or "PairsTradingStrategy").strip() or "PairsTradingStrategy"
    exchange_name = str(exchange or "binance").strip().lower() or "binance"
    tf = str(timeframe or "1h").strip().lower() or "1h"
    primary_symbol = normalize_symbol(symbol) or "BTC/USDT"
    fallback_symbols = _normalize_symbol_list(get_strategy_recommended_symbols(strategy_name))
    normalized_universe = _normalize_symbol_list([primary_symbol, pair_symbol, *universe_symbols, *fallback_symbols])
    defaults = dict(get_strategy_defaults(strategy_name) or {})
    params = dict(defaults)
    params["exchange"] = exchange_name

    if strategy_name == "PairsTradingStrategy":
        secondary = normalize_symbol(pair_symbol)
        if not secondary or secondary == primary_symbol:
            secondary = next((item for item in normalized_universe if item != primary_symbol), "")
        if not secondary:
            secondary = normalize_symbol(defaults.get("pair_symbol")) or "ETH/USDT"
        params["pair_symbol"] = secondary
        params["lookback_period"] = max(20, min(2400, int(lookback or params.get("lookback_period") or 720)))
        normalized_universe = _normalize_symbol_list([primary_symbol, secondary, *normalized_universe])
    elif strategy_name == "FamaFactorArbitrageStrategy":
        fama_universe = normalized_universe[:_ARBITRAGE_MAX_UNIVERSE_SYMBOLS]
        if len(fama_universe) < 4:
            fama_universe = _normalize_symbol_list([primary_symbol, *fallback_symbols], max_count=_ARBITRAGE_MAX_UNIVERSE_SYMBOLS)
        params["factor_timeframe"] = tf
        params["lookback_bars"] = max(240, int(lookback or params.get("lookback_bars") or 720))
        params["universe_symbols"] = fama_universe
        params["max_symbols"] = max(4, min(100, len(fama_universe)))
        params["min_universe_size"] = max(2, min(int(params.get("min_universe_size", 12) or 12), len(fama_universe)))
        params["top_n"] = max(1, min(int(params.get("top_n", 8) or 8), max(1, len(fama_universe) // 2)))
        normalized_universe = fama_universe
    elif strategy_name == "CEXArbitrageStrategy":
        params["exchanges"] = [venue for venue in venues if venue in {"binance", "okx", "gate"}] or list(params.get("exchanges") or ["binance", "okx", "gate"])
    elif strategy_name == "TriangularArbitrageStrategy":
        params["exchange"] = exchange_name
        params["bridge_assets"] = list(params.get("bridge_assets") or ["ETH", "BNB", "SOL"])
    elif strategy_name in {"DEXArbitrageStrategy", "FlashLoanArbitrageStrategy"}:
        params["dex_list"] = [venue for venue in venues if venue in {"uniswap", "sushiswap"}] or list(params.get("dex_list") or ["uniswap", "sushiswap"])

    return {
        "strategy": strategy_name,
        "exchange": exchange_name,
        "timeframe": tf,
        "symbol": primary_symbol,
        "pair_symbol": str(params.get("pair_symbol") or "").strip(),
        "universe_symbols": normalized_universe,
        "params": params,
    }


def _arbitrage_leg_snapshot(symbol: str, frame: Optional[pd.DataFrame]) -> Dict[str, Any]:
    df = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    if not df.empty:
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep="last")].sort_index()
    latest_timestamp = _safe_iso_timestamp(df.index[-1]) if not df.empty else None
    return {
        "symbol": normalize_symbol(symbol) or str(symbol or "").strip(),
        "bars": int(len(df)),
        "latest_timestamp": latest_timestamp,
        "ready": bool(len(df) >= _ARBITRAGE_READY_MIN_BARS_PER_LEG),
    }


def _common_overlap_bars(frames: List[pd.DataFrame]) -> tuple[int, Optional[str]]:
    cleaned = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        item = frame.copy()
        item.index = pd.to_datetime(item.index)
        item = item[~item.index.duplicated(keep="last")].sort_index()
        cleaned.append(item)
    if not cleaned:
        return 0, None
    common_index = pd.Index(cleaned[0].index)
    for frame in cleaned[1:]:
        common_index = common_index.intersection(pd.Index(frame.index))
    common_index = pd.Index(common_index).sort_values()
    if not len(common_index):
        return 0, None
    return int(len(common_index)), _safe_iso_timestamp(common_index[-1])


def _build_arbitrage_data_status(
    strategy: str,
    resolved_symbol: str,
    timeframe: str,
    params: Dict[str, Any],
    df: pd.DataFrame,
    market_bundle: Optional[Dict[str, pd.DataFrame]],
) -> Dict[str, Any]:
    strategy_name = str(strategy or "").strip()
    bundle = dict(market_bundle or {})
    legs: List[Dict[str, Any]] = []
    overlap_bars = 0
    latest_overlap_ts = None
    reasons: List[str] = []

    if strategy_name == "PairsTradingStrategy":
        primary_symbol = normalize_symbol(resolved_symbol) or normalize_symbol(params.get("symbol")) or "BTC/USDT"
        pair_symbol = normalize_symbol(params.get("pair_symbol")) or ""
        primary_df = bundle.get(primary_symbol, df)
        pair_df = bundle.get(pair_symbol)
        legs = [
            _arbitrage_leg_snapshot(primary_symbol, primary_df),
            _arbitrage_leg_snapshot(pair_symbol, pair_df),
        ]
        overlap_bars, latest_overlap_ts = _common_overlap_bars([primary_df, pair_df] if pair_df is not None else [primary_df])
        if any(int(item.get("bars") or 0) < _ARBITRAGE_READY_MIN_BARS_PER_LEG for item in legs):
            reasons.append(
                f"双腿历史数据不足：每条腿至少需要 {_ARBITRAGE_READY_MIN_BARS_PER_LEG} 根 K 线"
            )
        if overlap_bars < _ARBITRAGE_READY_MIN_OVERLAP_BARS:
            reasons.append(
                f"公共样本不足：当前仅 {overlap_bars} 根，至少需要 {_ARBITRAGE_READY_MIN_OVERLAP_BARS} 根"
            )
        ready = not reasons
    elif strategy_name == "FamaFactorArbitrageStrategy":
        requested_symbols = _normalize_symbol_list(params.get("universe_symbols") or [], max_count=100)
        for item in requested_symbols:
            legs.append(_arbitrage_leg_snapshot(item, bundle.get(item)))
        loaded_frames = [bundle.get(item) for item in requested_symbols if bundle.get(item) is not None]
        overlap_bars, latest_overlap_ts = _common_overlap_bars([frame for frame in loaded_frames if frame is not None])
        eligible_legs = sum(1 for item in legs if int(item.get("bars") or 0) >= _ARBITRAGE_READY_MIN_BARS_PER_LEG)
        min_universe = max(2, int(params.get("min_universe_size", 12) or 12))
        if eligible_legs < min_universe:
            reasons.append(
                f"可用横截面样本不足：当前满足条件 {eligible_legs} 个，至少需要 {min_universe} 个标的"
            )
        if overlap_bars < _ARBITRAGE_READY_MIN_OVERLAP_BARS:
            reasons.append(
                f"横截面公共样本不足：当前仅 {overlap_bars} 根，至少需要 {_ARBITRAGE_READY_MIN_OVERLAP_BARS} 根"
            )
        ready = not reasons
    else:
        primary_df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        legs = [_arbitrage_leg_snapshot(resolved_symbol, primary_df)]
        overlap_bars = int(len(primary_df))
        latest_overlap_ts = _safe_iso_timestamp(primary_df.index[-1]) if not primary_df.empty else None
        if overlap_bars < _ARBITRAGE_READY_MIN_BARS_PER_LEG:
            reasons.append(
                f"本地历史数据不足：至少需要 {_ARBITRAGE_READY_MIN_BARS_PER_LEG} 根 K 线，当前 {overlap_bars} 根"
            )
        ready = not reasons

    return {
        "ready": bool(ready),
        "mode": "cross_sectional" if strategy_name == "FamaFactorArbitrageStrategy" else "pair" if strategy_name == "PairsTradingStrategy" else "single_leg",
        "timeframe": str(timeframe or "1h").strip().lower() or "1h",
        "required_leg_bars": _ARBITRAGE_READY_MIN_BARS_PER_LEG,
        "required_overlap_bars": _ARBITRAGE_READY_MIN_OVERLAP_BARS,
        "legs": legs,
        "eligible_leg_count": int(sum(1 for item in legs if bool(item.get("ready")))),
        "requested_leg_count": int(len(legs)),
        "overlap_bars": int(overlap_bars),
        "latest_timestamp": latest_overlap_ts,
        "reasons": reasons,
    }


def _build_arbitrage_backtest_status(strategy: str) -> Dict[str, Any]:
    info = dict(get_backtest_strategy_info(strategy) or {})
    supported = bool(info.get("supported", info.get("backtest_supported", False)))
    description = str(info.get("description") or "").strip()
    reason = str(info.get("reason") or info.get("backtest_reason") or "").strip()

    if not supported:
        return {
            "supported": False,
            "mode": "realtime_only",
            "headline": "仅实时验证",
            "reason": reason or "依赖实时盘口 / 跨场所 / 链上执行，单一 K 线回测会失真",
        }
    if str(strategy or "").strip() == "PairsTradingStrategy" or "近似" in description:
        return {
            "supported": True,
            "mode": "spread_approx",
            "headline": "近似价差回测",
            "reason": description or "当前回测基于价差合成序列，并非真实交易所执行仿真",
        }
    return {
        "supported": True,
        "mode": "factor_backtest" if str(strategy or "").strip() == "FamaFactorArbitrageStrategy" else "backtest",
        "headline": "多因子回测" if str(strategy or "").strip() == "FamaFactorArbitrageStrategy" else "支持回测",
        "reason": description or "可直接进入单策略自定义回测验证",
    }


def _build_arbitrage_cost_status(result: Optional[Dict[str, Any]], *, supported: bool) -> Dict[str, Any]:
    if not supported:
        return {
            "status": "unknown",
            "headline": "仅实时验证",
            "diagnostic": "live_only",
            "gross_total_return": None,
            "net_total_return": None,
            "cost_drag_return_pct": None,
            "estimated_trade_cost_usd": None,
            "quality_flag": None,
            "anomaly_bar_ratio": None,
            "reasons": ["该策略不支持单策略回测，成本压缩需要实时验证"],
        }
    if not result:
        return {
            "status": "unknown",
            "headline": "先回测再评估成本",
            "diagnostic": "missing_backtest",
            "gross_total_return": None,
            "net_total_return": None,
            "cost_drag_return_pct": None,
            "estimated_trade_cost_usd": None,
            "quality_flag": None,
            "anomaly_bar_ratio": None,
            "reasons": ["当前缺少可用回测结果，无法判断成本是否吞噬边际"],
        }

    gross = float(result.get("gross_total_return") or 0.0)
    net = float(result.get("total_return") or 0.0)
    drag = float(result.get("cost_drag_return_pct") or max(gross - net, 0.0))
    estimated_cost_usd = float(result.get("estimated_trade_cost_usd") or 0.0)
    quality_flag = str(result.get("quality_flag") or "").strip().lower() or "ok"
    anomaly_ratio = float(result.get("anomaly_bar_ratio") or 0.0)
    reasons: List[str] = []
    diagnostic = "cost_ok"
    status = "pass"
    headline = "成本可接受"

    if quality_flag == "invalid" or anomaly_ratio > 0.02:
        status = "fail"
        diagnostic = "quality_invalid"
        headline = "结果不可信"
        reasons.append("回测异常值占比过高或结果已失真，当前结论不宜直接用于开仓")
    elif gross <= 0:
        status = "fail"
        diagnostic = "gross_edge_negative"
        headline = "结构边际不足"
        reasons.append("毛收益本身不成立，问题不只是交易成本")
    elif gross > 0 and net <= 0:
        status = "fail"
        diagnostic = "cost_consumes_edge"
        headline = "成本吞噬边际"
        reasons.append("毛收益为正但净收益转负，手续费/滑点已吃掉策略边际")
    elif drag > _ARBITRAGE_COST_WARN_MAX:
        status = "fail"
        diagnostic = "cost_too_high"
        headline = "成本过高"
        reasons.append("成本拖累已经超过可接受阈值，继续执行的胜率和盈亏比都偏弱")
    elif drag >= _ARBITRAGE_COST_PASS_MAX:
        status = "warn"
        diagnostic = "cost_drag_watch"
        headline = "成本压缩明显"
        reasons.append("成本拖累较大，建议收紧执行阈值或继续等待更极端的价差")

    if quality_flag.startswith("warning_") or quality_flag.startswith("watch_") or anomaly_ratio > 0.005:
        reasons.append("样本存在一定异常波动，回测结果需要保守解读")

    return {
        "status": status,
        "headline": headline,
        "diagnostic": diagnostic,
        "gross_total_return": round(gross, 4),
        "net_total_return": round(net, 4),
        "cost_drag_return_pct": round(drag, 4),
        "estimated_trade_cost_usd": round(estimated_cost_usd, 2),
        "quality_flag": quality_flag,
        "anomaly_bar_ratio": round(anomaly_ratio, 6),
        "reasons": reasons,
    }


def _build_arbitrage_entry_status(
    strategy: str,
    timeframe: str,
    params: Dict[str, Any],
    result: Optional[Dict[str, Any]],
    market_bundle: Optional[Dict[str, pd.DataFrame]],
) -> Dict[str, Any]:
    strategy_name = str(strategy or "").strip()
    if strategy_name == "PairsTradingStrategy":
        entry_z = abs(float(params.get("entry_z_score", 2.0) or 2.0))
        exit_z = abs(float(params.get("exit_z_score", 0.6) or 0.6))
        latest_z = float(result.get("z_score_last") or float("nan")) if result else float("nan")
        signal_bias = str(result.get("signal_bias") or "").strip() if result else ""
        if signal_bias == "long_spread_bias" and math.isfinite(latest_z) and abs(latest_z) >= entry_z:
            return {
                "state": "long_spread",
                "entry_ready": True,
                "headline": "可开多价差",
                "latest_z_score": round(latest_z, 4),
                "reasons": [f"当前 |z|={abs(latest_z):.2f}，已达到 entry_z={entry_z:.2f}"],
            }
        if signal_bias == "short_spread_bias" and math.isfinite(latest_z) and abs(latest_z) >= entry_z:
            return {
                "state": "short_spread",
                "entry_ready": True,
                "headline": "可开空价差",
                "latest_z_score": round(latest_z, 4),
                "reasons": [f"当前 |z|={abs(latest_z):.2f}，已达到 entry_z={entry_z:.2f}"],
            }
        if math.isfinite(latest_z) and abs(latest_z) <= exit_z:
            return {
                "state": "no_trade",
                "entry_ready": False,
                "headline": "接近回归完成，暂不追单",
                "latest_z_score": round(latest_z, 4),
                "reasons": [f"当前 |z|={abs(latest_z):.2f}，已接近 exit_z={exit_z:.2f} 的平衡区"],
            }
        if math.isfinite(latest_z):
            return {
                "state": "watch",
                "entry_ready": False,
                "headline": "研究有效，但仍需等待入场区",
                "latest_z_score": round(latest_z, 4),
                "reasons": [f"当前 |z|={abs(latest_z):.2f}，尚未达到 entry_z={entry_z:.2f}"],
            }
        return {
            "state": "no_trade",
            "entry_ready": False,
            "headline": "当前缺少有效价差信号",
            "latest_z_score": None,
            "reasons": ["缺少有效 z-score，暂不建议执行"],
        }

    if strategy_name == "FamaFactorArbitrageStrategy" and market_bundle:
        try:
            components = _build_fama_backtest_components(market_bundle=market_bundle, timeframe=timeframe, params=params)
            weights = components.get("weights")
            if isinstance(weights, pd.DataFrame) and not weights.empty:
                latest_weights = weights.iloc[-1].fillna(0.0)
                long_count = int((latest_weights > 1e-9).sum())
                short_count = int((latest_weights < -1e-9).sum())
                if long_count > 0 and short_count > 0:
                    return {
                        "state": "watch",
                        "entry_ready": True,
                        "headline": f"横截面调仓就绪：多 {long_count} / 空 {short_count}",
                        "latest_z_score": None,
                        "reasons": [
                            f"当前横截面已形成可执行多空篮子，quantile={float(components.get('quantile') or 0.0):.2f}"
                        ],
                    }
        except Exception:
            pass
        return {
            "state": "no_trade",
            "entry_ready": False,
            "headline": "当前横截面未形成可执行多空篮子",
            "latest_z_score": None,
            "reasons": ["当前满足门槛的多空候选不足，继续观察更稳妥"],
        }

    if str(strategy or "").strip() in {"CEXArbitrageStrategy", "TriangularArbitrageStrategy", "DEXArbitrageStrategy", "FlashLoanArbitrageStrategy"}:
        return {
            "state": "watch",
            "entry_ready": False,
            "headline": "仅实时验证",
            "latest_z_score": None,
            "reasons": ["该策略依赖实时盘口 / 链上报价，页面仅提供风险预检，不直接给开仓指令"],
        }

    return {
        "state": "watch",
        "entry_ready": False,
        "headline": "等待进一步验证",
        "latest_z_score": None,
        "reasons": ["当前策略尚未提供统一的执行闸门解释"],
    }


def _build_arbitrage_gate_snapshot(
    data_status: Dict[str, Any],
    backtest_status: Dict[str, Any],
    cost_status: Dict[str, Any],
    entry_status: Dict[str, Any],
) -> Dict[str, Any]:
    blocked_reasons: List[str] = []
    blocked_reasons.extend(list(data_status.get("reasons") or []))
    blocked_reasons.extend(list(cost_status.get("reasons") or []))
    blocked_reasons.extend(list(entry_status.get("reasons") or []))
    deduped: List[str] = []
    seen = set()
    for item in blocked_reasons:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return {
        "research_candidate": bool(data_status.get("eligible_leg_count") or data_status.get("ready")),
        "backtest_required": bool(backtest_status.get("supported")) and str(cost_status.get("status") or "unknown") == "unknown",
        "live_only": not bool(backtest_status.get("supported")),
        "cost_blocked": str(cost_status.get("status") or "").strip() == "fail",
        "entry_ready": bool(entry_status.get("entry_ready")),
        "blocked_reasons": deduped[:8],
    }


def _recommend_arbitrage_action(
    data_status: Dict[str, Any],
    backtest_status: Dict[str, Any],
    cost_status: Dict[str, Any],
    entry_status: Dict[str, Any],
) -> str:
    if not bool(data_status.get("ready")):
        return "先补数据"
    if not bool(backtest_status.get("supported")):
        return "加入观察"
    if str(cost_status.get("status") or "unknown") == "unknown":
        return "先回测"
    if str(cost_status.get("status") or "") == "fail":
        return "先回测"
    if not bool(entry_status.get("entry_ready")):
        return "加入观察"
    return "允许开仓"


async def _build_arbitrage_derivatives_overlay(
    *,
    strategy: str,
    symbol: str,
    pair_symbol: str = "",
    universe_symbols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    strategy_name = str(strategy or "").strip()
    supported_strategies = {"PairsTradingStrategy", "FamaFactorArbitrageStrategy", "CEXArbitrageStrategy"}
    if strategy_name not in supported_strategies:
        return {
            "available": False,
            "source": "coinglass",
            "strategy": strategy_name,
            "sample_size": 0,
            "monitored_symbols": [],
            "cards": [],
            "alerts": [],
            "alert_count": 0,
            "reason": "strategy_not_supported",
        }
    if not bool(getattr(settings, "COINGLASS_ENABLED", False)):
        return {
            "available": False,
            "source": "coinglass",
            "strategy": strategy_name,
            "sample_size": 0,
            "monitored_symbols": [],
            "cards": [],
            "alerts": [],
            "alert_count": 0,
            "reason": "coinglass_disabled",
        }

    raw_symbols: List[str] = [symbol, pair_symbol, *(universe_symbols or [])]
    monitored_symbols: List[str] = []
    seen = set()
    for item in raw_symbols:
        normalized = normalize_symbol(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        monitored_symbols.append(normalized)
        if strategy_name == "PairsTradingStrategy" and len(monitored_symbols) >= 2:
            break
        if strategy_name == "CEXArbitrageStrategy" and len(monitored_symbols) >= 1:
            break
        if strategy_name == "FamaFactorArbitrageStrategy" and len(monitored_symbols) >= 4:
            break

    cards: List[Dict[str, Any]] = []
    alerts: List[str] = []

    def _promote_severity(current: str, target: str) -> str:
        order = {"ok": 0, "warn": 1, "alert": 2}
        if order.get(target, 0) > order.get(current, 0):
            return target
        return current

    for monitored_symbol in monitored_symbols:
        try:
            overview = await build_coinglass_overview_payload(symbol=monitored_symbol, refresh=False, manual=False)
        except Exception as exc:
            cards.append(
                {
                    "symbol": monitored_symbol,
                    "available": False,
                    "tone": "unavailable",
                    "headline": "CoinGlass 衍生品上下文缺失",
                    "reasons": [f"读取 {monitored_symbol} 的 CoinGlass 快照失败：{_error_text(exc)}"],
                    "labels": [],
                    "history_ready": False,
                    "funding_zscore": None,
                    "basis_pct": None,
                    "liquidation_burst_score": None,
                    "long_short_ratio_change_24h": None,
                }
            )
            continue

        snapshot = dict(overview.get("snapshot") or {})
        payload = dict(snapshot.get("payload") or {})
        history_ready = bool(payload.get("history_ready"))
        funding_zscore = _safe_float(payload.get("funding_zscore"))
        basis_pct = _safe_float(snapshot.get("basis_pct"))
        liquidation_burst_score = _safe_float(payload.get("liquidation_burst_score"))
        long_short_ratio_change = _safe_float(payload.get("long_short_ratio_change_24h"))
        labels = [str(item).strip() for item in list(payload.get("derivatives_labels") or []) if str(item).strip()]

        reasons: List[str] = []
        severity = "ok"
        if not bool(overview.get("available")) or not snapshot:
            severity = "unavailable"
            reasons.append("尚无可用的 CoinGlass 快照，funding / basis 风险仅能依赖本地回测判断。")
        else:
            if not history_ready:
                severity = _promote_severity(severity, "warn")
                reasons.append("历史序列未就绪，Funding z-score 与回归速度信号置信度偏低。")
            if funding_zscore is not None and abs(funding_zscore) >= 1.5:
                severity = _promote_severity(severity, "alert" if abs(funding_zscore) >= 2.0 else "warn")
                reasons.append(f"Funding z-score {funding_zscore:+.2f}，资金费率相对近期开出明显偏离。")
            if basis_pct is not None and abs(basis_pct) >= 0.02:
                severity = _promote_severity(severity, "alert" if abs(basis_pct) >= 0.03 else "warn")
                reasons.append(f"Basis {basis_pct:+.2%}，跨腿或跨所执行需防止基差回归与滑点放大。")
            if bool(payload.get("basis_dislocation")):
                severity = _promote_severity(severity, "alert")
                reasons.append("CoinGlass 已标记 basis dislocation，当前更适合降杠杆或延后执行。")
            if bool(payload.get("crowded_long")) or bool(payload.get("crowded_short")):
                severity = _promote_severity(severity, "warn")
                reasons.append("持仓拥挤度抬升，套利腿可能被拥挤方向拖累。")
            if liquidation_burst_score is not None and liquidation_burst_score >= 0.65:
                severity = _promote_severity(severity, "warn")
                reasons.append(f"Liquidation burst {liquidation_burst_score:.2f}，短时冲击仍偏强。")
            if long_short_ratio_change is not None and abs(long_short_ratio_change) >= 0.08:
                reasons.append(f"24h 多空比变化 {long_short_ratio_change:+.2%}，说明仓位偏移仍在继续。")
            if strategy_name == "CEXArbitrageStrategy" and bool(payload.get("order_flow_confirmed")):
                severity = _promote_severity(severity, "alert")
                reasons.append("订单流仍在确认当前方向，跨所价差更容易被单边挤压。")

        headline = (
            "Funding / Basis 异常"
            if severity == "alert"
            else "Funding / Basis 风险抬升"
            if severity == "warn"
            else "CoinGlass 衍生品上下文缺失"
            if severity == "unavailable"
            else "Funding / Basis 正常"
        )
        cards.append(
            {
                "symbol": monitored_symbol,
                "available": bool(overview.get("available")) and bool(snapshot),
                "tone": severity,
                "headline": headline,
                "reasons": reasons[:4],
                "labels": labels[:6],
                "history_ready": history_ready,
                "funding_zscore": funding_zscore,
                "basis_pct": basis_pct,
                "liquidation_burst_score": liquidation_burst_score,
                "long_short_ratio_change_24h": long_short_ratio_change,
            }
        )
        if severity in {"warn", "alert"}:
            summary_bits: List[str] = [monitored_symbol, headline]
            if labels:
                summary_bits.append(",".join(labels[:2]))
            if funding_zscore is not None and abs(funding_zscore) >= 1.5:
                summary_bits.append(f"funding_z={funding_zscore:+.2f}")
            if basis_pct is not None and abs(basis_pct) >= 0.02:
                summary_bits.append(f"basis={basis_pct:+.2%}")
            alerts.append(" | ".join(summary_bits))

    return {
        "available": any(bool(item.get("available")) for item in cards),
        "source": "coinglass",
        "strategy": strategy_name,
        "sample_size": len(cards),
        "monitored_symbols": monitored_symbols,
        "cards": cards,
        "alerts": alerts[:6],
        "alert_count": len(alerts),
        "reason": None if cards else "no_symbols_to_monitor",
    }


async def _load_pair_scan_series(
    exchange: str,
    timeframe: str,
    symbols: List[str],
    lookback: int,
) -> Dict[str, pd.Series]:
    tf_seconds = max(1, _timeframe_seconds(timeframe))
    window_seconds = max(3600, min(int(lookback * tf_seconds * 2.5), 366 * 24 * 3600))
    min_required = _pair_scan_min_overlap(lookback)

    async def _load_one(symbol: str) -> tuple[str, Optional[pd.Series]]:
        latest_end = _latest_partition_end_time(exchange=exchange, symbol=symbol, timeframe=timeframe)
        query_start = (latest_end - timedelta(seconds=window_seconds)) if latest_end else None
        frame = await _load_symbol_df(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=query_start,
            end_time=latest_end,
        )
        if frame.empty and timeframe not in _SUB_MINUTE_TIMEFRAMES:
            frame = await _load_symbol_df(exchange=exchange, symbol=symbol, timeframe=timeframe, end_time=latest_end)
        if frame.empty:
            return symbol, None
        close = pd.to_numeric(frame.get("close"), errors="coerce").dropna()
        if close.empty:
            return symbol, None
        close = close[~close.index.duplicated(keep="last")].sort_index().tail(max(lookback * 2, min_required))
        if len(close) < min_required:
            return symbol, None
        return symbol, close

    results = await asyncio.gather(*[_load_one(symbol) for symbol in symbols], return_exceptions=True)
    loaded: Dict[str, pd.Series] = {}
    for item in results:
        if isinstance(item, Exception):
            continue
        symbol, series = item
        if series is None or series.empty:
            continue
        loaded[symbol] = series
    return loaded


@router.get("/research/pairs-ranking")
async def get_research_pairs_ranking(
    exchange: str = "binance",
    timeframe: str = "1h",
    limit: int = 10,
    symbols: str = "",
):
    exchange_name = str(exchange or "binance").strip().lower() or "binance"
    tf = str(timeframe or "1h").strip().lower() or "1h"
    if tf not in _PAIR_SCAN_ALLOWED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {timeframe}")

    row_limit = max(1, min(int(limit or 10), _PAIR_SCAN_MAX_ROWS))
    lookback = _pair_scan_default_lookback(tf)

    requested_symbols = [
        normalize_symbol(item)
        for item in str(symbols or "").split(",")
        if normalize_symbol(item)
    ]
    if not requested_symbols:
        research_payload = await get_research_symbols(exchange=exchange_name)
        requested_symbols = list(research_payload.get("symbols") or [])[:30]

    candidate_symbols = _expand_symbols_with_local(
        exchange=exchange_name,
        timeframe=tf,
        requested=requested_symbols,
        min_symbols=min(12, max(4, len(requested_symbols))),
        max_symbols=_PAIR_SCAN_MAX_SYMBOLS,
    )
    candidate_symbols = candidate_symbols[:_PAIR_SCAN_MAX_SYMBOLS]
    loaded_series = await _load_pair_scan_series(
        exchange=exchange_name,
        timeframe=tf,
        symbols=candidate_symbols,
        lookback=lookback,
    )

    ranked_pairs: List[Dict[str, Any]] = []
    symbol_list = list(loaded_series.keys())
    for idx, symbol1 in enumerate(symbol_list):
        for symbol2 in symbol_list[idx + 1 :]:
            metrics = _pair_scan_pair_metrics(
                symbol1=symbol1,
                symbol2=symbol2,
                close1=loaded_series[symbol1],
                close2=loaded_series[symbol2],
                lookback=lookback,
            )
            if metrics:
                ranked_pairs.append(metrics)

    ranked_pairs.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            float(row.get("current_abs_z") or 0.0),
            float(row.get("level_corr") or 0.0),
        ),
        reverse=True,
    )
    top_rows = ranked_pairs[:row_limit]

    top_symbols: List[str] = []
    seen_symbols = set()
    for row in top_rows:
        for key in ("primary_symbol", "pair_symbol"):
            symbol = str(row.get(key) or "").strip()
            if not symbol or symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            top_symbols.append(symbol)

    warnings: List[str] = []
    if len(loaded_series) < 4:
        warnings.append("可用本地K线币种不足 4 个，榜单可能不稳定")
    if not ranked_pairs:
        warnings.append("未找到满足相关性与价差稳定条件的币对，请先补足该周期本地K线")

    return {
        "exchange": exchange_name,
        "timeframe": tf,
        "lookback_period": int(lookback),
        "requested_symbol_count": int(len(requested_symbols)),
        "candidate_symbol_count": int(len(candidate_symbols)),
        "loaded_symbol_count": int(len(loaded_series)),
        "eligible_pair_count": int(len(ranked_pairs)),
        "top_symbols": top_symbols,
        "pairs": top_rows,
        "warnings": warnings,
        "generated_at": _utc_iso(),
    }


@router.get("/research/arbitrage-readiness")
async def get_arbitrage_readiness(
    strategy: str = "PairsTradingStrategy",
    exchange: str = "binance",
    timeframe: str = "1h",
    symbol: str = "BTC/USDT",
    pair_symbol: str = "",
    symbols: str = "",
    venues: str = "",
    lookback: int = 720,
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
):
    requested_symbols = _normalize_symbol_list(str(symbols or "").split(","), max_count=_ARBITRAGE_MAX_UNIVERSE_SYMBOLS)
    requested_venues = [
        str(item or "").strip().lower()
        for item in str(venues or "").split(",")
        if str(item or "").strip()
    ]
    spec = _build_arbitrage_readiness_params(
        strategy=strategy,
        exchange=exchange,
        timeframe=timeframe,
        symbol=symbol,
        pair_symbol=pair_symbol,
        universe_symbols=requested_symbols,
        venues=requested_venues,
        lookback=lookback,
    )

    strategy_name = str(spec.get("strategy") or "PairsTradingStrategy").strip() or "PairsTradingStrategy"
    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=strategy_name,
        symbol=str(spec.get("symbol") or symbol),
        timeframe=str(spec.get("timeframe") or timeframe),
        params=dict(spec.get("params") or {}),
    )
    data_status = _build_arbitrage_data_status(
        strategy=strategy_name,
        resolved_symbol=resolved_symbol,
        timeframe=str(spec.get("timeframe") or timeframe),
        params=dict(spec.get("params") or {}),
        df=df,
        market_bundle=market_bundle,
    )
    backtest_status = _build_arbitrage_backtest_status(strategy_name)

    result: Optional[Dict[str, Any]] = None
    backtest_error = ""
    if bool(backtest_status.get("supported")) and bool(data_status.get("ready")):
        params_for_run = dict(spec.get("params") or {})
        # Trim the dataset for the readiness backtest so it stays interactive
        # (~3-5 s). The full backtest endpoint still uses the entire history.
        lookback_hint = int(
            params_for_run.get("lookback_period")
            or params_for_run.get("lookback_bars")
            or lookback
            or 720
        )
        budget_bars = max(1500, min(3000, lookback_hint * 3))
        trimmed_df = df.iloc[-budget_bars:].copy() if len(df) > budget_bars else df
        trimmed_bundle = market_bundle
        if isinstance(market_bundle, dict) and market_bundle:
            trimmed_bundle = {
                key: (frame.iloc[-budget_bars:].copy() if len(frame) > budget_bars else frame)
                for key, frame in market_bundle.items()
            }
        try:
            result = _run_backtest_core(
                strategy=strategy_name,
                df=trimmed_df,
                timeframe=str(spec.get("timeframe") or timeframe),
                initial_capital=max(100.0, float(initial_capital or 10000.0)),
                params=params_for_run,
                include_series=False,
                commission_rate=max(0.0, float(commission_rate or 0.0)),
                slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                market_bundle=trimmed_bundle,
            )
        except Exception as exc:
            backtest_error = str(exc)

    cost_status = _build_arbitrage_cost_status(result, supported=bool(backtest_status.get("supported")))
    if backtest_error and bool(backtest_status.get("supported")):
        cost_status = {
            **cost_status,
            "status": "unknown",
            "headline": "先回测再评估成本",
            "diagnostic": "backtest_error",
            "reasons": list(cost_status.get("reasons") or []) + [f"轻量回测评估失败：{backtest_error}"],
        }

    entry_status = _build_arbitrage_entry_status(
        strategy=strategy_name,
        timeframe=str(spec.get("timeframe") or timeframe),
        params=dict(spec.get("params") or {}),
        result=result,
        market_bundle=market_bundle,
    )
    gates = _build_arbitrage_gate_snapshot(
        data_status=data_status,
        backtest_status=backtest_status,
        cost_status=cost_status,
        entry_status=entry_status,
    )
    recommended_action = _recommend_arbitrage_action(
        data_status=data_status,
        backtest_status=backtest_status,
        cost_status=cost_status,
        entry_status=entry_status,
    )
    derivatives_overlay = await _build_arbitrage_derivatives_overlay(
        strategy=strategy_name,
        symbol=resolved_symbol,
        pair_symbol=str(spec.get("pair_symbol") or ""),
        universe_symbols=list(spec.get("universe_symbols") or []),
    )
    gates["derivatives_alert"] = bool(derivatives_overlay.get("alert_count"))
    gates["derivatives_notes"] = list(derivatives_overlay.get("alerts") or [])[:4]

    return {
        "strategy": strategy_name,
        "exchange": str(spec.get("exchange") or exchange),
        "timeframe": str(spec.get("timeframe") or timeframe),
        "symbol": resolved_symbol,
        "pair_symbol": str(spec.get("pair_symbol") or ""),
        "universe_symbols": list(spec.get("universe_symbols") or []),
        "data_status": data_status,
        "backtest_status": backtest_status,
        "cost_status": cost_status,
        "entry_status": entry_status,
        "recommended_action": recommended_action,
        "gates": gates,
        "derivatives_overlay": derivatives_overlay,
        "generated_at": _utc_iso(),
    }


@router.get("/collector/tasks")
async def get_collector_tasks():
    return {
        "running": data_collector.is_running,
        "task_count": data_collector.task_count,
        "tasks": data_collector.list_tasks(),
    }


@router.get("/download/tasks")
async def list_download_tasks(
    task_ids: Optional[str] = None,
    batch_id: Optional[str] = None,
    limit: int = 100,
):
    _sync_download_task_liveness()
    requested_ids = {
        str(raw or "").strip()
        for raw in str(task_ids or "").split(",")
        if str(raw or "").strip()
    }
    tasks = list(_DOWNLOAD_TASKS.values())
    if requested_ids:
        tasks = [item for item in tasks if str(item.get("task_id") or "") in requested_ids]
    if batch_id:
        tasks = [item for item in tasks if str(item.get("batch_id") or "") == str(batch_id)]
    tasks = sorted(
        tasks,
        key=lambda item: str(item.get("created_at") or ""),
        reverse=True,
    )
    if not requested_ids and not batch_id:
        safe_limit = max(1, min(int(limit or 100), 500))
        tasks = tasks[:safe_limit]
    return {
        "count": len(tasks),
        "tasks": tasks,
        "summary": {
            "pending": sum(1 for item in tasks if str(item.get("status") or "") == "pending"),
            "running": sum(1 for item in tasks if str(item.get("status") or "") == "running"),
            "completed": sum(1 for item in tasks if str(item.get("status") or "") == "completed"),
            "failed": sum(1 for item in tasks if str(item.get("status") or "") == "failed"),
        },
    }


@router.get("/download/tasks/{task_id}")
async def get_download_task(task_id: str):
    _sync_download_task_liveness()
    task = _DOWNLOAD_TASKS.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.post("/seconds/backfill/start", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def start_second_level_backfill(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    days: int = 365,
    window_days: int = 1,
):
    days = max(1, min(days, 1200))
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    task = second_level_backfill_manager.start_task(
        exchange=exchange,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        window_days=max(1, min(window_days, 7)),
    )
    return {
        "success": True,
        "message": "秒级回填任务已启动",
        "task": task,
    }


@router.get("/seconds/backfill/tasks")
async def list_second_level_backfill_tasks():
    tasks = second_level_backfill_manager.list_tasks()
    return {
        "count": len(tasks),
        "tasks": tasks,
    }


@router.get("/seconds/backfill/tasks/{task_id}")
async def get_second_level_backfill_task(task_id: str):
    task = second_level_backfill_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.post("/seconds/backfill/tasks/{task_id}/stop", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def stop_second_level_backfill_task(task_id: str):
    ok = second_level_backfill_manager.stop_task(task_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"success": True, "task_id": task_id}


@router.post("/replay/start", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def start_replay(req: ReplayStartRequest):
    _prune_replay_sessions()
    df = await _load_symbol_df(
        exchange=req.exchange,
        symbol=req.symbol,
        timeframe=req.timeframe,
        start_time=req.start_time,
        end_time=req.end_time,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少回放数据")

    window = max(20, min(int(req.window or 300), 5000))
    replay_id = _new_replay_id(
        {
            "exchange": req.exchange,
            "symbol": req.symbol,
            "timeframe": req.timeframe,
        }
    )
    now_utc = datetime.now(timezone.utc)
    now_monotonic = time.monotonic()
    _REPLAY_SESSIONS[replay_id] = {
        "exchange": req.exchange,
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "window": window,
        "speed": max(0.1, min(float(req.speed or 1.0), 100.0)),
        "data": df,
        "cursor": 0,
        "started_at": now_utc.isoformat(),
        "created_monotonic": now_monotonic,
        "last_access_monotonic": now_monotonic,
        "last_accessed_at": now_utc.isoformat(),
    }
    _prune_replay_sessions(keep_id=replay_id)
    return {
        "replay_id": replay_id,
        "exchange": req.exchange,
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "total": int(len(df)),
        "window": window,
        "started_at": _REPLAY_SESSIONS[replay_id]["started_at"],
    }


@router.get("/replay/{replay_id}")
async def get_replay_status(replay_id: str):
    session = _get_replay_session(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")
    total = int(len(session["data"]))
    cursor = int(session["cursor"])
    return {
        "replay_id": replay_id,
        "exchange": session["exchange"],
        "symbol": session["symbol"],
        "timeframe": session["timeframe"],
        "cursor": cursor,
        "total": total,
        "progress": round(cursor / total, 6) if total > 0 else 0.0,
        "done": cursor >= total,
    }


@router.get("/replay/{replay_id}/next")
async def replay_next(replay_id: str, steps: int = 1):
    session = _get_replay_session(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")

    df = session["data"]
    total = int(len(df))
    cursor = int(session["cursor"])
    steps = max(1, min(int(steps), 2000))
    next_cursor = min(total, cursor + steps)
    session["cursor"] = next_cursor

    window = int(session["window"])
    start = max(0, next_cursor - window)
    chunk = df.iloc[start:next_cursor]
    rows = [
        {
            # Replay session feeds the same candlestick chart on the data page.
            # Naive ISO would shift bars by the user's TZ offset — see
            # _to_utc_iso() and the /klines fix on the same module.
            "timestamp": _to_utc_iso(idx),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume", 0.0)),
        }
        for idx, row in chunk.iterrows()
    ]
    return {
        "replay_id": replay_id,
        "cursor": next_cursor,
        "total": total,
        "done": next_cursor >= total,
        "window": window,
        "data": rows,
    }


@router.post("/replay/{replay_id}/seek", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def replay_seek(replay_id: str, timestamp: str):
    session = _get_replay_session(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")
    df = session["data"]
    try:
        ts = pd.to_datetime(timestamp)
    except Exception:
        raise HTTPException(status_code=400, detail="timestamp 格式错误")
    idx = int(df.index.searchsorted(ts, side="left"))
    idx = max(0, min(idx, len(df)))
    session["cursor"] = idx
    return {"replay_id": replay_id, "cursor": idx, "total": int(len(df))}


@router.delete("/replay/{replay_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def stop_replay(replay_id: str):
    if replay_id in _REPLAY_SESSIONS:
        _REPLAY_SESSIONS.pop(replay_id, None)
        return {"success": True, "replay_id": replay_id}
    raise HTTPException(status_code=404, detail="Replay session not found")


@router.get("/onchain/overview")
async def get_onchain_overview(
    symbol: str = "BTC/USDT",
    exchange: str = "binance",
    whale_threshold_btc: float = 10.0,
    chain: str = "auto",
    refresh: bool = False,
    hours: int = 4,
):
    chain_context = resolve_onchain_chain_context(symbol, chain)
    cache_key = _onchain_overview_cache_key(
        exchange,
        symbol,
        whale_threshold_btc,
        str(chain_context.get("cache_key") or chain or "auto"),
    )
    cached_payload = _prepare_cached_onchain_payload(cache_key, refresh=bool(refresh))
    if cached_payload and not refresh and not cached_payload.get("stale"):
        return cached_payload

    existing_task = _ONCHAIN_OVERVIEW_REFRESH_TASKS.get(cache_key)
    if existing_task is None or existing_task.done():
        _ONCHAIN_OVERVIEW_REFRESH_TASKS[cache_key] = asyncio.create_task(
            _refresh_onchain_overview_cache(
                cache_key,
                exchange=exchange,
                symbol=symbol,
                whale_threshold_btc=whale_threshold_btc,
                chain=chain,
            )
        )
    current_task = _ONCHAIN_OVERVIEW_REFRESH_TASKS.get(cache_key)
    if refresh and not cached_payload:
        placeholder = _build_onchain_placeholder(
            exchange=exchange,
            symbol=symbol,
            whale_threshold_btc=whale_threshold_btc,
            chain=chain,
            reason="链上概览正在后台预热",
        )
        placeholder["window_hours"] = max(4, int(hours or 4))
        return placeholder
    if not cached_payload and not refresh:
        placeholder = _build_onchain_placeholder(
            exchange=exchange,
            symbol=symbol,
            whale_threshold_btc=whale_threshold_btc,
            chain=chain,
            reason="链上概览正在后台预热",
        )
        placeholder["window_hours"] = max(4, int(hours or 4))
        return placeholder
    if not cached_payload and current_task:
        try:
            await asyncio.wait_for(asyncio.shield(current_task), timeout=7.5)
            refreshed = _prepare_cached_onchain_payload(cache_key, refresh=bool(refresh))
            if refreshed:
                refreshed["window_hours"] = max(4, int(hours or 4))
                return refreshed
        except Exception:
            pass
    if cached_payload:
        payload = _clone_jsonable(cached_payload)
        payload["refreshing"] = True
        payload["served_mode"] = "cache_refresh"
        payload["window_hours"] = max(4, int(hours or 4))
        return payload
    placeholder = _build_onchain_placeholder(
        exchange=exchange,
        symbol=symbol,
        whale_threshold_btc=whale_threshold_btc,
        chain=chain,
        reason="链上概览正在后台预热",
    )
    placeholder["window_hours"] = max(4, int(hours or 4))
    return placeholder


@router.get("/coinglass/overview")
async def get_coinglass_overview(
    symbol: str = "BTC/USDT",
    refresh: bool = False,
    manual: bool = False,
):
    return await build_coinglass_overview_payload(symbol=symbol, refresh=bool(refresh), manual=bool(manual))


@router.get("/derivatives/overview")
async def get_derivatives_overview(
    symbol: str = "BTC/USDT",
    refresh: bool = False,
    manual: bool = False,
):
    return await build_coinglass_overview_payload(symbol=symbol, refresh=bool(refresh), manual=bool(manual))


@router.get("/multi-assets/overview")
async def get_multi_assets_overview(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT",
    timeframe: str = "1h",
    lookback: int = 500,
    exclude_retired: bool = True,
):
    requested = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    filtered_requested, excluded_retired = _research_retired_filter(
        exchange=exchange, timeframe=timeframe, requested=requested, exclude_retired=exclude_retired
    )
    symbol_list = filtered_requested
    symbol_list = symbol_list[:30]
    rows = []
    ret_map: Dict[str, pd.Series] = {}

    load_tasks = [
        _load_symbol_df(exchange=exchange, symbol=sym, timeframe=timeframe)
        for sym in symbol_list
    ]
    results = await asyncio.gather(*load_tasks, return_exceptions=True)

    for sym, result in zip(symbol_list, results):
        if isinstance(result, Exception):
            continue
        df = result
        if df.empty:
            continue
        sdf = df.tail(max(80, int(lookback)))
        close = sdf["close"].astype(float)
        ret = close.pct_change().dropna()
        if ret.empty:
            continue
        ret_map[sym] = ret
        rows.append(
            {
                "symbol": sym,
                "last": float(close.iloc[-1]),
                "return_pct": round(float((close.iloc[-1] / close.iloc[0] - 1) * 100), 4),
                "volatility_pct": round(float(ret.std() * math.sqrt(len(ret)) * 100), 4),
                "avg_volume": round(float(sdf["volume"].astype(float).mean()), 6),
                "max_drawdown_pct": round(float(((close / close.cummax()) - 1).min() * 100), 4),
            }
        )

    corr = {}
    if ret_map:
        corr_df = pd.DataFrame(ret_map).dropna(how="all")
        if len(corr_df.columns) >= 2:
            corr = corr_df.corr().round(4).fillna(0.0).to_dict()

    rows.sort(key=lambda x: x["return_pct"], reverse=True)
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_count": len(requested),
            "requested_after_filter_count": len(symbol_list),
        },
        "count": len(rows),
        "assets": rows,
        "correlation": corr,
    }


@router.get("/factors/fama")
async def get_fama_like_factors(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT,DOGE/USDT,ADA/USDT",
    timeframe: str = "1h",
    lookback: int = 1000,
    exclude_retired: bool = True,
):
    requested0 = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    cache_key = _research_payload_cache_key(
        "fama",
        exchange=exchange,
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=exclude_retired,
        symbols=requested0,
    )
    cached_payload = _prepare_research_cached_payload(_FAMA_CACHE, _FAMA_REFRESH_TASKS, cache_key, refresh=False)
    if cached_payload and not cached_payload.get("stale"):
        return cached_payload

    existing_task = _FAMA_REFRESH_TASKS.get(cache_key)
    if existing_task is None or existing_task.done():
        _FAMA_REFRESH_TASKS[cache_key] = asyncio.create_task(
            _refresh_fama_cache(
                cache_key,
                exchange=exchange,
                symbols_requested=requested0,
                timeframe=timeframe,
                lookback=int(lookback),
                exclude_retired=exclude_retired,
            )
        )
    current_task = _FAMA_REFRESH_TASKS.get(cache_key)
    if not cached_payload and current_task:
        try:
            await asyncio.wait_for(asyncio.shield(current_task), timeout=2.0)
            refreshed = _prepare_research_cached_payload(_FAMA_CACHE, _FAMA_REFRESH_TASKS, cache_key, refresh=False)
            if refreshed:
                return refreshed
        except Exception:
            pass
    if cached_payload:
        payload = _clone_jsonable(cached_payload)
        payload["refreshing"] = True
        payload["served_mode"] = "cache_refresh"
        return payload
    return _build_fama_placeholder(
        exchange=exchange,
        timeframe=timeframe,
        symbols_requested=requested0,
        exclude_retired=exclude_retired,
        reason="Fama 因子正在后台计算",
    )


@router.get("/factors/library")
async def get_factor_library(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT,DOGE/USDT,ADA/USDT",
    timeframe: str = "1h",
    lookback: int = 1200,
    quantile: float = 0.3,
    series_limit: int = 500,
    exclude_retired: bool = True,
):
    timeframe = str(timeframe or "1h").lower()
    lookback = int(lookback)
    if timeframe.endswith("s"):
        lookback = min(lookback, 300)
    elif timeframe == "1m":
        lookback = min(lookback, 480)
    elif timeframe == "5m":
        lookback = min(lookback, 900)
    elif timeframe == "15m":
        lookback = min(lookback, 1400)
    elif timeframe == "1h":
        lookback = min(lookback, 1800)
    else:
        lookback = min(lookback, 2400)
    lookback = max(120, lookback)

    requested0 = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    cache_key = _research_payload_cache_key(
        "factor_library",
        exchange=exchange,
        timeframe=timeframe,
        lookback=lookback,
        quantile=quantile,
        series_limit=series_limit,
        exclude_retired=exclude_retired,
        symbols=requested0,
    )
    cached_payload = _prepare_research_cached_payload(_FACTOR_LIBRARY_CACHE, _FACTOR_LIBRARY_REFRESH_TASKS, cache_key, refresh=False)
    if not cached_payload:
        disk_payload = _load_research_disk_cached_payload("factor_library", cache_key)
        if disk_payload:
            cached_payload = _prime_research_cached_payload(
                _FACTOR_LIBRARY_CACHE,
                cache_key,
                disk_payload,
                age_sec=float(disk_payload.get("cache_age_sec") or 0.0),
            )
            cached_payload = _prepare_research_cached_payload(
                _FACTOR_LIBRARY_CACHE,
                _FACTOR_LIBRARY_REFRESH_TASKS,
                cache_key,
                refresh=False,
            )
    if cached_payload and not cached_payload.get("stale"):
        return cached_payload

    existing_task = _FACTOR_LIBRARY_REFRESH_TASKS.get(cache_key)
    if existing_task is None or existing_task.done():
        _FACTOR_LIBRARY_REFRESH_META[cache_key] = {
            "started_at": _utc_iso(),
            "started_monotonic": time.monotonic(),
            "exchange": exchange,
            "timeframe": timeframe,
            "lookback": lookback,
            "symbol_count": len(requested0),
            "symbols_requested": list(requested0),
        }
        _FACTOR_LIBRARY_REFRESH_TASKS[cache_key] = asyncio.create_task(
            _refresh_factor_library_cache(
                cache_key,
                exchange=exchange,
                symbols_requested=requested0,
                timeframe=timeframe,
                lookback=lookback,
                quantile=float(quantile),
                series_limit=int(series_limit),
                exclude_retired=exclude_retired,
            )
        )
    current_task = _FACTOR_LIBRARY_REFRESH_TASKS.get(cache_key)
    if not cached_payload and current_task:
        try:
            await asyncio.wait_for(asyncio.shield(current_task), timeout=float(_FACTOR_LIBRARY_BOOTSTRAP_WAIT_SEC))
            refreshed = _prepare_research_cached_payload(_FACTOR_LIBRARY_CACHE, _FACTOR_LIBRARY_REFRESH_TASKS, cache_key, refresh=False)
            if refreshed:
                return refreshed
        except Exception:
            pass
    if cached_payload:
        return _augment_factor_library_refreshing_payload(
            cached_payload,
            refresh_meta=_factor_library_refresh_meta(cache_key),
        )
    return _build_factor_library_pending_placeholder(
        exchange=exchange,
        timeframe=timeframe,
        symbols_requested=requested0,
        exclude_retired=exclude_retired,
        refresh_meta=_factor_library_refresh_meta(cache_key),
    )
