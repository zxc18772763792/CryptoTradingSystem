"""Trading API endpoints."""
import asyncio
import contextlib
import copy
import hashlib
import hmac
import inspect
import json
import math
import re
import statistics
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
import pandas as pd
from fastapi import HTTPException
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import case, func, select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsHistoryIngestStatus,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from config.exchanges import get_exchange_config
from config.settings import settings
from core.audit import audit_logger
from core.data import data_storage
from core.data.coinglass_client import CoinglassClient, coinglass_enabled
from core.exchanges.exchange_manager import exchange_manager
from core.exchanges.base_exchange import OrderSide, OrderType
from core.exchanges.binance_connector import BinanceConnector
from core.marketdata.runtime_price_provider import get_realtime_price
from core.notifications import notification_manager as _notification_manager
from core.risk.risk_manager import risk_manager
from core.runtime import runtime_state
from core.strategies import Signal, SignalType, strategy_manager as _strategy_manager
from core.trading import (
    account_manager,
    account_snapshot_manager,
    execution_engine,
    order_manager,
    position_manager,
)
from core.trading.binance_rest import (
    binance_ccxt_symbol as shared_binance_ccxt_symbol,
    binance_has_credentials as shared_binance_has_credentials,
    binance_market_symbol as shared_binance_market_symbol,
    binance_signed_request as shared_binance_signed_request,
)
from core.trading.order_manager import OrderRequest as CoreOrderRequest
from core.utils.asyncio_compat import LoopBoundAsyncLock
from core.utils.asset_valuation import STABLE_COINS
from core.utils.shared_ssl import get_shared_ssl_context

# Backward-compatible module aliases for code and tests that monkeypatch
# `web.api.trading.*` directly.
notification_manager = _notification_manager
strategy_manager = _strategy_manager

_BALANCE_FETCH_TIMEOUT_SEC = 18.0  # was 5.5 — too tight for the 3-4 sequential wallet fetches (funding+spot+futures) through a proxy (~1.3s/call); the cache fast-path still serves the UI within seconds, so a longer background-refresh budget just keeps the cache fresh instead of timing out
_TICKER_FETCH_TIMEOUT_SEC = 1.6
_BALANCE_SNAPSHOT_CACHE_TTL_SEC = 300.0
_BALANCE_SNAPSHOT_FAST_AGE_SEC = 12.0
_LIVE_ORDER_DETAILS_CACHE_TTL_SEC = 60.0
_BALANCE_SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}
_LIVE_POSITION_SNAPSHOT_CACHE: Dict[str, Any] = {"ts": 0.0, "data": {}}
# Single-flight guard so concurrent TTL misses don't all launch their own
# full cross-exchange position scans; waiters reuse the refresher's result.
_LIVE_POSITION_SNAPSHOT_LOCK = LoopBoundAsyncLock()
_LIVE_POSITION_SNAPSHOT_TTL_SEC = 6.0
_LIVE_POSITION_FETCH_TIMEOUT_SEC = 8.5
_LIVE_DAILY_REALIZED_PNL_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "day_start": "",
    "payload": {},
}
_LIVE_DAILY_REALIZED_PNL_TTL_SEC = 45.0
_LIVE_POSITION_DETAILS_CACHE_TTL_SEC = 12.0
_LIVE_POSITION_DETAILS_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "positions": [],
    "diagnostics": None,
}


def _normalize_position_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if ":" in text:
        text = text.split(":", 1)[0]
    return text


def _normalize_position_side(value: Any) -> str:
    raw = getattr(value, "value", value)
    text = str(raw or "").strip().lower()
    if text in {"sell", "short"}:
        return "short"
    if text in {"buy", "long"}:
        return "long"
    return text


def _is_manual_strategy_label(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text.startswith("manual")


def _is_system_owned_local_position(position: Any) -> bool:
    strategy = str(getattr(position, "strategy", "") or "").strip()
    if not strategy or _is_manual_strategy_label(strategy):
        return False
    metadata = getattr(position, "metadata", {}) or {}
    source = str(metadata.get("source") or "").strip().lower() if isinstance(metadata, dict) else ""
    return source not in {"manual", "exchange_live", "external"}


def _local_system_position_qty_by_key() -> Dict[Tuple[str, str, str], float]:
    try:
        local_positions = position_manager.get_all_positions(scope="live")
    except TypeError:
        local_positions = position_manager.get_all_positions()
    except Exception:
        return {}

    quantities: Dict[Tuple[str, str, str], float] = {}
    for pos in local_positions or []:
        if not _is_system_owned_local_position(pos):
            continue
        qty = abs(_safe_float(getattr(pos, "quantity", 0.0), default=0.0))
        if qty <= 0:
            continue
        key = (
            str(getattr(pos, "exchange", "") or "").strip().lower(),
            _normalize_position_symbol(getattr(pos, "symbol", "")),
            _normalize_position_side(getattr(pos, "side", "")),
        )
        if not all(key):
            continue
        quantities[key] = quantities.get(key, 0.0) + qty
    return quantities


def _schedule_audit_log(**kwargs: Any) -> None:
    audit_logger.schedule(**kwargs)


_LIVE_ORDER_DETAILS_CACHE: Dict[str, Any] = {"ts": 0.0, "orders": []}
_LIVE_CONDITIONAL_ORDER_CACHE: Dict[str, Any] = {"ts": 0.0, "orders": []}
_RULE_PRICE_CACHE_TTL_SEC = 10.0
_RULE_PRICE_FETCH_TIMEOUT_SEC = 1.5
_RULE_PRICE_CACHE: Dict[str, Any] = {"ts": 0.0, "prices": {}}
_RULE_PRICE_IN_FLIGHT: Optional[asyncio.Task] = None
_MICROSTRUCTURE_SNAPSHOT_CACHE: Dict[str, Any] = {}
_MICROSTRUCTURE_SNAPSHOT_CACHE_TTL_SEC = 6.0
_MICROSTRUCTURE_SNAPSHOT_STALE_MAX_AGE_SEC = 120.0
_COMMUNITY_OVERVIEW_CACHE: Dict[str, Any] = {}
_COMMUNITY_OVERVIEW_CACHE_TTL_SEC = 15.0
_COMMUNITY_OVERVIEW_STALE_MAX_AGE_SEC = 10 * 60.0
_RISK_DASHBOARD_CACHE: Dict[str, Any] = {}
_RISK_DASHBOARD_CACHE_TTL_SEC = 20.0
_RISK_DASHBOARD_STALE_MAX_AGE_SEC = 5 * 60.0
_RISK_DASHBOARD_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_MICROSTRUCTURE_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_COMMUNITY_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_ANALYTICS_ROOT = Path("./data/cache/analytics")
_BEHAVIOR_JOURNAL_PATH = _ANALYTICS_ROOT / "behavior_journal.json"
_STOPLOSS_POLICY_PATH = _ANALYTICS_ROOT / "stoploss_policy.json"
_LIVE_EQUITY_BASELINE_PATH = _ANALYTICS_ROOT / "live_equity_baseline.json"
_ANALYTICS_ORDERBOOK_TIMEOUT_SEC = 3.6
_ANALYTICS_TRADE_IMBALANCE_TIMEOUT_SEC = 3.6
_ANALYTICS_FUNDING_TIMEOUT_SEC = 1.8
_ANALYTICS_BASIS_TIMEOUT_SEC = 2.2
_ANALYTICS_OI_TIMEOUT_SEC = 3.2
_ANALYTICS_LONG_SHORT_TIMEOUT_SEC = 3.2
_ANALYTICS_OPTIONS_TIMEOUT_SEC = 1.8
_ANALYTICS_WHALE_TIMEOUT_SEC = 6.0
_ANALYTICS_WHALE_MIN_BTC = 10.0
_ANALYTICS_ANNOUNCEMENT_TIMEOUT_SEC = 4.0
_ANALYTICS_CALENDAR_TIMEOUT_SEC = 6.0
_ANALYTICS_SECURITY_ALERT_TIMEOUT_SEC = 6.0
_ANALYTICS_COLLECTOR_TIMEOUT_SEC = 15.0
_ANALYTICS_COINGLASS_REFRESH_MAX_AGE_SEC = 5 * 60.0
_ANALYTICS_COINGLASS_OVERVIEW_TIMEOUT_SEC = 2.2
_ANALYTICS_PUBLIC_CORE_TIMEOUT_FLOOR_SEC = 12.0
_ANALYTICS_HISTORY_HEALTH_CACHE_TTL_SEC = 20.0
_ANALYTICS_HISTORY_STATUS_CACHE_TTL_SEC = 8.0
_ANALYTICS_HISTORY_HEALTH_READ_TIMEOUT_SEC = 6.0
_ANALYTICS_HISTORY_STATUS_READ_TIMEOUT_SEC = 4.0
_DEFAULT_STOPLOSS_POLICY: Dict[str, Any] = {
    "atr": {"enabled": True, "period": 14, "multiplier": 2.0},
    "time_stop": {"enabled": True, "max_hours": 24},
    "r_stop": {"enabled": True, "max_loss_r": 1.0},
    "trailing": {"enabled": True},
    "partial_stop": {"enabled": True, "r1_ratio": 0.5, "r2_ratio": 0.5},
}
_SLOWMIST_HACKED_URL = "https://hacked.slowmist.io/"
_SECURITY_ALERTS_CACHE: Dict[str, Any] = {}
_SECURITY_ALERTS_CACHE_TTL_SEC = 10 * 60.0
_SECURITY_ALERTS_STALE_MAX_AGE_SEC = 6 * 60 * 60.0
_TRADING_CALENDAR_CACHE: Dict[str, Any] = {}
_TRADING_CALENDAR_CACHE_TTL_SEC = 5 * 60.0
_TRADING_CALENDAR_STALE_MAX_AGE_SEC = 60 * 60.0
_COINGLASS_CALENDAR_LOOKAHEAD_DAYS = 15
_COINGLASS_UNLOCK_MAX_PAGES = 2
_COINGLASS_CALENDAR_MAJOR_COUNTRY_CODES = {
    "US",
    "USA",
    "CN",
    "CHN",
    "EA",
    "EMU",
    "EU",
    "EUR",
    "JP",
    "JPN",
    "GB",
    "GBR",
    "UK",
    "CA",
    "CAN",
    "AU",
    "AUS",
    "CH",
    "CHE",
}
_COINGLASS_CALENDAR_PRIORITY_KEYWORDS = (
    "cpi",
    "ppi",
    "pce",
    "gdp",
    "pmi",
    "nonfarm",
    "nfp",
    "payroll",
    "unemployment",
    "jobless",
    "retail sales",
    "interest rate",
    "rate decision",
    "fomc",
    "minutes",
    "inflation",
    "利率",
    "非农",
    "失业",
    "就业",
    "零售销售",
    "通胀",
    "纪要",
    "国内生产总值",
    "gdp",
    "制造业pmi",
    "服务业pmi",
    "pmi",
    "消费者物价指数",
    "生产者物价指数",
    "核心pce",
)
_SLOWMIST_SYMBOL_ALIASES: Dict[str, List[str]] = {
    "ADA": ["cardano", "ada"],
    "APT": ["aptos", "apt"],
    "ARB": ["arbitrum", "arb"],
    "ATOM": ["cosmos", "atom"],
    "AVAX": ["avalanche", "avax"],
    "BNB": ["bnb", "bsc", "binance smart chain", "binance"],
    "BTC": ["bitcoin", "btc"],
    "DOGE": ["dogecoin", "doge"],
    "ETH": ["ethereum", "eth"],
    "FIL": ["filecoin", "fil"],
    "HBAR": ["hedera", "hbar"],
    "ICP": ["internet computer", "icp"],
    "INJ": ["injective", "inj"],
    "LINK": ["chainlink", "link"],
    "MATIC": ["polygon", "matic", "pol"],
    "MKR": ["maker", "mkr"],
    "NEAR": ["near"],
    "OP": ["optimism", "op"],
    "POL": ["polygon", "pol", "matic"],
    "SOL": ["solana", "sol"],
    "SUI": ["sui"],
    "TON": ["the open network", "ton"],
    "TRX": ["tron", "trx"],
    "UNI": ["uniswap", "uni"],
    "XRP": ["ripple", "xrp"],
}

_BINANCE_RECV_WINDOW = 5000
_BINANCE_REST_TIMEOUT_SEC = 4.5
_BINANCE_TIME_OFFSET_MS: Dict[str, Any] = {"api": 0, "fapi": 0, "ts": 0.0}
_HTTPX_SUPPORTS_PROXY_KW = (
    "proxy" in inspect.signature(httpx.AsyncClient.__init__).parameters
)
_CACHE_RUNTIME_FIELDS = {
    "cache_hit",
    "cache_age_sec",
    "stale",
    "stale_reason",
    "source_status",
}


def _to_utc_iso(value: Any) -> str:
    """Serialize a datetime / pd.Timestamp as unambiguous UTC ISO (``...Z``).

    Mirrors ``web/api/data.py:_to_utc_iso`` — kept local to avoid cross-module
    import. Returning naive ISO from chart endpoints causes browsers to parse
    timestamps as local time, visually shifting candlesticks and benchmark
    series by the user's timezone offset.
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
    if iso.endswith("+00:00"):
        return iso[:-6] + "Z"
    return iso


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


def _strip_cache_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(payload or {}).items()
        if key not in _CACHE_RUNTIME_FIELDS
    }


def _resolve_cache_source_status(
    *,
    source_status: str,
    cache_hit: bool,
    stale: bool,
) -> str:
    if source_status:
        return str(source_status)
    if stale:
        return "cache_stale"
    if cache_hit:
        return "cache_fresh"
    return "live"


def _with_cache_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
    stale_note: Optional[str] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(_strip_cache_runtime_fields(payload))
    out["cache_hit"] = bool(cache_hit)
    out["cache_age_sec"] = (
        round(float(cache_age_sec or 0.0), 3) if cache_age_sec is not None else None
    )
    out["stale"] = bool(stale)
    out["source_status"] = _resolve_cache_source_status(
        source_status=str(source_status or "").strip(),
        cache_hit=bool(cache_hit),
        stale=bool(stale),
    )
    if stale_reason:
        out["stale_reason"] = str(stale_reason)
        note_parts = [str(out.get("note") or "").strip(), str(stale_note or "").strip()]
        out["note"] = "；".join(dict.fromkeys(part for part in note_parts if part))
    else:
        out.pop("stale_reason", None)
    return out


def _clear_trading_api_runtime_caches() -> Dict[str, Any]:
    global _RULE_PRICE_IN_FLIGHT

    balance_entries = len(_BALANCE_SNAPSHOT_CACHE)
    micro_entries = len(_MICROSTRUCTURE_SNAPSHOT_CACHE)
    community_entries = len(_COMMUNITY_OVERVIEW_CACHE)
    risk_entries = len(_RISK_DASHBOARD_CACHE)
    security_entries = len(_SECURITY_ALERTS_CACHE)
    calendar_entries = len(_TRADING_CALENDAR_CACHE)
    analytics_history_health_entries = len(_ANALYTICS_HISTORY_HEALTH_CACHE)
    analytics_history_status_entries = len(_ANALYTICS_HISTORY_STATUS_CACHE)
    analytics_history_last_entries = len(_ANALYTICS_HISTORY_STATUS_LAST)
    rule_price_entries = len(_RULE_PRICE_CACHE.get("prices") or {})
    risk_refresh_tasks_cancelled = _cancel_pending_task_map(_RISK_DASHBOARD_REFRESH_TASKS)
    micro_refresh_tasks_cancelled = _cancel_pending_task_map(_MICROSTRUCTURE_REFRESH_TASKS)
    community_refresh_tasks_cancelled = _cancel_pending_task_map(_COMMUNITY_REFRESH_TASKS)
    rule_price_in_flight_cancelled = _cancel_pending_task(_RULE_PRICE_IN_FLIGHT)

    _BALANCE_SNAPSHOT_CACHE.clear()
    _MICROSTRUCTURE_SNAPSHOT_CACHE.clear()
    _COMMUNITY_OVERVIEW_CACHE.clear()
    _RISK_DASHBOARD_CACHE.clear()
    _SECURITY_ALERTS_CACHE.clear()
    _TRADING_CALENDAR_CACHE.clear()
    _ANALYTICS_HISTORY_HEALTH_CACHE.clear()
    _ANALYTICS_HISTORY_STATUS_CACHE.clear()
    _ANALYTICS_HISTORY_STATUS_LAST.clear()
    _RULE_PRICE_CACHE["ts"] = 0.0
    _RULE_PRICE_CACHE["prices"] = {}
    _RULE_PRICE_IN_FLIGHT = None
    _LIVE_POSITION_SNAPSHOT_CACHE["ts"] = 0.0
    _LIVE_POSITION_SNAPSHOT_CACHE["data"] = {}
    _LIVE_DAILY_REALIZED_PNL_CACHE["ts"] = 0.0
    _LIVE_DAILY_REALIZED_PNL_CACHE["day_start"] = ""
    _LIVE_DAILY_REALIZED_PNL_CACHE["payload"] = {}
    _LIVE_POSITION_DETAILS_CACHE["ts"] = 0.0
    _LIVE_POSITION_DETAILS_CACHE["positions"] = []
    _LIVE_POSITION_DETAILS_CACHE["diagnostics"] = None
    _LIVE_ORDER_DETAILS_CACHE["ts"] = 0.0
    _LIVE_ORDER_DETAILS_CACHE["orders"] = []
    _LIVE_CONDITIONAL_ORDER_CACHE["ts"] = 0.0
    _LIVE_CONDITIONAL_ORDER_CACHE["orders"] = []
    return {
        "balance_entries_cleared": balance_entries,
        "microstructure_entries_cleared": micro_entries,
        "community_overview_entries_cleared": community_entries,
        "risk_dashboard_entries_cleared": risk_entries,
        "security_alert_entries_cleared": security_entries,
        "trading_calendar_entries_cleared": calendar_entries,
        "analytics_history_health_entries_cleared": analytics_history_health_entries,
        "analytics_history_status_entries_cleared": analytics_history_status_entries,
        "analytics_history_status_last_entries_cleared": analytics_history_last_entries,
        "rule_price_entries_cleared": rule_price_entries,
        "risk_dashboard_refresh_tasks_cancelled": risk_refresh_tasks_cancelled,
        "microstructure_refresh_tasks_cancelled": micro_refresh_tasks_cancelled,
        "community_refresh_tasks_cancelled": community_refresh_tasks_cancelled,
        "rule_price_in_flight_cancelled": rule_price_in_flight_cancelled,
    }


def _inspect_trading_api_runtime_caches() -> Dict[str, Any]:
    now_ts = time.time()

    def _age(value: float) -> Optional[float]:
        if value <= 0:
            return None
        return round(max(0.0, now_ts - float(value)), 3)

    return {
        "balance_snapshot_entries": len(_BALANCE_SNAPSHOT_CACHE),
        "microstructure_snapshot_entries": len(_MICROSTRUCTURE_SNAPSHOT_CACHE),
        "community_overview_entries": len(_COMMUNITY_OVERVIEW_CACHE),
        "risk_dashboard_entries": len(_RISK_DASHBOARD_CACHE),
        "security_alert_entries": len(_SECURITY_ALERTS_CACHE),
        "trading_calendar_entries": len(_TRADING_CALENDAR_CACHE),
        "analytics_history_health_entries": len(_ANALYTICS_HISTORY_HEALTH_CACHE),
        "analytics_history_status_entries": len(_ANALYTICS_HISTORY_STATUS_CACHE),
        "analytics_history_status_last_entries": len(_ANALYTICS_HISTORY_STATUS_LAST),
        "rule_price_entries": len(_RULE_PRICE_CACHE.get("prices") or {}),
        "risk_dashboard_refresh_tasks": _pending_tasks_count(_RISK_DASHBOARD_REFRESH_TASKS),
        "microstructure_refresh_tasks": _pending_tasks_count(_MICROSTRUCTURE_REFRESH_TASKS),
        "community_refresh_tasks": _pending_tasks_count(_COMMUNITY_REFRESH_TASKS),
        "rule_price_in_flight": bool(_RULE_PRICE_IN_FLIGHT and not _RULE_PRICE_IN_FLIGHT.done()),
        "rule_price_cache_age_sec": _age(float(_RULE_PRICE_CACHE.get("ts") or 0.0)),
        "live_position_snapshot_age_sec": _age(
            float(_LIVE_POSITION_SNAPSHOT_CACHE.get("ts") or 0.0)
        ),
        "live_daily_realized_pnl_age_sec": _age(
            float(_LIVE_DAILY_REALIZED_PNL_CACHE.get("ts") or 0.0)
        ),
        "live_position_details_age_sec": _age(
            float(_LIVE_POSITION_DETAILS_CACHE.get("ts") or 0.0)
        ),
        "live_order_details_age_sec": _age(
            float(_LIVE_ORDER_DETAILS_CACHE.get("ts") or 0.0)
        ),
        "live_conditional_order_age_sec": _age(
            float(_LIVE_CONDITIONAL_ORDER_CACHE.get("ts") or 0.0)
        ),
    }


runtime_state.register_cache(
    "web_api_trading",
    clear=_clear_trading_api_runtime_caches,
    inspect=_inspect_trading_api_runtime_caches,
    scope="global",
)


def _apply_httpx_proxy_kw(
    client_kwargs: Dict[str, Any], proxy_url: Optional[str]
) -> None:
    # Reuse the process-wide TLS context: per-call AsyncClient construction does
    # a blocking CA-bundle read on the event loop, which froze the live service
    # on 2026-07-02 under disk saturation (see core/utils/shared_ssl.py; warmed
    # at startup so this is a cached read here).
    client_kwargs.setdefault("verify", get_shared_ssl_context())
    proxy = str(proxy_url or "").strip()
    if not proxy:
        return
    if _HTTPX_SUPPORTS_PROXY_KW:
        client_kwargs["proxy"] = proxy
    else:
        client_kwargs["proxies"] = proxy


def _binance_rest_symbol(symbol: str) -> str:
    text = str(symbol or "").upper().strip()
    if ":" in text:
        text = text.split(":", 1)[0]
    return text.replace("/", "").replace("-", "")


def _gate_futures_contract_symbol(symbol: str) -> str:
    text = str(symbol or "").upper().strip()
    if ":" in text:
        text = text.split(":", 1)[0]
    text = text.replace("-", "/")
    if "/" in text:
        base, quote = text.split("/", 1)
        base = base.strip() or "BTC"
        quote = quote.strip() or "USDT"
        return f"{base}_{quote}"
    for quote in ("USDT", "USDC", "USD"):
        if text.endswith(quote) and len(text) > len(quote):
            base = text[: -len(quote)]
            return f"{base}_{quote}"
    return f"{text}_USDT" if text else "BTC_USDT"


def _exchange_symbol_candidates(exchange: str, symbol: str) -> List[str]:
    raw = str(symbol or "").strip()
    if not raw:
        return []
    text = raw.upper()
    if ":" in text:
        text = text.split(":", 1)[0]
    if "/" in text:
        base, quote = text.split("/", 1)
    else:
        base, quote = text, "USDT"
    spot = f"{base}/{quote}"
    perp = f"{spot}:{quote}"
    gate_contract = f"{base}_{quote}"
    candidates: List[str] = [raw, text, spot, perp]
    if str(exchange or "").lower() == "gate":
        candidates.insert(0, gate_contract)
    dedup: List[str] = []
    seen = set()
    for item in candidates:
        key = str(item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(key)
    return dedup


async def _fetch_binance_public_json(
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout_sec: float,
    futures: bool = False,
) -> Dict[str, Any]:
    base_url = "https://fapi.binance.com" if futures else "https://api.binance.com"
    client_kwargs: Dict[str, Any] = {"timeout": timeout_sec}
    _apply_httpx_proxy_kw(client_kwargs, settings.HTTP_PROXY or settings.HTTPS_PROXY)
    async with httpx.AsyncClient(**client_kwargs) as client:
        resp = await client.get(f"{base_url}{path}", params=params or {})
        resp.raise_for_status()
        return resp.json() or {}


async def _fetch_gate_public_json(
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout_sec: float,
) -> Any:
    client_kwargs: Dict[str, Any] = {"timeout": timeout_sec}
    _apply_httpx_proxy_kw(client_kwargs, settings.HTTP_PROXY or settings.HTTPS_PROXY)
    async with httpx.AsyncClient(**client_kwargs) as client:
        resp = await client.get(
            f"https://api.gateio.ws/api/v4{path}", params=params or {}
        )
        resp.raise_for_status()
        return resp.json()


async def _fetch_binance_public_orderbook(
    symbol: str, limit: int = 80
) -> Dict[str, Any]:
    try:
        payload = await _fetch_binance_public_json(
            "/api/v3/depth",
            params={
                "symbol": _binance_rest_symbol(symbol),
                "limit": max(5, min(int(limit), 100)),
            },
            timeout_sec=_ANALYTICS_ORDERBOOK_TIMEOUT_SEC,
        )
        return {
            "available": True,
            "bids": payload.get("bids") or [],
            "asks": payload.get("asks") or [],
            "timestamp": payload.get("lastUpdateId"),
        }
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "bids": [],
            "asks": [],
            "timestamp": None,
        }


def _parse_orderbook_level_pair(item: Any) -> Optional[List[float]]:
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        price = _safe_float(item[0])
        qty = _safe_float(item[1])
        if price > 0 and qty > 0:
            return [price, qty]
    if isinstance(item, dict):
        price = _safe_float(item.get("price") or item.get("p"))
        qty = _safe_float(
            item.get("size")
            or item.get("amount")
            or item.get("qty")
            or item.get("q")
            or item.get("s")
        )
        if price > 0 and qty > 0:
            return [price, qty]
    return None


async def _fetch_gate_public_orderbook(symbol: str, limit: int = 80) -> Dict[str, Any]:
    contract = _gate_futures_contract_symbol(symbol)
    try:
        payload = await _fetch_gate_public_json(
            "/futures/usdt/order_book",
            params={
                "contract": contract,
                "limit": max(5, min(int(limit), 100)),
                "with_id": "true",
            },
            timeout_sec=_ANALYTICS_ORDERBOOK_TIMEOUT_SEC,
        )
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    raw_bids = list((payload or {}).get("bids") or [])
    raw_asks = list((payload or {}).get("asks") or [])
    bids = [
        level
        for level in (_parse_orderbook_level_pair(row) for row in raw_bids)
        if level
    ]
    asks = [
        level
        for level in (_parse_orderbook_level_pair(row) for row in raw_asks)
        if level
    ]
    return {
        "available": bool(bids and asks),
        "bids": bids,
        "asks": asks,
        "timestamp": (payload or {}).get("id") or (payload or {}).get("current"),
        "source": "gate_public",
    }


async def _fetch_binance_public_trade_imbalance(
    symbol: str, limit: int = 600
) -> Dict[str, Any]:
    try:
        rows = await _fetch_binance_public_json(
            "/api/v3/trades",
            params={
                "symbol": _binance_rest_symbol(symbol),
                "limit": max(50, min(int(limit), 1000)),
            },
            timeout_sec=_ANALYTICS_TRADE_IMBALANCE_TIMEOUT_SEC,
        )
        trades = list(rows or [])
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }
    buy_volume = 0.0
    sell_volume = 0.0
    for row in trades:
        qty = abs(_safe_float(row.get("qty")))
        if bool(row.get("isBuyerMaker")):
            sell_volume += qty
        else:
            buy_volume += qty
    total = buy_volume + sell_volume
    return {
        "available": True,
        "count": len(trades),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(
            ((buy_volume - sell_volume) / total) if total > 0 else 0.0, 6
        ),
        "source": "binance_public",
    }


async def _fetch_gate_public_trade_imbalance(
    symbol: str, limit: int = 600
) -> Dict[str, Any]:
    contract = _gate_futures_contract_symbol(symbol)
    try:
        rows = await _fetch_gate_public_json(
            "/futures/usdt/trades",
            params={"contract": contract, "limit": max(50, min(int(limit), 1000))},
            timeout_sec=_ANALYTICS_TRADE_IMBALANCE_TIMEOUT_SEC,
        )
        trades = list(rows or [])
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }
    buy_volume = 0.0
    sell_volume = 0.0
    for row in trades:
        side = str((row or {}).get("side") or "").strip().lower()
        size = _safe_float((row or {}).get("size"))
        qty = abs(size)
        if qty <= 0:
            qty = abs(_safe_float((row or {}).get("amount")))
        if qty <= 0:
            continue
        if side in {"buy", "bid"} or (not side and size > 0):
            buy_volume += qty
        else:
            sell_volume += qty
    total = buy_volume + sell_volume
    return {
        "available": bool(total > 0),
        "count": len(trades),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(
            ((buy_volume - sell_volume) / total) if total > 0 else 0.0, 6
        ),
        "source": "gate_public",
    }


async def _fetch_binance_public_funding_and_basis(
    symbol: str,
) -> Dict[str, Dict[str, Any]]:
    rest_symbol = _binance_rest_symbol(symbol)
    try:
        premium_index, spot_ticker, perp_ticker = await asyncio.gather(
            _fetch_binance_public_json(
                "/fapi/v1/premiumIndex",
                params={"symbol": rest_symbol},
                timeout_sec=_ANALYTICS_FUNDING_TIMEOUT_SEC,
                futures=True,
            ),
            _fetch_binance_public_json(
                "/api/v3/ticker/price",
                params={"symbol": rest_symbol},
                timeout_sec=_ANALYTICS_BASIS_TIMEOUT_SEC,
            ),
            _fetch_binance_public_json(
                "/fapi/v1/ticker/price",
                params={"symbol": rest_symbol},
                timeout_sec=_ANALYTICS_BASIS_TIMEOUT_SEC,
                futures=True,
            ),
        )
    except Exception:
        return {"funding": {"available": False}, "basis": {"available": False}}
    funding = {
        "available": True,
        "source": "binance_public",
        "symbol": f"{symbol}:USDT" if ":" not in str(symbol or "") else symbol,
        "funding_rate": _safe_float(premium_index.get("lastFundingRate")),
        "next_funding_time": _safe_dt(premium_index.get("nextFundingTime")).isoformat()
        if _safe_dt(premium_index.get("nextFundingTime"))
        else None,
    }
    spot_px = _safe_float(spot_ticker.get("price"))
    perp_px = _safe_float(perp_ticker.get("price") or premium_index.get("markPrice"))
    if spot_px > 0 and perp_px > 0:
        basis_val = (perp_px - spot_px) / spot_px
        basis = {
            "available": True,
            "source": "binance_public",
            "spot_symbol": symbol,
            "perp_symbol": f"{symbol}:USDT" if ":" not in str(symbol or "") else symbol,
            "spot_price": spot_px,
            "perp_price": perp_px,
            "basis_pct": round(basis_val * 100, 6),
        }
    else:
        basis = {"available": False}
    return {"funding": funding, "basis": basis}


async def _fetch_gate_public_funding_and_basis(
    symbol: str,
) -> Dict[str, Dict[str, Any]]:
    contract = _gate_futures_contract_symbol(symbol)
    try:
        contract_payload, spot_tickers = await asyncio.gather(
            _fetch_gate_public_json(
                f"/futures/usdt/contracts/{contract}",
                timeout_sec=max(
                    _ANALYTICS_FUNDING_TIMEOUT_SEC, _ANALYTICS_BASIS_TIMEOUT_SEC
                ),
            ),
            _fetch_gate_public_json(
                "/spot/tickers",
                params={"currency_pair": contract},
                timeout_sec=_ANALYTICS_BASIS_TIMEOUT_SEC,
            ),
        )
    except Exception:
        return {"funding": {"available": False}, "basis": {"available": False}}

    contract_payload = dict(contract_payload or {})
    spot_ticker = {}
    if isinstance(spot_tickers, list) and spot_tickers:
        spot_ticker = dict(spot_tickers[0] or {})
    elif isinstance(spot_tickers, dict):
        spot_ticker = dict(spot_tickers)

    funding_rate = _safe_float(
        contract_payload.get("funding_rate")
        or contract_payload.get("funding_rate_indicative")
    )
    next_funding_dt = _safe_dt(
        contract_payload.get("next_funding_time")
        or contract_payload.get("funding_next_apply")
        or contract_payload.get("funding_next_apply_time")
    )
    funding = {
        "available": bool(funding_rate != 0.0 or next_funding_dt is not None),
        "symbol": symbol if ":" in str(symbol or "") else f"{symbol}:USDT",
        "funding_rate": funding_rate,
        "next_funding_time": next_funding_dt.isoformat() if next_funding_dt else None,
        "source": "gate_public",
    }

    spot_px = _safe_float(spot_ticker.get("last"))
    perp_px = _safe_float(
        contract_payload.get("mark_price")
        or contract_payload.get("last_price")
        or contract_payload.get("index_price")
    )
    if spot_px > 0 and perp_px > 0:
        basis_val = (perp_px - spot_px) / spot_px
        basis = {
            "available": True,
            "spot_symbol": symbol,
            "perp_symbol": symbol if ":" in str(symbol or "") else f"{symbol}:USDT",
            "spot_price": spot_px,
            "perp_price": perp_px,
            "basis_pct": round(basis_val * 100, 6),
            "source": "gate_public",
        }
    else:
        basis = {"available": False}
    return {"funding": funding, "basis": basis}


async def _fetch_binance_public_open_interest(symbol: str) -> Dict[str, Any]:
    rest_symbol = _binance_rest_symbol(symbol)
    try:
        current_payload, history_payload = await asyncio.gather(
            _fetch_binance_public_json(
                "/fapi/v1/openInterest",
                params={"symbol": rest_symbol},
                timeout_sec=_ANALYTICS_OI_TIMEOUT_SEC,
                futures=True,
            ),
            _fetch_binance_public_json(
                "/futures/data/openInterestHist",
                params={"symbol": rest_symbol, "period": "5m", "limit": 13},
                timeout_sec=_ANALYTICS_OI_TIMEOUT_SEC,
                futures=True,
            ),
        )
    except Exception as exc:
        return {
            "available": False,
            "source": "binance_public",
            "error": str(exc),
            "symbol": symbol,
            "volume": 0.0,
            "value": 0.0,
            "change_pct_1h": None,
            "timestamp": None,
        }

    rows = list(history_payload or [])
    rows = [row for row in rows if isinstance(row, dict)]
    rows.sort(key=lambda row: _safe_float(row.get("timestamp")))

    latest_row = rows[-1] if rows else {}
    latest_volume = (
        _safe_float(latest_row.get("sumOpenInterest")) if latest_row else 0.0
    )
    latest_value = (
        _safe_float(latest_row.get("sumOpenInterestValue")) if latest_row else 0.0
    )
    current_volume = _safe_float(current_payload.get("openInterest"))
    effective_volume = current_volume if current_volume > 0 else latest_volume
    effective_value = latest_value

    change_pct_1h: Optional[float] = None
    if len(rows) >= 13:
        ref_row = rows[-13]
        ref_value = _safe_float(ref_row.get("sumOpenInterestValue"))
        if ref_value > 0:
            change_pct_1h = round((effective_value - ref_value) / ref_value * 100.0, 6)

    timestamp_ms = _safe_float(current_payload.get("time"))
    if timestamp_ms <= 0 and latest_row:
        timestamp_ms = _safe_float(latest_row.get("timestamp"))
    ts = _safe_dt(timestamp_ms)

    return {
        "available": bool(effective_volume > 0 or effective_value > 0),
        "source": "binance_public",
        "error": None,
        "symbol": symbol,
        "volume": round(effective_volume, 6),
        "value": round(effective_value, 6),
        "change_pct_1h": change_pct_1h,
        "timestamp": ts.isoformat() if ts else None,
        "sample_size": len(rows),
    }


async def _fetch_gate_public_open_interest(symbol: str) -> Dict[str, Any]:
    contract = _gate_futures_contract_symbol(symbol)
    try:
        payload = await _fetch_gate_public_json(
            f"/futures/usdt/contracts/{contract}",
            timeout_sec=_ANALYTICS_OI_TIMEOUT_SEC,
        )
    except Exception as exc:
        return {
            "available": False,
            "source": "gate_public",
            "error": str(exc),
            "symbol": symbol,
            "volume": 0.0,
            "value": 0.0,
            "change_pct_1h": None,
            "timestamp": None,
            "sample_size": 0,
        }
    payload = dict(payload or {})
    volume = _safe_float(payload.get("open_interest") or payload.get("position_size"))
    value = _safe_float(
        payload.get("open_interest_usd") or payload.get("open_interest_value")
    )
    if value <= 0 and volume > 0:
        mark_price = _safe_float(
            payload.get("mark_price")
            or payload.get("last_price")
            or payload.get("index_price")
        )
        multiplier = _safe_float(payload.get("quanto_multiplier"), default=1.0)
        if mark_price > 0:
            value = volume * max(multiplier, 1e-9) * mark_price
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "available": bool(volume > 0 or value > 0),
        "source": "gate_public",
        "error": None,
        "symbol": symbol,
        "volume": round(volume, 6),
        "value": round(value, 6),
        "change_pct_1h": None,
        "timestamp": ts,
        "sample_size": 1,
    }


async def _fetch_open_interest_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    normalized = str(exchange or "").lower()
    if normalized == "binance":
        return await _fetch_binance_public_open_interest(symbol=symbol)
    if normalized == "gate":
        gate_payload = await _fetch_gate_public_open_interest(symbol=symbol)
        if bool((gate_payload or {}).get("available")):
            return gate_payload
    payload = await _fetch_binance_public_open_interest(symbol=symbol)
    payload = dict(payload or {})
    payload["source"] = "binance_public_fallback"
    return payload


def _normalize_long_short_ratio_payload(
    *,
    symbol: str,
    source: str,
    error: Optional[str] = None,
    long_ratio: Optional[float] = None,
    short_ratio: Optional[float] = None,
    long_short_ratio: Optional[float] = None,
    sample_size: int = 0,
    timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    lr = _safe_float(long_ratio) if long_ratio is not None else 0.0
    sr = _safe_float(short_ratio) if short_ratio is not None else 0.0
    ratio = _safe_float(long_short_ratio) if long_short_ratio is not None else 0.0

    if ratio <= 0 and lr > 0 and sr > 0:
        ratio = lr / max(sr, 1e-9)
    if ratio > 0 and lr <= 0 and sr <= 0:
        lr = ratio / (1.0 + ratio)
        sr = 1.0 - lr

    available = bool(ratio > 0 or lr > 0 or sr > 0)
    if not available:
        lr = 0.0
        sr = 0.0
        ratio = 0.0

    return {
        "available": available,
        "source": source,
        "error": str(error or "") or None,
        "symbol": symbol,
        "long_ratio": round(lr, 6),
        "short_ratio": round(sr, 6),
        "long_short_ratio": round(ratio, 6),
        "sample_size": int(max(0, sample_size)),
        "timestamp": timestamp,
    }


async def _fetch_binance_public_long_short_ratio(symbol: str) -> Dict[str, Any]:
    rest_symbol = _binance_rest_symbol(symbol)
    try:
        rows = await _fetch_binance_public_json(
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": rest_symbol, "period": "5m", "limit": 12},
            timeout_sec=_ANALYTICS_LONG_SHORT_TIMEOUT_SEC,
            futures=True,
        )
    except Exception as exc:
        return _normalize_long_short_ratio_payload(
            symbol=symbol,
            source="binance_public",
            error=str(exc),
        )

    items = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    items.sort(
        key=lambda row: _safe_float(row.get("timestamp") or row.get("time") or 0)
    )
    latest = dict(items[-1] if items else {})
    ts = _safe_dt(latest.get("timestamp") or latest.get("time"))

    return _normalize_long_short_ratio_payload(
        symbol=symbol,
        source="binance_public",
        long_ratio=latest.get("longAccount") or latest.get("longPosition"),
        short_ratio=latest.get("shortAccount") or latest.get("shortPosition"),
        long_short_ratio=latest.get("longShortRatio"),
        sample_size=len(items),
        timestamp=ts.isoformat() if ts else None,
    )


async def _fetch_gate_public_long_short_ratio(symbol: str) -> Dict[str, Any]:
    contract = _gate_futures_contract_symbol(symbol)
    try:
        payload = await _fetch_gate_public_json(
            f"/futures/usdt/contracts/{contract}",
            timeout_sec=_ANALYTICS_LONG_SHORT_TIMEOUT_SEC,
        )
    except Exception as exc:
        return _normalize_long_short_ratio_payload(
            symbol=symbol,
            source="gate_public",
            error=str(exc),
        )

    row = dict(payload or {})
    ts = _safe_dt(
        row.get("funding_next_apply")
        or row.get("next_funding_time")
        or row.get("funding_next_apply_time")
        or datetime.now(timezone.utc).isoformat()
    )
    return _normalize_long_short_ratio_payload(
        symbol=symbol,
        source="gate_public",
        long_ratio=row.get("long_users") or row.get("long_user"),
        short_ratio=row.get("short_users") or row.get("short_user"),
        long_short_ratio=row.get("long_short_ratio") or row.get("ls_ratio"),
        sample_size=1,
        timestamp=ts.isoformat() if ts else None,
    )


async def _fetch_long_short_ratio_snapshot(
    exchange: str, symbol: str
) -> Dict[str, Any]:
    normalized = str(exchange or "").lower()
    if normalized == "binance":
        return await _fetch_binance_public_long_short_ratio(symbol=symbol)
    if normalized == "gate":
        gate_payload = await _fetch_gate_public_long_short_ratio(symbol=symbol)
        if bool((gate_payload or {}).get("available")):
            return gate_payload

    fallback = await _fetch_binance_public_long_short_ratio(symbol=symbol)
    fallback = dict(fallback or {})
    fallback["source"] = "binance_public_fallback"
    return fallback


async def _fetch_funding_basis_snapshot(
    exchange: str, symbol: str
) -> Dict[str, Dict[str, Any]]:
    funding = {"available": False}
    basis = {"available": False}
    normalized = str(exchange or "").lower()
    if normalized == "binance":
        funding_basis = await _fetch_binance_public_funding_and_basis(symbol)
        funding = dict(funding_basis.get("funding") or {"available": False})
        basis = dict(funding_basis.get("basis") or {"available": False})
    else:
        # For non-Binance exchanges, try exchange-specific client first, then fall back
        # to Binance public API as a market-wide reference for funding/basis data.
        connector = exchange_manager.get_exchange(exchange)
        client = getattr(connector, "_client", None) if connector else None
        if client:
            fetch_funding_rate = getattr(client, "fetch_funding_rate", None)
            perp_symbol = symbol if ":" in symbol else f"{symbol}:USDT"
            fetch_ticker = getattr(client, "fetch_ticker", None)
            jobs: List[Any] = []
            if callable(fetch_funding_rate):
                jobs.append(
                    asyncio.wait_for(
                        fetch_funding_rate(perp_symbol),
                        timeout=_ANALYTICS_FUNDING_TIMEOUT_SEC,
                    )
                )
            else:
                jobs.append(asyncio.sleep(0, result=None))
            if callable(fetch_ticker):
                jobs.append(
                    asyncio.wait_for(
                        asyncio.gather(
                            fetch_ticker(symbol),
                            fetch_ticker(perp_symbol),
                        ),
                        timeout=_ANALYTICS_BASIS_TIMEOUT_SEC,
                    )
                )
            else:
                jobs.append(asyncio.sleep(0, result=None))
            funding_result, basis_result = await asyncio.gather(
                *jobs, return_exceptions=True
            )
            if not isinstance(funding_result, Exception) and funding_result:
                fr = funding_result or {}
                funding = {
                    "available": True,
                    "symbol": perp_symbol,
                    "funding_rate": _safe_float(fr.get("fundingRate")),
                    "next_funding_time": _safe_dt(
                        fr.get("nextFundingTimestamp")
                    ).isoformat()
                    if _safe_dt(fr.get("nextFundingTimestamp"))
                    else None,
                }
            if not isinstance(basis_result, Exception) and basis_result:
                spot_ticker, perp_ticker = basis_result
                spot_px = _safe_float((spot_ticker or {}).get("last"))
                perp_px = _safe_float((perp_ticker or {}).get("last"))
                if spot_px > 0 and perp_px > 0:
                    basis_val = (perp_px - spot_px) / spot_px
                    basis = {
                        "available": True,
                        "spot_symbol": symbol,
                        "perp_symbol": perp_symbol,
                        "spot_price": spot_px,
                        "perp_price": perp_px,
                        "basis_pct": round(basis_val * 100, 6),
                    }

    if normalized == "gate" and (
        not funding.get("available") or not basis.get("available")
    ):
        gate_fb = await _fetch_gate_public_funding_and_basis(symbol)
        if not funding.get("available") and gate_fb.get("funding", {}).get("available"):
            funding = dict(gate_fb.get("funding") or {"available": False})
        if not basis.get("available") and gate_fb.get("basis", {}).get("available"):
            basis = dict(gate_fb.get("basis") or {"available": False})

    # Fallback: if still no funding/basis data, use Binance public API as reference
    if not funding.get("available") or not basis.get("available"):
        try:
            fb = await _fetch_binance_public_funding_and_basis(symbol)
            if not funding.get("available") and fb.get("funding", {}).get("available"):
                funding = {**fb["funding"], "source": "binance_public_fallback"}
            if not basis.get("available") and fb.get("basis", {}).get("available"):
                basis = {**fb["basis"], "source": "binance_public_fallback"}
        except Exception:
            pass

    return {"funding": funding, "basis": basis}


async def _fetch_options_snapshot(symbol: str) -> Dict[str, Any]:
    # F1: Deribit options snapshot (best-effort, strict timeout for API latency budget)
    options_data: Dict[str, Any] = {"available": False}
    try:
        from core.data.options_collector import options_collector  # noqa: PLC0415

        currency = symbol.split("/")[0].split(":")[0].upper()
        snap = await asyncio.wait_for(
            options_collector.fetch_snapshot(currency),
            timeout=_ANALYTICS_OPTIONS_TIMEOUT_SEC,
        )
        if snap is not None:
            options_data = snap.to_dict()
    except Exception:
        pass
    return options_data


class OrderRequest(BaseModel):
    exchange: str
    symbol: str
    side: str  # buy/sell
    order_type: str  # market/limit
    amount: float
    price: Optional[float] = None
    leverage: float = 1.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    trigger_price: Optional[float] = None
    order_mode: str = "normal"  # normal/iceberg/twap/vwap/conditional
    iceberg_parts: int = 1
    algo_slices: int = 1
    algo_interval_sec: int = 0
    account_id: str = "main"
    reduce_only: bool = False


class OrderResponse(BaseModel):
    order_id: str
    status: str
    symbol: str
    side: str
    price: float
    amount: float
    filled: float
    timestamp: str


class RiskUpdateRequest(BaseModel):
    max_position_size: Optional[float] = None
    max_daily_loss_ratio: Optional[float] = None
    max_daily_loss_usd: Optional[float] = None
    max_daily_trades: Optional[int] = None
    max_open_positions: Optional[int] = None
    max_leverage: Optional[float] = None
    balance_volatility_alert_pct: Optional[float] = None


class TradingModeRequest(BaseModel):
    target_mode: str = Field(..., pattern="^(paper|live)$")
    reason: Optional[str] = None


class TradingModeConfirmRequest(BaseModel):
    token: str
    confirm_text: str


class AccountCreateRequest(BaseModel):
    account_id: str
    name: str
    exchange: str
    mode: str = "paper"
    parent_account_id: Optional[str] = None
    enabled: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AccountUpdateRequest(BaseModel):
    name: Optional[str] = None
    exchange: Optional[str] = None
    mode: Optional[str] = None
    parent_account_id: Optional[str] = None
    enabled: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


class PositionCloseRequest(BaseModel):
    exchange: str
    symbol: str
    side: str = Field(..., pattern="^(long|short)$")
    quantity: Optional[float] = None
    account_id: Optional[str] = None
    source: Optional[str] = None  # local | exchange_live


class BehaviorJournalRequest(BaseModel):
    mood: str = Field(default="neutral")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    plan_adherence: float = Field(default=0.5, ge=0.0, le=1.0)
    note: str = ""
    symbol: Optional[str] = None
    strategy: Optional[str] = None


class StoplossPolicyUpdateRequest(BaseModel):
    policy: Dict[str, Any] = Field(default_factory=dict)


def _serialize_order(order: Any) -> Dict[str, Any]:
    meta = order_manager.get_order_metadata(order.id)
    order_type = str(
        getattr(getattr(order, "type", None), "value", getattr(order, "type", "")) or ""
    ).lower()
    order_price = float(order.price or 0.0)
    stop_loss = meta.get("stop_loss")
    take_profit = meta.get("take_profit")
    trigger_price = meta.get("trigger_price")
    if stop_loss is None and "stop" in order_type and order_price > 0:
        stop_loss = order_price
    if take_profit is None and "take_profit" in order_type and order_price > 0:
        take_profit = order_price
    return {
        "id": order.id,
        "exchange": order.exchange,
        "symbol": order.symbol,
        "side": order.side.value,
        "type": order.type.value,
        "price": float(order.price or 0.0),
        "amount": float(order.amount or 0.0),
        "filled": float(order.filled or 0.0),
        "status": order.status.value,
        "timestamp": order.timestamp.isoformat() if order.timestamp else None,
        "strategy": meta.get("strategy"),
        "account_id": meta.get("account_id", "main"),
        "order_mode": meta.get("order_mode", "normal"),
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "trailing_stop_pct": meta.get("trailing_stop_pct"),
        "trailing_stop_distance": meta.get("trailing_stop_distance"),
        "trigger_price": trigger_price,
        "reduce_only": bool(meta.get("reduce_only", False)),
        "rejected": bool(meta.get("rejected", False)),
        "reject_reason": meta.get("reject_reason"),
        "paper_fee_rate": float(meta.get("paper_fee_rate") or 0.0),
        "paper_fee_usd": float(meta.get("paper_fee_usd") or 0.0),
        "paper_slippage_bps": float(meta.get("paper_slippage_bps") or 0.0),
        "paper_slippage_cost_usd": float(meta.get("paper_slippage_cost_usd") or 0.0),
        "paper_reference_price": float(meta.get("paper_reference_price") or 0.0),
        "paper_notional_usd": float(meta.get("paper_notional_usd") or 0.0),
    }


def _calc_usd_value(currency: str, total: float, last_price: Optional[float]) -> float:
    if total <= 0:
        return 0.0
    if currency in {"USDT", "USDC", "USD", "BUSD"}:
        return float(total)
    if last_price and last_price > 0:
        return float(total) * float(last_price)
    return 0.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return float(default)
        return out
    except Exception:
        return float(default)


def _safe_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        if ts > 0:
            try:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                return None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_runtime_mode(value: Any, default: str = "paper") -> str:
    text = str(value or default).strip().lower()
    return "live" if text == "live" else "paper"


def _resolve_account_mode(account_id: Any) -> Optional[str]:
    aid = str(account_id or "").strip()
    if not aid:
        return None
    if aid == "exchange_live":
        return "live"
    try:
        account = account_manager.get_account(aid)
    except Exception:
        account = None
    if isinstance(account, dict):
        return _normalize_runtime_mode(account.get("mode"), default="paper")
    return None


def _infer_trade_item_mode(
    *,
    payload: Optional[Dict[str, Any]] = None,
    account_id: Any = None,
    source: Any = None,
    order_id: Any = None,
) -> Optional[str]:
    details = dict(payload or {})
    for key in ("mode", "trading_mode", "runtime_mode"):
        candidate = str(details.get(key) or "").strip().lower()
        if candidate in {"paper", "live"}:
            return candidate
    if details.get("paper") is True:
        return "paper"
    source_value = str(details.get("source") or source or "").strip().lower()
    if source_value == "exchange_live":
        return "live"
    order_text = str(order_id or details.get("order_id") or "").strip().lower()
    if order_text.startswith("paper_"):
        return "paper"
    return _resolve_account_mode(account_id or details.get("account_id"))


def _optional_finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _should_refresh_coinglass_overview(
    overview: Dict[str, Any],
    *,
    max_age_sec: float = _ANALYTICS_COINGLASS_REFRESH_MAX_AGE_SEC,
) -> bool:
    overview = dict(overview or {})
    if not bool(overview.get("key_configured")):
        return False
    freshness_sec = _optional_finite_float(overview.get("freshness_sec"))
    degraded_reason = str(overview.get("degraded_reason") or "").strip()
    return (
        not bool(overview.get("available"))
        or bool(degraded_reason)
        or freshness_sec is None
        or freshness_sec > float(max_age_sec)
    )


async def _load_preferred_coinglass_overview(
    symbol: str,
    *,
    max_age_sec: float = _ANALYTICS_COINGLASS_REFRESH_MAX_AGE_SEC,
) -> Dict[str, Any]:
    try:
        from core.data.coinglass_feature_builder import (
            build_coinglass_overview_payload,
        )  # noqa: PLC0415
    except Exception as exc:
        logger.debug(f"coinglass overview import unavailable for {symbol}: {exc}")
        return {}

    try:
        overview = dict(
            await build_coinglass_overview_payload(
                symbol=symbol, refresh=False, manual=False
            )
            or {}
        )
    except Exception as exc:
        logger.debug(f"coinglass overview unavailable for {symbol}: {exc}")
        return {}

    return overview


def _apply_coinglass_derivatives_overlay(
    payload: Dict[str, Any],
    overview: Dict[str, Any],
) -> Dict[str, Any]:
    out = copy.deepcopy(payload or {})
    overview = dict(overview or {})
    snapshot = dict(overview.get("snapshot") or {})
    snapshot_payload = dict(snapshot.get("payload") or {})
    freshness_sec = _optional_finite_float(overview.get("freshness_sec"))
    context = {
        "provider": "coinglass",
        "preferred_provider": "coinglass",
        "available": bool(overview.get("available")),
        "key_configured": bool(overview.get("key_configured")),
        "freshness_sec": freshness_sec,
        "degraded_reason": str(overview.get("degraded_reason") or "").strip() or None,
        "refresh_recommended": _should_refresh_coinglass_overview(
            overview,
            max_age_sec=_ANALYTICS_COINGLASS_REFRESH_MAX_AGE_SEC,
        ),
        "active_datasets": list(overview.get("active_datasets") or []),
        "snapshot_at": snapshot.get("timestamp"),
    }
    for key in (
        "liquidation_map_total_usd",
        "liquidation_map_above_usd",
        "liquidation_map_below_usd",
        "liquidation_map_pressure_score",
        "liquidation_map_largest_cluster_price",
        "liquidation_map_largest_cluster_usd",
        "orderbook_agg_bid_usd",
        "orderbook_agg_ask_usd",
        "orderbook_agg_imbalance",
        "orderbook_wall_above_usd",
        "orderbook_wall_below_usd",
        "orderbook_wall_above_price",
        "orderbook_wall_below_price",
        "liquidity_heatmap_total_usd",
        "liquidity_heatmap_above_usd",
        "liquidity_heatmap_below_usd",
        "liquidity_wall_nearest_above_price",
        "liquidity_wall_nearest_below_price",
        "liquidity_wall_nearest_above_usd",
        "liquidity_wall_nearest_below_usd",
        "liquidity_void_score",
        "heatmap_pressure_score",
        "top_account_long_short_ratio",
        "top_position_long_short_ratio",
        "net_position",
        "coinbase_premium",
        "option_max_pain",
        "option_put_call_ratio",
        "option_open_interest_usd",
        "option_volume_usd",
        "option_iv",
        "option_iv_skew",
        "option_distance_to_max_pain_pct",
        "option_vs_futures_oi_ratio",
        "gamma_exposure",
        "bitcoin_etf_net_flow",
        "spot_exchange_inflow_usd",
        "spot_exchange_outflow_usd",
        "spot_exchange_netflow_usd",
        "spot_exchange_inflow_count",
        "spot_exchange_outflow_count",
        "spot_netflow_score",
        "exchange_balance_btc",
        "exchange_balance_usd",
        "exchange_balance_change_24h",
        "exchange_balance_change_7d",
        "stablecoin_exchange_balance_usd",
        "stablecoin_netflow_usd",
        "onchain_activity_score",
        "exchange_reserve_pressure_score",
    ):
        value = _optional_finite_float(snapshot_payload.get(key))
        if value is not None:
            context[key] = value
    out["derivatives_context"] = context

    if not context["available"]:
        return out

    snapshot_at = snapshot.get("timestamp")
    symbol = str(overview.get("symbol") or out.get("symbol") or "").strip() or None
    long_short_ratio = _optional_finite_float(snapshot.get("long_short_ratio"))
    if long_short_ratio is None:
        long_short_ratio = _optional_finite_float(
            snapshot_payload.get("long_short_ratio")
        )
    if long_short_ratio is not None and long_short_ratio > 0:
        out["long_short_ratio"] = {
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "symbol": symbol,
            "long_ratio": _optional_finite_float(snapshot.get("long_ratio")),
            "short_ratio": _optional_finite_float(snapshot.get("short_ratio")),
            "long_short_ratio": round(long_short_ratio, 6),
            "sample_size": int(_safe_float(snapshot.get("sample_size"), default=0.0)),
            "timestamp": snapshot_at,
        }

    funding_rate = _optional_finite_float(snapshot.get("funding_rate"))
    if funding_rate is None:
        funding_rate = _optional_finite_float(snapshot.get("funding_rate_oi_weighted"))
    if funding_rate is None:
        funding_rate = _optional_finite_float(
            snapshot_payload.get("funding_rate_oi_weighted")
        )
    if funding_rate is None:
        funding_rate = _optional_finite_float(snapshot_payload.get("funding_rate"))
    if funding_rate is not None:
        existing_funding = dict(out.get("funding_rate") or {})
        out["funding_rate"] = {
            **existing_funding,
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "funding_rate": funding_rate,
            "timestamp": snapshot_at,
        }

    basis_pct = _optional_finite_float(snapshot.get("basis_pct"))
    if basis_pct is None:
        basis_pct = _optional_finite_float(snapshot_payload.get("basis_pct"))
    if basis_pct is not None:
        existing_basis = dict(out.get("spot_futures_basis") or {})
        out["spot_futures_basis"] = {
            **existing_basis,
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "basis_pct": basis_pct,
            "timestamp": snapshot_at,
        }

    option_fields = {
        "max_pain": _optional_finite_float(snapshot_payload.get("option_max_pain")),
        "put_call_ratio": _optional_finite_float(snapshot_payload.get("option_put_call_ratio")),
        "open_interest_usd": _optional_finite_float(snapshot_payload.get("option_open_interest_usd")),
        "volume_usd": _optional_finite_float(snapshot_payload.get("option_volume_usd")),
        "atm_iv": _optional_finite_float(snapshot_payload.get("option_iv")),
        "skew_25d": _optional_finite_float(snapshot_payload.get("option_iv_skew")),
        "distance_to_max_pain_pct": _optional_finite_float(snapshot_payload.get("option_distance_to_max_pain_pct")),
        "option_vs_futures_oi_ratio": _optional_finite_float(snapshot_payload.get("option_vs_futures_oi_ratio")),
        "gamma_exposure": _optional_finite_float(snapshot_payload.get("gamma_exposure")),
    }
    if any(value is not None for value in option_fields.values()):
        existing_options = dict(out.get("options") or {})
        out["options"] = {
            **existing_options,
            **{key: value for key, value in option_fields.items() if value is not None},
            "available": True,
            "source": "coinglass_cache",
            "timestamp": snapshot_at,
        }

    oi_usd = _optional_finite_float(snapshot.get("oi_usd"))
    if oi_usd is None:
        oi_usd = _optional_finite_float(snapshot_payload.get("oi_usd"))
    oi_change_1h = _optional_finite_float(snapshot.get("oi_change_1h"))
    if oi_change_1h is None:
        oi_change_1h = _optional_finite_float(snapshot_payload.get("oi_change_1h_history"))
    if oi_usd is not None or oi_change_1h is not None:
        existing_oi = dict(out.get("oi") or {})
        out["oi"] = {
            **existing_oi,
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "value": oi_usd if oi_usd is not None else existing_oi.get("value"),
            "volume": existing_oi.get("volume"),
            "change_pct_1h": oi_change_1h,
            "timestamp": snapshot_at,
            "sample_size": int(_safe_float(existing_oi.get("sample_size"), default=0.0)),
        }

    taker_imbalance = _optional_finite_float(snapshot.get("taker_buy_sell_imbalance"))
    if taker_imbalance is None:
        taker_imbalance = _optional_finite_float(
            snapshot_payload.get("taker_imbalance_1h")
        )
    if taker_imbalance is None:
        taker_imbalance = _optional_finite_float(
            snapshot_payload.get("taker_imbalance_4h")
        )
    existing_flow = dict(out.get("aggressor_flow") or {})
    existing_flow_count = int(_safe_float(existing_flow.get("count"), default=0.0))
    existing_flow_imbalance = (
        _optional_finite_float(existing_flow.get("imbalance")) or 0.0
    )
    has_exchange_flow = bool(existing_flow.get("available")) and (
        existing_flow_count > 0 or abs(existing_flow_imbalance) > 1e-9
    )
    if taker_imbalance is not None and not has_exchange_flow:
        out["aggressor_flow"] = {
            **existing_flow,
            "available": True,
            "source": "coinglass_cache",
            "error": None,
            "count": existing_flow_count,
            "buy_volume": existing_flow.get("buy_volume"),
            "sell_volume": existing_flow.get("sell_volume"),
            "imbalance": taker_imbalance,
            "timestamp": snapshot_at,
        }

    return out


async def _await_optional_analytics_task(
    task: asyncio.Task,
    *,
    default: Any,
    timeout_sec: float,
    label: str,
) -> Any:
    try:
        return await asyncio.wait_for(task, timeout=max(0.05, float(timeout_sec)))
    except asyncio.TimeoutError:
        if not task.done():
            task.cancel()
        logger.debug(f"{label} timed out after {timeout_sec:.2f}s; using fallback")
        return default
    except Exception as exc:
        logger.debug(f"{label} failed: {exc}")
        return default


async def _bounded_analytics_call(
    coro: Any,
    *,
    default: Any,
    timeout_sec: float,
    label: str,
) -> Any:
    task = asyncio.create_task(coro)
    return await _await_optional_analytics_task(
        task,
        default=default,
        timeout_sec=timeout_sec,
        label=label,
    )


def _public_core_timeout(timeout_sec: float) -> float:
    return max(
        float(timeout_sec),
        float(_ANALYTICS_PUBLIC_CORE_TIMEOUT_FLOOR_SEC),
    )


def _microstructure_has_core_signal(payload: Optional[Dict[str, Any]]) -> bool:
    data = dict(payload or {})
    orderbook = dict(data.get("orderbook") or {})
    aggressor = dict(data.get("aggressor_flow") or {})
    if bool(orderbook.get("bid_depth") or orderbook.get("ask_depth")):
        return True
    if _safe_float(orderbook.get("mid_price")) > 0:
        return True
    if bool(aggressor.get("count")) or abs(_safe_float(aggressor.get("imbalance"))) > 0:
        return True
    for key in ("funding_rate", "spot_futures_basis", "long_short_ratio", "oi"):
        if bool((data.get(key) or {}).get("available")):
            return True
    return False


def _apply_public_derivatives_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(payload or {})
    existing = dict(out.get("derivatives_context") or {})
    if bool(existing.get("available")):
        return out

    active: List[str] = []
    if bool((out.get("funding_rate") or {}).get("available")):
        active.append("funding_rate")
    if bool((out.get("spot_futures_basis") or {}).get("available")):
        active.append("spot_futures_basis")
    if bool((out.get("long_short_ratio") or {}).get("available")):
        active.append("long_short_ratio")
    if bool((out.get("oi") or {}).get("available")):
        active.append("open_interest")
    if bool((out.get("options") or {}).get("available")):
        active.append("options")

    if not active:
        return out

    out["derivatives_context"] = {
        **existing,
        "provider": "exchange_public",
        "preferred_provider": "coinglass",
        "fallback_provider": "exchange_public",
        "fallback_used": True,
        "available": True,
        "status": "public_fallback",
        "key_configured": False,
        "freshness_sec": None,
        "degraded_reason": existing.get("degraded_reason") or "coinglass_unavailable_public_fallback",
        "active_datasets": active,
        "dataset_count": len(active),
        "snapshot_at": out.get("timestamp"),
        "note": "CoinGlass enhanced context unavailable; using exchange public derivatives fields.",
    }
    return out


def _microstructure_live_source_status(payload: Dict[str, Any]) -> str:
    context = dict((payload or {}).get("derivatives_context") or {})
    provider = str(context.get("provider") or "").strip().lower()
    status = str(context.get("status") or "").strip().lower()
    if provider == "coinglass" and bool(context.get("available")):
        return "live_coinglass_preferred"
    if status == "public_fallback" or bool(context.get("fallback_used")):
        return "live_public_fallback"
    if bool(context):
        return "live_coinglass_unavailable"
    return "live"


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if not isinstance(value, datetime):
        return None
    dt = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
    return dt.isoformat().replace("+00:00", "Z")


def _compact_microstructure_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    orderbook = payload.get("orderbook") or {}
    return {
        "timestamp": payload.get("timestamp"),
        "available": bool(payload.get("available", True)),
        "source_error": payload.get("source_error"),
        "orderbook": {
            "best_bid": _safe_float(orderbook.get("best_bid")),
            "best_ask": _safe_float(orderbook.get("best_ask")),
            "mid_price": _safe_float(orderbook.get("mid_price")),
            "spread": _safe_float(orderbook.get("spread")),
            "spread_bps": _safe_float(orderbook.get("spread_bps")),
            "bid_depth": list(orderbook.get("bid_depth") or [])[:10],
            "ask_depth": list(orderbook.get("ask_depth") or [])[:10],
        },
        "large_orders": list(payload.get("large_orders") or [])[:10],
        "iceberg_detection": payload.get("iceberg_detection") or {},
        "aggressor_flow": payload.get("aggressor_flow") or {},
        "long_short_ratio": payload.get("long_short_ratio") or {},
        "funding_rate": payload.get("funding_rate") or {},
        "spot_futures_basis": payload.get("spot_futures_basis") or {},
    }


def _compact_community_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": payload.get("timestamp"),
        "source_error": payload.get("source_error"),
        "twitter_watchlist": list(payload.get("twitter_watchlist") or [])[:10],
        "flow_proxy": payload.get("flow_proxy") or {},
        "security_alerts": payload.get("security_alerts") or {},
        "news_provider": payload.get("news_provider"),
        "news_sources": list(payload.get("news_sources") or [])[:10],
        "announcements": list(payload.get("announcements") or [])[:10],
        "whale_transfers": payload.get("whale_transfers") or {},
    }


def _compact_whale_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "available": bool(payload.get("available", False)),
        "error": payload.get("error"),
        "source_name": payload.get("source_name"),
        "threshold_btc": _safe_float(payload.get("threshold_btc")),
        "btc_price": _safe_float(payload.get("btc_price")),
        "count": int(_safe_float(payload.get("count"))),
        "exchange_flow_summary": dict(payload.get("exchange_flow_summary") or {}),
        "transactions": list(payload.get("transactions") or [])[:10],
    }


def _strip_risk_dashboard_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _strip_cache_runtime_fields(payload)


def _with_risk_dashboard_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return _with_cache_runtime_fields(
        payload,
        cache_hit=cache_hit,
        cache_age_sec=cache_age_sec,
        stale=stale,
        source_status=source_status,
        stale_reason=stale_reason,
        stale_note="Risk dashboard live refresh pending，已回退到最近一次成功快照",
    )


def _strip_microstructure_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _strip_cache_runtime_fields(payload)


def _strip_cacheable_microstructure_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    stripped = _strip_microstructure_runtime_fields(payload)
    if not _microstructure_has_core_signal(stripped):
        raise ValueError("microstructure refresh returned no core signal")
    return stripped


def _with_microstructure_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return _with_cache_runtime_fields(
        payload,
        cache_hit=cache_hit,
        cache_age_sec=cache_age_sec,
        stale=stale,
        source_status=source_status,
        stale_reason=stale_reason,
        stale_note="Microstructure live refresh pending，已回退到最近一次成功快照",
    )


def _strip_community_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _strip_cache_runtime_fields(payload)


def _with_community_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return _with_cache_runtime_fields(
        payload,
        cache_hit=cache_hit,
        cache_age_sec=cache_age_sec,
        stale=stale,
        source_status=source_status,
        stale_reason=stale_reason,
        stale_note="Community overview live refresh pending，已回退到最近一次成功快照",
    )


_ANALYTICS_HISTORY_INGEST_VERSION = "v1"
_ANALYTICS_HISTORY_COLLECTORS = ("microstructure", "community", "whales")
_ANALYTICS_HISTORY_DEFAULT_EXCHANGE = "binance"
_ANALYTICS_HISTORY_DEFAULT_SYMBOL = "BTC/USDT"
# SQLite writes are serialized to avoid lock contention between background workers
# and manual refresh endpoints.
_ANALYTICS_HISTORY_COLLECTION_LOCK = LoopBoundAsyncLock()
_ANALYTICS_HISTORY_HEALTH_CACHE: Dict[str, Dict[str, Any]] = {}
_ANALYTICS_HISTORY_STATUS_CACHE: Dict[str, Dict[str, Any]] = {}
_ANALYTICS_HISTORY_STATUS_LAST: Dict[str, Dict[str, Any]] = {}


def _clip_analytics_error(error: Any, limit: int = 280) -> str:
    text = str(error or "").strip()
    if not text:
        return ""
    return text[:limit]


def _analytics_history_cache_key(
    exchange: str, symbol: str, hours: Optional[int] = None
) -> str:
    ex = str(exchange or _ANALYTICS_HISTORY_DEFAULT_EXCHANGE).strip().lower()
    sym = str(symbol or _ANALYTICS_HISTORY_DEFAULT_SYMBOL).strip().upper()
    if hours is None:
        return f"{ex}|{sym}"
    return f"{ex}|{sym}|{int(hours)}"


def _cache_put(
    cache: Dict[str, Dict[str, Any]], key: str, payload: Dict[str, Any]
) -> None:
    cache[key] = {
        "ts": time.time(),
        "payload": copy.deepcopy(payload or {}),
    }


def _cache_get(
    cache: Dict[str, Dict[str, Any]],
    key: str,
    *,
    max_age_sec: Optional[float] = None,
) -> tuple[Optional[Dict[str, Any]], Optional[float]]:
    row = cache.get(key)
    if not row:
        return None, None
    age_sec = max(0.0, time.time() - float(row.get("ts") or 0.0))
    if max_age_sec is not None and age_sec > float(max_age_sec):
        return None, age_sec
    payload = copy.deepcopy(row.get("payload") or {})
    return payload, age_sec


def _schedule_cache_refresh(
    tasks: Dict[str, asyncio.Task],
    key: str,
    *,
    build_coro: Any,
    cache: Dict[str, Dict[str, Any]],
    strip_payload: Any,
) -> None:
    existing = tasks.get(key)
    if existing is not None and not existing.done():
        return

    async def _runner() -> None:
        try:
            payload = await build_coro()
            if isinstance(payload, dict) and payload:
                _cache_put(cache, key, strip_payload(payload))
        except Exception:
            return
        finally:
            tasks.pop(key, None)

    with contextlib.suppress(RuntimeError):
        tasks[key] = asyncio.create_task(_runner())


def _invalidate_analytics_history_cache(exchange: str, symbol: str) -> None:
    prefix = _analytics_history_cache_key(exchange=exchange, symbol=symbol)
    for cache in (_ANALYTICS_HISTORY_HEALTH_CACHE, _ANALYTICS_HISTORY_STATUS_CACHE):
        stale_keys = [key for key in cache.keys() if str(key).startswith(prefix)]
        for key in stale_keys:
            cache.pop(key, None)


def _status_map_to_collectors(
    status_map: Dict[str, Dict[str, Any]], *, exchange: str, symbol: str
) -> List[Dict[str, Any]]:
    collectors: List[Dict[str, Any]] = []
    exchange_lower = str(exchange or "").lower()
    symbol_text = str(symbol or "")
    for collector in _ANALYTICS_HISTORY_COLLECTORS:
        row = dict(status_map.get(collector) or {})
        if not row:
            continue
        if row.get("exchange") and str(row.get("exchange")).lower() != exchange_lower:
            row["scope_warning"] = f"最近一次采集是 {row.get('exchange')} {row.get('symbol')}"
        elif row.get("symbol") and str(row.get("symbol")) != symbol_text:
            row["scope_warning"] = f"最近一次采集是 {row.get('exchange')} {row.get('symbol')}"
        collectors.append(row)
    return collectors


def _merge_analytics_collector_row(
    collectors: List[Dict[str, Any]],
    row: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged = [dict(item or {}) for item in list(collectors or [])]
    collector_key = str((row or {}).get("collector") or "").strip().lower()
    if not collector_key:
        return merged
    replaced = False
    for idx, item in enumerate(merged):
        if str(item.get("collector") or "").strip().lower() == collector_key:
            merged[idx] = dict(row or {})
            replaced = True
            break
    if not replaced:
        merged.append(dict(row or {}))
    return merged


def _merge_analytics_dataset_row(
    datasets: List[Dict[str, Any]],
    row: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged = [dict(item or {}) for item in list(datasets or [])]
    dataset_key = str((row or {}).get("key") or "").strip().lower()
    if not dataset_key:
        return merged
    replaced = False
    for idx, item in enumerate(merged):
        if str(item.get("key") or "").strip().lower() == dataset_key:
            merged[idx] = dict(row or {})
            replaced = True
            break
    if not replaced:
        merged.append(dict(row or {}))
    return merged


def _merge_analytics_source_row(
    sources: List[Dict[str, Any]],
    row: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged = [dict(item or {}) for item in list(sources or [])]
    stored_as = str((row or {}).get("stored_as") or "").strip().lower()
    if not stored_as:
        return merged
    replaced = False
    for idx, item in enumerate(merged):
        if str(item.get("stored_as") or "").strip().lower() == stored_as:
            merged[idx] = dict(row or {})
            replaced = True
            break
    if not replaced:
        merged.append(dict(row or {}))
    return merged


def _coinglass_overview_to_analytics_status(
    overview: Dict[str, Any],
    *,
    exchange: str,
    symbol: str,
    generated_at: str,
) -> Dict[str, Any]:
    overview = dict(overview or {})
    status_rows = list(overview.get("status") or [])
    active_datasets = list(overview.get("active_datasets") or [])
    freshness_sec = overview.get("freshness_sec")
    degraded_reason = str(overview.get("degraded_reason") or "").strip() or None
    latest_candidates: List[datetime] = []
    snapshot_ts = _safe_dt((overview.get("snapshot") or {}).get("timestamp"))
    if snapshot_ts:
        latest_candidates.append(snapshot_ts)
    for item in status_rows:
        for field in ("last_success_at", "updated_at", "last_attempt_at"):
            parsed = _safe_dt((item or {}).get(field))
            if parsed:
                latest_candidates.append(parsed)
    latest_at = max(latest_candidates) if latest_candidates else None

    status_value = "idle"
    if bool(overview.get("available")) and not degraded_reason:
        status_value = "ok"
    elif (
        degraded_reason
        or active_datasets
        or bool(overview.get("key_configured"))
        or status_rows
    ):
        status_value = "degraded"

    error_text = degraded_reason
    if not error_text:
        for item in status_rows:
            candidate = _clip_analytics_error((item or {}).get("error"))
            if candidate:
                error_text = candidate
                break

    return {
        "collector": "derivatives",
        "title": "衍生品/CoinGlass",
        "exchange": exchange,
        "symbol": str(overview.get("symbol") or symbol),
        "status": status_value,
        "source_name": "coinglass_cache",
        "available": bool(overview.get("available")),
        "cached": bool(overview.get("cached")),
        "finished_at": _utc_iso(latest_at) if latest_at else None,
        "updated_at": generated_at,
        "error": error_text,
        "details": {
            "provider": "coinglass",
            "key_configured": bool(overview.get("key_configured")),
            "active_datasets": active_datasets,
            "freshness_sec": _safe_float(freshness_sec, default=0.0)
            if freshness_sec is not None
            else None,
            "degraded_reason": degraded_reason,
            "quota_headroom": dict(overview.get("quota_headroom") or {}),
            "status_count": len(status_rows),
            "snapshot": dict(overview.get("snapshot") or {}),
        },
    }


async def _build_derivatives_analytics_status(
    *,
    exchange: str,
    symbol: str,
) -> Dict[str, Any]:
    generated_at = _utc_iso(datetime.now(timezone.utc))
    try:
        overview = await _load_preferred_coinglass_overview(symbol=symbol)
    except Exception as exc:
        return {
            "collector": "derivatives",
            "title": "衍生品/CoinGlass",
            "exchange": exchange,
            "symbol": symbol,
            "status": "failed",
            "source_name": "coinglass_cache",
            "available": False,
            "cached": False,
            "finished_at": None,
            "updated_at": generated_at,
            "error": _clip_analytics_error(exc) or "coinglass_status_unavailable",
            "details": {
                "provider": "coinglass",
                "key_configured": False,
                "active_datasets": [],
                "freshness_sec": None,
                "degraded_reason": "coinglass_status_unavailable",
                "quota_headroom": {},
                "status_count": 0,
            },
        }
    return _coinglass_overview_to_analytics_status(
        overview,
        exchange=exchange,
        symbol=symbol,
        generated_at=generated_at,
    )


async def _attach_derivatives_analytics_status(
    payload: Dict[str, Any],
    *,
    exchange: str,
    symbol: str,
) -> Dict[str, Any]:
    out = dict(payload or {})
    derivatives = await _build_derivatives_analytics_status(
        exchange=exchange, symbol=symbol
    )
    out["collectors"] = _merge_analytics_collector_row(
        list(out.get("collectors") or []),
        derivatives,
    )
    out["derivatives"] = dict(derivatives)
    return out


async def _attach_derivatives_analytics_health(
    payload: Dict[str, Any],
    *,
    exchange: str,
    symbol: str,
) -> Dict[str, Any]:
    out = dict(payload or {})
    generated_at = _utc_iso(datetime.now(timezone.utc))
    try:
        overview = await _load_preferred_coinglass_overview(symbol=symbol)
        derivatives_status = _coinglass_overview_to_analytics_status(
            overview,
            exchange=exchange,
            symbol=symbol,
            generated_at=generated_at,
        )
    except Exception as exc:
        overview = {}
        derivatives_status = {
            "collector": "derivatives",
            "title": "衍生品/CoinGlass",
            "exchange": exchange,
            "symbol": symbol,
            "status": "failed",
            "source_name": "coinglass_cache",
            "available": False,
            "cached": False,
            "finished_at": None,
            "updated_at": generated_at,
            "error": _clip_analytics_error(exc) or "coinglass_status_unavailable",
            "details": {
                "provider": "coinglass",
                "key_configured": False,
                "active_datasets": [],
                "freshness_sec": None,
                "degraded_reason": "coinglass_status_unavailable",
                "quota_headroom": {},
                "status_count": 0,
            },
        }

    snapshot = dict(overview.get("snapshot") or {})
    latest_at = (
        str(
            snapshot.get("timestamp") or derivatives_status.get("finished_at") or ""
        ).strip()
        or None
    )
    latest_summary = {
        "capture_status": derivatives_status.get("status"),
        "source_name": derivatives_status.get("source_name"),
        "source_error": derivatives_status.get("error"),
        "funding_rate": snapshot.get("funding_rate"),
        "basis_pct": snapshot.get("basis_pct"),
        "long_short_ratio": snapshot.get("long_short_ratio"),
        "oi_change_1h": snapshot.get("oi_change_1h"),
        "oi_change_24h": snapshot.get("oi_change_24h"),
        "crowding_score": snapshot.get("crowding_score"),
        "squeeze_score": snapshot.get("squeeze_score"),
        "distribution_score": snapshot.get("distribution_score"),
        "taker_buy_sell_imbalance": snapshot.get("taker_buy_sell_imbalance"),
        "orderbook_imbalance_score": snapshot.get("orderbook_imbalance_score"),
        "depth_thinness_score": snapshot.get("depth_thinness_score"),
    }
    derivatives_dataset = {
        "key": "derivatives",
        "title": "衍生品/CoinGlass",
        "count": 1 if latest_at else 0,
        "recent_count": 1 if latest_at else 0,
        "first_at": latest_at,
        "latest_at": latest_at,
        "ok_count": 1 if derivatives_status.get("status") == "ok" and latest_at else 0,
        "degraded_count": 1
        if derivatives_status.get("status") == "degraded" and latest_at
        else 0,
        "failed_count": 1 if derivatives_status.get("status") == "failed" else 0,
        "coverage_hours": 0.0,
        "latest_summary": latest_summary,
    }
    derivatives_recent = []
    if latest_at:
        derivatives_recent.append(
            {
                "timestamp": latest_at,
                "value": _safe_float(snapshot.get("crowding_score"), default=0.0),
                "capture_status": derivatives_status.get("status"),
                "source_name": derivatives_status.get("source_name"),
                "source_error": derivatives_status.get("error"),
                "latency_ms": int(_safe_float(snapshot.get("latency_ms"), default=0.0)),
                "funding_rate": snapshot.get("funding_rate"),
                "basis_pct": snapshot.get("basis_pct"),
                "long_short_ratio": snapshot.get("long_short_ratio"),
                "crowding_score": snapshot.get("crowding_score"),
                "squeeze_score": snapshot.get("squeeze_score"),
                "distribution_score": snapshot.get("distribution_score"),
            }
        )

    storage = dict(out.get("storage") or {})
    tables = list(storage.get("tables") or [])
    for table_name in (
        "analytics_derivatives_snapshots",
        "analytics_market_structure_snapshots",
    ):
        if table_name not in tables:
            tables.append(table_name)
    storage["tables"] = tables
    out["storage"] = storage

    out["sources"] = _merge_analytics_source_row(
        list(out.get("sources") or []),
        {
            "name": "衍生品/CoinGlass",
            "acquisition": "CoinGlass 缓存快照、衍生品聚合指标、后台标准化导出",
            "stored_as": "analytics_derivatives_snapshots",
            "quality_note": "前台只读缓存；快照缺失或过旧时会显式降级，并透出额度头寸与活跃数据集。",
        },
    )
    status_map = dict(out.get("status") or {})
    status_map["derivatives"] = dict(derivatives_status)
    out["status"] = status_map
    out["datasets"] = _merge_analytics_dataset_row(
        list(out.get("datasets") or []),
        derivatives_dataset,
    )
    recent = dict(out.get("recent") or {})
    recent["derivatives"] = derivatives_recent
    out["recent"] = recent
    out["derivatives"] = dict(derivatives_status)

    datasets = list(out.get("datasets") or [])
    out["summary"] = {
        "dataset_count": len(datasets),
        "total_rows": sum(int(item.get("count") or 0) for item in datasets),
        "ready_datasets": sum(
            1 for item in datasets if int(item.get("count") or 0) > 0
        ),
        "latest_at": max(
            (item.get("latest_at") for item in datasets if item.get("latest_at")),
            default=None,
        ),
        "ok_rows": sum(int(item.get("ok_count") or 0) for item in datasets),
        "degraded_rows": sum(int(item.get("degraded_count") or 0) for item in datasets),
        "failed_rows": sum(int(item.get("failed_count") or 0) for item in datasets),
    }
    return out


def _empty_analytics_history_health(
    *,
    exchange: str,
    symbol: str,
    hours: int,
    error: str,
) -> Dict[str, Any]:
    status_map: Dict[str, Dict[str, Any]] = {}
    for collector in _ANALYTICS_HISTORY_COLLECTORS:
        status_map[collector] = {
            "collector": collector,
            "exchange": exchange,
            "symbol": symbol,
            "status": "degraded",
            "error": error,
            "rows_written": 0,
            "started_at": None,
            "finished_at": None,
            "updated_at": _utc_iso(datetime.now(timezone.utc)),
            "details": {"phase": "fallback"},
        }
    return {
        "exchange": exchange,
        "symbol": symbol,
        "hours": int(hours),
        "generated_at": _utc_iso(datetime.now(timezone.utc)),
        "storage": {
            "database": str(
                Path(settings.DATABASE_URL.replace("sqlite+aiosqlite:///", ""))
            ).replace("\\", "/")
            if str(settings.DATABASE_URL).startswith("sqlite+aiosqlite:///")
            else settings.DATABASE_URL,
            "tables": [
                "analytics_microstructure_snapshots",
                "analytics_community_snapshots",
                "analytics_whale_snapshots",
                "analytics_history_ingest_status",
            ],
        },
        "sources": [],
        "status": status_map,
        "summary": {
            "dataset_count": 0,
            "total_rows": 0,
            "ready_datasets": 0,
            "latest_at": None,
            "ok_rows": 0,
            "degraded_rows": 0,
            "failed_rows": 0,
        },
        "datasets": [],
        "recent": {},
        "error": error,
    }


def _status_fallback_analytics_history_health(
    *,
    exchange: str,
    symbol: str,
    hours: int,
    status_map: Dict[str, Dict[str, Any]],
    error: str,
) -> Dict[str, Any]:
    collector_meta = {
        "microstructure": ("微观结构", "analytics_microstructure_snapshots"),
        "community": ("社区资金与公告", "analytics_community_snapshots"),
        "whales": ("巨鲸转账", "analytics_whale_snapshots"),
    }
    datasets: List[Dict[str, Any]] = []
    for collector in _ANALYTICS_HISTORY_COLLECTORS:
        row = dict(status_map.get(collector) or {})
        if not row:
            continue
        status = str(row.get("status") or "degraded")
        details = dict(row.get("details") or {})
        latest_summary = dict(details.get("summary") or {})
        latest_at = row.get("finished_at") or row.get("updated_at")
        rows_written = int(_safe_float(row.get("rows_written"), default=0.0))
        if rows_written <= 0:
            rows_written = 1
        title, _ = collector_meta.get(collector, (collector, ""))
        datasets.append(
            {
                "key": collector,
                "title": title,
                "count": rows_written,
                "recent_count": rows_written,
                "first_at": latest_at,
                "latest_at": latest_at,
                "ok_count": 1 if status == "ok" else 0,
                "degraded_count": 1 if status == "degraded" else 0,
                "failed_count": 1 if status == "failed" else 0,
                "coverage_hours": 0.0,
                "latest_summary": latest_summary,
            }
        )

    latest_at = max(
        (item.get("latest_at") for item in datasets if item.get("latest_at")),
        default=None,
    )
    return {
        "exchange": exchange,
        "symbol": symbol,
        "hours": int(hours),
        "generated_at": _utc_iso(datetime.now(timezone.utc)),
        "storage": {
            "database": str(
                Path(settings.DATABASE_URL.replace("sqlite+aiosqlite:///", ""))
            ).replace("\\", "/")
            if str(settings.DATABASE_URL).startswith("sqlite+aiosqlite:///")
            else settings.DATABASE_URL,
            "tables": [
                "analytics_microstructure_snapshots",
                "analytics_community_snapshots",
                "analytics_whale_snapshots",
                "analytics_history_ingest_status",
            ],
        },
        "sources": [],
        "status": status_map,
        "summary": {
            "dataset_count": len(datasets),
            "total_rows": sum(int(item.get("count") or 0) for item in datasets),
            "ready_datasets": sum(
                1 for item in datasets if int(item.get("count") or 0) > 0
            ),
            "latest_at": latest_at,
            "ok_rows": sum(int(item.get("ok_count") or 0) for item in datasets),
            "degraded_rows": sum(
                int(item.get("degraded_count") or 0) for item in datasets
            ),
            "failed_rows": sum(int(item.get("failed_count") or 0) for item in datasets),
        },
        "datasets": datasets,
        "recent": {},
        "error": error,
        "fallback_mode": "status_snapshot",
    }


def _calc_buy_sell_ratio(imbalance: Any) -> tuple[float, float]:
    value = _safe_float(imbalance)
    buy_ratio = max(0.0, min(1.0, (1.0 + value) / 2.0))
    sell_ratio = max(0.0, min(1.0, (1.0 - value) / 2.0))
    return buy_ratio, sell_ratio


def _community_source_name(payload: Dict[str, Any]) -> str:
    parts: List[str] = ["proxy_layer"]
    if list(payload.get("announcements") or []):
        parts.append("official_announcements")
    news_provider = str(payload.get("news_provider") or "").strip().lower()
    if news_provider:
        parts.append(news_provider)
    source = (
        str(((payload.get("security_alerts") or {}).get("source")) or "")
        .strip()
        .lower()
    )
    if source:
        parts.append(source)
    return "+".join(dict.fromkeys(parts))


def _micro_quality(payload: Dict[str, Any], latency_ms: int) -> Dict[str, Any]:
    orderbook = payload.get("orderbook") or {}
    funding = payload.get("funding_rate") or {}
    basis = payload.get("spot_futures_basis") or {}
    source_error = _clip_analytics_error(payload.get("source_error"))
    available = bool(payload.get("available", True))
    funding_available = bool(funding.get("available"))
    basis_available = bool(basis.get("available"))
    has_core = (
        _safe_float(orderbook.get("mid_price")) > 0
        and _safe_float(orderbook.get("spread_bps")) >= 0
    )
    if not available or not has_core:
        capture_status = "failed" if source_error else "degraded"
    elif source_error or not (funding_available and basis_available):
        capture_status = "degraded"
    else:
        capture_status = "ok"
    return {
        "capture_status": capture_status,
        "source_error": source_error,
        "source_name": "exchange_public",
        "latency_ms": int(max(0, latency_ms)),
        "ingest_version": _ANALYTICS_HISTORY_INGEST_VERSION,
        "source_ok": capture_status == "ok",
    }


def _community_quality(payload: Dict[str, Any], latency_ms: int) -> Dict[str, Any]:
    source_error = _clip_analytics_error(payload.get("source_error"))
    capture_status = "degraded" if source_error else "ok"
    return {
        "capture_status": capture_status,
        "source_error": source_error,
        "source_name": _community_source_name(payload),
        "latency_ms": int(max(0, latency_ms)),
        "ingest_version": _ANALYTICS_HISTORY_INGEST_VERSION,
    }


def _whale_quality(payload: Dict[str, Any], latency_ms: int) -> Dict[str, Any]:
    source_error = _clip_analytics_error(payload.get("error"))
    available = bool(payload.get("available", False))
    transactions = list(payload.get("transactions") or [])
    count = int(_safe_float(payload.get("count")))
    has_core = available and (count > 0 or bool(transactions))
    source_name = (
        str(payload.get("source_name") or "public_chain_proxy").strip()
        or "public_chain_proxy"
    )
    if has_core and not source_error:
        capture_status = "ok"
    elif has_core:
        capture_status = "degraded"
    elif source_error:
        capture_status = "failed"
    else:
        capture_status = "degraded"
    return {
        "capture_status": capture_status,
        "source_error": source_error,
        "source_name": source_name,
        "latency_ms": int(max(0, latency_ms)),
        "ingest_version": _ANALYTICS_HISTORY_INGEST_VERSION,
    }


async def _record_analytics_ingest_status(
    *,
    collector: str,
    exchange: str,
    symbol: str,
    status: str,
    error: str = "",
    rows_written: int = 0,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    async with async_session_maker() as session:
        row = (
            (
                await session.execute(
                    select(AnalyticsHistoryIngestStatus).where(
                        AnalyticsHistoryIngestStatus.collector == collector
                    )
                )
            )
            .scalars()
            .first()
        )
        if not row:
            row = AnalyticsHistoryIngestStatus(
                collector=collector,
                exchange=exchange,
                symbol=symbol,
            )
            session.add(row)
        row.exchange = exchange
        row.symbol = symbol
        row.status = str(status or "idle")
        row.error = _clip_analytics_error(error)
        row.rows_written = int(max(0, rows_written))
        if started_at is not None:
            row.started_at = started_at
        if finished_at is not None:
            row.finished_at = finished_at
        row.details = dict(details or {})
        await session.commit()
        result = {
            "collector": collector,
            "exchange": exchange,
            "symbol": symbol,
            "status": row.status,
            "error": row.error,
            "rows_written": row.rows_written,
            "started_at": _utc_iso(row.started_at),
            "finished_at": _utc_iso(row.finished_at),
            "updated_at": _utc_iso(row.updated_at),
            "details": dict(row.details or {}),
        }
        _ANALYTICS_HISTORY_STATUS_LAST[str(collector)] = dict(result)
        return result


async def _load_analytics_ingest_status_map() -> Dict[str, Dict[str, Any]]:
    async with async_session_maker() as session:
        rows = (
            (
                await session.execute(
                    select(AnalyticsHistoryIngestStatus).where(
                        AnalyticsHistoryIngestStatus.collector.in_(
                            _ANALYTICS_HISTORY_COLLECTORS
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        out[str(row.collector)] = {
            "collector": str(row.collector),
            "exchange": str(row.exchange or ""),
            "symbol": str(row.symbol or ""),
            "status": str(row.status or "idle"),
            "error": str(row.error or ""),
            "rows_written": int(row.rows_written or 0),
            "started_at": _utc_iso(row.started_at),
            "finished_at": _utc_iso(row.finished_at),
            "updated_at": _utc_iso(row.updated_at),
            "details": dict(row.details or {}),
        }
    for collector in _ANALYTICS_HISTORY_COLLECTORS:
        out.setdefault(
            collector,
            {
                "collector": collector,
                "exchange": _ANALYTICS_HISTORY_DEFAULT_EXCHANGE,
                "symbol": _ANALYTICS_HISTORY_DEFAULT_SYMBOL,
                "status": "idle",
                "error": "",
                "rows_written": 0,
                "started_at": None,
                "finished_at": None,
                "updated_at": None,
                "details": {},
            },
        )
    return out


async def _persist_microstructure_snapshot(
    *,
    exchange: str,
    symbol: str,
    payload: Dict[str, Any],
    latency_ms: int,
) -> Dict[str, Any]:
    captured_at = _utc_now_naive()
    orderbook = payload.get("orderbook") or {}
    aggressor_flow = payload.get("aggressor_flow") or {}
    funding = payload.get("funding_rate") or {}
    basis = payload.get("spot_futures_basis") or {}
    buy_ratio, sell_ratio = _calc_buy_sell_ratio(aggressor_flow.get("imbalance"))
    quality = _micro_quality(payload, latency_ms)
    row = AnalyticsMicrostructureSnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        source_ok=bool(quality.get("source_ok")),
        capture_status=str(quality.get("capture_status")),
        source_error=str(quality.get("source_error")),
        source_name=str(quality.get("source_name")),
        latency_ms=int(quality.get("latency_ms") or 0),
        ingest_version=str(quality.get("ingest_version")),
        spread_bps=_safe_float(orderbook.get("spread_bps")),
        mid_price=_safe_float(orderbook.get("mid_price")),
        order_flow_imbalance=_safe_float(aggressor_flow.get("imbalance")),
        buy_ratio=buy_ratio,
        sell_ratio=sell_ratio,
        large_order_count=len(list(payload.get("large_orders") or [])),
        iceberg_candidates=int(
            _safe_float((payload.get("iceberg_detection") or {}).get("candidate_count"))
        ),
        funding_rate=_safe_float(funding.get("funding_rate"))
        if bool(funding.get("available"))
        else None,
        basis_pct=_safe_float(basis.get("basis_pct"))
        if bool(basis.get("available"))
        else None,
        payload=_compact_microstructure_payload(payload),
    )
    async with async_session_maker() as session:
        session.add(row)
        await session.commit()
    return {
        "id": row.id,
        "captured_at": _utc_iso(captured_at),
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": int(row.latency_ms or 0),
        "rows_written": 1,
        "summary": {
            "available": bool(payload.get("available", True)),
            "spread_bps": _safe_float(orderbook.get("spread_bps")),
            "funding_available": bool(funding.get("available")),
            "basis_available": bool(basis.get("available")),
        },
    }


async def _persist_community_snapshot(
    *,
    exchange: str,
    symbol: str,
    payload: Dict[str, Any],
    latency_ms: int,
) -> Dict[str, Any]:
    captured_at = _utc_now_naive()
    flow = payload.get("flow_proxy") or {}
    security_alerts = payload.get("security_alerts") or {}
    announcements = list(payload.get("announcements") or [])
    buy_ratio, sell_ratio = _calc_buy_sell_ratio(flow.get("imbalance"))
    quality = _community_quality(payload, latency_ms)
    row = AnalyticsCommunitySnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        capture_status=str(quality.get("capture_status")),
        source_error=str(quality.get("source_error")),
        source_name=str(quality.get("source_name")),
        latency_ms=int(quality.get("latency_ms") or 0),
        ingest_version=str(quality.get("ingest_version")),
        flow_imbalance=_safe_float(flow.get("imbalance")),
        buy_ratio=buy_ratio,
        sell_ratio=sell_ratio,
        announcement_count=len(announcements),
        security_alert_count=len(list(security_alerts.get("events") or [])),
        payload=_compact_community_payload(payload),
    )
    async with async_session_maker() as session:
        session.add(row)
        await session.commit()
    return {
        "id": row.id,
        "captured_at": _utc_iso(captured_at),
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": int(row.latency_ms or 0),
        "rows_written": 1,
        "summary": {
            "announcement_count": len(announcements),
            "security_alert_count": len(list(security_alerts.get("events") or [])),
            "flow_imbalance": _safe_float(flow.get("imbalance")),
        },
    }


async def _persist_whale_snapshot(
    *,
    exchange: str,
    symbol: str,
    payload: Dict[str, Any],
    latency_ms: int,
) -> Dict[str, Any]:
    captured_at = _utc_now_naive()
    transactions = list(payload.get("transactions") or [])
    quality = _whale_quality(payload, latency_ms)
    row = AnalyticsWhaleSnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        capture_status=str(quality.get("capture_status")),
        source_error=str(quality.get("source_error")),
        source_name=str(quality.get("source_name")),
        latency_ms=int(quality.get("latency_ms") or 0),
        ingest_version=str(quality.get("ingest_version")),
        whale_count=int(_safe_float(payload.get("count"))),
        total_btc=round(sum(_safe_float(item.get("btc")) for item in transactions), 6),
        max_btc=round(
            max((_safe_float(item.get("btc")) for item in transactions), default=0.0), 6
        ),
        payload=_compact_whale_payload(payload),
    )
    async with async_session_maker() as session:
        session.add(row)
        await session.commit()
    return {
        "id": row.id,
        "captured_at": _utc_iso(captured_at),
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": int(row.latency_ms or 0),
        "rows_written": 1,
        "summary": {
            "available": bool(payload.get("available", False)),
            "count": int(_safe_float(payload.get("count"))),
            "total_btc": round(
                sum(_safe_float(item.get("btc")) for item in transactions), 6
            ),
        },
    }


async def _persist_analytics_snapshots(
    *,
    exchange: str,
    symbol: str,
    microstructure: Dict[str, Any],
    community: Dict[str, Any],
) -> Dict[str, Any]:
    captured_at = _utc_now_naive()
    flow = community.get("flow_proxy") or {}
    whales = community.get("whale_transfers") or {}
    security_alerts = community.get("security_alerts") or {}
    announcements = list(community.get("announcements") or [])
    whale_transactions = list(whales.get("transactions") or [])
    micro_orderbook = microstructure.get("orderbook") or {}
    aggressor_flow = microstructure.get("aggressor_flow") or {}
    funding = microstructure.get("funding_rate") or {}
    basis = microstructure.get("spot_futures_basis") or {}

    micro_row = AnalyticsMicrostructureSnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        source_ok=bool(microstructure.get("available", True))
        and not bool(microstructure.get("source_error")),
        spread_bps=_safe_float(micro_orderbook.get("spread_bps")),
        mid_price=_safe_float(micro_orderbook.get("mid_price")),
        order_flow_imbalance=_safe_float(aggressor_flow.get("imbalance")),
        buy_ratio=max(
            0.0, min(1.0, (1.0 + _safe_float(aggressor_flow.get("imbalance"))) / 2.0)
        ),
        sell_ratio=max(
            0.0, min(1.0, (1.0 - _safe_float(aggressor_flow.get("imbalance"))) / 2.0)
        ),
        large_order_count=len(list(microstructure.get("large_orders") or [])),
        iceberg_candidates=int(
            _safe_float(
                (microstructure.get("iceberg_detection") or {}).get("candidate_count")
            )
        ),
        funding_rate=(
            _safe_float(funding.get("funding_rate"))
            if bool(funding.get("available"))
            else None
        ),
        basis_pct=(
            _safe_float(basis.get("basis_pct"))
            if bool(basis.get("available"))
            else None
        ),
        payload=_compact_microstructure_payload(microstructure),
    )

    community_row = AnalyticsCommunitySnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        flow_imbalance=_safe_float(flow.get("imbalance")),
        buy_ratio=max(0.0, min(1.0, (1.0 + _safe_float(flow.get("imbalance"))) / 2.0)),
        sell_ratio=max(0.0, min(1.0, (1.0 - _safe_float(flow.get("imbalance"))) / 2.0)),
        announcement_count=len(announcements),
        security_alert_count=len(list(security_alerts.get("events") or [])),
        payload=_compact_community_payload(community),
    )

    whale_row = AnalyticsWhaleSnapshot(
        timestamp=captured_at,
        exchange=exchange,
        symbol=symbol,
        whale_count=int(_safe_float(whales.get("count"))),
        total_btc=round(
            sum(_safe_float(item.get("btc")) for item in whale_transactions), 6
        ),
        max_btc=round(
            max(
                (_safe_float(item.get("btc")) for item in whale_transactions),
                default=0.0,
            ),
            6,
        ),
        payload=_compact_whale_payload(whales),
    )

    async with async_session_maker() as session:
        session.add_all([micro_row, community_row, whale_row])
        await session.commit()

    return {
        "captured_at": _utc_iso(captured_at),
        "microstructure_id": micro_row.id,
        "community_id": community_row.id,
        "whale_id": whale_row.id,
    }


def _analytics_fallback_microstructure(
    exchange: str, symbol: str, error: str
) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exchange": exchange,
        "symbol": symbol,
        "available": False,
        "source_error": str(error or "microstructure unavailable"),
        "orderbook": {"spread_bps": 0.0, "mid_price": 0.0, "bids": [], "asks": []},
        "aggressor_flow": {"imbalance": 0.0, "buy_volume": 0.0, "sell_volume": 0.0},
        "large_orders": [],
        "iceberg_detection": {"candidate_count": 0},
        "long_short_ratio": {"available": False},
        "funding_rate": {"available": False},
        "spot_futures_basis": {"available": False},
    }


def _analytics_fallback_community(
    exchange: str, symbol: str, error: str
) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "exchange": exchange,
        "source_error": str(error or "community unavailable"),
        "twitter_watchlist": [],
        "flow_proxy": {"imbalance": 0.0, "buy_volume": 0.0, "sell_volume": 0.0},
        "whale_transfers": {
            "available": False,
            "count": 0,
            "transactions": [],
            "error": str(error or ""),
        },
        "security_alerts": {
            "available": False,
            "source": "unavailable",
            "events": [],
            "note": "社区/安全告警源暂不可用，当前未返回安全事件。",
        },
        "announcements": [],
    }


def _analytics_fallback_whales(error: str) -> Dict[str, Any]:
    return {
        "available": False,
        "error": str(error or "whale transfers unavailable"),
        "threshold_btc": _ANALYTICS_WHALE_MIN_BTC,
        "btc_price": 0.0,
        "count": 0,
        "transactions": [],
    }


async def _collect_analytics_component(
    *,
    label: str,
    timeout_sec: float,
    coro,
    fallback_payload: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(coro, timeout=max(1.0, float(timeout_sec)))
    except Exception as exc:
        payload = dict(fallback_payload or {})
        exc_text = str(exc).strip() or type(exc).__name__
        payload.setdefault("source_error", f"{label} failed: {exc_text}")
        logger.warning(f"{label} analytics fallback: {exc_text}")
        return payload


async def _collect_analytics_component_with_meta(
    *,
    label: str,
    timeout_sec: float,
    coro,
    fallback_payload: Dict[str, Any],
) -> Dict[str, Any]:
    started = time.perf_counter()
    payload = await _collect_analytics_component(
        label=label,
        timeout_sec=timeout_sec,
        coro=coro,
        fallback_payload=fallback_payload,
    )
    return {
        "payload": payload,
        "latency_ms": int(round((time.perf_counter() - started) * 1000)),
    }


def _serialize_analytics_series_row(
    row: Any, metric: str, extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    payload = dict(extra or {})
    payload.update(
        {
            "timestamp": _utc_iso(getattr(row, "timestamp", None)),
            "value": _safe_float(getattr(row, metric, 0.0)),
        }
    )
    return payload


def _normalize_analytics_collectors(collectors: Optional[Any]) -> List[str]:
    if collectors is None:
        return list(_ANALYTICS_HISTORY_COLLECTORS)
    if isinstance(collectors, str):
        values = [item.strip().lower() for item in str(collectors).split(",")]
    else:
        values = [str(item or "").strip().lower() for item in list(collectors or [])]
    normalized = [item for item in values if item in _ANALYTICS_HISTORY_COLLECTORS]
    return normalized or list(_ANALYTICS_HISTORY_COLLECTORS)


async def run_analytics_history_collection(
    *,
    exchange: str = _ANALYTICS_HISTORY_DEFAULT_EXCHANGE,
    symbol: str = _ANALYTICS_HISTORY_DEFAULT_SYMBOL,
    depth_limit: int = 80,
    collectors: Optional[Any] = None,
) -> Dict[str, Any]:
    async with _ANALYTICS_HISTORY_COLLECTION_LOCK:
        selected = _normalize_analytics_collectors(collectors)
        started_at = _utc_now_naive()
        started_map = {collector: _utc_now_naive() for collector in selected}
        for collector in selected:
            await _record_analytics_ingest_status(
                collector=collector,
                exchange=exchange,
                symbol=symbol,
                status="running",
                error="",
                rows_written=0,
                started_at=started_map[collector],
                finished_at=None,
                details={"phase": "collecting"},
            )

        jobs: Dict[str, Any] = {}
        if "microstructure" in selected:
            jobs["microstructure"] = _collect_analytics_component_with_meta(
                label="microstructure",
                timeout_sec=_ANALYTICS_COLLECTOR_TIMEOUT_SEC,
                coro=_build_microstructure_history_payload(
                    exchange=exchange, symbol=symbol, depth_limit=depth_limit
                ),
                fallback_payload=_analytics_fallback_microstructure(
                    exchange=exchange, symbol=symbol, error="微观结构抓取超时"
                ),
            )
        if "community" in selected:
            jobs["community"] = _collect_analytics_component_with_meta(
                label="community",
                timeout_sec=_ANALYTICS_COLLECTOR_TIMEOUT_SEC,
                coro=_build_community_history_payload(symbol=symbol, exchange=exchange),
                fallback_payload=_analytics_fallback_community(
                    exchange=exchange, symbol=symbol, error="社区/公告抓取超时"
                ),
            )
        if "whales" in selected:
            jobs["whales"] = _collect_analytics_component_with_meta(
                label="whales",
                timeout_sec=_ANALYTICS_COLLECTOR_TIMEOUT_SEC,
                coro=_build_whales_history_payload(
                    symbol=symbol,
                    min_btc=_ANALYTICS_WHALE_MIN_BTC,
                ),
                fallback_payload=_analytics_fallback_whales("巨鲸抓取超时"),
            )

        job_names = list(jobs.keys())
        job_results = await asyncio.gather(*jobs.values()) if jobs else []
        resolved = {name: result for name, result in zip(job_names, job_results)}
        collector_results: Dict[str, Dict[str, Any]] = {}

        async def _finalize_collector(
            collector: str, save_result: Dict[str, Any]
        ) -> None:
            collector_results[collector] = dict(save_result or {})
            await _record_analytics_ingest_status(
                collector=collector,
                exchange=exchange,
                symbol=symbol,
                status=str(save_result.get("capture_status") or "ok"),
                error=str(save_result.get("source_error") or ""),
                rows_written=int(save_result.get("rows_written") or 0),
                started_at=started_map.get(collector),
                finished_at=_utc_now_naive(),
                details={
                    "source_name": save_result.get("source_name"),
                    "latency_ms": int(save_result.get("latency_ms") or 0),
                    "captured_at": save_result.get("captured_at"),
                    "summary": dict(save_result.get("summary") or {}),
                },
            )

        try:
            if "microstructure" in selected:
                meta = resolved.get("microstructure") or {}
                await _finalize_collector(
                    "microstructure",
                    await _persist_microstructure_snapshot(
                        exchange=exchange,
                        symbol=symbol,
                        payload=dict(meta.get("payload") or {}),
                        latency_ms=int(meta.get("latency_ms") or 0),
                    ),
                )
            if "community" in selected:
                meta = resolved.get("community") or {}
                community_payload = dict(meta.get("payload") or {})
                await _finalize_collector(
                    "community",
                    await _persist_community_snapshot(
                        exchange=exchange,
                        symbol=symbol,
                        payload=community_payload,
                        latency_ms=int(meta.get("latency_ms") or 0),
                    ),
                )
            if "whales" in selected:
                whale_meta = resolved.get("whales") or {}
                whale_payload = dict(whale_meta.get("payload") or {})
                whale_latency_ms = int(whale_meta.get("latency_ms") or 0)
                await _finalize_collector(
                    "whales",
                    await _persist_whale_snapshot(
                        exchange=exchange,
                        symbol=symbol,
                        payload=whale_payload,
                        latency_ms=whale_latency_ms,
                    ),
                )
        except Exception as exc:
            error_text = _clip_analytics_error(exc)
            for collector in selected:
                if collector in collector_results:
                    continue
                await _record_analytics_ingest_status(
                    collector=collector,
                    exchange=exchange,
                    symbol=symbol,
                    status="failed",
                    error=error_text,
                    rows_written=0,
                    started_at=started_map.get(collector),
                    finished_at=_utc_now_naive(),
                    details={"phase": "persist_failed"},
                )
            raise

        finished_at = _utc_now_naive()
        return {
            "success": True,
            "exchange": exchange,
            "symbol": symbol,
            "collectors": selected,
            "started_at": _utc_iso(started_at),
            "finished_at": _utc_iso(finished_at),
            "rows_written": sum(
                int(item.get("rows_written") or 0)
                for item in collector_results.values()
            ),
            "results": collector_results,
        }


async def _build_analytics_history_health(
    *,
    exchange: str,
    symbol: str,
    hours: int,
) -> Dict[str, Any]:
    cutoff = _utc_now_naive() - timedelta(hours=hours)

    async with async_session_maker() as session:
        datasets: List[Dict[str, Any]] = []
        recent: Dict[str, List[Dict[str, Any]]] = {}

        specs = [
            (
                "microstructure",
                "微观结构",
                AnalyticsMicrostructureSnapshot,
                "spread_bps",
                lambda row: {
                    "source_ok": bool(row.source_ok),
                    "capture_status": str(getattr(row, "capture_status", "ok") or "ok"),
                    "source_name": str(
                        getattr(row, "source_name", "exchange_public")
                        or "exchange_public"
                    ),
                    "source_error": str(getattr(row, "source_error", "") or ""),
                    "latency_ms": int(getattr(row, "latency_ms", 0) or 0),
                    "ingest_version": str(getattr(row, "ingest_version", "v1") or "v1"),
                    "funding_rate": row.funding_rate,
                    "basis_pct": row.basis_pct,
                    "large_order_count": int(row.large_order_count or 0),
                    "iceberg_candidates": int(row.iceberg_candidates or 0),
                    "mid_price": _safe_float(row.mid_price),
                },
            ),
            (
                "community",
                "社区资金与公告",
                AnalyticsCommunitySnapshot,
                "flow_imbalance",
                lambda row: {
                    "capture_status": str(getattr(row, "capture_status", "ok") or "ok"),
                    "source_name": str(
                        getattr(row, "source_name", "proxy_layer") or "proxy_layer"
                    ),
                    "source_error": str(getattr(row, "source_error", "") or ""),
                    "latency_ms": int(getattr(row, "latency_ms", 0) or 0),
                    "ingest_version": str(getattr(row, "ingest_version", "v1") or "v1"),
                    "announcement_count": int(row.announcement_count or 0),
                    "security_alert_count": int(row.security_alert_count or 0),
                    "buy_ratio": _safe_float(row.buy_ratio),
                    "sell_ratio": _safe_float(row.sell_ratio),
                },
            ),
            (
                "whales",
                "巨鲸转账",
                AnalyticsWhaleSnapshot,
                "whale_count",
                lambda row: {
                    "capture_status": str(getattr(row, "capture_status", "ok") or "ok"),
                    "source_name": str(
                        getattr(row, "source_name", "public_chain_proxy")
                        or "public_chain_proxy"
                    ),
                    "source_error": str(getattr(row, "source_error", "") or ""),
                    "latency_ms": int(getattr(row, "latency_ms", 0) or 0),
                    "ingest_version": str(getattr(row, "ingest_version", "v1") or "v1"),
                    "whale_count": int(row.whale_count or 0),
                    "total_btc": _safe_float(row.total_btc),
                    "max_btc": _safe_float(row.max_btc),
                },
            ),
        ]

        for key, title, model, metric, extra_builder in specs:
            filters = [model.exchange == exchange, model.symbol == symbol]
            agg_row = (
                await session.execute(
                    select(
                        func.count(model.id),
                        func.min(model.timestamp),
                        func.max(model.timestamp),
                        func.sum(case((model.capture_status == "ok", 1), else_=0)),
                        func.sum(
                            case((model.capture_status == "degraded", 1), else_=0)
                        ),
                        func.sum(case((model.capture_status == "failed", 1), else_=0)),
                    ).where(*filters)
                )
            ).one_or_none()
            total = int(_safe_float(agg_row[0] if agg_row else 0, default=0.0))
            first_ts = agg_row[1] if agg_row else None
            latest_ts = agg_row[2] if agg_row else None
            ok_count = int(_safe_float(agg_row[3] if agg_row else 0, default=0.0))
            degraded_count = int(_safe_float(agg_row[4] if agg_row else 0, default=0.0))
            failed_count = int(_safe_float(agg_row[5] if agg_row else 0, default=0.0))
            recent_count = int(
                _safe_float(
                    await session.scalar(
                        select(func.count())
                        .select_from(model)
                        .where(*filters, model.timestamp >= cutoff)
                    ),
                    default=0.0,
                )
            )
            latest_row = (
                (
                    await session.execute(
                        select(model)
                        .where(*filters)
                        .order_by(model.timestamp.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            recent_rows = (
                (
                    await session.execute(
                        select(model)
                        .where(*filters)
                        .order_by(model.timestamp.desc())
                        .limit(24)
                    )
                )
                .scalars()
                .all()
            )
            recent[key] = [
                _serialize_analytics_series_row(row, metric, extra_builder(row))
                for row in reversed(list(recent_rows or []))
            ]
            datasets.append(
                {
                    "key": key,
                    "title": title,
                    "count": total,
                    "recent_count": recent_count,
                    "first_at": _utc_iso(first_ts) if first_ts else None,
                    "latest_at": _utc_iso(latest_ts) if latest_ts else None,
                    "ok_count": ok_count,
                    "degraded_count": degraded_count,
                    "failed_count": failed_count,
                    "coverage_hours": round(
                        ((latest_ts - first_ts).total_seconds() / 3600.0)
                        if first_ts and latest_ts
                        else 0.0,
                        2,
                    ),
                    "latest_summary": extra_builder(latest_row) if latest_row else {},
                }
            )

    now_utc = datetime.now(timezone.utc)
    try:
        status_map = await asyncio.wait_for(
            _load_analytics_ingest_status_map(),
            timeout=max(1.0, _ANALYTICS_HISTORY_STATUS_READ_TIMEOUT_SEC),
        )
    except Exception as exc:
        logger.warning(
            f"analytics history status-map read degraded: {_clip_analytics_error(exc)}"
        )
        status_map = {}
    return {
        "exchange": exchange,
        "symbol": symbol,
        "hours": hours,
        "generated_at": _utc_iso(now_utc),
        "storage": {
            "database": str(
                Path(settings.DATABASE_URL.replace("sqlite+aiosqlite:///", ""))
            ).replace("\\", "/")
            if str(settings.DATABASE_URL).startswith("sqlite+aiosqlite:///")
            else settings.DATABASE_URL,
            "tables": [
                "analytics_microstructure_snapshots",
                "analytics_community_snapshots",
                "analytics_whale_snapshots",
                "analytics_history_ingest_status",
            ],
        },
        "sources": [
            {
                "name": "微观结构",
                "acquisition": "交易所订单簿、逐笔成交、资金费率、现货/合约基差",
                "stored_as": "analytics_microstructure_snapshots",
                "quality_note": "基础层为公开交易所接口；资金费率或基差缺失时允许降级入库。",
            },
            {
                "name": "社区/公告",
                "acquisition": "逐笔成交流向代理、官方公告源、内部安全事件占位源",
                "stored_as": "analytics_community_snapshots",
                "quality_note": "这是社区代理层，不是完整社交媒体舆情库；占位源会显式标记。",
            },
            {
                "name": "巨鲸",
                "acquisition": "Blockchain 未确认交易 + Binance BTC 价格估值",
                "stored_as": "analytics_whale_snapshots",
                "quality_note": "这是公开链上大额转账代理，不是地址标签级鲸鱼画像。",
            },
        ],
        "status": status_map,
        "summary": {
            "dataset_count": len(datasets),
            "total_rows": sum(int(item.get("count") or 0) for item in datasets),
            "ready_datasets": sum(
                1 for item in datasets if int(item.get("count") or 0) > 0
            ),
            "latest_at": max(
                (item.get("latest_at") for item in datasets if item.get("latest_at")),
                default=None,
            ),
            "ok_rows": sum(int(item.get("ok_count") or 0) for item in datasets),
            "degraded_rows": sum(
                int(item.get("degraded_count") or 0) for item in datasets
            ),
            "failed_rows": sum(int(item.get("failed_count") or 0) for item in datasets),
        },
        "datasets": datasets,
        "recent": recent,
    }


def _fresh_live_position_snapshot(force_refresh: bool) -> Optional[Dict[str, Any]]:
    if force_refresh:
        return None
    cached = _LIVE_POSITION_SNAPSHOT_CACHE.get("data") or {}
    cached_ts = float(_LIVE_POSITION_SNAPSHOT_CACHE.get("ts") or 0.0)
    if cached and (time.time() - cached_ts) <= _LIVE_POSITION_SNAPSHOT_TTL_SEC:
        return dict(cached)
    return None


async def _collect_live_position_snapshot(
    force_refresh: bool = False,
) -> Dict[str, Any]:
    if execution_engine.is_paper_mode():
        return {
            "unrealized_pnl_usd": 0.0,
            "system_unrealized_pnl_usd": 0.0,
            "external_unrealized_pnl_usd": 0.0,
            "position_count": 0,
            "system_position_count": 0,
            "external_position_count": 0,
            "by_exchange": {},
            "distribution": {},
            "system_distribution": {},
            "external_distribution": {},
        }

    fresh = _fresh_live_position_snapshot(force_refresh)
    if fresh is not None:
        return fresh

    async with _LIVE_POSITION_SNAPSHOT_LOCK:
        # Re-check after acquiring: another caller may have refreshed while
        # we waited on the lock.
        fresh = _fresh_live_position_snapshot(force_refresh)
        if fresh is not None:
            return fresh
        return await _collect_live_position_snapshot_refresh()


async def _collect_live_position_snapshot_refresh() -> Dict[str, Any]:
    now_ts = time.time()
    rows: List[tuple[str, Any]] = []
    for exchange_name in exchange_manager.get_connected_exchanges():
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            continue
        rows.append((exchange_name, connector))

    if not rows:
        snapshot = {
            "unrealized_pnl_usd": 0.0,
            "system_unrealized_pnl_usd": 0.0,
            "external_unrealized_pnl_usd": 0.0,
            "position_count": 0,
            "system_position_count": 0,
            "external_position_count": 0,
            "by_exchange": {},
            "distribution": {},
            "system_distribution": {},
            "external_distribution": {},
        }
        _LIVE_POSITION_SNAPSHOT_CACHE["ts"] = now_ts
        _LIVE_POSITION_SNAPSHOT_CACHE["data"] = dict(snapshot)
        return snapshot

    def _contract_bucket_label(symbol: str, side: str) -> str:
        text = str(symbol or "").upper()
        if ":" in text:
            text = text.split(":", 1)[0]
        base = text.split("/", 1)[0].strip() if "/" in text else text.strip()
        base = base or text or "UNKNOWN"
        side_text = "多" if str(side or "").lower() == "long" else "空"
        return f"{base} {side_text}(合约)"

    system_qty_by_key = _local_system_position_qty_by_key()

    async def _fetch_one(exchange_name: str, connector: Any) -> Dict[str, Any]:
        try:
            if exchange_name == "binance":
                positions = await asyncio.wait_for(
                    _fetch_binance_positions_fast(),
                    timeout=min(_LIVE_POSITION_FETCH_TIMEOUT_SEC, 4.8),
                )
            else:
                positions = await asyncio.wait_for(
                    connector.get_positions(),
                    timeout=_LIVE_POSITION_FETCH_TIMEOUT_SEC,
                )
        except Exception as e:
            if exchange_name == "binance":
                try:
                    positions = await asyncio.wait_for(
                        _fetch_binance_positions_via_fallback(),
                        timeout=min(
                            max(_LIVE_POSITION_FETCH_TIMEOUT_SEC * 0.75, 4.0), 7.0
                        ),
                    )
                    if positions is None:
                        positions = []
                except Exception as fallback_err:
                    return {
                        "exchange": exchange_name,
                        "position_count": 0,
                        "unrealized_pnl_usd": 0.0,
                        "system_position_count": 0,
                        "system_unrealized_pnl_usd": 0.0,
                        "external_position_count": 0,
                        "external_unrealized_pnl_usd": 0.0,
                        "distribution": {},
                        "system_distribution": {},
                        "external_distribution": {},
                        "error": str(fallback_err or e),
                    }
            else:
                return {
                    "exchange": exchange_name,
                    "position_count": 0,
                    "unrealized_pnl_usd": 0.0,
                    "system_position_count": 0,
                    "system_unrealized_pnl_usd": 0.0,
                    "external_position_count": 0,
                    "external_unrealized_pnl_usd": 0.0,
                    "distribution": {},
                    "system_distribution": {},
                    "external_distribution": {},
                    "error": (
                        f"position request timeout after {_LIVE_POSITION_FETCH_TIMEOUT_SEC:.1f}s"
                        if isinstance(e, asyncio.TimeoutError)
                        else str(e)
                    ),
                }

        count = 0
        unrealized = 0.0
        system_count = 0
        external_count = 0
        system_unrealized = 0.0
        external_unrealized = 0.0
        distribution: Dict[str, float] = {}
        system_distribution: Dict[str, float] = {}
        external_distribution: Dict[str, float] = {}
        system_remaining = {
            key: qty
            for key, qty in system_qty_by_key.items()
            if key[0] == str(exchange_name or "").strip().lower()
        }
        for pos in positions or []:
            amount = abs(
                float(
                    (
                        pos.get("amount")
                        if isinstance(pos, dict)
                        else getattr(pos, "amount", 0.0)
                    )
                    or 0.0
                )
            )
            if amount <= 0:
                continue
            count += 1
            pos_unrealized = float(
                (
                    pos.get("unrealized_pnl")
                    if isinstance(pos, dict)
                    else getattr(pos, "unrealized_pnl", 0.0)
                )
                or 0.0
            )
            unrealized += pos_unrealized
            current_price = float(
                (
                    pos.get("current_price")
                    if isinstance(pos, dict)
                    else getattr(pos, "current_price", 0.0)
                )
                or 0.0
            )
            if current_price <= 0:
                current_price = float(
                    (
                        pos.get("entry_price")
                        if isinstance(pos, dict)
                        else getattr(pos, "entry_price", 0.0)
                    )
                    or 0.0
                )
            notional_usd = amount * max(current_price, 0.0)
            symbol_value = str(
                (
                    pos.get("symbol")
                    if isinstance(pos, dict)
                    else getattr(pos, "symbol", "")
                )
                or ""
            )
            side_value = str(
                (
                    pos.get("side")
                    if isinstance(pos, dict)
                    else getattr(pos, "side", "")
                )
                or ""
            )
            match_key = (
                str(exchange_name or "").strip().lower(),
                _normalize_position_symbol(symbol_value),
                _normalize_position_side(side_value),
            )
            owned_qty = min(amount, max(0.0, float(system_remaining.get(match_key, 0.0) or 0.0)))
            if owned_qty > 0:
                system_remaining[match_key] = max(
                    0.0,
                    float(system_remaining.get(match_key, 0.0) or 0.0) - owned_qty,
                )
            external_qty = max(0.0, amount - owned_qty)
            owned_ratio = min(1.0, max(0.0, owned_qty / amount)) if amount > 0 else 0.0
            system_pnl = pos_unrealized * owned_ratio
            external_pnl = pos_unrealized - system_pnl
            system_unrealized += system_pnl
            external_unrealized += external_pnl
            if owned_qty > 1e-12:
                system_count += 1
            if external_qty > 1e-12:
                external_count += 1
            if notional_usd > 0:
                label = _contract_bucket_label(
                    symbol_value,
                    side_value,
                )
                distribution[label] = distribution.get(label, 0.0) + float(notional_usd)
                system_notional = notional_usd * owned_ratio
                external_notional = notional_usd - system_notional
                if system_notional > 0:
                    system_distribution[label] = system_distribution.get(label, 0.0) + float(system_notional)
                if external_notional > 0:
                    external_distribution[label] = external_distribution.get(label, 0.0) + float(external_notional)
        return {
            "exchange": exchange_name,
            "position_count": count,
            "unrealized_pnl_usd": unrealized,
            "system_position_count": system_count,
            "system_unrealized_pnl_usd": system_unrealized,
            "external_position_count": external_count,
            "external_unrealized_pnl_usd": external_unrealized,
            "distribution": distribution,
            "system_distribution": system_distribution,
            "external_distribution": external_distribution,
        }

    fetched = await asyncio.gather(
        *[_fetch_one(name, conn) for name, conn in rows], return_exceptions=False
    )

    by_exchange: Dict[str, Dict[str, Any]] = {}
    total_count = 0
    total_unrealized = 0.0
    total_system_count = 0
    total_system_unrealized = 0.0
    total_external_count = 0
    total_external_unrealized = 0.0
    total_distribution: Dict[str, float] = {}
    total_system_distribution: Dict[str, float] = {}
    total_external_distribution: Dict[str, float] = {}
    for row in fetched:
        ex_name = str(row.get("exchange") or "").lower()
        if not ex_name:
            continue
        by_exchange[ex_name] = {
            "position_count": int(row.get("position_count") or 0),
            "unrealized_pnl_usd": round(float(row.get("unrealized_pnl_usd") or 0.0), 4),
            "system_position_count": int(row.get("system_position_count") or 0),
            "system_unrealized_pnl_usd": round(float(row.get("system_unrealized_pnl_usd") or 0.0), 4),
            "external_position_count": int(row.get("external_position_count") or 0),
            "external_unrealized_pnl_usd": round(float(row.get("external_unrealized_pnl_usd") or 0.0), 4),
            "error": row.get("error"),
            "distribution": dict(row.get("distribution") or {}),
            "system_distribution": dict(row.get("system_distribution") or {}),
            "external_distribution": dict(row.get("external_distribution") or {}),
        }
        total_count += int(row.get("position_count") or 0)
        total_unrealized += float(row.get("unrealized_pnl_usd") or 0.0)
        total_system_count += int(row.get("system_position_count") or 0)
        total_system_unrealized += float(row.get("system_unrealized_pnl_usd") or 0.0)
        total_external_count += int(row.get("external_position_count") or 0)
        total_external_unrealized += float(row.get("external_unrealized_pnl_usd") or 0.0)
        for label, usd_value in (row.get("distribution") or {}).items():
            key = str(label or "").strip()
            if not key:
                continue
            total_distribution[key] = total_distribution.get(key, 0.0) + float(
                usd_value or 0.0
            )
        for label, usd_value in (row.get("system_distribution") or {}).items():
            key = str(label or "").strip()
            if key:
                total_system_distribution[key] = total_system_distribution.get(key, 0.0) + float(usd_value or 0.0)
        for label, usd_value in (row.get("external_distribution") or {}).items():
            key = str(label or "").strip()
            if key:
                total_external_distribution[key] = total_external_distribution.get(key, 0.0) + float(usd_value or 0.0)

    # Fallback: include local live strategy/manual positions when exchange snapshots
    # are unavailable, so dashboard exposure still reflects actual contract holdings.
    for pos in position_manager.get_all_positions():
        try:
            exchange_name = str(getattr(pos, "exchange", "") or "").strip().lower()
            if not exchange_name:
                continue
            qty = abs(float(getattr(pos, "quantity", 0.0) or 0.0))
            if qty <= 0:
                continue
            source = (
                str((getattr(pos, "metadata", {}) or {}).get("source") or "")
                .strip()
                .lower()
            )
            if source == "exchange_live":
                continue
            exchange_row = by_exchange.setdefault(
                exchange_name,
                {
                    "position_count": 0,
                    "unrealized_pnl_usd": 0.0,
                    "system_position_count": 0,
                    "system_unrealized_pnl_usd": 0.0,
                    "external_position_count": 0,
                    "external_unrealized_pnl_usd": 0.0,
                    "error": None,
                    "distribution": {},
                    "system_distribution": {},
                    "external_distribution": {},
                },
            )
            if int(exchange_row.get("position_count") or 0) > 0:
                continue
            current_price = float(getattr(pos, "current_price", 0.0) or 0.0)
            if current_price <= 0:
                current_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
            notional_usd = qty * max(current_price, 0.0)
            if notional_usd <= 0:
                continue
            side_value = str(
                getattr(getattr(pos, "side", None), "value", getattr(pos, "side", ""))
                or ""
            )
            label = _contract_bucket_label(
                str(getattr(pos, "symbol", "") or ""), side_value
            )
            exchange_row["position_count"] = (
                int(exchange_row.get("position_count") or 0) + 1
            )
            exchange_row["unrealized_pnl_usd"] = float(
                exchange_row.get("unrealized_pnl_usd") or 0.0
            ) + float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
            is_system_owned = _is_system_owned_local_position(pos)
            pnl_value = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
            if is_system_owned:
                exchange_row["system_position_count"] = int(exchange_row.get("system_position_count") or 0) + 1
                exchange_row["system_unrealized_pnl_usd"] = float(exchange_row.get("system_unrealized_pnl_usd") or 0.0) + pnl_value
            else:
                exchange_row["external_position_count"] = int(exchange_row.get("external_position_count") or 0) + 1
                exchange_row["external_unrealized_pnl_usd"] = float(exchange_row.get("external_unrealized_pnl_usd") or 0.0) + pnl_value
            local_distribution = dict(exchange_row.get("distribution") or {})
            local_distribution[label] = local_distribution.get(label, 0.0) + float(
                notional_usd
            )
            exchange_row["distribution"] = local_distribution
            if is_system_owned:
                local_system_distribution = dict(exchange_row.get("system_distribution") or {})
                local_system_distribution[label] = local_system_distribution.get(label, 0.0) + float(notional_usd)
                exchange_row["system_distribution"] = local_system_distribution
            else:
                local_external_distribution = dict(exchange_row.get("external_distribution") or {})
                local_external_distribution[label] = local_external_distribution.get(label, 0.0) + float(notional_usd)
                exchange_row["external_distribution"] = local_external_distribution
            total_count += 1
            total_unrealized += pnl_value
            total_distribution[label] = total_distribution.get(label, 0.0) + float(
                notional_usd
            )
            if is_system_owned:
                total_system_count += 1
                total_system_unrealized += pnl_value
                total_system_distribution[label] = total_system_distribution.get(label, 0.0) + float(notional_usd)
            else:
                total_external_count += 1
                total_external_unrealized += pnl_value
                total_external_distribution[label] = total_external_distribution.get(label, 0.0) + float(notional_usd)
        except Exception:
            continue

    snapshot = {
        "unrealized_pnl_usd": round(total_unrealized, 4),
        "system_unrealized_pnl_usd": round(total_system_unrealized, 4),
        "external_unrealized_pnl_usd": round(total_external_unrealized, 4),
        "position_count": int(total_count),
        "system_position_count": int(total_system_count),
        "external_position_count": int(total_external_count),
        "by_exchange": by_exchange,
        "distribution": total_distribution,
        "system_distribution": total_system_distribution,
        "external_distribution": total_external_distribution,
    }
    _LIVE_POSITION_SNAPSHOT_CACHE["ts"] = now_ts
    _LIVE_POSITION_SNAPSHOT_CACHE["data"] = dict(snapshot)
    return snapshot


def _apply_live_snapshot_to_risk_report(
    risk_report: Dict[str, Any],
    live_snapshot: Dict[str, Any],
    live_daily_total_pnl: Optional[float] = None,
    live_day_start_equity: Optional[float] = None,
    live_daily_realized_pnl: Optional[float] = None,
    live_daily_realized_source: Optional[str] = None,
) -> Dict[str, Any]:
    out = dict(risk_report or {})
    equity = dict(out.get("equity") or {})
    live_total_unrealized = float(live_snapshot.get("unrealized_pnl_usd") or 0.0)
    live_system_unrealized = float(
        live_snapshot.get("system_unrealized_pnl_usd", live_total_unrealized) or 0.0
    )
    live_external_unrealized = float(
        live_snapshot.get(
            "external_unrealized_pnl_usd",
            live_total_unrealized - live_system_unrealized,
        )
        or 0.0
    )
    daily_equity_delta = float(equity.get("daily_pnl_usd") or 0.0)
    daily_total = (
        float(live_daily_total_pnl)
        if live_daily_total_pnl is not None
        else daily_equity_delta
    )
    existing_realized = float(equity.get("daily_realized_pnl_usd") or 0.0)
    has_live_realized = live_daily_realized_pnl is not None
    daily_realized = (
        float(live_daily_realized_pnl or 0.0)
        if has_live_realized
        else existing_realized
    )
    daily_realized_source = (
        str(live_daily_realized_source or "").strip()
        or str(equity.get("daily_realized_pnl_source") or "").strip()
        or ("live_resolved" if has_live_realized else "risk_manager")
    )
    daily_stop_basis = daily_realized + min(0.0, live_system_unrealized)
    daily_unrealized_component = daily_total - daily_realized

    equity["current_unrealized_pnl_usd"] = round(live_system_unrealized, 4)
    equity["system_unrealized_pnl_usd"] = round(live_system_unrealized, 4)
    equity["external_unrealized_pnl_usd"] = round(live_external_unrealized, 4)
    equity["total_open_unrealized_pnl_usd"] = round(live_total_unrealized, 4)
    equity["daily_total_pnl_usd"] = round(daily_total, 4)
    equity["daily_pnl_usd"] = round(daily_total, 4)
    equity["daily_realized_pnl_usd"] = round(daily_realized, 4)
    equity["daily_realized_pnl_source"] = daily_realized_source
    equity["daily_stop_basis_usd"] = round(daily_stop_basis, 4)
    equity["daily_unrealized_component_usd"] = round(daily_unrealized_component, 4)
    day_start_equity = 0.0
    if live_day_start_equity is not None and float(live_day_start_equity or 0.0) > 0:
        day_start_equity = float(live_day_start_equity or 0.0)
    else:
        day_start_equity = _safe_float(equity.get("day_start"), default=0.0)
        if day_start_equity <= 0:
            current_equity = _safe_float(equity.get("current"), default=0.0)
            derived_day_start = current_equity - daily_total
            if current_equity > 0 and derived_day_start > 0:
                day_start_equity = float(derived_day_start)

    if day_start_equity > 0:
        equity["day_start"] = round(day_start_equity, 4)
        equity["daily_total_pnl_ratio"] = round(
            daily_total / max(day_start_equity, 1e-6), 6
        )
        equity["daily_stop_basis_ratio"] = round(
            daily_stop_basis / max(day_start_equity, 1e-6), 6
        )
        equity["daily_pnl_ratio"] = equity["daily_stop_basis_ratio"]
    else:
        total_ratio = _safe_float(
            equity.get("daily_total_pnl_ratio"),
            default=_safe_float(equity.get("daily_pnl_ratio"), default=0.0),
        )
        stop_ratio = _safe_float(
            equity.get("daily_stop_basis_ratio"),
            default=_safe_float(equity.get("daily_pnl_ratio"), default=0.0),
        )
        equity["daily_total_pnl_ratio"] = round(total_ratio, 6)
        equity["daily_stop_basis_ratio"] = round(stop_ratio, 6)
        equity["daily_pnl_ratio"] = round(stop_ratio, 6)
    equity["pnl_scope_note"] = (
        "daily_total_pnl_usd is account equity change; current_unrealized_pnl_usd "
        "is system-owned strategy unrealized PnL; external_unrealized_pnl_usd is "
        "shown for visibility but excluded from strategy circuit-breaker basis; "
        "daily_stop_basis_usd = daily_realized_pnl_usd + current system floating loss."
    )
    out["equity"] = equity
    limits = dict(out.get("limits") or {})
    max_daily_loss_ratio = abs(_safe_float(limits.get("max_daily_loss_ratio"), default=0.0))
    stop_ratio_for_level = _safe_float(equity.get("daily_stop_basis_ratio"), default=0.0)
    if bool(out.get("trading_halted")):
        out["risk_level"] = "critical"
    elif max_daily_loss_ratio > 0 and stop_ratio_for_level <= -(max_daily_loss_ratio * 0.7):
        out["risk_level"] = "high"
    out["live_positions"] = {
        "position_count": int(live_snapshot.get("position_count") or 0),
        "system_position_count": int(live_snapshot.get("system_position_count") or 0),
        "external_position_count": int(live_snapshot.get("external_position_count") or 0),
        "unrealized_pnl_usd": round(live_total_unrealized, 4),
        "system_unrealized_pnl_usd": round(live_system_unrealized, 4),
        "external_unrealized_pnl_usd": round(live_external_unrealized, 4),
        "by_exchange": live_snapshot.get("by_exchange") or {},
    }
    return out


async def _build_effective_risk_report(
    force_live_refresh: bool = False,
) -> Dict[str, Any]:
    report = risk_manager.get_risk_report()
    if execution_engine.is_paper_mode():
        return report
    live_snapshot = await _collect_live_position_snapshot(
        force_refresh=force_live_refresh
    )
    realized_payload = _resolve_live_system_daily_realized_pnl()
    resolved_realized = realized_payload.get("pnl")
    return _apply_live_snapshot_to_risk_report(
        report,
        live_snapshot,
        live_daily_realized_pnl=(
            float(resolved_realized) if resolved_realized is not None else None
        ),
        live_daily_realized_source=str(realized_payload.get("source") or ""),
    )


def _bucket_key(ts: datetime, mode: str) -> str:
    if mode == "hour":
        return ts.strftime("%Y-%m-%d %H:00")
    return ts.strftime("%Y-%m-%d")


def _init_analytics_paths() -> None:
    _ANALYTICS_ROOT.mkdir(parents=True, exist_ok=True)


def _read_json_file(path: Path, default: Any) -> Any:
    _init_analytics_paths()
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_file(path: Path, payload: Any) -> None:
    _init_analytics_paths()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_behavior_journal() -> List[Dict[str, Any]]:
    rows = _read_json_file(_BEHAVIOR_JOURNAL_PATH, default=[])
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def _save_behavior_journal(rows: List[Dict[str, Any]]) -> None:
    _write_json_file(_BEHAVIOR_JOURNAL_PATH, rows[-5000:])


def _load_stoploss_policy() -> Dict[str, Any]:
    data = _read_json_file(_STOPLOSS_POLICY_PATH, default={})
    if not isinstance(data, dict):
        data = {}
    merged = dict(_DEFAULT_STOPLOSS_POLICY)
    for key, value in data.items():
        if isinstance(value, dict):
            merged[key] = dict(merged.get(key, {}), **value)
        else:
            merged[key] = value
    return merged


def _save_stoploss_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
    merged = _load_stoploss_policy()
    for key, value in policy.items():
        if isinstance(value, dict):
            merged[key] = dict(merged.get(key, {}), **value)
        else:
            merged[key] = value
    _write_json_file(_STOPLOSS_POLICY_PATH, merged)
    return merged


# Cached fallback connector — reused across calls so we don't repeat the
# connect/load_markets/disconnect cycle on every balance/position request.
# Replaced (transparently) if credentials change or the connector goes stale.
_FALLBACK_CONNECTOR_LOCK = asyncio.Lock()
_FALLBACK_CONNECTOR: Optional[BinanceConnector] = None
_FALLBACK_CONNECTOR_KEY: Optional[Tuple[str, str, str]] = None


async def _create_binance_readonly_connector() -> Optional[BinanceConnector]:
    global _FALLBACK_CONNECTOR, _FALLBACK_CONNECTOR_KEY

    base_cfg = get_exchange_config("binance")
    if not base_cfg:
        return None

    api_key = settings.BINANCE_API_KEY or base_cfg.api_key or ""
    api_secret = settings.BINANCE_API_SECRET or base_cfg.api_secret or ""
    default_type = str(
        getattr(settings, "BINANCE_DEFAULT_TYPE", base_cfg.default_type)
        or base_cfg.default_type
        or "spot"
    )
    key = (api_key, api_secret, default_type)

    async with _FALLBACK_CONNECTOR_LOCK:
        existing = _FALLBACK_CONNECTOR
        if (
            existing is not None
            and _FALLBACK_CONNECTOR_KEY == key
            and getattr(existing, "is_connected", False)
        ):
            return existing

        # Credentials/type changed or prior connector dead — drop and rebuild.
        if existing is not None and _FALLBACK_CONNECTOR_KEY != key:
            with contextlib.suppress(Exception):
                await existing.disconnect()
            _FALLBACK_CONNECTOR = None
            _FALLBACK_CONNECTOR_KEY = None

        cfg = copy.deepcopy(base_cfg)
        cfg.api_key = api_key
        cfg.api_secret = api_secret
        cfg.default_type = default_type
        connector = BinanceConnector(cfg)
        try:
            ok = await connector.connect()
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await connector.disconnect()
            raise
        if not ok:
            with contextlib.suppress(Exception):
                await connector.disconnect()
            return None
        _FALLBACK_CONNECTOR = connector
        _FALLBACK_CONNECTOR_KEY = key
        return connector


async def _fetch_binance_balances_via_fallback() -> Optional[List[Any]]:
    connector = await _create_binance_readonly_connector()
    if not connector:
        return None
    try:
        return await connector.get_balance()
    except Exception:
        # Drop cached connector on failure so the next call rebuilds it.
        global _FALLBACK_CONNECTOR, _FALLBACK_CONNECTOR_KEY
        async with _FALLBACK_CONNECTOR_LOCK:
            if _FALLBACK_CONNECTOR is connector:
                with contextlib.suppress(Exception):
                    await connector.disconnect()
                _FALLBACK_CONNECTOR = None
                _FALLBACK_CONNECTOR_KEY = None
        raise


async def _fetch_binance_positions_via_fallback() -> Optional[List[Any]]:
    connector = await _create_binance_readonly_connector()
    if not connector:
        return None
    try:
        return await connector.get_positions()
    except Exception:
        global _FALLBACK_CONNECTOR, _FALLBACK_CONNECTOR_KEY
        async with _FALLBACK_CONNECTOR_LOCK:
            if _FALLBACK_CONNECTOR is connector:
                with contextlib.suppress(Exception):
                    await connector.disconnect()
                _FALLBACK_CONNECTOR = None
                _FALLBACK_CONNECTOR_KEY = None
        raise


def _binance_has_credentials() -> bool:
    return bool(
        (settings.BINANCE_API_KEY or "").strip()
        and (settings.BINANCE_API_SECRET or "").strip()
    )


def _binance_market_symbol(symbol: Optional[str]) -> Optional[str]:
    text = str(symbol or "").upper().strip()
    if not text:
        return None
    if ":" in text:
        text = text.split(":", 1)[0]
    return text.replace("/", "").replace("-", "")


def _binance_ccxt_symbol(
    symbol: str, quote: str = "USDT", futures: bool = False
) -> str:
    base = str(symbol or "").upper().replace("/", "").replace("-", "")
    if base.endswith(quote):
        asset = base[: -len(quote)]
        if asset:
            return f"{asset}/{quote}:USDT" if futures else f"{asset}/{quote}"
    return symbol


async def _binance_signed_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    host: str = "api",
    timeout_sec: float = _BINANCE_REST_TIMEOUT_SEC,
) -> Any:
    if not _binance_has_credentials():
        raise RuntimeError("binance api credentials unavailable")
    base_url = "https://api.binance.com"
    if host == "fapi":
        base_url = "https://fapi.binance.com"
    elif host == "sapi":
        base_url = "https://api.binance.com"
    proxy_url = settings.HTTP_PROXY or settings.HTTPS_PROXY or None

    async def _refresh_time_offset(target_host: str, *, force: bool = False) -> int:
        now_ts = time.time()
        if (not force) and (
            now_ts - float(_BINANCE_TIME_OFFSET_MS.get("ts") or 0.0)
        ) <= 180.0:
            cached = int(_BINANCE_TIME_OFFSET_MS.get(target_host, 0) or 0)
            if cached:
                return cached
        time_url = "https://api.binance.com/api/v3/time"
        if target_host == "fapi":
            time_url = "https://fapi.binance.com/fapi/v1/time"
        client_kwargs: Dict[str, Any] = {"timeout": 3.0}
        _apply_httpx_proxy_kw(client_kwargs, proxy_url)
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.get(time_url)
            resp.raise_for_status()
            server_ms = int((resp.json() or {}).get("serverTime") or 0)
        offset = int(server_ms - int(time.time() * 1000))
        _BINANCE_TIME_OFFSET_MS[target_host] = offset
        _BINANCE_TIME_OFFSET_MS["ts"] = now_ts
        return offset

    async def _ensure_offsets() -> None:
        await asyncio.gather(
            _refresh_time_offset("api", force=False),
            _refresh_time_offset("fapi", force=False),
            return_exceptions=True,
        )

    async def _send_once(force_time_refresh: bool = False) -> httpx.Response:
        if force_time_refresh:
            await _refresh_time_offset("fapi" if host == "fapi" else "api", force=True)
        offset_ms = int(
            _BINANCE_TIME_OFFSET_MS.get("fapi" if host == "fapi" else "api", 0) or 0
        )
        payload: Dict[str, Any] = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000) + offset_ms
        payload["recvWindow"] = int(_BINANCE_RECV_WINDOW)
        query = urlencode(
            [(k, v) for k, v in payload.items() if v is not None], doseq=True
        )
        secret = (settings.BINANCE_API_SECRET or "").strip().encode("utf-8")
        signature = hmac.new(secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": (settings.BINANCE_API_KEY or "").strip()}
        url = f"{base_url}{path}"
        client_kwargs: Dict[str, Any] = {"timeout": timeout_sec, "headers": headers}
        _apply_httpx_proxy_kw(client_kwargs, proxy_url)
        async with httpx.AsyncClient(**client_kwargs) as client:
            if method.upper() == "GET":
                return await client.get(url, params={**payload, "signature": signature})
            return await client.post(url, data={**payload, "signature": signature})

    await _ensure_offsets()
    resp = await _send_once(force_time_refresh=False)
    if resp.status_code >= 400:
        text = resp.text or ""
        if "-1021" in text:
            resp = await _send_once(force_time_refresh=True)
    resp.raise_for_status()
    return resp.json()


async def _binance_public_price_usd(asset: str, timeout_sec: float = 1.6) -> float:
    ccy = str(asset or "").upper().strip()
    if not ccy:
        return 0.0
    if ccy in STABLE_COINS:
        return 1.0
    symbol = f"{ccy}USDT"
    async with httpx.AsyncClient(timeout=timeout_sec, verify=get_shared_ssl_context()) as client:
        try:
            resp = await client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol},
            )
            resp.raise_for_status()
            return _safe_float((resp.json() or {}).get("price"), default=0.0)
        except Exception:
            return 0.0


async def _binance_public_quotes_usd(assets: List[str]) -> Dict[str, float]:
    unique_assets = []
    seen = set()
    for asset in assets:
        ccy = str(asset or "").upper().strip()
        if not ccy or ccy in seen:
            continue
        seen.add(ccy)
        unique_assets.append(ccy)
    if not unique_assets:
        return {}
    proxy_url = settings.HTTP_PROXY or settings.HTTPS_PROXY or None
    client_kwargs: Dict[str, Any] = {"timeout": 2.5}
    _apply_httpx_proxy_kw(client_kwargs, proxy_url)
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.get("https://api.binance.com/api/v3/ticker/price")
            resp.raise_for_status()
            rows = resp.json() or []
        price_map = {
            str(row.get("symbol") or "").upper(): _safe_float(
                row.get("price"), default=0.0
            )
            for row in rows
            if isinstance(row, dict)
        }
        return {
            asset: float(price_map.get(f"{asset}USDT", 0.0) or 0.0)
            for asset in unique_assets
        }
    except Exception:
        prices = await asyncio.gather(
            *[_binance_public_price_usd(asset) for asset in unique_assets],
            return_exceptions=False,
        )
        return {
            asset: float(price or 0.0) for asset, price in zip(unique_assets, prices)
        }


async def _fetch_binance_realized_pnl_income(days: int = 30) -> List[Dict[str, Any]]:
    if not _binance_has_credentials():
        return []
    start_time_ms = int(
        (
            datetime.now(timezone.utc) - timedelta(days=max(1, int(days or 30)))
        ).timestamp()
        * 1000
    )
    rows = await _binance_signed_request(
        "GET",
        "/fapi/v1/income",
        host="fapi",
        params={
            "incomeType": "REALIZED_PNL",
            "startTime": start_time_ms,
            "limit": 1000,
        },
        timeout_sec=8.0,
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip()
        ts = _safe_dt(row.get("time"))
        pnl = _safe_float(row.get("income"), default=0.0)
        if not symbol or not ts:
            continue
        out.append(
            {
                "symbol": _binance_ccxt_symbol(symbol, futures=True),
                "timestamp": ts,
                "pnl": pnl,
            }
        )
    return out


def _current_utc_day_start() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _sum_realized_pnl_since(
    rows: List[Dict[str, Any]],
    *,
    day_start: datetime,
) -> Dict[str, Any]:
    total = 0.0
    count = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ts = _safe_dt(row.get("timestamp"))
        if ts is None or ts < day_start:
            continue
        total += _safe_float(row.get("pnl"), default=0.0)
        count += 1
    return {"pnl": total, "row_count": count}


def _resolve_live_daily_realized_from_runtime_history(
    *,
    day_start: datetime,
) -> Dict[str, Any]:
    try:
        rows = risk_manager.get_trade_history(limit=50000, scope="live")
    except Exception:
        rows = []
    summed = _sum_realized_pnl_since(list(rows or []), day_start=day_start)
    if int(summed.get("row_count") or 0) <= 0:
        return {
            "pnl": None,
            "source": "unavailable",
            "row_count": 0,
            "day_start": day_start.isoformat(),
        }
    return {
        "pnl": float(summed.get("pnl") or 0.0),
        "source": "runtime_trade_history",
        "row_count": int(summed.get("row_count") or 0),
        "day_start": day_start.isoformat(),
    }


def _is_system_realized_pnl_row(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    strategy = str(row.get("strategy") or row.get("strategy_name") or "").strip()
    if not strategy or _is_manual_strategy_label(strategy):
        return False
    action = str(row.get("action") or "").strip().lower()
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    source = str(metadata.get("source") or row.get("source") or "").strip().lower()
    if action == "manual_order" or source in {"manual", "external", "exchange_live"}:
        return False
    mode = str(row.get("mode") or row.get("runtime_mode") or row.get("trading_mode") or "").strip().lower()
    if mode and mode != "live":
        return False
    order_id = str(row.get("order_id") or "").strip()
    if order_id.startswith("paper_"):
        return False
    return True


def _resolve_live_system_daily_realized_pnl(
    *,
    day_start: Optional[datetime] = None,
) -> Dict[str, Any]:
    resolved_day_start = day_start or _current_utc_day_start()
    try:
        rows = risk_manager.get_trade_history(limit=50000, scope="live")
    except Exception:
        rows = []
    system_rows = [
        row
        for row in (rows or [])
        if isinstance(row, dict) and _is_system_realized_pnl_row(row)
    ]
    summed = _sum_realized_pnl_since(system_rows, day_start=resolved_day_start)
    return {
        "pnl": float(summed.get("pnl") or 0.0),
        "source": "system_runtime_trade_history",
        "row_count": int(summed.get("row_count") or 0),
        "day_start": resolved_day_start.isoformat(),
    }


async def _resolve_live_daily_realized_pnl(
    *,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    day_start = _current_utc_day_start()
    day_key = day_start.isoformat()
    now_ts = time.time()
    cached = dict(_LIVE_DAILY_REALIZED_PNL_CACHE.get("payload") or {})
    cached_ts = float(_LIVE_DAILY_REALIZED_PNL_CACHE.get("ts") or 0.0)
    if (
        not force_refresh
        and cached
        and str(_LIVE_DAILY_REALIZED_PNL_CACHE.get("day_start") or "") == day_key
        and (now_ts - cached_ts) <= _LIVE_DAILY_REALIZED_PNL_TTL_SEC
    ):
        return dict(cached)

    payload: Dict[str, Any]
    if _binance_has_credentials():
        try:
            rows = await asyncio.wait_for(
                _fetch_binance_realized_pnl_income(days=2),
                timeout=5.5,
            )
            summed = _sum_realized_pnl_since(list(rows or []), day_start=day_start)
            payload = {
                "pnl": float(summed.get("pnl") or 0.0),
                "source": "binance_income",
                "row_count": int(summed.get("row_count") or 0),
                "day_start": day_key,
            }
        except Exception as exc:
            logger.debug(f"binance live realized pnl income unavailable: {exc}")
            payload = _resolve_live_daily_realized_from_runtime_history(
                day_start=day_start
            )
    else:
        payload = _resolve_live_daily_realized_from_runtime_history(
            day_start=day_start
        )

    _LIVE_DAILY_REALIZED_PNL_CACHE["ts"] = now_ts
    _LIVE_DAILY_REALIZED_PNL_CACHE["day_start"] = day_key
    _LIVE_DAILY_REALIZED_PNL_CACHE["payload"] = dict(payload)
    return dict(payload)


async def _fetch_binance_live_wallet_snapshot_fast() -> Dict[str, Any]:
    if not _binance_has_credentials():
        raise RuntimeError("binance api credentials unavailable")

    async def _get_spot_account():
        return await _binance_signed_request("GET", "/api/v3/account", host="api")

    async def _get_futures_balance():
        try:
            return await _binance_signed_request("GET", "/fapi/v3/balance", host="fapi")
        except Exception:
            return await _binance_signed_request("GET", "/fapi/v2/balance", host="fapi")

    async def _get_funding_wallet():
        try:
            return await _binance_signed_request(
                "POST",
                "/sapi/v1/asset/get-funding-asset",
                host="sapi",
                params={"needBtcValuation": "false"},
            )
        except Exception:
            return []

    spot_raw, futures_raw, funding_raw = await asyncio.gather(
        _get_spot_account(),
        _get_futures_balance(),
        _get_funding_wallet(),
        return_exceptions=True,
    )

    warnings: List[str] = []
    balances: List[Dict[str, Any]] = []
    distribution: Dict[str, float] = {}
    components: Dict[str, float] = {"spot": 0.0, "funding": 0.0, "futures": 0.0}
    quote_assets: List[str] = []

    def _append_balance(
        currency: str,
        free: float,
        used: float,
        total: float,
        source: str,
        unit_usd: float = 0.0,
    ):
        ccy = str(currency or "").upper().strip()
        total_amt = float(total or 0.0)
        if not ccy or total_amt <= 0:
            return
        if ccy not in STABLE_COINS and unit_usd <= 0:
            quote_assets.append(ccy)
        balances.append(
            {
                "currency": ccy,
                "free": float(free or 0.0),
                "used": float(used or 0.0),
                "total": total_amt,
                "unit_usd": float(unit_usd or 0.0),
                "usd_value": 0.0,
                "valuation_source": source,
                "wallet_source": source,
            }
        )

    if isinstance(spot_raw, Exception):
        warnings.append(f"spot: {spot_raw}")
    else:
        for row in (spot_raw or {}).get("balances", []) or []:
            free = _safe_float(row.get("free"), default=0.0)
            locked = _safe_float(row.get("locked"), default=0.0)
            total = free + locked
            if total <= 0:
                continue
            _append_balance(str(row.get("asset") or ""), free, locked, total, "spot")

    if isinstance(funding_raw, Exception):
        warnings.append(f"funding: {funding_raw}")
    else:
        funding_rows = funding_raw if isinstance(funding_raw, list) else []
        for row in funding_rows:
            free = _safe_float(row.get("free"), default=0.0)
            locked = (
                _safe_float(row.get("locked"), default=0.0)
                + _safe_float(row.get("freeze"), default=0.0)
                + _safe_float(row.get("withdrawing"), default=0.0)
            )
            total = free + locked
            if total <= 0:
                continue
            _append_balance(str(row.get("asset") or ""), free, locked, total, "funding")

    if isinstance(futures_raw, Exception):
        warnings.append(f"futures: {futures_raw}")
    else:
        futures_rows = futures_raw if isinstance(futures_raw, list) else []
        for row in futures_rows:
            currency = str(row.get("asset") or "").upper().strip()
            wallet_balance = _safe_float(row.get("balance"), default=0.0)
            available = _safe_float(row.get("availableBalance"), default=wallet_balance)
            unrealized = _safe_float(row.get("crossUnPnl"), default=0.0)
            total = wallet_balance + unrealized
            used = max(total - available, 0.0)
            if total <= 0:
                continue
            _append_balance(
                currency,
                available,
                used,
                total,
                "futures",
                1.0 if currency in STABLE_COINS else 0.0,
            )

    quotes = await _binance_public_quotes_usd(quote_assets)
    total_usd = 0.0
    for row in balances:
        currency = str(row.get("currency") or "").upper()
        unit_usd = (
            1.0
            if currency in STABLE_COINS
            else _safe_float(quotes.get(currency), default=0.0)
        )
        usd_value = (
            _safe_float(row.get("total"), default=0.0) * unit_usd
            if unit_usd > 0
            else 0.0
        )
        row["unit_usd"] = round(unit_usd, 8) if unit_usd > 0 else 0.0
        row["usd_value"] = round(usd_value, 4)
        row["valuation_source"] = (
            "stable"
            if currency in STABLE_COINS
            else ("live" if unit_usd > 0 else "unpriced")
        )
        distribution[currency] = distribution.get(currency, 0.0) + float(
            usd_value or 0.0
        )
        wallet_source = str(row.get("wallet_source") or "spot")
        components[wallet_source] = components.get(wallet_source, 0.0) + float(
            usd_value or 0.0
        )
        total_usd += float(usd_value or 0.0)

    balances.sort(key=lambda item: float(item.get("usd_value") or 0.0), reverse=True)
    return {
        "balances": balances,
        "distribution": distribution,
        "total_usd": round(total_usd, 2),
        "components": {k: round(v, 2) for k, v in components.items()},
        "warnings": warnings,
        "valuation_coverage": {
            "priced_assets": sum(
                1 for row in balances if float(row.get("usd_value") or 0.0) > 0
            ),
            "unpriced_assets": sum(
                1
                for row in balances
                if float(row.get("total") or 0.0) > 0
                and float(row.get("usd_value") or 0.0) <= 0
            ),
        },
    }


async def _fetch_binance_positions_fast() -> List[Dict[str, Any]]:
    if not _binance_has_credentials():
        return []
    try:
        rows = await _binance_signed_request(
            "GET", "/fapi/v2/positionRisk", host="fapi"
        )
    except Exception:
        rows = await _binance_signed_request(
            "GET", "/fapi/v3/positionRisk", host="fapi"
        )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        amount = _safe_float(row.get("positionAmt"), default=0.0)
        if abs(amount) <= 0:
            continue
        side = "short" if amount < 0 else "long"
        out.append(
            {
                "symbol": _binance_ccxt_symbol(
                    str(row.get("symbol") or ""), futures=True
                ),
                "side": side,
                "amount": abs(amount),
                "entry_price": _safe_float(row.get("entryPrice"), default=0.0),
                "current_price": _safe_float(row.get("markPrice"), default=0.0),
                "unrealized_pnl": _safe_float(row.get("unRealizedProfit"), default=0.0),
                "leverage": _safe_float(row.get("leverage"), default=1.0),
                "liquidation_price": _safe_float(
                    row.get("liquidationPrice"), default=0.0
                ),
            }
        )
    return out


async def _fetch_binance_open_orders_fast(
    symbol: Optional[str] = None,
    *,
    account_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not shared_binance_has_credentials(account_id):
        return []
    params: Dict[str, Any] = {}
    raw_symbol = shared_binance_market_symbol(symbol)
    if raw_symbol:
        params["symbol"] = raw_symbol
    rows = await shared_binance_signed_request(
        "GET",
        "/fapi/v1/openOrders",
        host="fapi",
        params=params,
        timeout_sec=7.5,
        account_id=account_id,
    )
    orders: List[Dict[str, Any]] = []
    for row in rows or []:
        raw_type = str(row.get("type") or "").lower()
        stop_loss = None
        take_profit = None
        trigger_price = _safe_float(row.get("stopPrice"), default=0.0)
        if "take_profit" in raw_type:
            take_profit = (
                trigger_price
                if trigger_price > 0
                else _safe_float(row.get("price"), default=0.0)
            )
        elif "stop" in raw_type:
            stop_loss = (
                trigger_price
                if trigger_price > 0
                else _safe_float(row.get("price"), default=0.0)
            )
        orders.append(
            {
                "id": str(row.get("orderId") or row.get("clientOrderId") or ""),
                "exchange": "binance",
                "symbol": shared_binance_ccxt_symbol(
                    str(row.get("symbol") or ""), futures=True
                ),
                "side": str(row.get("side") or "").lower(),
                "type": str(row.get("type") or "").lower(),
                "price": _safe_float(row.get("price"), default=0.0),
                "amount": _safe_float(row.get("origQty"), default=0.0),
                "filled": _safe_float(row.get("executedQty"), default=0.0),
                "status": str(row.get("status") or "").lower(),
                "timestamp": _safe_dt(
                    row.get("time") or row.get("updateTime")
                ).isoformat()
                if _safe_dt(row.get("time") or row.get("updateTime"))
                else None,
                "strategy": None,
                "account_id": "exchange_live",
                "order_mode": "normal",
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "trailing_stop_pct": None,
                "trailing_stop_distance": None,
                "trigger_price": trigger_price if trigger_price > 0 else None,
                "reduce_only": bool(row.get("reduceOnly")),
                "rejected": False,
                "reject_reason": None,
                "paper_fee_rate": 0.0,
                "paper_fee_usd": 0.0,
                "paper_slippage_bps": 0.0,
                "paper_slippage_cost_usd": 0.0,
                "paper_reference_price": 0.0,
                "paper_notional_usd": 0.0,
                "client_order_id": str(row.get("clientOrderId") or ""),
                "time_in_force": str(row.get("timeInForce") or ""),
                "working_type": str(row.get("workingType") or ""),
                "price_protect": bool(row.get("priceProtect")),
                "raw_type": str(row.get("type") or ""),
            }
        )
    return orders


def _is_exchange_trigger_order(order: Dict[str, Any]) -> bool:
    order_type = str(order.get("type") or order.get("raw_type") or "").lower()
    return any(
        token in order_type
        for token in ("stop", "take_profit", "trailing_stop")
    ) or _safe_float(order.get("trigger_price"), default=0.0) > 0


def _exchange_order_to_conditional(order: Dict[str, Any]) -> Dict[str, Any]:
    order_type = str(order.get("type") or order.get("raw_type") or "").lower()
    conditional_id = str(order.get("id") or order.get("client_order_id") or "")
    trigger_price = _safe_float(order.get("trigger_price"), default=0.0)
    return {
        "conditional_id": conditional_id,
        "exchange_order_id": str(order.get("id") or ""),
        "client_order_id": str(order.get("client_order_id") or ""),
        "created_at": order.get("timestamp"),
        "exchange": str(order.get("exchange") or "binance"),
        "symbol": str(order.get("symbol") or ""),
        "side": str(order.get("side") or "").lower(),
        "order_type": order_type,
        "amount": _safe_float(order.get("amount"), default=0.0),
        "price": _safe_float(order.get("price"), default=0.0),
        "stop_loss": order.get("stop_loss"),
        "take_profit": order.get("take_profit"),
        "trigger_price": trigger_price,
        "account_id": str(order.get("account_id") or "exchange_live"),
        "strategy": order.get("strategy"),
        "reduce_only": bool(order.get("reduce_only")),
        "status": str(order.get("status") or "open"),
        "source": "exchange",
        "working_type": str(order.get("working_type") or ""),
        "price_protect": bool(order.get("price_protect")),
    }


def _merge_conditional_orders(
    local_orders: List[Dict[str, Any]],
    exchange_orders: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in list(local_orders or []) + [
        _exchange_order_to_conditional(order)
        for order in exchange_orders
        if _is_exchange_trigger_order(order)
    ]:
        exchange = str(row.get("exchange") or "").lower()
        symbol = str(row.get("symbol") or "").upper()
        cid = str(
            row.get("conditional_id")
            or row.get("exchange_order_id")
            or row.get("client_order_id")
            or ""
        )
        key = (exchange, symbol, cid)
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
    merged.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return merged


async def _resolve_live_equity_baseline(
    current_total_usd: float,
    exchange_totals: Dict[str, float],
    live_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    day_key = now.strftime("%Y-%m-%d")
    stored = _read_json_file(_LIVE_EQUITY_BASELINE_PATH, default={})
    if not isinstance(stored, dict):
        stored = {}

    stored_day = str(stored.get("day") or "")
    stored_portfolio = _safe_float(stored.get("portfolio_total_usd"), default=0.0)
    stored_by_exchange = (
        stored.get("by_exchange") if isinstance(stored.get("by_exchange"), dict) else {}
    )
    stored_binance = _safe_float(stored_by_exchange.get("binance"), default=0.0)

    db_portfolio = await account_snapshot_manager.get_day_start_total(
        mode="live", exchange="all", day=now
    )
    db_binance = await account_snapshot_manager.get_day_start_total(
        mode="live", exchange="binance", day=now
    )

    baseline_portfolio = _safe_float(db_portfolio, default=0.0)
    if baseline_portfolio <= 0 and stored_day == day_key:
        baseline_portfolio = stored_portfolio
    if baseline_portfolio <= 0:
        baseline_portfolio = _safe_float(current_total_usd, default=0.0)

    baseline_binance = _safe_float(db_binance, default=0.0)
    if baseline_binance <= 0 and stored_day == day_key:
        baseline_binance = stored_binance
    if baseline_binance <= 0:
        baseline_binance = _safe_float(exchange_totals.get("binance"), default=0.0)

    current_total = _safe_float(current_total_usd, default=0.0)
    live_unrealized = abs(
        _safe_float((live_snapshot or {}).get("unrealized_pnl_usd"), default=0.0)
    )
    if baseline_portfolio > 0 and current_total > 0:
        delta_usd = current_total - baseline_portfolio
        delta_ratio = abs(delta_usd) / max(abs(baseline_portfolio), 1e-6)
        if delta_ratio >= 0.35 and live_unrealized < abs(delta_usd) * 0.45:
            logger.warning(
                "Reset live equity baseline due to incompatible day-start snapshot: "
                f"baseline={baseline_portfolio:.4f}, current={current_total:.4f}, "
                f"delta={delta_usd:.4f}, live_unrealized={live_unrealized:.4f}"
            )
            baseline_portfolio = current_total
            baseline_binance = _safe_float(
                exchange_totals.get("binance"), default=baseline_binance
            )

    payload = {
        "day": day_key,
        "portfolio_total_usd": round(_safe_float(baseline_portfolio), 8),
        "by_exchange": {
            "binance": round(_safe_float(baseline_binance), 8),
        },
        "updated_at": now.isoformat(),
    }
    _write_json_file(_LIVE_EQUITY_BASELINE_PATH, payload)
    return payload


def _iter_trade_records(days: int = 90, mode: Optional[str] = None) -> List[Dict[str, Any]]:
    cutoff_ts = datetime.now(timezone.utc).timestamp() - max(1, int(days)) * 86400
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    out: List[Dict[str, Any]] = []
    signatures = set()

    for pos in position_manager.get_closed_positions(limit=20000, scope=target_mode):
        ts = _safe_dt(getattr(pos, "updated_at", None) or getattr(pos, "opened_at", None))
        if not ts or ts.timestamp() < cutoff_ts:
            continue
        qty = _safe_float(getattr(pos, "quantity", 0.0))
        entry = _safe_float(getattr(pos, "entry_price", 0.0))
        close = _safe_float(getattr(pos, "current_price", 0.0))
        notional = abs(entry * qty)
        out.append(
            {
                "timestamp": ts,
                "symbol": str(getattr(pos, "symbol", "") or ""),
                "strategy": str(getattr(pos, "strategy", "") or "unknown"),
                "pnl": _safe_float(getattr(pos, "realized_pnl", 0.0)),
                "entry_price": entry,
                "close_price": close,
                "quantity": qty,
                "notional": notional,
                "source": "position",
            }
        )
        signatures.add(
            (
                int(ts.timestamp()),
                str(getattr(pos, "symbol", "") or ""),
                round(_safe_float(getattr(pos, "realized_pnl", 0.0)), 6),
                str(getattr(pos, "strategy", "") or "unknown"),
            )
        )

    for row in risk_manager.get_trade_history(limit=30000, scope=target_mode):
        ts = _safe_dt(row.get("timestamp"))
        if not ts or ts.timestamp() < cutoff_ts:
            continue
        symbol = str(row.get("symbol") or "")
        strategy = str(row.get("strategy") or "unknown")
        pnl = _safe_float(row.get("pnl"))
        sig = (int(ts.timestamp()), symbol, round(pnl, 6), strategy)
        if sig in signatures:
            continue
        out.append(
            {
                "timestamp": ts,
                "symbol": symbol,
                "strategy": strategy,
                "pnl": pnl,
                "entry_price": 0.0,
                "close_price": 0.0,
                "quantity": 0.0,
                "notional": abs(_safe_float(row.get("notional"))),
                "source": "risk_trade",
            }
        )
        signatures.add(sig)

    out.sort(
        key=lambda x: _safe_dt(x.get("timestamp"))
        or datetime.min.replace(tzinfo=timezone.utc)
    )
    return out


def _calc_max_streak(values: List[float], positive: bool = True) -> int:
    best = 0
    cur = 0
    for value in values:
        cond = value > 0 if positive else value < 0
        if cond:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return best


def _drawdown_profile(equity: List[float]) -> Dict[str, Any]:
    if not equity:
        return {
            "max_drawdown_usd": 0.0,
            "max_drawdown_pct": 0.0,
            "duration": 0,
            "recovery": 0,
        }
    peak = equity[0]
    peak_idx = 0
    max_dd = 0.0
    max_dd_pct = 0.0
    max_dd_start = 0
    max_dd_end = 0
    for idx, val in enumerate(equity):
        if val >= peak:
            peak = val
            peak_idx = idx
        dd = peak - val
        dd_pct = (dd / peak) if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = dd_pct
            max_dd_start = peak_idx
            max_dd_end = idx

    recovery_idx = max_dd_end
    for idx in range(max_dd_end + 1, len(equity)):
        if equity[idx] >= equity[max_dd_start]:
            recovery_idx = idx
            break

    return {
        "max_drawdown_usd": round(max_dd, 4),
        "max_drawdown_pct": round(max_dd_pct * 100, 4),
        "duration": max(0, max_dd_end - max_dd_start),
        "recovery": max(0, recovery_idx - max_dd_end),
    }


def _var_quantile(returns: List[float], confidence: float) -> float:
    if not returns:
        return 0.0
    series = sorted(float(x) for x in returns)
    q = max(0.0, min(1.0, 1.0 - confidence))
    idx = int(round((len(series) - 1) * q))
    return float(series[idx])


async def _load_symbol_returns(symbol: str, lookback: int = 240) -> pd.Series:
    frames = []
    for ex in ["binance", "gate", "okx"]:
        df = await data_storage.load_klines_from_parquet(
            exchange=ex, symbol=symbol, timeframe="1h"
        )
        if df is not None and not df.empty:
            frames.append(
                df.tail(max(60, int(lookback)))[["close"]].rename(columns={"close": ex})
            )
    if not frames:
        return pd.Series(dtype=float)
    merged = pd.concat(frames, axis=1).ffill().bfill()
    close = merged.iloc[:, 0].astype(float)
    ret = close.pct_change().replace([math.inf, -math.inf], pd.NA).dropna()
    return ret.tail(max(30, int(lookback)))


async def _fetch_orderbook(
    exchange: str, symbol: str, limit: int = 80
) -> Dict[str, Any]:
    normalized = str(exchange or "").lower()
    if normalized == "binance":
        return await _fetch_binance_public_orderbook(symbol=symbol, limit=limit)
    if normalized == "gate":
        gate_payload = await _fetch_gate_public_orderbook(symbol=symbol, limit=limit)
        if bool((gate_payload or {}).get("available")):
            return gate_payload
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return {
            "available": False,
            "error": f"exchange_not_connected:{exchange}",
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    last_error = ""
    for candidate in _exchange_symbol_candidates(exchange, symbol):
        try:
            orderbook = await asyncio.wait_for(
                connector.get_order_book(candidate, limit=max(5, min(int(limit), 200))),
                timeout=_ANALYTICS_ORDERBOOK_TIMEOUT_SEC,
            )
            bids = list((orderbook or {}).get("bids") or [])
            asks = list((orderbook or {}).get("asks") or [])
            if bids and asks:
                return {
                    "available": True,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": (orderbook or {}).get("timestamp"),
                }
            last_error = "orderbook_empty"
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            last_error = f"timeout_or_cancelled:{e}"
        except Exception as e:
            last_error = str(e)
    return {
        "available": False,
        "error": last_error or "orderbook_unavailable",
        "bids": [],
        "asks": [],
        "timestamp": None,
    }


async def _fetch_trade_imbalance(
    exchange: str, symbol: str, limit: int = 600
) -> Dict[str, Any]:
    normalized = str(exchange or "").lower()
    if normalized == "binance":
        return await _fetch_binance_public_trade_imbalance(symbol=symbol, limit=limit)
    if normalized == "gate":
        gate_flow = await _fetch_gate_public_trade_imbalance(symbol=symbol, limit=limit)
        if bool((gate_flow or {}).get("available")):
            return gate_flow
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return {
            "available": False,
            "error": f"exchange_not_connected:{exchange}",
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }
    client = getattr(connector, "_client", None)
    fetch_trades = getattr(client, "fetch_trades", None)
    if not callable(fetch_trades):
        return {
            "available": False,
            "error": "fetch_trades_unavailable",
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }
    trades: List[Dict[str, Any]] = []
    last_error = ""
    for candidate in _exchange_symbol_candidates(exchange, symbol):
        try:
            rows = await asyncio.wait_for(
                fetch_trades(candidate, limit=max(50, min(int(limit), 2000))),
                timeout=_ANALYTICS_TRADE_IMBALANCE_TIMEOUT_SEC,
            )
            rows = list(rows or [])
            if rows:
                trades = rows
                break
            last_error = "trades_empty"
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            last_error = f"timeout_or_cancelled:{e}"
        except Exception as e:
            last_error = str(e)
    if not trades:
        return {
            "available": False,
            "error": last_error or "trades_unavailable",
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }
    buy_volume = 0.0
    sell_volume = 0.0
    for row in trades or []:
        qty = abs(_safe_float(row.get("amount")))
        side = str(row.get("side") or "").lower()
        if side == "buy":
            buy_volume += qty
        elif side == "sell":
            sell_volume += qty
        elif bool(row.get("takerOrMaker")):
            sell_volume += qty
        else:
            buy_volume += qty
    total = buy_volume + sell_volume
    return {
        "available": True,
        "count": len(trades or []),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(
            ((buy_volume - sell_volume) / total) if total > 0 else 0.0, 6
        ),
    }


async def _load_rule_prices() -> Dict[str, float]:
    global _RULE_PRICE_IN_FLIGHT

    now_ts = time.time()
    cached_prices = dict(_RULE_PRICE_CACHE.get("prices") or {})
    cached_ts = float(_RULE_PRICE_CACHE.get("ts") or 0.0)
    if cached_prices and (now_ts - cached_ts) <= _RULE_PRICE_CACHE_TTL_SEC:
        return cached_prices

    in_flight = _RULE_PRICE_IN_FLIGHT
    if in_flight and not in_flight.done():
        try:
            return dict(await asyncio.shield(in_flight))
        except Exception:
            pass

    async def _fetch_prices() -> Dict[str, float]:
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        tasks: List[asyncio.Task] = []
        for exchange_name in ["gate", "binance", "okx"]:
            connector = exchange_manager.get_exchange(exchange_name)
            for symbol in symbols:

                async def _fetch_one(
                    *,
                    _exchange_name: str = exchange_name,
                    _connector: Any = connector,
                    _symbol: str = symbol,
                ) -> tuple[str, str, float]:
                    try:
                        result = await get_realtime_price(
                            _exchange_name,
                            _symbol,
                            connector=_connector,
                            allow_rest_fallback=_connector is not None,
                            max_age_sec=float(
                                getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0)
                                or 10.0
                            ),
                            rest_timeout_sec=_RULE_PRICE_FETCH_TIMEOUT_SEC,
                        )
                        return _exchange_name, _symbol, float(result.price or 0.0) if result.ok else 0.0
                    except Exception as exc:
                        logger.debug(
                            f"rule price fetch failed: exchange={_exchange_name} symbol={_symbol} error={exc}"
                        )
                        return _exchange_name, _symbol, 0.0

                tasks.append(asyncio.create_task(_fetch_one()))

        prices: Dict[str, float] = {}
        if tasks:
            rows = await asyncio.gather(*tasks, return_exceptions=False)
            for _exchange_name, symbol, price in rows:
                if symbol in prices or price <= 0:
                    continue
                prices[symbol] = price

        if cached_prices:
            for symbol, price in cached_prices.items():
                if symbol not in prices and float(price or 0.0) > 0:
                    prices[symbol] = float(price)
        return prices

    task = asyncio.create_task(_fetch_prices())
    _RULE_PRICE_IN_FLIGHT = task
    try:
        prices = dict(await asyncio.shield(task))
        if prices:
            _RULE_PRICE_CACHE["ts"] = time.time()
            _RULE_PRICE_CACHE["prices"] = dict(prices)
        return prices
    finally:
        if _RULE_PRICE_IN_FLIGHT is task:
            _RULE_PRICE_IN_FLIGHT = None


async def _precheck_binance_futures_order(request: OrderRequest) -> None:
    if str(request.exchange or "").lower() != "binance":
        return
    if execution_engine.is_paper_mode():
        return
    if bool(request.reduce_only):
        return

    account_id = str(request.account_id or "main")
    connector = exchange_manager.get_exchange("binance", account_id=account_id)
    if not connector:
        if account_manager.requires_live_connector_isolation(account_id):
            raise HTTPException(
                status_code=400,
                detail=f"Binance live connector unavailable for account {account_id}",
            )
        return
    default_type = str(
        getattr(getattr(connector, "config", None), "default_type", "") or ""
    ).lower()
    if default_type not in {"future", "swap"}:
        return

    px = float(request.price or 0.0)
    if px <= 0:
        try:
            result = await get_realtime_price(
                "binance",
                request.symbol,
                connector=connector,
                allow_rest_fallback=True,
                max_age_sec=float(getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0) or 10.0),
                rest_timeout_sec=2.5,
            )
            px = float(result.price or 0.0) if result.ok else 0.0
        except Exception:
            px = 0.0
    notional = float(request.amount or 0.0) * max(px, 0.0)
    if notional > 0 and notional < 20.0:
        raise HTTPException(
            status_code=400,
            detail=f"Binance 合约最小名义金额为 20 USDT，当前约 {notional:.2f} USDT。请提高数量或价格。",
        )

    try:
        balances = await asyncio.wait_for(connector.get_balance(), timeout=4.0)
        usdt_free = 0.0
        for b in balances or []:
            ccy = str(getattr(b, "currency", "") or "").upper()
            if ccy == "USDT":
                usdt_free = float(getattr(b, "free", 0.0) or 0.0)
                break
        lev = max(1.0, float(request.leverage or 1.0))
        required_margin = (notional / lev) if notional > 0 else 0.0
        if required_margin > 0 and usdt_free > 0 and required_margin > usdt_free * 0.98:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"保证金不足：估算需 {required_margin:.2f} USDT（杠杆 {lev:.1f}x），"
                    f"当前 USDT 可用约 {usdt_free:.2f}。"
                ),
            )
    except HTTPException:
        raise
    except Exception:
        # Non-critical precheck failure should not block actual order path.
        return


async def create_order(request: OrderRequest):
    async with execution_engine.mode_access_guard():
        return await _create_order_locked(request)


async def _create_order_locked(request: OrderRequest):
    mode = str(request.order_mode or "normal").lower()
    timeout_sec = 30.0
    if mode in {"iceberg", "twap", "vwap"}:
        pieces = max(
            1, int(request.iceberg_parts if mode == "iceberg" else request.algo_slices)
        )
        interval_sec = max(0, int(request.algo_interval_sec or 0))
        timeout_sec = min(
            180.0, max(40.0, float(interval_sec * max(0, pieces - 1) + 35))
        )
    elif mode == "conditional":
        timeout_sec = 40.0

    await _precheck_binance_futures_order(request)

    try:
        result = await asyncio.wait_for(
            execution_engine.execute_manual_order(
                exchange=request.exchange,
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                amount=request.amount,
                price=request.price,
                leverage=request.leverage,
                stop_loss=request.stop_loss,
                take_profit=request.take_profit,
                trailing_stop_pct=request.trailing_stop_pct,
                trailing_stop_distance=request.trailing_stop_distance,
                trigger_price=request.trigger_price,
                order_mode=request.order_mode,
                iceberg_parts=request.iceberg_parts,
                algo_slices=request.algo_slices,
                algo_interval_sec=request.algo_interval_sec,
                account_id=request.account_id,
                reduce_only=request.reduce_only,
                strategy="manual",
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        detail = f"下单超时（{int(timeout_sec)}s），请检查交易所连接后重试"
        _schedule_audit_log(
            module="trading",
            action="create_order",
            status="failed",
            message=detail,
            details={**request.model_dump(), "timeout_sec": timeout_sec},
        )
        raise HTTPException(status_code=504, detail=detail)

    if not result:
        risk = risk_manager.get_risk_report()
        raw_error = str(order_manager.get_last_error() or "")
        mapped_error = raw_error
        if "-4164" in raw_error:
            mapped_error = "下单名义金额不足 20 USDT（非 reduce-only）。请提高数量或价格。"
        elif "-2019" in raw_error:
            mapped_error = "保证金不足。请确认 Binance U 本位合约可用余额，并降低数量或提高杠杆。"
        detail = risk.get("halt_reason") or mapped_error or "下单失败，可能触发风控或交易所限制"
        _schedule_audit_log(
            module="trading",
            action="create_order",
            status="failed",
            message=detail,
            details=request.model_dump(),
        )
        raise HTTPException(status_code=400, detail=detail)

    _schedule_audit_log(
        module="trading",
        action="create_order",
        status="success",
        message=f"{request.side} {request.symbol}",
        details={
            **request.model_dump(),
            "order_id": result.get("order_id") or result.get("conditional_id"),
            "filled": result.get("filled"),
        },
    )

    order_id = str(result.get("order_id") or result.get("conditional_id") or "")
    status = str(result.get("status") or "unknown")
    result_price = float(result.get("price") or 0.0)
    result_amount = float(result.get("amount") or request.amount or 0.0)
    result_filled = float(result.get("filled") or 0.0)

    return OrderResponse(
        order_id=order_id,
        status=status,
        symbol=request.symbol,
        side=request.side,
        price=result_price,
        amount=result_amount,
        filled=result_filled,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


async def get_orders(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    include_history: bool = True,
    limit: int = 100,
):
    async with execution_engine.mode_access_guard():
        return await _get_orders_locked(
            symbol=symbol,
            exchange=exchange,
            include_history=include_history,
            limit=limit,
        )


async def _get_orders_locked(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    include_history: bool = True,
    limit: int = 100,
):
    if include_history:
        orders = order_manager.get_recent_orders(
            symbol=symbol,
            exchange=exchange,
            limit=limit,
        )
        if not execution_engine.is_paper_mode():
            orders = [
                o
                for o in orders
                if not (
                    str(getattr(o, "id", "")).startswith("paper_")
                    or bool(
                        order_manager.get_order_metadata(str(getattr(o, "id", ""))).get(
                            "paper"
                        )
                    )
                )
            ]
        return {"orders": [_serialize_order(o) for o in orders]}

    request_limit = max(1, int(limit or 100))
    cache_age = max(
        0.0, time.time() - float(_LIVE_ORDER_DETAILS_CACHE.get("ts") or 0.0)
    )
    cached_orders = list(_LIVE_ORDER_DETAILS_CACHE.get("orders") or [])
    if not execution_engine.is_paper_mode() and (
        exchange is None or str(exchange).lower() == "binance"
    ):
        try:
            fast_orders = await asyncio.wait_for(
                _fetch_binance_open_orders_fast(symbol=symbol),
                timeout=8.0,
            )
            _LIVE_ORDER_DETAILS_CACHE["ts"] = time.time()
            _LIVE_ORDER_DETAILS_CACHE["orders"] = list(fast_orders)
            return {
                "orders": fast_orders[:request_limit],
                "count": len(fast_orders[:request_limit]),
                "source": "binance_futures_rest",
                "cache_fallback": {"used": False},
            }
        except Exception as fast_err:
            reason = str(fast_err) or fast_err.__class__.__name__
            logger.warning(f"[binance] fast open orders fetch failed: {reason}")
            if cached_orders and cache_age <= _LIVE_ORDER_DETAILS_CACHE_TTL_SEC:
                return {
                    "orders": cached_orders[:request_limit],
                    "count": len(cached_orders[:request_limit]),
                    "source": "cache",
                    "cache_fallback": {
                        "used": True,
                        "age_sec": round(cache_age, 2),
                        "reason": reason,
                    },
                }
            return {
                "orders": [],
                "count": 0,
                "source": "binance_futures_rest",
                "cache_fallback": {
                    "used": False,
                    "age_sec": round(cache_age, 2),
                    "reason": reason,
                },
            }

    try:
        orders = await asyncio.wait_for(
            order_manager.get_open_orders(
                symbol=symbol,
                exchange=exchange,
                trading_mode=execution_engine.get_trading_mode(),
            ),
            timeout=4.5,
        )
    except asyncio.TimeoutError:
        if (
            (not execution_engine.is_paper_mode())
            and cached_orders
            and cache_age <= _LIVE_ORDER_DETAILS_CACHE_TTL_SEC
        ):
            return {
                "orders": cached_orders[:request_limit],
                "count": len(cached_orders[:request_limit]),
                "source": "cache",
                "cache_fallback": {
                    "used": True,
                    "age_sec": round(cache_age, 2),
                    "reason": "timeout",
                },
            }
        raise HTTPException(
            status_code=504, detail="Order query timed out. Please retry."
        )
    except Exception as exc:
        if (
            (not execution_engine.is_paper_mode())
            and cached_orders
            and cache_age <= _LIVE_ORDER_DETAILS_CACHE_TTL_SEC
        ):
            return {
                "orders": cached_orders[:request_limit],
                "count": len(cached_orders[:request_limit]),
                "source": "cache",
                "cache_fallback": {
                    "used": True,
                    "age_sec": round(cache_age, 2),
                    "reason": str(exc),
                },
            }
        raise HTTPException(status_code=502, detail=f"Order query failed: {exc}")

    serialized = [_serialize_order(o) for o in orders[:request_limit]]
    if not execution_engine.is_paper_mode():
        _LIVE_ORDER_DETAILS_CACHE["ts"] = time.time()
        _LIVE_ORDER_DETAILS_CACHE["orders"] = list(serialized)
    return {
        "orders": serialized,
        "count": len(serialized),
        "source": "exchange_connector",
        "cache_fallback": {"used": False},
    }


async def get_conditional_orders():
    async with execution_engine.mode_access_guard():
        return await _get_conditional_orders_locked()


async def _get_conditional_orders_locked():
    local_orders = execution_engine.list_conditional_orders()
    if execution_engine.is_paper_mode():
        return {
            "orders": local_orders,
            "count": len(local_orders),
            "source": "local_execution_engine",
            "cache_fallback": {"used": False},
        }

    cache_age = max(
        0.0, time.time() - float(_LIVE_CONDITIONAL_ORDER_CACHE.get("ts") or 0.0)
    )
    cached_orders = list(_LIVE_CONDITIONAL_ORDER_CACHE.get("orders") or [])
    try:
        exchange_open_orders = await asyncio.wait_for(
            _fetch_binance_open_orders_fast(),
            timeout=4.2,
        )
        merged = _merge_conditional_orders(local_orders, exchange_open_orders)
        exchange_conditionals = [
            order
            for order in exchange_open_orders
            if _is_exchange_trigger_order(order)
        ]
        _LIVE_CONDITIONAL_ORDER_CACHE["ts"] = time.time()
        _LIVE_CONDITIONAL_ORDER_CACHE["orders"] = list(exchange_conditionals)
        return {
            "orders": merged,
            "count": len(merged),
            "source": "local_plus_binance_futures_rest",
            "local_count": len(local_orders),
            "exchange_count": len(exchange_conditionals),
            "cache_fallback": {"used": False},
        }
    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        logger.warning(f"[binance] conditional orders fetch failed: {reason}")
        if cached_orders and cache_age <= _LIVE_ORDER_DETAILS_CACHE_TTL_SEC:
            merged = _merge_conditional_orders(local_orders, cached_orders)
            return {
                "orders": merged,
                "count": len(merged),
                "source": "local_plus_cache",
                "local_count": len(local_orders),
                "exchange_count": len(cached_orders),
                "cache_fallback": {
                    "used": True,
                    "age_sec": round(cache_age, 2),
                    "reason": reason,
                },
            }
        return {
            "orders": local_orders,
            "count": len(local_orders),
            "source": "local_execution_engine",
            "local_count": len(local_orders),
            "exchange_count": 0,
            "cache_fallback": {
                "used": False,
                "age_sec": round(cache_age, 2),
                "reason": reason,
            },
        }


async def cancel_conditional_order(conditional_id: str):
    async with execution_engine.mode_access_guard():
        return await _cancel_conditional_order_locked(conditional_id)


async def _cancel_conditional_order_locked(conditional_id: str):
    ok = execution_engine.cancel_conditional_order(conditional_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Conditional order not found")
    return {"success": True, "conditional_id": conditional_id}


async def cancel_order(
    order_id: str,
    symbol: str,
    exchange: str = "binance",
):
    async with execution_engine.mode_access_guard():
        return await _cancel_order_locked(order_id=order_id, symbol=symbol, exchange=exchange)


async def _cancel_order_locked(
    order_id: str,
    symbol: str,
    exchange: str = "binance",
):
    success = await order_manager.cancel_order(order_id, symbol, exchange)
    if success:
        _schedule_audit_log(
            module="trading",
            action="cancel_order",
            status="success",
            message=f"{exchange} {symbol} {order_id}",
            details={"order_id": order_id, "symbol": symbol, "exchange": exchange},
        )
        return {"success": True, "order_id": order_id}
    _schedule_audit_log(
        module="trading",
        action="cancel_order",
        status="failed",
        message=f"{exchange} {symbol} {order_id}",
        details={"order_id": order_id, "symbol": symbol, "exchange": exchange},
    )
    raise HTTPException(status_code=400, detail="Failed to cancel order")


async def cancel_all_orders(
    symbol: Optional[str] = None,
    exchange: str = "binance",
):
    async with execution_engine.mode_access_guard():
        return await _cancel_all_orders_locked(symbol=symbol, exchange=exchange)


async def _cancel_all_orders_locked(
    symbol: Optional[str] = None,
    exchange: str = "binance",
):
    count = await order_manager.cancel_all_orders(
        symbol,
        exchange,
        trading_mode=execution_engine.get_trading_mode(),
    )
    _schedule_audit_log(
        module="trading",
        action="cancel_all_orders",
        status="success",
        message=f"{exchange} cancelled={count}",
        details={"symbol": symbol, "exchange": exchange, "cancelled": count},
    )
    return {"cancelled": count}


async def get_positions(mode: Optional[str] = None):
    async with execution_engine.mode_access_guard():
        return await _get_positions_locked(mode=mode)


async def _get_positions_locked(mode: Optional[str] = None):
    now_ts = time.time()
    cached_positions = list(_LIVE_POSITION_DETAILS_CACHE.get("positions") or [])
    cached_diagnostics = _LIVE_POSITION_DETAILS_CACHE.get("diagnostics")
    cached_ts = float(_LIVE_POSITION_DETAILS_CACHE.get("ts") or 0.0)

    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    local_positions = list(position_manager.get_all_positions(scope=target_mode))
    positions = [p.to_dict() for p in local_positions]
    exchange_positions: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {"fetched_exchanges": [], "skipped_exchanges": []}
    live_mode = target_mode == "live"

    def _canonical_symbol(sym: Any) -> str:
        text = str(sym or "").upper().strip()
        if ":" in text:
            text = text.split(":", 1)[0].strip()
        if "_" in text and "/" not in text:
            left, right = text.split("_", 1)
            text = f"{left}/{right}"
        if text.endswith("USDT") and "/" not in text and len(text) > 4:
            text = f"{text[:-4]}/USDT"
        return text

    def _parse_exchange_position(
        raw: Any, exchange_name: str, *, fallback_used: bool = False
    ) -> Optional[Dict[str, Any]]:
        exchange_key = str(exchange_name or "").strip().lower()
        if not exchange_key:
            return None
        if isinstance(raw, dict):
            raw_exchange = str(raw.get("exchange") or exchange_key).strip().lower()
            if raw_exchange and raw_exchange != exchange_key:
                return None

        symbol = str(
            (raw.get("symbol") if isinstance(raw, dict) else getattr(raw, "symbol", ""))
            or ""
        )
        symbol_key = _canonical_symbol(symbol)
        if not symbol_key:
            return None

        amount = float(
            (
                raw.get("amount")
                if isinstance(raw, dict)
                else getattr(raw, "amount", 0.0)
            )
            or 0.0
        )
        if isinstance(raw, dict) and abs(amount) <= 1e-12:
            amount = float(raw.get("quantity") or 0.0)
        if abs(amount) <= 1e-12:
            return None

        side = (
            str(
                (raw.get("side") if isinstance(raw, dict) else getattr(raw, "side", ""))
                or ""
            )
            .strip()
            .lower()
        )
        if side not in {"long", "short"}:
            side = "short" if amount < 0 else "long"

        entry_px = float(
            (
                raw.get("entry_price")
                if isinstance(raw, dict)
                else getattr(raw, "entry_price", 0.0)
            )
            or 0.0
        )
        current_px = float(
            (
                raw.get("current_price")
                if isinstance(raw, dict)
                else getattr(raw, "current_price", 0.0)
            )
            or 0.0
        )
        unrealized = float(
            (
                raw.get("unrealized_pnl")
                if isinstance(raw, dict)
                else getattr(raw, "unrealized_pnl", 0.0)
            )
            or 0.0
        )
        leverage = float(
            (
                raw.get("leverage")
                if isinstance(raw, dict)
                else getattr(raw, "leverage", 1.0)
            )
            or 1.0
        )
        liquidation_price = (
            raw.get("liquidation_price")
            if isinstance(raw, dict)
            else getattr(raw, "liquidation_price", None)
        )
        value = abs(amount) * (current_px if current_px > 0 else entry_px)

        meta = {
            "source": "exchange_live",
            "liquidation_price": liquidation_price,
            "synced_from_exchange": exchange_name,
        }
        if fallback_used:
            meta["fallback_used"] = True

        return {
            "key": (exchange_key, symbol_key, side),
            "symbol_key": symbol_key,
            "row": {
                "symbol": symbol,
                "exchange": exchange_name,
                "side": side,
                "entry_price": entry_px,
                "current_price": current_px,
                "quantity": abs(amount),
                "value": value,
                "unrealized_pnl": unrealized,
                "unrealized_pnl_pct": 0.0,
                "realized_pnl": 0.0,
                "leverage": leverage,
                "margin": 0.0,
                "opened_at": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "strategy": None,
                "account_id": "exchange_live",
                "metadata": meta,
            },
        }

    def _build_positions_response(
        all_positions: List[Dict[str, Any]],
        exchange_rows: List[Dict[str, Any]],
        diagnostics_payload: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        stats_positions = all_positions
        stats = {
            "position_count": len(stats_positions),
            "total_value": round(
                sum(float(p.get("value") or 0.0) for p in stats_positions), 8
            ),
            "total_unrealized_pnl": round(
                sum(float(p.get("unrealized_pnl") or 0.0) for p in stats_positions), 8
            ),
            "total_realized_pnl": round(
                sum(float(p.get("realized_pnl") or 0.0) for p in stats_positions), 8
            ),
            "long_positions": len(
                [
                    p
                    for p in stats_positions
                    if str(p.get("side") or "").lower() == "long"
                ]
            ),
            "short_positions": len(
                [
                    p
                    for p in stats_positions
                    if str(p.get("side") or "").lower() == "short"
                ]
            ),
            "winning_positions": len(
                [
                    p
                    for p in stats_positions
                    if float(p.get("unrealized_pnl") or 0.0) > 0
                ]
            ),
            "losing_positions": len(
                [
                    p
                    for p in stats_positions
                    if float(p.get("unrealized_pnl") or 0.0) < 0
                ]
            ),
        }
        return {
            "positions": all_positions,
            "stats": stats,
            "exchange_positions_count": len(exchange_rows),
            "diagnostics": diagnostics_payload
            if target_mode == "live"
            else None,
        }

    cache_age_sec = max(0.0, now_ts - cached_ts)
    if (
        live_mode
        and cached_positions
        and cache_age_sec <= _LIVE_POSITION_DETAILS_CACHE_TTL_SEC
    ):
        exchange_symbol_set = set()
        for row in cached_positions:
            exchange_key = str(row.get("exchange", "") or "").strip().lower()
            symbol_key = _canonical_symbol(row.get("symbol"))
            if exchange_key and symbol_key:
                exchange_symbol_set.add((exchange_key, symbol_key))
        if exchange_symbol_set:
            positions = [
                p
                for p in positions
                if (
                    str(p.get("exchange", "")).lower(),
                    _canonical_symbol(p.get("symbol")),
                )
                not in exchange_symbol_set
            ]
        diagnostics = copy.deepcopy(cached_diagnostics or diagnostics)
        diagnostics["cache_hit"] = {
            "used": True,
            "age_sec": round(cache_age_sec, 2),
        }
        exchange_positions = copy.deepcopy(cached_positions)
        return _build_positions_response(
            positions + exchange_positions, exchange_positions, diagnostics
        )

    if live_mode:
        # Include live exchange positions so manually-held futures positions are visible in the UI.
        # In live mode, exchange positions are treated as source-of-truth for the same symbol.
        exchange_keys = set()
        exchange_symbol_set = set()
        exchange_side_set = set()
        fetched_exchange_set = set()
        for exchange_name in exchange_manager.get_connected_exchanges():
            connector = exchange_manager.get_exchange(exchange_name)
            if not connector:
                diagnostics["skipped_exchanges"].append(
                    {"exchange": exchange_name, "reason": "not_connected"}
                )
                continue
            default_type = str(
                getattr(getattr(connector, "config", None), "default_type", "") or ""
            ).lower()
            if default_type not in {"future", "swap"}:
                diagnostics["skipped_exchanges"].append(
                    {
                        "exchange": exchange_name,
                        "reason": f"default_type={default_type or 'unknown'}",
                    }
                )
                continue
            try:
                if exchange_name == "binance":
                    raw_fast_positions = await asyncio.wait_for(
                        _fetch_binance_positions_fast(),
                        timeout=min(_LIVE_POSITION_FETCH_TIMEOUT_SEC, 4.8),
                    )
                    ex_positions = raw_fast_positions or []
                else:
                    ex_positions = await asyncio.wait_for(
                        connector.get_positions(),
                        timeout=min(_LIVE_POSITION_FETCH_TIMEOUT_SEC, 6.5),
                    )
                fetched_exchange_set.add(str(exchange_name or "").lower())
                diagnostics["fetched_exchanges"].append(
                    {
                        "exchange": exchange_name,
                        "count": len(ex_positions),
                        "default_type": default_type,
                    }
                )
                for p in ex_positions:
                    parsed = _parse_exchange_position(
                        p, exchange_name, fallback_used=False
                    )
                    if not parsed:
                        continue
                    key = parsed["key"]
                    if key in exchange_keys:
                        continue
                    exchange_keys.add(key)
                    exchange_symbol_set.add(
                        (str(exchange_name).lower(), parsed["symbol_key"])
                    )
                    exchange_side_set.add(key)
                    exchange_positions.append(parsed["row"])
            except Exception as e:
                if exchange_name == "binance":
                    try:
                        ex_positions = (
                            await asyncio.wait_for(
                                _fetch_binance_positions_via_fallback(),
                                timeout=10.0,
                            )
                            or []
                        )
                        if (
                            not ex_positions
                            and cached_positions
                            and (now_ts - cached_ts)
                            <= _LIVE_POSITION_DETAILS_CACHE_TTL_SEC
                        ):
                            ex_positions = cached_positions
                        fetched_exchange_set.add(str(exchange_name or "").lower())
                        diagnostics["fetched_exchanges"].append(
                            {
                                "exchange": exchange_name,
                                "count": len(ex_positions),
                                "default_type": default_type,
                                "fallback_used": True,
                            }
                        )
                        for p in ex_positions:
                            parsed = _parse_exchange_position(
                                p, exchange_name, fallback_used=True
                            )
                            if not parsed:
                                continue
                            key = parsed["key"]
                            if key in exchange_keys:
                                continue
                            exchange_keys.add(key)
                            exchange_symbol_set.add(
                                (str(exchange_name).lower(), parsed["symbol_key"])
                            )
                            exchange_side_set.add(key)
                            exchange_positions.append(parsed["row"])
                        continue
                    except Exception as fallback_err:
                        diagnostics["skipped_exchanges"].append(
                            {
                                "exchange": exchange_name,
                                "reason": str(fallback_err or e),
                            }
                        )
                        continue
                diagnostics["skipped_exchanges"].append(
                    {"exchange": exchange_name, "reason": str(e)}
                )

        reconciled_local_positions: List[Dict[str, Any]] = []
        if fetched_exchange_set:
            for local_pos in list(local_positions):
                local_exchange = (
                    str(getattr(local_pos, "exchange", "") or "").strip().lower()
                )
                if not local_exchange or local_exchange not in fetched_exchange_set:
                    continue
                source = (
                    str((getattr(local_pos, "metadata", {}) or {}).get("source") or "")
                    .strip()
                    .lower()
                )
                if source == "exchange_live":
                    continue
                local_updated_at = getattr(local_pos, "updated_at", None)
                if isinstance(local_updated_at, datetime):
                    age_sec = max(0.0, now_ts - float(local_updated_at.timestamp()))
                    if age_sec < 20.0:
                        continue
                local_symbol_key = _canonical_symbol(getattr(local_pos, "symbol", ""))
                local_side = (
                    str(getattr(getattr(local_pos, "side", None), "value", "") or "")
                    .strip()
                    .lower()
                )
                if local_side not in {"long", "short"}:
                    continue
                if (local_exchange, local_symbol_key, local_side) in exchange_side_set:
                    continue

                close_price = float(
                    getattr(local_pos, "current_price", 0.0)
                    or getattr(local_pos, "entry_price", 0.0)
                    or 0.0
                )
                if close_price <= 0:
                    close_price = float(getattr(local_pos, "entry_price", 0.0) or 0.0)
                closed = position_manager.close_position(
                    exchange=local_exchange,
                    symbol=str(getattr(local_pos, "symbol", "") or ""),
                    close_price=close_price,
                    quantity=float(getattr(local_pos, "quantity", 0.0) or 0.0),
                    account_id=str(getattr(local_pos, "account_id", "main") or "main"),
                )
                if not closed:
                    continue
                reconciled_local_positions.append(
                    {
                        "exchange": local_exchange,
                        "symbol": closed.symbol,
                        "side": local_side,
                        "account_id": closed.account_id,
                        "reason": "exchange_flat_manual_close",
                    }
                )
                logger.warning(
                    "Reconciled stale local position on positions API read: "
                    f"exchange={local_exchange} symbol={closed.symbol} side={local_side} account_id={closed.account_id}"
                )
            if reconciled_local_positions:
                diagnostics["reconciled_local_positions"] = reconciled_local_positions
                local_positions = list(position_manager.get_all_positions(scope=target_mode))
                positions = [p.to_dict() for p in local_positions]
                _LIVE_POSITION_SNAPSHOT_CACHE["ts"] = 0.0
                _LIVE_POSITION_SNAPSHOT_CACHE["data"] = {}

        if exchange_symbol_set:
            positions = [
                p
                for p in positions
                if (
                    str(p.get("exchange", "")).lower(),
                    _canonical_symbol(p.get("symbol")),
                )
                not in exchange_symbol_set
            ]

    all_positions = positions + exchange_positions
    if exchange_positions:
        _LIVE_POSITION_DETAILS_CACHE["ts"] = now_ts
        _LIVE_POSITION_DETAILS_CACHE["positions"] = list(exchange_positions)
        _LIVE_POSITION_DETAILS_CACHE["diagnostics"] = copy.deepcopy(diagnostics)
    elif (
        live_mode
        and cached_positions
        and (now_ts - cached_ts) <= _LIVE_POSITION_DETAILS_CACHE_TTL_SEC
    ):
        diagnostics = dict(cached_diagnostics or diagnostics)
        diagnostics["cache_fallback"] = {
            "used": True,
            "age_sec": round(max(0.0, now_ts - cached_ts), 2),
        }
        all_positions = positions + cached_positions

    return _build_positions_response(all_positions, exchange_positions, diagnostics)


async def close_position(req: PositionCloseRequest):
    async with execution_engine.mode_access_guard():
        return await _close_position_locked(req)


async def _close_position_locked(req: PositionCloseRequest):
    exchange = str(req.exchange or "").strip().lower()
    symbol = str(req.symbol or "").strip().upper()
    side = str(req.side or "").strip().lower()
    source = str(req.source or "").strip().lower()
    requested_qty = float(req.quantity or 0.0)
    if requested_qty < 0:
        requested_qty = abs(requested_qty)

    if not exchange or not symbol:
        raise HTTPException(status_code=400, detail="exchange/symbol is required")

    # Prefer closing through execution_engine when the position exists in local position_manager
    # so paper/live accounting, risk, and order history remain consistent.
    local_pos = position_manager.get_position(
        exchange, symbol, account_id=req.account_id
    )
    if local_pos and str(local_pos.side.value) == side:
        close_signal = Signal(
            symbol=symbol,
            signal_type=(
                SignalType.CLOSE_LONG if side == "long" else SignalType.CLOSE_SHORT
            ),
            price=float(local_pos.current_price or local_pos.entry_price or 0.0),
            timestamp=datetime.now(timezone.utc),
            strategy_name=str(local_pos.strategy or "manual_ui_close"),
            strength=1.0,
            quantity=float(local_pos.quantity or 0.0),
            metadata={
                "exchange": exchange,
                "account_id": str(local_pos.account_id or req.account_id or "main"),
                "source": "manual_ui_close",
                "requested_from": source or "local",
            },
        )
        result = await execution_engine.execute_signal(close_signal)
        if not result:
            raise HTTPException(
                status_code=400, detail="Failed to close local position"
            )
        _schedule_audit_log(
            module="trading",
            action="close_position",
            status="success",
            message=f"closed local {exchange} {symbol} {side}",
            details={
                "exchange": exchange,
                "symbol": symbol,
                "side": side,
                "quantity": float(local_pos.quantity or 0.0),
                "source": "local",
                "account_id": str(local_pos.account_id or "main"),
            },
        )
        return {
            "ok": True,
            "mode": execution_engine.get_trading_mode(),
            "source": "local",
            "exchange": exchange,
            "symbol": symbol,
            "side": side,
            "result": result,
        }

    # For live-only exchange-synced positions (e.g. manual futures positions not tracked in position_manager),
    # send a reduce-only market order to the exchange.
    if execution_engine.is_paper_mode():
        raise HTTPException(
            status_code=400,
            detail="Paper mode cannot close exchange-synced positions directly",
        )

    close_account_id = str(req.account_id or "main")
    connector = exchange_manager.get_exchange(exchange, account_id=close_account_id)
    if not connector:
        raise HTTPException(
            status_code=404,
            detail=f"Exchange connector not found: {exchange} account_id={close_account_id}",
        )

    default_type = str(
        getattr(getattr(connector, "config", None), "default_type", "") or ""
    ).lower()
    if default_type not in {"future", "swap"}:
        raise HTTPException(
            status_code=400,
            detail=f"{exchange} is not a derivatives exchange (default_type={default_type or 'unknown'})",
        )

    qty = requested_qty
    matched_side = side
    if qty <= 0:
        try:
            ex_positions = await connector.get_positions()
        except Exception as e:
            raise HTTPException(
                status_code=502, detail=f"Failed to fetch exchange positions: {e}"
            ) from e

        norm_symbol = symbol.upper()
        for p in ex_positions:
            p_symbol = str(getattr(p, "symbol", "") or "").upper()
            p_side = str(getattr(p, "side", "") or "").lower()
            p_amt = float(getattr(p, "amount", 0.0) or 0.0)
            if not p_symbol:
                continue
            if p_symbol != norm_symbol:
                continue
            if p_side and p_side != side:
                continue
            if abs(p_amt) <= 0:
                continue
            qty = abs(p_amt)
            matched_side = p_side or ("short" if p_amt < 0 else "long")
            break
    if qty <= 0:
        raise HTTPException(status_code=404, detail="No closable position found")

    close_side = "sell" if matched_side == "long" else "buy"
    order = await order_manager.create_order(
        CoreOrderRequest(
            symbol=symbol,
            side=OrderSide.SELL if close_side == "sell" else OrderSide.BUY,
            order_type=OrderType.MARKET,
            amount=qty,
            price=None,
            exchange=exchange,
            strategy="manual_ui_close",
            account_id=close_account_id,
            reduce_only=True,
            params={"source": "manual_ui_close", "position_side": matched_side},
        )
    )
    if not order:
        raise HTTPException(
            status_code=400, detail="Failed to place exchange close order"
        )

    _schedule_audit_log(
        module="trading",
        action="close_position",
        status="success",
        message=f"reduce-only close {exchange} {symbol} {matched_side}",
        details={
            "exchange": exchange,
            "symbol": symbol,
            "side": matched_side,
            "quantity": qty,
            "source": "exchange_live",
            "order_id": order.id,
            "reduce_only": True,
        },
    )
    return {
        "ok": True,
        "mode": execution_engine.get_trading_mode(),
        "source": "exchange_live",
        "exchange": exchange,
        "symbol": symbol,
        "side": matched_side,
        "quantity": qty,
        "order": {
            "id": order.id,
            "status": getattr(
                getattr(order, "status", None),
                "value",
                str(getattr(order, "status", "")),
            ),
            "price": float(getattr(order, "price", 0.0) or 0.0),
            "amount": float(getattr(order, "amount", 0.0) or qty),
            "filled": float(getattr(order, "filled", 0.0) or 0.0),
        },
    }


def _session_name(ts: datetime) -> str:
    hour = int(ts.hour)
    if 0 <= hour < 8:
        return "亚盘"
    if 8 <= hour < 16:
        return "欧盘"
    return "美盘"


def _parse_target_allocations(raw: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for part in str(raw or "").split(","):
        item = part.strip()
        if not item or ":" not in item:
            continue
        k, v = item.split(":", 1)
        sym = k.strip().upper()
        if not sym:
            continue
        out[sym] = max(0.0, _safe_float(v))
    total = sum(out.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in out.items()}


async def _estimate_atr_for_symbol(symbol: str, period: int = 14) -> Optional[float]:
    period = max(3, min(int(period or 14), 200))
    for ex in ["binance", "gate", "okx"]:
        df = await data_storage.load_klines_from_parquet(
            exchange=ex, symbol=symbol, timeframe="1h"
        )
        if df is None or df.empty or len(df) < (period + 5):
            continue
        src = df.tail(period * 4).copy()
        high = pd.to_numeric(src["high"], errors="coerce")
        low = pd.to_numeric(src["low"], errors="coerce")
        close = pd.to_numeric(src["close"], errors="coerce")
        tr = pd.concat(
            [
                (high - low),
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(period, min_periods=period).mean().dropna()
        if not atr.empty:
            return _safe_float(atr.iloc[-1], default=0.0)
    return None


async def _fetch_whale_transfers(
    min_btc: float = _ANALYTICS_WHALE_MIN_BTC,
) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=_ANALYTICS_WHALE_TIMEOUT_SEC, verify=get_shared_ssl_context()) as client:
            tx_res, px_res = await asyncio.gather(
                client.get(
                    "https://blockchain.info/unconfirmed-transactions?format=json"
                ),
                client.get(
                    "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
                ),
            )
            tx_res.raise_for_status()
            px_res.raise_for_status()
            tx_json = tx_res.json() or {}
            px_json = px_res.json() or {}
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        return {
            "available": False,
            "error": f"whale_timeout:{type(e).__name__}",
            "source_name": "public_chain_proxy",
            "count": 0,
            "transactions": [],
        }
    except Exception as e:
        return {
            "available": False,
            "error": str(e),
            "source_name": "public_chain_proxy",
            "count": 0,
            "transactions": [],
        }

    btc_price = _safe_float(px_json.get("price"), default=0.0)
    candidates = []
    for tx in (tx_json.get("txs") or [])[:500]:
        out_value_satoshi = sum(
            _safe_float(v.get("value")) for v in (tx.get("out") or [])
        )
        btc_amount = out_value_satoshi / 1e8
        ts = int(_safe_float(tx.get("time"), default=0))
        candidates.append(
            {
                "hash": tx.get("hash"),
                "btc": round(btc_amount, 6),
                "usd_estimate": round(btc_amount * btc_price, 2)
                if btc_price > 0
                else None,
                "timestamp": _to_utc_iso(datetime.utcfromtimestamp(ts))
                if ts > 0
                else None,
            }
        )
    requested_threshold = float(max(1.0, min_btc))
    effective_threshold = requested_threshold
    whales = [
        item
        for item in candidates
        if _safe_float(item.get("btc")) >= effective_threshold
    ]
    if not whales and requested_threshold > _ANALYTICS_WHALE_MIN_BTC:
        effective_threshold = _ANALYTICS_WHALE_MIN_BTC
        whales = [
            item
            for item in candidates
            if _safe_float(item.get("btc")) >= effective_threshold
        ]
    whales.sort(key=lambda x: _safe_float(x.get("btc")), reverse=True)
    return {
        "available": True,
        "source_name": "public_chain_proxy",
        "threshold_btc": float(effective_threshold),
        "requested_threshold_btc": float(requested_threshold),
        "btc_price": btc_price,
        "count": len(whales),
        "transactions": whales[:30],
    }


async def _fetch_binance_announcements(limit: int = 6) -> List[Dict[str, Any]]:
    announcements: List[Dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(
            timeout=_ANALYTICS_ANNOUNCEMENT_TIMEOUT_SEC,
            verify=get_shared_ssl_context(),
        ) as client:
            resp = await client.get(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                params={
                    "type": 1,
                    "catalogId": 48,
                    "pageNo": 1,
                    "pageSize": max(1, min(int(limit), 12)),
                },
            )
            if resp.status_code != 200:
                return announcements
            rows = ((resp.json() or {}).get("data") or {}).get("articles") or []
            for row in rows[:limit]:
                announcements.append(
                    {
                        "title": row.get("title"),
                        "code": row.get("code"),
                        "release_date": row.get("releaseDate"),
                    }
                )
    except Exception:
        return announcements
    return announcements


def _symbol_base(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if ":" in text:
        text = text.split(":", 1)[0]
    if "/" in text:
        text = text.split("/", 1)[0]
    return text


def _strip_html_text(value: Any) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return " ".join(unescape(text).split())


def _parse_usd_amount(value: Any) -> Optional[float]:
    text = str(value or "")
    match = re.search(r"\$[\s]*([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except Exception:
        return None


def _security_alert_severity(amount_usd: Optional[float]) -> str:
    if amount_usd is None:
        return "medium"
    if amount_usd >= 10_000_000:
        return "critical"
    if amount_usd >= 1_000_000:
        return "high"
    if amount_usd >= 100_000:
        return "medium"
    return "low"


def _security_aliases_for_symbol(symbol: str) -> List[str]:
    base = _symbol_base(symbol)
    aliases = {base.lower()} if base else set()
    aliases.update(_SLOWMIST_SYMBOL_ALIASES.get(base, []))
    return [alias.lower() for alias in aliases if str(alias or "").strip()]


def _parse_slowmist_security_events(html: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for raw_item in re.findall(
        r"<li>\s*<span class=\"time\">.*?</li>", str(html or ""), flags=re.S
    ):
        date_match = re.search(
            r"<span class=\"time\">(.*?)</span>", raw_item, flags=re.S
        )
        target_match = re.search(
            r"<h3><em>Hacked target:\s*</em>(.*?)</h3>", raw_item, flags=re.S
        )
        desc_match = re.search(
            r"<p><em>Description of the event:\s*</em>(.*?)</p>",
            raw_item,
            flags=re.S,
        )
        amount_match = re.search(
            r"<span><em>Amount of loss:\s*</em>(.*?)</span>",
            raw_item,
            flags=re.S,
        )
        method_match = re.search(
            r"<span><em>Attack method:\s*</em>(.*?)</span>",
            raw_item,
            flags=re.S,
        )
        ref_match = re.search(
            r"<p class=\"link-reference\"><a href=\"(.*?)\"", raw_item, flags=re.S
        )

        date_text = _strip_html_text(date_match.group(1) if date_match else "")
        target = _strip_html_text(target_match.group(1) if target_match else "")
        description = _strip_html_text(desc_match.group(1) if desc_match else "")
        amount_label = _strip_html_text(amount_match.group(1) if amount_match else "")
        attack_method = _strip_html_text(method_match.group(1) if method_match else "")
        amount_usd = _parse_usd_amount(amount_label)
        severity = _security_alert_severity(amount_usd)
        reference_url = str(ref_match.group(1) if ref_match else "").strip() or None

        occurred_at = None
        try:
            occurred_at = (
                datetime.strptime(date_text, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .isoformat()
            )
        except Exception:
            occurred_at = None

        if not (target or description):
            continue
        events.append(
            {
                "title": target or "Security incident",
                "target": target,
                "description": description,
                "amount_usd": round(amount_usd, 2) if amount_usd is not None else None,
                "amount_label": amount_label or None,
                "attack_method": attack_method or None,
                "severity": severity,
                "occurred_at": occurred_at,
                "source": "slowmist_hacked",
                "reference_url": reference_url,
            }
        )
    return events


def _filter_security_events_for_symbol(
    events: List[Dict[str, Any]], symbol: str
) -> tuple[List[Dict[str, Any]], str]:
    aliases = _security_aliases_for_symbol(symbol)
    if not aliases:
        return list(events or []), "global_fallback"

    matched: List[Dict[str, Any]] = []
    for item in list(events or []):
        haystack = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("target") or ""),
                str(item.get("description") or ""),
                str(item.get("attack_method") or ""),
            ]
        ).lower()
        if any(alias in haystack for alias in aliases):
            matched.append(dict(item))

    if matched:
        return matched, "symbol"
    return list(events or []), "global_fallback"


def _strip_security_alert_runtime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(payload or {}).items()
        if key
        not in {"cache_hit", "cache_age_sec", "stale", "stale_reason", "source_status"}
    }


def _with_security_alert_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(_strip_security_alert_runtime_fields(payload))
    out["cache_hit"] = bool(cache_hit)
    out["cache_age_sec"] = (
        round(float(cache_age_sec or 0.0), 3) if cache_age_sec is not None else None
    )
    out["stale"] = bool(stale)
    out["source_status"] = str(
        source_status
        or ("cache_stale" if stale else ("cache_fresh" if cache_hit else "live"))
    )
    if stale_reason:
        out["stale_reason"] = str(stale_reason)
        stale_note = "SlowMist Hacked 刷新失败，已回退到最近一次成功快照"
        note_parts = [str(out.get("note") or "").strip(), stale_note]
        out["note"] = "；".join(dict.fromkeys(part for part in note_parts if part))
    else:
        out.pop("stale_reason", None)
    return out


async def _fetch_slowmist_security_alerts(
    symbol: str, limit: int = 6
) -> Dict[str, Any]:
    cache_key = f"{_symbol_base(symbol)}|{max(1, min(int(limit or 6), 12))}"
    cached, cached_age = _cache_get(
        _SECURITY_ALERTS_CACHE,
        cache_key,
        max_age_sec=_SECURITY_ALERTS_CACHE_TTL_SEC,
    )
    if cached is not None:
        return _with_security_alert_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )
    stale_cached, stale_age = _cache_get(
        _SECURITY_ALERTS_CACHE,
        cache_key,
        max_age_sec=_SECURITY_ALERTS_STALE_MAX_AGE_SEC,
    )

    try:
        async with httpx.AsyncClient(
            timeout=_ANALYTICS_SECURITY_ALERT_TIMEOUT_SEC,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
            trust_env=True,
            verify=get_shared_ssl_context(),
        ) as client:
            resp = await client.get(_SLOWMIST_HACKED_URL)
            resp.raise_for_status()
            html = resp.text
    except Exception as exc:
        if stale_cached is not None:
            return _with_security_alert_runtime_fields(
                stale_cached,
                cache_hit=True,
                cache_age_sec=stale_age,
                stale=True,
                stale_reason=exc,
                source_status="cache_stale",
            )
        return {
            "available": False,
            "source": "slowmist_hacked",
            "scope": "global_fallback",
            "error": str(exc),
            "events": [],
            "cache_hit": False,
            "cache_age_sec": None,
            "stale": False,
            "source_status": "unavailable",
        }

    parsed_events = _parse_slowmist_security_events(html)
    if not parsed_events and stale_cached is not None:
        return _with_security_alert_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            stale_reason="empty_parse",
            source_status="cache_stale",
        )
    cutoff = datetime.now(timezone.utc) - timedelta(days=45)
    recent_events = []
    for item in parsed_events:
        occurred_at = _safe_dt(item.get("occurred_at"))
        if occurred_at and occurred_at < cutoff:
            continue
        recent_events.append(dict(item))

    recent_events.sort(
        key=lambda item: (
            _safe_dt(item.get("occurred_at"))
            or datetime.min.replace(tzinfo=timezone.utc),
            _safe_float(item.get("amount_usd")),
        ),
        reverse=True,
    )
    filtered_events, scope = _filter_security_events_for_symbol(recent_events, symbol)
    selected_events = filtered_events[: max(1, min(int(limit or 6), 12))]

    payload = {
        "available": bool(selected_events),
        "source": "slowmist_hacked",
        "scope": scope,
        "error": "",
        "events": selected_events,
        "query_symbol": _symbol_base(symbol),
        "total_recent": len(recent_events),
    }
    _cache_put(
        _SECURITY_ALERTS_CACHE, cache_key, _strip_security_alert_runtime_fields(payload)
    )
    return _with_security_alert_runtime_fields(
        payload,
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


def _coinglass_ts_to_iso(value: Any) -> Optional[str]:
    ts = _safe_dt(value)
    if ts is None:
        numeric = int(_safe_float(value))
        if numeric > 0:
            with contextlib.suppress(Exception):
                ts = datetime.fromtimestamp(numeric, tz=timezone.utc)
    if ts is None:
        return None
    return ts.astimezone(timezone.utc).isoformat()


def _calendar_importance_label(value: Any) -> str:
    level = int(_safe_float(value))
    if level >= 3:
        return "high"
    if level >= 2:
        return "medium"
    return "low"


def _calendar_importance_rank(value: Any) -> int:
    text = str(value or "").strip().lower()
    if text == "critical":
        return 4
    if text == "high":
        return 3
    if text == "medium":
        return 2
    if text == "low":
        return 1
    return max(0, min(4, int(_safe_float(value))))


def _calendar_sort_key(item: Dict[str, Any]) -> tuple[str, int, int, float]:
    category_rank = {
        "economic": 0,
        "central_bank": 1,
        "unlock": 2,
        "expiry": 3,
    }.get(str(item.get("category") or "").strip().lower(), 9)
    return (
        str(item.get("time_utc") or ""),
        category_rank,
        -_calendar_importance_rank(item.get("importance")),
        -_safe_float(item.get("amount_usd")),
    )


def _dedupe_calendar_events(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in list(items or []):
        row = dict(item or {})
        key = (
            str(row.get("category") or "").strip().lower(),
            str(row.get("time_utc") or "").strip(),
            str(row.get("name") or row.get("title") or "").strip().lower(),
        )
        if not key[1] or not key[2] or key in seen:
            continue
        seen.add(key)
        out.append(row)
    out.sort(key=_calendar_sort_key)
    return out


def _calendar_dt_utc(value: Any) -> Optional[datetime]:
    dt = _safe_dt(value)
    if dt is None:
        return None
    return (
        dt.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None
        else dt.astimezone(timezone.utc)
    )


def _calendar_note_parts(*parts: Any) -> Optional[str]:
    values = [str(part or "").strip() for part in parts if str(part or "").strip()]
    return " | ".join(values) if values else None


def _build_microstructure_orderbook_view(ob: Dict[str, Any]) -> Dict[str, Any]:
    bids = [
        [_safe_float(x[0]), _safe_float(x[1])]
        for x in (ob.get("bids") or [])
        if len(x) >= 2
    ]
    asks = [
        [_safe_float(x[0]), _safe_float(x[1])]
        for x in (ob.get("asks") or [])
        if len(x) >= 2
    ]
    bids = [x for x in bids if x[0] > 0 and x[1] > 0]
    asks = [x for x in asks if x[0] > 0 and x[1] > 0]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
    mid = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0

    bid_depth: List[Dict[str, float]] = []
    ask_depth: List[Dict[str, float]] = []
    cumulative = 0.0
    for price, qty in bids[:100]:
        cumulative += qty
        bid_depth.append(
            {
                "price": round(price, 8),
                "qty": round(qty, 8),
                "cum_qty": round(cumulative, 8),
            }
        )
    cumulative = 0.0
    for price, qty in asks[:100]:
        cumulative += qty
        ask_depth.append(
            {
                "price": round(price, 8),
                "qty": round(qty, 8),
                "cum_qty": round(cumulative, 8),
            }
        )

    all_sizes = sorted([x[1] for x in bids + asks])
    size_threshold = all_sizes[int(len(all_sizes) * 0.95)] if all_sizes else 0.0
    large_orders: List[Dict[str, Any]] = []
    for side, rows in [("bid", bids), ("ask", asks)]:
        for price, qty in rows[:200]:
            if qty >= size_threshold and size_threshold > 0:
                large_orders.append(
                    {
                        "side": side,
                        "price": round(price, 8),
                        "qty": round(qty, 8),
                        "notional": round(price * qty, 4),
                    }
                )
    large_orders = sorted(large_orders, key=lambda x: x["notional"], reverse=True)[:30]

    iceberg_candidates = 0
    for rows in [bids[:60], asks[:60]]:
        prev_qty = None
        repeat = 0
        for _, qty in rows:
            if prev_qty is not None and abs(qty - prev_qty) <= max(
                1e-9, prev_qty * 0.003
            ):
                repeat += 1
            prev_qty = qty
        if repeat >= 3:
            iceberg_candidates += 1

    return {
        "orderbook": {
            "available": bool(ob.get("available", True)) and mid > 0,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": round(mid, 8),
            "spread": round(spread, 8),
            "spread_bps": round((spread / mid * 10000) if mid > 0 else 0.0, 6),
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
        },
        "large_orders": large_orders,
        "iceberg_candidate_count": iceberg_candidates,
    }


def _is_major_calendar_country(country_code: Any, country_name: Any) -> bool:
    code = str(country_code or "").strip().upper()
    if code in _COINGLASS_CALENDAR_MAJOR_COUNTRY_CODES:
        return True
    name = str(country_name or "").strip().lower()
    return any(
        token in name
        for token in (
            "united states",
            "america",
            "美国",
            "china",
            "中国",
            "euro",
            "欧元",
            "japan",
            "日本",
            "united kingdom",
            "britain",
            "英国",
            "canada",
            "加拿大",
            "australia",
            "澳大利亚",
            "switzerland",
            "瑞士",
        )
    )


def _is_market_moving_calendar_name(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    compact = text.replace(" ", "")
    return any(
        keyword in text or keyword.replace(" ", "") in compact
        for keyword in _COINGLASS_CALENDAR_PRIORITY_KEYWORDS
    )


def _normalize_coinglass_economic_calendar_item(row: Dict[str, Any]) -> Dict[str, Any]:
    country_name = str(row.get("country_name") or "").strip()
    country_code = str(row.get("country_code") or "").strip().upper()
    event_name = str(row.get("calendar_name") or "").strip()
    if not event_name:
        return {}
    timestamp = _coinglass_ts_to_iso(row.get("publish_timestamp"))
    if not timestamp:
        return {}
    note = _calendar_note_parts(
        f"影响 {str(row.get('data_effect') or '').strip()}",
        f"前值 {str(row.get('previous_value') or '').strip()}",
        f"预期 {str(row.get('forecast_value') or '').strip()}",
        f"公布 {str(row.get('published_value') or '').strip()}",
    )
    return {
        "category": "economic",
        "name": " ".join(part for part in (country_name, event_name) if part).strip(),
        "time_utc": timestamp,
        "importance": _calendar_importance_label(row.get("importance_level")),
        "source": "coinglass_economic_data",
        "provider": "coinglass_economic_data",
        "country_code": country_code or None,
        "country_name": country_name or None,
        "event_name": event_name,
        "has_exact_publish_time": bool(
            int(_safe_float(row.get("has_exact_publish_time")))
        ),
        "note": note,
    }


def _should_keep_coinglass_economic_event(item: Dict[str, Any]) -> bool:
    importance_rank = _calendar_importance_rank(item.get("importance"))
    if importance_rank >= 3:
        return True
    if importance_rank < 2:
        return False
    if not _is_major_calendar_country(
        item.get("country_code"), item.get("country_name")
    ):
        return False
    return _is_market_moving_calendar_name(item.get("event_name") or item.get("name"))


def _normalize_coinglass_central_bank_calendar_item(
    row: Dict[str, Any]
) -> Dict[str, Any]:
    country_name = str(row.get("country_name") or "").strip()
    country_code = str(row.get("country_code") or "").strip().upper()
    event_name = str(row.get("calendar_name") or "").strip()
    if not event_name:
        return {}
    timestamp = _coinglass_ts_to_iso(row.get("publish_timestamp"))
    if not timestamp:
        return {}
    return {
        "category": "central_bank",
        "name": " ".join(part for part in (country_name, event_name) if part).strip(),
        "time_utc": timestamp,
        "importance": _calendar_importance_label(row.get("importance_level")),
        "source": "coinglass_central_bank",
        "provider": "coinglass_central_bank",
        "country_code": country_code or None,
        "country_name": country_name or None,
        "event_name": event_name,
        "has_exact_publish_time": bool(
            int(_safe_float(row.get("has_exact_publish_time")))
        ),
        "note": "央行活动",
    }


def _should_keep_coinglass_central_bank_event(item: Dict[str, Any]) -> bool:
    importance_rank = _calendar_importance_rank(item.get("importance"))
    if importance_rank >= 3:
        return True
    return importance_rank >= 2 and _is_major_calendar_country(
        item.get("country_code"), item.get("country_name")
    )


def _unlock_event_importance(
    amount_usd: Optional[float],
    circulating_pct: Optional[float],
    supply_pct: Optional[float],
) -> str:
    if (
        (amount_usd is not None and amount_usd >= 50_000_000)
        or (circulating_pct is not None and circulating_pct >= 5.0)
        or (supply_pct is not None and supply_pct >= 2.0)
    ):
        return "high"
    if (
        (amount_usd is not None and amount_usd >= 10_000_000)
        or (circulating_pct is not None and circulating_pct >= 1.0)
        or (supply_pct is not None and supply_pct >= 0.5)
    ):
        return "medium"
    return "low"


def _normalize_coinglass_unlock_calendar_item(row: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    project_name = str(row.get("name") or "").strip()
    if not symbol:
        return {}
    timestamp = _coinglass_ts_to_iso(row.get("next_unlock_date"))
    if not timestamp:
        return {}
    amount_usd = _optional_finite_float(row.get("next_unlock_usd"))
    circulating_pct = _optional_finite_float(row.get("next_unlock_of_circulating"))
    supply_pct = _optional_finite_float(row.get("next_unlock_of_supply"))
    importance = _unlock_event_importance(amount_usd, circulating_pct, supply_pct)
    note = _calendar_note_parts(
        f"项目 {project_name}",
        f"约 ${amount_usd:,.0f}" if amount_usd is not None else None,
        f"流通占比 {circulating_pct:.2f}%" if circulating_pct is not None else None,
        f"总供给占比 {supply_pct:.2f}%" if supply_pct is not None else None,
    )
    return {
        "category": "unlock",
        "name": f"{symbol} 代币解锁",
        "time_utc": timestamp,
        "importance": importance,
        "source": "coinglass_unlock_list",
        "provider": "coinglass_unlock_list",
        "symbol": symbol,
        "project_name": project_name or None,
        "amount_usd": round(amount_usd, 2) if amount_usd is not None else None,
        "unlock_of_circulating_pct": round(circulating_pct, 4)
        if circulating_pct is not None
        else None,
        "unlock_of_supply_pct": round(supply_pct, 4)
        if supply_pct is not None
        else None,
        "note": note,
    }


def _coinglass_payload_rows(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = dict((response or {}).get("payload") or {})
    rows = payload.get("data")
    return [dict(item or {}) for item in list(rows or []) if isinstance(item, dict)]


async def _fetch_coinglass_economic_calendar_events(
    *, days: int, language: str = "zh"
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "source": "coinglass_economic_data",
            "error": "coinglass_disabled",
            "events": [],
            "coverage_end": None,
        }

    now = datetime.now(timezone.utc)
    lookahead_days = max(1, min(int(days or 1), _COINGLASS_CALENDAR_LOOKAHEAD_DAYS))
    coverage_end = now + timedelta(days=lookahead_days)
    try:
        async with CoinglassClient(
            timeout_sec=_ANALYTICS_CALENDAR_TIMEOUT_SEC
        ) as client:
            response = await client.request_json(
                "/v4/api/calendar/economic-data",
                params={
                    "language": str(language or "zh"),
                    "start_time": int(now.timestamp() * 1000),
                    "end_time": int(coverage_end.timestamp() * 1000),
                },
                manual=False,
            )
    except Exception as exc:
        return {
            "available": False,
            "source": "coinglass_economic_data",
            "error": str(exc),
            "events": [],
            "coverage_end": coverage_end.isoformat(),
        }

    events: List[Dict[str, Any]] = []
    for row in _coinglass_payload_rows(response):
        event = _normalize_coinglass_economic_calendar_item(row)
        if not event or not _should_keep_coinglass_economic_event(event):
            continue
        event_dt = _calendar_dt_utc(event.get("time_utc"))
        if not event_dt or event_dt < now or event_dt > coverage_end:
            continue
        events.append(event)
    events = _dedupe_calendar_events(events)[:40]
    return {
        "available": bool(events),
        "source": "coinglass_economic_data",
        "error": "",
        "events": events,
        "coverage_end": coverage_end.isoformat(),
    }


async def _fetch_coinglass_central_bank_calendar_events(
    *, days: int, language: str = "zh"
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "source": "coinglass_central_bank",
            "error": "coinglass_disabled",
            "events": [],
            "coverage_end": None,
        }

    now = datetime.now(timezone.utc)
    lookahead_days = max(1, min(int(days or 1), _COINGLASS_CALENDAR_LOOKAHEAD_DAYS))
    coverage_end = now + timedelta(days=lookahead_days)
    try:
        async with CoinglassClient(
            timeout_sec=_ANALYTICS_CALENDAR_TIMEOUT_SEC
        ) as client:
            response = await client.request_json(
                "/v4/api/calendar/central-bank-activities",
                params={
                    "language": str(language or "zh"),
                    "start_time": int(now.timestamp() * 1000),
                    "end_time": int(coverage_end.timestamp() * 1000),
                },
                manual=False,
            )
    except Exception as exc:
        return {
            "available": False,
            "source": "coinglass_central_bank",
            "error": str(exc),
            "events": [],
            "coverage_end": coverage_end.isoformat(),
        }

    events: List[Dict[str, Any]] = []
    for row in _coinglass_payload_rows(response):
        event = _normalize_coinglass_central_bank_calendar_item(row)
        if not event or not _should_keep_coinglass_central_bank_event(event):
            continue
        event_dt = _calendar_dt_utc(event.get("time_utc"))
        if not event_dt or event_dt < now or event_dt > coverage_end:
            continue
        events.append(event)
    events = _dedupe_calendar_events(events)[:24]
    return {
        "available": bool(events),
        "source": "coinglass_central_bank",
        "error": "",
        "events": events,
        "coverage_end": coverage_end.isoformat(),
    }


async def _fetch_coinglass_unlock_calendar_events(*, days: int) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "source": "coinglass_unlock_list",
            "error": "coinglass_disabled",
            "events": [],
        }

    now = datetime.now(timezone.utc)
    end = now + timedelta(days=max(1, min(int(days or 1), 180)))
    events: List[Dict[str, Any]] = []
    try:
        async with CoinglassClient(
            timeout_sec=_ANALYTICS_CALENDAR_TIMEOUT_SEC
        ) as client:
            for page in range(1, _COINGLASS_UNLOCK_MAX_PAGES + 1):
                response = await client.request_json(
                    "/v4/api/coin/unlock-list",
                    params={"page": page, "per_page": 100},
                    manual=False,
                )
                rows = _coinglass_payload_rows(response)
                if not rows:
                    break

                page_dates: List[datetime] = []
                for row in rows:
                    event = _normalize_coinglass_unlock_calendar_item(row)
                    if not event:
                        continue
                    event_dt = _calendar_dt_utc(event.get("time_utc"))
                    if event_dt is None:
                        continue
                    page_dates.append(event_dt)
                    if event_dt < now or event_dt > end:
                        continue
                    events.append(event)

                if page_dates and min(page_dates) > end:
                    break
    except Exception as exc:
        return {
            "available": False,
            "source": "coinglass_unlock_list",
            "error": str(exc),
            "events": [],
        }

    deduped = _dedupe_calendar_events(events)
    significant = [
        item
        for item in deduped
        if _calendar_importance_rank(item.get("importance")) >= 2
    ]
    selected = significant or deduped
    selected.sort(
        key=lambda item: (
            str(item.get("time_utc") or ""),
            -_safe_float(item.get("amount_usd")),
            -_calendar_importance_rank(item.get("importance")),
        )
    )
    selected = selected[: max(6, min(int(days or 1), 12))]
    return {
        "available": bool(selected),
        "source": "coinglass_unlock_list",
        "error": "",
        "events": selected,
    }


def _build_internal_estimate_calendar_events(
    *,
    now: datetime,
    end: datetime,
    start: Optional[datetime] = None,
    include_economic: bool = True,
    include_unlocks: bool = True,
    include_expiry: bool = True,
) -> List[Dict[str, Any]]:
    window_start = start or now
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    events: List[Dict[str, Any]] = []

    if include_economic:
        month_cursor = datetime(
            window_start.year, window_start.month, 1, tzinfo=timezone.utc
        )
        while month_cursor <= end:
            cpi_day = datetime(
                month_cursor.year, month_cursor.month, 12, 13, 30, tzinfo=timezone.utc
            )
            while cpi_day.weekday() >= 5:
                cpi_day += timedelta(days=1)
            if window_start <= cpi_day <= end:
                events.append(
                    {
                        "category": "economic",
                        "name": "美国 CPI（预估）",
                        "time_utc": cpi_day.isoformat(),
                        "importance": "high",
                        "source": "internal_estimate",
                        "provider": "internal_estimate",
                        "note": "内置估算日历",
                    }
                )

            first_day = datetime(
                month_cursor.year, month_cursor.month, 1, 13, 30, tzinfo=timezone.utc
            )
            offset = (4 - first_day.weekday()) % 7
            nfp_day = first_day + timedelta(days=offset)
            if window_start <= nfp_day <= end:
                events.append(
                    {
                        "category": "economic",
                        "name": "美国非农就业（预估）",
                        "time_utc": nfp_day.isoformat(),
                        "importance": "high",
                        "source": "internal_estimate",
                        "provider": "internal_estimate",
                        "note": "内置估算日历",
                    }
                )
            if month_cursor.month == 12:
                month_cursor = datetime(
                    month_cursor.year + 1, 1, 1, tzinfo=timezone.utc
                )
            else:
                month_cursor = datetime(
                    month_cursor.year, month_cursor.month + 1, 1, tzinfo=timezone.utc
                )

        fomc_2026 = [
            "2026-03-18T18:00:00",
            "2026-04-29T18:00:00",
            "2026-06-17T18:00:00",
            "2026-07-29T18:00:00",
            "2026-09-16T18:00:00",
            "2026-10-28T18:00:00",
            "2026-12-09T18:00:00",
        ]
        for item in fomc_2026:
            dt = _calendar_dt_utc(item)
            if dt and window_start <= dt <= end:
                events.append(
                    {
                        "category": "economic",
                        "name": "FOMC 利率决议（预估）",
                        "time_utc": dt.isoformat(),
                        "importance": "high",
                        "source": "internal_estimate",
                        "provider": "internal_estimate",
                        "note": "内置估算日历",
                    }
                )

    if include_unlocks:
        unlock_templates = [
            ("APT", 20),
            ("SUI", 25),
            ("ARB", 28),
            ("OP", 21),
        ]
        for token, base_day in unlock_templates:
            dt = datetime(
                window_start.year,
                window_start.month,
                min(base_day, 28),
                8,
                0,
                tzinfo=timezone.utc,
            )
            for _ in range(6):
                if dt < window_start:
                    dt = (dt + timedelta(days=32)).replace(day=min(base_day, 28))
                    continue
                if dt > end:
                    break
                events.append(
                    {
                        "category": "unlock",
                        "name": f"{token} 代币解锁（估算）",
                        "time_utc": dt.isoformat(),
                        "importance": "medium",
                        "source": "internal_estimate",
                        "provider": "internal_estimate",
                        "symbol": token,
                        "note": "内置估算日历",
                    }
                )
                dt = (dt + timedelta(days=32)).replace(day=min(base_day, 28))

    if include_expiry:
        expiry = window_start.replace(hour=8, minute=0, second=0, microsecond=0)
        for _ in range(24):
            while expiry.weekday() != 4:
                expiry += timedelta(days=1)
            if expiry > end:
                break
            if expiry >= window_start:
                events.append(
                    {
                        "category": "expiry",
                        "name": "周五交割 / 到期提醒",
                        "time_utc": expiry.isoformat(),
                        "importance": "medium",
                        "source": "internal_estimate",
                        "provider": "internal_estimate",
                        "note": "内部规则提醒",
                    }
                )
            expiry += timedelta(days=7)

    return _dedupe_calendar_events(events)


def _trading_calendar_official_count(payload: Optional[Dict[str, Any]]) -> int:
    data = dict(payload or {})
    source_details = dict(data.get("source_details") or {})
    total = sum(
        int((source_details.get(key) or {}).get("count") or 0)
        for key in ("economic", "central_bank", "unlocks")
    )
    if total > 0:
        return total
    return sum(
        1
        for item in list(data.get("events") or [])
        if str((item or {}).get("source") or "").strip().startswith("coinglass_")
    )


def _calendar_all_official_sources_failed(
    economic_payload: Dict[str, Any],
    central_bank_payload: Dict[str, Any],
    unlock_payload: Dict[str, Any],
) -> bool:
    return all(
        bool(str((payload or {}).get("error") or "").strip())
        for payload in (economic_payload, central_bank_payload, unlock_payload)
    )


def _with_trading_calendar_runtime_fields(
    payload: Dict[str, Any],
    *,
    cache_hit: bool,
    cache_age_sec: Optional[float],
    stale: bool,
    source_status: str,
    stale_reason: Optional[str] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(payload or {})
    out["cache_hit"] = bool(cache_hit)
    out["cache_age_sec"] = (
        round(float(cache_age_sec or 0.0), 3) if cache_age_sec is not None else None
    )
    out["stale"] = bool(stale)
    out["source_status"] = str(
        source_status
        or ("cache_stale" if stale else ("cache_fresh" if cache_hit else "live"))
    )
    if stale_reason:
        out["stale_reason"] = str(stale_reason)
        stale_note = "CoinGlass 官方日历刷新失败，已回退到最近一次成功快照"
        note_parts = [str(out.get("note") or "").strip(), stale_note]
        out["note"] = "；".join(dict.fromkeys(part for part in note_parts if part))
    else:
        out.pop("stale_reason", None)
    return out


def _normalize_coinglass_news_item(row: Dict[str, Any]) -> Dict[str, Any]:
    title = str(row.get("article_title") or row.get("title") or "").strip()
    article_id = row.get("article_id") or row.get("id")
    published_at = (
        _coinglass_ts_to_iso(row.get("article_publish_time"))
        or _coinglass_ts_to_iso(row.get("created_at"))
        or _coinglass_ts_to_iso(row.get("published_at"))
    )
    source = str(
        row.get("article_source_name") or row.get("source_name") or "coinglass"
    ).strip()
    return {
        "title": title,
        "code": article_id,
        "release_date": published_at,
        "url": row.get("article_url") or row.get("url"),
        "source": source,
        "provider": "coinglass_news",
    }


def _normalize_coinglass_whale_item(
    row: Dict[str, Any], *, btc_price: float
) -> Dict[str, Any]:
    amount_usd = _safe_float(row.get("amount_usd"))
    asset_qty = _safe_float(row.get("asset_quantity"))
    btc_equiv = amount_usd / btc_price if btc_price > 0 and amount_usd > 0 else 0.0
    return {
        "hash": row.get("transaction_hash"),
        "btc": round(btc_equiv, 6),
        "amount_usd": round(amount_usd, 2),
        "asset_quantity": round(asset_qty, 8),
        "asset_symbol": str(row.get("asset_symbol") or "").upper(),
        "from": row.get("from"),
        "to": row.get("to"),
        "chain": str(row.get("blockchain_name") or "").lower(),
        "timestamp": _coinglass_ts_to_iso(row.get("block_timestamp")),
        "provider": "coinglass_whale_transfer",
    }


def _normalize_coinglass_exchange_chain_item(
    row: Dict[str, Any], *, btc_price: float
) -> Dict[str, Any]:
    amount_usd = _safe_float(row.get("amount_usd"))
    asset_qty = _safe_float(row.get("asset_quantity"))
    btc_equiv = amount_usd / btc_price if btc_price > 0 and amount_usd > 0 else 0.0
    return {
        "hash": row.get("transaction_hash"),
        "btc": round(btc_equiv, 6),
        "amount_usd": round(amount_usd, 2),
        "asset_quantity": round(asset_qty, 8),
        "asset_symbol": str(row.get("asset_symbol") or "").upper(),
        "exchange_name": str(row.get("exchange_name") or "").strip(),
        "transfer_type": str(row.get("transfer_type") or "").strip(),
        "from": row.get("from_address"),
        "to": row.get("to_address"),
        "timestamp": _coinglass_ts_to_iso(row.get("transaction_time")),
        "provider": "coinglass_exchange_chain_tx",
    }


def _exchange_flow_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "other"
    if any(token in text for token in ("deposit", "inflow", "to_exchange")):
        return "inflow"
    if any(token in text for token in ("withdraw", "outflow", "from_exchange")):
        return "outflow"
    if text in {"in", "into", "inbound"}:
        return "inflow"
    if text in {"out", "outbound"}:
        return "outflow"
    return "other"


def _build_exchange_flow_summary(transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    relevant = [
        dict(item or {})
        for item in transactions
        if str((item or {}).get("provider") or "").strip()
        == "coinglass_exchange_chain_tx"
    ]
    if not relevant:
        return {}

    inflow_count = 0
    outflow_count = 0
    other_count = 0
    transfer_type_breakdown: Dict[str, int] = {}
    exchange_names: List[str] = []
    asset_symbols: List[str] = []
    total_amount_usd = 0.0

    for item in relevant:
        direction = _exchange_flow_direction(item.get("transfer_type"))
        if direction == "inflow":
            inflow_count += 1
        elif direction == "outflow":
            outflow_count += 1
        else:
            other_count += 1
        transfer_type = (
            str(item.get("transfer_type") or "").strip().lower() or "unknown"
        )
        transfer_type_breakdown[transfer_type] = (
            int(transfer_type_breakdown.get(transfer_type) or 0) + 1
        )
        exchange_name = str(item.get("exchange_name") or "").strip()
        if exchange_name:
            exchange_names.append(exchange_name)
        asset_symbol = str(item.get("asset_symbol") or "").strip().upper()
        if asset_symbol:
            asset_symbols.append(asset_symbol)
        total_amount_usd += _safe_float(item.get("amount_usd"))

    return {
        "count": len(relevant),
        "inflow_count": inflow_count,
        "outflow_count": outflow_count,
        "other_count": other_count,
        "transfer_type_breakdown": transfer_type_breakdown,
        "exchange_names": list(dict.fromkeys(exchange_names))[:10],
        "asset_symbols": list(dict.fromkeys(asset_symbols))[:10],
        "total_amount_usd": round(total_amount_usd, 2),
    }


def _dedupe_announcements(
    items: List[Dict[str, Any]], *, limit: int
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        title = str((item or {}).get("title") or "").strip()
        code = str((item or {}).get("code") or "").strip()
        key = (title.lower(), code.lower())
        if not title or key in seen:
            continue
        seen.add(key)
        out.append(dict(item or {}))
        if len(out) >= limit:
            break
    return out


def _merge_whale_payloads(
    *payloads: Dict[str, Any], threshold_btc: float, btc_price: float
) -> Dict[str, Any]:
    transactions: List[Dict[str, Any]] = []
    seen_hashes = set()
    source_names: List[str] = []
    available = False
    errors: List[str] = []
    for payload in payloads:
        item = dict(payload or {})
        if bool(item.get("available")):
            available = True
        source_name = str(item.get("source_name") or "").strip()
        if source_name:
            source_names.append(source_name)
        error = str(item.get("error") or "").strip()
        if error:
            errors.append(error)
        for tx in list(item.get("transactions") or []):
            tx_hash = (
                str((tx or {}).get("hash") or (tx or {}).get("transaction_hash") or "")
                .strip()
                .lower()
            )
            dedupe_key = tx_hash or json.dumps(tx, ensure_ascii=False, sort_keys=True)
            if dedupe_key in seen_hashes:
                continue
            seen_hashes.add(dedupe_key)
            transactions.append(dict(tx or {}))
    transactions.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    return {
        "available": available,
        "error": "; ".join(dict.fromkeys(error for error in errors if error)),
        "source_name": "+".join(dict.fromkeys(name for name in source_names if name))
        or "public_chain_proxy",
        "threshold_btc": threshold_btc,
        "btc_price": btc_price,
        "count": len(transactions),
        "exchange_flow_summary": _build_exchange_flow_summary(transactions),
        "transactions": transactions[:20],
    }


async def _fetch_coinglass_news(*, symbol: str, limit: int = 6) -> List[Dict[str, Any]]:
    if not coinglass_enabled():
        return []
    base_symbol = _symbol_base(symbol)
    try:
        async with CoinglassClient(
            timeout_sec=_ANALYTICS_ANNOUNCEMENT_TIMEOUT_SEC
        ) as client:
            response = await client.request_json(
                "/v4/api/article/list",
                params={
                    "page": 1,
                    "per_page": max(1, min(int(limit) * 3, 30)),
                    "language": "en",
                },
                manual=False,
            )
    except Exception:
        return []

    rows = list(((response.get("payload") or {}).get("data") or []))
    announcements: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _normalize_coinglass_news_item(dict(row or {}))
        title_upper = str(normalized.get("title") or "").upper()
        if base_symbol and title_upper and base_symbol not in title_upper:
            continue
        announcements.append(normalized)
        if len(announcements) >= limit:
            break
    if announcements:
        return announcements
    return [_normalize_coinglass_news_item(dict(row or {})) for row in rows[:limit]]


async def _fetch_coinglass_whale_transfers(
    *,
    symbol: str,
    min_btc: float = _ANALYTICS_WHALE_MIN_BTC,
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "error": "coinglass_disabled",
            "source_name": "coinglass_whale_transfer",
            "threshold_btc": min_btc,
            "btc_price": 0.0,
            "count": 0,
            "transactions": [],
        }

    btc_price = 0.0
    try:
        px = await _fetch_binance_public_json(
            "/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout_sec=_ANALYTICS_FUNDING_TIMEOUT_SEC,
        )
        btc_price = _safe_float((px or {}).get("price"))
    except Exception:
        btc_price = 0.0

    base_symbol = _symbol_base(symbol)
    try:
        async with CoinglassClient(timeout_sec=_ANALYTICS_WHALE_TIMEOUT_SEC) as client:
            response = await client.request_json(
                "/v4/api/chain/v2/whale-transfer",
                params={"symbol": base_symbol, "limit": 20},
                manual=False,
            )
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "source_name": "coinglass_whale_transfer",
            "threshold_btc": min_btc,
            "btc_price": btc_price,
            "count": 0,
            "transactions": [],
        }

    rows = list(((response.get("payload") or {}).get("data") or []))
    transactions = [
        _normalize_coinglass_whale_item(dict(row or {}), btc_price=btc_price)
        for row in rows
    ]
    threshold_usd = (min_btc * btc_price) if btc_price > 0 else 0.0
    filtered = (
        [
            item
            for item in transactions
            if _safe_float(item.get("amount_usd")) >= threshold_usd
        ]
        if threshold_usd > 0
        else transactions
    )
    return {
        "available": bool(filtered),
        "error": "",
        "source_name": "coinglass_whale_transfer",
        "threshold_btc": min_btc,
        "btc_price": btc_price,
        "count": len(filtered),
        "transactions": filtered[:20],
    }


async def _fetch_coinglass_exchange_chain_transfers(
    *,
    symbol: str,
    min_btc: float = _ANALYTICS_WHALE_MIN_BTC,
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "error": "coinglass_disabled",
            "source_name": "coinglass_exchange_chain_tx",
            "threshold_btc": min_btc,
            "btc_price": 0.0,
            "count": 0,
            "transactions": [],
        }

    btc_price = 0.0
    try:
        px = await _fetch_binance_public_json(
            "/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout_sec=_ANALYTICS_FUNDING_TIMEOUT_SEC,
        )
        btc_price = _safe_float((px or {}).get("price"))
    except Exception:
        btc_price = 0.0

    base_symbol = _symbol_base(symbol)
    threshold_usd = (min_btc * btc_price) if btc_price > 0 else 0.0
    try:
        async with CoinglassClient(timeout_sec=_ANALYTICS_WHALE_TIMEOUT_SEC) as client:
            response = await client.request_json(
                "/v4/api/exchange/chain/tx/list",
                params={
                    "symbol": base_symbol,
                    "min_usd": round(threshold_usd, 2) if threshold_usd > 0 else None,
                    "per_page": 20,
                    "page": 1,
                },
                manual=False,
            )
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "source_name": "coinglass_exchange_chain_tx",
            "threshold_btc": min_btc,
            "btc_price": btc_price,
            "count": 0,
            "transactions": [],
        }

    rows = list(((response.get("payload") or {}).get("data") or []))
    transactions = [
        _normalize_coinglass_exchange_chain_item(dict(row or {}), btc_price=btc_price)
        for row in rows
    ]
    filtered = (
        [
            item
            for item in transactions
            if _safe_float(item.get("amount_usd")) >= threshold_usd
        ]
        if threshold_usd > 0
        else transactions
    )
    return {
        "available": bool(filtered),
        "error": "",
        "source_name": "coinglass_exchange_chain_tx",
        "threshold_btc": min_btc,
        "btc_price": btc_price,
        "count": len(filtered),
        "transactions": filtered[:20],
    }


async def _capture_analytics(task_name: str, coro: Any) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        data = await coro
        return {
            "task": task_name,
            "ok": True,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            "data": data,
        }
    except Exception as e:
        return {
            "task": task_name,
            "ok": False,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            "error": str(e),
        }


async def get_analytics_overview(
    days: int = 90,
    lookback: int = 240,
    calendar_days: int = 30,
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    mode: Optional[str] = None,
):
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    module_jobs = {
        "performance": _capture_analytics(
            "performance",
            get_advanced_performance(days=max(1, min(int(days or 90), 720)), mode=target_mode),
        ),
        "risk_dashboard": _capture_analytics(
            "risk_dashboard",
            get_risk_dashboard(
                lookback=max(60, min(int(lookback or 240), 2000)),
                mode=target_mode,
            ),
        ),
        "calendar": _capture_analytics(
            "calendar",
            get_trading_calendar(days=max(1, min(int(calendar_days or 30), 180))),
        ),
        "microstructure": _capture_analytics(
            "microstructure",
            get_market_microstructure(exchange=exchange, symbol=symbol, depth_limit=80),
        ),
        "equity_rebalance": _capture_analytics(
            "equity_rebalance",
            get_equity_rebalance(hours=168, target_alloc="BTC:0.4,ETH:0.3,USDT:0.3"),
        ),
        "community": _capture_analytics(
            "community",
            get_community_overview(symbol=symbol, exchange=exchange),
        ),
        "behavior_report": _capture_analytics(
            "behavior_report",
            get_behavior_report(days=7),
        ),
        "stoploss_policy": _capture_analytics(
            "stoploss_policy",
            get_stoploss_policy(mode=target_mode),
        ),
    }
    module_names = list(module_jobs.keys())
    module_results = await asyncio.gather(*module_jobs.values())
    modules = {name: result for name, result in zip(module_names, module_results)}
    ok_count = len([x for x in modules.values() if x.get("ok")])
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "all_ok": ok_count == len(modules),
        "ok_count": ok_count,
        "total": len(modules),
        "modules": modules,
    }


async def get_advanced_performance(days: int = 90, mode: Optional[str] = None):
    days = max(1, min(days, 720))
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    records = _iter_trade_records(days=days, mode=target_mode)
    if not records:
        return {
            "days": days,
            "trade_count": 0,
            "risk_adjusted": {"sharpe": 0.0, "sortino": 0.0, "calmar": 0.0},
            "trade_quality": {"ev": 0.0, "avg_r_multiple": 0.0, "profit_factor": 0.0},
            "win_rate_breakdown": {
                "overall": 0.0,
                "by_strategy": [],
                "by_symbol": [],
                "by_session": [],
            },
            "drawdown": {
                "max_drawdown_usd": 0.0,
                "max_drawdown_pct": 0.0,
                "duration": 0,
                "recovery": 0,
            },
            "streaks": {"max_win_streak": 0, "max_loss_streak": 0},
        }

    pnls = [_safe_float(x.get("pnl")) for x in records]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    trade_count = len(pnls)
    win_rate = (len(wins) / trade_count) if trade_count > 0 else 0.0
    avg_win = statistics.fmean(wins) if wins else 0.0
    avg_loss = statistics.fmean(losses) if losses else 0.0
    ev = statistics.fmean(pnls) if pnls else 0.0
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else 0.0

    trade_returns = []
    r_values = []
    for row in records:
        pnl = _safe_float(row.get("pnl"))
        notional = abs(_safe_float(row.get("notional")))
        if notional > 0:
            trade_returns.append(pnl / notional)
        risk_unit = max(1e-6, notional * 0.01) if notional > 0 else max(1.0, abs(pnl))
        r_values.append(pnl / risk_unit)

    ret_mean = statistics.fmean(trade_returns) if trade_returns else 0.0
    ret_std = statistics.pstdev(trade_returns) if len(trade_returns) > 1 else 0.0
    downside = [x for x in trade_returns if x < 0]
    downside_std = statistics.pstdev(downside) if len(downside) > 1 else 0.0
    annual_factor = math.sqrt(252.0)
    sharpe = (ret_mean / ret_std * annual_factor) if ret_std > 0 else 0.0
    sortino = (ret_mean / downside_std * annual_factor) if downside_std > 0 else 0.0

    equity_curve = [10000.0]
    for pnl in pnls:
        equity_curve.append(equity_curve[-1] + pnl)
    dd = _drawdown_profile(equity_curve)
    annual_return = (
        ((equity_curve[-1] / equity_curve[0]) ** (365.0 / max(1.0, float(days))) - 1.0)
        if equity_curve[0] > 0
        else 0.0
    )
    calmar = (
        (annual_return / max(1e-9, dd["max_drawdown_pct"] / 100.0))
        if dd["max_drawdown_pct"] > 0
        else 0.0
    )

    def _breakdown(key: str) -> List[Dict[str, Any]]:
        rows: Dict[str, List[float]] = {}
        for rec in records:
            if key == "session":
                k = _session_name(rec["timestamp"])
            else:
                k = str(rec.get(key) or "unknown")
            rows.setdefault(k, []).append(_safe_float(rec.get("pnl")))
        out = []
        for k, values in rows.items():
            c = len(values)
            w = len([x for x in values if x > 0])
            out.append(
                {
                    "key": k,
                    "count": c,
                    "win_rate": round((w / c * 100) if c > 0 else 0.0, 4),
                    "avg_pnl": round(statistics.fmean(values) if values else 0.0, 6),
                    "net_pnl": round(sum(values), 6),
                }
            )
        out.sort(key=lambda x: x["net_pnl"], reverse=True)
        return out[:20]

    return {
        "days": days,
        "trade_count": trade_count,
        "risk_adjusted": {
            "sharpe": round(sharpe, 6),
            "sortino": round(sortino, 6),
            "calmar": round(calmar, 6),
        },
        "trade_quality": {
            "avg_win": round(avg_win, 6),
            "avg_loss": round(avg_loss, 6),
            "ev": round(ev, 6),
            "avg_r_multiple": round(statistics.fmean(r_values) if r_values else 0.0, 6),
            "median_r_multiple": round(
                statistics.median(r_values) if r_values else 0.0, 6
            ),
            "profit_factor": round(profit_factor, 6),
        },
        "win_rate_breakdown": {
            "overall": round(win_rate * 100, 4),
            "by_strategy": _breakdown("strategy"),
            "by_symbol": _breakdown("symbol"),
            "by_session": _breakdown("session"),
        },
        "drawdown": dd,
        "streaks": {
            "max_win_streak": _calc_max_streak(pnls, positive=True),
            "max_loss_streak": _calc_max_streak(pnls, positive=False),
        },
    }


async def _build_risk_dashboard_payload(
    lookback: int = 240, mode: Optional[str] = None
) -> Dict[str, Any]:
    lookback = max(60, min(int(lookback or 240), 2000))
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    report = risk_manager.get_risk_report(scope=target_mode)
    positions = position_manager.get_all_positions(scope=target_mode)
    equity = _safe_float((report.get("equity") or {}).get("current"))

    exposure_by_symbol: Dict[str, float] = {}
    weighted_lev = 0.0
    liq_rows = []
    total_exposure = 0.0
    for p in positions:
        symbol = str(getattr(p, "symbol", "") or "")
        value = abs(_safe_float(getattr(p, "value", 0.0)))
        total_exposure += value
        exposure_by_symbol[symbol] = exposure_by_symbol.get(symbol, 0.0) + value
        lev = max(1.0, _safe_float(getattr(p, "leverage", 1.0), default=1.0))
        weighted_lev += value * lev

        entry = _safe_float(getattr(p, "entry_price", 0.0))
        current = _safe_float(getattr(p, "current_price", 0.0))
        side = str(getattr(p, "side", "") or "")
        liq_price = _safe_float(getattr(p, "liquidation_price", 0.0))
        if liq_price <= 0 and entry > 0:
            liq_price = (
                entry * (1.0 - (0.9 / lev))
                if side == "long"
                else entry * (1.0 + (0.9 / lev))
            )
        dist_pct = (
            abs((current - liq_price) / current * 100)
            if current > 0 and liq_price > 0
            else None
        )
        liq_rows.append(
            {
                "symbol": symbol,
                "side": side,
                "current_price": round(current, 8),
                "liquidation_price": round(liq_price, 8) if liq_price > 0 else None,
                "distance_pct": round(dist_pct, 4) if dist_pct is not None else None,
            }
        )

    concentration = []
    for symbol, value in sorted(
        exposure_by_symbol.items(), key=lambda x: x[1], reverse=True
    ):
        concentration.append(
            {
                "symbol": symbol,
                "exposure": round(value, 6),
                "weight": round(
                    (value / total_exposure) if total_exposure > 0 else 0.0, 6
                ),
            }
        )

    corr_matrix: Dict[str, Dict[str, float]] = {}
    avg_abs_corr = 0.0
    symbols = [x["symbol"] for x in concentration[:8] if x["symbol"]]
    if len(symbols) >= 2:
        ret_map: Dict[str, pd.Series] = {}
        for symbol in symbols:
            ret = await _load_symbol_returns(symbol, lookback=lookback)
            if not ret.empty:
                ret_map[symbol] = ret
        if len(ret_map) >= 2:
            corr_df = pd.DataFrame(ret_map).dropna(how="any")
            if len(corr_df) >= 10:
                corr_df = corr_df.corr().fillna(0.0)
                corr_matrix = corr_df.round(4).to_dict()
                vals = []
                cols = list(corr_df.columns)
                for i in range(len(cols)):
                    for j in range(i + 1, len(cols)):
                        vals.append(abs(_safe_float(corr_df.iloc[i, j])))
                avg_abs_corr = statistics.fmean(vals) if vals else 0.0

    history = await account_snapshot_manager.get_history(
        hours=168, exchange="all", limit=1200, mode=target_mode
    )
    ret = []
    prev = None
    for row in history:
        total = _safe_float(row.get("total_usd"))
        if prev and prev > 0 and total > 0:
            ret.append((total - prev) / prev)
        prev = total
    var95 = abs(_var_quantile(ret, 0.95))
    var99 = abs(_var_quantile(ret, 0.99))

    implicit_lev = (total_exposure / equity) if equity > 0 else 0.0
    explicit_lev = (weighted_lev / total_exposure) if total_exposure > 0 else 0.0
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_level": report.get("risk_level", "low"),
        "total_exposure": round(total_exposure, 6),
        "exposure_pct_of_equity": round(
            (total_exposure / equity * 100) if equity > 0 else 0.0, 4
        ),
        "concentration": concentration,
        "correlation_risk": {
            "avg_abs_correlation": round(avg_abs_corr, 6),
            "matrix": corr_matrix,
        },
        "leverage": {
            "implicit": round(implicit_lev, 6),
            "explicit_weighted": round(explicit_lev, 6),
        },
        "liquidation_distance": liq_rows,
        "var": {
            "var95_pct": round(var95 * 100, 6),
            "var99_pct": round(var99 * 100, 6),
            "sample_points": len(ret),
        },
    }


async def get_risk_dashboard(lookback: int = 240, mode: Optional[str] = None):
    lookback = max(60, min(int(lookback or 240), 2000))
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    cache_key = f"mode:{target_mode}|lookback:{lookback}"
    cached, cached_age = _cache_get(
        _RISK_DASHBOARD_CACHE,
        cache_key,
        max_age_sec=_RISK_DASHBOARD_CACHE_TTL_SEC,
    )
    if cached is not None:
        return _with_risk_dashboard_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )

    stale_cached, stale_age = _cache_get(
        _RISK_DASHBOARD_CACHE,
        cache_key,
        max_age_sec=_RISK_DASHBOARD_STALE_MAX_AGE_SEC,
    )
    if stale_cached is not None:
        _schedule_cache_refresh(
            _RISK_DASHBOARD_REFRESH_TASKS,
            cache_key,
            build_coro=lambda: _build_risk_dashboard_payload(lookback, mode=target_mode),
            cache=_RISK_DASHBOARD_CACHE,
            strip_payload=_strip_risk_dashboard_runtime_fields,
        )
        return _with_risk_dashboard_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            source_status="cache_stale",
            stale_reason="background_refresh_scheduled",
        )

    payload = await _build_risk_dashboard_payload(lookback, mode=target_mode)
    _cache_put(
        _RISK_DASHBOARD_CACHE, cache_key, _strip_risk_dashboard_runtime_fields(payload)
    )
    return _with_risk_dashboard_runtime_fields(
        payload,
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


async def get_trading_calendar(days: int = 30):
    days = max(1, min(int(days or 30), 180))
    cache_key = f"days:{days}"
    fresh_cached, fresh_age = _cache_get(
        _TRADING_CALENDAR_CACHE,
        cache_key,
        max_age_sec=_TRADING_CALENDAR_CACHE_TTL_SEC,
    )
    if fresh_cached is not None:
        return _with_trading_calendar_runtime_fields(
            fresh_cached,
            cache_hit=True,
            cache_age_sec=fresh_age,
            stale=False,
            source_status="cache_fresh",
        )
    stale_cached, stale_age = _cache_get(
        _TRADING_CALENDAR_CACHE,
        cache_key,
        max_age_sec=_TRADING_CALENDAR_STALE_MAX_AGE_SEC,
    )

    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days)
    coinglass_days = min(days, _COINGLASS_CALENDAR_LOOKAHEAD_DAYS)
    economic_task = asyncio.create_task(
        _fetch_coinglass_economic_calendar_events(days=coinglass_days)
    )
    central_bank_task = asyncio.create_task(
        _fetch_coinglass_central_bank_calendar_events(days=coinglass_days)
    )
    unlock_task = asyncio.create_task(
        _fetch_coinglass_unlock_calendar_events(days=days)
    )
    economic_payload, central_bank_payload, unlock_payload = await asyncio.gather(
        economic_task,
        central_bank_task,
        unlock_task,
    )

    economic_payload = dict(economic_payload or {})
    central_bank_payload = dict(central_bank_payload or {})
    unlock_payload = dict(unlock_payload or {})

    macro_events = _dedupe_calendar_events(
        [
            *(economic_payload.get("events") or []),
            *(central_bank_payload.get("events") or []),
        ]
    )
    unlock_events = _dedupe_calendar_events(list(unlock_payload.get("events") or []))
    macro_available = bool(macro_events)
    unlock_available = bool(unlock_events)

    fallback_events = _build_internal_estimate_calendar_events(
        now=now,
        end=end,
        start=now,
        include_economic=not macro_available,
        include_unlocks=not unlock_available,
        include_expiry=True,
    )

    supplemental_macro_events: List[Dict[str, Any]] = []
    if macro_available and days > _COINGLASS_CALENDAR_LOOKAHEAD_DAYS:
        supplement_start = now + timedelta(days=_COINGLASS_CALENDAR_LOOKAHEAD_DAYS)
        if supplement_start < end:
            supplemental_macro_events = _build_internal_estimate_calendar_events(
                now=now,
                end=end,
                start=supplement_start,
                include_economic=True,
                include_unlocks=False,
                include_expiry=False,
            )

    events = _dedupe_calendar_events(
        [
            *macro_events,
            *unlock_events,
            *fallback_events,
            *supplemental_macro_events,
        ]
    )

    source_parts: List[str] = []
    if macro_events:
        if economic_payload.get("events"):
            source_parts.append("coinglass_economic_data")
        if central_bank_payload.get("events"):
            source_parts.append("coinglass_central_bank")
    if unlock_events:
        source_parts.append("coinglass_unlock_list")
    if fallback_events or supplemental_macro_events:
        source_parts.append("internal_estimate")

    note_parts: List[str] = []
    if macro_events:
        note_parts.append("宏观/央行事件优先使用 CoinGlass 官方日历接口")
    elif economic_payload.get("error") or central_bank_payload.get("error"):
        note_parts.append("CoinGlass 宏观日历不可用，已回退到内置估算")
    else:
        note_parts.append("当前窗口内未命中可用的 CoinGlass 重点宏观事件，已补内置估算")

    if unlock_events:
        note_parts.append("代币解锁优先使用 CoinGlass Unlock List")
    elif unlock_payload.get("error"):
        note_parts.append("CoinGlass 解锁列表不可用，已回退到内置解锁模板")
    else:
        note_parts.append("当前窗口内未命中 CoinGlass 解锁事件，已保留内部提醒")

    if days > _COINGLASS_CALENDAR_LOOKAHEAD_DAYS and macro_events:
        note_parts.append(
            f"CoinGlass 宏观日历未来视窗最多约 {_COINGLASS_CALENDAR_LOOKAHEAD_DAYS} 天，超出部分以内置估算补齐"
        )

    payload = {
        "source": "+".join(dict.fromkeys(source_parts)) or "internal_estimate",
        "note": "；".join(dict.fromkeys(part for part in note_parts if part)),
        "days": days,
        "events": events,
        "count": len(events),
        "source_details": {
            "economic": {
                "available": bool(economic_payload.get("events")),
                "count": len(list(economic_payload.get("events") or [])),
                "coverage_end": economic_payload.get("coverage_end"),
                "error": economic_payload.get("error") or None,
            },
            "central_bank": {
                "available": bool(central_bank_payload.get("events")),
                "count": len(list(central_bank_payload.get("events") or [])),
                "coverage_end": central_bank_payload.get("coverage_end"),
                "error": central_bank_payload.get("error") or None,
            },
            "unlocks": {
                "available": bool(unlock_payload.get("events")),
                "count": len(list(unlock_payload.get("events") or [])),
                "error": unlock_payload.get("error") or None,
            },
            "internal_estimate": {
                "used": bool(fallback_events or supplemental_macro_events),
                "count": len(fallback_events) + len(supplemental_macro_events),
            },
        },
    }
    if (
        stale_cached is not None
        and _calendar_all_official_sources_failed(
            economic_payload, central_bank_payload, unlock_payload
        )
        and _trading_calendar_official_count(payload) <= 0
        and _trading_calendar_official_count(stale_cached) > 0
    ):
        return _with_trading_calendar_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            stale_reason="official_calendar_refresh_failed",
            source_status="cache_stale",
        )
    _cache_put(_TRADING_CALENDAR_CACHE, cache_key, payload)
    return _with_trading_calendar_runtime_fields(
        payload,
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


async def _build_market_microstructure_payload(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
):
    tasks = {
        "orderbook": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_orderbook(exchange=exchange, symbol=symbol, limit=depth_limit),
                default={
                    "available": False,
                    "error": "orderbook_timeout",
                    "bids": [],
                    "asks": [],
                    "timestamp": None,
                },
                timeout_sec=_public_core_timeout(
                    _ANALYTICS_ORDERBOOK_TIMEOUT_SEC + 0.3
                ),
                label="orderbook",
            )
        ),
        "flow": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_trade_imbalance(
                    exchange=exchange,
                    symbol=symbol,
                    limit=800,
                ),
                default={
                    "available": False,
                    "error": "trade_imbalance_timeout",
                    "count": 0,
                    "buy_volume": 0.0,
                    "sell_volume": 0.0,
                    "imbalance": 0.0,
                },
                timeout_sec=_public_core_timeout(
                    _ANALYTICS_TRADE_IMBALANCE_TIMEOUT_SEC + 0.3
                ),
                label="trade imbalance",
            )
        ),
        "funding_basis": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_funding_basis_snapshot(exchange=exchange, symbol=symbol),
                default={
                    "funding": {
                        "available": False,
                        "error": "funding_timeout",
                    },
                    "basis": {
                        "available": False,
                        "error": "basis_timeout",
                    },
                },
                timeout_sec=_public_core_timeout(
                    max(
                        _ANALYTICS_FUNDING_TIMEOUT_SEC,
                        _ANALYTICS_BASIS_TIMEOUT_SEC,
                    )
                    + 0.4
                ),
                label="funding/basis",
            )
        ),
        "long_short": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_long_short_ratio_snapshot(exchange=exchange, symbol=symbol),
                default={
                    "available": False,
                    "error": "long_short_timeout",
                    "sample_size": 0,
                },
                timeout_sec=_public_core_timeout(
                    _ANALYTICS_LONG_SHORT_TIMEOUT_SEC + 0.3
                ),
                label="long/short ratio",
            )
        ),
        "open_interest": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_open_interest_snapshot(exchange=exchange, symbol=symbol),
                default={
                    "available": False,
                    "error": "open_interest_timeout",
                    "sample_size": 0,
                },
                timeout_sec=_public_core_timeout(_ANALYTICS_OI_TIMEOUT_SEC + 0.3),
                label="open interest",
            )
        ),
        "options": asyncio.create_task(
            _bounded_analytics_call(
                _fetch_options_snapshot(symbol=symbol),
                default={"available": False, "error": "options_timeout"},
                timeout_sec=_ANALYTICS_OPTIONS_TIMEOUT_SEC + 0.3,
                label="options snapshot",
            )
        ),
        "coinglass": asyncio.create_task(
            _bounded_analytics_call(
                _load_preferred_coinglass_overview(symbol=symbol),
                default={},
                timeout_sec=_ANALYTICS_COINGLASS_OVERVIEW_TIMEOUT_SEC,
                label="coinglass overview",
            )
        ),
    }

    (
        ob,
        flow,
        funding_basis,
        long_short_ratio,
        oi,
        options_data,
        coinglass_overview,
    ) = await asyncio.gather(
        tasks["orderbook"],
        tasks["flow"],
        tasks["funding_basis"],
        tasks["long_short"],
        tasks["open_interest"],
        tasks["options"],
        tasks["coinglass"],
    )
    funding = dict((funding_basis or {}).get("funding") or {"available": False})
    basis = dict((funding_basis or {}).get("basis") or {"available": False})
    orderbook_view = _build_microstructure_orderbook_view(ob)

    options_payload = dict(options_data or {})
    options_payload.setdefault("available", False)
    options_payload.setdefault(
        "currency", symbol.split("/")[0].split(":")[0].upper() if symbol else None
    )
    options_payload.setdefault("atm_iv", None)
    options_payload.setdefault("skew_25d", None)
    options_payload.setdefault("put_call_ratio", None)
    options_payload.setdefault("signal", None)
    options_payload.setdefault("timestamp", None)

    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "available": bool(ob.get("available", True)),
        "source_error": ob.get("error"),
        "orderbook": dict(orderbook_view.get("orderbook") or {}),
        "large_orders": list(orderbook_view.get("large_orders") or []),
        "iceberg_detection": {
            "candidate_count": int(orderbook_view.get("iceberg_candidate_count") or 0),
            "note": "基于盘口重复量级的启发式检测。",
        },
        "aggressor_flow": {
            "available": bool(flow.get("available", True)),
            "error": flow.get("error"),
            "count": int(_safe_float(flow.get("count"))),
            "buy_volume": _safe_float(flow.get("buy_volume")),
            "sell_volume": _safe_float(flow.get("sell_volume")),
            "imbalance": _safe_float(flow.get("imbalance")),
        },
        "long_short_ratio": {
            "available": bool(long_short_ratio.get("available", False)),
            "source": long_short_ratio.get("source"),
            "error": long_short_ratio.get("error"),
            "symbol": long_short_ratio.get("symbol"),
            "long_ratio": _safe_float(long_short_ratio.get("long_ratio")),
            "short_ratio": _safe_float(long_short_ratio.get("short_ratio")),
            "long_short_ratio": _safe_float(long_short_ratio.get("long_short_ratio")),
            "sample_size": int(_safe_float(long_short_ratio.get("sample_size"))),
            "timestamp": long_short_ratio.get("timestamp"),
        },
        "oi": {
            "available": bool(oi.get("available", False)),
            "source": oi.get("source"),
            "error": oi.get("error"),
            "volume": _safe_float(oi.get("volume")),
            "value": _safe_float(oi.get("value")),
            "change_pct_1h": oi.get("change_pct_1h"),
            "timestamp": oi.get("timestamp"),
            "sample_size": int(_safe_float(oi.get("sample_size"))),
        },
        "funding_rate": funding,
        "spot_futures_basis": basis,
        "options": options_payload,
    }
    payload = _apply_coinglass_derivatives_overlay(payload, coinglass_overview)
    payload = _apply_public_derivatives_context(payload)
    return payload


async def get_market_microstructure(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
):
    cache_key = f"{str(exchange or '').lower()}|{str(symbol or '').upper()}|{max(5, min(int(depth_limit), 200))}"
    cached, cached_age = _cache_get(
        _MICROSTRUCTURE_SNAPSHOT_CACHE,
        cache_key,
        max_age_sec=_MICROSTRUCTURE_SNAPSHOT_CACHE_TTL_SEC,
    )
    if cached is not None:
        return _with_microstructure_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )

    stale_cached, stale_age = _cache_get(
        _MICROSTRUCTURE_SNAPSHOT_CACHE,
        cache_key,
        max_age_sec=_MICROSTRUCTURE_SNAPSHOT_STALE_MAX_AGE_SEC,
    )
    if stale_cached is not None and _microstructure_has_core_signal(stale_cached):
        _schedule_cache_refresh(
            _MICROSTRUCTURE_REFRESH_TASKS,
            cache_key,
            build_coro=lambda: _build_market_microstructure_payload(
                exchange=exchange, symbol=symbol, depth_limit=depth_limit
            ),
            cache=_MICROSTRUCTURE_SNAPSHOT_CACHE,
            strip_payload=_strip_cacheable_microstructure_payload,
        )
        return _with_microstructure_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            source_status="cache_stale",
            stale_reason="background_refresh_scheduled",
        )

    payload = await _build_market_microstructure_payload(
        exchange=exchange, symbol=symbol, depth_limit=depth_limit
    )
    if _microstructure_has_core_signal(payload):
        _cache_put(
            _MICROSTRUCTURE_SNAPSHOT_CACHE,
            cache_key,
            _strip_microstructure_runtime_fields(payload),
        )
    return _with_microstructure_runtime_fields(
        payload,
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status=_microstructure_live_source_status(payload),
    )


async def add_behavior_journal(request: BehaviorJournalRequest):
    rows = _load_behavior_journal()
    item = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mood": str(request.mood or "neutral").strip().lower(),
        "confidence": round(_safe_float(request.confidence), 6),
        "plan_adherence": round(_safe_float(request.plan_adherence), 6),
        "note": str(request.note or "").strip(),
        "symbol": str(request.symbol or "").strip().upper() or None,
        "strategy": str(request.strategy or "").strip() or None,
    }
    rows.append(item)
    _save_behavior_journal(rows)
    return {"success": True, "entry": item, "count": len(rows)}


async def get_behavior_report(days: int = 7):
    days = max(1, min(int(days or 7), 90))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = []
    for row in _load_behavior_journal():
        ts = _safe_dt(row.get("timestamp"))
        if not ts or ts < cutoff:
            continue
        rows.append(dict(row, _ts=ts))

    total = len(rows)
    impulsive = [
        x
        for x in rows
        if _safe_float(x.get("plan_adherence")) < 0.5
        or _safe_float(x.get("confidence")) < 0.35
    ]
    mood_count: Dict[str, int] = {}
    for row in rows:
        mood = str(row.get("mood") or "neutral")
        mood_count[mood] = mood_count.get(mood, 0) + 1

    risk = risk_manager.get_risk_report()
    trade_util = _safe_float(
        (risk.get("utilization") or {}).get("daily_trade_utilization")
    )
    overtrade_warn = trade_util >= 0.8

    return {
        "days": days,
        "entries": total,
        "mood_distribution": mood_count,
        "impulsive_ratio": round((len(impulsive) / total) if total > 0 else 0.0, 6),
        "avg_confidence": round(
            statistics.fmean([_safe_float(x.get("confidence")) for x in rows])
            if rows
            else 0.0,
            6,
        ),
        "avg_plan_adherence": round(
            statistics.fmean([_safe_float(x.get("plan_adherence")) for x in rows])
            if rows
            else 0.0,
            6,
        ),
        "overtrading_warning": overtrade_warn,
        "daily_trade_utilization": round(trade_util, 6),
        "deviation_alert": bool(len(impulsive) >= 3 and total >= 5),
        "recent_notes": [
            {
                "timestamp": x.get("timestamp"),
                "mood": x.get("mood"),
                "note": x.get("note"),
                "symbol": x.get("symbol"),
                "strategy": x.get("strategy"),
            }
            for x in rows[-10:]
        ],
    }


async def get_stoploss_policy(mode: Optional[str] = None):
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    policy = _load_stoploss_policy()
    suggestions = []
    for pos in position_manager.get_all_positions(scope=target_mode)[:50]:
        symbol = str(getattr(pos, "symbol", "") or "")
        atr = await _estimate_atr_for_symbol(
            symbol, period=int(((policy.get("atr") or {}).get("period") or 14))
        )
        atr_mult = _safe_float((policy.get("atr") or {}).get("multiplier"), default=2.0)
        entry = _safe_float(getattr(pos, "entry_price", 0.0))
        current = _safe_float(getattr(pos, "current_price", 0.0))
        qty = abs(_safe_float(getattr(pos, "quantity", 0.0)))
        side = str(getattr(pos, "side", "") or "")
        opened_at = getattr(pos, "opened_at", None)
        hold_hours = (
            ((datetime.now(timezone.utc) - opened_at).total_seconds() / 3600.0)
            if isinstance(opened_at, datetime)
            else 0.0
        )

        atr_stop = None
        if atr and entry > 0:
            atr_stop = (
                entry - atr * atr_mult if side == "long" else entry + atr * atr_mult
            )
        risk_unit = max(1e-6, entry * qty * 0.01) if entry > 0 and qty > 0 else 1.0
        current_r = _safe_float(getattr(pos, "unrealized_pnl", 0.0)) / risk_unit
        suggestions.append(
            {
                "symbol": symbol,
                "side": side,
                "entry_price": round(entry, 8),
                "current_price": round(current, 8),
                "atr_estimate": round(atr, 8) if atr else None,
                "atr_dynamic_stop": round(atr_stop, 8) if atr_stop else None,
                "time_stop_triggered": hold_hours
                >= _safe_float(
                    (policy.get("time_stop") or {}).get("max_hours"), default=24
                ),
                "r_value": round(current_r, 6),
                "r_stop_triggered": current_r
                <= -abs(
                    _safe_float(
                        (policy.get("r_stop") or {}).get("max_loss_r"), default=1.0
                    )
                ),
                "trailing_stop_price": _safe_float(
                    getattr(pos, "trailing_stop_price", 0.0)
                )
                or None,
                "partial_exit_plan": policy.get("partial_stop") or {},
            }
        )
    return {"policy": policy, "position_suggestions": suggestions}


async def update_stoploss_policy(request: StoplossPolicyUpdateRequest):
    policy = _save_stoploss_policy(request.policy or {})
    return {"success": True, "policy": policy}


async def get_equity_rebalance(
    hours: int = 168,
    target_alloc: str = "BTC:0.4,ETH:0.3,USDT:0.3",
    drift_threshold: float = 0.08,
    monthly_return: float = 0.03,
    months: int = 12,
):
    hours = max(24, min(int(hours or 168), 24 * 365))
    hist = await account_snapshot_manager.get_history(
        hours=hours, exchange="all", limit=2000
    )
    equity_series = [
        {"timestamp": x.get("timestamp"), "value": _safe_float(x.get("total_usd"))}
        for x in hist
    ]
    equity_series = [x for x in equity_series if x["value"] > 0]

    benchmark = {}
    points = max(60, min(len(equity_series), 800))
    for sym in ["BTC/USDT", "ETH/USDT"]:
        bdf = await data_storage.load_klines_from_parquet(
            exchange="binance", symbol=sym, timeframe="1h"
        )
        if bdf is None or bdf.empty:
            continue
        close = pd.to_numeric(bdf["close"], errors="coerce").dropna().tail(points)
        if close.empty:
            continue
        base = _safe_float(close.iloc[0], default=0.0)
        if base <= 0:
            continue
        benchmark[sym] = [
            # Benchmark series feeds the dashboard chart — naive ISO would
            # offset every point by the user's TZ. See _to_utc_iso() above.
            {"timestamp": _to_utc_iso(idx), "value": round(_safe_float(px) / base, 6)}
            for idx, px in close.items()
        ]

    dist_map: Dict[str, float] = {}
    for _, item in _BALANCE_SNAPSHOT_CACHE.items():
        for ccy, value in (item.get("distribution") or {}).items():
            dist_map[str(ccy).upper()] = dist_map.get(
                str(ccy).upper(), 0.0
            ) + _safe_float(value)
    total_dist = sum(dist_map.values())
    current_alloc = (
        {k: (v / total_dist) for k, v in dist_map.items()} if total_dist > 0 else {}
    )

    target = _parse_target_allocations(target_alloc)
    drifts = []
    for sym, tar in target.items():
        cur = _safe_float(current_alloc.get(sym), default=0.0)
        drift = cur - tar
        drifts.append(
            {
                "asset": sym,
                "target": round(tar, 6),
                "current": round(cur, 6),
                "drift": round(drift, 6),
            }
        )
    suggestions = [
        x
        for x in drifts
        if abs(_safe_float(x["drift"])) >= abs(_safe_float(drift_threshold))
    ]
    suggestions.sort(key=lambda x: abs(_safe_float(x["drift"])), reverse=True)

    latest_equity = equity_series[-1]["value"] if equity_series else 0.0
    months = max(1, min(int(months or 12), 120))
    mret = _safe_float(monthly_return, default=0.03)
    compound_end = (
        latest_equity * ((1.0 + mret) ** months) if latest_equity > 0 else 0.0
    )

    return {
        "hours": hours,
        "equity_curve": equity_series[-800:],
        "benchmark": benchmark,
        "rebalance": {
            "target": target,
            "current": {k: round(v, 6) for k, v in current_alloc.items()},
            "drifts": drifts,
            "drift_threshold": drift_threshold,
            "suggestions": suggestions,
        },
        "compounding": {
            "start_equity": round(latest_equity, 6),
            "monthly_return_assumption": mret,
            "months": months,
            "projected_equity": round(compound_end, 6),
        },
    }


async def _build_community_overview_payload(
    symbol: str = "BTC/USDT", exchange: str = "binance"
) -> Dict[str, Any]:
    (
        flow,
        whales,
        announcements,
        security_alerts,
        coinglass_whales,
        coinglass_exchange_transfers,
        coinglass_news,
    ) = await asyncio.gather(
        _fetch_trade_imbalance(exchange=exchange, symbol=symbol, limit=600),
        _fetch_whale_transfers(min_btc=_ANALYTICS_WHALE_MIN_BTC),
        _fetch_binance_announcements(limit=6),
        _fetch_slowmist_security_alerts(symbol=symbol, limit=6),
        _fetch_coinglass_whale_transfers(
            symbol=symbol, min_btc=_ANALYTICS_WHALE_MIN_BTC
        ),
        _fetch_coinglass_exchange_chain_transfers(
            symbol=symbol, min_btc=_ANALYTICS_WHALE_MIN_BTC
        ),
        _fetch_coinglass_news(symbol=symbol, limit=6),
    )
    merged_whales = _merge_whale_payloads(
        whales,
        coinglass_whales,
        coinglass_exchange_transfers,
        threshold_btc=_ANALYTICS_WHALE_MIN_BTC,
        btc_price=(
            _safe_float(whales.get("btc_price"))
            or _safe_float(coinglass_whales.get("btc_price"))
            or _safe_float(coinglass_exchange_transfers.get("btc_price"))
        ),
    )
    merged_announcements = _dedupe_announcements(
        [*(announcements or []), *(coinglass_news or [])],
        limit=10,
    )
    security_alerts_payload = dict(security_alerts or {})
    security_alerts_payload.setdefault("available", False)
    security_alerts_payload.setdefault("source", "slowmist_hacked")
    security_alerts_payload.setdefault("scope", "global_fallback")
    security_alerts_payload.setdefault("error", "")
    security_alerts_payload.setdefault("events", [])
    security_alerts_payload.setdefault("cache_hit", False)
    security_alerts_payload.setdefault("cache_age_sec", None)
    security_alerts_payload.setdefault("stale", False)
    security_alerts_payload.setdefault(
        "source_status",
        "live" if security_alerts_payload.get("available") else "unavailable",
    )
    news_sources = list(
        dict.fromkeys(
            source
            for source in (
                *(
                    str(item.get("provider") or "binance_announcements").strip()
                    for item in (announcements or [])
                ),
                *(
                    str(item.get("provider") or "").strip()
                    for item in (coinglass_news or [])
                ),
            )
            if source
        )
    )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "exchange": exchange,
        "twitter_watchlist": [
            "elonmusk",
            "cz_binance",
            "VitalikButerin",
            "lookonchain",
            "WuBlockchain",
        ],
        "flow_proxy": flow,
        "whale_transfers": merged_whales,
        "security_alerts": security_alerts_payload,
        "announcements": merged_announcements,
        "news_provider": "+".join(news_sources) if news_sources else None,
        "news_sources": news_sources,
    }


async def _build_microstructure_history_payload(
    *,
    exchange: str,
    symbol: str,
    depth_limit: int,
) -> Dict[str, Any]:
    ob, flow, funding_basis = await asyncio.gather(
        _fetch_orderbook(exchange=exchange, symbol=symbol, limit=depth_limit),
        _fetch_trade_imbalance(exchange=exchange, symbol=symbol, limit=800),
        _fetch_funding_basis_snapshot(exchange=exchange, symbol=symbol),
    )
    orderbook_view = _build_microstructure_orderbook_view(dict(ob or {}))
    funding = dict((funding_basis or {}).get("funding") or {"available": False})
    basis = dict((funding_basis or {}).get("basis") or {"available": False})
    return {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "available": bool((ob or {}).get("available", True)),
        "source_error": (ob or {}).get("error"),
        "orderbook": dict(orderbook_view.get("orderbook") or {}),
        "large_orders": list(orderbook_view.get("large_orders") or []),
        "iceberg_detection": {
            "candidate_count": int(orderbook_view.get("iceberg_candidate_count") or 0),
            "note": "基于盘口重复量级的启发式检测。",
        },
        "aggressor_flow": {
            "available": bool((flow or {}).get("available", True)),
            "error": (flow or {}).get("error"),
            "count": int(_safe_float((flow or {}).get("count"))),
            "buy_volume": _safe_float((flow or {}).get("buy_volume")),
            "sell_volume": _safe_float((flow or {}).get("sell_volume")),
            "imbalance": _safe_float((flow or {}).get("imbalance")),
        },
        "long_short_ratio": {"available": False},
        "funding_rate": funding,
        "spot_futures_basis": basis,
    }


async def _build_community_history_payload(
    *,
    symbol: str,
    exchange: str,
) -> Dict[str, Any]:
    flow, announcements, security_alerts, coinglass_news = await asyncio.gather(
        _fetch_trade_imbalance(exchange=exchange, symbol=symbol, limit=600),
        _fetch_binance_announcements(limit=6),
        _fetch_slowmist_security_alerts(symbol=symbol, limit=6),
        _fetch_coinglass_news(symbol=symbol, limit=6),
    )
    merged_announcements = _dedupe_announcements(
        [*(announcements or []), *(coinglass_news or [])],
        limit=10,
    )
    security_alerts_payload = dict(security_alerts or {})
    security_alerts_payload.setdefault("available", False)
    security_alerts_payload.setdefault("source", "slowmist_hacked")
    security_alerts_payload.setdefault("scope", "global_fallback")
    security_alerts_payload.setdefault("error", "")
    security_alerts_payload.setdefault("events", [])
    security_alerts_payload.setdefault("cache_hit", False)
    security_alerts_payload.setdefault("cache_age_sec", None)
    security_alerts_payload.setdefault("stale", False)
    security_alerts_payload.setdefault(
        "source_status",
        "live" if security_alerts_payload.get("available") else "unavailable",
    )
    news_sources = list(
        dict.fromkeys(
            source
            for source in (
                *(
                    str(item.get("provider") or "binance_announcements").strip()
                    for item in (announcements or [])
                ),
                *(
                    str(item.get("provider") or "").strip()
                    for item in (coinglass_news or [])
                ),
            )
            if source
        )
    )
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "exchange": exchange,
        "source_error": "",
        "twitter_watchlist": [],
        "flow_proxy": flow,
        "whale_transfers": _analytics_fallback_whales(
            "whale_history_collector_runs_separately"
        ),
        "security_alerts": security_alerts_payload,
        "announcements": merged_announcements,
        "news_provider": "+".join(news_sources) if news_sources else None,
        "news_sources": news_sources,
    }


async def _build_whales_history_payload(
    *,
    symbol: str,
    min_btc: float = _ANALYTICS_WHALE_MIN_BTC,
) -> Dict[str, Any]:
    whales, coinglass_whales, coinglass_exchange_transfers = await asyncio.gather(
        _fetch_whale_transfers(min_btc=min_btc),
        _fetch_coinglass_whale_transfers(symbol=symbol, min_btc=min_btc),
        _fetch_coinglass_exchange_chain_transfers(symbol=symbol, min_btc=min_btc),
    )
    return _merge_whale_payloads(
        whales,
        coinglass_whales,
        coinglass_exchange_transfers,
        threshold_btc=min_btc,
        btc_price=(
            _safe_float((whales or {}).get("btc_price"))
            or _safe_float((coinglass_whales or {}).get("btc_price"))
            or _safe_float((coinglass_exchange_transfers or {}).get("btc_price"))
        ),
    )


async def get_community_overview(symbol: str = "BTC/USDT", exchange: str = "binance"):
    cache_key = f"{str(exchange or '').lower()}|{str(symbol or '').upper()}"
    cached, cached_age = _cache_get(
        _COMMUNITY_OVERVIEW_CACHE,
        cache_key,
        max_age_sec=_COMMUNITY_OVERVIEW_CACHE_TTL_SEC,
    )
    if cached is not None:
        return _with_community_runtime_fields(
            cached,
            cache_hit=True,
            cache_age_sec=cached_age,
            stale=False,
            source_status="cache_fresh",
        )

    stale_cached, stale_age = _cache_get(
        _COMMUNITY_OVERVIEW_CACHE,
        cache_key,
        max_age_sec=_COMMUNITY_OVERVIEW_STALE_MAX_AGE_SEC,
    )
    if stale_cached is not None:
        _schedule_cache_refresh(
            _COMMUNITY_REFRESH_TASKS,
            cache_key,
            build_coro=lambda: _build_community_overview_payload(
                symbol=symbol, exchange=exchange
            ),
            cache=_COMMUNITY_OVERVIEW_CACHE,
            strip_payload=_strip_community_runtime_fields,
        )
        return _with_community_runtime_fields(
            stale_cached,
            cache_hit=True,
            cache_age_sec=stale_age,
            stale=True,
            source_status="cache_stale",
            stale_reason="background_refresh_scheduled",
        )

    payload = await _build_community_overview_payload(symbol=symbol, exchange=exchange)
    _cache_put(
        _COMMUNITY_OVERVIEW_CACHE, cache_key, _strip_community_runtime_fields(payload)
    )
    return _with_community_runtime_fields(
        payload,
        cache_hit=False,
        cache_age_sec=0.0,
        stale=False,
        source_status="live",
    )


async def collect_analytics_history(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
    collectors: Optional[str] = None,
):
    result = await run_analytics_history_collection(
        exchange=exchange,
        symbol=symbol,
        depth_limit=depth_limit,
        collectors=collectors,
    )
    _invalidate_analytics_history_cache(exchange=exchange, symbol=symbol)
    return {
        **result,
        "saved": {
            "captured_at": result.get("finished_at"),
            "rows_written": int(result.get("rows_written") or 0),
        },
        "microstructure": dict(
            (result.get("results") or {}).get("microstructure", {}).get("summary") or {}
        ),
        "community": dict(
            (result.get("results") or {}).get("community", {}).get("summary") or {}
        ),
        "whales": dict(
            (result.get("results") or {}).get("whales", {}).get("summary") or {}
        ),
    }


async def get_analytics_history_health(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    hours: int = 24 * 7,
    refresh: bool = False,
    depth_limit: int = 80,
):
    hours = max(24, min(int(hours or 24 * 7), 24 * 365))
    cache_key = _analytics_history_cache_key(
        exchange=exchange, symbol=symbol, hours=hours
    )

    def _with_common_fields(
        payload: Dict[str, Any],
        *,
        cache_hit: bool,
        cache_age: Optional[float],
        stale: bool,
    ) -> Dict[str, Any]:
        out = dict(payload or {})
        out["cache_hit"] = bool(cache_hit)
        out["cache_age_sec"] = (
            round(float(cache_age or 0.0), 3) if cache_age is not None else None
        )
        out["stale"] = bool(stale)
        out["refreshed"] = None
        out["refresh_requested"] = bool(refresh)
        out[
            "refresh_note"
        ] = "health 接口当前为纯读接口；实时采集请改用 POST /api/trading/analytics/history/collect。"
        return out

    cached, cached_age = _cache_get(
        _ANALYTICS_HISTORY_HEALTH_CACHE,
        cache_key,
        max_age_sec=_ANALYTICS_HISTORY_HEALTH_CACHE_TTL_SEC,
    )
    if cached is not None:
        return await _attach_derivatives_analytics_health(
            _with_common_fields(
                cached, cache_hit=True, cache_age=float(cached_age or 0.0), stale=False
            ),
            exchange=exchange,
            symbol=symbol,
        )

    stale_cached, stale_age = _cache_get(
        _ANALYTICS_HISTORY_HEALTH_CACHE,
        cache_key,
        max_age_sec=None,
    )
    if (not bool(refresh)) and (stale_cached is not None):
        return await _attach_derivatives_analytics_health(
            _with_common_fields(
                stale_cached,
                cache_hit=True,
                cache_age=float(stale_age or 0.0),
                stale=True,
            ),
            exchange=exchange,
            symbol=symbol,
        )

    # Fast path for dashboard polling: derive health from ingest status map only.
    # This avoids expensive aggregate scans causing frontend timeout.
    if not bool(refresh):
        status_map: Dict[str, Dict[str, Any]] = {}
        status_cache_key = _analytics_history_cache_key(
            exchange=exchange, symbol=symbol
        )
        cached_status_payload, _ = _cache_get(
            _ANALYTICS_HISTORY_STATUS_CACHE,
            status_cache_key,
            max_age_sec=None,
        )
        if cached_status_payload is not None:
            for row in list(cached_status_payload.get("collectors") or []):
                collector = str((row or {}).get("collector") or "").strip().lower()
                if collector:
                    status_map[collector] = dict(row or {})
        if not status_map and _ANALYTICS_HISTORY_STATUS_LAST:
            for collector in _ANALYTICS_HISTORY_COLLECTORS:
                if collector in _ANALYTICS_HISTORY_STATUS_LAST:
                    status_map[collector] = dict(
                        _ANALYTICS_HISTORY_STATUS_LAST.get(collector) or {}
                    )
        if not status_map:
            with contextlib.suppress(Exception):
                status_map = await asyncio.wait_for(
                    _load_analytics_ingest_status_map(),
                    timeout=max(
                        0.6, min(1.2, _ANALYTICS_HISTORY_STATUS_READ_TIMEOUT_SEC)
                    ),
                )
        quick_payload = (
            _status_fallback_analytics_history_health(
                exchange=exchange,
                symbol=symbol,
                hours=hours,
                status_map=status_map,
                error="quick_status_mode",
            )
            if status_map
            else _empty_analytics_history_health(
                exchange=exchange,
                symbol=symbol,
                hours=hours,
                error="quick_status_mode",
            )
        )
        quick_payload["fallback_mode"] = "quick_status"
        _cache_put(_ANALYTICS_HISTORY_HEALTH_CACHE, cache_key, quick_payload)
        return await _attach_derivatives_analytics_health(
            _with_common_fields(
                quick_payload, cache_hit=False, cache_age=0.0, stale=True
            ),
            exchange=exchange,
            symbol=symbol,
        )

    # Explicit refresh mode: allow expensive read and fallback to stale snapshot on failure.
    try:
        health = await asyncio.wait_for(
            _build_analytics_history_health(
                exchange=exchange, symbol=symbol, hours=hours
            ),
            timeout=max(1.0, _ANALYTICS_HISTORY_HEALTH_READ_TIMEOUT_SEC),
        )
        _cache_put(_ANALYTICS_HISTORY_HEALTH_CACHE, cache_key, health)
        return await _attach_derivatives_analytics_health(
            _with_common_fields(health, cache_hit=False, cache_age=0.0, stale=False),
            exchange=exchange,
            symbol=symbol,
        )
    except Exception as exc:
        if stale_cached is not None:
            stale_payload = dict(stale_cached)
            stale_payload["stale_reason"] = _clip_analytics_error(exc)
            return await _attach_derivatives_analytics_health(
                _with_common_fields(
                    stale_payload,
                    cache_hit=True,
                    cache_age=float(stale_age or 0.0),
                    stale=True,
                ),
                exchange=exchange,
                symbol=symbol,
            )
        fallback = _empty_analytics_history_health(
            exchange=exchange,
            symbol=symbol,
            hours=hours,
            error=_clip_analytics_error(exc) or "analytics history health read failed",
        )
        return await _attach_derivatives_analytics_health(
            _with_common_fields(fallback, cache_hit=False, cache_age=None, stale=True),
            exchange=exchange,
            symbol=symbol,
        )


async def get_analytics_history_status(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
):
    cache_key = _analytics_history_cache_key(exchange=exchange, symbol=symbol)
    status_timeout_sec = max(0.8, min(1.8, _ANALYTICS_HISTORY_STATUS_READ_TIMEOUT_SEC))
    stale_cached, stale_age = _cache_get(
        _ANALYTICS_HISTORY_STATUS_CACHE,
        cache_key,
        max_age_sec=None,
    )
    if stale_cached is not None and float(stale_age or 0.0) <= 90.0:
        return await _attach_derivatives_analytics_status(
            {
                **stale_cached,
                "generated_at": _utc_iso(datetime.now(timezone.utc)),
                "cache_hit": True,
                "cache_age_sec": round(float(stale_age or 0.0), 3),
                "stale": bool(
                    float(stale_age or 0.0) > _ANALYTICS_HISTORY_STATUS_CACHE_TTL_SEC
                ),
            },
            exchange=exchange,
            symbol=symbol,
        )
    cached, cached_age = _cache_get(
        _ANALYTICS_HISTORY_STATUS_CACHE,
        cache_key,
        max_age_sec=_ANALYTICS_HISTORY_STATUS_CACHE_TTL_SEC,
    )
    if cached is not None:
        collectors = list(cached.get("collectors") or [])
        return await _attach_derivatives_analytics_status(
            {
                "generated_at": _utc_iso(datetime.now(timezone.utc)),
                "exchange": exchange,
                "symbol": symbol,
                "collectors": collectors,
                "cache_hit": True,
                "cache_age_sec": round(float(cached_age or 0.0), 3),
            },
            exchange=exchange,
            symbol=symbol,
        )

    if _ANALYTICS_HISTORY_STATUS_LAST:
        collectors = _status_map_to_collectors(
            {k: dict(v) for k, v in _ANALYTICS_HISTORY_STATUS_LAST.items()},
            exchange=exchange,
            symbol=symbol,
        )
        if collectors:
            payload = {
                "generated_at": _utc_iso(datetime.now(timezone.utc)),
                "exchange": exchange,
                "symbol": symbol,
                "collectors": collectors,
                "cache_hit": False,
                "cache_age_sec": None,
                "stale": True,
                "fallback_mode": "in_memory_status",
            }
            _cache_put(_ANALYTICS_HISTORY_STATUS_CACHE, cache_key, payload)
            return await _attach_derivatives_analytics_status(
                payload, exchange=exchange, symbol=symbol
            )

    try:
        status_map = await asyncio.wait_for(
            _load_analytics_ingest_status_map(),
            timeout=status_timeout_sec,
        )
        collectors = _status_map_to_collectors(
            status_map, exchange=exchange, symbol=symbol
        )
        payload = {
            "generated_at": _utc_iso(datetime.now(timezone.utc)),
            "exchange": exchange,
            "symbol": symbol,
            "collectors": collectors,
            "cache_hit": False,
            "cache_age_sec": 0.0,
        }
        _cache_put(_ANALYTICS_HISTORY_STATUS_CACHE, cache_key, payload)
        return await _attach_derivatives_analytics_status(
            payload, exchange=exchange, symbol=symbol
        )
    except Exception as exc:
        stale, stale_age = _cache_get(_ANALYTICS_HISTORY_STATUS_CACHE, cache_key)
        if stale is not None:
            return await _attach_derivatives_analytics_status(
                {
                    **stale,
                    "generated_at": _utc_iso(datetime.now(timezone.utc)),
                    "cache_hit": True,
                    "cache_age_sec": round(float(stale_age or 0.0), 3),
                    "stale": True,
                    "stale_reason": _clip_analytics_error(exc),
                },
                exchange=exchange,
                symbol=symbol,
            )
        if _ANALYTICS_HISTORY_STATUS_LAST:
            collectors = _status_map_to_collectors(
                {k: dict(v) for k, v in _ANALYTICS_HISTORY_STATUS_LAST.items()},
                exchange=exchange,
                symbol=symbol,
            )
            if collectors:
                return await _attach_derivatives_analytics_status(
                    {
                        "generated_at": _utc_iso(datetime.now(timezone.utc)),
                        "exchange": exchange,
                        "symbol": symbol,
                        "collectors": collectors,
                        "cache_hit": False,
                        "cache_age_sec": None,
                        "stale": True,
                        "stale_reason": _clip_analytics_error(exc),
                        "fallback_mode": "in_memory_status",
                    },
                    exchange=exchange,
                    symbol=symbol,
                )
        fallback_status_map: Dict[str, Dict[str, Any]] = {}
        for collector in _ANALYTICS_HISTORY_COLLECTORS:
            fallback_status_map[collector] = {
                "collector": collector,
                "exchange": exchange,
                "symbol": symbol,
                "status": "degraded",
                "error": _clip_analytics_error(exc),
                "rows_written": 0,
                "started_at": None,
                "finished_at": None,
                "updated_at": _utc_iso(datetime.now(timezone.utc)),
                "details": {"phase": "fallback"},
            }
        return await _attach_derivatives_analytics_status(
            {
                "generated_at": _utc_iso(datetime.now(timezone.utc)),
                "exchange": exchange,
                "symbol": symbol,
                "collectors": _status_map_to_collectors(
                    fallback_status_map, exchange=exchange, symbol=symbol
                ),
                "cache_hit": False,
                "cache_age_sec": None,
                "stale": True,
                "stale_reason": _clip_analytics_error(exc),
            },
            exchange=exchange,
            symbol=symbol,
        )


async def get_audit_logs(
    hours: int = 168,
    limit: int = 100,
    module: Optional[str] = None,
    action: Optional[str] = None,
    status: Optional[str] = None,
):
    rows = await audit_logger.list_logs(
        module=(module or None),
        action=(action or None),
        status=(status or None),
        hours=max(1, min(int(hours or 168), 24 * 365)),
        limit=max(1, min(int(limit or 100), 500)),
    )
    return {
        "hours": max(1, min(int(hours or 168), 24 * 365)),
        "count": len(rows),
        "logs": rows,
    }


async def get_live_trade_review(
    hours: int = 24 * 7,
    limit: int = 200,
    strategy: Optional[str] = None,
):
    return execution_engine.get_live_trade_review(
        hours=max(1, min(int(hours or 24 * 7), 24 * 365)),
        limit=max(1, min(int(limit or 200), 2000)),
        strategy=(strategy or None),
    )


async def get_pnl_heatmap(
    days: int = 30,
    bucket: str = "day",
    mode: Optional[str] = None,
):
    bucket_name = "hour" if str(bucket or "").lower() == "hour" else "day"
    days = max(1, min(int(days or 30), 365))
    target_mode = _normalize_runtime_mode(mode or execution_engine.get_trading_mode())
    mode_label = "模拟盘" if target_mode == "paper" else "实盘"
    records = _iter_trade_records(days=days, mode=target_mode)

    filtered: List[Dict[str, Any]] = []
    for row in records:
        closed_at = _safe_dt(row.get("timestamp"))
        if not closed_at:
            continue
        symbol = str(row.get("symbol") or "").strip() or "UNKNOWN"
        pnl = _safe_float(row.get("pnl"), default=0.0)
        filtered.append(
            {
                "symbol": symbol,
                "closed_at": closed_at,
                "value": pnl,
            }
        )

    display_mode = "realized_pnl"
    value_title = "PnL"
    value_hover = "PnL"
    note = f"当前展示 {mode_label} 已平仓真实交易盈亏聚合。"

    if not filtered:
        if target_mode == "live":
            try:
                live_income_rows = await asyncio.wait_for(
                    _fetch_binance_realized_pnl_income(days=days),
                    timeout=5.5,
                )
            except Exception:
                live_income_rows = []
            for row in live_income_rows:
                filtered.append(
                    {
                        "symbol": str(row.get("symbol") or "").strip() or "UNKNOWN",
                        "closed_at": _safe_dt(row.get("timestamp")),
                        "value": _safe_float(row.get("pnl"), default=0.0),
                    }
                )
            filtered = [row for row in filtered if row.get("closed_at")]

    if not filtered:
        try:
            audit_rows = await audit_logger.list_logs(
                module="trading",
                action="trade_close",
                status="success",
                hours=days * 24,
                limit=5000,
            )
        except Exception:
            audit_rows = []
        for row in audit_rows:
            details = dict(row.get("details") or {})
            if _infer_trade_item_mode(payload=details) != target_mode:
                continue
            ts = _safe_dt(details.get("timestamp") or row.get("timestamp"))
            if not ts:
                continue
            symbol = str(details.get("symbol") or "").strip() or "UNKNOWN"
            pnl = _safe_float(details.get("pnl"), default=0.0)
            filtered.append(
                {
                    "symbol": symbol,
                    "closed_at": ts,
                    "value": pnl,
                }
            )

    if not filtered:
        fallback_orders = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        for order in order_manager.get_recent_orders(limit=5000):
            meta = order_manager.get_order_metadata(str(getattr(order, "id", "") or ""))
            if _infer_trade_item_mode(
                payload=meta,
                account_id=getattr(order, "account_id", None),
                order_id=getattr(order, "id", None),
            ) != target_mode:
                continue
            ts = _safe_dt(getattr(order, "timestamp", None))
            if not ts or ts < cutoff:
                continue
            status = str(
                getattr(
                    getattr(order, "status", None),
                    "value",
                    getattr(order, "status", ""),
                )
                or ""
            ).lower()
            if status not in {"closed", "filled"}:
                continue
            amount = _safe_float(
                getattr(order, "filled", None),
                default=_safe_float(getattr(order, "amount", 0.0)),
            )
            price = _safe_float(getattr(order, "price", 0.0))
            symbol = str(getattr(order, "symbol", "") or "").strip() or "UNKNOWN"
            if amount <= 0 or price <= 0:
                continue
            side = str(
                getattr(
                    getattr(order, "side", None), "value", getattr(order, "side", "")
                )
                or ""
            ).lower()
            signed_cashflow = amount * price * (-1.0 if side == "buy" else 1.0)
            fallback_orders.append(
                {
                    "symbol": symbol,
                    "closed_at": ts,
                    "value": signed_cashflow,
                }
            )
        filtered = fallback_orders
        display_mode = "cashflow_proxy"
        value_title = "Cashflow"
        value_hover = "现金流代理"
        note = (
            f"当前 {mode_label} 无已平仓盈亏记录，回退显示已成交订单现金流代理"
            "（卖出为正，买入为负）。"
        )

    if not filtered:
        return {
            "bucket": bucket_name,
            "days": days,
            "mode": target_mode,
            "times": [],
            "symbols": [],
            "matrix": [],
            "trade_count": 0,
            "display_mode": "empty",
            "value_title": value_title,
            "value_hover": value_hover,
            "note": f"当前 {mode_label} 暂无可用于绘制热力图的已平仓交易或已成交订单记录。",
        }

    symbol_set = sorted({row["symbol"] for row in filtered})
    bucket_set = sorted(
        {_bucket_key(row["closed_at"], bucket_name) for row in filtered}
    )
    symbol_index = {sym: idx for idx, sym in enumerate(symbol_set)}
    bucket_index = {ts: idx for idx, ts in enumerate(bucket_set)}
    matrix = [[0.0 for _ in symbol_set] for _ in bucket_set]

    for row in filtered:
        x = symbol_index[row["symbol"]]
        y = bucket_index[_bucket_key(row["closed_at"], bucket_name)]
        matrix[y][x] += float(row["value"])

    return {
        "bucket": bucket_name,
        "days": days,
        "mode": target_mode,
        "times": bucket_set,
        "symbols": symbol_set,
        "matrix": [[round(float(v), 6) for v in row] for row in matrix],
        "trade_count": len(filtered),
        "display_mode": display_mode,
        "value_title": value_title,
        "value_hover": value_hover,
        "note": note,
    }
