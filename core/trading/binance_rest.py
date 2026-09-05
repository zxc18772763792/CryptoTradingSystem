from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import threading
import time
import weakref
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx

from config.settings import settings
from core.trading.account_manager import account_manager
from core.utils.asset_valuation import STABLE_COINS

_BINANCE_REST_TIMEOUT_SEC = 8.0
_BINANCE_RECV_WINDOW = 59000
_BINANCE_TIME_OFFSET_MS: Dict[str, Any] = {"api": 0, "fapi": 0, "ts": 0.0}
_BINANCE_TIME_OFFSET_LOCKS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]" = (
    weakref.WeakKeyDictionary()
)
_BINANCE_TIME_OFFSET_LOCK_GUARD = threading.Lock()


def _apply_httpx_proxy_kw(client_kwargs: Dict[str, Any], proxy_url: Optional[str]) -> None:
    if not proxy_url:
        return
    try:
        params = inspect.signature(httpx.AsyncClient.__init__).parameters
    except (TypeError, ValueError):
        client_kwargs["proxy"] = proxy_url
        return

    if "proxy" in params:
        client_kwargs["proxy"] = proxy_url
    elif "proxies" in params:
        client_kwargs["proxies"] = proxy_url
    else:
        client_kwargs.setdefault("trust_env", True)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _get_binance_time_offset_lock() -> asyncio.Lock:
    """Return a refresh lock bound to the currently running event loop.

    The module is reused by the web runtime, scripts, and tests. A single
    process-global ``asyncio.Lock`` becomes permanently bound after its first
    contended use and then fails when a later runner creates a new loop.
    Weakly keying locks by loop keeps refresh serialization without leaking
    closed loops or coupling independent runtimes.
    """
    loop = asyncio.get_running_loop()
    with _BINANCE_TIME_OFFSET_LOCK_GUARD:
        lock = _BINANCE_TIME_OFFSET_LOCKS.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            _BINANCE_TIME_OFFSET_LOCKS[loop] = lock
        return lock


async def _refresh_binance_time_offset(
    target_host: str,
    *,
    proxy_url: Optional[str],
    force: bool = False,
) -> int:
    now_ts = time.time()
    if (not force) and (now_ts - float(_BINANCE_TIME_OFFSET_MS.get("ts") or 0.0)) <= 180.0:
        cached = int(_BINANCE_TIME_OFFSET_MS.get(target_host, 0) or 0)
        if cached:
            return cached

    async with _get_binance_time_offset_lock():
        now_ts = time.time()
        if (not force) and (now_ts - float(_BINANCE_TIME_OFFSET_MS.get("ts") or 0.0)) <= 180.0:
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


def _binance_credentials(account_id: Optional[str] = None) -> Dict[str, str]:
    credentials = account_manager.get_exchange_credentials(account_id, "binance")
    return {
        "api_key": str(credentials.get("api_key") or "").strip(),
        "api_secret": str(credentials.get("api_secret") or "").strip(),
        "proxy": str(credentials.get("proxy") or settings.HTTP_PROXY or settings.HTTPS_PROXY or "").strip(),
    }


def binance_has_credentials(account_id: Optional[str] = None) -> bool:
    creds = _binance_credentials(account_id)
    return bool(creds["api_key"] and creds["api_secret"])


def binance_market_symbol(symbol: Optional[str]) -> Optional[str]:
    text = str(symbol or "").upper().strip()
    if not text:
        return None
    if ":" in text:
        text = text.split(":", 1)[0]
    return text.replace("/", "").replace("-", "")


def binance_ccxt_symbol(symbol: str, quote: str = "USDT", futures: bool = False) -> str:
    base = str(symbol or "").upper().replace("/", "").replace("-", "")
    if base.endswith(quote):
        asset = base[: -len(quote)]
        if asset:
            return f"{asset}/{quote}:USDT" if futures else f"{asset}/{quote}"
    return symbol


async def binance_signed_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    host: str = "api",
    timeout_sec: float = _BINANCE_REST_TIMEOUT_SEC,
    account_id: Optional[str] = None,
) -> Any:
    credentials = _binance_credentials(account_id)
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]
    if not api_key or not api_secret:
        raise RuntimeError("binance api credentials unavailable")

    base_url = "https://api.binance.com"
    if host == "fapi":
        base_url = "https://fapi.binance.com"
    elif host == "sapi":
        base_url = "https://api.binance.com"

    proxy_url = credentials["proxy"] or None

    async def _ensure_offsets() -> None:
        await asyncio.gather(
            _refresh_binance_time_offset("api", proxy_url=proxy_url, force=False),
            _refresh_binance_time_offset("fapi", proxy_url=proxy_url, force=False),
            return_exceptions=True,
        )

    async def _send_once(force_time_refresh: bool = False) -> httpx.Response:
        if force_time_refresh:
            await _refresh_binance_time_offset(
                "fapi" if host == "fapi" else "api",
                proxy_url=proxy_url,
                force=True,
            )
        offset_ms = int(_BINANCE_TIME_OFFSET_MS.get("fapi" if host == "fapi" else "api", 0) or 0)
        payload: Dict[str, Any] = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000) + offset_ms
        payload["recvWindow"] = int(_BINANCE_RECV_WINDOW)
        query = urlencode([(k, v) for k, v in payload.items() if v is not None], doseq=True)
        signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": api_key}
        url = f"{base_url}{path}"
        client_kwargs: Dict[str, Any] = {"timeout": timeout_sec, "headers": headers}
        _apply_httpx_proxy_kw(client_kwargs, proxy_url)
        async with httpx.AsyncClient(**client_kwargs) as client:
            if method.upper() == "GET":
                return await client.get(url, params={**payload, "signature": signature})
            return await client.post(url, data={**payload, "signature": signature})

    await _ensure_offsets()
    resp = await _send_once(force_time_refresh=False)
    if resp.status_code >= 400 and "-1021" in (resp.text or ""):
        resp = await _send_once(force_time_refresh=True)
    if resp.status_code >= 400:
        # httpx's default HTTPStatusError omits the response body, which hides
        # the actionable Binance error code (e.g. -4014 tick size, -1111
        # precision, -2010 balance). Surface it so callers/logs can diagnose.
        body = (resp.text or "").strip()
        raise httpx.HTTPStatusError(
            f"{method.upper()} {path} -> {resp.status_code}: {body}",
            request=resp.request,
            response=resp,
        )
    return resp.json()


async def _binance_public_price_usd(asset: str, timeout_sec: float = 1.6) -> float:
    ccy = str(asset or "").upper().strip()
    if not ccy:
        return 0.0
    if ccy in STABLE_COINS:
        return 1.0
    symbol = f"{ccy}USDT"
    client_kwargs: Dict[str, Any] = {"timeout": timeout_sec}
    proxy_url = str(settings.HTTP_PROXY or settings.HTTPS_PROXY or "").strip() or None
    _apply_httpx_proxy_kw(client_kwargs, proxy_url)
    async with httpx.AsyncClient(**client_kwargs) as client:
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
    unique_assets: List[str] = []
    seen = set()
    for asset in assets:
        ccy = str(asset or "").upper().strip()
        if not ccy or ccy in seen:
            continue
        seen.add(ccy)
        unique_assets.append(ccy)
    if not unique_assets:
        return {}

    proxy_url = str(settings.HTTP_PROXY or settings.HTTPS_PROXY or "").strip() or None
    client_kwargs: Dict[str, Any] = {"timeout": 2.5}
    _apply_httpx_proxy_kw(client_kwargs, proxy_url)
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.get("https://api.binance.com/api/v3/ticker/price")
            resp.raise_for_status()
            rows = resp.json() or []
        price_map = {
            str(row.get("symbol") or "").upper(): _safe_float(row.get("price"), default=0.0)
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


async def fetch_binance_live_wallet_snapshot_fast(account_id: Optional[str] = None) -> Dict[str, Any]:
    if not binance_has_credentials(account_id):
        raise RuntimeError("binance api credentials unavailable")

    async def _get_spot_account():
        return await binance_signed_request("GET", "/api/v3/account", host="api", account_id=account_id)

    async def _get_futures_balance():
        try:
            return await binance_signed_request("GET", "/fapi/v3/balance", host="fapi", account_id=account_id)
        except Exception:
            return await binance_signed_request("GET", "/fapi/v2/balance", host="fapi", account_id=account_id)

    async def _get_funding_wallet():
        try:
            return await binance_signed_request(
                "POST",
                "/sapi/v1/asset/get-funding-asset",
                host="sapi",
                params={"needBtcValuation": "false"},
                account_id=account_id,
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
    ) -> None:
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
        for row in funding_raw if isinstance(funding_raw, list) else []:
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
        for row in futures_raw if isinstance(futures_raw, list) else []:
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
        unit_usd = 1.0 if currency in STABLE_COINS else _safe_float(quotes.get(currency), default=0.0)
        usd_value = _safe_float(row.get("total"), default=0.0) * unit_usd if unit_usd > 0 else 0.0
        row["unit_usd"] = round(unit_usd, 8) if unit_usd > 0 else 0.0
        row["usd_value"] = round(usd_value, 4)
        row["valuation_source"] = "stable" if currency in STABLE_COINS else ("live" if unit_usd > 0 else "unpriced")
        distribution[currency] = distribution.get(currency, 0.0) + float(usd_value or 0.0)
        wallet_source = str(row.get("wallet_source") or "spot")
        components[wallet_source] = components.get(wallet_source, 0.0) + float(usd_value or 0.0)
        total_usd += float(usd_value or 0.0)

    balances.sort(key=lambda item: float(item.get("usd_value") or 0.0), reverse=True)
    return {
        "balances": balances,
        "distribution": distribution,
        "total_usd": round(total_usd, 2),
        "components": {key: round(value, 2) for key, value in components.items()},
        "warnings": warnings,
        "valuation_coverage": {
            "priced_assets": sum(1 for row in balances if float(row.get("usd_value") or 0.0) > 0),
            "unpriced_assets": sum(
                1
                for row in balances
                if float(row.get("total") or 0.0) > 0
                and float(row.get("usd_value") or 0.0) <= 0
            ),
        },
        "account_id": str(account_id or "main"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
