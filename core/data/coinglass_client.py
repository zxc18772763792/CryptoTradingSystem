from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import urlparse

import aiohttp
from loguru import logger
import pandas as pd
from sqlalchemy import select

from config.database import CoinglassBudgetLedger, CoinglassIngestStatus, async_session_maker
from config.settings import settings
from core.data.coinglass_registry import (
    COINGLASS_DEFAULT_DATASETS,
    CoinglassDatasetManifest,
    CoinglassRouteSpec,
    coinglass_pair_symbol,
    coinglass_range_for_interval,
    coinglass_symbol_matches,
    get_coinglass_manifest,
    normalize_coinglass_exchange,
    normalize_coinglass_interval,
    normalize_coinglass_symbol,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CACHE_ROOT = _PROJECT_ROOT / "data" / "premium" / "coinglass"
_RAW_ROOT = _CACHE_ROOT / "raw"
_NORMALIZED_ROOT = _CACHE_ROOT / "normalized"
_API_SPEC_PATH = _CACHE_ROOT / "api_spec_v4.json"
_CAPABILITY_MATRIX_PATH = _CACHE_ROOT / "capability_matrix.json"
_SYMBOL_REGISTRY_PATH = _CACHE_ROOT / "symbol_registry.json"
_API_KEY_FILE = _PROJECT_ROOT / "config" / "coinglass_api_key.txt"
_BUDGET_SCOPE = "global"
_REQUEST_TIMEOUT_SEC = 20
_NON_MANUAL_MINUTE_RESERVE = 2
_REQUEST_LOCK = asyncio.Lock()


class CoinglassError(RuntimeError):
    """Base class for CoinGlass runtime failures."""


class CoinglassBudgetExceeded(CoinglassError):
    """Raised when local budget checks deny an outbound request."""


@dataclass
class CoinglassBudgetState:
    enabled: bool
    key_configured: bool
    minute_limit: int
    minute_used: int
    minute_remaining: int
    daily_limit: int
    daily_used: int
    daily_remaining: int
    monthly_limit: int
    monthly_used: int
    monthly_remaining: int
    request_success_count: int
    rate_limit_hit_count: int
    last_success_at: Optional[str]
    last_error: str
    last_http_status: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "key_configured": self.key_configured,
            "minute_limit": self.minute_limit,
            "minute_used": self.minute_used,
            "minute_remaining": self.minute_remaining,
            "daily_limit": self.daily_limit,
            "daily_used": self.daily_used,
            "daily_remaining": self.daily_remaining,
            "monthly_limit": self.monthly_limit,
            "monthly_used": self.monthly_used,
            "monthly_remaining": self.monthly_remaining,
            "request_success_count": self.request_success_count,
            "rate_limit_hit_count": self.rate_limit_hit_count,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "last_http_status": self.last_http_status,
        }


@dataclass
class CoinglassIngestStatusPayload:
    dataset: str
    scope_key: str
    status: str
    symbol: str
    exchange: str
    interval: str
    rows_written: int
    last_success_at: Optional[str]
    last_attempt_at: Optional[str]
    updated_at: Optional[str]
    fresh_until_at: Optional[str]
    error: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "scope_key": self.scope_key,
            "status": self.status,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "interval": self.interval,
            "rows_written": self.rows_written,
            "last_success_at": self.last_success_at,
            "last_attempt_at": self.last_attempt_at,
            "updated_at": self.updated_at,
            "fresh_until_at": self.fresh_until_at,
            "error": self.error,
            "details": dict(self.details or {}),
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _utc_naive(value: Optional[datetime] = None) -> datetime:
    current = value or _utc_now()
    if current.tzinfo is not None:
        current = current.astimezone(timezone.utc).replace(tzinfo=None)
    return current


def _day_key(now: Optional[datetime] = None) -> str:
    return (now or _utc_now()).strftime("%Y-%m-%d")


def _month_key(now: Optional[datetime] = None) -> str:
    return (now or _utc_now()).strftime("%Y-%m")


def _minute_window(now: Optional[datetime] = None) -> datetime:
    current = (now or _utc_now()).astimezone(timezone.utc)
    rounded = current.replace(second=0, microsecond=0)
    return rounded.replace(tzinfo=None)


def _coinglass_base_url() -> str:
    return str(getattr(settings, "COINGLASS_BASE_URL", "") or "").strip().rstrip("/")


def _coinglass_root_url() -> str:
    base = _coinglass_base_url()
    if not base:
        return ""
    parsed = urlparse(base)
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def coinglass_api_key() -> str:
    key = str(getattr(settings, "COINGLASS_API_KEY", "") or "").strip()
    if key:
        return key
    try:
        return _API_KEY_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def coinglass_key_configured() -> bool:
    return bool(coinglass_api_key())


def coinglass_enabled() -> bool:
    return bool(getattr(settings, "COINGLASS_ENABLED", False) and _coinglass_base_url() and coinglass_key_configured())


def _ensure_cache_dirs() -> None:
    _RAW_ROOT.mkdir(parents=True, exist_ok=True)
    _NORMALIZED_ROOT.mkdir(parents=True, exist_ok=True)


def canonical_request_key(
    *,
    dataset: str,
    api_version: str,
    market_type: str,
    normalized_symbol: str,
    exchange: str,
    interval: str,
    source_ts: str,
) -> str:
    parts = [
        str(dataset or "").strip(),
        str(api_version or "").strip(),
        str(market_type or "").strip(),
        str(normalized_symbol or "").strip(),
        str(exchange or "").strip(),
        str(interval or "").strip(),
        str(source_ts or "").strip(),
    ]
    return "|".join(parts)


def _clip_error(error: Any, limit: int = 280) -> str:
    text = str(error or "").strip()
    if not text:
        return ""
    return text[:limit]


def should_pause_coinglass_requests(error: Any) -> bool:
    text = _clip_error(error).lower()
    if not text:
        return False
    return (
        "minute_budget_exhausted" in text
        or "daily_budget_exhausted" in text
        or "monthly_budget_exhausted" in text
        or "http_429" in text
        or "code_429" in text
        or "rate_limit" in text
    )


def _safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":"))


def _coinglass_watch_symbols(max_items: int = 12) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    defaults = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    for symbol in defaults + str(getattr(settings, "AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS", "") or "").split(","):
        normalized = normalize_coinglass_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
        if len(out) >= max(1, int(max_items or 0)):
            break
    return out


async def _get_budget_row(session) -> CoinglassBudgetLedger:
    row = (
        await session.execute(select(CoinglassBudgetLedger).where(CoinglassBudgetLedger.scope == _BUDGET_SCOPE))
    ).scalars().first()
    if row is None:
        row = CoinglassBudgetLedger(scope=_BUDGET_SCOPE)
        session.add(row)
        await session.flush()
    return row


def _roll_budget_windows(row: CoinglassBudgetLedger, now: Optional[datetime] = None) -> None:
    current = now or _utc_now()
    minute_started_at = _minute_window(current)
    if row.minute_window_started_at is None or row.minute_window_started_at != minute_started_at:
        row.minute_window_started_at = minute_started_at
        row.minute_requests_used = 0
    if str(row.day_key or "") != _day_key(current):
        row.day_key = _day_key(current)
        row.day_requests_used = 0
    if str(row.month_key or "") != _month_key(current):
        row.month_key = _month_key(current)
        row.month_requests_used = 0


def _effective_minute_cap(*, manual: bool) -> int:
    limit = max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 10) or 10))
    if manual:
        return limit
    return max(1, limit - _NON_MANUAL_MINUTE_RESERVE)


async def _reserve_budget(*, manual: bool) -> CoinglassBudgetState:
    async with async_session_maker() as session:
        row = await _get_budget_row(session)
        _roll_budget_windows(row)
        minute_limit = max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 10) or 10))
        daily_limit = max(1, int(getattr(settings, "COINGLASS_DAILY_BUDGET", 50000) or 50000))
        monthly_limit = max(1, int(getattr(settings, "COINGLASS_MONTHLY_BUDGET", 500000) or 500000))
        effective_minute_cap = _effective_minute_cap(manual=manual)
        if row.minute_requests_used >= effective_minute_cap:
            raise CoinglassBudgetExceeded("minute_budget_exhausted")
        if row.day_requests_used >= daily_limit:
            raise CoinglassBudgetExceeded("daily_budget_exhausted")
        if row.month_requests_used >= monthly_limit:
            raise CoinglassBudgetExceeded("monthly_budget_exhausted")
        row.minute_requests_used += 1
        row.day_requests_used += 1
        row.month_requests_used += 1
        await session.commit()
        return CoinglassBudgetState(
            enabled=coinglass_enabled(),
            key_configured=coinglass_key_configured(),
            minute_limit=minute_limit,
            minute_used=row.minute_requests_used,
            minute_remaining=max(0, minute_limit - row.minute_requests_used),
            daily_limit=daily_limit,
            daily_used=row.day_requests_used,
            daily_remaining=max(0, daily_limit - row.day_requests_used),
            monthly_limit=monthly_limit,
            monthly_used=row.month_requests_used,
            monthly_remaining=max(0, monthly_limit - row.month_requests_used),
            request_success_count=int(row.request_success_count or 0),
            rate_limit_hit_count=int(row.rate_limit_hit_count or 0),
            last_success_at=_utc_iso(row.last_success_at),
            last_error=str(row.last_error or ""),
            last_http_status=row.last_http_status,
        )


async def _finalize_budget(*, status_code: Optional[int], error_text: str = "") -> None:
    async with async_session_maker() as session:
        row = await _get_budget_row(session)
        _roll_budget_windows(row)
        row.last_http_status = int(status_code) if status_code is not None else None
        row.last_error = _clip_error(error_text)
        if int(status_code or 0) == 429:
            row.rate_limit_hit_count = int(row.rate_limit_hit_count or 0) + 1
        if int(status_code or 0) == 200:
            row.request_success_count = int(row.request_success_count or 0) + 1
            row.last_success_at = _utc_naive()
        await session.commit()


async def get_coinglass_budget_state() -> CoinglassBudgetState:
    async with async_session_maker() as session:
        row = await _get_budget_row(session)
        _roll_budget_windows(row)
        await session.commit()
        minute_limit = max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 10) or 10))
        daily_limit = max(1, int(getattr(settings, "COINGLASS_DAILY_BUDGET", 50000) or 50000))
        monthly_limit = max(1, int(getattr(settings, "COINGLASS_MONTHLY_BUDGET", 500000) or 500000))
        return CoinglassBudgetState(
            enabled=coinglass_enabled(),
            key_configured=coinglass_key_configured(),
            minute_limit=minute_limit,
            minute_used=int(row.minute_requests_used or 0),
            minute_remaining=max(0, minute_limit - int(row.minute_requests_used or 0)),
            daily_limit=daily_limit,
            daily_used=int(row.day_requests_used or 0),
            daily_remaining=max(0, daily_limit - int(row.day_requests_used or 0)),
            monthly_limit=monthly_limit,
            monthly_used=int(row.month_requests_used or 0),
            monthly_remaining=max(0, monthly_limit - int(row.month_requests_used or 0)),
            request_success_count=int(row.request_success_count or 0),
            rate_limit_hit_count=int(row.rate_limit_hit_count or 0),
            last_success_at=_utc_iso(row.last_success_at),
            last_error=str(row.last_error or ""),
            last_http_status=row.last_http_status,
        )


def _parquet_path(dataset: str) -> Path:
    return _NORMALIZED_ROOT / f"{dataset}.parquet"


def _raw_jsonl_path(dataset: str, now: Optional[datetime] = None) -> Path:
    current = now or _utc_now()
    folder = _RAW_ROOT / str(dataset or "unknown")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{current.strftime('%Y-%m-%d')}.jsonl"


def load_normalized_dataset(dataset: str) -> pd.DataFrame:
    path = _parquet_path(dataset)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        logger.warning(f"coinglass: failed to read {path.name}: {exc}")
        return pd.DataFrame()


def load_dataset_rows_for_symbol(dataset: str, symbol: str) -> pd.DataFrame:
    frame = load_normalized_dataset(dataset)
    if frame.empty:
        return frame
    normalized_symbol = normalize_coinglass_symbol(symbol)
    if "normalized_symbol" in frame.columns:
        frame = frame[frame["normalized_symbol"].astype(str).str.upper() == normalized_symbol]
    if frame.empty:
        return frame
    sort_columns = [column for column in ("ingested_at", "source_ts") if column in frame.columns]
    if sort_columns:
        frame = frame.sort_values(sort_columns)
    return frame.reset_index(drop=True)


def load_coinglass_cached_source_snapshot() -> Dict[str, Any]:
    active_datasets: List[str] = []
    covered_symbols: set[str] = set()
    latest_source_ts: Optional[datetime] = None
    total_rows = 0

    for dataset in COINGLASS_DEFAULT_DATASETS:
        frame = load_normalized_dataset(dataset)
        if frame.empty:
            continue
        active_datasets.append(dataset)
        total_rows += int(len(frame.index))
        if "normalized_symbol" in frame.columns:
            covered_symbols.update(
                str(value or "").strip().upper()
                for value in frame["normalized_symbol"].dropna().tolist()
                if str(value or "").strip()
            )
        if "source_ts" in frame.columns:
            try:
                parsed = pd.to_datetime(frame["source_ts"], utc=True, errors="coerce").dropna()
                if not parsed.empty:
                    candidate = parsed.max().to_pydatetime()
                    if latest_source_ts is None or candidate > latest_source_ts:
                        latest_source_ts = candidate
            except Exception:
                pass

    freshness_sec = None
    if latest_source_ts is not None:
        freshness_sec = max(0.0, (_utc_now() - latest_source_ts.astimezone(timezone.utc)).total_seconds())

    return {
        "enabled": coinglass_enabled(),
        "key_configured": coinglass_key_configured(),
        "has_cached_data": bool(active_datasets),
        "active_datasets": list(active_datasets),
        "covered_symbols": sorted(covered_symbols),
        "dataset_count": len(active_datasets),
        "rows": total_rows,
        "latest_source_ts": _utc_iso(latest_source_ts),
        "freshness_sec": freshness_sec,
    }


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            if isinstance(value, (int, float)):
                numeric = float(value)
                if numeric > 1_000_000_000_000:
                    numeric = numeric / 1000.0
                dt = datetime.fromtimestamp(numeric, tz=timezone.utc)
            else:
                dt = pd.Timestamp(value).to_pydatetime()
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_source_ts(row: Mapping[str, Any], fallback: Optional[datetime] = None) -> datetime:
    for key in ("t", "ts", "time", "timestamp", "create_time", "updated_at", "date"):
        parsed = _parse_timestamp(row.get(key))
        if parsed is not None:
            return parsed
    return fallback or _utc_now()


def _unwrap_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item or {}) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("data", "list", "rows", "items", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(item or {}) for item in value if isinstance(item, Mapping)]
            if isinstance(value, Mapping):
                nested = _unwrap_rows(value)
                if nested:
                    return nested
        return [dict(payload)]
    return []


def _coinglass_business_code(payload: Any) -> Optional[int]:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("code")
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _coinglass_business_message(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return ""
    for key in ("msg", "message", "error"):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    return ""


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return float(parsed)


def _row_value(row: Mapping[str, Any], *candidates: str) -> Any:
    lowered = {"".join(ch for ch in str(key or "").lower() if ch.isalnum()): value for key, value in dict(row or {}).items()}
    for candidate in candidates:
        key = "".join(ch for ch in str(candidate or "").lower() if ch.isalnum())
        if key in lowered:
            return lowered[key]
    return None


def _coalesce_float(row: Mapping[str, Any], *candidates: str) -> Optional[float]:
    for candidate in candidates:
        value = _to_float(_row_value(row, candidate))
        if value is not None:
            return value
    return None


def _normalize_open_interest_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    for interval_key in ("5m", "15m", "1h", "4h", "24h"):
        source = _coalesce_float(
            record,
            f"open_interest_change_percent_{interval_key}",
            f"open_interest_change_{interval_key}",
            f"change{interval_key}",
            f"oiChange{interval_key}",
        )
        if source is not None:
            record[f"open_interest_change_{interval_key}"] = source
    return record


def _normalize_open_interest_history_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    open_value = _coalesce_float(record, "open", "o")
    high_value = _coalesce_float(record, "high", "h")
    low_value = _coalesce_float(record, "low", "l")
    close_value = _coalesce_float(record, "close", "c")
    if open_value is not None:
        record["open_interest_open"] = open_value
    if high_value is not None:
        record["open_interest_high"] = high_value
    if low_value is not None:
        record["open_interest_low"] = low_value
    if close_value is not None:
        record["open_interest_close"] = close_value
        record["open_interest_usd"] = close_value
    return record


def _normalize_funding_rate_history_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    open_value = _coalesce_float(record, "open", "o")
    high_value = _coalesce_float(record, "high", "h")
    low_value = _coalesce_float(record, "low", "l")
    close_value = _coalesce_float(record, "close", "c")
    if open_value is not None:
        record["funding_rate_open"] = open_value
    if high_value is not None:
        record["funding_rate_high"] = high_value
    if low_value is not None:
        record["funding_rate_low"] = low_value
    if close_value is not None:
        record["funding_rate_close"] = close_value
        record["funding_rate"] = close_value
    return record


def _normalize_taker_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    buy_volume = _coalesce_float(
        record,
        "taker_buy_volume",
        "buy_volume",
        "buyVolume",
        "buy",
        "buy_usd",
        "buyUsd",
        "buyVolUsd",
    )
    sell_volume = _coalesce_float(
        record,
        "taker_sell_volume",
        "sell_volume",
        "sellVolume",
        "sell",
        "sell_usd",
        "sellUsd",
        "sellVolUsd",
    )
    if buy_volume is not None:
        record["taker_buy_volume"] = buy_volume
    if sell_volume is not None:
        record["taker_sell_volume"] = sell_volume
    if buy_volume is not None and sell_volume is not None:
        record["volume_usd"] = float(buy_volume + sell_volume)
        total = buy_volume + sell_volume
        if total > 0:
            record["taker_buy_sell_imbalance"] = float((buy_volume - sell_volume) / total)
    return record


def _normalize_liquidation_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    long_liquidation = _coalesce_float(
        record,
        "long_liquidation_usd",
        "longLiquidationUsd",
        "longVolUsd",
        "longUsd",
        "long",
    )
    short_liquidation = _coalesce_float(
        record,
        "short_liquidation_usd",
        "shortLiquidationUsd",
        "shortVolUsd",
        "shortUsd",
        "short",
    )
    if long_liquidation is not None:
        record["long_liquidation_usd"] = long_liquidation
    if short_liquidation is not None:
        record["short_liquidation_usd"] = short_liquidation
    total = (long_liquidation or 0.0) + (short_liquidation or 0.0)
    if total > 0:
        record["liquidation_total_usd"] = total
        record["burst_score"] = max(0.0, min(total / 50_000_000.0, 1.0))
    return record


def _normalize_ratio_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    long_short_ratio = _coalesce_float(
        record,
        "long_short_ratio",
        "longShortRatio",
        "global_account_long_short_ratio",
        "globalAccountLongShortRatio",
        "ratio",
    )
    if long_short_ratio is None:
        long_account = _coalesce_float(record, "long_account", "longAccount", "longRate", "long_ratio")
        short_account = _coalesce_float(record, "short_account", "shortAccount", "shortRate", "short_ratio")
        if long_account is not None and short_account not in (None, 0):
            long_short_ratio = float(long_account / short_account)
    if long_short_ratio is not None:
        record["long_short_ratio"] = long_short_ratio
    return record


def _normalize_funding_rate_rows(rows: List[Dict[str, Any]], *, requested_symbol: str) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        row_symbol = row.get("symbol")
        if not coinglass_symbol_matches(requested_symbol, row_symbol):
            continue
        for margin_key, margin_type in (
            ("stablecoin_margin_list", "stablecoin"),
            ("token_margin_list", "token"),
        ):
            margin_rows = row.get(margin_key)
            if not isinstance(margin_rows, list):
                continue
            for item in margin_rows:
                if not isinstance(item, Mapping):
                    continue
                normalized = dict(item or {})
                normalized["symbol"] = row_symbol
                normalized["margin_type"] = margin_type
                normalized_rows.append(normalized)
    return normalized_rows


def _normalize_funding_arbitrage_rows(rows: List[Dict[str, Any]], *, requested_symbol: str) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not coinglass_symbol_matches(requested_symbol, row.get("symbol")):
            continue
        normalized = dict(row or {})
        buy = dict(normalized.get("buy") or {})
        sell = dict(normalized.get("sell") or {})
        normalized["buy_exchange"] = buy.get("exchange")
        normalized["sell_exchange"] = sell.get("exchange")
        normalized["buy_open_interest_usd"] = _to_float(buy.get("open_interest_usd"))
        normalized["sell_open_interest_usd"] = _to_float(sell.get("open_interest_usd"))
        normalized["buy_funding_rate"] = _to_float(buy.get("funding_rate"))
        normalized["sell_funding_rate"] = _to_float(sell.get("funding_rate"))
        funding_spread = None
        if normalized["buy_funding_rate"] is not None and normalized["sell_funding_rate"] is not None:
            funding_spread = float(normalized["sell_funding_rate"] - normalized["buy_funding_rate"])
        if funding_spread is not None:
            normalized["funding_rate_spread"] = funding_spread
        normalized_rows.append(normalized)
    return normalized_rows


def normalize_dataset_response(
    *,
    dataset: str,
    request_meta: Mapping[str, Any],
    response_payload: Any,
) -> Dict[str, Any]:
    business_code = _coinglass_business_code(response_payload)
    business_message = _coinglass_business_message(response_payload)
    raw_rows = _unwrap_rows(response_payload)
    requested_symbol = normalize_coinglass_symbol(request_meta.get("symbol"))

    if business_code not in (None, 0):
        status = "degraded" if int(business_code or 0) == 429 else "failed"
        return {
            "status": status,
            "error": f"coinglass_code_{business_code}:{business_message or 'request_failed'}",
            "rows": [],
            "details": {
                "response_code": business_code,
                "response_msg": business_message,
                "raw_rows": len(raw_rows),
                "matched_rows": 0,
            },
        }

    if dataset == "funding_rate_exchange_list":
        rows = _normalize_funding_rate_rows(raw_rows, requested_symbol=requested_symbol)
    elif dataset == "funding_arbitrage":
        rows = _normalize_funding_arbitrage_rows(raw_rows, requested_symbol=requested_symbol)
    else:
        rows = []
        for row in raw_rows:
            if dataset in {"open_interest_exchange_list", "taker_buy_sell_volume_exchange_list"}:
                row_symbol = row.get("symbol")
                if row_symbol and not coinglass_symbol_matches(requested_symbol, row_symbol):
                    continue
            if dataset == "open_interest_exchange_list":
                rows.append(_normalize_open_interest_row(row))
            elif dataset == "open_interest_history":
                rows.append(_normalize_open_interest_history_row(row))
            elif dataset == "funding_rate_history":
                rows.append(_normalize_funding_rate_history_row(row))
            elif dataset in {"taker_buy_sell_volume_exchange_list", "taker_buy_sell_volume_history"}:
                rows.append(_normalize_taker_row(row))
            elif dataset == "liquidation_history":
                rows.append(_normalize_liquidation_row(row))
            elif dataset == "global_long_short_account_ratio_history":
                rows.append(_normalize_ratio_row(row))
            else:
                rows.append(dict(row or {}))

    if rows:
        return {
            "status": "ok",
            "error": "",
            "rows": rows,
            "details": {
                "response_code": business_code if business_code is not None else 0,
                "response_msg": business_message,
                "raw_rows": len(raw_rows),
                "matched_rows": len(rows),
            },
        }

    empty_reason = business_message or "no_rows_after_dataset_filter"
    return {
        "status": "empty",
        "error": empty_reason,
        "rows": [],
        "details": {
            "response_code": business_code if business_code is not None else 0,
            "response_msg": business_message,
            "raw_rows": len(raw_rows),
            "matched_rows": 0,
        },
    }


def persist_raw_snapshot(
    *,
    dataset: str,
    route: CoinglassRouteSpec,
    request_meta: Mapping[str, Any],
    response_payload: Any,
) -> Path:
    _ensure_cache_dirs()
    path = _raw_jsonl_path(dataset)
    record = {
        "recorded_at": _utc_now().isoformat(),
        "dataset": dataset,
        "api_version": route.api_version,
        "path": route.path,
        "request": dict(request_meta or {}),
        "payload": response_payload,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(_safe_json_dumps(record) + "\n")
    return path


def persist_symbol_registry(symbols: Iterable[str]) -> Path:
    registry = {
        "updated_at": _utc_now().isoformat(),
        "symbols": [
            {
                "requested_symbol": str(symbol or ""),
                "normalized_symbol": normalize_coinglass_symbol(symbol),
            }
            for symbol in symbols
            if normalize_coinglass_symbol(symbol)
        ],
    }
    _ensure_cache_dirs()
    _SYMBOL_REGISTRY_PATH.write_text(_safe_json_dumps(registry), encoding="utf-8")
    return _SYMBOL_REGISTRY_PATH


def persist_normalized_rows(
    *,
    dataset: str,
    manifest: CoinglassDatasetManifest,
    route: CoinglassRouteSpec,
    request_meta: Mapping[str, Any],
    response_payload: Any,
) -> pd.DataFrame:
    rows = _unwrap_rows(response_payload)
    if not rows:
        return pd.DataFrame()
    now = _utc_now()
    requested_symbol = normalize_coinglass_symbol(request_meta.get("symbol"))
    exchange = str(request_meta.get("exchange") or "aggregate").strip() or "aggregate"
    interval = str(request_meta.get("interval") or "").strip()
    records: List[Dict[str, Any]] = []
    for row in rows:
        source_ts = _extract_source_ts(row, fallback=now)
        row_symbol = normalize_coinglass_symbol(row.get("symbol") or requested_symbol)
        row_exchange = str(row.get("exchange") or row.get("exchange_name") or exchange or "aggregate").strip() or "aggregate"
        row_variant = str(row.get("margin_type") or "").strip().lower()
        canonical_exchange = row_exchange if not row_variant else f"{row_exchange}:{row_variant}"
        canonical_key = canonical_request_key(
            dataset=dataset,
            api_version=route.api_version,
            market_type=manifest.market_type,
            normalized_symbol=row_symbol,
            exchange=canonical_exchange,
            interval=interval,
            source_ts=source_ts.isoformat(),
        )
        records.append(
            {
                "dataset": dataset,
                "api_version": route.api_version,
                "market_type": manifest.market_type,
                "normalized_symbol": row_symbol,
                "exchange": row_exchange,
                "row_variant": row_variant,
                "interval": interval,
                "request_key": str(request_meta.get("request_key") or canonical_key),
                "canonical_key": canonical_key,
                "source_ts": source_ts.isoformat(),
                "ingested_at": now.isoformat(),
                "latency_ms": int(request_meta.get("latency_ms") or 0),
                "path": route.path,
                "payload_json": _safe_json_dumps(row),
            }
        )
    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        return frame
    _ensure_cache_dirs()
    path = _parquet_path(dataset)
    if path.exists():
        try:
            existing = pd.read_parquet(path)
        except Exception:
            existing = pd.DataFrame()
        if not existing.empty:
            frame = pd.concat([existing, frame], ignore_index=True)
    frame = frame.drop_duplicates(subset=["canonical_key"], keep="last").sort_values(
        ["normalized_symbol", "source_ts", "exchange", "row_variant"], ignore_index=True
    )
    frame.to_parquet(path, index=False)
    return frame


async def record_coinglass_ingest_status(
    *,
    dataset: str,
    symbol: str = "",
    exchange: str = "aggregate",
    interval: str = "",
    status: str,
    rows_written: int = 0,
    latency_ms: int = 0,
    error: str = "",
    details: Optional[Dict[str, Any]] = None,
    manifest: Optional[CoinglassDatasetManifest] = None,
) -> Dict[str, Any]:
    normalized_symbol = normalize_coinglass_symbol(symbol)
    scope_key = "|".join(
        [
            str(dataset or "").strip(),
            normalized_symbol,
            str(exchange or "aggregate").strip(),
            str(interval or "").strip(),
        ]
    )
    now = _utc_now()
    fresh_until = None
    if str(status or "").strip().lower() == "ok":
        fresh_window = int((manifest.freshness_sec if manifest else 0) or 0)
        if fresh_window > 0:
            fresh_until = now + pd.Timedelta(seconds=fresh_window).to_pytimedelta()
    async with async_session_maker() as session:
        row = (
            await session.execute(select(CoinglassIngestStatus).where(CoinglassIngestStatus.scope_key == scope_key))
        ).scalars().first()
        if row is None:
            row = CoinglassIngestStatus(
                dataset=dataset,
                scope_key=scope_key,
                symbol=normalized_symbol,
                exchange=exchange,
                interval=interval,
            )
            session.add(row)
        row.dataset = dataset
        row.symbol = normalized_symbol
        row.exchange = exchange
        row.interval = interval
        row.status = str(status or "idle")
        row.rows_written = int(max(0, rows_written))
        row.latency_ms = int(max(0, latency_ms))
        row.error = _clip_error(error)
        row.last_attempt_at = _utc_naive(now)
        if str(row.status) == "ok":
            row.last_success_at = _utc_naive(now)
        if fresh_until is not None:
            row.fresh_until_at = _utc_naive(fresh_until)
        row.details = dict(details or {})
        await session.commit()
        return CoinglassIngestStatusPayload(
            dataset=str(row.dataset or ""),
            scope_key=str(row.scope_key or ""),
            status=str(row.status or "idle"),
            symbol=str(row.symbol or ""),
            exchange=str(row.exchange or ""),
            interval=str(row.interval or ""),
            rows_written=int(row.rows_written or 0),
            last_success_at=_utc_iso(row.last_success_at),
            last_attempt_at=_utc_iso(row.last_attempt_at),
            updated_at=_utc_iso(row.updated_at),
            fresh_until_at=_utc_iso(row.fresh_until_at),
            error=str(row.error or ""),
            details=dict(row.details or {}),
        ).to_dict()


async def load_coinglass_ingest_statuses(*, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    normalized_symbol = normalize_coinglass_symbol(symbol)
    async with async_session_maker() as session:
        stmt = select(CoinglassIngestStatus).order_by(CoinglassIngestStatus.updated_at.desc())
        if normalized_symbol:
            stmt = stmt.where(CoinglassIngestStatus.symbol == normalized_symbol)
        rows = (await session.execute(stmt)).scalars().all()
    payloads = [
        CoinglassIngestStatusPayload(
            dataset=str(row.dataset or ""),
            scope_key=str(row.scope_key or ""),
            status=str(row.status or "idle"),
            symbol=str(row.symbol or ""),
            exchange=str(row.exchange or ""),
            interval=str(row.interval or ""),
            rows_written=int(row.rows_written or 0),
            last_success_at=_utc_iso(row.last_success_at),
            last_attempt_at=_utc_iso(row.last_attempt_at),
            updated_at=_utc_iso(row.updated_at),
            fresh_until_at=_utc_iso(row.fresh_until_at),
            error=str(row.error or ""),
            details=dict(row.details or {}),
        ).to_dict()
        for row in rows
    ]
    if not normalized_symbol:
        return payloads
    return [row for row in payloads if str(row.get("symbol") or "") == normalized_symbol]


def _manifest_params(
    manifest: CoinglassDatasetManifest,
    route: CoinglassRouteSpec,
    *,
    symbol: Optional[str],
    exchange: Optional[str],
    interval: Optional[str],
    limit: Optional[int],
    start_time: Optional[Any] = None,
    end_time: Optional[Any] = None,
) -> Dict[str, Any]:
    params = dict(route.default_params or {})
    normalized_exchange = normalize_coinglass_exchange(exchange or params.get("exchange") or "Binance")
    normalized_interval = normalize_coinglass_interval(interval or params.get("interval") or "h4")
    if "symbol" in route.required_params:
        if manifest.dataset in {
            "liquidation_history",
            "global_long_short_account_ratio_history",
            "open_interest_history",
            "funding_rate_history",
            "taker_buy_sell_volume_history",
            "price_history",
        }:
            params["symbol"] = coinglass_pair_symbol(symbol, normalized_exchange)
        else:
            params["symbol"] = normalize_coinglass_symbol(symbol)
    if "exchange" in route.required_params:
        params["exchange"] = normalized_exchange
    if "interval" in route.required_params:
        if manifest.dataset == "price_history":
            params["interval"] = str(interval or params.get("interval") or "1h").strip().lower() or "1h"
        else:
            params["interval"] = normalized_interval
    if "range" in route.required_params:
        params["range"] = coinglass_range_for_interval(interval or params.get("interval") or "h4")
    if limit is not None:
        params["limit"] = int(limit)
    if start_time is not None:
        params["start_time"] = start_time
    if end_time is not None:
        params["end_time"] = end_time
    return params


class CoinglassClient:
    def __init__(self, timeout_sec: int = _REQUEST_TIMEOUT_SEC):
        self._timeout = aiohttp.ClientTimeout(total=int(max(5, timeout_sec)))
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "CoinglassClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def _request_json(self, path: str, *, params: Mapping[str, Any], manual: bool) -> Dict[str, Any]:
        if not coinglass_enabled():
            raise CoinglassError("coinglass_disabled_or_key_missing")
        headers = {"X-Api-Key": coinglass_api_key()}
        base_url = _coinglass_root_url() if path.startswith("/api/v1/modules/") else _coinglass_base_url()
        url = f"{base_url}{path}"
        async with _REQUEST_LOCK:
            await _reserve_budget(manual=manual)
        status_code = None
        error_text = ""
        started = _utc_now()
        session = await self._get_session()
        try:
            async with session.get(url, params=dict(params or {}), headers=headers) as response:
                status_code = int(response.status)
                payload = await response.json(content_type=None)
                if status_code >= 400:
                    error_text = _clip_error(payload.get("msg") if isinstance(payload, Mapping) else payload)
                    raise CoinglassError(f"http_{status_code}:{error_text or 'request_failed'}")
                return {
                    "status_code": status_code,
                    "latency_ms": int((_utc_now() - started).total_seconds() * 1000),
                    "payload": payload,
                }
        except Exception as exc:
            if not error_text:
                error_text = _clip_error(exc)
            raise
        finally:
            await _finalize_budget(status_code=status_code, error_text=error_text)

    async def request_json(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        manual: bool = False,
    ) -> Dict[str, Any]:
        return await self._request_json(path, params=dict(params or {}), manual=manual)

    async def request_dataset(
        self,
        manifest: CoinglassDatasetManifest,
        *,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        manual: bool = False,
    ) -> Dict[str, Any]:
        last_error = ""
        for route in manifest.routes:
            params = _manifest_params(
                manifest,
                route,
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                limit=limit,
                start_time=start_time,
                end_time=end_time,
            )
            try:
                response = await self._request_json(route.path, params=params, manual=manual)
                request_key = canonical_request_key(
                    dataset=manifest.dataset,
                    api_version=route.api_version,
                    market_type=manifest.market_type,
                    normalized_symbol=normalize_coinglass_symbol(symbol),
                    exchange=str(params.get("exchange") or "aggregate"),
                    interval=str(params.get("interval") or ""),
                    source_ts=_utc_now().isoformat(),
                )
                return {
                    "dataset": manifest.dataset,
                    "manifest": manifest,
                    "route": route,
                    "params": params,
                    "request_key": request_key,
                    "status_code": int(response.get("status_code") or 0),
                    "latency_ms": int(response.get("latency_ms") or 0),
                    "payload": response.get("payload"),
                }
            except Exception as exc:
                last_error = _clip_error(exc)
                if not route.fallback:
                    continue
        raise CoinglassError(last_error or f"{manifest.dataset}:no_route_available")

    async def fetch_api_spec(self, *, manual: bool = True) -> Dict[str, Any]:
        return await self._request_json("/api/v1/modules/coinglass/api-spec", params={}, manual=manual)


async def discover_and_persist_coinglass_capabilities(
    *,
    datasets: Optional[Iterable[str]] = None,
    manual: bool = True,
) -> Dict[str, Any]:
    requested = [str(item or "").strip() for item in (datasets or COINGLASS_DEFAULT_DATASETS) if str(item or "").strip()]
    capabilities: List[Dict[str, Any]] = []
    api_spec_payload: Dict[str, Any] = {}
    stop_reason = ""
    async with CoinglassClient() as client:
        try:
            api_spec_payload = await client.fetch_api_spec(manual=manual)
            _ensure_cache_dirs()
            _API_SPEC_PATH.write_text(
                _safe_json_dumps(api_spec_payload.get("payload") or {}),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning(f"coinglass: api-spec fetch failed: {exc}")
        for dataset in requested:
            manifest = get_coinglass_manifest(dataset)
            if manifest is None:
                continue
            for route in manifest.routes:
                params = _manifest_params(manifest, route, symbol="BTC", exchange="Binance", interval="h4", limit=2)
                try:
                    result = await client._request_json(route.path, params=params, manual=manual)
                    capabilities.append(
                        {
                            "dataset": dataset,
                            "api_version": route.api_version,
                            "path": route.path,
                            "status_code": int(result.get("status_code") or 0),
                            "available": True,
                        }
                    )
                    break
                except Exception as exc:
                    error_text = _clip_error(exc)
                    capabilities.append(
                        {
                            "dataset": dataset,
                            "api_version": route.api_version,
                            "path": route.path,
                            "status_code": None,
                            "available": False,
                            "error": error_text,
                        }
                    )
                    if should_pause_coinglass_requests(error_text):
                        stop_reason = error_text
                        break
            if stop_reason:
                break
    _ensure_cache_dirs()
    capability_payload = {"generated_at": _utc_now().isoformat(), "items": capabilities}
    _CAPABILITY_MATRIX_PATH.write_text(_safe_json_dumps(capability_payload), encoding="utf-8")
    return {
        "generated_at": capability_payload["generated_at"],
        "api_spec_path": str(_API_SPEC_PATH),
        "capability_matrix_path": str(_CAPABILITY_MATRIX_PATH),
        "items": capabilities,
        "available_count": sum(1 for item in capabilities if item.get("available")),
        "stopped_early": bool(stop_reason),
        "stop_reason": stop_reason or None,
    }
