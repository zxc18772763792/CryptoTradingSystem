from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from loguru import logger

from config.settings import settings
from core.ai.autonomous_learning import (
    build_blocked_symbol_side_map,
    build_learning_memory,
    coerce_learning_memory,
    default_learning_memory,
    normalize_symbol,
)
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.ai.signal_aggregator import signal_aggregator
from core.backtest.cost_models import dynamic_slippage_rate, microstructure_proxies
from core.data import data_storage
from core.exchanges import exchange_manager
from core.news.storage import db as news_db
from core.runtime import runtime_state
from core.strategies import Signal, SignalType
from core.strategies.strategy_manager import strategy_manager
from core.trading import execution_engine, position_manager
from core.utils.openai_responses import (
    build_openai_headers,
    build_responses_payload,
    extract_response_text,
    is_retryable_openai_status,
    openai_endpoint_targets,
    read_aiohttp_responses_json,
    responses_endpoint,
)


_DEFAULT_OPENAI_BASE_URL = "https://vpsairobot.com/v1"
_DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com"
_DEFAULT_GLM_BASE_URL = "https://open.bigmodel.cn/api/coding/paas/v4"

_SUPPORTED_PROVIDERS = {"glm", "codex", "claude"}
_SUPPORTED_MODES = {"shadow", "execute"}
_SUPPORTED_ACTIONS = {"buy", "sell", "hold", "close_long", "close_short"}
_SUPPORTED_SYMBOL_MODES = {"manual", "auto"}
_FIXED_AUTONOMOUS_AGENT_LEVERAGE = 1.0
_SAME_DIRECTION_MAX_EXPOSURE_RATIO = 0.5
_MODEL_FEEDBACK_OUTAGE_ALERT_SEC = 30 * 60
_MODEL_FEEDBACK_HARD_TIMEOUT_SEC = 30 * 60
_DEFAULT_AUTO_UNIVERSE = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "LINK/USDT",
    "AVAX/USDT",
    "DOT/USDT",
]
_AI_EXECUTION_DYNAMIC_SLIP_PARAMS = {
    "min_slip": 0.00005,
    "k_atr": 0.15,
    "k_rv": 0.80,
    "k_spread": 0.50,
}
_AI_EXECUTION_LIQUIDITY_TARGET_PARTICIPATION = 0.001
_AI_PROFIT_PROTECT_TRIGGER_PCT_MIN = 0.0035
_AI_PROFIT_PROTECT_LOCK_BUFFER_PCT = 0.0004
_AI_PARTIAL_TAKE_PROFIT_TRIGGER_PCT_MIN = 0.0060
_AI_PARTIAL_TAKE_PROFIT_FRACTION = 0.5
_AI_POST_PARTIAL_TRAILING_STOP_PCT_MIN = 0.0025
_AI_OUTAGE_TIGHT_TRAILING_STOP_PCT_MIN = 0.0015


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_provider(value: Any) -> str:
    text = str(value or "codex").strip().lower()
    aliases = {"openai": "codex"}
    text = aliases.get(text, text)
    if text not in _SUPPORTED_PROVIDERS:
        raise ValueError("provider must be one of: glm/codex(openai)/claude")
    return text


def _normalize_mode(value: Any) -> str:
    text = str(value or "shadow").strip().lower()
    if text not in _SUPPORTED_MODES:
        raise ValueError("mode must be one of: shadow/execute")
    return text


def _normalize_action(value: Any) -> str:
    text = str(value or "hold").strip().lower()
    aliases = {
        "long": "buy",
        "short": "sell",
        "flat": "hold",
        "close": "hold",
        "exit": "hold",
    }
    text = aliases.get(text, text)
    if text not in _SUPPORTED_ACTIONS:
        return "hold"
    return text


def _merge_symbol_sequence(*groups: List[Any], max_items: int = 30) -> List[str]:
    items: List[str] = []
    seen: set[str] = set()
    limit = max(1, int(max_items or 30))
    for group in groups:
        for value in group or []:
            symbol = _normalize_symbol_text(value)
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            items.append(symbol)
            if len(items) >= limit:
                return items
    return items


def _coerce_float(value: Any, default: float, *, low: float, high: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(low, min(high, parsed))


def _coerce_int(value: Any, default: int, *, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(low, min(high, parsed))


def _safe_nonnegative_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if parsed < 0:
        return float(default)
    return parsed


def _normalize_symbol_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "/" not in text and text.endswith("USDT") and len(text) > 4:
        return f"{text[:-4]}/USDT"
    return text


def _canonical_symbol_key(value: Any) -> str:
    text = _normalize_symbol_text(value)
    if ":" in text:
        text = text.split(":", 1)[0].strip()
    if "_" in text and "/" not in text:
        left, right = text.split("_", 1)
        text = f"{left}/{right}"
    return text


def _normalize_symbol_mode(value: Any) -> str:
    text = str(value or "manual").strip().lower()
    if text not in _SUPPORTED_SYMBOL_MODES:
        return "manual"
    return text


def _normalize_symbol_list(value: Any, *, default: Optional[List[str]] = None, max_items: int = 30) -> List[str]:
    raw_items: List[Any]
    if value is None:
        raw_items = list(default or [])
    elif isinstance(value, str):
        raw_items = [item for item in re.split(r"[\s,;\n\r\t]+", value) if item.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = list(default or [])

    items: List[str] = []
    seen: set[str] = set()
    for item in raw_items:
        symbol = _normalize_symbol_text(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        items.append(symbol)
        if len(items) >= max(1, int(max_items or 30)):
            break
    return items or list(default or ["BTC/USDT"])


def _extract_json_obj(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty response")
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
        raw = re.sub(r"```$", "", raw).strip()
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    left = raw.find("{")
    right = raw.rfind("}")
    if left >= 0 and right > left:
        data = json.loads(raw[left : right + 1])
        if isinstance(data, dict):
            return data
    raise ValueError("invalid json object")


def _format_exception_short(exc: Exception) -> str:
    text = str(exc or "").strip()
    if text:
        return text
    return exc.__class__.__name__


def _utc_iso_from_unix(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    with contextlib.suppress(Exception):
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    return None


def _classify_model_feedback_error(exc: BaseException) -> Optional[str]:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "timeout"
    text = str(exc or "").strip().lower()
    if not text:
        return None
    status = _extract_model_feedback_http_status(text)
    if "timeout" in text:
        return "timeout"
    if status == 429 or "_http_429" in text or "usage_limit_exceeded" in text or "rate limit" in text or "too many requests" in text:
        return "rate_limit"
    if status in {502, 503, 504} or "service temporarily unavailable" in text or "service unavailable" in text:
        return "service_unavailable"
    return None


def _extract_model_feedback_http_status(exc: Any) -> Optional[int]:
    text = str(exc or "").strip().lower()
    if not text:
        return None
    match = re.search(r"_http_(\d{3})", text)
    if not match:
        return None
    with contextlib.suppress(Exception):
        return int(match.group(1))
    return None


def _describe_model_feedback_issue(raw_error: Any) -> Dict[str, Any]:
    normalized = str(raw_error or "").strip()
    if normalized.startswith("model_error:"):
        normalized = normalized.split("model_error:", 1)[1].strip()

    kind = _classify_model_feedback_error(RuntimeError(normalized or ""))
    http_status = _extract_model_feedback_http_status(normalized)
    if kind == "rate_limit":
        label = "模型限流或额度受限 (429)"
        detail = "上游模型接口触发了频率或额度限制，本轮已回退为 hold。"
        code = "model_rate_limit"
    elif kind == "service_unavailable":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型服务暂时不可用{status_suffix}"
        detail = "上游模型服务或代理网关暂时不可用，本轮已回退为 hold，稍后会自动重试。"
        code = "model_service_unavailable"
    elif kind == "timeout":
        label = "模型响应超时"
        detail = "等待模型返回超过超时阈值，本轮已回退为 hold。"
        code = "model_timeout"
    else:
        label = "模型接口异常"
        detail = "模型接口返回了未分类异常，本轮已回退为 hold。"
        code = "model_error"

    if normalized:
        detail = f"{detail} 原始错误: {normalized[:220]}"

    return {
        "kind": kind,
        "http_status": http_status,
        "label": label,
        "detail": detail,
        "code": code,
        "raw_error": normalized[:300],
    }


def _build_model_output_debug(
    raw_decision: Optional[Dict[str, Any]],
    normalized_decision: Optional[Dict[str, Any]],
    *,
    source: str,
) -> Dict[str, Any]:
    raw_payload = dict(raw_decision or {}) if isinstance(raw_decision, dict) else {}
    normalized_payload = dict(normalized_decision or {}) if isinstance(normalized_decision, dict) else {}
    raw_action = str(raw_payload.get("action") or "").strip().lower()
    normalized_action = str(normalized_payload.get("action") or "").strip().lower()
    raw_reason = str(raw_payload.get("reason") or "").strip()
    normalized_reason = str(normalized_payload.get("reason") or "").strip()
    raw_confidence = _safe_nonnegative_float(raw_payload.get("confidence"), 0.0)
    normalized_confidence = _safe_nonnegative_float(normalized_payload.get("confidence"), 0.0)
    action_changed = bool(raw_action != normalized_action)
    reason_changed = bool(raw_reason != normalized_reason)
    confidence_changed = abs(raw_confidence - normalized_confidence) > 1e-9
    changed = bool(action_changed or reason_changed or confidence_changed)
    return {
        "source": str(source or "synthetic"),
        "raw_action": raw_action,
        "normalized_action": normalized_action,
        "raw_reason": raw_reason,
        "normalized_reason": normalized_reason,
        "raw_confidence": float(raw_confidence),
        "normalized_confidence": float(normalized_confidence),
        "action_changed": action_changed,
        "reason_changed": reason_changed,
        "confidence_changed": confidence_changed,
        "changed": changed,
    }


def _timeframe_to_seconds(timeframe: str) -> int:
    text = str(timeframe or "15m").strip().lower()
    m = re.fullmatch(r"(\d+)([smhdw])", text)
    if not m:
        if text == "1m":
            return 60
        if text == "1h":
            return 3600
        if text == "1d":
            return 86400
        return 900
    value = int(m.group(1))
    unit = m.group(2)
    mul = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 86400 * 7}[unit]
    return max(1, value * mul)


def _default_profile() -> Dict[str, Any]:
    return {
        "updated_at": None,
        "decision_count": 0,
        "executed_count": 0,
        "action_counts": {},
        "avg_confidence": 0.0,
        "avg_strength": 0.0,
        "avg_leverage": 1.0,
        "avg_stop_loss_pct": 0.02,
        "avg_take_profit_pct": 0.04,
    }


_AGENT_PERSISTABLE_KEYS = frozenset({
    "AI_AUTONOMOUS_AGENT_ENABLED",
    "AI_AUTONOMOUS_AGENT_AUTO_START",
    "AI_AUTONOMOUS_AGENT_PROVIDER",
    "AI_AUTONOMOUS_AGENT_MODEL",
    "AI_AUTONOMOUS_AGENT_INTERVAL_SEC",
    "AI_AUTONOMOUS_AGENT_COOLDOWN_SEC",
    "AI_AUTONOMOUS_AGENT_MODE",
    "AI_AUTONOMOUS_AGENT_ALLOW_LIVE",
    "AI_AUTONOMOUS_AGENT_ACCOUNT_ID",
    "AI_AUTONOMOUS_AGENT_STRATEGY_NAME",
    "AI_AUTONOMOUS_AGENT_EXCHANGE",
    "AI_AUTONOMOUS_AGENT_SYMBOL",
    "AI_AUTONOMOUS_AGENT_TIMEFRAME",
    "AI_AUTONOMOUS_AGENT_LOOKBACK_BARS",
    "AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE",
    "AI_AUTONOMOUS_AGENT_SYMBOL_MODE",
    "AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS",
    "AI_AUTONOMOUS_AGENT_SELECTION_TOP_N",
})


class AutonomousTradingAgent:
    """Independent AI trading agent that generates and executes signals."""

    def __init__(self, cache_root: Optional[Path] = None) -> None:
        self._override: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

        root = cache_root or (Path(settings.CACHE_PATH) / "ai")
        self._cache_root = root
        self._journal_path = self._cache_root / "autonomous_agent_journal.jsonl"
        self._profile_path = self._cache_root / "autonomous_agent_profile.json"
        self._learning_memory_path = self._cache_root / "autonomous_agent_learning_memory.json"
        self._overlay_path = Path(
            os.environ.get("AI_AGENT_CONFIG_PATH", str(self._cache_root / "agent_runtime_config.json"))
        )

        self._load_overlay()
        self._profile = self._load_profile()
        self._learning_memory = self._load_learning_memory()
        self._last_error: Optional[str] = None
        self._last_run_at: Optional[str] = None
        self._last_decision: Optional[Dict[str, Any]] = None
        self._last_execution: Optional[Dict[str, Any]] = None
        self._last_research_context: Optional[Dict[str, Any]] = None
        self._last_diagnostics: Optional[Dict[str, Any]] = None
        self._last_symbol_scan: Optional[Dict[str, Any]] = None
        self._tick_count: int = 0
        self._submitted_count: int = 0
        self._last_submit_at: Optional[float] = None
        self._last_model_feedback_at: Optional[float] = None
        self._model_feedback_outage_started_at: Optional[float] = None
        self._model_feedback_failure_streak: int = 0
        self._model_feedback_last_failure_kind: Optional[str] = None
        self._model_feedback_last_failure_error: Optional[str] = None
        self._model_feedback_alert_sent_at: Optional[float] = None
        self._last_learning_refresh_at: Optional[float] = None

    def _provider_base_url(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL).rstrip("/")
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_BASE_URL", "") or _DEFAULT_ANTHROPIC_BASE_URL).rstrip("/")
        return str(getattr(settings, "ZHIPU_BASE_URL", "") or _DEFAULT_GLM_BASE_URL).rstrip("/")

    def _provider_model(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return str(getattr(settings, "OPENAI_MODEL", "") or "gpt-5.4")
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_MODEL", "") or "claude-3-5-sonnet-latest")
        return str(getattr(settings, "ZHIPU_MODEL", "") or "GLM-4.5-Air")

    def _provider_api_key(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            primary = str(getattr(settings, "OPENAI_API_KEY", "") or "").strip()
            if primary:
                return primary
            return str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip()
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_API_KEY", "") or "").strip()
        return str(getattr(settings, "ZHIPU_API_KEY", "") or "").strip()

    def _provider_endpoint_targets(self, provider: str) -> List[Dict[str, Any]]:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return openai_endpoint_targets(
                primary_base_url=str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL),
                backup_base_urls=getattr(settings, "OPENAI_BACKUP_BASE_URL", "") or "",
                primary_api_key=str(getattr(settings, "OPENAI_API_KEY", "") or "").strip(),
                backup_api_key=str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip(),
            )
        return [
            {
                "index": 0,
                "base_url": self._provider_base_url(provider),
                "api_key": self._provider_api_key(provider),
                "is_backup": False,
            }
        ]

    def _provider_catalog(self) -> Dict[str, Dict[str, Any]]:
        providers: Dict[str, Dict[str, Any]] = {}
        for item in sorted(_SUPPORTED_PROVIDERS):
            targets = self._provider_endpoint_targets(item)
            base_urls = [str(target.get("base_url") or "").rstrip("/") for target in targets if str(target.get("base_url") or "").strip()]
            providers[item] = {
                "available": any(bool(str(target.get("api_key") or "").strip()) for target in targets),
                "default_model": self._provider_model(item),
                "base_url": (base_urls[0] if base_urls else self._provider_base_url(item)),
            }
            if item == "codex" and len(base_urls) > 1:
                providers[item]["backup_base_urls"] = base_urls[1:]
                providers[item]["failover_enabled"] = True
        return providers

    def _resolve_provider(self, provider: str, providers: Dict[str, Dict[str, Any]]) -> tuple[str, bool]:
        provider = _normalize_provider(provider)
        if providers.get(provider, {}).get("available"):
            return provider, False
        if providers.get("codex", {}).get("available"):
            return "codex", True
        for item, meta in providers.items():
            if meta.get("available"):
                return str(item), True
        return provider, False

    def _get(self, name: str, fallback: Any = None) -> Any:
        if name in self._override:
            return self._override[name]
        return getattr(settings, name, fallback)

    @staticmethod
    def _extract_live_position_snapshot(row: Any) -> Optional[Dict[str, Any]]:
        row_symbol = str((row.get("symbol") if isinstance(row, dict) else getattr(row, "symbol", "")) or "")
        symbol = _normalize_symbol_text(_canonical_symbol_key(row_symbol))
        if not symbol:
            return None

        amount = float((row.get("amount") if isinstance(row, dict) else getattr(row, "amount", 0.0)) or 0.0)
        if abs(amount) <= 1e-12:
            amount = float((row.get("quantity") if isinstance(row, dict) else getattr(row, "quantity", 0.0)) or 0.0)
        if abs(amount) <= 1e-12:
            return None

        side = str((row.get("side") if isinstance(row, dict) else getattr(row, "side", "")) or "").strip().lower()
        if side not in {"long", "short"}:
            side = "short" if amount < 0 else "long"
        if side not in {"long", "short"}:
            return None

        entry_price = float((row.get("entry_price") if isinstance(row, dict) else getattr(row, "entry_price", 0.0)) or 0.0)
        current_price = float((row.get("current_price") if isinstance(row, dict) else getattr(row, "current_price", 0.0)) or 0.0)
        if current_price <= 0:
            current_price = float((row.get("markPrice") if isinstance(row, dict) else getattr(row, "markPrice", 0.0)) or 0.0)
        if current_price <= 0:
            current_price = entry_price
        unrealized_pnl = float(
            (row.get("unrealized_pnl") if isinstance(row, dict) else getattr(row, "unrealized_pnl", 0.0))
            or (row.get("unrealizedPnl") if isinstance(row, dict) else getattr(row, "unrealizedPnl", 0.0))
            or 0.0
        )
        return {
            "symbol": symbol,
            "side": side,
            "quantity": abs(float(amount)),
            "entry_price": entry_price,
            "current_price": current_price,
            "unrealized_pnl": unrealized_pnl,
            "leverage": float((row.get("leverage") if isinstance(row, dict) else getattr(row, "leverage", 1.0)) or 1.0),
            "source": "exchange_live",
        }

    async def _load_live_position_snapshots(self, *, exchange: str) -> List[Dict[str, Any]]:
        if str(execution_engine.get_trading_mode() or "").strip().lower() != "live":
            return []

        connector = exchange_manager.get_exchange(exchange)
        if connector is None:
            return []
        default_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").strip().lower()
        if default_type not in {"future", "futures", "swap", "contract", "perp", "perpetual"}:
            return []

        try:
            positions = await asyncio.wait_for(connector.get_positions(), timeout=8.0)
        except Exception as exc:
            logger.debug(f"autonomous agent live position lookup failed: {exc}")
            return []

        snapshots: List[Dict[str, Any]] = []
        for row in positions or []:
            snapshot = self._extract_live_position_snapshot(row)
            if snapshot:
                snapshots.append(snapshot)
        return snapshots

    async def _tracked_position_symbols(self, *, exchange: str, account_id: str) -> List[str]:
        local_symbols: List[str] = []
        for position in position_manager.get_all_positions():
            try:
                if str(getattr(position, "exchange", "") or "").strip().lower() != str(exchange or "").strip().lower():
                    continue
                if str(getattr(position, "account_id", "main") or "main") != str(account_id or "main"):
                    continue
                if abs(float(getattr(position, "quantity", 0.0) or 0.0)) <= 1e-12:
                    continue
                local_symbols.append(str(getattr(position, "symbol", "") or ""))
            except Exception:
                continue

        live_symbols = [str(item.get("symbol") or "") for item in await self._load_live_position_snapshots(exchange=exchange)]
        return _merge_symbol_sequence(local_symbols, live_symbols, max_items=30)

    async def _resolve_position_payload(self, *, exchange: str, symbol: str, account_id: str) -> Dict[str, Any]:
        position = position_manager.get_position(exchange, symbol, account_id=account_id)
        if position is not None:
            with contextlib.suppress(Exception):
                return {
                    "side": str(getattr(position.side, "value", "") or ""),
                    "quantity": float(getattr(position, "quantity", 0.0) or 0.0),
                    "entry_price": float(getattr(position, "entry_price", 0.0) or 0.0),
                    "current_price": float(getattr(position, "current_price", 0.0) or 0.0),
                    "unrealized_pnl": float(getattr(position, "unrealized_pnl", 0.0) or 0.0),
                    "leverage": float(getattr(position, "leverage", 1.0) or 1.0),
                }

        target_symbol = _canonical_symbol_key(symbol)
        if not target_symbol:
            return {}

        for snapshot in await self._load_live_position_snapshots(exchange=exchange):
            if _canonical_symbol_key(snapshot.get("symbol")) != target_symbol:
                continue
            payload = dict(snapshot)
            payload.pop("symbol", None)
            return payload
        return {}

    def get_runtime_config(self) -> Dict[str, Any]:
        requested_provider = _normalize_provider(self._get("AI_AUTONOMOUS_AGENT_PROVIDER", "codex"))
        model_override = str(self._get("AI_AUTONOMOUS_AGENT_MODEL", "") or "").strip()
        providers = self._provider_catalog()
        provider, provider_fallback = self._resolve_provider(requested_provider, providers)
        model = ("" if provider_fallback else model_override) or self._provider_model(provider)
        symbol_mode = _normalize_symbol_mode(self._get("AI_AUTONOMOUS_AGENT_SYMBOL_MODE", "manual"))
        configured_symbol = _normalize_symbol_text(self._get("AI_AUTONOMOUS_AGENT_SYMBOL", "BTC/USDT") or "BTC/USDT")
        universe_symbols = _normalize_symbol_list(
            self._get("AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS", None),
            default=_DEFAULT_AUTO_UNIVERSE if symbol_mode == "auto" else [configured_symbol],
            max_items=30,
        )
        return {
            "enabled": bool(self._get("AI_AUTONOMOUS_AGENT_ENABLED", False)),
            "auto_start": bool(self._get("AI_AUTONOMOUS_AGENT_AUTO_START", False)),
            "mode": _normalize_mode(self._get("AI_AUTONOMOUS_AGENT_MODE", "shadow")),
            "provider": provider,
            "model": model,
            "provider_requested": requested_provider,
            "provider_fallback": provider_fallback,
            "exchange": str(self._get("AI_AUTONOMOUS_AGENT_EXCHANGE", "binance") or "binance").strip().lower(),
            "symbol": configured_symbol,
            "symbol_mode": symbol_mode,
            "universe_symbols": universe_symbols,
            "selection_top_n": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_SELECTION_TOP_N", 10), 10, low=3, high=20),
            "timeframe": str(self._get("AI_AUTONOMOUS_AGENT_TIMEFRAME", "15m") or "15m").strip(),
            "interval_sec": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_INTERVAL_SEC", 120), 120, low=15, high=7200),
            "lookback_bars": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_LOOKBACK_BARS", 240), 240, low=30, high=4000),
            "min_confidence": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE", 0.58), 0.58, low=0.0, high=1.0),
            "default_leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
            "max_leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
            "default_stop_loss_pct": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_STOP_LOSS_PCT", 0.02), 0.02, low=0.001, high=0.5),
            "default_take_profit_pct": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_TAKE_PROFIT_PCT", 0.04), 0.04, low=0.001, high=2.0),
            "timeout_ms": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_TIMEOUT_MS", 30000), 30000, low=1000, high=120000),
            "max_tokens": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_MAX_TOKENS", 420), 420, low=32, high=4096),
            "temperature": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_TEMPERATURE", 0.15), 0.15, low=0.0, high=1.5),
            "cooldown_sec": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_COOLDOWN_SEC", 180), 180, low=0, high=86400),
            "allow_live": bool(self._get("AI_AUTONOMOUS_AGENT_ALLOW_LIVE", False)),
            "account_id": str(self._get("AI_AUTONOMOUS_AGENT_ACCOUNT_ID", "main") or "main").strip() or "main",
            "strategy_name": str(self._get("AI_AUTONOMOUS_AGENT_STRATEGY_NAME", "AI_AutonomousAgent") or "AI_AutonomousAgent").strip() or "AI_AutonomousAgent",
            "providers": providers,
        }

    async def update_runtime_config(self, **kwargs: Any) -> Dict[str, Any]:
        updates: Dict[str, Any] = {}
        if "enabled" in kwargs and kwargs["enabled"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ENABLED"] = bool(kwargs["enabled"])
        if "auto_start" in kwargs and kwargs["auto_start"] is not None:
            updates["AI_AUTONOMOUS_AGENT_AUTO_START"] = bool(kwargs["auto_start"])
        if "mode" in kwargs and kwargs["mode"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MODE"] = _normalize_mode(kwargs["mode"])
        if "provider" in kwargs and kwargs["provider"] is not None:
            updates["AI_AUTONOMOUS_AGENT_PROVIDER"] = _normalize_provider(kwargs["provider"])
        if "model" in kwargs and kwargs["model"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MODEL"] = str(kwargs["model"]).strip()
        if "exchange" in kwargs and kwargs["exchange"] is not None:
            updates["AI_AUTONOMOUS_AGENT_EXCHANGE"] = str(kwargs["exchange"]).strip().lower() or "binance"
        if "symbol" in kwargs and kwargs["symbol"] is not None:
            updates["AI_AUTONOMOUS_AGENT_SYMBOL"] = _normalize_symbol_text(kwargs["symbol"]) or "BTC/USDT"
        if "symbol_mode" in kwargs and kwargs["symbol_mode"] is not None:
            updates["AI_AUTONOMOUS_AGENT_SYMBOL_MODE"] = _normalize_symbol_mode(kwargs["symbol_mode"])
        if "universe_symbols" in kwargs and kwargs["universe_symbols"] is not None:
            updates["AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS"] = _normalize_symbol_list(
                kwargs["universe_symbols"],
                default=_DEFAULT_AUTO_UNIVERSE,
                max_items=30,
            )
        if "selection_top_n" in kwargs and kwargs["selection_top_n"] is not None:
            updates["AI_AUTONOMOUS_AGENT_SELECTION_TOP_N"] = _coerce_int(kwargs["selection_top_n"], 10, low=3, high=20)
        if "timeframe" in kwargs and kwargs["timeframe"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TIMEFRAME"] = str(kwargs["timeframe"]).strip() or "15m"
        if "interval_sec" in kwargs and kwargs["interval_sec"] is not None:
            updates["AI_AUTONOMOUS_AGENT_INTERVAL_SEC"] = _coerce_int(kwargs["interval_sec"], 120, low=15, high=7200)
        if "lookback_bars" in kwargs and kwargs["lookback_bars"] is not None:
            updates["AI_AUTONOMOUS_AGENT_LOOKBACK_BARS"] = _coerce_int(kwargs["lookback_bars"], 240, low=30, high=4000)
        if "min_confidence" in kwargs and kwargs["min_confidence"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE"] = _coerce_float(kwargs["min_confidence"], 0.58, low=0.0, high=1.0)
        if "default_leverage" in kwargs and kwargs["default_leverage"] is not None:
            updates["AI_AUTONOMOUS_AGENT_DEFAULT_LEVERAGE"] = _FIXED_AUTONOMOUS_AGENT_LEVERAGE
        if "max_leverage" in kwargs and kwargs["max_leverage"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MAX_LEVERAGE"] = _FIXED_AUTONOMOUS_AGENT_LEVERAGE
        if "default_stop_loss_pct" in kwargs and kwargs["default_stop_loss_pct"] is not None:
            updates["AI_AUTONOMOUS_AGENT_STOP_LOSS_PCT"] = _coerce_float(kwargs["default_stop_loss_pct"], 0.02, low=0.001, high=0.5)
        if "default_take_profit_pct" in kwargs and kwargs["default_take_profit_pct"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TAKE_PROFIT_PCT"] = _coerce_float(kwargs["default_take_profit_pct"], 0.04, low=0.001, high=2.0)
        if "timeout_ms" in kwargs and kwargs["timeout_ms"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TIMEOUT_MS"] = _coerce_int(kwargs["timeout_ms"], 30000, low=1000, high=120000)
        if "max_tokens" in kwargs and kwargs["max_tokens"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MAX_TOKENS"] = _coerce_int(kwargs["max_tokens"], 420, low=32, high=4096)
        if "temperature" in kwargs and kwargs["temperature"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TEMPERATURE"] = _coerce_float(kwargs["temperature"], 0.15, low=0.0, high=1.5)
        if "cooldown_sec" in kwargs and kwargs["cooldown_sec"] is not None:
            updates["AI_AUTONOMOUS_AGENT_COOLDOWN_SEC"] = _coerce_int(kwargs["cooldown_sec"], 180, low=0, high=86400)
        if "allow_live" in kwargs and kwargs["allow_live"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ALLOW_LIVE"] = bool(kwargs["allow_live"])
        if "account_id" in kwargs and kwargs["account_id"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ACCOUNT_ID"] = str(kwargs["account_id"]).strip() or "main"
        if "strategy_name" in kwargs and kwargs["strategy_name"] is not None:
            updates["AI_AUTONOMOUS_AGENT_STRATEGY_NAME"] = str(kwargs["strategy_name"]).strip() or "AI_AutonomousAgent"

        if not updates:
            return self.get_runtime_config()
        async with self._lock:
            self._override.update(updates)
        self._save_overlay()
        return self.get_runtime_config()

    def is_running(self) -> bool:
        return bool(self._task and not self._task.done())

    def get_status(self) -> Dict[str, Any]:
        return {
            "running": self.is_running(),
            "last_run_at": self._last_run_at,
            "last_error": self._last_error,
            "tick_count": int(self._tick_count),
            "submitted_count": int(self._submitted_count),
            "last_decision": self._last_decision,
            "last_execution": self._last_execution,
            "last_research_context": self._last_research_context,
            "last_diagnostics": self._last_diagnostics,
            "last_symbol_scan": self._last_symbol_scan,
            "model_feedback_guard": self._model_feedback_guard_status(),
            "profile": dict(self._profile or _default_profile()),
            "learning_memory": dict(self._learning_memory or {}),
            "journal_path": str(self._journal_path),
            "learning_memory_path": str(self._learning_memory_path),
        }

    def _model_feedback_guard_status(self) -> Dict[str, Any]:
        last_failure_issue = _describe_model_feedback_issue(self._model_feedback_last_failure_error or "")
        return {
            "last_success_at": _utc_iso_from_unix(self._last_model_feedback_at),
            "outage_started_at": _utc_iso_from_unix(self._model_feedback_outage_started_at),
            "failure_streak": int(self._model_feedback_failure_streak),
            "last_failure_kind": self._model_feedback_last_failure_kind,
            "last_failure_error": self._model_feedback_last_failure_error,
            "last_failure_http_status": last_failure_issue.get("http_status"),
            "last_failure_label": last_failure_issue.get("label") if self._model_feedback_last_failure_kind else None,
            "alert_sent_at": _utc_iso_from_unix(self._model_feedback_alert_sent_at),
            "alert_after_sec": _MODEL_FEEDBACK_OUTAGE_ALERT_SEC,
            "hard_timeout_sec": _MODEL_FEEDBACK_HARD_TIMEOUT_SEC,
        }

    def _reset_model_feedback_outage(self) -> None:
        self._model_feedback_outage_started_at = None
        self._model_feedback_failure_streak = 0
        self._model_feedback_last_failure_kind = None
        self._model_feedback_last_failure_error = None
        self._model_feedback_alert_sent_at = None

    def _record_model_feedback_success(self) -> None:
        self._last_model_feedback_at = time.time()
        self._reset_model_feedback_outage()

    def _record_model_feedback_failure(self, exc: BaseException) -> Optional[Dict[str, Any]]:
        kind = _classify_model_feedback_error(exc)
        if kind not in {"rate_limit", "service_unavailable", "timeout"}:
            self._reset_model_feedback_outage()
            return None

        now = time.time()
        if self._model_feedback_failure_streak <= 0:
            self._model_feedback_outage_started_at = now
        self._model_feedback_failure_streak += 1
        self._model_feedback_last_failure_kind = kind
        self._model_feedback_last_failure_error = _format_exception_short(exc)[:300]

        outage_anchor = self._last_model_feedback_at
        if outage_anchor is None:
            outage_anchor = self._model_feedback_outage_started_at or now
        outage_duration_sec = max(0.0, now - float(outage_anchor))
        if outage_duration_sec < float(_MODEL_FEEDBACK_OUTAGE_ALERT_SEC):
            return None
        if self._model_feedback_alert_sent_at is not None:
            return None

        return {
            "kind": kind,
            "failure_streak": int(self._model_feedback_failure_streak),
            "outage_duration_sec": outage_duration_sec,
            "error": self._model_feedback_last_failure_error,
            "last_success_at": _utc_iso_from_unix(self._last_model_feedback_at),
            "outage_started_at": _utc_iso_from_unix(self._model_feedback_outage_started_at),
        }

    def _current_model_feedback_outage_duration_sec(self) -> float:
        if self._model_feedback_failure_streak <= 0:
            return 0.0
        if self._model_feedback_last_failure_kind not in {"rate_limit", "service_unavailable", "timeout"}:
            return 0.0
        now = time.time()
        outage_anchor = self._last_model_feedback_at
        if outage_anchor is None:
            outage_anchor = self._model_feedback_outage_started_at or now
        return max(0.0, now - float(outage_anchor))

    async def _protect_profitable_local_position_during_model_outage(
        self,
        *,
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
        outage_duration_sec: float,
        model_feedback_issue: Dict[str, Any],
    ) -> Dict[str, Any]:
        exchange = str(cfg.get("exchange") or context_payload.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or context_payload.get("symbol") or "BTC/USDT")
        account_id = str(cfg.get("account_id") or "main")
        local_position = position_manager.get_position(exchange, symbol, account_id=account_id)
        if local_position is None:
            return {"applied": False, "reason": "no_local_position"}
        result = await execution_engine.tighten_profitable_position_protection(
            exchange=exchange,
            symbol=symbol,
            account_id=account_id,
            current_price=_safe_nonnegative_float(context_payload.get("price"), 0.0),
            reason=f"model_feedback_outage:{model_feedback_issue.get('kind') or 'unknown'}",
        )
        if bool(result.get("applied")):
            logger.warning(
                "autonomous agent armed outage profit protection "
                f"symbol={symbol} exchange={exchange} account_id={account_id} "
                f"duration_min={float(outage_duration_sec) / 60.0:.1f} "
                f"kind={model_feedback_issue.get('kind') or 'unknown'}"
            )
        return result

    async def _send_model_feedback_outage_alert(
        self,
        *,
        provider: str,
        model: str,
        cfg: Dict[str, Any],
        selection: Dict[str, Any],
        context_payload: Dict[str, Any],
        failure: Dict[str, Any],
    ) -> None:
        try:
            from core.notifications import notification_manager

            selected_symbol = str(
                selection.get("selected_symbol")
                or context_payload.get("symbol")
                or cfg.get("symbol")
                or "BTC/USDT"
            )
            title = f"AI自动交易模型反馈中断告警: {provider}/{model}"
            message = (
                "连续出现模型 429/503/timeout，且超过 30 分钟没有得到成功模型反馈；"
                "当前轮次已回退为 hold。\n"
                f"异常类型: {failure.get('kind')}\n"
                f"持续时长: {float(failure.get('outage_duration_sec') or 0.0) / 60.0:.1f} 分钟\n"
                f"连续失败: {int(failure.get('failure_streak') or 0)} 次\n"
                f"最近错误: {str(failure.get('error') or '')}\n"
                f"执行模式: {cfg.get('mode')} / allow_live={bool(cfg.get('allow_live'))}\n"
                f"交易模式: {execution_engine.get_trading_mode()}\n"
                f"配置币种: {cfg.get('symbol')}\n"
                f"本轮选中: {selected_symbol}\n"
                f"时间框架: {cfg.get('timeframe')}\n"
                f"上次成功反馈: {failure.get('last_success_at') or 'none'}\n"
                f"本次失联开始: {failure.get('outage_started_at') or 'unknown'}"
            )
            result = await notification_manager.send_message(
                title=title,
                message=message,
                channels=["feishu"],
            )
            if not bool((result or {}).get("feishu")):
                logger.warning(
                    "autonomous agent model feedback outage alert did not reach feishu "
                    f"(provider={provider}, model={model})"
                )
            else:
                self._model_feedback_alert_sent_at = time.time()
                logger.warning(
                    "autonomous agent model feedback outage alert sent to feishu "
                    f"(provider={provider}, model={model}, symbol={selected_symbol})"
                )
        except Exception as exc:
            logger.warning(f"autonomous agent model feedback outage alert failed: {exc}")

    async def start(self) -> Dict[str, Any]:
        if self.is_running():
            return self.get_status()
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="ai_autonomous_agent")
        runtime_state.register_task("ai_autonomous_agent", restart_on_failure=False)
        runtime_state.mark_task_started("ai_autonomous_agent")
        logger.info("AutonomousTradingAgent started")
        return self.get_status()

    async def stop(self) -> Dict[str, Any]:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        self._stop_event = None
        runtime_state.mark_task_stopped("ai_autonomous_agent")
        logger.info("AutonomousTradingAgent stopped")
        return self.get_status()

    async def _loop(self) -> None:
        await asyncio.sleep(2)
        while self._stop_event is not None and not self._stop_event.is_set():
            try:
                await self.run_once(trigger="loop")
                runtime_state.touch_task("ai_autonomous_agent", success=True)
            except Exception as exc:
                self._last_error = str(exc)
                runtime_state.mark_task_failed("ai_autonomous_agent", str(exc), will_restart=False)
                logger.warning(f"autonomous agent tick failed: {exc}")
            interval = int(self.get_runtime_config().get("interval_sec") or 120)
            for _ in range(max(1, interval)):
                if self._stop_event is None or self._stop_event.is_set():
                    break
                await asyncio.sleep(1)

    async def _call_provider(
        self,
        *,
        provider: str,
        model: str,
        timeout_ms: int,
        max_tokens: int,
        temperature: float,
        system_prompt: str,
        user_prompt: str,
    ) -> Dict[str, Any]:
        provider = _normalize_provider(provider)
        timeout = aiohttp.ClientTimeout(total=max(1, int(timeout_ms)) / 1000.0)
        base_url = self._provider_base_url(provider)

        if provider == "claude":
            api_key = self._provider_api_key(provider)
            if not api_key:
                raise RuntimeError(f"{provider}_api_key_missing")
            url = f"{base_url}/v1/messages" if not base_url.endswith("/v1") else f"{base_url}/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            payload = {
                "model": model,
                "max_tokens": int(max_tokens),
                "temperature": float(temperature),
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status >= 400:
                        body = (await resp.text())[:300]
                        raise RuntimeError(f"claude_http_{resp.status}:{body}")
                    data = await resp.json()
            text = ""
            content = data.get("content") if isinstance(data, dict) else None
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and str(item.get("type") or "") == "text":
                        text = str(item.get("text") or "")
                        if text:
                            break
            if not text:
                raise RuntimeError("claude_empty_content")
            return _extract_json_obj(text)

        if provider == "codex":
            targets = self._provider_endpoint_targets(provider)
            if not any(bool(str(target.get("api_key") or "").strip()) for target in targets):
                raise RuntimeError(f"{provider}_api_key_missing")
            payload = build_responses_payload(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_output_tokens=int(max_tokens),
                temperature=float(temperature),
                text_format="json_object",
                stream=False,
            )
            async with aiohttp.ClientSession(timeout=timeout) as session:
                last_exc: Optional[BaseException] = None
                total_targets = len(targets)
                for idx, target in enumerate(targets):
                    target_base_url = str(target.get("base_url") or "").rstrip("/")
                    target_api_key = str(target.get("api_key") or "").strip()
                    if not target_base_url or not target_api_key:
                        continue
                    url = responses_endpoint(target_base_url)
                    headers = build_openai_headers(target_api_key)
                    try:
                        async with session.post(url, headers=headers, json=payload) as resp:
                            if resp.status >= 400:
                                body = (await resp.text())[:300]
                                err = RuntimeError(f"{provider}_http_{resp.status}:{body}")
                                if idx + 1 < total_targets and is_retryable_openai_status(resp.status):
                                    last_exc = err
                                    logger.warning(
                                        f"autonomous_agent codex primary endpoint failed with {resp.status}; "
                                        f"trying backup {idx + 2}/{total_targets}"
                                    )
                                    continue
                                raise err
                            data = await read_aiohttp_responses_json(resp)
                        text = extract_response_text(data)
                        if not text:
                            err = RuntimeError(f"{provider}_empty_content")
                            if idx + 1 < total_targets:
                                last_exc = err
                                logger.warning(
                                    f"autonomous_agent codex endpoint returned empty content; "
                                    f"trying backup {idx + 2}/{total_targets}"
                                )
                                continue
                            raise err
                        return _extract_json_obj(text)
                    except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                        if idx + 1 < total_targets:
                            last_exc = exc
                            logger.warning(
                                f"autonomous_agent codex endpoint transport failure; "
                                f"trying backup {idx + 2}/{total_targets}: {exc}"
                            )
                            continue
                        raise
                if last_exc is not None:
                    raise last_exc
                raise RuntimeError(f"{provider}_base_url_missing")

        api_key = self._provider_api_key(provider)
        if not api_key:
            raise RuntimeError(f"{provider}_api_key_missing")
        url = f"{base_url}/chat/completions"
        headers = build_openai_headers(api_key)
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
            "response_format": {"type": "json_object"},
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    body = (await resp.text())[:300]
                    raise RuntimeError(f"{provider}_http_{resp.status}:{body}")
                data = await resp.json()

        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            raise RuntimeError(f"{provider}_empty_choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = message.get("content")
        if isinstance(content, list):
            text = "\n".join(
                str(x.get("text") or x.get("content") or "")
                for x in content
                if isinstance(x, dict)
            ).strip()
        else:
            text = str(content or "").strip()
        if not text:
            raise RuntimeError(f"{provider}_empty_content")
        return _extract_json_obj(text)

    async def _load_market_data(self, cfg: Dict[str, Any]) -> pd.DataFrame:
        now = _utc_now()
        timeframe_sec = _timeframe_to_seconds(str(cfg.get("timeframe") or "15m"))
        lookback = int(cfg.get("lookback_bars") or 240)
        span_sec = max(timeframe_sec * lookback, 3600 * 8)
        start_time = now - timedelta(seconds=span_sec + timeframe_sec * 2)
        exchange = str(cfg.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        timeframe = str(cfg.get("timeframe") or "15m")
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=now,
        )
        df = df.copy() if df is not None and not df.empty else pd.DataFrame()

        connector = exchange_manager.get_exchange(exchange)
        if connector is not None:
            try:
                live_klines = await connector.get_klines(symbol, timeframe, limit=max(40, lookback))
                live_df = self._df_from_klines(live_klines)
                if not live_df.empty:
                    if df.empty:
                        df = live_df
                    else:
                        df = pd.concat([df, live_df])
                        df = df[~df.index.duplicated(keep="last")].sort_index()
            except Exception as exc:
                logger.debug(
                    f"autonomous_agent live klines fallback failed for {exchange} {symbol} {timeframe}: {exc}"
                )

        if df.empty:
            return pd.DataFrame()
        return df.tail(max(40, lookback)).copy()

    @staticmethod
    def _df_from_klines(klines: List[Any]) -> pd.DataFrame:
        if not klines:
            return pd.DataFrame()
        frame = pd.DataFrame(
            [
                {
                    "timestamp": getattr(kline, "timestamp", None),
                    "open": getattr(kline, "open", None),
                    "high": getattr(kline, "high", None),
                    "low": getattr(kline, "low", None),
                    "close": getattr(kline, "close", None),
                    "volume": getattr(kline, "volume", None),
                }
                for kline in klines
            ]
        )
        if frame.empty:
            return pd.DataFrame()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
        return frame

    async def _resolve_last_price(self, cfg: Dict[str, Any], market_data: pd.DataFrame) -> float:
        if market_data is not None and not market_data.empty and "close" in market_data.columns:
            try:
                value = float(pd.to_numeric(market_data["close"], errors="coerce").dropna().iloc[-1])
                if value > 0:
                    return value
            except Exception:
                pass
        exchange = str(cfg.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        connector = exchange_manager.get_exchange(exchange)
        if connector is None:
            return 0.0
        try:
            ticker = await connector.get_ticker(symbol)
            return float(getattr(ticker, "last", 0.0) or 0.0)
        except Exception:
            return 0.0

    async def _resolve_account_risk_base(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        account_equity = 0.0
        try:
            account_equity = float(
                await asyncio.wait_for(execution_engine.get_account_equity_snapshot(force=False), timeout=12.0)
            )
        except Exception:
            account_equity = 0.0

        strategy_allocation = 0.0
        with contextlib.suppress(Exception):
            strategy_allocation = float(strategy_manager.get_strategy_allocation(cfg.get("strategy_name")) or 0.0)

        position_cap_notional = 0.0
        if account_equity > 0:
            with contextlib.suppress(Exception):
                position_cap_notional = float(
                    execution_engine.get_strategy_position_cap_notional(
                        account_equity=account_equity,
                        strategy_allocation=strategy_allocation,
                    )
                    or 0.0
                )

        return {
            "account_equity": float(max(0.0, account_equity)),
            "strategy_allocation": float(max(0.0, strategy_allocation)),
            "position_cap_notional": float(max(0.0, position_cap_notional)),
            "trading_mode": str(execution_engine.get_trading_mode() or "paper"),
        }

    async def _annotate_position_payload(
        self,
        *,
        cfg: Dict[str, Any],
        position_payload: Dict[str, Any],
        last_price: float,
        account_risk_base: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = dict(position_payload or {})
        if not payload:
            return payload

        quantity = _safe_nonnegative_float(payload.get("quantity"), 0.0)
        mark_price = 0.0
        for raw_price in (
            payload.get("current_price"),
            payload.get("entry_price"),
            last_price,
        ):
            price = _safe_nonnegative_float(raw_price, 0.0)
            if price > 0:
                mark_price = price
                break

        position_notional = quantity * mark_price if quantity > 0 and mark_price > 0 else 0.0
        base = dict(account_risk_base or {})
        position_cap_notional = _safe_nonnegative_float(base.get("position_cap_notional"), 0.0)
        same_direction_limit_ratio = _coerce_float(
            cfg.get("same_direction_max_exposure_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO),
            _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
            low=0.2,
            high=1.0,
        )

        exposure_ratio = 0.0
        remaining_notional = 0.0
        if position_cap_notional > 0:
            exposure_ratio = position_notional / position_cap_notional
            remaining_notional = max(
                0.0,
                position_cap_notional * same_direction_limit_ratio - position_notional,
            )

        payload.update(
            {
                "position_notional": float(position_notional),
                "position_cap_notional": float(position_cap_notional),
                "same_direction_exposure_ratio": float(exposure_ratio),
                "same_direction_exposure_limit_ratio": float(same_direction_limit_ratio),
                "same_direction_remaining_notional": float(remaining_notional),
            }
        )
        return payload

    def _build_market_structure_payload(
        self,
        *,
        market_data: pd.DataFrame,
        timeframe_sec: int,
        last_price: float,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "available": False,
            "last_bar_at": None,
            "bar_interval_sec": int(max(1, timeframe_sec)),
            "returns": {
                "r_15m": 0.0,
                "r_1h": 0.0,
                "r_4h": 0.0,
                "r_24h": 0.0,
            },
            "trend": {
                "ema_fast": 0.0,
                "ema_slow": 0.0,
                "ema_gap_pct": 0.0,
                "close_vs_ema_slow_pct": 0.0,
                "label": "unknown",
            },
            "microstructure": {
                "atr_pct": 0.0,
                "realized_vol": 0.0,
                "spread_proxy": 0.0,
            },
            "volume": {
                "last": 0.0,
                "avg_20": 0.0,
                "ratio_20": 0.0,
                "zscore_20": 0.0,
            },
            "range": {
                "lookback_bars": 0,
                "high": 0.0,
                "low": 0.0,
                "position_pct": 0.0,
            },
        }
        if market_data is None or market_data.empty:
            return payload

        window = market_data.tail(240).copy()
        close = (
            pd.to_numeric(window["close"], errors="coerce").dropna()
            if "close" in window.columns
            else pd.Series(dtype=float)
        )
        if close.empty:
            return payload

        last_close = float(close.iloc[-1])
        if last_close <= 0:
            return payload

        index = close.index if isinstance(close.index, pd.DatetimeIndex) else None
        last_bar_at = None
        if index is not None and len(index):
            with contextlib.suppress(Exception):
                ts = pd.Timestamp(index[-1])
                last_bar_at = ts.isoformat()

        def _return_for(lookback_sec: int) -> float:
            if close.empty:
                return 0.0
            if index is not None and len(index):
                target = pd.Timestamp(index[-1]) - pd.Timedelta(seconds=int(max(1, lookback_sec)))
                pos = index.searchsorted(target, side="right") - 1
                if 0 <= pos < len(close) - 1:
                    base = float(close.iloc[pos])
                    if base > 0:
                        return float(last_close / base - 1.0)
            steps = max(1, int(round(float(max(1, lookback_sec)) / max(1.0, float(timeframe_sec)))))
            if len(close) <= steps:
                return 0.0
            base = float(close.iloc[-steps - 1])
            if base <= 0:
                return 0.0
            return float(last_close / base - 1.0)

        ema_fast = float(close.ewm(span=8, adjust=False).mean().iloc[-1])
        ema_slow = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
        ema_gap_pct = float(ema_fast / ema_slow - 1.0) if ema_slow > 0 else 0.0
        close_vs_ema_slow_pct = float(last_close / ema_slow - 1.0) if ema_slow > 0 else 0.0
        trend_label = "range"
        if ema_gap_pct >= 0.002 and close_vs_ema_slow_pct >= 0.0:
            trend_label = "uptrend"
        elif ema_gap_pct <= -0.002 and close_vs_ema_slow_pct <= 0.0:
            trend_label = "downtrend"

        volume_series = (
            pd.to_numeric(window["volume"], errors="coerce").dropna()
            if "volume" in window.columns
            else pd.Series(dtype=float)
        )
        last_volume = float(volume_series.iloc[-1]) if not volume_series.empty else 0.0
        volume_tail = volume_series.tail(20)
        avg_volume = float(volume_tail.mean()) if not volume_tail.empty else 0.0
        volume_std = float(volume_tail.std(ddof=0)) if len(volume_tail) >= 2 else 0.0
        volume_ratio = float(last_volume / avg_volume) if avg_volume > 0 else 0.0
        volume_zscore = float((last_volume - avg_volume) / volume_std) if volume_std > 1e-12 else 0.0

        range_window = window.tail(min(len(window), 96)).copy()
        high_series = (
            pd.to_numeric(range_window["high"], errors="coerce").dropna()
            if "high" in range_window.columns
            else pd.Series(dtype=float)
        )
        low_series = (
            pd.to_numeric(range_window["low"], errors="coerce").dropna()
            if "low" in range_window.columns
            else pd.Series(dtype=float)
        )
        range_high = float(high_series.max()) if not high_series.empty else 0.0
        range_low = float(low_series.min()) if not low_series.empty else 0.0
        range_position = 0.0
        if range_high > range_low:
            price_anchor = float(last_price) if float(last_price or 0.0) > 0 else last_close
            range_position = float((price_anchor - range_low) / (range_high - range_low))
            range_position = _coerce_float(range_position, 0.0, low=0.0, high=1.0)

        payload.update(
            {
                "available": True,
                "last_bar_at": last_bar_at,
                "returns": {
                    "r_15m": _return_for(15 * 60),
                    "r_1h": _return_for(60 * 60),
                    "r_4h": _return_for(4 * 60 * 60),
                    "r_24h": _return_for(24 * 60 * 60),
                },
                "trend": {
                    "ema_fast": float(ema_fast),
                    "ema_slow": float(ema_slow),
                    "ema_gap_pct": float(ema_gap_pct),
                    "close_vs_ema_slow_pct": float(close_vs_ema_slow_pct),
                    "label": trend_label,
                },
                "microstructure": {
                    **{
                        key: float(value)
                        for key, value in (microstructure_proxies(window) or {}).items()
                    },
                },
                "volume": {
                    "last": float(last_volume),
                    "avg_20": float(avg_volume),
                    "ratio_20": float(volume_ratio),
                    "zscore_20": float(volume_zscore),
                },
                "range": {
                    "lookback_bars": int(len(range_window)),
                    "high": float(range_high),
                    "low": float(range_low),
                    "position_pct": float(range_position),
                },
            }
        )
        return payload

    async def _build_event_summary(self, *, symbol: str, timeframe_sec: int) -> Dict[str, Any]:
        since_minutes = max(240, int(round(max(1.0, float(timeframe_sec)) * 4.0 / 60.0)))
        event_symbol = _canonical_symbol_key(symbol).replace("/", "")
        payload: Dict[str, Any] = {
            "available": True,
            "symbol": event_symbol,
            "since_minutes": int(since_minutes),
            "events_count": 0,
            "source_diversity": 0,
            "sentiment_counts": {
                "positive": 0,
                "neutral": 0,
                "negative": 0,
            },
            "dominant_sentiment": "neutral",
            "dominant_sentiment_ratio": 0.0,
            "net_sentiment": 0.0,
            "news_alpha_proxy": 0.0,
            "weighted_half_life_min": 0.0,
            "event_concentration": 0.0,
            "top_event_types": [],
            "top_sources": [],
            "top_events": [],
        }
        if not event_symbol:
            return payload

        try:
            events = await news_db.get_recent_events(symbol=event_symbol, since_minutes=since_minutes)
        except Exception as exc:
            logger.debug(f"autonomous agent event summary failed for {symbol}: {exc}")
            payload["available"] = False
            payload["error"] = _format_exception_short(exc)
            return payload

        if not events:
            return payload

        now = _utc_now()
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        type_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        weighted_half_numer = 0.0
        weighted_half_denom = 0.0
        weighted_alpha = 0.0
        impact_weight_total = 0.0
        impact_sentiment_total = 0.0
        ranked_events: List[Tuple[float, Dict[str, Any]]] = []

        for event in events:
            event_dict = dict(event or {})
            evidence = dict(event_dict.get("evidence") or {})
            ts = pd.to_datetime(event_dict.get("ts"), utc=True, errors="coerce")
            event_ts = ts.to_pydatetime() if pd.notna(ts) else now
            age_min = max(0.0, (now - event_ts).total_seconds() / 60.0)
            impact_score = max(0.0, float(event_dict.get("impact_score") or 0.0))
            half_life_min = max(1.0, float(event_dict.get("half_life_min") or 180.0))
            sentiment = int(event_dict.get("sentiment") or 0)
            sentiment_key = "positive" if sentiment > 0 else "negative" if sentiment < 0 else "neutral"
            sentiment_counts[sentiment_key] += 1

            event_type = str(event_dict.get("event_type") or "other").strip().lower() or "other"
            type_counts[event_type] = type_counts.get(event_type, 0) + 1

            source = str(evidence.get("source") or "unknown").strip().lower() or "unknown"
            source_counts[source] = source_counts.get(source, 0) + 1

            decay = math.exp(-age_min / half_life_min)
            decayed_alpha = impact_score * sentiment * decay
            weighted_alpha += decayed_alpha
            abs_alpha = abs(decayed_alpha)
            weighted_half_numer += half_life_min * abs_alpha
            weighted_half_denom += abs_alpha
            impact_weight_total += impact_score
            impact_sentiment_total += impact_score * sentiment

            rank_score = abs_alpha if abs_alpha > 0 else impact_score
            ranked_events.append(
                (
                    rank_score,
                    {
                        "event_id": str(event_dict.get("event_id") or ""),
                        "ts": event_ts.isoformat(),
                        "title": str(evidence.get("title") or evidence.get("matched_reason") or "").strip()[:180],
                        "source": source,
                        "event_type": event_type,
                        "sentiment": int(sentiment),
                        "impact_score": float(impact_score),
                        "half_life_min": int(round(half_life_min)),
                        "age_min": float(round(age_min, 3)),
                        "decayed_alpha": float(round(decayed_alpha, 6)),
                    },
                )
            )

        ranked_events.sort(key=lambda item: item[0], reverse=True)
        total_events = int(len(events))
        dominant_sentiment = "neutral"
        dominant_count = sentiment_counts["neutral"]
        for key in ("positive", "negative", "neutral"):
            if sentiment_counts[key] > dominant_count:
                dominant_sentiment = key
                dominant_count = sentiment_counts[key]

        total_abs_rank = sum(score for score, _ in ranked_events)
        payload.update(
            {
                "events_count": total_events,
                "source_diversity": int(len(source_counts)),
                "sentiment_counts": sentiment_counts,
                "dominant_sentiment": dominant_sentiment,
                "dominant_sentiment_ratio": float(dominant_count / max(1, total_events)),
                "net_sentiment": float(impact_sentiment_total / max(impact_weight_total, 1e-9)),
                "news_alpha_proxy": float(weighted_alpha),
                "weighted_half_life_min": float(
                    weighted_half_numer / weighted_half_denom if weighted_half_denom > 0 else 0.0
                ),
                "event_concentration": float(
                    sum(score for score, _ in ranked_events[:3]) / max(total_abs_rank, 1e-9)
                ),
                "top_event_types": [
                    {"event_type": key, "count": int(count)}
                    for key, count in sorted(type_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
                ],
                "top_sources": [
                    {"source": key, "count": int(count)}
                    for key, count in sorted(source_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
                ],
                "top_events": [item for _, item in ranked_events[:3]],
            }
        )
        return payload

    def _build_account_risk_payload(
        self,
        *,
        cfg: Dict[str, Any],
        position_payload: Dict[str, Any],
        account_risk_base: Dict[str, Any],
        last_price: float,
    ) -> Dict[str, Any]:
        position = dict(position_payload or {})
        account_equity = _safe_nonnegative_float(account_risk_base.get("account_equity"), 0.0)
        strategy_allocation = _safe_nonnegative_float(account_risk_base.get("strategy_allocation"), 0.0)
        position_cap_notional = _safe_nonnegative_float(account_risk_base.get("position_cap_notional"), 0.0)
        same_direction_limit_ratio = _coerce_float(
            position.get("same_direction_exposure_limit_ratio", cfg.get("same_direction_max_exposure_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO)),
            _coerce_float(
                cfg.get("same_direction_max_exposure_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO),
                _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
                low=0.2,
                high=1.0,
            ),
            low=0.0,
            high=1.0,
        )
        current_position_side = str(position.get("side") or "").lower()
        current_position_notional = _safe_nonnegative_float(position.get("position_notional"), 0.0)
        same_direction_exposure_ratio = (
            current_position_notional / position_cap_notional if position_cap_notional > 0 else 0.0
        )
        same_direction_remaining_notional = max(
            0.0,
            position_cap_notional * same_direction_limit_ratio - current_position_notional,
        ) if position_cap_notional > 0 else 0.0
        execution_permitted_now = bool(
            str(cfg.get("mode") or "shadow") == "execute"
            and not (
                str(account_risk_base.get("trading_mode") or "paper") == "live"
                and not bool(cfg.get("allow_live"))
            )
        )
        return {
            "trading_mode": str(account_risk_base.get("trading_mode") or execution_engine.get_trading_mode()),
            "agent_mode": str(cfg.get("mode") or "shadow"),
            "allow_live": bool(cfg.get("allow_live")),
            "execution_permitted_now": execution_permitted_now,
            "fixed_leverage": float(_FIXED_AUTONOMOUS_AGENT_LEVERAGE),
            "min_confidence": float(cfg.get("min_confidence") or 0.0),
            "default_stop_loss_pct": float(cfg.get("default_stop_loss_pct") or 0.02),
            "default_take_profit_pct": float(cfg.get("default_take_profit_pct") or 0.04),
            "account_equity": float(account_equity),
            "strategy_allocation": float(strategy_allocation),
            "position_cap_notional": float(position_cap_notional),
            "last_price": float(last_price or 0.0),
            "has_position": bool(current_position_side),
            "current_position_side": current_position_side,
            "current_position_notional": float(current_position_notional),
            "same_direction_limit_ratio": float(same_direction_limit_ratio),
            "same_direction_exposure_ratio": float(same_direction_exposure_ratio),
            "same_direction_remaining_notional": float(same_direction_remaining_notional),
            "can_add_same_direction": bool(
                position_cap_notional > 0 and same_direction_exposure_ratio + 1e-9 < same_direction_limit_ratio
            ),
        }

    def _build_execution_cost_payload(
        self,
        *,
        cfg: Dict[str, Any],
        market_structure: Dict[str, Any],
        account_risk: Dict[str, Any],
    ) -> Dict[str, Any]:
        trading_mode = str(account_risk.get("trading_mode") or execution_engine.get_trading_mode()).strip().lower()
        is_live_mode = trading_mode == "live"
        fee_rate = _safe_nonnegative_float(
            getattr(settings, "LIVE_FEE_RATE", 0.0004) if is_live_mode else getattr(settings, "PAPER_FEE_RATE", 0.0),
            0.0004 if is_live_mode else 0.0,
        )
        configured_slippage_bps = _safe_nonnegative_float(
            getattr(settings, "LIVE_SLIPPAGE_BPS", getattr(settings, "PAPER_SLIPPAGE_BPS", 0.0))
            if is_live_mode
            else getattr(settings, "PAPER_SLIPPAGE_BPS", 0.0),
            0.0,
        )
        configured_slippage_rate = configured_slippage_bps / 10000.0
        micro = dict(market_structure.get("microstructure") or {})
        dynamic_slip_rate_raw = max(
            0.0,
            float(
                dynamic_slippage_rate(
                    atr_pct=float(micro.get("atr_pct") or 0.0),
                    realized_vol=float(micro.get("realized_vol") or 0.0),
                    spread_proxy=float(micro.get("spread_proxy") or 0.0),
                    params=_AI_EXECUTION_DYNAMIC_SLIP_PARAMS,
                )
            ),
        )
        position_cap_notional = _safe_nonnegative_float(account_risk.get("position_cap_notional"), 0.0)
        same_direction_remaining_notional = _safe_nonnegative_float(
            account_risk.get("same_direction_remaining_notional"),
            0.0,
        )
        current_position_notional = _safe_nonnegative_float(account_risk.get("current_position_notional"), 0.0)
        open_notional_reference = (
            same_direction_remaining_notional
            if same_direction_remaining_notional > 0
            else position_cap_notional
        )
        close_notional_reference = current_position_notional if current_position_notional > 0 else 0.0
        notional_reference = close_notional_reference if close_notional_reference > 0 else open_notional_reference
        volume = dict(market_structure.get("volume") or {})
        liquidity_volume_reference = max(
            _safe_nonnegative_float(volume.get("avg_20"), 0.0),
            _safe_nonnegative_float(volume.get("last"), 0.0),
        )
        last_price = _safe_nonnegative_float(account_risk.get("last_price"), 0.0)
        liquidity_reference_notional = max(0.0, liquidity_volume_reference * last_price)
        notional_participation_rate = 0.0
        liquidity_adjustment = 1.0
        dynamic_slip_rate = dynamic_slip_rate_raw
        if liquidity_reference_notional > 0.0 and notional_reference > 0.0:
            notional_participation_rate = min(1.0, notional_reference / liquidity_reference_notional)
            liquidity_adjustment = min(
                1.0,
                math.sqrt(
                    max(0.0, notional_participation_rate)
                    / max(_AI_EXECUTION_LIQUIDITY_TARGET_PARTICIPATION, 1e-9)
                ),
            )
            dynamic_slip_rate = max(0.0, dynamic_slip_rate_raw * liquidity_adjustment)
        min_order_usd = _safe_nonnegative_float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 0.0), 0.0)
        estimated_slippage_rate = max(configured_slippage_rate, dynamic_slip_rate)
        one_way_cost_rate = fee_rate + estimated_slippage_rate
        round_trip_cost_rate = one_way_cost_rate * 2.0
        return {
            "trading_mode": trading_mode,
            "fee_source": "live_default_fee_rate" if is_live_mode else "paper_default_fee_rate",
            "slippage_source": (
                "live_floor_or_dynamic_microstructure_max"
                if is_live_mode
                else "paper_floor_or_dynamic_microstructure_max"
            ),
            "fee_rate": float(fee_rate),
            "fee_bps": float(fee_rate * 10000.0),
            "configured_slippage_bps": float(configured_slippage_bps),
            "dynamic_slippage_raw_bps": float(dynamic_slip_rate_raw * 10000.0),
            "dynamic_slippage_bps": float(dynamic_slip_rate * 10000.0),
            "estimated_slippage_bps": float(estimated_slippage_rate * 10000.0),
            "estimated_one_way_cost_bps": float(one_way_cost_rate * 10000.0),
            "estimated_round_trip_cost_bps": float(round_trip_cost_rate * 10000.0),
            "position_cap_notional": float(position_cap_notional),
            "same_direction_remaining_notional": float(same_direction_remaining_notional),
            "current_position_notional": float(current_position_notional),
            "open_notional_reference": float(open_notional_reference),
            "close_notional_reference": float(close_notional_reference),
            "notional_reference": float(notional_reference),
            "liquidity_reference_notional": float(liquidity_reference_notional),
            "notional_participation_rate": float(notional_participation_rate),
            "liquidity_adjustment_factor": float(liquidity_adjustment),
            "estimated_one_way_cost_usd_at_reference": float(notional_reference * one_way_cost_rate),
            "estimated_round_trip_cost_usd_at_reference": float(notional_reference * round_trip_cost_rate),
            "min_strategy_order_usd": float(min_order_usd),
            "microstructure": {
                "atr_pct": float(micro.get("atr_pct") or 0.0),
                "realized_vol": float(micro.get("realized_vol") or 0.0),
                "spread_proxy": float(micro.get("spread_proxy") or 0.0),
            },
            "notes": [
                "fee_rate and configured_slippage_bps follow current paper/live execution defaults",
                "dynamic slippage is liquidity-adjusted before applying the configured floor",
                "estimated_slippage_bps uses max(configured floor, liquidity-adjusted dynamic estimate)",
            ],
        }

    def _build_trade_management_metadata(
        self,
        *,
        decision: Dict[str, Any],
        context_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        action = str(decision.get("action") or "").strip().lower()
        if action not in {"buy", "sell"}:
            return {}

        execution_cost = dict(context_payload.get("execution_cost") or {})
        market_structure = dict(context_payload.get("market_structure") or {})
        microstructure = dict(market_structure.get("microstructure") or {})

        one_way_cost_pct = _safe_nonnegative_float(
            execution_cost.get("estimated_one_way_cost_bps"),
            0.0,
        ) / 10000.0
        round_trip_cost_pct = _safe_nonnegative_float(
            execution_cost.get("estimated_round_trip_cost_bps"),
            0.0,
        ) / 10000.0
        atr_pct = _safe_nonnegative_float(microstructure.get("atr_pct"), 0.0)

        profit_protect_trigger_pct = max(
            _AI_PROFIT_PROTECT_TRIGGER_PCT_MIN,
            round_trip_cost_pct,
            atr_pct * 0.8,
        )
        profit_protect_lock_pct = max(
            round_trip_cost_pct,
            one_way_cost_pct + _AI_PROFIT_PROTECT_LOCK_BUFFER_PCT,
        )
        profit_protect_lock_pct = min(
            profit_protect_lock_pct,
            max(_AI_PROFIT_PROTECT_LOCK_BUFFER_PCT, profit_protect_trigger_pct * 0.85),
        )

        partial_take_profit_trigger_pct = max(
            _AI_PARTIAL_TAKE_PROFIT_TRIGGER_PCT_MIN,
            profit_protect_trigger_pct * 1.6,
            atr_pct * 1.35,
        )
        post_partial_trailing_stop_pct = max(
            _AI_POST_PARTIAL_TRAILING_STOP_PCT_MIN,
            atr_pct * 0.9,
        )
        post_partial_trailing_stop_pct = min(
            post_partial_trailing_stop_pct,
            max(_AI_POST_PARTIAL_TRAILING_STOP_PCT_MIN, partial_take_profit_trigger_pct * 0.7),
        )

        outage_tight_trailing_stop_pct = max(
            _AI_OUTAGE_TIGHT_TRAILING_STOP_PCT_MIN,
            round_trip_cost_pct * 0.6,
        )
        outage_tight_trailing_stop_pct = min(
            outage_tight_trailing_stop_pct,
            max(_AI_OUTAGE_TIGHT_TRAILING_STOP_PCT_MIN, post_partial_trailing_stop_pct * 0.75),
        )

        return {
            "profit_management_profile": "cost_aware_dynamic",
            "profit_management_cost_basis_bps": float(round_trip_cost_pct * 10000.0),
            "profit_management_atr_pct": float(atr_pct),
            "profit_protect_enabled": True,
            "profit_protect_trigger_pct": float(profit_protect_trigger_pct),
            "profit_protect_lock_pct": float(profit_protect_lock_pct),
            "partial_take_profit_enabled": True,
            "partial_take_profit_trigger_pct": float(partial_take_profit_trigger_pct),
            "partial_take_profit_fraction": float(_AI_PARTIAL_TAKE_PROFIT_FRACTION),
            "post_partial_trailing_stop_pct": float(post_partial_trailing_stop_pct),
            "outage_protection_enabled": True,
            "outage_tight_trailing_stop_pct": float(outage_tight_trailing_stop_pct),
        }

    def _apply_learning_score_adjustments(
        self,
        *,
        row: Dict[str, Any],
        cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        adjusted = dict(row or {})
        learning_memory = cfg.get("learning_memory") if isinstance(cfg, dict) else {}
        adaptive_risk = dict((learning_memory or {}).get("adaptive_risk") or {})
        blocked_map = build_blocked_symbol_side_map(
            learning_memory,
            base_min_confidence=float(cfg.get("min_confidence") or 0.58),
        )
        symbol = normalize_symbol(adjusted.get("symbol"))
        direction = str(adjusted.get("direction") or "").strip().upper()
        pair_side = "long" if direction == "LONG" else ("short" if direction == "SHORT" else "")

        score = float(adjusted.get("score") or 0.0)
        tradable_now = bool(adjusted.get("tradable_now"))
        notes: List[str] = []

        if symbol and pair_side:
            blocked = blocked_map.get((symbol, pair_side))
            if blocked:
                score -= 0.35
                tradable_now = False
                notes.append(f"review cooldown {pair_side}")

        require_research = bool(adaptive_risk.get("require_research_for_new_entries"))
        research = adjusted.get("research") if isinstance(adjusted.get("research"), dict) else {}
        if require_research and not bool(research.get("candidate_id")) and not bool(adjusted.get("has_position")):
            score -= 0.12
            tradable_now = False
            notes.append("review requires research")

        if bool(adaptive_risk.get("avoid_new_entries_during_service_instability")) and not bool(adjusted.get("has_position")):
            score -= 0.10
            if pair_side:
                tradable_now = False
            notes.append("service instability")

        if notes:
            summary = str(adjusted.get("summary") or "").strip()
            extra = ", ".join(notes)
            adjusted["summary"] = f"{summary}; {extra}" if summary else extra
        adjusted["score"] = round(score, 6)
        adjusted["tradable_now"] = tradable_now
        return adjusted

    def _apply_learning_entry_guards(
        self,
        *,
        decision: Dict[str, Any],
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        updated = dict(decision or {})
        action = str(updated.get("action") or "").strip().lower()
        if action not in {"buy", "sell"}:
            return updated

        learning_memory = cfg.get("learning_memory") if isinstance(cfg, dict) else {}
        adaptive_risk = dict((learning_memory or {}).get("adaptive_risk") or {})
        blocked_map = build_blocked_symbol_side_map(
            learning_memory,
            base_min_confidence=float(cfg.get("min_confidence") or 0.58),
        )
        symbol = normalize_symbol(context_payload.get("symbol") or cfg.get("symbol"))
        side = "long" if action == "buy" else "short"
        blocked = blocked_map.get((symbol, side))
        if blocked:
            updated["action"] = "hold"
            updated["reason"] = f"review_cooldown({symbol}:{side})"
            return updated

        position = context_payload.get("position") if isinstance(context_payload, dict) else {}
        has_position = bool(str((position or {}).get("side") or "").strip().lower())
        research = context_payload.get("research_context") if isinstance(context_payload, dict) else {}
        if (
            bool(adaptive_risk.get("require_research_for_new_entries"))
            and not has_position
            and not bool((research or {}).get("available"))
        ):
            updated["action"] = "hold"
            updated["reason"] = "review_requires_research"
            return updated

        if (
            bool(adaptive_risk.get("avoid_new_entries_during_service_instability"))
            and not has_position
            and self._current_model_feedback_outage_duration_sec() > 0
        ):
            updated["action"] = "hold"
            updated["reason"] = "review_service_instability"
            return updated
        return updated

    async def _handle_market_data_outage(
        self,
        *,
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        learning_memory = cfg.get("learning_memory") if isinstance(cfg, dict) else {}
        adaptive_risk = dict((learning_memory or {}).get("adaptive_risk") or {})
        position = context_payload.get("position") if isinstance(context_payload, dict) else {}
        position_side = str((position or {}).get("side") or "").strip().lower()
        unrealized_pnl = float((position or {}).get("unrealized_pnl") or 0.0)
        fallback_reason = "no_price"

        if position_side in {"long", "short"}:
            if unrealized_pnl > 0:
                with contextlib.suppress(Exception):
                    protection = await execution_engine.tighten_profitable_position_protection(
                        exchange=str(cfg.get("exchange") or context_payload.get("exchange") or "binance"),
                        symbol=str(cfg.get("symbol") or context_payload.get("symbol") or "BTC/USDT"),
                        account_id=str(cfg.get("account_id") or "main"),
                        current_price=_safe_nonnegative_float((position or {}).get("current_price"), 0.0),
                        reason="market_data_unavailable",
                    )
                    if bool((protection or {}).get("applied")):
                        fallback_reason = "no_price;profit_protection_armed"
            elif bool(adaptive_risk.get("force_close_on_data_outage_losing_position")) and unrealized_pnl < 0:
                close_action = "close_long" if position_side == "long" else "close_short"
                return {
                    "action": close_action,
                    "confidence": 1.0,
                    "strength": 0.2,
                    "leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
                    "stop_loss_pct": float(cfg.get("default_stop_loss_pct") or 0.02),
                    "take_profit_pct": float(cfg.get("default_take_profit_pct") or 0.04),
                    "reason": f"no_price_exit_{position_side}",
                }
            else:
                fallback_reason = "no_price_with_position"

        return {
            "action": "hold",
            "confidence": 0.0,
            "strength": 0.1,
            "leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
            "stop_loss_pct": float(cfg.get("default_stop_loss_pct") or 0.02),
            "take_profit_pct": float(cfg.get("default_take_profit_pct") or 0.04),
            "reason": fallback_reason,
        }

    async def _build_context(self, cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], pd.DataFrame]:
        market_data = await self._load_market_data(cfg)
        last_price = await self._resolve_last_price(cfg, market_data)
        timeframe = str(cfg.get("timeframe") or "15m")
        timeframe_sec = _timeframe_to_seconds(timeframe)
        market_structure = self._build_market_structure_payload(
            market_data=market_data,
            timeframe_sec=timeframe_sec,
            last_price=float(last_price or 0.0),
        )
        market_returns = dict((market_structure.get("returns") or {}))

        close_series = pd.Series(dtype=float)
        if market_data is not None and not market_data.empty and "close" in market_data.columns:
            close_series = pd.to_numeric(market_data["close"], errors="coerce").dropna()

        returns = close_series.pct_change().dropna()
        annual_factor = max(1.0, (86400.0 * 365.0) / max(1.0, float(timeframe_sec)))
        realized_vol = 0.0
        if not returns.empty:
            realized_vol = float(returns.tail(120).std() * (annual_factor ** 0.5))

        agg_signal: Dict[str, Any] = {}
        try:
            agg = await signal_aggregator.aggregate(symbol=str(cfg["symbol"]), market_data=market_data)
            agg_signal = agg.to_dict() if hasattr(agg, "to_dict") else {}
        except Exception as exc:
            logger.debug(f"autonomous agent aggregate signal failed: {exc}")

        account_id = str(cfg.get("account_id") or "main")
        exchange = str(cfg.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        account_risk_base = await self._resolve_account_risk_base(cfg)
        position_payload = await self._resolve_position_payload(
            exchange=exchange,
            symbol=symbol,
            account_id=account_id,
        )
        position_payload = await self._annotate_position_payload(
            cfg=cfg,
            position_payload=position_payload,
            last_price=float(last_price or 0.0),
            account_risk_base=account_risk_base,
        )
        event_summary = await self._build_event_summary(symbol=symbol, timeframe_sec=timeframe_sec)
        account_risk = self._build_account_risk_payload(
            cfg=cfg,
            position_payload=position_payload,
            account_risk_base=account_risk_base,
            last_price=float(last_price or 0.0),
        )
        execution_cost = self._build_execution_cost_payload(
            cfg=cfg,
            market_structure=market_structure,
            account_risk=account_risk,
        )

        research_context = resolve_runtime_research_context(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "price": float(last_price or 0.0),
            "returns": {
                "r_15m": float(market_returns.get("r_15m") or 0.0),
                "r_1h": float(market_returns.get("r_1h") or 0.0),
                "r_4h": float(market_returns.get("r_4h") or 0.0),
                "r_24h": float(market_returns.get("r_24h") or 0.0),
            },
            "realized_vol_annualized": float(realized_vol),
            "bars": int(len(market_data) if market_data is not None else 0),
            "market_structure": market_structure,
            "aggregated_signal": agg_signal,
            "event_summary": event_summary,
            "position": position_payload,
            "account_risk": account_risk,
            "execution_cost": execution_cost,
            "research_context": research_context,
            "profile": dict(self._profile or _default_profile()),
            "learning_memory": dict(cfg.get("learning_memory") or self._learning_memory or {}),
            "trading_mode": execution_engine.get_trading_mode(),
        }, market_data

    def _build_prompt(self, cfg: Dict[str, Any], context_payload: Dict[str, Any]) -> Tuple[str, str]:
        system_prompt = (
            "You are an autonomous crypto trading agent. "
            "Return strict JSON only. Never output markdown."
        )
        user_payload = {
            "task": "Decide the next single trading action for current market context.",
            "output_schema": {
                "action": "buy|sell|hold|close_long|close_short",
                "confidence": "float in [0,1]",
                "strength": "float in [0.1,1.0]",
                "leverage": "must be 1.0",
                "stop_loss_pct": "float > 0, fraction not percent",
                "take_profit_pct": "float > 0, fraction not percent",
                "reason": "short reason <= 180 chars",
            },
            "hard_rules": [
                "If uncertain or data quality is low, choose hold.",
                "Never fabricate certainty.",
                "Use tighter risk when volatility is high.",
                "Leverage is fixed at 1x. Always return leverage=1.0.",
                "Same-side add-ons are allowed only while same_direction_exposure_ratio is below same_direction_exposure_limit_ratio.",
                "Use market_structure to judge trend, volatility, volume abnormality, and where price sits inside the recent range.",
                "Use aggregated_signal.components as decomposed priors; do not rely only on the top-level direction/confidence.",
                "Use event_summary only when event concentration, news_alpha_proxy, or dominant sentiment are meaningfully non-zero.",
                "Always respect account_risk, especially min_confidence, fixed_leverage, and same_direction_remaining_notional.",
                "Use execution_cost to avoid marginal trades whose expected edge is smaller than estimated fees and slippage.",
                "If research_context is available, treat its selected_candidate as the current research champion hypothesis unless real-time risk clearly invalidates it.",
                "Treat learning_memory.adaptive_risk as the realized-trading guardrail layer built from recent outcomes.",
                "If learning_memory blocks a symbol-side or requires research for fresh entries, default to hold rather than forcing a trade.",
            ],
            "runtime_constraints": {
                "min_confidence": cfg.get("effective_min_confidence", cfg.get("min_confidence")),
                "fixed_leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
                "default_stop_loss_pct": cfg.get("default_stop_loss_pct"),
                "default_take_profit_pct": cfg.get("default_take_profit_pct"),
                "agent_mode": cfg.get("mode"),
                "allow_live": cfg.get("allow_live"),
                "trading_mode": context_payload.get("trading_mode"),
                "same_direction_max_exposure_ratio": cfg.get("same_direction_max_exposure_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO),
                "entry_size_scale": cfg.get("entry_size_scale", 1.0),
            },
            "input": context_payload,
        }
        return system_prompt, json.dumps(user_payload, ensure_ascii=False)

    def _normalize_decision(self, raw: Dict[str, Any], cfg: Dict[str, Any], context_payload: Dict[str, Any]) -> Dict[str, Any]:
        action = _normalize_action(raw.get("action"))
        confidence = _coerce_float(raw.get("confidence", 0.0), 0.0, low=0.0, high=1.0)
        strength = _coerce_float(raw.get("strength", max(0.2, confidence)), max(0.2, confidence), low=0.1, high=1.0)
        leverage = _FIXED_AUTONOMOUS_AGENT_LEVERAGE
        stop_loss_pct = _coerce_float(
            raw.get("stop_loss_pct", cfg.get("default_stop_loss_pct", 0.02)),
            float(cfg.get("default_stop_loss_pct") or 0.02),
            low=0.001,
            high=0.5,
        )
        take_profit_pct = _coerce_float(
            raw.get("take_profit_pct", cfg.get("default_take_profit_pct", 0.04)),
            float(cfg.get("default_take_profit_pct") or 0.04),
            low=0.001,
            high=2.0,
        )
        if take_profit_pct <= stop_loss_pct:
            take_profit_pct = min(2.0, max(stop_loss_pct * 1.6, float(cfg.get("default_take_profit_pct") or 0.04)))
        reason = str(raw.get("reason") or "model_decision").strip()[:180] or "model_decision"

        min_conf = float(cfg.get("effective_min_confidence") or cfg.get("min_confidence") or 0.0)
        if action in {"buy", "sell"} and confidence < min_conf:
            action = "hold"
            reason = f"below_min_confidence({confidence:.3f}<{min_conf:.3f})"

        position = context_payload.get("position") if isinstance(context_payload, dict) else {}
        current_side = str((position or {}).get("side") or "").lower()
        same_direction_limit_ratio = _coerce_float(
            (position or {}).get("same_direction_exposure_limit_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO),
            _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
            low=0.0,
            high=1.0,
        )
        same_direction_exposure_ratio = _coerce_float(
            (position or {}).get("same_direction_exposure_ratio", 0.0),
            0.0,
            low=0.0,
            high=1000000.0,
        )
        position_cap_notional = _safe_nonnegative_float((position or {}).get("position_cap_notional"), 0.0)
        allow_same_direction_add = bool(
            position_cap_notional > 0
            and same_direction_limit_ratio > 0
            and same_direction_exposure_ratio + 1e-9 < same_direction_limit_ratio
        )
        if action == "close_long" and str((position or {}).get("side") or "").lower() != "long":
            action = "hold"
            reason = "no_long_position"
        if action == "close_short" and str((position or {}).get("side") or "").lower() != "short":
            action = "hold"
            reason = "no_short_position"
        if action == "buy" and current_side == "long":
            if not allow_same_direction_add:
                action = "hold"
                reason = (
                    f"existing_long_position_limit_reached({same_direction_exposure_ratio:.3f}>="
                    f"{same_direction_limit_ratio:.3f})"
                    if position_cap_notional > 0 and same_direction_limit_ratio > 0
                    else "existing_long_position"
                )
        if action == "sell" and current_side == "short":
            if not allow_same_direction_add:
                action = "hold"
                reason = (
                    f"existing_short_position_limit_reached({same_direction_exposure_ratio:.3f}>="
                    f"{same_direction_limit_ratio:.3f})"
                    if position_cap_notional > 0 and same_direction_limit_ratio > 0
                    else "existing_short_position"
                )

        return {
            "action": action,
            "confidence": confidence,
            "strength": strength,
            "leverage": leverage,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "reason": reason,
        }

    @staticmethod
    def _signal_type_from_action(action: str) -> Optional[SignalType]:
        if action == "buy":
            return SignalType.BUY
        if action == "sell":
            return SignalType.SELL
        if action == "close_long":
            return SignalType.CLOSE_LONG
        if action == "close_short":
            return SignalType.CLOSE_SHORT
        return None

    def _build_signal(
        self,
        *,
        decision: Dict[str, Any],
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
    ) -> Optional[Signal]:
        action = str(decision.get("action") or "hold")
        signal_type = self._signal_type_from_action(action)
        if signal_type is None:
            return None

        price = float(context_payload.get("price") or 0.0)
        stop_loss = None
        take_profit = None
        if action in {"buy", "sell"} and price > 0:
            sl_pct = float(decision.get("stop_loss_pct") or cfg.get("default_stop_loss_pct") or 0.02)
            tp_pct = float(decision.get("take_profit_pct") or cfg.get("default_take_profit_pct") or 0.04)
            if action == "buy":
                stop_loss = max(0.0, price * (1.0 - sl_pct))
                take_profit = max(0.0, price * (1.0 + tp_pct))
            else:
                stop_loss = max(0.0, price * (1.0 + sl_pct))
                take_profit = max(0.0, price * (1.0 - tp_pct))

        metadata = {
            "exchange": str(cfg.get("exchange") or "binance"),
            "account_id": str(cfg.get("account_id") or "main"),
            "leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
            "timeframe": str(cfg.get("timeframe") or ""),
            "source": "ai_autonomous_agent",
            "skip_live_decision_review": True,
            "agent_provider": str(cfg.get("provider") or ""),
            "agent_model": str(cfg.get("model") or ""),
            "agent_confidence": float(decision.get("confidence") or 0.0),
            "agent_reason": str(decision.get("reason") or ""),
            "review_effective_min_confidence": float(
                cfg.get("effective_min_confidence") or cfg.get("min_confidence") or 0.0
            ),
            "review_same_direction_limit_ratio": float(
                cfg.get("same_direction_max_exposure_ratio") or _SAME_DIRECTION_MAX_EXPOSURE_RATIO
            ),
            "review_entry_size_scale": float(cfg.get("entry_size_scale") or 1.0),
        }
        learning_memory = cfg.get("learning_memory") if isinstance(cfg, dict) else {}
        lessons = list((learning_memory or {}).get("lessons") or [])
        if lessons:
            metadata["review_lessons"] = lessons[:3]
        guardrails = list((learning_memory or {}).get("guardrails") or [])
        if guardrails:
            metadata["review_guardrails"] = guardrails[:3]
        metadata.update(
            self._build_trade_management_metadata(
                decision=decision,
                context_payload=context_payload,
            )
        )
        position = context_payload.get("position") if isinstance(context_payload, dict) else {}
        current_side = str((position or {}).get("side") or "").lower()
        same_direction_add = bool(
            (action == "buy" and current_side == "long")
            or (action == "sell" and current_side == "short")
        )
        if same_direction_add:
            metadata.update(
                {
                    "same_direction_max_exposure_ratio": _safe_nonnegative_float(
                        (position or {}).get("same_direction_exposure_limit_ratio"),
                        _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
                    ),
                    "same_direction_existing_notional": _safe_nonnegative_float(
                        (position or {}).get("position_notional"),
                        0.0,
                    ),
                    "same_direction_exposure_ratio": _safe_nonnegative_float(
                        (position or {}).get("same_direction_exposure_ratio"),
                        0.0,
                    ),
                    "same_direction_position_cap_notional": _safe_nonnegative_float(
                        (position or {}).get("position_cap_notional"),
                        0.0,
                    ),
                    "same_direction_remaining_notional": _safe_nonnegative_float(
                        (position or {}).get("same_direction_remaining_notional"),
                        0.0,
                    ),
                }
            )
        research_context = context_payload.get("research_context") if isinstance(context_payload, dict) else {}
        if isinstance(research_context, dict) and research_context.get("available"):
            selected_candidate = dict(research_context.get("selected_candidate") or {})
            champion_candidate = dict(research_context.get("research_champion") or {})
            metadata.update(
                {
                    "research_context_available": True,
                    "research_selection_reason": str(research_context.get("selection_reason") or ""),
                    "research_candidate_id": str(selected_candidate.get("candidate_id") or ""),
                    "research_proposal_id": str(selected_candidate.get("proposal_id") or ""),
                    "research_strategy": str(selected_candidate.get("strategy") or ""),
                    "research_score": _coerce_float(selected_candidate.get("score", 0.0), 0.0, low=0.0, high=1000000.0),
                    "research_status": str(selected_candidate.get("status") or ""),
                    "research_role": str(selected_candidate.get("search_role") or ""),
                    "research_champion_candidate_id": str(champion_candidate.get("candidate_id") or ""),
                    "research_champion_strategy": str(champion_candidate.get("strategy") or ""),
                }
            )
        return Signal(
            symbol=str(cfg.get("symbol") or "BTC/USDT"),
            signal_type=signal_type,
            price=float(price or 0.0),
            timestamp=_utc_now(),
            strategy_name=str(cfg.get("strategy_name") or "AI_AutonomousAgent"),
            strength=max(
                0.1,
                min(
                    1.0,
                    float(decision.get("strength") or 0.5) * float(cfg.get("entry_size_scale") or 1.0),
                ),
            ),
            quantity=None,
            stop_loss=stop_loss if stop_loss and stop_loss > 0 else None,
            take_profit=take_profit if take_profit and take_profit > 0 else None,
            metadata=metadata,
        )

    def _score_symbol_candidate(self, cfg: Dict[str, Any], context_payload: Dict[str, Any]) -> Dict[str, Any]:
        agg = dict(context_payload.get("aggregated_signal") or {})
        research_context = dict(context_payload.get("research_context") or {})
        selected_candidate = dict(research_context.get("selected_candidate") or {})
        position = dict(context_payload.get("position") or {})
        validation = dict(selected_candidate.get("validation") or {})
        validation_reasons = [
            str(item).strip()
            for item in (validation.get("reasons") or [])
            if str(item).strip()
        ]

        direction = str(agg.get("direction") or "FLAT").upper()
        confidence = _coerce_float(agg.get("confidence", 0.0), 0.0, low=0.0, high=1.0)
        blocked = bool(agg.get("blocked_by_risk"))
        risk_reason = str(agg.get("risk_reason") or "").strip()
        min_confidence = float(cfg.get("effective_min_confidence") or cfg.get("min_confidence") or 0.0)
        bars = max(0, int(context_payload.get("bars") or 0))
        lookback = max(1, int(cfg.get("lookback_bars") or 240))
        vol = abs(float(context_payload.get("realized_vol_annualized") or 0.0))
        promotion_target = str(selected_candidate.get("promotion_target") or "").strip().lower()
        candidate_status = str(selected_candidate.get("status") or "").strip().lower()
        position_side = str(position.get("side") or "").strip().lower()
        has_position = bool(position_side)
        position_source = str(position.get("source") or ("local" if has_position else "")).strip().lower()
        position_unrealized_pnl = float(position.get("unrealized_pnl") or 0.0)
        entry_price = _safe_nonnegative_float(position.get("entry_price"), 0.0)
        current_price = _safe_nonnegative_float(position.get("current_price"), 0.0)
        position_unrealized_pnl_pct = 0.0
        if has_position and entry_price > 0 and current_price > 0:
            if position_side == "short":
                position_unrealized_pnl_pct = float((entry_price - current_price) / entry_price)
            else:
                position_unrealized_pnl_pct = float((current_price - entry_price) / entry_price)

        score = confidence
        score += 0.18 if direction in {"LONG", "SHORT"} else -0.08
        if direction in {"LONG", "SHORT"} and confidence >= min_confidence:
            score += 0.18
        if blocked:
            score -= 0.30
        score += min(1.0, bars / float(lookback)) * 0.05
        score -= min(vol, 1.0) * 0.08
        if selected_candidate:
            score += 0.04
        if promotion_target == "paper" or candidate_status in {"paper_running", "paper_ready", "new"}:
            score -= 0.04
        if any("trade count too low" in item.lower() for item in validation_reasons):
            score -= 0.05
        if has_position:
            score += 0.03
        score = round(score, 6)

        tradable_now = bool(
            direction in {"LONG", "SHORT"}
            and not blocked
            and confidence >= min_confidence
            and float(context_payload.get("price") or 0.0) > 0
        )

        summary_parts: List[str] = [f"{direction} {confidence:.3f}"]
        if blocked and risk_reason:
            summary_parts.append(risk_reason)
        elif direction in {"LONG", "SHORT"} and confidence < min_confidence:
            summary_parts.append(f"below threshold {confidence:.3f} < {min_confidence:.3f}")
        if has_position:
            pnl_text = f"{position_unrealized_pnl_pct:+.2%}" if abs(position_unrealized_pnl_pct) > 1e-9 else f"{position_unrealized_pnl:+.4f}"
            summary_parts.append(f"holding {position_side} ({position_source or 'local'}) {pnl_text}")
        if candidate_status:
            summary_parts.append(f"research {candidate_status}")
        if any("trade count too low" in item.lower() for item in validation_reasons):
            summary_parts.append("research trade count low")

        row = {
            "symbol": str(context_payload.get("symbol") or cfg.get("symbol") or "BTC/USDT"),
            "price": float(context_payload.get("price") or 0.0),
            "direction": direction,
            "confidence": confidence,
            "score": score,
            "tradable_now": tradable_now,
            "blocked_by_risk": blocked,
            "risk_reason": risk_reason,
            "bars": bars,
            "realized_vol_annualized": vol,
            "threshold_gap": round(confidence - min_confidence, 6),
            "summary": "; ".join(part for part in summary_parts if part),
            "has_position": has_position,
            "position_side": position_side,
            "position_source": position_source,
            "position_unrealized_pnl": float(position_unrealized_pnl),
            "position_unrealized_pnl_pct": float(position_unrealized_pnl_pct),
            "research": {
                "candidate_id": str(selected_candidate.get("candidate_id") or ""),
                "strategy": str(selected_candidate.get("strategy") or ""),
                "status": str(selected_candidate.get("status") or ""),
                "promotion_target": str(selected_candidate.get("promotion_target") or ""),
                "validation_reasons": validation_reasons[:3],
            },
        }
        return self._apply_learning_score_adjustments(row=row, cfg=cfg)

    async def get_symbol_scan(self, *, limit: Optional[int] = None, force: bool = False) -> Dict[str, Any]:
        cfg = self._cfg_with_learning_overlays(self.get_runtime_config(), force_learning_refresh=force)
        symbol_mode = _normalize_symbol_mode(cfg.get("symbol_mode"))
        configured_symbol = _normalize_symbol_text(cfg.get("symbol") or "BTC/USDT") or "BTC/USDT"
        selection_top_n = _coerce_int(limit or cfg.get("selection_top_n") or 10, 10, low=3, high=20)
        default_universe = _DEFAULT_AUTO_UNIVERSE if symbol_mode == "auto" else [configured_symbol]
        universe_symbols = _normalize_symbol_list(cfg.get("universe_symbols"), default=default_universe, max_items=30)
        if symbol_mode != "auto":
            universe_symbols = [configured_symbol]
        else:
            tracked_symbols = await self._tracked_position_symbols(
                exchange=str(cfg.get("exchange") or "binance"),
                account_id=str(cfg.get("account_id") or "main"),
            )
            universe_symbols = _merge_symbol_sequence(tracked_symbols, [configured_symbol], universe_symbols, max_items=30)

        rows: List[Dict[str, Any]] = []
        for symbol in universe_symbols:
            local_cfg = dict(cfg)
            local_cfg["symbol"] = symbol
            try:
                context_payload, _ = await self._build_context(local_cfg)
                rows.append(self._score_symbol_candidate(local_cfg, context_payload))
            except Exception as exc:
                rows.append(
                    {
                        "symbol": symbol,
                        "price": 0.0,
                        "direction": "FLAT",
                        "confidence": 0.0,
                        "score": -1.0,
                        "tradable_now": False,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "bars": 0,
                        "realized_vol_annualized": 0.0,
                        "threshold_gap": round(0.0 - float(cfg.get("effective_min_confidence") or cfg.get("min_confidence") or 0.0), 6),
                        "summary": f"scan_error:{_format_exception_short(exc)}",
                        "has_position": False,
                        "position_side": "",
                        "position_source": "",
                        "position_unrealized_pnl": 0.0,
                        "position_unrealized_pnl_pct": 0.0,
                        "research": {
                            "candidate_id": "",
                            "strategy": "",
                            "status": "",
                            "promotion_target": "",
                            "validation_reasons": [],
                        },
                    }
                )

        rows = sorted(
            rows,
            key=lambda item: (
                1 if item.get("has_position") else 0,
                1 if item.get("tradable_now") else 0,
                float(item.get("score") or -999.0),
                abs(float(item.get("position_unrealized_pnl_pct") or 0.0)),
                float(item.get("confidence") or 0.0),
                1 if str(item.get("direction") or "") in {"LONG", "SHORT"} else 0,
            ),
            reverse=True,
        )

        selected_row = rows[0] if rows else {
            "symbol": configured_symbol,
            "price": 0.0,
            "direction": "FLAT",
            "confidence": 0.0,
            "score": 0.0,
            "tradable_now": False,
            "blocked_by_risk": False,
            "risk_reason": "",
            "bars": 0,
            "realized_vol_annualized": 0.0,
            "threshold_gap": 0.0,
            "summary": "no_candidates",
            "has_position": False,
            "position_side": "",
            "position_source": "",
            "position_unrealized_pnl": 0.0,
            "position_unrealized_pnl_pct": 0.0,
            "research": {
                "candidate_id": "",
                "strategy": "",
                "status": "",
                "promotion_target": "",
                "validation_reasons": [],
            },
        }
        if symbol_mode != "auto":
            selection_reason = "manual_symbol"
        elif bool(selected_row.get("has_position")):
            selection_reason = "existing_position_priority"
        else:
            selection_reason = "top_ranked_tradable_symbol" if bool(selected_row.get("tradable_now")) else "top_ranked_watchlist_symbol"

        for index, row in enumerate(rows, start=1):
            row["rank"] = index
            row["selected"] = bool(row.get("symbol") == selected_row.get("symbol"))

        payload = {
            "generated_at": _utc_now().isoformat(),
            "symbol_mode": symbol_mode,
            "configured_symbol": configured_symbol,
            "selected_symbol": str(selected_row.get("symbol") or configured_symbol),
            "selection_reason": selection_reason,
            "candidate_count": len(rows),
            "top_n": selection_top_n,
            "top_candidates": rows[:selection_top_n],
        }
        if force or symbol_mode == "manual" or rows:
            self._last_symbol_scan = payload
        return payload

    def _build_decision_diagnostics(
        self,
        *,
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
        raw_decision: Optional[Dict[str, Any]],
        raw_decision_source: str,
        decision: Dict[str, Any],
        execution: Dict[str, Any],
        selection: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        items: List[Dict[str, Any]] = []

        def add_item(code: str, label: str, detail: str = "", tone: str = "warn", priority: int = 50) -> None:
            items.append(
                {
                    "code": code,
                    "label": label,
                    "detail": detail,
                    "tone": tone,
                    "priority": int(priority),
                }
            )

        action = str(decision.get("action") or "hold").strip().lower() or "hold"
        decision_reason = str(decision.get("reason") or "").strip()
        execution_reason = str(execution.get("reason") or "").strip()
        min_confidence = float(cfg.get("min_confidence") or 0.0)
        model_output = _build_model_output_debug(raw_decision, decision, source=raw_decision_source)
        model_feedback_issue = _describe_model_feedback_issue(decision_reason) if decision_reason.startswith("model_error:") else {
            "kind": None,
            "http_status": None,
            "label": "",
            "detail": "",
            "code": "",
            "raw_error": "",
        }

        if decision_reason.startswith("model_error:"):
            add_item(
                str(model_feedback_issue.get("code") or "model_error"),
                str(model_feedback_issue.get("label") or "模型接口异常"),
                str(model_feedback_issue.get("detail") or decision_reason.replace("model_error:", "", 1)),
                "danger",
                5,
            )
        elif decision_reason == "no_price":
            add_item("no_price", "当前价格不可用", "缺少可用行情，代理只能观望", "danger", 8)
        elif decision_reason.startswith("below_min_confidence"):
            add_item(
                "below_min_confidence",
                "模型信号低于开仓阈值",
                f"当前最小阈值 {min_confidence:.3f}",
                "warn",
                15,
            )
        elif decision_reason.startswith("cooldown("):
            add_item("local_cooldown", "代理本地下单冷却中", decision_reason, "warn", 18)
        elif decision_reason == "no_long_position":
            add_item("no_long_position", "没有多头可平", "模型想平多，但当前没有多头仓位", "warn", 20)
        elif decision_reason == "no_short_position":
            add_item("no_short_position", "没有空头可平", "模型想平空，但当前没有空头仓位", "warn", 20)

        agg = dict(context_payload.get("aggregated_signal") or {})
        agg_direction = str(agg.get("direction") or "FLAT").upper()
        agg_confidence = _coerce_float(agg.get("confidence", 0.0), 0.0, low=0.0, high=1.0)
        agg_risk_reason = str(agg.get("risk_reason") or "").strip()
        if bool(agg.get("blocked_by_risk")):
            add_item("aggregated_risk_blocked", "聚合信号被风险门拦截", agg_risk_reason or "risk gate blocked", "warn", 25)
        if agg_direction == "FLAT":
            add_item("aggregated_signal_flat", "聚合信号为空仓", f"聚合置信度 {agg_confidence:.3f}", "info", 40)
        elif agg_confidence < min_confidence:
            add_item(
                "aggregated_signal_below_threshold",
                "聚合信号低于阈值",
                f"{agg_direction} {agg_confidence:.3f} < {min_confidence:.3f}",
                "info",
                30,
            )

        execution_cost = dict(context_payload.get("execution_cost") or {})
        research_context = dict(context_payload.get("research_context") or {})
        selected_candidate = dict(research_context.get("selected_candidate") or {})
        validation = dict(selected_candidate.get("validation") or {})
        validation_reasons = [
            str(item).strip()
            for item in (validation.get("reasons") or [])
            if str(item).strip()
        ]
        candidate_status = str(selected_candidate.get("status") or "").strip().lower()
        promotion_target = str(selected_candidate.get("promotion_target") or "").strip().lower()
        if selected_candidate and (candidate_status in {"paper_running", "paper_ready", "new"} or promotion_target == "paper"):
            add_item(
                "research_not_live_ready",
                "研究候选仍偏纸盘阶段",
                f"{selected_candidate.get('strategy') or '--'} / {selected_candidate.get('status') or '--'}",
                "info",
                45,
            )
        if any("trade count too low" in item.lower() for item in validation_reasons):
            add_item("research_low_trade_count", "研究候选样本过少", validation_reasons[0], "info", 48)

        if execution_reason == "shadow_mode":
            add_item("shadow_mode", "当前只提示不执行", "运行模式是 shadow", "warn", 10)
        elif execution_reason == "live_mode_blocked":
            add_item("live_mode_blocked", "实盘执行被禁止", "交易引擎在 live，但 agent 未允许 live", "danger", 9)
        elif execution_reason == "submit_rejected":
            add_item("submit_rejected", "执行引擎拒绝了信号", "submit_signal returned false", "danger", 12)

        if model_output.get("source") == "provider" and bool(model_output.get("action_changed")):
            raw_action = str(model_output.get("raw_action") or "--")
            normalized_action = str(model_output.get("normalized_action") or "--")
            normalized_reason = str(model_output.get("normalized_reason") or "").strip()
            detail = f"{raw_action} -> {normalized_action}"
            if normalized_reason:
                detail = f"{detail} | {normalized_reason}"
            add_item("model_action_rewritten", "模型原始动作被本地规则改写", detail, "warn", 35)

        if not items and action == "hold":
            add_item("model_hold", "模型主动选择观望", decision_reason or "no explicit hold reason", "info", 60)

        items = sorted(items, key=lambda item: int(item.get("priority") or 99))
        primary = items[0] if items else {
            "code": "none",
            "label": "无结构化原因",
            "detail": "",
            "tone": "info",
            "priority": 99,
        }

        summary_parts = [str(primary.get("label") or "")]
        if primary.get("detail"):
            summary_parts.append(str(primary.get("detail") or ""))
        summary = " | ".join(part for part in summary_parts if part)

        selected_symbol = str((selection or {}).get("selected_symbol") or cfg.get("symbol") or "")
        configured_symbol = str((selection or {}).get("configured_symbol") or cfg.get("symbol") or "")

        return {
            "outcome": "submitted" if bool(execution.get("submitted")) else ("hold" if action == "hold" else "blocked"),
            "primary": primary,
            "summary": summary,
            "items": items,
            "action": action,
            "decision_reason_raw": decision_reason,
            "execution_reason": execution_reason,
            "symbol_mode": str(cfg.get("symbol_mode") or "manual"),
            "configured_symbol": configured_symbol,
            "selected_symbol": selected_symbol,
            "aggregated_signal": {
                "direction": agg_direction,
                "confidence": agg_confidence,
                "blocked_by_risk": bool(agg.get("blocked_by_risk")),
                "risk_reason": agg_risk_reason,
            },
            "execution_cost": {
                "fee_bps": _safe_nonnegative_float(execution_cost.get("fee_bps"), 0.0),
                "estimated_slippage_bps": _safe_nonnegative_float(execution_cost.get("estimated_slippage_bps"), 0.0),
                "estimated_one_way_cost_bps": _safe_nonnegative_float(
                    execution_cost.get("estimated_one_way_cost_bps"),
                    0.0,
                ),
                "estimated_round_trip_cost_bps": _safe_nonnegative_float(
                    execution_cost.get("estimated_round_trip_cost_bps"),
                    0.0,
                ),
                "notional_reference": _safe_nonnegative_float(execution_cost.get("notional_reference"), 0.0),
                "estimated_one_way_cost_usd_at_reference": _safe_nonnegative_float(
                    execution_cost.get("estimated_one_way_cost_usd_at_reference"),
                    0.0,
                ),
                "estimated_round_trip_cost_usd_at_reference": _safe_nonnegative_float(
                    execution_cost.get("estimated_round_trip_cost_usd_at_reference"),
                    0.0,
                ),
            },
            "research": {
                "candidate_id": str(selected_candidate.get("candidate_id") or ""),
                "strategy": str(selected_candidate.get("strategy") or ""),
                "status": str(selected_candidate.get("status") or ""),
                "promotion_target": str(selected_candidate.get("promotion_target") or ""),
                "validation_reasons": validation_reasons[:3],
            },
            "learning_memory": {
                "effective_min_confidence": _safe_nonnegative_float(
                    cfg.get("effective_min_confidence", cfg.get("min_confidence")),
                    0.0,
                ),
                "same_direction_max_exposure_ratio": _safe_nonnegative_float(
                    cfg.get("same_direction_max_exposure_ratio"),
                    _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
                ),
                "entry_size_scale": _safe_nonnegative_float(cfg.get("entry_size_scale"), 1.0),
                "guardrails": list(((cfg.get("learning_memory") or {}).get("guardrails") or []))[:4],
                "blocked_symbol_sides": list(((cfg.get("learning_memory") or {}).get("blocked_symbol_sides") or []))[:4],
                "lessons": list(((cfg.get("learning_memory") or {}).get("lessons") or []))[:4],
            },
            "model_feedback": {
                "kind": model_feedback_issue.get("kind"),
                "http_status": model_feedback_issue.get("http_status"),
                "label": model_feedback_issue.get("label"),
                "detail": model_feedback_issue.get("detail"),
                "raw_error": model_feedback_issue.get("raw_error"),
                "guard": self._model_feedback_guard_status(),
            },
            "model_output": model_output,
        }

    def _load_overlay(self) -> None:
        """Load persisted agent config from JSON overlay on startup."""
        try:
            if self._overlay_path.exists():
                raw = self._overlay_path.read_text(encoding="utf-8")
                data = json.loads(raw)
                if isinstance(data, dict):
                    safe = {k: v for k, v in data.items() if k in _AGENT_PERSISTABLE_KEYS}
                    self._override.update(safe)
                    logger.info(f"autonomous_agent: loaded {len(safe)} persisted config keys")
        except Exception as exc:
            logger.warning(f"autonomous_agent: failed to load overlay (using defaults): {exc}")

    def _save_overlay(self) -> None:
        """Atomically persist current _override to JSON overlay."""
        try:
            self._overlay_path.parent.mkdir(parents=True, exist_ok=True)
            safe = {k: v for k, v in self._override.items() if k in _AGENT_PERSISTABLE_KEYS}
            tmp = self._overlay_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(safe, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._overlay_path)
        except Exception as exc:
            logger.warning(f"autonomous_agent: failed to save overlay: {exc}")

    def _load_profile(self) -> Dict[str, Any]:
        try:
            if not self._profile_path.exists():
                return _default_profile()
            raw = json.loads(self._profile_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return _default_profile()
            merged = _default_profile()
            merged.update(raw)
            if not isinstance(merged.get("action_counts"), dict):
                merged["action_counts"] = {}
            return merged
        except Exception as exc:
            logger.debug(f"load autonomous profile failed: {exc}")
            return _default_profile()

    def _save_profile(self) -> None:
        try:
            self._cache_root.mkdir(parents=True, exist_ok=True)
            tmp = self._profile_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._profile, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._profile_path)
        except Exception as exc:
            logger.warning(f"save autonomous profile failed: {exc}")

    def _load_learning_memory(self) -> Dict[str, Any]:
        base_min_confidence = float(getattr(settings, "AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE", 0.58) or 0.58)
        try:
            if not self._learning_memory_path.exists():
                return default_learning_memory(base_min_confidence=base_min_confidence)
            raw = json.loads(self._learning_memory_path.read_text(encoding="utf-8"))
            return coerce_learning_memory(raw, base_min_confidence=base_min_confidence)
        except Exception as exc:
            logger.debug(f"load autonomous learning memory failed: {exc}")
            return default_learning_memory(base_min_confidence=base_min_confidence)

    def _save_learning_memory(self) -> None:
        try:
            self._cache_root.mkdir(parents=True, exist_ok=True)
            tmp = self._learning_memory_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._learning_memory, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._learning_memory_path)
        except Exception as exc:
            logger.warning(f"save autonomous learning memory failed: {exc}")

    def _positions_for_learning_memory(self, strategy_name: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        target_strategy = str(strategy_name or "").strip()
        for position in position_manager.get_all_positions():
            try:
                payload = position.to_dict() if hasattr(position, "to_dict") else dict(position)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            strategy = str(payload.get("strategy") or "").strip()
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            source = str(metadata.get("source") or "").strip().lower()
            if target_strategy and strategy != target_strategy and source != "ai_autonomous_agent":
                continue
            rows.append(payload)
        return rows

    def _refresh_learning_memory(self, *, cfg: Optional[Dict[str, Any]] = None, force: bool = False) -> Dict[str, Any]:
        runtime_cfg = dict(cfg or self.get_runtime_config())
        now_ts = time.time()
        refresh_ttl_sec = 300.0
        if (
            not force
            and self._last_learning_refresh_at is not None
            and now_ts - float(self._last_learning_refresh_at) < refresh_ttl_sec
        ):
            return dict(self._learning_memory or {})

        strategy_name = str(runtime_cfg.get("strategy_name") or "AI_AutonomousAgent")
        journal_rows = self.read_journal(limit=800)
        live_review = execution_engine.get_live_trade_review(
            limit=500,
            strategy=strategy_name,
            hours=24 * 14,
        )
        positions = self._positions_for_learning_memory(strategy_name)
        memory = build_learning_memory(
            journal_rows=journal_rows,
            live_review=live_review,
            positions=positions,
            base_min_confidence=float(runtime_cfg.get("min_confidence") or 0.58),
        )
        self._learning_memory = coerce_learning_memory(
            memory,
            base_min_confidence=float(runtime_cfg.get("min_confidence") or 0.58),
        )
        self._last_learning_refresh_at = now_ts
        self._save_learning_memory()
        return dict(self._learning_memory)

    def get_learning_memory(self, *, force: bool = False) -> Dict[str, Any]:
        return self._refresh_learning_memory(force=force)

    def _cfg_with_learning_overlays(self, cfg: Dict[str, Any], *, force_learning_refresh: bool = False) -> Dict[str, Any]:
        base_cfg = dict(cfg or {})
        learning_memory = self._refresh_learning_memory(cfg=base_cfg, force=force_learning_refresh)
        adaptive_risk = dict(learning_memory.get("adaptive_risk") or {})
        effective_min_confidence = _coerce_float(
            adaptive_risk.get("effective_min_confidence", base_cfg.get("min_confidence", 0.58)),
            float(base_cfg.get("min_confidence") or 0.58),
            low=0.0,
            high=1.0,
        )
        same_direction_ratio = _coerce_float(
            adaptive_risk.get("same_direction_max_exposure_ratio", _SAME_DIRECTION_MAX_EXPOSURE_RATIO),
            _SAME_DIRECTION_MAX_EXPOSURE_RATIO,
            low=0.2,
            high=1.0,
        )
        entry_size_scale = _coerce_float(
            adaptive_risk.get("entry_size_scale", 1.0),
            1.0,
            low=0.25,
            high=1.0,
        )
        base_cfg["learning_memory"] = learning_memory
        base_cfg["effective_min_confidence"] = float(max(float(base_cfg.get("min_confidence") or 0.0), effective_min_confidence))
        base_cfg["same_direction_max_exposure_ratio"] = float(same_direction_ratio)
        base_cfg["entry_size_scale"] = float(entry_size_scale)
        return base_cfg

    def _update_profile(self, decision: Dict[str, Any], *, submitted: bool) -> None:
        profile = dict(self._profile or _default_profile())
        action = str(decision.get("action") or "hold")
        profile["decision_count"] = int(profile.get("decision_count") or 0) + 1
        if submitted:
            profile["executed_count"] = int(profile.get("executed_count") or 0) + 1
        actions = dict(profile.get("action_counts") or {})
        actions[action] = int(actions.get(action) or 0) + 1
        profile["action_counts"] = actions

        n = max(1, int(profile.get("decision_count") or 1))
        for key, source_key in (
            ("avg_confidence", "confidence"),
            ("avg_strength", "strength"),
            ("avg_leverage", "leverage"),
            ("avg_stop_loss_pct", "stop_loss_pct"),
            ("avg_take_profit_pct", "take_profit_pct"),
        ):
            old_v = float(profile.get(key) or 0.0)
            new_v = float(decision.get(source_key) or 0.0)
            profile[key] = ((old_v * (n - 1)) + new_v) / n
        profile["updated_at"] = _utc_now().isoformat()
        self._profile = profile
        self._save_profile()

    def _append_journal(self, payload: Dict[str, Any]) -> None:
        try:
            self._cache_root.mkdir(parents=True, exist_ok=True)
            with self._journal_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.warning(f"append autonomous journal failed: {exc}")

    def read_journal(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        size = max(1, min(int(limit or 50), 500))
        if not self._journal_path.exists():
            return rows
        try:
            lines = self._journal_path.read_text(encoding="utf-8").splitlines()
            for line in lines[-size:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    if isinstance(item, dict):
                        rows.append(item)
                except Exception:
                    continue
        except Exception as exc:
            logger.debug(f"read autonomous journal failed: {exc}")
        return rows

    async def run_once(self, *, trigger: str = "manual", force: bool = False) -> Dict[str, Any]:
        cfg = self._cfg_with_learning_overlays(
            self.get_runtime_config(),
            force_learning_refresh=bool(force),
        )
        if not bool(cfg.get("enabled")) and not force:
            result = {
                "request_id": str(uuid.uuid4())[:8],
                "skipped": True,
                "reason": "agent_disabled",
                "rejection_reason": "agent_disabled",
                "execution_allowed": False,
                "trigger": trigger,
                "timestamp": _utc_now().isoformat(),
            }
            self._last_run_at = result["timestamp"]
            self._last_decision = {"action": "hold", "reason": "agent_disabled"}
            self._last_execution = {"submitted": False, "reason": "agent_disabled"}
            self._last_research_context = None
            self._last_diagnostics = {
                "outcome": "blocked",
                "primary": {
                    "code": "agent_disabled",
                    "label": "代理未启用",
                    "detail": "enabled=false",
                    "tone": "danger",
                    "priority": 1,
                },
                "summary": "代理未启用 | enabled=false",
                "items": [
                    {
                        "code": "agent_disabled",
                        "label": "代理未启用",
                        "detail": "enabled=false",
                        "tone": "danger",
                        "priority": 1,
                    }
                ],
                "action": "hold",
                "decision_reason_raw": "agent_disabled",
                "execution_reason": "agent_disabled",
                "symbol_mode": str(cfg.get("symbol_mode") or "manual"),
                "configured_symbol": str(cfg.get("symbol") or ""),
                "selected_symbol": str(cfg.get("symbol") or ""),
                "aggregated_signal": {
                    "direction": "FLAT",
                    "confidence": 0.0,
                    "blocked_by_risk": False,
                    "risk_reason": "",
                },
                "execution_cost": {
                    "fee_bps": 0.0,
                    "estimated_slippage_bps": 0.0,
                    "estimated_one_way_cost_bps": 0.0,
                    "estimated_round_trip_cost_bps": 0.0,
                    "notional_reference": 0.0,
                    "estimated_one_way_cost_usd_at_reference": 0.0,
                    "estimated_round_trip_cost_usd_at_reference": 0.0,
                },
                "research": {
                    "candidate_id": "",
                    "strategy": "",
                    "status": "",
                    "promotion_target": "",
                    "validation_reasons": [],
                },
                "model_output": _build_model_output_debug(
                    {"action": "hold", "reason": "agent_disabled"},
                    {"action": "hold", "reason": "agent_disabled"},
                    source="synthetic",
                ),
            }
            return result

        started = time.perf_counter()
        selection = await self.get_symbol_scan(limit=int(cfg.get("selection_top_n") or 10), force=True)
        effective_cfg = dict(cfg)
        effective_cfg["symbol"] = str(selection.get("selected_symbol") or cfg.get("symbol") or "BTC/USDT")
        context_payload, market_data = await self._build_context(effective_cfg)
        research_context = context_payload.get("research_context") if isinstance(context_payload, dict) else {}
        raw_decision_source = "synthetic"
        if float(context_payload.get("price") or 0.0) <= 0:
            raw_decision = await self._handle_market_data_outage(
                cfg=effective_cfg,
                context_payload=context_payload,
            )
            decision = self._normalize_decision(raw_decision, effective_cfg, context_payload)
        else:
            provider = str(effective_cfg.get("provider") or "codex")
            model = str(effective_cfg.get("model") or self._provider_model(provider))
            system_prompt, user_prompt = self._build_prompt(effective_cfg, context_payload)
            raw_decision: Dict[str, Any]
            try:
                raw_decision = await asyncio.wait_for(
                    self._call_provider(
                        provider=provider,
                        model=model,
                        timeout_ms=int(effective_cfg.get("timeout_ms") or 12000),
                        max_tokens=int(effective_cfg.get("max_tokens") or 420),
                        temperature=float(effective_cfg.get("temperature") or 0.15),
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                    ),
                    timeout=float(_MODEL_FEEDBACK_HARD_TIMEOUT_SEC),
                )
                raw_decision_source = "provider"
                self._record_model_feedback_success()
            except Exception as exc:
                normalized_exc: Exception = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
                if isinstance(exc, asyncio.TimeoutError) and "guard_timeout" not in str(exc or "").lower():
                    normalized_exc = TimeoutError(
                        f"model_feedback_guard_timeout({int(_MODEL_FEEDBACK_HARD_TIMEOUT_SEC)}s)"
                    )
                model_feedback_issue = _describe_model_feedback_issue(normalized_exc)
                logger.warning(
                    "autonomous agent model decision failed "
                    f"kind={model_feedback_issue.get('kind') or 'unknown'} "
                    f"http_status={model_feedback_issue.get('http_status') or 'na'} "
                    f"provider={provider} model={model} symbol={effective_cfg.get('symbol')} "
                    f"base_url={self._provider_base_url(provider)} "
                    f"error={model_feedback_issue.get('raw_error') or _format_exception_short(normalized_exc)}"
                )
                failure = self._record_model_feedback_failure(normalized_exc)
                outage_duration_sec = self._current_model_feedback_outage_duration_sec()
                outage_protection_result: Optional[Dict[str, Any]] = None
                forced_outage_close = False
                if outage_duration_sec >= float(_MODEL_FEEDBACK_OUTAGE_ALERT_SEC):
                    outage_protection_result = await self._protect_profitable_local_position_during_model_outage(
                        cfg=effective_cfg,
                        context_payload=context_payload,
                        outage_duration_sec=outage_duration_sec,
                        model_feedback_issue=model_feedback_issue,
                    )
                    position_payload = context_payload.get("position") if isinstance(context_payload, dict) else {}
                    position_side = str((position_payload or {}).get("side") or "").strip().lower()
                    unrealized_pnl = float((position_payload or {}).get("unrealized_pnl") or 0.0)
                    adaptive_risk = dict((effective_cfg.get("learning_memory") or {}).get("adaptive_risk") or {})
                    if (
                        position_side in {"long", "short"}
                        and unrealized_pnl < 0
                        and bool(adaptive_risk.get("force_close_on_data_outage_losing_position"))
                    ):
                        raw_decision = {
                            "action": "close_long" if position_side == "long" else "close_short",
                            "confidence": 1.0,
                            "strength": 0.2,
                            "leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
                            "stop_loss_pct": effective_cfg.get("default_stop_loss_pct"),
                            "take_profit_pct": effective_cfg.get("default_take_profit_pct"),
                            "reason": f"model_outage_exit_{position_side}",
                        }
                        raw_decision_source = "fallback"
                        forced_outage_close = True
                if failure:
                    await self._send_model_feedback_outage_alert(
                        provider=provider,
                        model=model,
                        cfg=effective_cfg,
                        selection=selection,
                        context_payload=context_payload,
                        failure=failure,
                    )
                if not forced_outage_close:
                    fallback_reason = f"model_error:{_format_exception_short(normalized_exc)}"
                    if bool((outage_protection_result or {}).get("applied")):
                        fallback_reason = f"{fallback_reason};profit_protection_armed"
                    raw_decision = {
                        "action": "hold",
                        "confidence": 0.0,
                        "strength": 0.1,
                        "leverage": _FIXED_AUTONOMOUS_AGENT_LEVERAGE,
                        "stop_loss_pct": effective_cfg.get("default_stop_loss_pct"),
                        "take_profit_pct": effective_cfg.get("default_take_profit_pct"),
                        "reason": fallback_reason,
                    }
                    raw_decision_source = "fallback"
            decision = self._normalize_decision(raw_decision, effective_cfg, context_payload)

        decision = self._apply_learning_entry_guards(
            decision=decision,
            cfg=effective_cfg,
            context_payload=context_payload,
        )

        cooldown_sec = int(effective_cfg.get("cooldown_sec") or 0)
        if (
            decision["action"] in {"buy", "sell", "close_long", "close_short"}
            and cooldown_sec > 0
            and self._last_submit_at is not None
        ):
            elapsed = time.time() - float(self._last_submit_at)
            if elapsed < cooldown_sec:
                decision["action"] = "hold"
                decision["reason"] = f"cooldown({elapsed:.1f}s<{cooldown_sec}s)"

        signal = self._build_signal(decision=decision, cfg=effective_cfg, context_payload=context_payload)
        execution = {
            "mode": str(effective_cfg.get("mode") or "shadow"),
            "submitted": False,
            "reason": "hold",
            "signal": signal.to_dict() if signal is not None else None,
        }
        if signal is not None:
            if str(effective_cfg.get("mode") or "shadow") != "execute":
                execution["reason"] = "shadow_mode"
            else:
                trading_mode = execution_engine.get_trading_mode()
                if trading_mode == "live" and not bool(effective_cfg.get("allow_live")):
                    execution["reason"] = "live_mode_blocked"
                else:
                    accepted = await execution_engine.submit_signal(signal)
                    execution["submitted"] = bool(accepted)
                    execution["reason"] = "submitted" if accepted else "submit_rejected"
                    if accepted:
                        self._submitted_count += 1
                        self._last_submit_at = time.time()

        diagnostics = self._build_decision_diagnostics(
            cfg=effective_cfg,
            context_payload=context_payload,
            raw_decision=raw_decision,
            raw_decision_source=raw_decision_source,
            decision=decision,
            execution=execution,
            selection=selection,
        )

        latency_ms = int((time.perf_counter() - started) * 1000)
        now_iso = _utc_now().isoformat()
        # Derive rejection_reason for easy observability:
        #   "none" if submitted, otherwise the execution.reason string
        exec_reason = str(execution.get("reason") or "hold")
        rejection_reason = None if execution.get("submitted") else exec_reason
        journal_row = {
            "request_id": str(uuid.uuid4())[:8],
            "timestamp": now_iso,
            "trigger": str(trigger or "manual"),
            "latency_ms": latency_ms,
            "execution_allowed": bool(execution.get("submitted")),
            "rejection_reason": rejection_reason,
            "config": {
                "provider": effective_cfg.get("provider"),
                "model": effective_cfg.get("model"),
                "mode": effective_cfg.get("mode"),
                "exchange": effective_cfg.get("exchange"),
                "symbol_mode": cfg.get("symbol_mode"),
                "configured_symbol": cfg.get("symbol"),
                "symbol": effective_cfg.get("symbol"),
                "universe_size": len(cfg.get("universe_symbols") or []),
                "timeframe": effective_cfg.get("timeframe"),
                "allow_live": effective_cfg.get("allow_live"),
            },
            "context": {
                "price": context_payload.get("price"),
                "bars": context_payload.get("bars"),
                "returns": context_payload.get("returns"),
                "vol": context_payload.get("realized_vol_annualized"),
                "market_structure": context_payload.get("market_structure"),
                "aggregated_signal": context_payload.get("aggregated_signal"),
                "event_summary": context_payload.get("event_summary"),
                "position": context_payload.get("position"),
                "account_risk": context_payload.get("account_risk"),
                "execution_cost": context_payload.get("execution_cost"),
                "research_context": research_context,
            },
            "selection": selection,
            "decision": decision,
            "diagnostics": diagnostics,
            "execution": execution,
        }
        self._append_journal(journal_row)
        self._update_profile(decision, submitted=bool(execution.get("submitted")))

        self._last_run_at = now_iso
        self._last_error = None
        self._last_decision = decision
        self._last_execution = execution
        self._last_research_context = research_context if isinstance(research_context, dict) else None
        self._last_diagnostics = diagnostics
        self._last_symbol_scan = selection
        self._tick_count += 1
        if bool(execution.get("submitted")):
            self._refresh_learning_memory(cfg=effective_cfg, force=True)

        return {
            "timestamp": now_iso,
            "trigger": str(trigger or "manual"),
            "latency_ms": latency_ms,
            "market_bars": int(len(market_data) if market_data is not None else 0),
            "effective_symbol": effective_cfg.get("symbol"),
            "selection": selection,
            "decision": decision,
            "diagnostics": diagnostics,
            "execution": execution,
            "status": self.get_status(),
        }


autonomous_trading_agent = AutonomousTradingAgent()
