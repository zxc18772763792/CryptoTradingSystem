"""FastAPI application entry."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from config.env_utils import env_bool as _env_bool
from config.env_utils import env_float as _env_float
from config.env_utils import env_int as _env_int
from config.env_utils import sync_settings_to_environ
from config.settings import settings
from core.ai.autonomous_agent import autonomous_trading_agent

_MODEL_ENV_FIELDS = (
    "ZHIPU_API_KEY",
    "ZHIPU_BASE_URL",
    "ZHIPU_MODEL",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_BACKUP_API_KEY",
    "OPENAI_BACKUP_BASE_URL",
    "OPENAI_MODEL",
    "NEWS_LLM_PROVIDER",
    "NEWS_LLM_API_KEY",
    "NEWS_LLM_BASE_URL",
    "NEWS_LLM_MODEL",
    "NEWS_LLM_FORCE_CHAT_COMPLETIONS",
)

# Sync model settings into environment variables for modules that still read os.environ.
sync_settings_to_environ(settings, _MODEL_ENV_FIELDS)

from core.data import data_storage, second_level_backfill_manager
from core.exchanges import exchange_manager
from core.marketdata.hub import market_data_hub

from core.notifications import notification_manager
from core.ops.service import create_router as create_ops_router, initialize_ops_runtime, shutdown_ops_runtime
from core.realtime import event_bus
from core.runtime import RuntimeTaskSupervisor, runtime_bootstrap, runtime_state
from core.strategies import (
    restore_strategies_from_db,
    strategy_health_monitor,
    strategy_manager,
)
from core.trading import account_manager, execution_engine, order_manager, position_manager
from web.asset_versions import static_asset_url
from web.api import ai_research
from web.api import ml
from web.api.auth import (
    _has_valid_local_ui_session,
    require_sensitive_ops_permissions,
    set_local_ui_session_cookie,
)
from web.startup_mode import StartupModeDecision, resolve_startup_trading_mode

_AUTO_SYNC_SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "BNB/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
]
_AUTO_SYNC_PRIMARY_EXCHANGE = "binance"
_AUTO_SYNC_SECONDARY_EXCHANGE = "gate"
_AUTO_SYNC_TIMEFRAMES = ["10s", "1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]


_NEWS_PULL_INTERVAL_SEC = max(20, _env_int("NEWS_PULL_INTERVAL_SEC", 60))
_NEWS_PULL_SINCE_MINUTES = max(30, _env_int("NEWS_PULL_SINCE_MINUTES", 180))
_NEWS_PULL_MAX_RECORDS = max(20, _env_int("NEWS_PULL_MAX_RECORDS", 80))
_NEWS_STARTUP_DELAY_SEC = max(0, _env_int("NEWS_STARTUP_DELAY_SEC", 0))
_NEWS_BACKGROUND_ENABLED = _env_bool("NEWS_BACKGROUND_ENABLED", True)
_NEWS_LLM_INTERVAL_SEC = max(15, _env_int("NEWS_LLM_INTERVAL_SEC", 60))
_NEWS_LLM_BATCH = max(1, min(12, _env_int("NEWS_LLM_BATCH", 4)))
_NEWS_LLM_STARTUP_DELAY_SEC = max(0, _env_int("NEWS_LLM_STARTUP_DELAY_SEC", 0))
_NEWS_LLM_BACKGROUND_ENABLED = _env_bool("NEWS_LLM_BACKGROUND_ENABLED", True)
_NEWS_LLM_EXTERNAL_ONLY = _env_bool("NEWS_LLM_EXTERNAL_ONLY", False)
_EXTERNAL_NEWS_WORKER_ENABLED = _env_bool("START_NEWS_WORKER", False)
_DATA_MAINTENANCE_ENABLED = _env_bool("DATA_MAINTENANCE_ENABLED", False)
_PUBLIC_MACRO_WORKERS_ENABLED = _env_bool(
    "PUBLIC_MACRO_WORKERS_ENABLED",
    bool(getattr(settings, "PUBLIC_MACRO_WORKERS_ENABLED", False)),
)
_PREMIUM_EXTERNAL_WORKERS_ENABLED = _env_bool(
    "PREMIUM_EXTERNAL_WORKERS_ENABLED",
    bool(getattr(settings, "PREMIUM_EXTERNAL_WORKERS_ENABLED", False)),
)
_COINGLASS_WORKER_ENABLED = _env_bool(
    "COINGLASS_WORKER_ENABLED",
    bool(getattr(settings, "COINGLASS_WORKER_ENABLED", True)),
)
_EXCHANGE_WATCHDOG_ENABLED = _env_bool(
    "EXCHANGE_WATCHDOG_ENABLED",
    bool(getattr(settings, "EXCHANGE_WATCHDOG_ENABLED", True)),
)
_MARKET_WS_ENABLED = _env_bool(
    "MARKET_WS_ENABLED",
    bool(getattr(settings, "MARKET_WS_ENABLED", False)),
)
_MARKET_WS_ALLOWED_MODES = {"off", "shadow", "ui_primary", "strategy_primary"}
_MARKET_WS_MODE = str(os.getenv("MARKET_WS_MODE", getattr(settings, "MARKET_WS_MODE", "off")) or "off").strip().lower()
if _MARKET_WS_MODE not in _MARKET_WS_ALLOWED_MODES:
    _MARKET_WS_MODE = "off"
# Backwards compatibility: before MARKET_WS_MODE existed, MARKET_WS_ENABLED=true
# meant "start the experimental WS feed". Treat that as shadow unless the mode
# was explicitly set.
if _MARKET_WS_ENABLED and "MARKET_WS_MODE" not in os.environ and _MARKET_WS_MODE == "off":
    _MARKET_WS_MODE = "shadow"
_MARKET_WS_FORCE_REST = _env_bool(
    "MARKET_WS_FORCE_REST",
    bool(getattr(settings, "MARKET_WS_FORCE_REST", False)),
)
_MARKET_WS_STREAM_ENABLED = bool(
    _MARKET_WS_ENABLED and not _MARKET_WS_FORCE_REST and _MARKET_WS_MODE != "off"
)
_MARKET_WS_SYMBOL_LIMIT = max(
    1,
    _env_int("MARKET_WS_SYMBOL_LIMIT", int(getattr(settings, "MARKET_WS_SYMBOL_LIMIT", 16) or 16)),
)
_MARKET_WS_SYMBOL_MAX_AGE_SEC = max(
    0.5,
    _env_float(
        "MARKET_WS_SYMBOL_MAX_AGE_SEC",
        float(getattr(settings, "MARKET_WS_SYMBOL_MAX_AGE_SEC", 10.0) or 10.0),
    ),
)
_MARKET_WS_HEALTH_MAX_AGE_SEC = max(
    0.5,
    _env_float(
        "MARKET_WS_HEALTH_MAX_AGE_SEC",
        float(getattr(settings, "MARKET_WS_HEALTH_MAX_AGE_SEC", 15.0) or 15.0),
    ),
)
_MARKET_WS_RECONNECT_MIN_SEC = max(
    0.5,
    _env_float(
        "MARKET_WS_RECONNECT_MIN_SEC",
        float(getattr(settings, "MARKET_WS_RECONNECT_MIN_SEC", 1.0) or 1.0),
    ),
)
_MARKET_WS_RECONNECT_MAX_SEC = max(
    _MARKET_WS_RECONNECT_MIN_SEC,
    _env_float(
        "MARKET_WS_RECONNECT_MAX_SEC",
        float(getattr(settings, "MARKET_WS_RECONNECT_MAX_SEC", 30.0) or 30.0),
    ),
)
_MARKET_WS_MAX_PRICE_DIFF_BPS = max(
    0.0,
    _env_float(
        "MARKET_WS_MAX_PRICE_DIFF_BPS",
        float(getattr(settings, "MARKET_WS_MAX_PRICE_DIFF_BPS", 20.0) or 20.0),
    ),
)
_MARKET_WS_REST_RECONCILE_SEC = max(
    1.0,
    _env_float(
        "MARKET_WS_REST_RECONCILE_SEC",
        float(getattr(settings, "MARKET_WS_REST_RECONCILE_SEC", 30.0) or 30.0),
    ),
)
market_data_hub.symbol_max_age_sec = _MARKET_WS_SYMBOL_MAX_AGE_SEC
market_data_hub.exchange_max_age_sec = _MARKET_WS_HEALTH_MAX_AGE_SEC
market_data_hub.max_price_diff_bps = _MARKET_WS_MAX_PRICE_DIFF_BPS
market_data_hub.shadow_compare_max_age_sec = max(
    _MARKET_WS_SYMBOL_MAX_AGE_SEC,
    _MARKET_WS_REST_RECONCILE_SEC * 2.0,
)
_ANALYTICS_HISTORY_ENABLED = _env_bool(
    "ANALYTICS_HISTORY_ENABLED",
    bool(getattr(settings, "ANALYTICS_HISTORY_ENABLED", False)),
)
_ANALYTICS_HISTORY_MICRO_INTERVAL_SEC = max(
    60,
    _env_int(
        "ANALYTICS_HISTORY_MICRO_INTERVAL_SEC",
        int(getattr(settings, "ANALYTICS_HISTORY_MICRO_INTERVAL_SEC", 300)),
    ),
)
_ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC = max(
    120,
    _env_int(
        "ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC",
        int(getattr(settings, "ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC", 900)),
    ),
)
_ANALYTICS_HISTORY_WHALE_INTERVAL_SEC = max(
    120,
    _env_int(
        "ANALYTICS_HISTORY_WHALE_INTERVAL_SEC",
        int(getattr(settings, "ANALYTICS_HISTORY_WHALE_INTERVAL_SEC", 600)),
    ),
)
_ANALYTICS_HISTORY_DEFAULT_EXCHANGE = str(
    os.getenv("ANALYTICS_HISTORY_EXCHANGE", _AUTO_SYNC_PRIMARY_EXCHANGE)
).strip().lower() or _AUTO_SYNC_PRIMARY_EXCHANGE
_ANALYTICS_HISTORY_DEFAULT_SYMBOL = str(
    os.getenv("ANALYTICS_HISTORY_SYMBOL", _AUTO_SYNC_SYMBOLS[0])
).strip().upper() or _AUTO_SYNC_SYMBOLS[0]
_ANALYTICS_HISTORY_WORKER_SPECS = (
    ("microstructure", _ANALYTICS_HISTORY_MICRO_INTERVAL_SEC, 12),
    ("community", _ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC, 24),
    ("whales", _ANALYTICS_HISTORY_WHALE_INTERVAL_SEC, 36),
)
_STATUS_CACHE_TTL_SEC = 1.5
_status_cache_payload: Dict[str, Any] | None = None
_status_cache_at: float = 0.0
_startup_mode_decision: StartupModeDecision | None = None


def invalidate_status_cache() -> None:
    global _status_cache_payload, _status_cache_at
    _status_cache_payload = None
    _status_cache_at = 0.0


def _inspect_status_cache() -> Dict[str, Any]:
    age_sec = None
    if _status_cache_payload is not None and _status_cache_at > 0:
        age_sec = round(max(0.0, time.monotonic() - _status_cache_at), 3)
    return {
        "has_payload": _status_cache_payload is not None,
        "age_sec": age_sec,
    }


runtime_state.register_cache(
    "web_status_cache",
    clear=invalidate_status_cache,
    inspect=_inspect_status_cache,
    scope="global",
)


def _is_market_ws_stream_enabled() -> bool:
    """Runtime gate for exchange WS feed, kept dynamic for tests and rollback."""
    return bool(_MARKET_WS_ENABLED and not _MARKET_WS_FORCE_REST and _MARKET_WS_MODE != "off")


def _sync_market_data_hub_runtime_config() -> None:
    market_data_hub.symbol_max_age_sec = _MARKET_WS_SYMBOL_MAX_AGE_SEC
    market_data_hub.exchange_max_age_sec = _MARKET_WS_HEALTH_MAX_AGE_SEC
    market_data_hub.max_price_diff_bps = _MARKET_WS_MAX_PRICE_DIFF_BPS
    market_data_hub.shadow_compare_max_age_sec = max(
        _MARKET_WS_SYMBOL_MAX_AGE_SEC,
        _MARKET_WS_REST_RECONCILE_SEC * 2.0,
    )
    market_data_hub.shadow_compare_enabled = _is_market_ws_stream_enabled()


def _touch_runtime_task(task_name: str, *, success: bool = False) -> None:
    runtime_state.touch_task(task_name, success=success)


def _sync_guarded_startup_account_mode(decision: StartupModeDecision | None) -> bool:
    if decision is None or not decision.blocked_persisted_live_restore:
        return False
    if decision.effective_mode != "paper":
        return False
    try:
        updated = bool(account_manager.set_mode("main", decision.effective_mode))
        if updated:
            logger.warning(
                "Synchronized main account mode to paper after blocking persisted live-mode restore."
            )
        return updated
    except Exception as exc:
        logger.warning(f"Failed to synchronize main account mode during guarded startup: {exc}")
        return False


def _sync_startup_account_modes(decision: StartupModeDecision | None) -> Dict[str, Any]:
    result = {
        "main_updated": False,
        "auto_strategy_accounts_updated": 0,
    }
    if decision is None:
        return result

    try:
        result["main_updated"] = bool(account_manager.set_mode("main", decision.effective_mode))
        if result["main_updated"]:
            logger.warning(
                f"Synchronized main account mode to {decision.effective_mode} during startup."
            )
    except Exception as exc:
        logger.warning(f"Failed to synchronize main account mode during startup: {exc}")

    if decision.effective_mode == "paper":
        try:
            sync_auto_accounts = getattr(account_manager, "set_mode_for_auto_strategy_accounts", None)
            if callable(sync_auto_accounts):
                result["auto_strategy_accounts_updated"] = int(sync_auto_accounts("paper") or 0)
                if result["auto_strategy_accounts_updated"] > 0:
                    logger.warning(
                        "Synchronized {} auto-created strategy account(s) to paper mode during startup.",
                        result["auto_strategy_accounts_updated"],
                    )
        except Exception as exc:
            logger.warning(f"Failed to synchronize strategy account modes during startup: {exc}")

    return result


def _safe_json(obj: Any) -> Dict[str, Any]:
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return {"raw": str(obj)}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _format_number(value: Any, *, digits: int = 6, signed: bool = False) -> str:
    number = _safe_float(value, 0.0)
    magnitude = f"{abs(number):.{digits}f}".rstrip("0").rstrip(".") or "0"
    if signed:
        if number > 0:
            return f"+{magnitude}"
        if number < 0:
            return f"-{magnitude}"
        return "0"
    return f"-{magnitude}" if number < 0 else magnitude


def _autonomous_trade_action_label(signal_type: str, metadata: Dict[str, Any]) -> str:
    same_direction_existing_notional = _safe_float(metadata.get("same_direction_existing_notional"), 0.0)
    normalized = str(signal_type or "").strip().lower()
    if normalized == "buy":
        return "加多" if same_direction_existing_notional > 0 else "开多"
    if normalized == "sell":
        return "加空" if same_direction_existing_notional > 0 else "开空"
    if normalized == "close_long":
        return "平多"
    if normalized == "close_short":
        return "平空"
    return ""


def _build_ai_trade_execution_notification(event: str, data: Any) -> Optional[Dict[str, str]]:
    if str(event or "").strip() != "order_executed":
        return None

    payload = dict(data or {}) if isinstance(data, dict) else {}
    signal = dict(payload.get("signal") or {})
    metadata = dict(signal.get("metadata") or {})
    strategy_name = str(signal.get("strategy_name") or payload.get("strategy") or "").strip()
    source = str(metadata.get("source") or "").strip().lower()
    if strategy_name != "AI_AutonomousAgent" and source != "ai_autonomous_agent":
        return None

    signal_type = str(signal.get("signal_type") or "").strip().lower()
    action_label = _autonomous_trade_action_label(signal_type, metadata)
    if not action_label:
        return None

    symbol = str(signal.get("symbol") or payload.get("symbol") or "").strip() or "unknown"
    exchange = str(metadata.get("exchange") or payload.get("exchange") or "").strip().lower() or "-"
    account_id = str(metadata.get("account_id") or payload.get("account_id") or "main").strip() or "main"
    timeframe = str(metadata.get("timeframe") or "").strip() or "-"
    provider = str(metadata.get("agent_provider") or "").strip() or "-"
    model = str(metadata.get("agent_model") or "").strip() or "-"
    reason = str(metadata.get("agent_reason") or payload.get("reason") or "").strip() or "-"
    confidence = _safe_float(metadata.get("agent_confidence"), 0.0)
    strength = _safe_float(signal.get("strength"), 0.0)
    order = dict(payload.get("order") or {})
    filled = max(
        _safe_float(order.get("filled"), 0.0),
        _safe_float(order.get("amount"), 0.0),
        _safe_float(payload.get("quantity"), 0.0),
    )
    price = max(
        _safe_float(order.get("price"), 0.0),
        _safe_float(payload.get("close_price"), 0.0),
        _safe_float(signal.get("price"), 0.0),
    )
    notional = filled * price if filled > 0 and price > 0 else 0.0
    order_id = str(order.get("id") or "").strip()
    trading_mode = str(execution_engine.get_trading_mode() or "-")

    lines = [
        f"动作: {action_label}",
        f"交易模式: {trading_mode}",
        f"交易所/账户: {exchange}/{account_id}",
        f"币种: {symbol}",
        f"时间框架: {timeframe}",
    ]
    if price > 0:
        lines.append(f"成交价格: {_format_number(price, digits=8)}")
    if filled > 0:
        lines.append(f"成交数量: {_format_number(filled, digits=8)}")
    if notional > 0:
        lines.append(f"成交名义金额: {_format_number(notional, digits=4)}")
    if signal_type in {"close_long", "close_short"} and "pnl" in payload:
        lines.append(f"平仓盈亏: {_format_number(payload.get('pnl'), digits=4, signed=True)}")
    if confidence > 0:
        lines.append(f"模型置信度: {_format_number(confidence, digits=3)}")
    if strength > 0:
        lines.append(f"信号强度: {_format_number(strength, digits=3)}")
    lines.append(f"模型: {provider}/{model}")
    if order_id:
        lines.append(f"订单ID: {order_id}")
    lines.append(f"理由: {reason}")

    return {
        "title": f"AI自治代理{action_label}提醒: {symbol}",
        "message": "\n".join(lines),
    }


async def _send_ai_trade_execution_notification(event: str, data: Any) -> None:
    notification = _build_ai_trade_execution_notification(event, data)
    if not notification:
        return
    try:
        result = await notification_manager.send_message(
            title=str(notification.get("title") or "AI自治代理成交提醒"),
            message=str(notification.get("message") or ""),
            channels=["feishu"],
        )
        if not bool((result or {}).get("feishu")):
            logger.warning(
                "AI autonomous trade notification did not reach feishu "
                f"(event={event}, title={notification.get('title')})"
            )
    except Exception as exc:
        logger.warning(f"AI autonomous trade notification failed: {exc}")


async def _emit_runtime_snapshot() -> None:
    if not event_bus.has_subscribers():
        return
    await event_bus.publish_nowait_safe(
        event="runtime_snapshot",
        payload={
            "mode": execution_engine.get_trading_mode(),
            "queue_size": execution_engine.get_queue_size(),
            "strategy_summary": strategy_manager.get_dashboard_summary(signal_limit=10),
            "positions": position_manager.get_stats(),
            "orders": order_manager.get_stats(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


async def _on_execution_event(event: str, data: Any) -> None:
    payload = _safe_json(data)
    await event_bus.publish_nowait_safe(
        event="execution_event",
        payload={"event": event, "data": payload},
    )
    await _send_ai_trade_execution_notification(event, payload)


async def _on_strategy_signal(signal: Any) -> None:
    payload = signal.to_dict() if hasattr(signal, "to_dict") else _safe_json(signal)
    await event_bus.publish_nowait_safe(event="strategy_signal", payload=payload)


async def _on_order_event(order: Any, event: str) -> None:
    meta = order_manager.get_order_metadata(order.id)
    payload = {
        "event": event,
        "order": {
            "id": order.id,
            "exchange": order.exchange,
            "symbol": order.symbol,
            "side": order.side.value,
            "type": order.type.value,
            "status": order.status.value,
            "price": float(order.price or 0.0),
            "amount": float(order.amount or 0.0),
            "filled": float(order.filled or 0.0),
            "timestamp": order.timestamp.isoformat() if order.timestamp else None,
            "strategy": meta.get("strategy"),
            "account_id": meta.get("account_id", "main"),
            "order_mode": meta.get("order_mode", "normal"),
            "stop_loss": meta.get("stop_loss"),
            "take_profit": meta.get("take_profit"),
            "trailing_stop_pct": meta.get("trailing_stop_pct"),
            "trailing_stop_distance": meta.get("trailing_stop_distance"),
            "rejected": bool(meta.get("rejected", False)),
            "reject_reason": meta.get("reject_reason"),
        },
    }
    await event_bus.publish_nowait_safe(event="order_event", payload=payload)


async def _on_position_event(position: Any, event: str) -> None:
    payload = {
        "event": event,
        "position": position.to_dict() if hasattr(position, "to_dict") else _safe_json(position),
    }
    await event_bus.publish_nowait_safe(event="position_event", payload=payload)


def _collect_watch_symbols() -> List[str]:
    symbols = {"BTC/USDT", "ETH/USDT"}
    try:
        for item in strategy_manager.list_strategies():
            if item.get("state") != "running":
                continue
            for symbol in item.get("symbols", []):
                if symbol:
                    symbols.add(str(symbol))
    except Exception:
        pass
    return list(symbols)[:_MARKET_WS_SYMBOL_LIMIT]


_MARKET_TICK_PER_CALL_TIMEOUT_SEC = 3.0
_MARKET_TICK_RECONNECT_MIN_SEC = 60.0
_market_tick_reconnect_last_attempt: Dict[str, float] = {}


async def _fetch_one_ticker(connector: Any, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        ticker = await asyncio.wait_for(
            connector.get_ticker(symbol),
            timeout=_MARKET_TICK_PER_CALL_TIMEOUT_SEC,
        )
        return {
            "last": float(ticker.last or 0.0),
            "bid": float(ticker.bid or 0.0),
            "ask": float(ticker.ask or 0.0),
            "timestamp": ticker.timestamp.isoformat() if ticker.timestamp else None,
        }
    except Exception:
        return None


def _configured_market_ws_exchange_names() -> List[str]:
    configured = str(getattr(settings, "MARKET_WS_EXCHANGES", "") or "").strip()
    if not configured:
        return []
    return [name.strip().lower() for name in configured.split(",") if name.strip()]


def _symbols_requiring_ws_fallback(symbols: Iterable[str]) -> List[str]:
    _sync_market_data_hub_runtime_config()
    missing_or_stale: List[str] = []
    seen = set()
    exchanges = _configured_market_ws_exchange_names()
    if not exchanges:
        exchanges = market_data_hub.healthy_exchanges(
            max_age_sec=_MARKET_WS_HEALTH_MAX_AGE_SEC,
            source="ws",
        )
    for symbol in symbols:
        normalized = str(symbol or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        has_fresh_ws = False
        for exchange_name in exchanges:
            current = market_data_hub.get_tick(
                exchange_name,
                normalized,
                max_age_sec=_MARKET_WS_SYMBOL_MAX_AGE_SEC,
                source="ws",
            )
            if not current:
                continue
            meta = current.get("meta") if isinstance(current, dict) else {}
            tick = current.get("tick") if isinstance(current, dict) else {}
            if (
                isinstance(meta, dict)
                and isinstance(tick, dict)
                and str(meta.get("source") or tick.get("source") or "").lower() == "ws"
                and not bool(meta.get("is_stale", True))
            ):
                has_fresh_ws = True
                break
        if not has_fresh_ws:
            missing_or_stale.append(normalized)
    return missing_or_stale


async def _market_tick_connector_items() -> List[Tuple[str, Any]]:
    names: List[str] = []
    seen = set()
    try:
        candidates = list(exchange_manager.get_connected_exchanges())
    except Exception:
        candidates = []
    try:
        candidates.extend(list(exchange_manager.get_all_exchanges().keys()))
    except Exception:
        pass
    if _is_market_ws_stream_enabled():
        candidates.extend(_configured_market_ws_exchange_names())
    for name in candidates:
        exchange_name = str(name)
        if exchange_name in seen:
            continue
        seen.add(exchange_name)
        names.append(exchange_name)

    connectors: List[Tuple[str, Any]] = []
    now = time.monotonic()
    for exchange_name in names:
        connector = exchange_manager.get_exchange(exchange_name)
        if connector is None or not bool(getattr(connector, "is_connected", True)):
            last_attempt = _market_tick_reconnect_last_attempt.get(exchange_name, 0.0)
            if now - last_attempt >= _MARKET_TICK_RECONNECT_MIN_SEC:
                _market_tick_reconnect_last_attempt[exchange_name] = now
                try:
                    connector = await exchange_manager.ensure_exchange(exchange_name)
                except Exception as exc:
                    logger.debug(f"market_tick reconnect skipped for {exchange_name}: {exc}")
                    connector = None
        if connector is not None and bool(getattr(connector, "is_connected", True)):
            connectors.append((exchange_name, connector))
    return connectors


async def _emit_market_ticks(
    *,
    hub_source: str = "rest_snapshot",
    fallback_reason: str = "periodic_rest_snapshot",
    publish: bool = True,
    require_subscribers: bool = True,
    symbols: Optional[Iterable[str]] = None,
) -> None:
    _sync_market_data_hub_runtime_config()
    if require_subscribers and not event_bus.has_subscribers():
        return
    watch_symbols = list(symbols) if symbols is not None else _collect_watch_symbols()
    if not watch_symbols:
        return

    payload: Dict[str, Dict[str, Any]] = {}
    # Fetch all (exchange, symbol) tickers concurrently — sequential REST was
    # spending ~16 calls × per-call latency every cycle and starving other
    # Binance traffic of rate-limit budget.
    jobs: List[Tuple[str, str, asyncio.Task]] = []
    for exchange_name, connector in await _market_tick_connector_items():
        for symbol in watch_symbols:
            jobs.append(
                (
                    exchange_name,
                    symbol,
                    asyncio.create_task(_fetch_one_ticker(connector, symbol)),
                )
            )

    if not jobs:
        return

    try:
        results = await asyncio.gather(*(j[2] for j in jobs), return_exceptions=True)
    except Exception:
        # Best-effort: cancel any stragglers then bail out for this cycle.
        for _, _, task in jobs:
            if not task.done():
                task.cancel()
        return

    for (exchange_name, symbol, _task), result in zip(jobs, results):
        if isinstance(result, BaseException) or not isinstance(result, dict):
            continue
        market_data_hub.upsert_rest_tick(
            exchange_name,
            symbol,
            result,
            source=hub_source,  # type: ignore[arg-type]
            reason=fallback_reason,
        )
        payload.setdefault(exchange_name, {})[symbol] = result

    if payload and publish and event_bus.has_subscribers():
        try:
            await event_bus.publish_nowait_safe(event="market_tick", payload=payload)
        except Exception as exc:
            logger.warning(f"market_tick REST publish failed after hub write: {exc}")


# Push snapshots every 2s (in-memory data, cheap) but only fan out REST-heavy
# market ticks every _MARKET_TICK_INTERVAL_SEC to keep the exchange rate-limit
# budget under control. Reduces REST QPS roughly 3-4×.
_MARKET_TICK_INTERVAL_SEC = 6.0
_MARKET_WS_EXCHANGE_DISCOVERY_INTERVAL_SEC = 5.0

# Holder for the live WS feed. Set by _market_ws_feed_worker when the feed is
# running so the REST pusher can defer to it (and back-fill only when the
# socket goes quiet). None when the feed is disabled or not yet started.
_market_ws_feed: Optional[Any] = None

# ── optional WS quality auto-degrade guard (off by default) ─────────────────
_MARKET_WS_QUALITY_GUARD_ENABLED = _env_bool(
    "MARKET_WS_QUALITY_GUARD_ENABLED",
    bool(getattr(settings, "MARKET_WS_QUALITY_GUARD_ENABLED", False)),
)
try:
    from core.marketdata.ws_quality_guard import WsQualityGuard, sample_from_market_ws_status

    _market_ws_quality_guard = (
        WsQualityGuard(enabled=True, max_tick_age_ms=float(_MARKET_WS_SYMBOL_MAX_AGE_SEC) * 1000.0)
        if _MARKET_WS_QUALITY_GUARD_ENABLED
        else None
    )
except Exception as exc:  # pragma: no cover - guard module is optional
    WsQualityGuard = None  # type: ignore
    sample_from_market_ws_status = None  # type: ignore
    _market_ws_quality_guard = None
    logger.debug(f"ws quality guard unavailable: {exc}")


def _observe_ws_quality_guard() -> bool:
    """Feed the WS quality guard one sample; return True if it says force REST.

    No-op (False) unless the guard is enabled and we're in a primary mode — in
    shadow/off, REST is already authoritative so the guard is irrelevant.
    """
    guard = _market_ws_quality_guard
    if guard is None or not getattr(guard, "enabled", False):
        return False
    if _MARKET_WS_MODE not in {"ui_primary", "strategy_primary"}:
        return False
    if sample_from_market_ws_status is None:
        return False
    try:
        decision = guard.observe(sample_from_market_ws_status(_market_ws_status_snapshot()))
        if decision.action == "degrade":
            logger.warning(
                "market_ws quality guard DEGRADE -> forcing REST: %s",
                "; ".join(decision.reasons),
            )
        elif decision.action == "recover":
            logger.info("market_ws quality guard RECOVER -> WS primary restored")
        return bool(decision.force_rest)
    except Exception as exc:
        logger.debug(f"ws quality guard observe failed: {exc}")
        return False


async def _runtime_pusher(stop_event: asyncio.Event) -> None:
    last_market_tick_at = 0.0
    last_rest_reconcile_at = 0.0
    while not stop_event.is_set():
        try:
            now = asyncio.get_event_loop().time()
            has_subscribers = event_bus.has_subscribers()
            if has_subscribers:
                await _emit_runtime_snapshot()
            if has_subscribers and now - last_market_tick_at >= _MARKET_TICK_INTERVAL_SEC:
                # In shadow mode REST remains the UI/runtime tick source while
                # WS only feeds the hub. In ui_primary/strategy_primary, a
                # fresh hub tick is allowed to suppress REST fan-out.
                _sync_market_data_hub_runtime_config()
                feed = _market_ws_feed
                feed_healthy = bool(feed is not None and feed.is_healthy())
                watch_symbols = _collect_watch_symbols()
                fallback_symbols = (
                    _symbols_requiring_ws_fallback(watch_symbols)
                    if feed_healthy
                    else list(watch_symbols)
                )
                hub_healthy = not fallback_symbols
                guard_force_rest = _observe_ws_quality_guard()
                ws_can_suppress_rest = bool(
                    _is_market_ws_stream_enabled()
                    and _MARKET_WS_MODE in {"ui_primary", "strategy_primary"}
                    and feed_healthy
                    and hub_healthy
                    and not guard_force_rest
                )
                if not ws_can_suppress_rest:
                    fallback_active = bool(
                        _is_market_ws_stream_enabled()
                        and _MARKET_WS_MODE in {"ui_primary", "strategy_primary"}
                    )
                    reason = "ws_unhealthy" if not feed_healthy else "ws_stale"
                    await _emit_market_ticks(
                        hub_source="rest_fallback" if fallback_active else "rest_snapshot",
                        fallback_reason=reason if fallback_active else "periodic_rest_snapshot",
                        symbols=fallback_symbols if fallback_active else None,
                    )
                    if _is_market_ws_stream_enabled() and _MARKET_WS_MODE == "shadow":
                        last_rest_reconcile_at = now
                last_market_tick_at = now
            if (
                _is_market_ws_stream_enabled()
                and _MARKET_WS_MODE == "shadow"
                and now - last_rest_reconcile_at >= _MARKET_WS_REST_RECONCILE_SEC
            ):
                await _emit_market_ticks(
                    hub_source="rest_snapshot",
                    fallback_reason="shadow_rest_reconcile",
                    publish=False,
                    require_subscribers=False,
                )
                last_rest_reconcile_at = now
            _touch_runtime_task("runtime", success=True)
        except Exception as e:
            logger.debug(f"runtime snapshot push failed: {e}")
        await asyncio.sleep(2)


async def _publish_market_ticks(payload: Dict[str, Dict[str, Any]]) -> None:
    """Callback for the WS feed — cache ticks and optionally forward to UI."""
    if not payload:
        return
    _sync_market_data_hub_runtime_config()
    normalized_payload: Dict[str, Dict[str, Any]] = {}
    for exchange_name, symbols in payload.items():
        if not isinstance(symbols, dict):
            continue
        for symbol, tick_payload in symbols.items():
            if not isinstance(tick_payload, dict):
                market_data_hub.upsert_ws_tick(exchange_name, symbol, {})
                continue
            tick = market_data_hub.upsert_ws_tick(exchange_name, symbol, tick_payload)
            if tick is None:
                continue
            normalized_payload.setdefault(tick.exchange, {})[tick.symbol] = tick.to_payload()
    if (
        normalized_payload
        and event_bus.has_subscribers()
        and _is_market_ws_stream_enabled()
        and _MARKET_WS_MODE in {"ui_primary", "strategy_primary"}
    ):
        try:
            await event_bus.publish_nowait_safe(event="market_tick", payload=normalized_payload)
        except Exception as exc:
            logger.warning(f"market_tick WS publish failed after hub write: {exc}")


def _market_ws_status_snapshot(*, include_symbols: bool = False) -> Dict[str, Any]:
    _sync_market_data_hub_runtime_config()
    feed = _market_ws_feed
    feed_healthy = bool(feed is not None and getattr(feed, "is_healthy", lambda: False)())
    try:
        feed_exchanges = (
            list(feed.healthy_exchanges(max_age_sec=_MARKET_WS_HEALTH_MAX_AGE_SEC))
            if feed is not None and hasattr(feed, "healthy_exchanges")
            else []
        )
    except Exception:
        feed_exchanges = []
    feed_status: Dict[str, Any] = {}
    if feed is not None and hasattr(feed, "status_snapshot"):
        try:
            raw_feed_status = feed.status_snapshot()
            if isinstance(raw_feed_status, dict):
                feed_status = raw_feed_status
        except Exception as exc:
            feed_status = {"snapshot_error": str(exc)}
    hub_snapshot = market_data_hub.snapshot(include_symbols=include_symbols)
    return {
        "enabled": bool(_is_market_ws_stream_enabled()),
        "configured_enabled": bool(_MARKET_WS_ENABLED),
        "mode": _MARKET_WS_MODE,
        "force_rest": bool(_MARKET_WS_FORCE_REST),
        "fail_closed_for_live": bool(getattr(settings, "MARKET_WS_FAIL_CLOSED_FOR_LIVE", True)),
        "feed_present": feed is not None,
        "feed_healthy": feed_healthy,
        "feed_healthy_exchanges": feed_exchanges,
        "feed_status": feed_status,
        "feed_watch_attempt_count": int(feed_status.get("watch_attempt_count") or 0),
        "feed_watch_timeout_count": int(feed_status.get("watch_timeout_count") or 0),
        "feed_watch_error_count": int(feed_status.get("watch_error_count") or 0),
        "feed_watch_empty_count": int(feed_status.get("watch_empty_count") or 0),
        "feed_last_error": feed_status.get("last_error"),
        "symbol_limit": _MARKET_WS_SYMBOL_LIMIT,
        "symbol_max_age_sec": _MARKET_WS_SYMBOL_MAX_AGE_SEC,
        "health_max_age_sec": _MARKET_WS_HEALTH_MAX_AGE_SEC,
        "max_price_diff_bps": _MARKET_WS_MAX_PRICE_DIFF_BPS,
        "quality_guard": (
            _market_ws_quality_guard.status()
            if _market_ws_quality_guard is not None
            else {"enabled": False}
        ),
        **hub_snapshot,
    }


async def _market_ws_feed_worker(stop_event: asyncio.Event) -> None:
    """Run the ccxt.pro market-data feed for the session lifetime."""
    global _market_ws_feed
    try:
        from core.marketdata.ccxt_pro_feed import CcxtProMarketFeed, CCXT_PRO_AVAILABLE
    except Exception as exc:  # pragma: no cover - import guard
        logger.warning(f"market_ws_feed: import failed, staying on REST: {exc}")
        return
    if not CCXT_PRO_AVAILABLE:
        logger.warning("market_ws_feed: ccxt.pro unavailable, staying on REST ticks")
        return

    def _resolve_exchanges() -> List[str]:
        configured = _configured_market_ws_exchange_names()
        if configured:
            return configured
        return list(exchange_manager.get_connected_exchanges())

    # When the list is sourced from live connections it can be empty at boot
    # because this worker may start before exchange_manager finishes connecting.
    # Wait for them rather than returning: a normal return is NOT restarted by
    # the supervisor (only exceptions are), so returning here would disable the
    # feed for the whole session.
    exchanges = _resolve_exchanges()
    while not exchanges and not stop_event.is_set():
        logger.info("market_ws_feed: no exchanges to stream yet, waiting...")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=_MARKET_WS_EXCHANGE_DISCOVERY_INTERVAL_SEC,
            )
        exchanges = _resolve_exchanges()
    if stop_event.is_set():
        return

    feed = CcxtProMarketFeed(
        on_tick=_publish_market_ticks,
        symbols_provider=_collect_watch_symbols,
        exchanges=exchanges,
        watch_timeout_sec=_env_float(
            "MARKET_WS_WATCH_TIMEOUT_SEC",
            float(getattr(settings, "MARKET_WS_WATCH_TIMEOUT_SEC", 25.0) or 25.0),
        ),
        reconnect_min_sec=_MARKET_WS_RECONNECT_MIN_SEC,
        reconnect_max_sec=_MARKET_WS_RECONNECT_MAX_SEC,
        health_max_age_sec=_MARKET_WS_HEALTH_MAX_AGE_SEC,
    )
    _market_ws_feed = feed
    try:
        await feed.run(stop_event)
    finally:
        _market_ws_feed = None


async def _emit_news_preview(app: FastAPI, limit: int = 10, hours: int = 24) -> None:
    if not event_bus.has_subscribers():
        return
    from web.api import news as news_api

    cfg = getattr(app.state, "news_cfg", None)
    if not isinstance(cfg, dict):
        cfg = news_api.load_news_cfg()
        app.state.news_cfg = cfg

    feed = await news_api.build_latest_feed(cfg=cfg, symbol=None, hours=hours, limit=limit)
    await event_bus.publish_nowait_safe(
        event="news_update",
        payload={
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": int(feed.get("count") or 0),
            "items": feed.get("items") or [],
        },
    )


async def _news_refresh_worker(app: FastAPI, stop_event: asyncio.Event) -> None:
    from web.api import news as news_api

    if _NEWS_STARTUP_DELAY_SEC > 0:
        await asyncio.sleep(_NEWS_STARTUP_DELAY_SEC)
    emit_counter = 0
    sleep_seconds = _NEWS_PULL_INTERVAL_SEC
    while not stop_event.is_set():
        try:
            cfg = getattr(app.state, "news_cfg", None)
            if not isinstance(cfg, dict):
                cfg = news_api.load_news_cfg()
                app.state.news_cfg = cfg

            pull_stats = await news_api.pull_and_store_news(
                cfg=cfg,
                payload=news_api.PullNowRequest(
                    since_minutes=_NEWS_PULL_SINCE_MINUTES,
                    max_records=_NEWS_PULL_MAX_RECORDS,
                ),
            )
            app.state.news_last_pull = pull_stats

            # Back off only when all active sources are rate-limited.
            source_stats = pull_stats.get("source_stats") if isinstance(pull_stats.get("source_stats"), dict) else {}
            active_sources = 0
            rate_limited_sources = 0
            for stat in source_stats.values():
                if not isinstance(stat, dict):
                    continue
                active_sources += 1
                stat_errors = [str(x) for x in (stat.get("errors") or [])]
                pulled_count = int(stat.get("pulled_count") or 0)
                if pulled_count <= 0 and any("429" in msg for msg in stat_errors):
                    rate_limited_sources += 1
            if active_sources > 0 and rate_limited_sources >= active_sources:
                sleep_seconds = max(_NEWS_PULL_INTERVAL_SEC, 300)
            else:
                sleep_seconds = _NEWS_PULL_INTERVAL_SEC

            emit_counter += 1
            should_emit = True
            if should_emit:
                emit_counter = 0
                await _emit_news_preview(app=app, limit=12, hours=24)
            _touch_runtime_task("news", success=True)
        except Exception as e:
            logger.debug(f"background news refresh failed: {e}")
            sleep_seconds = max(_NEWS_PULL_INTERVAL_SEC, 300)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass


async def _news_llm_worker(app: FastAPI, stop_event: asyncio.Event) -> None:
    from web.api import news as news_api

    if _NEWS_LLM_STARTUP_DELAY_SEC > 0:
        await asyncio.sleep(_NEWS_LLM_STARTUP_DELAY_SEC)
    while not stop_event.is_set():
        try:
            cfg = getattr(app.state, "news_cfg", None)
            if not isinstance(cfg, dict):
                cfg = news_api.load_news_cfg()
                app.state.news_cfg = cfg
            result = await news_api.process_llm_batch(cfg, limit=_NEWS_LLM_BATCH)
            failed_requeue = await news_api.auto_requeue_failed_llm_tasks(cfg)
            retry_result = {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}
            if int(failed_requeue.get("requeued_count") or 0) > 0:
                retry_result = await news_api.process_llm_batch(
                    cfg,
                    limit=max(1, min(int(_NEWS_LLM_BATCH or 4), int(failed_requeue.get("requeued_count") or 0))),
                )
            summary_repair = await news_api.repair_recent_news_summaries(cfg)
            app.state.news_last_llm_batch = {
                **_safe_json(result),
                "failed_requeue": _safe_json(failed_requeue),
                "retry_result": _safe_json(retry_result),
                "summary_repair": _safe_json(summary_repair),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "limit": _NEWS_LLM_BATCH,
            }
            _touch_runtime_task("news_llm", success=True)
        except Exception as e:
            app.state.news_last_llm_batch = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "claimed": 0,
                "events_count": 0,
                "errors": [str(e)],
                "limit": _NEWS_LLM_BATCH,
            }
            logger.warning(f"background news llm worker failed: {e}")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_NEWS_LLM_INTERVAL_SEC)
        except asyncio.TimeoutError:
            pass


async def _analytics_history_worker(
    app: FastAPI,
    stop_event: asyncio.Event,
    *,
    collector: str,
    interval_sec: int,
    exchange: str,
    symbol: str,
    depth_limit: int = 80,
    startup_delay_sec: int = 12,
) -> None:
    from web.api import trading as trading_api

    await asyncio.sleep(max(3, int(startup_delay_sec)))
    while not stop_event.is_set():
        try:
            result = await trading_api.run_analytics_history_collection(
                exchange=exchange,
                symbol=symbol,
                depth_limit=depth_limit,
                collectors=[collector],
            )
            app.state.analytics_history_last_runs = getattr(app.state, "analytics_history_last_runs", {})
            app.state.analytics_history_last_runs[collector] = result
            _touch_runtime_task(f"analytics_history_{collector}", success=True)
        except Exception as e:
            logger.warning(f"analytics history worker failed collector={collector}: {e}")
            app.state.analytics_history_last_runs = getattr(app.state, "analytics_history_last_runs", {})
            app.state.analytics_history_last_runs[collector] = {
                "success": False,
                "collector": collector,
                "exchange": exchange,
                "symbol": symbol,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        sleep_span = max(5, int(interval_sec))
        if collector == "community":
            sleep_span += 7
        elif collector == "whales":
            sleep_span += 13
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_span)
        except asyncio.TimeoutError:
            pass


def _maintenance_snapshot_path(kind: str) -> Path:
    root = Path(settings.BASE_DIR) / "data" / "research" / "auto_snapshots" / kind
    root.mkdir(parents=True, exist_ok=True)
    return root


def _save_maintenance_snapshot(kind: str, payload: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    folder = _maintenance_snapshot_path(kind)
    file_path = folder / f"{kind}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path = folder / "latest.json"
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _sync_days_for_timeframe(timeframe: str) -> int:
    tf = str(timeframe or "1h")
    if tf == "1s":
        return 3
    if tf in {"5s", "10s", "30s"}:
        return 45
    if tf in {"1m", "5m", "15m", "30m"}:
        return 365
    if tf in {"1h", "4h"}:
        return 900
    return 1200


async def _has_recent_kline(exchange: str, symbol: str, timeframe: str, hours: int = 12) -> bool:
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=max(1, int(hours)))
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
        )
        return df is not None and not df.empty
    except Exception:
        return False


async def _sync_market_dataset(exchange: str, symbol: str, timeframe: str) -> Dict[str, Any]:
    from web.api import data as data_api

    result: Dict[str, Any] = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "download": None,
        "integrity": None,
        "repair": None,
    }
    try:
        if timeframe == "1s":
            recent_ok = await _has_recent_kline(exchange=exchange, symbol=symbol, timeframe="1s", hours=18)
            if not recent_ok:
                active_tasks = [
                    t
                    for t in second_level_backfill_manager.list_tasks()
                    if str(t.get("exchange")) == exchange
                    and str(t.get("symbol")) == symbol
                    and str(t.get("status")) in {"pending", "running"}
                ]
                if active_tasks:
                    result["seconds_backfill"] = {
                        "started": False,
                        "reason": "existing_active_task",
                        "task_id": active_tasks[0].get("task_id"),
                    }
                else:
                    now = datetime.now(timezone.utc)
                    result["seconds_backfill"] = second_level_backfill_manager.start_task(
                        exchange=exchange,
                        symbol=symbol,
                        start_time=now - timedelta(days=365),
                        end_time=now,
                        window_days=1,
                    )
            result["download"] = await data_api.run_download_historical_data(
                exchange=exchange,
                symbol=symbol,
                timeframe="1s",
                days=_sync_days_for_timeframe("1s"),
            )
        else:
            result["download"] = await data_api.run_download_historical_data(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                days=_sync_days_for_timeframe(timeframe),
            )

        if timeframe not in {"1w", "1M"}:
            integrity = await data_api.check_data_integrity(exchange=exchange, symbol=symbol, timeframe=timeframe)
            result["integrity"] = integrity
            missing_count = int(((integrity or {}).get("missing") or {}).get("missing_count") or 0)
            invalid_rows = int(((integrity or {}).get("quality") or {}).get("invalid_rows") or 0)
            duplicate_rows = int(((integrity or {}).get("quality") or {}).get("duplicate_rows") or 0)
            if missing_count > 0 or invalid_rows > 0 or duplicate_rows > 0:
                result["repair"] = await data_api.repair_data_integrity(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                )
    except Exception as e:
        result["error"] = str(e)
    return result


async def _collect_news_snapshot() -> Dict[str, Any]:
    try:
        from core.data.news_collector import NewsCollector

        collector = NewsCollector(storage_path=str(Path(settings.BASE_DIR) / "data" / "research" / "news"))
        news_items = await collector.collect_all_news()
        saved = collector.save_news(news_items)
        return {
            "count": len(news_items),
            "saved_path": saved,
            "sentiment": collector.get_sentiment_summary(news_items),
            "categories": collector.get_category_distribution(news_items),
        }
    except Exception as e:
        return {"error": str(e), "count": 0}


async def _maintenance_safe_call(name: str, coro: Any) -> Dict[str, Any]:
    started = datetime.now(timezone.utc)
    try:
        data = await coro
        return {
            "ok": True,
            "name": name,
            "latency_ms": round((datetime.now(timezone.utc) - started).total_seconds() * 1000, 3),
            "data": data,
        }
    except Exception as e:
        return {
            "ok": False,
            "name": name,
            "latency_ms": round((datetime.now(timezone.utc) - started).total_seconds() * 1000, 3),
            "error": str(e),
        }


async def _run_data_maintenance_once() -> Dict[str, Any]:
    from web.api import data as data_api
    from web.api import trading as trading_api

    started_at = datetime.now(timezone.utc)
    tasks: List[Dict[str, Any]] = []
    _save_maintenance_snapshot(
        "maintenance_progress",
        {
            "started_at": started_at.isoformat(),
            "status": "running",
            "message": "后台数据维护任务已启动，正在下载/校验/补全历史数据",
        },
    )

    # Primary exchange: richer and finer datasets.
    for symbol in _AUTO_SYNC_SYMBOLS:
        tasks.append(await _sync_market_dataset(_AUTO_SYNC_PRIMARY_EXCHANGE, symbol, "1s"))
        for timeframe in _AUTO_SYNC_TIMEFRAMES:
            tasks.append(await _sync_market_dataset(_AUTO_SYNC_PRIMARY_EXCHANGE, symbol, timeframe))

    # Secondary exchange: keep key frames for cross validation and failover.
    for symbol in _AUTO_SYNC_SYMBOLS[:5]:
        for timeframe in ["1m", "5m", "1h", "1d"]:
            tasks.append(await _sync_market_dataset(_AUTO_SYNC_SECONDARY_EXCHANGE, symbol, timeframe))

    symbols_csv = ",".join(_AUTO_SYNC_SYMBOLS)
    analytics = await _maintenance_safe_call(
        "analytics_overview",
        trading_api.get_analytics_overview(
            days=90,
            lookback=240,
            calendar_days=45,
            exchange=_AUTO_SYNC_PRIMARY_EXCHANGE,
            symbol="BTC/USDT",
        ),
    )
    community = await _maintenance_safe_call(
        "community_overview",
        trading_api.get_community_overview(
            symbol="BTC/USDT",
            exchange=_AUTO_SYNC_PRIMARY_EXCHANGE,
        ),
    )
    factor_library = await _maintenance_safe_call(
        "factor_library",
        data_api.get_factor_library(
            exchange=_AUTO_SYNC_PRIMARY_EXCHANGE,
            symbols=symbols_csv,
            timeframe="1h",
            lookback=2200,
            quantile=0.3,
            series_limit=1200,
        ),
    )
    onchain = await _maintenance_safe_call(
        "onchain_overview",
        data_api.get_onchain_overview(
            symbol="BTC/USDT",
            exchange=_AUTO_SYNC_PRIMARY_EXCHANGE,
            whale_threshold_btc=100.0,
            chain="auto",
        ),
    )
    multi_assets = await _maintenance_safe_call(
        "multi_assets",
        data_api.get_multi_assets_overview(
            exchange=_AUTO_SYNC_PRIMARY_EXCHANGE,
            symbols=symbols_csv,
            timeframe="1h",
            lookback=1200,
        ),
    )
    news_snapshot = await _collect_news_snapshot()

    report = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_sec": round((datetime.now(timezone.utc) - started_at).total_seconds(), 3),
        "market_sync_count": len(tasks),
        "market_sync": tasks,
        "analytics_overview": analytics,
        "community_overview": community,
        "factor_library": factor_library,
        "onchain_overview": onchain,
        "multi_assets": multi_assets,
        "news": news_snapshot,
    }
    _save_maintenance_snapshot(
        "maintenance_progress",
        {
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "market_sync_count": len(tasks),
        },
    )
    _save_maintenance_snapshot("maintenance", report)
    return report


async def _google_trends_worker(stop_event: asyncio.Event) -> None:
    """Update Google Trends cache every 6 hours (requires pytrends, graceful no-op if absent)."""
    INTERVAL = 6 * 3600
    STARTUP_DELAY = 120  # let other workers start first
    await asyncio.sleep(STARTUP_DELAY)
    while not stop_event.is_set():
        try:
            from core.data.google_trends_collector import update_all_keywords  # noqa: PLC0415
            result = await update_all_keywords()
            if result:
                logger.debug(f"google_trends_worker: updated {list(result.keys())}")
            _touch_runtime_task("google_trends", success=True)
        except ImportError as exc:
            # Optional dependency (pytrends) missing — disable worker permanently to avoid wasted loops.
            logger.info(f"google_trends_worker: dependency missing, disabling worker: {exc}")
            _touch_runtime_task("google_trends", success=False)
            return
        except Exception as exc:
            logger.debug(f"google_trends_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _macro_cache_worker(stop_event: asyncio.Event) -> None:
    """Update FRED macro cache once daily (requires FRED_API_KEY env var)."""
    INTERVAL = 24 * 3600
    STARTUP_DELAY = 180
    await asyncio.sleep(STARTUP_DELAY)
    while not stop_event.is_set():
        try:
            from core.data.macro_collector import update_macro_cache  # noqa: PLC0415
            result = await update_macro_cache()
            if result:
                logger.debug(f"macro_cache_worker: updated {list(result.keys())}")
            _touch_runtime_task("macro_cache", success=True)
        except ImportError as exc:
            logger.info(f"macro_cache_worker: dependency missing, disabling worker: {exc}")
            _touch_runtime_task("macro_cache", success=False)
            return
        except Exception as exc:
            logger.debug(f"macro_cache_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _glassnode_worker(stop_event: asyncio.Event) -> None:
    """Update Glassnode on-chain cache every 4h (no-op without GLASSNODE_API_KEY)."""
    INTERVAL = 4 * 3600
    await asyncio.sleep(240)  # stagger: 4 min after startup
    while not stop_event.is_set():
        try:
            from core.data.glassnode_collector import update_glassnode_cache  # noqa: PLC0415
            result = await update_glassnode_cache()
            if result:
                logger.debug(f"glassnode_worker: updated {list(result.keys())}")
            _touch_runtime_task("glassnode", success=True)
        except ImportError as exc:
            logger.info(f"glassnode_worker: dependency missing, disabling worker: {exc}")
            _touch_runtime_task("glassnode", success=False)
            return
        except Exception as exc:
            logger.debug(f"glassnode_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _cryptoquant_worker(stop_event: asyncio.Event) -> None:
    """Update CryptoQuant on-chain cache every 4h (no-op without CRYPTOQUANT_API_KEY)."""
    INTERVAL = 4 * 3600
    await asyncio.sleep(270)  # stagger: 4.5 min after startup
    while not stop_event.is_set():
        try:
            from core.data.cryptoquant_collector import update_cryptoquant_cache  # noqa: PLC0415
            result = await update_cryptoquant_cache()
            if result:
                logger.debug(f"cryptoquant_worker: updated {list(result.keys())}")
            _touch_runtime_task("cryptoquant", success=True)
        except ImportError as exc:
            logger.info(f"cryptoquant_worker: dependency missing, disabling worker: {exc}")
            _touch_runtime_task("cryptoquant", success=False)
            return
        except Exception as exc:
            logger.debug(f"cryptoquant_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _nansen_worker(stop_event: asyncio.Event) -> None:
    """Update Nansen smart-money cache every 4h (no-op without NANSEN_API_KEY)."""
    INTERVAL = 4 * 3600
    await asyncio.sleep(300)  # stagger: 5 min after startup
    while not stop_event.is_set():
        try:
            from core.data.nansen_collector import update_nansen_cache  # noqa: PLC0415
            result = await update_nansen_cache()
            if result:
                logger.debug(f"nansen_worker: updated {list(result.keys())}")
            _touch_runtime_task("nansen", success=True)
        except Exception as exc:
            logger.debug(f"nansen_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _kaiko_worker(stop_event: asyncio.Event) -> None:
    """Update Kaiko microstructure cache every 1h (no-op without KAIKO_API_KEY)."""
    INTERVAL = 3600
    await asyncio.sleep(330)  # stagger: 5.5 min after startup
    while not stop_event.is_set():
        try:
            from core.data.kaiko_collector import update_kaiko_cache  # noqa: PLC0415
            result = await update_kaiko_cache()
            if result:
                logger.debug(f"kaiko_worker: updated {list(result.keys())}")
            _touch_runtime_task("kaiko", success=True)
        except Exception as exc:
            logger.debug(f"kaiko_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _coinglass_worker(stop_event: asyncio.Event) -> None:
    """Refresh CoinGlass premium cache in the background (no-op when disabled).

    Each refresh round can fire >30 requests (3 symbols × ~10 datasets), which
    saturates COINGLASS_RATE_LIMIT_PER_MIN=30 in a single shot. At INTERVAL=180s
    that meant a 429 every cycle (576/day in production logs). Stretching to
    600s lets the minute-budget recover between cycles and stops the steady
    rate-limit alarms.
    """
    INTERVAL = 600
    await asyncio.sleep(360)  # stagger: 6 min after startup
    while not stop_event.is_set():
        try:
            from core.data.coinglass_feature_builder import update_coinglass_cache  # noqa: PLC0415

            result = await update_coinglass_cache(max_symbols_per_run=3, manual=False)
            if result.get("updated"):
                logger.debug(
                    "coinglass_worker: updated {} datasets for {}",
                    len(result.get("updated") or []),
                    ",".join(result.get("symbols") or []),
                )
            _touch_runtime_task("coinglass", success=True)
        except Exception as exc:
            logger.debug(f"coinglass_worker: {exc}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _exchange_watchdog_worker(stop_event: asyncio.Event) -> None:
    """Periodically health-check all exchanges and reconnect any that have dropped.

    Runs every 60 s. A failed health-check triggers one reconnect attempt per
    exchange; subsequent failures are retried on the next cycle with exponential
    back-off (max 5 min between attempts for a persistently-failing exchange).
    """
    _INTERVAL = 60
    _MAX_BACKOFF = 300  # 5 min
    # {exchange_name: next_attempt_monotonic_time}
    _backoff_until: Dict[str, float] = {}

    await asyncio.sleep(30)  # let exchange_manager.initialize() finish first

    while not stop_event.is_set():
        try:
            _touch_runtime_task("exchange_watchdog", success=True)
            now = asyncio.get_event_loop().time()
            health = await exchange_manager.health_check()
            for name, healthy in health.items():
                if healthy:
                    _backoff_until.pop(name, None)  # reset backoff on recovery
                    continue
                # Check backoff
                retry_at = _backoff_until.get(name, 0.0)
                if now < retry_at:
                    logger.debug(
                        f"exchange_watchdog: {name} unhealthy, retry in "
                        f"{retry_at - now:.0f}s"
                    )
                    continue
                logger.warning(f"exchange_watchdog: {name} unhealthy, attempting reconnect")
                ok = await exchange_manager.reconnect_exchange(name)
                if not ok:
                    # Exponential backoff: 60 → 120 → 240 → 300 → 300...
                    prior = _backoff_until.get(name, now)
                    gap = max(_INTERVAL, min(prior - now + _INTERVAL * 2, _MAX_BACKOFF))
                    _backoff_until[name] = now + gap
                    logger.warning(
                        f"exchange_watchdog: {name} reconnect failed, "
                        f"next retry in {gap:.0f}s"
                    )
        except Exception as exc:
            logger.error(f"exchange_watchdog: unexpected error: {exc}")

        await asyncio.sleep(_INTERVAL)


async def _cusum_monitor_worker(stop_event: asyncio.Event, app: FastAPI) -> None:
    """Periodically scan all running candidates for CUSUM decay (every 5 min)."""
    from core.monitoring.cusum_watcher import run_cusum_checks_for_all_candidates

    INTERVAL = 300  # 5 minutes
    await asyncio.sleep(30)  # stagger startup
    while not stop_event.is_set():
        try:
            reports = await run_cusum_checks_for_all_candidates(app)
            if reports:
                logger.info(f"CUSUM watcher: {len(reports)} decay trigger(s) detected and processed")
            _touch_runtime_task("cusum_monitor", success=True)
        except Exception as e:
            logger.warning(f"CUSUM watcher error: {e}")
        for _ in range(INTERVAL):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _circuit_breaker_monitor_worker(stop_event: asyncio.Event, app: FastAPI) -> None:
    """Periodically evaluate per-strategy + portfolio drawdown for the circuit breaker."""
    from core.risk.circuit_breaker import (
        run_circuit_breaker_checks,
        register_close_positions_hook,
        circuit_breaker,
    )

    if not bool(getattr(settings, "CIRCUIT_BREAKER_ENABLED", True)):
        logger.info("circuit_breaker_monitor: disabled via settings, worker idling")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    monitor_loop = asyncio.get_running_loop()

    # Wire close-positions hook (best-effort — strategy_manager may not have it yet)
    try:
        from core.strategies.strategy_manager import strategy_manager as _sm

        def _hook(strategy_name: str, reason: str) -> Any:
            closer = getattr(_sm, "_close_positions_for_strategy_stop", None)
            if closer is None:
                return None
            return closer(strategy_name, reason=reason)

        register_close_positions_hook(_hook)
    except Exception as exc:
        logger.debug(f"circuit_breaker_monitor: could not register close hook: {exc}")

    # Notification listener (best-effort)
    try:
        from core.notifications import notification_manager  # noqa: PLC0415

        def _notify(event: str, payload: Dict[str, Any]) -> None:
            if event not in {"strategy_tripped", "portfolio_tripped"}:
                return
            title = (
                "⚠ 组合熔断触发"
                if event == "portfolio_tripped"
                else f"⚠ 策略熔断: {payload.get('strategy')}"
            )
            msg = (
                f"原因: {payload.get('reason')}\n"
                f"24h回撤: {float(payload.get('daily_dd') or 0.0)*100:.2f}%\n"
                f"7d回撤: {float(payload.get('weekly_dd') or 0.0)*100:.2f}%"
            )
            try:
                asyncio.run_coroutine_threadsafe(
                    notification_manager.send_message(
                        title=title, message=msg, channels=["feishu", "telegram"]
                    ),
                    monitor_loop,
                )
            except Exception:
                pass

        circuit_breaker.add_listener(_notify)
    except Exception as exc:
        logger.debug(f"circuit_breaker_monitor: notification listener not wired: {exc}")

    interval = max(15, int(getattr(settings, "CB_MONITOR_INTERVAL_SEC", 60) or 60))
    await asyncio.sleep(45)  # stagger startup after CUSUM
    while not stop_event.is_set():
        try:
            report = await asyncio.to_thread(run_circuit_breaker_checks)
            # `trip_strategy` fires the close-positions hook itself; the
            # circuit breaker schedules async hooks back onto this loop.
            new_strategy_trips = [
                t for t in (report.get("strategy_trips") or []) if t.get("new_trip")
            ]
            if new_strategy_trips or report.get("portfolio_trip"):
                logger.warning(
                    "circuit_breaker_monitor: trip detected "
                    f"strategy={len(new_strategy_trips)} "
                    f"portfolio={'yes' if report.get('portfolio_trip') else 'no'}"
                )
            _touch_runtime_task("circuit_breaker_monitor", success=True)
        except Exception as e:
            logger.warning(f"circuit_breaker_monitor error: {e}")
        for _ in range(interval):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _data_maintenance_worker(stop_event: asyncio.Event) -> None:
    await asyncio.sleep(10)
    while not stop_event.is_set():
        started = datetime.now(timezone.utc)
        try:
            # Hard 5-minute cap; a hung exchange call must not block the entire worker forever.
            result = await asyncio.wait_for(_run_data_maintenance_once(), timeout=300)
            logger.info(
                "Background data maintenance done: "
                f"sync={result.get('market_sync_count', 0)}, "
                f"duration={result.get('duration_sec', 0)}s"
            )
            _touch_runtime_task("data_maintenance", success=True)
        except asyncio.TimeoutError:
            logger.warning("Background data maintenance timed out after 300s")
            _save_maintenance_snapshot(
                "maintenance_error",
                {"timestamp": datetime.now(timezone.utc).isoformat(), "error": "timeout after 300s"},
            )
        except Exception as e:
            logger.warning(f"Background data maintenance failed: {e}")
            _save_maintenance_snapshot(
                "maintenance_error",
                {"timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)},
            )

        # Run every 6 hours after one full pass.
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        sleep_seconds = max(300, int(6 * 3600 - elapsed))
        for _ in range(sleep_seconds):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _ai_research_scheduler_worker(app: FastAPI, stop_event: asyncio.Event) -> None:
    from core.ai.research_scheduler import research_scheduler
    from core.research.orchestrator import ensure_ai_research_runtime_state

    ensure_ai_research_runtime_state(app)
    research_scheduler.set_app(app)
    research_scheduler.start()
    try:
        while not stop_event.is_set():
            _touch_runtime_task("ai_research_scheduler", success=True)
            await asyncio.sleep(5)
    finally:
        with contextlib.suppress(Exception):
            await research_scheduler.stop()


def _build_runtime_task_factories(app: FastAPI) -> Dict[str, Dict[str, Any]]:
    factories: Dict[str, Dict[str, Any]] = {
        "runtime": {
            "factory": lambda stop_event: _runtime_pusher(stop_event),
            "restart_on_failure": True,
        },
        "ai_research_scheduler": {
            "factory": lambda stop_event: _ai_research_scheduler_worker(app, stop_event),
            "restart_on_failure": True,
        },
        "cusum_monitor": {
            "factory": lambda stop_event: _cusum_monitor_worker(stop_event, app),
            "restart_on_failure": True,
        },
        "circuit_breaker_monitor": {
            "factory": lambda stop_event: _circuit_breaker_monitor_worker(stop_event, app),
            "restart_on_failure": True,
        },
    }
    if _COINGLASS_WORKER_ENABLED:
        factories["coinglass"] = {
            "factory": lambda stop_event: _coinglass_worker(stop_event),
            "restart_on_failure": False,
        }
    if _EXCHANGE_WATCHDOG_ENABLED:
        factories["exchange_watchdog"] = {
            "factory": lambda stop_event: _exchange_watchdog_worker(stop_event),
            "restart_on_failure": True,  # must stay alive for the session lifetime
        }
    if _is_market_ws_stream_enabled():
        factories["market_ws_feed"] = {
            "factory": lambda stop_event: _market_ws_feed_worker(stop_event),
            # Keep retrying: it may start before exchanges finish connecting,
            # and we want it to recover across transient socket failures.
            "restart_on_failure": True,
        }
    if _PUBLIC_MACRO_WORKERS_ENABLED:
        factories.update(
            {
                "google_trends": {
                    "factory": lambda stop_event: _google_trends_worker(stop_event),
                    "restart_on_failure": False,
                },
                "macro_cache": {
                    "factory": lambda stop_event: _macro_cache_worker(stop_event),
                    "restart_on_failure": False,
                },
            }
        )
    if _PREMIUM_EXTERNAL_WORKERS_ENABLED:
        factories.update(
            {
                "glassnode": {
                    "factory": lambda stop_event: _glassnode_worker(stop_event),
                    "restart_on_failure": False,
                },
                "cryptoquant": {
                    "factory": lambda stop_event: _cryptoquant_worker(stop_event),
                    "restart_on_failure": False,
                },
                "nansen": {
                    "factory": lambda stop_event: _nansen_worker(stop_event),
                    "restart_on_failure": False,
                },
                "kaiko": {
                    "factory": lambda stop_event: _kaiko_worker(stop_event),
                    "restart_on_failure": False,
                },
            }
        )
    if _DATA_MAINTENANCE_ENABLED:
        factories["data_maintenance"] = {
            "factory": lambda stop_event: _data_maintenance_worker(stop_event),
            "restart_on_failure": True,
        }
    if _NEWS_BACKGROUND_ENABLED and not _EXTERNAL_NEWS_WORKER_ENABLED:
        factories["news"] = {
            "factory": lambda stop_event: _news_refresh_worker(app, stop_event),
            "restart_on_failure": True,
        }
    if _NEWS_LLM_BACKGROUND_ENABLED and not _NEWS_LLM_EXTERNAL_ONLY:
        factories["news_llm"] = {
            "factory": lambda stop_event: _news_llm_worker(app, stop_event),
            "restart_on_failure": True,
        }
    if _ANALYTICS_HISTORY_ENABLED:
        for collector, interval_sec, startup_delay_sec in _ANALYTICS_HISTORY_WORKER_SPECS:
            factories[f"analytics_history_{collector}"] = {
                "factory": lambda stop_event, collector=collector, interval_sec=interval_sec, startup_delay_sec=startup_delay_sec: _analytics_history_worker(
                    app,
                    stop_event,
                    collector=collector,
                    interval_sec=interval_sec,
                    exchange=_ANALYTICS_HISTORY_DEFAULT_EXCHANGE,
                    symbol=_ANALYTICS_HISTORY_DEFAULT_SYMBOL,
                    depth_limit=80,
                    startup_delay_sec=startup_delay_sec,
                ),
                "restart_on_failure": True,
            }
    return factories


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Crypto Trading System...")

    await runtime_bootstrap.initialize_shared_runtime(
        include_news=True,
        initialize_exchanges=True,
    )

    try:
        from web.api import news as news_api

        app.state.news_cfg = news_api.load_news_cfg()
    except Exception as e:
        logger.warning(f"Load news config failed: {e}")
        app.state.news_cfg = {}

    await initialize_ops_runtime(app, standalone=False)

    global _startup_mode_decision
    main_account: Dict[str, Any] = {}
    try:
        main_account = account_manager.get_account("main") or {}
    except Exception as e:
        logger.warning(f"Failed to restore trading mode from account config: {e}")
        main_account = {}

    _startup_mode_decision = resolve_startup_trading_mode(
        configured_mode=settings.TRADING_MODE,
        persisted_account=main_account,
        allow_persisted_live_mode_start=bool(settings.ALLOW_PERSISTED_LIVE_MODE_START),
    )
    if _startup_mode_decision.blocked_persisted_live_restore:
        logger.warning(
            "Blocked persisted live-mode restore during startup. "
            "Set ALLOW_PERSISTED_LIVE_MODE_START=true or TRADING_MODE=live to allow live startup."
        )

    runtime_state.initialize_mode(_startup_mode_decision.effective_mode, reason="lifespan.startup")
    execution_engine.set_paper_trading(
        _startup_mode_decision.effective_mode != "live",
        sync_runtime_state=False,
    )
    _sync_startup_account_modes(_startup_mode_decision)
    logger.info(
        "Startup trading mode resolved: effective={}, configured={}, persisted={}, source={}",
        _startup_mode_decision.effective_mode,
        _startup_mode_decision.configured_mode,
        _startup_mode_decision.persisted_mode or "unset",
        _startup_mode_decision.source,
    )
    await execution_engine.start()

    if not getattr(app.state, "strategy_signal_hooked", False):
        strategy_manager.register_signal_callback(execution_engine.submit_signal)
        app.state.strategy_signal_hooked = True

    if not getattr(app.state, "strategy_signal_pushed", False):
        strategy_manager.register_signal_callback(_on_strategy_signal)
        app.state.strategy_signal_pushed = True

    if not getattr(app.state, "runtime_callbacks_hooked", False):
        execution_engine.register_callback(_on_execution_event)
        order_manager.register_callback(_on_order_event)
        position_manager.register_callback(_on_position_event)
        app.state.runtime_callbacks_hooked = True

    restore_result = await restore_strategies_from_db(
        startup_mode=_startup_mode_decision.effective_mode,
        allow_live_restore=_startup_mode_decision.effective_mode == "live",
    )
    logger.info(
        "Strategy restore summary: "
        f"loaded={restore_result.get('loaded', 0)}, "
        f"restored={restore_result.get('restored', 0)}, "
        f"started={restore_result.get('started', 0)}, "
        f"paused={restore_result.get('paused', 0)}, "
        f"skipped={len(restore_result.get('skipped', []))}"
    )
    await strategy_health_monitor.start()
    app.state.news_last_llm_batch = None
    app.state.analytics_history_last_runs = {}
    app.state.runtime_supervisor = RuntimeTaskSupervisor(runtime_state)
    app.state.runtime_task_factories = _build_runtime_task_factories(app)
    app.state.analytics_history_stop_events = {}
    app.state.analytics_history_tasks = {}
    for name, item in app.state.runtime_task_factories.items():
        managed = app.state.runtime_supervisor.start_task(
            name,
            item["factory"],
            restart_on_failure=bool(item.get("restart_on_failure", False)),
        )
        setattr(app.state, f"{name}_task", managed.task)
        setattr(app.state, f"{name}_stop_event", managed.stop_event)
        if name.startswith("analytics_history_"):
            collector = name.replace("analytics_history_", "", 1)
            app.state.analytics_history_stop_events[collector] = managed.stop_event
            app.state.analytics_history_tasks[collector] = managed.task
    logger.info(
        "Managed background tasks started: "
        + ", ".join(sorted(app.state.runtime_task_factories.keys()))
    )
    if bool(getattr(settings, "AI_AUTONOMOUS_AGENT_AUTO_START", False)):
        with contextlib.suppress(Exception):
            await autonomous_trading_agent.update_runtime_config(enabled=True)
            await autonomous_trading_agent.start()
    with contextlib.suppress(Exception):
        agent_cfg = autonomous_trading_agent.get_runtime_config()
        if str(agent_cfg.get("symbol_mode") or "manual").strip().lower() == "auto":
            autonomous_trading_agent.ensure_symbol_scan_preview_warm(
                limit=int(agent_cfg.get("selection_top_n") or 10),
                force=False,
            )
    with contextlib.suppress(Exception):
        await _emit_news_preview(app=app, limit=10, hours=24)

    logger.info("System started successfully")
    yield

    logger.info("Shutting down Crypto Trading System...")
    supervisor: RuntimeTaskSupervisor | None = getattr(app.state, "runtime_supervisor", None)
    if supervisor is not None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(supervisor.stop_all(timeout_sec=6.0), timeout=15)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(autonomous_trading_agent.stop(), timeout=15)

    with contextlib.suppress(Exception):
        await asyncio.wait_for(strategy_health_monitor.stop(), timeout=15)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(shutdown_ops_runtime(app, standalone=False), timeout=15)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(
            strategy_manager.stop_all(close_positions=False, reason="service_shutdown"),
            timeout=15,
        )
    with contextlib.suppress(Exception):
        await asyncio.wait_for(execution_engine.stop(), timeout=15)
    with contextlib.suppress(Exception):
        position_manager.flush()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(
            runtime_bootstrap.shutdown_shared_runtime(
                include_news=True,
                close_exchanges=True,
                close_database=True,
            ),
            timeout=15,
        )

    logger.info("System shutdown complete")


app = FastAPI(
    title="Crypto Trading System",
    description="加密货币交易系统",
    version="1.0.0",
    lifespan=lifespan,
)

_allowed_origins: List[str] = []
try:
    raw = list(getattr(settings, "WEB_ALLOWED_ORIGINS", []) or [])
    _allowed_origins = [str(o).strip() for o in raw if str(o).strip() and str(o).strip() != "*"]
except Exception:
    _allowed_origins = []
if not _allowed_origins:
    _allowed_origins = ["http://localhost:8000", "http://127.0.0.1:8000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

static_path = Path(__file__).parent / "static"
static_path.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

templates_path = Path(__file__).parent / "templates"
templates_path.mkdir(parents=True, exist_ok=True)
templates = Jinja2Templates(directory=str(templates_path))
templates.env.globals["static_asset_url"] = static_asset_url

from web.api import (
    altcoin,
    ai_agent,
    backtest,
    data,
    news,
    notifications,
    research,
    risk as risk_api,
    strategies,
    trading_accounts,
    trading_analytics,
    trading_balances,
    trading_orders,
    trading_positions,
    trading_runtime,
)

app.include_router(trading_orders.router, prefix="/api/trading", tags=["trading"])
app.include_router(trading_positions.router, prefix="/api/trading", tags=["trading"])
app.include_router(trading_accounts.router, prefix="/api/trading", tags=["trading"])
app.include_router(trading_balances.router, prefix="/api/trading", tags=["trading"])
app.include_router(trading_analytics.router, prefix="/api/trading", tags=["trading"])
app.include_router(trading_runtime.router, prefix="/api/trading", tags=["trading"])
app.include_router(data.router, prefix="/api/data", tags=["data"])
app.include_router(altcoin.router, prefix="/api/altcoin", tags=["altcoin"])
app.include_router(research.router, prefix="/api/research", tags=["research"])
app.include_router(ai_research.router, prefix="/api/ai", tags=["ai_research"])
app.include_router(ai_agent.router, prefix="/api/ai", tags=["ai_agent"])
app.include_router(strategies.router, prefix="/api/strategies", tags=["strategies"])
app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
app.include_router(notifications.router, prefix="/api/notifications", tags=["notifications"])
app.include_router(news.router, prefix="/api/news", tags=["news"])
app.include_router(ml.router, prefix="/api/ml", tags=["ml"])
app.include_router(risk_api.router, prefix="/api/risk", tags=["risk"])
app.include_router(create_ops_router())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    response = templates.TemplateResponse("index.html", {"request": request})
    set_local_ui_session_cookie(request, response)
    return response


@app.get("/news", response_class=HTMLResponse)
async def news_page(request: Request):
    response = templates.TemplateResponse("news.html", {"request": request})
    set_local_ui_session_cookie(request, response)
    return response


@app.get("/ai")
async def ai_page(request: Request):
    return RedirectResponse(url="/?tab=ai-research", status_code=307)


@app.get("/api/status", dependencies=[Depends(require_sensitive_ops_permissions("read_trading_state"))])
async def get_status():
    global _status_cache_payload, _status_cache_at
    now_mono = time.monotonic()
    if _status_cache_payload is not None and (now_mono - _status_cache_at) <= _STATUS_CACHE_TTL_SEC:
        return _status_cache_payload
    try:
        exchange_targets = ["gate", "binance", "okx"]
        exchange_default_type: Dict[str, str] = {}
        exchange_status = {
            name: bool(getattr(exchange_manager.get_exchange(name), "is_connected", False))
            for name in exchange_targets
        }
        for name in exchange_targets:
            connector = exchange_manager.get_exchange(name)
            default_type = str(getattr(getattr(connector, "config", None), "default_type", "") or "").strip().lower()
            if not default_type:
                default_type = str(getattr(settings, f"{name.upper()}_DEFAULT_TYPE", "spot") or "spot").lower()
            exchange_default_type[name] = default_type
        connected = [name for name, ok in exchange_status.items() if ok]
        payload = {
            "status": "running",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "trading_mode": execution_engine.get_trading_mode(),
            "paper_trading": execution_engine.is_paper_mode(),
            "runtime": {
                "account_scope": runtime_state.get_account_scope(),
                "task_count": len(runtime_state.get_task_diagnostics()),
                "last_mode_switch_at": runtime_state.snapshot().get("last_mode_switch_at"),
                "analytics_history_enabled": bool(_ANALYTICS_HISTORY_ENABLED),
                "analytics_history_collectors": (
                    [collector for collector, _, _ in _ANALYTICS_HISTORY_WORKER_SPECS]
                    if _ANALYTICS_HISTORY_ENABLED
                    else []
                ),
                "startup_mode": {
                    "configured_mode": (
                        _startup_mode_decision.configured_mode if _startup_mode_decision else settings.TRADING_MODE
                    ),
                    "persisted_mode": (
                        _startup_mode_decision.persisted_mode if _startup_mode_decision else ""
                    ),
                    "source": _startup_mode_decision.source if _startup_mode_decision else "unknown",
                    "blocked_persisted_live_restore": bool(
                        _startup_mode_decision.blocked_persisted_live_restore if _startup_mode_decision else False
                    ),
                },
            },
            "execution_engine": {
                "running": bool(execution_engine.is_running),
                "queue_size": int(execution_engine.get_queue_size()),
                "queue_worker_alive": bool(execution_engine.is_queue_worker_alive()),
                "signal_diagnostics": execution_engine.get_signal_diagnostics(),
            },
            "paper_cost_model": {
                "initial_equity": float(settings.PAPER_INITIAL_EQUITY or 0.0),
                "fee_rate": float(settings.PAPER_FEE_RATE or 0.0),
                "slippage_bps": float(settings.PAPER_SLIPPAGE_BPS or 0.0),
                "min_strategy_order_usd": float(settings.MIN_STRATEGY_ORDER_USD or 0.0),
            },
            "exchanges": connected,
            "exchange_count": len(connected),
            "total_exchange_count": len(exchange_targets),
            "exchange_targets": exchange_targets,
            "exchange_status": exchange_status,
            "exchange_default_type": exchange_default_type,
            "market_ws": _market_ws_status_snapshot(),
        }
        _status_cache_payload = payload
        _status_cache_at = now_mono
        return payload
    except Exception:
        # Do not break the dashboard status badge if one dependency is temporarily slow/broken.
        if _status_cache_payload is not None:
            return {
                **_status_cache_payload,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": _status_cache_payload.get("status", "running"),
            }
        raise


@app.get("/api/market-data/status", dependencies=[Depends(require_sensitive_ops_permissions("read_trading_state"))])
async def get_market_data_status():
    return _market_ws_status_snapshot(include_symbols=True)


_WS_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _ws_client_ip(websocket: WebSocket) -> str:
    try:
        host = (websocket.client.host if websocket.client else "") or ""
    except Exception:
        host = ""
    text = str(host).strip().lower().strip("[]")
    if text.startswith("::ffff:"):
        text = text.split("::ffff:", 1)[1]
    return text


def _ws_is_authorized(websocket: WebSocket) -> bool:
    """Authorize a WebSocket before accepting.

    Allow when EITHER:
      - the request bears a valid local-UI session cookie (`cts_local_ui_session`), OR
      - the request includes a valid Ops token (header `X-Ops-Token` or `Authorization: Bearer ...`)

    Loopback requests without any credentials are also allowed (legacy local-only UX),
    but non-loopback requests without credentials are rejected.
    """
    try:
        if _has_valid_local_ui_session(websocket):
            return True
    except Exception:
        pass
    try:
        from core.ops.service.auth import get_ops_token  # noqa: PLC0415
        expected = str(get_ops_token(required=False) or "").strip()
    except Exception:
        expected = ""
    if expected:
        header_token = str(websocket.headers.get("x-ops-token") or "").strip()
        auth_header = str(websocket.headers.get("authorization") or "").strip()
        bearer = ""
        if auth_header.lower().startswith("bearer "):
            bearer = auth_header[7:].strip()
        if header_token and header_token == expected:
            return True
        if bearer and bearer == expected:
            return True
    # Loopback fallback: preserve existing local UI experience
    if _ws_client_ip(websocket) in _WS_LOOPBACK_HOSTS:
        return True
    return False


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    if not _ws_is_authorized(websocket):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    queue = await event_bus.subscribe(maxsize=300)
    await websocket.send_json(
        {
            "event": "hello",
            "payload": {
                "mode": execution_engine.get_trading_mode(),
                "server_time": datetime.now(timezone.utc).isoformat(),
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    recv_task: Optional[asyncio.Task] = None
    send_task: Optional[asyncio.Task] = None
    try:
        while True:
            recv_task = asyncio.create_task(websocket.receive_text())
            send_task = asyncio.create_task(queue.get())
            try:
                done, pending = await asyncio.wait(
                    {recv_task, send_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except BaseException:
                # asyncio.wait was cancelled — cancel both children before propagating
                for t in (recv_task, send_task):
                    if t and not t.done():
                        t.cancel()
                        with contextlib.suppress(BaseException):
                            await t
                recv_task = send_task = None
                raise

            try:
                if send_task in done:
                    payload = send_task.result()
                    await websocket.send_json(payload)

                if recv_task in done:
                    message = (recv_task.result() or "").strip().lower()
                    if message in {"ping", "heartbeat"}:
                        await websocket.send_json(
                            {
                                "event": "pong",
                                "payload": {"server_time": datetime.now(timezone.utc).isoformat()},
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                    elif message == "status":
                        await _emit_runtime_snapshot()
            finally:
                # Always cancel/await the pending task so the loop never leaks
                # a queue.get() or receive_text() across iterations or on
                # WebSocketDisconnect raised from send_json.
                for task in pending:
                    if not task.done():
                        task.cancel()
                    with contextlib.suppress(BaseException):
                        await task
                # Retrieve the result of every completed task. When a disconnect
                # completes both recv_task and send_task in the same wait() and
                # the send path raises, the unconsumed task's exception would
                # otherwise be reported as "Task exception was never retrieved".
                for task in done:
                    with contextlib.suppress(BaseException):
                        task.exception()
                recv_task = send_task = None
    except WebSocketDisconnect:
        pass
    finally:
        # Belt-and-braces: if we exited via an exception path before the inner
        # finally ran, ensure no child task is left dangling.
        for t in (recv_task, send_task):
            if t and not t.done():
                t.cancel()
                with contextlib.suppress(BaseException):
                    await t
        await event_bus.unsubscribe(queue)


@app.get("/livez")
async def livez_check():
    """Liveness probe: always returns 200 if the process is up."""
    return {"status": "alive", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/readyz")
async def readyz_check():
    """Readiness probe: 200 only when DB ping + exchanges + strategy manager are usable."""
    from fastapi.responses import JSONResponse  # noqa: PLC0415
    checks: Dict[str, Any] = {}
    overall_ready = True
    # DB ping
    try:
        from config.database import async_session_maker  # noqa: PLC0415
        from sqlalchemy import text  # noqa: PLC0415
        async with async_session_maker() as session:
            await session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:
        overall_ready = False
        checks["db"] = f"error: {exc}"
    # Exchanges
    try:
        connected = getattr(exchange_manager, "connected_exchanges", None)
        if callable(connected):
            connected_list = list(connected() or [])
        else:
            connected_list = [n for n in ("gate", "binance", "okx")
                              if bool(getattr(exchange_manager.get_exchange(n), "is_connected", False))]
        if not connected_list:
            overall_ready = False
            checks["exchanges"] = "no connected exchanges"
        else:
            checks["exchanges"] = connected_list
    except Exception as exc:
        overall_ready = False
        checks["exchanges"] = f"error: {exc}"
    # Strategy manager
    try:
        sm_running = bool(getattr(strategy_manager, "is_running", True))
        checks["strategy_manager"] = "ok" if sm_running else "not_running"
        if not sm_running:
            overall_ready = False
    except Exception as exc:
        overall_ready = False
        checks["strategy_manager"] = f"error: {exc}"

    body = {
        "status": "ready" if overall_ready else "not_ready",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
    }
    if not overall_ready:
        return JSONResponse(status_code=503, content=body)
    return body


@app.get("/health")
async def health_check():
    """Alias of /livez for backwards compatibility. Use /livez or /readyz instead."""
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "web.main:app",
        host=settings.WEB_HOST,
        port=settings.WEB_PORT,
        reload=True,
    )
