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
from typing import Any, Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from config.database import close_db, init_db
from config.settings import settings
from core.ai.autonomous_agent import autonomous_trading_agent

# Sync LLM API keys to environment variables for modules that use os.environ.get()
if settings.ZHIPU_API_KEY:
    os.environ["ZHIPU_API_KEY"] = settings.ZHIPU_API_KEY
if settings.ZHIPU_BASE_URL:
    os.environ["ZHIPU_BASE_URL"] = settings.ZHIPU_BASE_URL
if settings.ZHIPU_MODEL:
    os.environ["ZHIPU_MODEL"] = settings.ZHIPU_MODEL
if settings.OPENAI_API_KEY:
    os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY
if settings.OPENAI_BASE_URL:
    os.environ["OPENAI_BASE_URL"] = settings.OPENAI_BASE_URL
if settings.OPENAI_BACKUP_API_KEY:
    os.environ["OPENAI_BACKUP_API_KEY"] = settings.OPENAI_BACKUP_API_KEY
if settings.OPENAI_BACKUP_BASE_URL:
    os.environ["OPENAI_BACKUP_BASE_URL"] = settings.OPENAI_BACKUP_BASE_URL
if settings.OPENAI_MODEL:
    os.environ["OPENAI_MODEL"] = settings.OPENAI_MODEL

from core.data import data_storage, second_level_backfill_manager
from core.exchanges import exchange_manager
from core.news.storage import db as news_db
from core.ops.service import create_router as create_ops_router, initialize_ops_runtime, shutdown_ops_runtime
from core.realtime import event_bus
from core.runtime import RuntimeTaskSupervisor, runtime_state
from core.strategies import (
    restore_strategies_from_db,
    strategy_health_monitor,
    strategy_manager,
)
from core.trading import account_manager, execution_engine, order_manager, position_manager
from web.api import ai_research

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


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


_NEWS_PULL_INTERVAL_SEC = max(20, _env_int("NEWS_PULL_INTERVAL_SEC", 60))
_NEWS_PULL_SINCE_MINUTES = max(30, _env_int("NEWS_PULL_SINCE_MINUTES", 180))
_NEWS_PULL_MAX_RECORDS = max(20, _env_int("NEWS_PULL_MAX_RECORDS", 80))
_NEWS_BACKGROUND_ENABLED = _env_bool("NEWS_BACKGROUND_ENABLED", True)
_NEWS_LLM_INTERVAL_SEC = max(15, _env_int("NEWS_LLM_INTERVAL_SEC", 60))
_NEWS_LLM_BATCH = max(1, min(12, _env_int("NEWS_LLM_BATCH", 4)))
_NEWS_LLM_BACKGROUND_ENABLED = _env_bool("NEWS_LLM_BACKGROUND_ENABLED", True)
_EXTERNAL_NEWS_WORKER_ENABLED = _env_bool("START_NEWS_WORKER", False)
_EXTERNAL_NEWS_LLM_WORKER_ENABLED = _env_bool("START_NEWS_LLM_WORKER", False)
_DATA_MAINTENANCE_ENABLED = _env_bool("DATA_MAINTENANCE_ENABLED", False)
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


def _touch_runtime_task(task_name: str, *, success: bool = False) -> None:
    runtime_state.touch_task(task_name, success=success)


def _safe_json(obj: Any) -> Dict[str, Any]:
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return {"raw": str(obj)}


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
    await event_bus.publish_nowait_safe(
        event="execution_event",
        payload={"event": event, "data": _safe_json(data)},
    )


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
    return list(symbols)[:8]


async def _emit_market_ticks() -> None:
    if not event_bus.has_subscribers():
        return
    symbols = _collect_watch_symbols()
    if not symbols:
        return

    payload: Dict[str, Dict[str, Any]] = {}
    for exchange_name in exchange_manager.get_connected_exchanges():
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            continue

        ticks: Dict[str, Any] = {}
        for symbol in symbols:
            try:
                ticker = await connector.get_ticker(symbol)
                ticks[symbol] = {
                    "last": float(ticker.last or 0.0),
                    "bid": float(ticker.bid or 0.0),
                    "ask": float(ticker.ask or 0.0),
                    "timestamp": ticker.timestamp.isoformat() if ticker.timestamp else None,
                }
            except Exception:
                continue
        if ticks:
            payload[exchange_name] = ticks

    if payload:
        await event_bus.publish_nowait_safe(event="market_tick", payload=payload)


async def _runtime_pusher(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            if not event_bus.has_subscribers():
                await asyncio.sleep(2)
                continue
            await _emit_runtime_snapshot()
            await _emit_market_ticks()
            _touch_runtime_task("runtime", success=True)
        except Exception as e:
            logger.debug(f"runtime snapshot push failed: {e}")
        await asyncio.sleep(2)


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

    await asyncio.sleep(5)
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

        for _ in range(sleep_seconds):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


async def _news_llm_worker(app: FastAPI, stop_event: asyncio.Event) -> None:
    from web.api import news as news_api

    await asyncio.sleep(8)
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

        for _ in range(_NEWS_LLM_INTERVAL_SEC):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


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
        for _ in range(sleep_span):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)


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
            chain="Ethereum",
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


async def _data_maintenance_worker(stop_event: asyncio.Event) -> None:
    await asyncio.sleep(10)
    while not stop_event.is_set():
        started = datetime.now(timezone.utc)
        try:
            result = await _run_data_maintenance_once()
            logger.info(
                "Background data maintenance done: "
                f"sync={result.get('market_sync_count', 0)}, "
                f"duration={result.get('duration_sec', 0)}s"
            )
            _touch_runtime_task("data_maintenance", success=True)
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
        "google_trends": {
            "factory": lambda stop_event: _google_trends_worker(stop_event),
            "restart_on_failure": False,  # non-critical; don't spam restarts on 429s
        },
        "macro_cache": {
            "factory": lambda stop_event: _macro_cache_worker(stop_event),
            "restart_on_failure": False,
        },
        "glassnode": {
            "factory": lambda stop_event: _glassnode_worker(stop_event),
            "restart_on_failure": False,  # no-op without key
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
    if _NEWS_LLM_BACKGROUND_ENABLED and not _EXTERNAL_NEWS_LLM_WORKER_ENABLED:
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

    await init_db()
    await news_db.init_news_db()
    await data_storage.initialize()
    await exchange_manager.initialize()

    try:
        from web.api import news as news_api

        app.state.news_cfg = news_api.load_news_cfg()
    except Exception as e:
        logger.warning(f"Load news config failed: {e}")
        app.state.news_cfg = {}

    await initialize_ops_runtime(app, standalone=False)

    restored_mode = "paper"
    try:
        main_account = account_manager.get_account("main") or {}
        restored_mode = str(main_account.get("mode") or settings.TRADING_MODE or "paper").strip().lower()
        if restored_mode not in {"paper", "live"}:
            restored_mode = "paper"
    except Exception as e:
        logger.warning(f"Failed to restore trading mode from account config: {e}")
        restored_mode = str(settings.TRADING_MODE or "paper").strip().lower()
        if restored_mode not in {"paper", "live"}:
            restored_mode = "paper"

    runtime_state.initialize_mode(restored_mode, reason="lifespan.startup")
    execution_engine.set_paper_trading(restored_mode != "live", sync_runtime_state=False)
    logger.info(f"Startup trading mode restored: {restored_mode}")
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

    restore_result = await restore_strategies_from_db()
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
        await _emit_news_preview(app=app, limit=10, hours=24)

    logger.info("System started successfully")
    yield

    logger.info("Shutting down Crypto Trading System...")
    supervisor: RuntimeTaskSupervisor | None = getattr(app.state, "runtime_supervisor", None)
    if supervisor is not None:
        await supervisor.stop_all(timeout_sec=6.0)
    with contextlib.suppress(Exception):
        await autonomous_trading_agent.stop()

    await strategy_health_monitor.stop()
    await shutdown_ops_runtime(app, standalone=False)
    await strategy_manager.stop_all()
    await execution_engine.stop()
    await exchange_manager.close_all()
    await data_storage.close()
    await news_db.close_news_db()
    await close_db()

    logger.info("System shutdown complete")


app = FastAPI(
    title="Crypto Trading System",
    description="加密货币交易系统",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_path = Path(__file__).parent / "static"
static_path.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

templates_path = Path(__file__).parent / "templates"
templates_path.mkdir(parents=True, exist_ok=True)
templates = Jinja2Templates(directory=str(templates_path))

from web.api import (
    backtest,
    data,
    news,
    notifications,
    research,
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
app.include_router(research.router, prefix="/api/research", tags=["research"])
app.include_router(ai_research.router, prefix="/api/ai", tags=["ai_research"])
app.include_router(strategies.router, prefix="/api/strategies", tags=["strategies"])
app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
app.include_router(notifications.router, prefix="/api/notifications", tags=["notifications"])
app.include_router(news.router, prefix="/api/news", tags=["news"])
app.include_router(create_ops_router())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/news", response_class=HTMLResponse)
async def news_page(request: Request):
    return templates.TemplateResponse("news.html", {"request": request})


@app.get("/ai")
async def ai_page(request: Request):
    return RedirectResponse(url="/?tab=ai-research", status_code=307)


@app.get("/api/status")
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
            "timestamp": datetime.now().isoformat(),
            "trading_mode": execution_engine.get_trading_mode(),
            "paper_trading": execution_engine.is_paper_mode(),
            "runtime": {
                "account_scope": runtime_state.get_account_scope(),
                "task_count": len(runtime_state.get_task_diagnostics()),
                "last_mode_switch_at": runtime_state.snapshot().get("last_mode_switch_at"),
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
        }
        _status_cache_payload = payload
        _status_cache_at = now_mono
        return payload
    except Exception:
        # Do not break the dashboard status badge if one dependency is temporarily slow/broken.
        if _status_cache_payload is not None:
            return {
                **_status_cache_payload,
                "timestamp": datetime.now().isoformat(),
                "status": _status_cache_payload.get("status", "running"),
            }
        raise


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
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
    try:
        while True:
            recv_task = asyncio.create_task(websocket.receive_text())
            send_task = asyncio.create_task(queue.get())
            done, pending = await asyncio.wait(
                {recv_task, send_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

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

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
    except WebSocketDisconnect:
        pass
    finally:
        await event_bus.unsubscribe(queue)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "web.main:app",
        host=settings.WEB_HOST,
        port=settings.WEB_PORT,
        reload=True,
    )
