from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config.env_utils import env_bool as _env_bool
from config.env_utils import env_int as _env_int
from config.settings import settings
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.async_glm_client import extract_events_async_with_meta, summarize_batch_async
from core.news.eventizer.rules import load_news_rule_config
from core.news.storage import db as news_db


DEFAULT_INTERVALS = {
    "chaincatcher_flash": 20,
    "okx_announcements": 25,
    "bybit_announcements": 25,
    "binance_announcements": 25,
    "coinglass_newsflash": 30,
    "cryptopanic": 90,
    "coinglass_articles": 180,
    "cryptocompare_news": 90,
    "opennews": 45,
    "jin10": 120,
    "coinglass_economic_data": 300,
    "coinglass_financial_events": 600,
    "coinglass_central_bank": 900,
    "rss": 300,
    "gdelt": 600,
    "newsapi": 600,
}

HIGH_PRIORITY = {
    "chaincatcher_flash",
    "okx_announcements",
    "bybit_announcements",
    "binance_announcements",
    "coinglass_newsflash",
}
MID_PRIORITY = {
    "cryptopanic",
    "cryptocompare_news",
    "jin10",
    "coinglass_articles",
    "coinglass_economic_data",
}
LOW_PRIORITY = {"rss", "gdelt", "newsapi", "coinglass_financial_events", "coinglass_central_bank"}
_COINGLASS_LOW_BUDGET_INTERVALS = {
    "coinglass_newsflash": 600,
    "coinglass_articles": 300,
    "coinglass_economic_data": 1800,
    "coinglass_financial_events": 3600,
    "coinglass_central_bank": 3600,
}


def _norm_url(u: str) -> str:
    return str(u or "").strip().split("?")[0].split("#")[0].rstrip("/").lower()


def _is_llm_summary_source(source: Any) -> bool:
    checker = getattr(news_db, "_is_llm_summary_source", None)
    if callable(checker):
        return bool(checker(source))
    text = str(source or "").strip().lower()
    return bool(
        text
        and (
            "glm" in text
            or text.startswith(("llm", "openai", "codex", "responses"))
            or text.startswith(("nim_summary:", "gm_summary:", "ds_summary:"))
            or text in {"nim_summary", "gm_summary", "ds_summary"}
            or text.endswith("_summary")
        )
    )


async def _persist_llm_title_summaries(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    *,
    events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """Summarize claimed news immediately so worker output is durable, not UI-lazy."""
    raw_targets: List[Dict[str, Any]] = []
    seen_raw_ids: set[int] = set()
    for item in batch:
        raw_id = item.get("id")
        title = str(item.get("title") or "").strip()
        if not raw_id or not title:
            continue
        try:
            raw_key = int(raw_id)
        except Exception:
            continue
        if raw_key in seen_raw_ids:
            continue
        seen_raw_ids.add(raw_key)
        raw_targets.append({"raw_news_id": raw_key, "title": title})

    if not raw_targets:
        return {"raw_updated_count": 0, "event_updated_count": 0, "skipped_non_llm": 0}

    summary_cfg = dict(cfg or {})
    llm_cfg = dict(summary_cfg.get("llm") or {})
    has_local_gemma_backup = _has_local_gemma_backup(summary_cfg)
    batch_size = max(1, min(int(llm_cfg.get("summarize_batch_size") or 6), len(raw_targets)))
    timeout_sec = max(8, min(int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 16), 45))
    if has_local_gemma_backup:
        batch_size = max(1, min(batch_size, _env_int("NEWS_LLM_LOCAL_SUMMARY_BATCH_SIZE", 1)))
        timeout_sec = max(timeout_sec, _env_int("NEWS_LLM_LOCAL_SUMMARIZE_TIMEOUT_SEC", 120))
    llm_cfg["summarize_batch_size"] = batch_size
    llm_cfg["summarize_timeout_sec"] = timeout_sec
    llm_cfg.setdefault("summarize_max_llm_items", len(raw_targets))
    summary_cfg["llm"] = llm_cfg

    try:
        summarized = await asyncio.wait_for(
            summarize_batch_async([item["title"] for item in raw_targets], summary_cfg, 60),
            timeout=max(timeout_sec + 2, timeout_sec * max(1, (len(raw_targets) + batch_size - 1) // batch_size) + 2),
        )
    except Exception as exc:
        logger.warning(f"llm summary persist skipped: {type(exc).__name__}: {exc}")
        return {"raw_updated_count": 0, "event_updated_count": 0, "skipped_non_llm": len(raw_targets)}

    raw_updates: List[Dict[str, Any]] = []
    summary_by_raw_id: Dict[int, Dict[str, Any]] = {}
    skipped_non_llm = 0
    for target, result in zip(raw_targets, summarized):
        source = str((result or {}).get("source") or "").strip().lower()
        if not _is_llm_summary_source(source):
            skipped_non_llm += 1
            continue
        row = {
            "summary_title": (result or {}).get("summary") or target.get("title") or "",
            "summary_sentiment": (result or {}).get("sentiment") or "neutral",
            "summary_source": source,
        }
        raw_id = int(target["raw_news_id"])
        summary_by_raw_id[raw_id] = row
        raw_updates.append({"raw_news_id": raw_id, **row})

    event_updates: List[Dict[str, Any]] = []
    for event in events or []:
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        raw_id = event.get("raw_news_id")
        try:
            row = summary_by_raw_id.get(int(raw_id)) if raw_id else None
        except Exception:
            row = None
        if row is None:
            evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
            title = str(evidence.get("title") or "").strip()
            match = next((item for item in raw_targets if item.get("title") == title), None)
            if match:
                row = summary_by_raw_id.get(int(match["raw_news_id"]))
        if row:
            event_updates.append({"event_id": event_id, **row})

    raw_result = await news_db.save_news_raw_summaries(raw_updates) if raw_updates else {"updated_count": 0}
    event_result = await news_db.save_news_event_summaries(event_updates) if event_updates else {"updated_count": 0}
    return {
        "raw_updated_count": int(raw_result.get("updated_count") or 0),
        "event_updated_count": int(event_result.get("updated_count") or 0),
        "skipped_non_llm": skipped_non_llm,
    }


async def _execute_llm_batch(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    raw_ids: List[int],
    *,
    url_to_raw_id: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], bool, str, int]:
    """Extract events via LLM, persist results, and mark tasks done.

    Returns (events, llm_used, error_type, events_count).
    On exception: marks tasks failed and re-raises.
    """
    try:
        events, llm_used, error_type = await extract_events_async_with_meta(batch, cfg)

        # Optionally link each event back to its source news row via URL matching.
        if url_to_raw_id is not None:
            for event in events:
                if event.get("raw_news_id"):
                    continue
                evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
                ev_url = _norm_url(str(evidence.get("url") or ""))
                if ev_url and ev_url in url_to_raw_id:
                    event["raw_news_id"] = url_to_raw_id[ev_url]
                elif len(batch) == 1:
                    event["raw_news_id"] = batch[0].get("id")

        event_stats = await news_db.save_events(events, model_source="mixed")
        summary_stats = (
            await _persist_llm_title_summaries(batch, cfg, events=event_stats.get("inserted") or events)
            if llm_used
            else {"raw_updated_count": 0, "event_updated_count": 0, "skipped_non_llm": 0}
        )
        if summary_stats.get("raw_updated_count") or summary_stats.get("event_updated_count"):
            logger.debug(
                "LLM summaries persisted raw={} event={} skipped_non_llm={}",
                summary_stats.get("raw_updated_count"),
                summary_stats.get("event_updated_count"),
                summary_stats.get("skipped_non_llm"),
            )

        is_rate_limited = error_type == "rate_limit"
        is_transient_failure = error_type in {"rate_limit", "timeout"}
        is_success = not is_transient_failure

        if is_rate_limited:
            from core.news.eventizer.rate_limiter import rate_limiter
            backoff_seconds = int(rate_limiter.get_backoff_time())
            backoff_until = datetime.now(timezone.utc) + timedelta(seconds=max(30, backoff_seconds))
            providers = {
                str(item.get("source") or (item.get("payload") or {}).get("provider") or "unknown")
                for item in batch
            }
            for provider in providers:
                await news_db.set_provider_backoff(provider, backoff_until)
                logger.warning(f"Rate limit hit for provider={provider}, backoff until {backoff_until.isoformat()}")

        await news_db.finish_llm_tasks(
            raw_ids,
            success=is_success,
            error=f"LLM extraction failed: {error_type}" if not is_success else None,
            error_type=error_type,
            is_rate_limited=is_rate_limited,
        )

        return events, llm_used, error_type, int(event_stats.get("events_count") or 0)

    except Exception as exc:
        await news_db.finish_llm_tasks(
            raw_ids,
            success=False,
            error=str(exc),
            error_type="other",
            is_rate_limited=False,
        )
        raise


async def _process_llm_task_batches(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """Process claimed news rows in worker-sized chunks so task status stays accurate."""
    if not batch:
        return {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}

    effective_cfg = _worker_cfg(cfg, limit=len(batch))
    llm_cfg = effective_cfg.get("llm") or {}
    chunk_size = max(1, min(int(llm_cfg.get("batch_size") or len(batch)), len(batch)))
    total_events_count = 0
    llm_used = False
    errors: List[str] = []

    for offset in range(0, len(batch), chunk_size):
        subbatch = batch[offset : offset + chunk_size]
        raw_ids = [int(item.get("id")) for item in subbatch if item.get("id")]
        url_to_raw_id = {
            _norm_url(item.get("url", "")): item.get("id")
            for item in subbatch
            if item.get("url") and item.get("id")
        }
        try:
            _, sub_llm_used, error_type, events_count = await _execute_llm_batch(
                subbatch,
                effective_cfg,
                raw_ids,
                url_to_raw_id=url_to_raw_id,
            )
            total_events_count += int(events_count or 0)
            llm_used = llm_used or bool(sub_llm_used)
            if error_type in {"rate_limit", "timeout"}:
                errors.append(str(error_type))
        except Exception as exc:
            errors.append(str(exc))

    return {
        "claimed": len(batch),
        "events_count": total_events_count,
        "llm_used": llm_used,
        "errors": errors,
        "error_type": errors[0] if len(errors) == 1 else ("mixed" if errors else "none"),
    }


def _config_paths() -> Dict[str, Path]:
    root = Path(__file__).resolve().parents[3]
    return {
        "rules": root / "config" / "news_rules.yaml",
        "symbols": root / "config" / "symbols.yaml",
    }


def load_service_config() -> Dict[str, Any]:
    paths = _config_paths()
    return load_news_rule_config(rules_path=paths["rules"], symbols_path=paths["symbols"])


def _coinglass_rate_limit_per_min() -> int:
    try:
        raw_env = os.getenv("COINGLASS_RATE_LIMIT_PER_MIN")
        if raw_env is not None and str(raw_env).strip():
            return max(1, int(raw_env))
    except Exception:
        pass
    try:
        return max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 30) or 30))
    except Exception:
        return 30


def _coinglass_low_budget_mode() -> bool:
    if os.getenv("NEWS_COINGLASS_LOW_BUDGET_MODE") is not None:
        return _env_bool("NEWS_COINGLASS_LOW_BUDGET_MODE", True)
    return _coinglass_rate_limit_per_min() < 20


def _min_importance() -> int:
    return max(0, min(100, _env_int("NEWS_LLM_MIN_IMPORTANCE", 35)))


def _env_int_override(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _positive_int(value: Any, default: int, minimum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(int(minimum), parsed)


def _has_local_gemma_backup(cfg: Optional[Dict[str, Any]] = None) -> bool:
    llm_cfg = (cfg or {}).get("llm") if isinstance(cfg, dict) else {}
    if not isinstance(llm_cfg, dict):
        llm_cfg = {}
    setting_names = (
        "NEWS_LLM_BACKUP_BASE_URL",
        "NEWS_LLM_BACKUP_MODEL",
        "OPENAI_BACKUP_BASE_URL",
        "OPENAI_BACKUP_MODEL",
    )
    values = [
        llm_cfg.get("backup_base_url"),
        llm_cfg.get("backup_model"),
        *(os.getenv(name) for name in setting_names),
        *(getattr(settings, name, "") for name in setting_names),
    ]
    text = " ".join(str(value or "") for value in values).lower()
    hints = ("gemma4-local", "local-gemma", "192.168.", "localhost", "127.0.0.1", "host.docker.internal")
    return any(hint in text for hint in hints)


def _source_interval(source: str) -> int:
    source_name = str(source or "").strip().lower()
    default_interval = DEFAULT_INTERVALS.get(source_name, 300)
    if source_name in _COINGLASS_LOW_BUDGET_INTERVALS and _coinglass_low_budget_mode():
        default_interval = max(default_interval, _COINGLASS_LOW_BUDGET_INTERVALS[source_name])
    return max(10, _env_int(f"NEWS_INTERVAL_{source_name.upper()}", default_interval))


def _worker_cfg(cfg: Dict[str, Any], limit: int) -> Dict[str, Any]:
    effective = dict(cfg or {})
    llm_cfg = dict(effective.get("llm") or {})
    timeout_override = _env_int_override("NEWS_LLM_WORKER_TIMEOUT_SEC")
    connect_timeout_override = _env_int_override("NEWS_LLM_WORKER_CONNECT_TIMEOUT_SEC")
    has_local_gemma_backup = _has_local_gemma_backup(effective)
    default_batch_size = 1 if has_local_gemma_backup else 8
    worker_batch_size = max(1, min(int(limit or 1), _env_int("NEWS_LLM_WORKER_BATCH_SIZE", default_batch_size)))
    if has_local_gemma_backup:
        local_cap = max(1, _env_int("NEWS_LLM_LOCAL_BACKUP_BATCH_SIZE", 1))
        worker_batch_size = min(worker_batch_size, local_cap)
    current_timeout = _positive_int(llm_cfg.get("timeout_sec"), 45, 8)
    current_connect_timeout = _positive_int(llm_cfg.get("connect_timeout_sec"), 10, 3)
    current_batch_size = int(llm_cfg.get("batch_size") or worker_batch_size)
    llm_cfg["timeout_sec"] = (
        max(8, int(timeout_override))
        if timeout_override is not None
        else current_timeout
    )
    llm_cfg["connect_timeout_sec"] = (
        max(3, int(connect_timeout_override))
        if connect_timeout_override is not None
        else current_connect_timeout
    )
    llm_cfg["batch_size"] = min(current_batch_size, worker_batch_size)
    llm_cfg["disable_thinking"] = bool(llm_cfg.get("disable_thinking", True))
    effective["llm"] = llm_cfg
    return effective


async def process_llm_batch(cfg: Dict[str, Any], limit: int = 8) -> Dict[str, Any]:
    """Process a batch of LLM tasks with intelligent error handling and backoff.

    Args:
        cfg: Configuration dictionary
        limit: Maximum number of tasks to claim

    Returns:
        Dictionary with processing statistics
    """
    # Check global backoff first
    global_backoff = await news_db.get_global_backoff()
    if global_backoff:
        logger.info(f"LLM worker in global backoff until {global_backoff.isoformat()}")
        return {
            "claimed": 0,
            "events_count": 0,
            "llm_used": False,
            "errors": ["global_backoff"],
            "backoff_until": global_backoff.isoformat(),
        }

    # Exclude providers currently in backoff up-front so we don't claim + requeue
    # (and inflate attempt_count on) their tasks every poll, which would starve
    # healthy providers within the batch budget. The per-item filter below remains
    # a safety net for the payload.provider edge case.
    backed_off_providers = await news_db.get_backed_off_providers()
    batch = await news_db.claim_llm_tasks(limit=limit, exclude_providers=backed_off_providers)
    if not batch:
        return {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}

    # Filter out items whose provider is in backoff
    filtered_batch = []
    backoff_ids = []
    for item in batch:
        provider = str(item.get("source") or (item.get("payload") or {}).get("provider") or "unknown")
        provider_backoff = await news_db.get_provider_backoff(provider)
        if provider_backoff:
            logger.debug(f"Skipping item from provider={provider}, in backoff until {provider_backoff.isoformat()}")
            if item.get("id"):
                backoff_ids.append(int(item["id"]))
        else:
            filtered_batch.append(item)

    # Re-queue items that are in provider backoff
    if backoff_ids:
        await news_db.finish_llm_tasks(
            backoff_ids,
            success=False,
            error="provider in backoff",
            error_type="rate_limit",
            is_rate_limited=True,
        )

    if not filtered_batch:
        return {"claimed": len(batch), "events_count": 0, "llm_used": False, "errors": ["provider_backoff"]}

    result = await _process_llm_task_batches(filtered_batch, cfg)
    if int(result.get("events_count") or 0) <= 0:
        logger.debug(
            f"LLM batch processed {len(filtered_batch)} items, 0 events extracted "
            "(normal for non-market-moving news)"
        )
    return result


# Event queue for non-blocking news processing
_llm_event_queue: Optional[asyncio.Queue] = None
_event_processor_task: Optional[asyncio.Task] = None


def _ensure_event_queue() -> asyncio.Queue:
    """Ensure the LLM event queue exists."""
    global _llm_event_queue
    if _llm_event_queue is None:
        _llm_event_queue = asyncio.Queue(maxsize=1000)
    return _llm_event_queue


async def on_news_inserted(news_items: List[Dict[str, Any]]) -> None:
    """Event handler called when news is inserted into database.

    This is non-blocking - it just queues the items for LLM processing.
    The actual LLM processing happens in the background.

    Args:
        news_items: List of inserted news items
    """
    if not news_items:
        return

    queue = _ensure_event_queue()
    for item in news_items:
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.warning("LLM event queue full, dropping news item")
            break

    # Start processor if not running
    await _ensure_event_processor()


async def _ensure_event_processor() -> None:
    """Ensure the background event processor is running."""
    global _event_processor_task
    if _event_processor_task is not None and not _event_processor_task.done():
        return

    _event_processor_task = asyncio.create_task(_event_processor_loop(), name="news-llm-event-processor")


async def _event_processor_loop() -> None:
    """Background loop to process queued news items with LLM."""
    global _event_processor_task

    queue = _ensure_event_queue()

    batch: List[Dict[str, Any]] = []
    batch_timeout = 2.0  # Wait up to 2 seconds for batch to fill

    logger.info("LLM event processor started")

    try:
        while True:
            try:
                # Wait for first item with timeout
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=batch_timeout)
                    batch.append(item)
                except asyncio.TimeoutError:
                    # Batch timeout, process current batch
                    if batch:
                        await _process_event_batch(batch)
                        batch.clear()
                    continue

                # Try to collect more items for batching
                batch_size = max(1, _env_int("NEWS_LLM_EVENT_BATCH_SIZE", 8))
                while len(batch) < batch_size:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=0.1)
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break

                # Process the batch
                if batch:
                    await _process_event_batch(batch)
                    batch.clear()

            except asyncio.CancelledError:
                # Propagate task cancellation immediately — do not swallow.
                raise
            except Exception as e:
                logger.error(f"Event processor error: {e}")
                await asyncio.sleep(5)  # Back off on error

    except asyncio.CancelledError:
        logger.info("LLM event processor cancelled")
        raise
    finally:
        logger.info("LLM event processor stopped")
        if _event_processor_task is asyncio.current_task():
            _event_processor_task = None


async def _process_event_batch(batch: List[Dict[str, Any]]) -> None:
    """Process a batch of news items with LLM extraction (called from background event processor)."""
    if not batch:
        return
    try:
        cfg = load_service_config()
        queue_stats = await news_db.enqueue_llm_tasks(batch, min_importance=_min_importance())
        limit = max(1, min(len(batch), _env_int("NEWS_LLM_EVENT_PROCESS_LIMIT", len(batch))))
        result = await process_llm_batch(cfg, limit=limit)
        logger.debug(
            f"Event processor processed batch: {len(batch)} items, "
            f"queued={int(queue_stats.get('queued_count') or 0)}, "
            f"claimed={int(result.get('claimed') or 0)}, "
            f"{int(result.get('events_count') or 0)} events, llm_used={bool(result.get('llm_used'))}"
        )
    except Exception as e:
        logger.error(f"Error processing event batch: {e}")


async def pull_source_once(
    cfg: Dict[str, Any],
    source: str,
    *,
    query: Optional[str] = None,
    max_records: int = 120,
    since_minutes: int = 240,
) -> Dict[str, Any]:
    state = await news_db.get_source_state(source)
    if state and state.get("paused_until"):
        paused_until = datetime.fromisoformat(str(state["paused_until"]).replace("Z", "+00:00"))
        if paused_until > datetime.now(timezone.utc):
            return {"source": source, "skipped": True, "reason": "paused", "paused_until": state.get("paused_until")}

    collector = MultiSourceNewsCollector(cfg)
    bundle = await collector.pull_latest_incremental(
        query=query,
        max_records=max_records,
        since_minutes=since_minutes,
        source_names=[source],
    )
    items = bundle.get("items") or []
    raw_stats = await news_db.save_news_raw(items)
    inserted = raw_stats.get("inserted") or []
    queue_stats = await news_db.enqueue_llm_tasks(inserted, min_importance=_min_importance())

    # Trigger non-blocking LLM processing for inserted items
    if inserted:
        asyncio.create_task(on_news_inserted(inserted))

    source_errors = (bundle.get("source_stats") or {}).get(source, {}).get("errors") or []
    if source_errors:
        threshold = max(2, _env_int("NEWS_SOURCE_BREAKER_ERRORS", 3))
        cooldown = max(30, _env_int("NEWS_SOURCE_BREAKER_COOLDOWN_SEC", 180))
        fresh_state = await news_db.get_source_state(source)
        if fresh_state and int(fresh_state.get("error_count") or 0) >= threshold:
            await news_db.set_source_state(
                source,
                paused_until=datetime.now(timezone.utc) + timedelta(seconds=cooldown),
            )

    return {
        "source": source,
        "pulled_count": int(bundle.get("pulled_total") or len(items)),
        "kept_count": int(bundle.get("kept_total") or len(items)),
        "inserted_count": len(inserted),
        "queued_count": int(queue_stats.get("queued_count") or 0),
        "errors": bundle.get("errors") or [],
        "source_stats": bundle.get("source_stats") or {},
    }


async def run_pull_cycle(cfg: Dict[str, Any], sources: List[str]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for source in sources:
        results.append(await pull_source_once(cfg, source))
    return {"results": results, "ts": datetime.now(timezone.utc).isoformat()}


async def worker_loop(cfg: Dict[str, Any], *, once: bool = False, pull_enabled: bool = True, llm_enabled: bool = True, sources: Optional[List[str]] = None) -> None:
    """Main worker loop with non-blocking LLM processing.

    News collection and LLM processing run independently:
    - News collection happens on scheduled intervals
    - LLM processing is triggered by events when news is inserted
    - LLM also has a periodic poll for any pending tasks

    Args:
        cfg: Configuration dictionary
        once: Run once and exit
        pull_enabled: Enable news collection
        llm_enabled: Enable LLM event extraction
        sources: Filter to specific sources
    """
    collector = MultiSourceNewsCollector(cfg)
    enabled_sources = [name for name in collector.sources if not sources or name in sources]
    next_due = {source: 0.0 for source in enabled_sources}
    next_llm_due = 0.0
    llm_interval = max(15, _env_int("NEWS_LLM_WORKER_INTERVAL_SEC", 20))
    llm_batch = max(1, _env_int("NEWS_LLM_BATCH_LIMIT", 8))

    # Start event processor for non-blocking LLM processing
    if llm_enabled:
        await _ensure_event_processor()
        logger.info("Event-driven LLM processor started")

    while True:
        now = asyncio.get_running_loop().time()
        did_work = False

        if pull_enabled:
            for source in enabled_sources:
                if now < next_due[source]:
                    continue
                try:
                    result = await pull_source_once(cfg, source)
                    logger.info(f"news pull source={source} inserted={result.get('inserted_count', 0)} queued={result.get('queued_count', 0)} errors={len(result.get('errors') or [])}")
                except Exception as exc:
                    logger.warning(f"news worker source={source} failed: {exc}")
                next_due[source] = now + _source_interval(source)
                did_work = True
                if once:
                    await asyncio.sleep(0)

        # Periodic LLM task polling (in addition to event-driven processing)
        if llm_enabled and now >= next_llm_due:
            try:
                llm_stats = await process_llm_batch(load_service_config(), limit=llm_batch)
                if llm_stats.get("claimed"):
                    errors_count = len(llm_stats.get('errors') or [])
                    logger.info(f"llm worker claimed={llm_stats.get('claimed')} events={llm_stats.get('events_count')} errors={errors_count}")
            except Exception as exc:
                logger.warning(f"llm worker failed: {exc}")
            next_llm_due = now + llm_interval
            did_work = True

        if once:
            break
        if not did_work:
            await asyncio.sleep(1.0)


async def main_async(args: argparse.Namespace) -> None:
    cfg = load_service_config()
    await news_db.init_news_db()
    try:
        source_filter = [x.strip().lower() for x in str(args.sources or "").split(",") if x.strip()] if args.sources else None
        await worker_loop(
            cfg,
            once=bool(args.once),
            pull_enabled=not bool(args.llm_only),
            llm_enabled=not bool(args.pull_only),
            sources=source_filter,
        )
    finally:
        await news_db.close_news_db()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crypto news incremental worker")
    parser.add_argument("--once", action="store_true", help="Run one pull/llm cycle and exit")
    parser.add_argument("--pull-only", action="store_true", help="Only run pull loop")
    parser.add_argument("--llm-only", action="store_true", help="Only run llm loop")
    parser.add_argument("--sources", type=str, default="", help="Comma-separated source filter")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
