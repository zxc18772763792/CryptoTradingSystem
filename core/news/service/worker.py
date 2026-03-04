from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.async_glm_client import extract_events_async_with_meta
from core.news.eventizer.rules import load_news_rule_config
from core.news.storage import db as news_db


DEFAULT_INTERVALS = {
    "chaincatcher_flash": 20,
    "okx_announcements": 25,
    "bybit_announcements": 25,
    "binance_announcements": 25,
    "cryptopanic": 90,
    "cryptocompare_news": 90,
    "jin10": 120,
    "rss": 300,
    "gdelt": 600,
    "newsapi": 600,
}

HIGH_PRIORITY = {"chaincatcher_flash", "okx_announcements", "bybit_announcements", "binance_announcements"}
MID_PRIORITY = {"cryptopanic", "cryptocompare_news", "jin10"}
LOW_PRIORITY = {"rss", "gdelt", "newsapi"}


def _config_paths() -> Dict[str, Path]:
    root = Path(__file__).resolve().parents[3]
    return {
        "rules": root / "config" / "news_rules.yaml",
        "symbols": root / "config" / "symbols.yaml",
    }


def load_service_config() -> Dict[str, Any]:
    paths = _config_paths()
    return load_news_rule_config(rules_path=paths["rules"], symbols_path=paths["symbols"])


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _min_importance() -> int:
    return max(0, min(100, _env_int("NEWS_LLM_MIN_IMPORTANCE", 35)))


def _source_interval(source: str) -> int:
    return max(10, _env_int(f"NEWS_INTERVAL_{source.upper()}", DEFAULT_INTERVALS.get(source, 300)))


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

    batch = await news_db.claim_llm_tasks(limit=limit)
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

    batch = filtered_batch
    raw_ids = [int(item.get("id")) for item in batch if item.get("id")]

    # Build URL → raw_news_id map for linking extracted events back to source news
    def _norm_url(u: str) -> str:
        return str(u or "").strip().split("?")[0].split("#")[0].rstrip("/").lower()

    url_to_raw_id = {_norm_url(item.get("url", "")): item.get("id") for item in batch if item.get("url") and item.get("id")}

    try:
        events, llm_used, error_type = await extract_events_async_with_meta(batch, cfg)

        # Attach raw_news_id to each event by matching evidence URL to source news URL
        for event in events:
            if event.get("raw_news_id"):
                continue
            evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
            ev_url = _norm_url(str(evidence.get("url") or ""))
            if ev_url and ev_url in url_to_raw_id:
                event["raw_news_id"] = url_to_raw_id[ev_url]
            elif len(batch) == 1:
                event["raw_news_id"] = batch[0].get("id")

        if not events:
            logger.debug(f"LLM batch processed {len(batch)} items, 0 events extracted (normal for non-market-moving news)")

        event_stats = await news_db.save_events(events, model_source="mixed")

        # Determine success/failure based on error type
        is_success = error_type == "none"
        is_rate_limited = error_type == "rate_limit"

        # Set per-provider backoff if rate limited
        if is_rate_limited:
            from core.news.eventizer.rate_limiter import rate_limiter
            backoff_seconds = int(rate_limiter.get_backoff_time())
            backoff_until = datetime.now(timezone.utc) + timedelta(seconds=max(30, backoff_seconds))
            # Determine which provider(s) caused the rate limit
            providers = {str(item.get("source") or (item.get("payload") or {}).get("provider") or "unknown") for item in batch}
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

        return {
            "claimed": len(batch),
            "events_count": int(event_stats.get("events_count") or 0),
            "llm_used": bool(llm_used),
            "errors": [] if is_success else [error_type],
            "error_type": error_type,
        }
    except Exception as exc:
        await news_db.finish_llm_tasks(
            raw_ids,
            success=False,
            error=str(exc),
            error_type="other",
            is_rate_limited=False,
        )
        return {"claimed": len(batch), "events_count": 0, "llm_used": False, "errors": [str(exc)]}


# Event queue for non-blocking news processing
_llm_event_queue: Optional[asyncio.Queue] = None
_event_processor_running = False


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
    global _event_processor_running
    if _event_processor_running:
        return

    _event_processor_running = True
    asyncio.create_task(_event_processor_loop())


async def _event_processor_loop() -> None:
    """Background loop to process queued news items with LLM."""
    global _event_processor_running

    cfg = load_service_config()
    queue = _ensure_event_queue()

    batch: List[Dict[str, Any]] = []
    batch_size = 8
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
                        await _process_event_batch(batch, cfg)
                        batch.clear()
                    continue

                # Try to collect more items for batching
                while len(batch) < batch_size:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=0.1)
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break

                # Process the batch
                if batch:
                    await _process_event_batch(batch, cfg)
                    batch.clear()

            except Exception as e:
                logger.error(f"Event processor error: {e}")
                await asyncio.sleep(5)  # Back off on error

    finally:
        logger.info("LLM event processor stopped")
        _event_processor_running = False


async def _process_event_batch(batch: List[Dict[str, Any]], cfg: Dict[str, Any]) -> None:
    """Process a batch of news items with LLM extraction.

    This is called from the background event processor.

    Args:
        batch: List of news items to process
        cfg: Configuration dictionary
    """
    try:
        events, llm_used, error_type = await extract_events_async_with_meta(batch, cfg)

        # Save events to database
        await news_db.save_events(events, model_source="mixed")

        # Update LLM tasks status
        raw_ids = [item.get("id") for item in batch if item.get("id")]
        is_success = error_type == "none"
        is_rate_limited = error_type == "rate_limit"

        if is_rate_limited:
            from core.news.eventizer.rate_limiter import rate_limiter
            backoff_seconds = int(rate_limiter.get_backoff_time())
            backoff_until = datetime.now(timezone.utc) + timedelta(seconds=max(30, backoff_seconds))
            providers = {str(item.get("source") or (item.get("payload") or {}).get("provider") or "unknown") for item in batch}
            for provider in providers:
                await news_db.set_provider_backoff(provider, backoff_until)
                logger.warning(f"Rate limit in event processor for provider={provider}, backoff until {backoff_until.isoformat()}")

        await news_db.finish_llm_tasks(
            raw_ids,
            success=is_success,
            error=f"LLM extraction failed: {error_type}" if not is_success else None,
            error_type=error_type,
            is_rate_limited=is_rate_limited,
        )

        logger.debug(
            f"Event processor processed batch: {len(batch)} items, "
            f"{len(events)} events, llm_used={llm_used}"
        )

    except Exception as e:
        logger.error(f"Error processing event batch: {e}")
        # Mark tasks as failed
        raw_ids = [item.get("id") for item in batch if item.get("id")]
        await news_db.finish_llm_tasks(
            raw_ids,
            success=False,
            error=str(e),
            error_type="other",
            is_rate_limited=False,
        )


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
                llm_stats = await process_llm_batch(cfg, limit=llm_batch)
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
