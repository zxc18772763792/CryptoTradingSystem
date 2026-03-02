from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.llm_glm5 import extract_events_glm5_with_meta
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
    batch = await news_db.claim_llm_tasks(limit=limit)
    if not batch:
        return {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}
    raw_ids = [int(item.get("id")) for item in batch if item.get("id")]
    try:
        events, llm_used, errors = extract_events_glm5_with_meta(batch, cfg)
        event_stats = await news_db.save_events(events, model_source="mixed")
        await news_db.finish_llm_tasks(raw_ids, success=True)
        return {
            "claimed": len(batch),
            "events_count": int(event_stats.get("events_count") or 0),
            "llm_used": bool(llm_used),
            "errors": errors,
        }
    except Exception as exc:
        await news_db.finish_llm_tasks(raw_ids, success=False, error=str(exc))
        return {"claimed": len(batch), "events_count": 0, "llm_used": False, "errors": [str(exc)]}


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
    collector = MultiSourceNewsCollector(cfg)
    enabled_sources = [name for name in collector.sources if not sources or name in sources]
    next_due = {source: 0.0 for source in enabled_sources}
    next_llm_due = 0.0
    llm_interval = max(15, _env_int("NEWS_LLM_WORKER_INTERVAL_SEC", 20))
    llm_batch = max(1, _env_int("NEWS_LLM_BATCH_LIMIT", 8))

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

        if llm_enabled and now >= next_llm_due:
            try:
                llm_stats = await process_llm_batch(cfg, limit=llm_batch)
                if llm_stats.get("claimed"):
                    logger.info(f"llm worker claimed={llm_stats.get('claimed')} events={llm_stats.get('events_count')} errors={len(llm_stats.get('errors') or [])}")
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
