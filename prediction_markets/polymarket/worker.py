from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from prediction_markets.polymarket.clob_reader import ClobReader
from prediction_markets.polymarket.config import load_polymarket_config
from prediction_markets.polymarket.db import (
    cleanup_old_quotes,
    close_pm_db,
    compute_and_store_alerts,
    get_markets_map,
    get_pm_status,
    init_pm_db,
    insert_quotes,
    list_active_subscriptions,
    list_source_states,
    set_source_state,
    set_subscriptions,
    upsert_markets,
)
from prediction_markets.polymarket.gamma_client import GammaClient
from prediction_markets.polymarket.market_resolver import MarketResolver
from prediction_markets.polymarket.utils import parse_ts_any, utc_now


@dataclass
class PMWorkerRuntime:
    worker_running: bool = False
    ws_connected: bool = False
    ws_mode: str = "idle"
    last_market_refresh_at: Optional[str] = None
    last_quote_refresh_at: Optional[str] = None
    last_error: Optional[str] = None
    subscriptions_count: int = 0
    markets_count: int = 0
    quotes_last_run: int = 0
    alerts_last_run: int = 0
    source_states: List[Dict[str, Any]] = field(default_factory=list)


_RUNTIME = PMWorkerRuntime()
_STOP_EVENT: Optional[asyncio.Event] = None


def get_runtime_status() -> Dict[str, Any]:
    return asdict(_RUNTIME)


def _fallback_quote_from_market_snapshot(sub: Dict[str, Any], market_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload_market = ((market_row.get("payload") or {}).get("market") or {})
    bid = payload_market.get("bestBid")
    ask = payload_market.get("bestAsk")
    spread = payload_market.get("spread")
    last_price = payload_market.get("lastTradePrice")
    try:
        bid_f = float(bid) if bid not in (None, "") else None
    except Exception:
        bid_f = None
    try:
        ask_f = float(ask) if ask not in (None, "") else None
    except Exception:
        ask_f = None
    midpoint = None
    if bid_f is not None and ask_f is not None:
        midpoint = (bid_f + ask_f) / 2.0
    try:
        price_f = float(last_price) if last_price not in (None, "") else None
    except Exception:
        price_f = None
    if midpoint is None and price_f is None:
        return None
    try:
        spread_f = float(spread) if spread not in (None, "") else None
    except Exception:
        spread_f = None
    return {
        "ts": utc_now(),
        "market_id": str(sub.get("market_id") or ""),
        "token_id": str(sub.get("token_id") or ""),
        "outcome": str(sub.get("outcome") or "YES").upper(),
        "price": float(midpoint if midpoint is not None else price_f or 0.0),
        "bid": bid_f,
        "ask": ask_f,
        "midpoint": midpoint,
        "spread": spread_f,
        "depth1": None,
        "depth5": None,
        "fetched_at": utc_now(),
        "payload": {"source": "gamma_market_snapshot", "market": payload_market},
    }


async def _discover_markets(cfg: Dict[str, Any], categories: Optional[List[str]] = None) -> Dict[str, Any]:
    defaults = cfg.get("defaults") or {}
    gamma_cfg = defaults.get("gamma") or {}
    gamma = GammaClient(
        base_url=gamma_cfg.get("base_url") or "https://gamma-api.polymarket.com",
        request_timeout_sec=int(gamma_cfg.get("request_timeout_sec") or 15),
    )
    resolver = MarketResolver(cfg)
    selected_categories = [c.upper() for c in (categories or list(resolver.categories.keys())) if c.upper() in resolver.categories]
    events: List[Dict[str, Any]] = []
    search_hits: Dict[str, List[Dict[str, Any]]] = {}
    max_offset = 0
    page_limit = int(gamma_cfg.get("page_limit") or 100)
    try:
        while True:
            batch = await asyncio.wait_for(
                gamma.list_events(active=True, closed=False, limit=page_limit, offset=max_offset),
                timeout=max(8, int(gamma_cfg.get("request_timeout_sec") or 15)),
            )
            if not batch:
                break
            events.extend(batch)
            latest_updated = max((str(item.get("updatedAt") or item.get("updated_at") or "") for item in batch), default="")
            await set_source_state("gamma_events", cursor_type="updatedAt", cursor_value=latest_updated, last_ts=utc_now(), mark_success=True)
            if len(batch) < page_limit or max_offset >= page_limit * 3:
                break
            max_offset += page_limit
    except Exception as exc:
        await set_source_state("gamma_events", last_error=str(exc), mark_failure=True, paused_until=utc_now() + timedelta(minutes=5))
        logger.warning(f"Polymarket events discovery failed: {type(exc).__name__}: {exc}")

    try:
        tags = await asyncio.wait_for(gamma.list_tags(limit=200), timeout=8)
        await set_source_state("gamma_tags", cursor_type="ts", cursor_value=str(len(tags)), last_ts=utc_now(), mark_success=True)
    except Exception as exc:
        await set_source_state("gamma_tags", last_error=str(exc), mark_failure=True)

    async def _safe_search(keyword: str) -> List[Dict[str, Any]]:
        try:
            return await asyncio.wait_for(gamma.search_public(keyword, limit=15), timeout=8)
        except Exception as exc:
            logger.debug(f"search_public failed for {keyword}: {type(exc).__name__}: {exc}")
            return []

    for category_name in selected_categories:
        cat = resolver.categories[category_name]
        keyword_limit = min(6, len(cat.keywords))
        per_cat_hits: List[Dict[str, Any]] = []
        if keyword_limit > 0:
            batches = await asyncio.gather(*[_safe_search(keyword) for keyword in cat.keywords[:keyword_limit]])
            for batch in batches:
                per_cat_hits.extend(batch)
        search_hits[category_name] = per_cat_hits

    resolved = resolver.resolve(events=events, keyword_search_hits=search_hits)
    market_stats = await upsert_markets(resolved.get("markets") or [])
    sub_stats = {}
    for category_name, subs in (resolved.get("subscriptions") or {}).items():
        if categories and category_name.upper() not in selected_categories:
            continue
        sub_stats[category_name] = await set_subscriptions(category_name, subs)
    _RUNTIME.last_market_refresh_at = utc_now().isoformat()
    _RUNTIME.markets_count = int(market_stats.get("upserted") or 0)
    _RUNTIME.source_states = await list_source_states()
    return {"markets": market_stats, "subscriptions": sub_stats}


async def _poll_quotes_once(cfg: Dict[str, Any], categories: Optional[List[str]] = None) -> Dict[str, Any]:
    defaults = cfg.get("defaults") or {}
    clob_cfg = defaults.get("clob") or {}
    subscriptions = await list_active_subscriptions()
    if categories:
        wanted = {c.upper() for c in categories}
        subscriptions = [item for item in subscriptions if str(item.get("category") or "").upper() in wanted]
    reader = ClobReader(
        base_url=clob_cfg.get("base_url") or "https://clob.polymarket.com",
        ws_url=clob_cfg.get("ws_url") or "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        request_timeout_sec=int(clob_cfg.get("request_timeout_sec") or 8),
        burst=int(clob_cfg.get("burst") or 20),
        refill_per_sec=float(clob_cfg.get("refill_per_sec") or 5),
        max_concurrency=int(clob_cfg.get("max_concurrency") or 6),
        breaker_errors=int(clob_cfg.get("breaker_errors") or 8),
        breaker_cooldown_sec=int(clob_cfg.get("breaker_cooldown_sec") or 120),
    )
    quotes = []
    alerts = {"inserted": 0}
    try:
        quotes = await reader.fetch_quotes_for_subscriptions(subscriptions)
        if not quotes and subscriptions:
            market_map = await get_markets_map([str(item.get("market_id") or "") for item in subscriptions])
            for sub in subscriptions:
                market_row = market_map.get(str(sub.get("market_id") or ""))
                if not market_row:
                    continue
                quote = _fallback_quote_from_market_snapshot(sub, market_row)
                if quote:
                    quotes.append(quote)
        await insert_quotes(quotes)
        await set_source_state("clob_rest", cursor_type="ts", cursor_value=str(len(quotes)), last_ts=utc_now(), mark_success=True)
    except Exception as exc:
        await set_source_state("clob_rest", last_error=str(exc), mark_failure=True, paused_until=utc_now() + timedelta(minutes=2))
        logger.warning(f"Polymarket quote polling failed: {exc}")
    if quotes:
        try:
            alerts = await compute_and_store_alerts(utc_now() - timedelta(minutes=15), utc_now())
        except Exception as exc:
            logger.debug(f"compute_and_store_alerts failed: {exc}")
    _RUNTIME.last_quote_refresh_at = utc_now().isoformat()
    _RUNTIME.quotes_last_run = len(quotes)
    _RUNTIME.alerts_last_run = int(alerts.get("inserted") or 0)
    _RUNTIME.ws_connected = bool(reader.get_runtime_status().get("ws_connected"))
    _RUNTIME.ws_mode = "streaming" if _RUNTIME.ws_connected else "polling"
    _RUNTIME.subscriptions_count = len(subscriptions)
    return {"quotes_inserted": len(quotes), "alerts": alerts, "subscriptions": len(subscriptions)}


async def refresh_markets_once(cfg: Optional[Dict[str, Any]] = None, categories: Optional[List[str]] = None) -> Dict[str, Any]:
    return await _discover_markets(cfg or load_polymarket_config(), categories=categories)


async def refresh_quotes_once(cfg: Optional[Dict[str, Any]] = None, categories: Optional[List[str]] = None) -> Dict[str, Any]:
    return await _poll_quotes_once(cfg or load_polymarket_config(), categories=categories)


async def run_worker_once(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    refresh_markets: bool = True,
    refresh_quotes: bool = True,
    categories: Optional[List[str]] = None,
) -> Dict[str, Any]:
    config = cfg or load_polymarket_config()
    out: Dict[str, Any] = {"ts": utc_now().isoformat()}
    if refresh_markets:
        out["markets"] = await refresh_markets_once(config, categories=categories)
    if refresh_quotes:
        out["quotes"] = await refresh_quotes_once(config, categories=categories)
    _RUNTIME.source_states = await list_source_states()
    out["status"] = await get_pm_status()
    return out


async def _quotes_loop(cfg: Dict[str, Any], stop_event: asyncio.Event) -> None:
    interval = int(((cfg.get("defaults") or {}).get("worker") or {}).get("quote_loop_sec") or 10)
    retention_days = int(((cfg.get("defaults") or {}).get("storage") or {}).get("raw_quote_retention_days") or 14)
    while not stop_event.is_set():
        try:
            await refresh_quotes_once(cfg)
            await cleanup_old_quotes(retention_days)
            _RUNTIME.last_error = None
        except Exception as exc:
            _RUNTIME.last_error = str(exc)
            logger.warning(f"Polymarket quotes loop error: {exc}")
        await asyncio.sleep(max(5, interval))


async def _ws_quotes_loop(cfg: Dict[str, Any], stop_event: asyncio.Event) -> None:
    defaults = cfg.get("defaults") or {}
    clob_cfg = defaults.get("clob") or {}
    backoffs = [1, 2, 5, 10, 30]
    failure_count = 0

    async def _on_quote(quote: Dict[str, Any]) -> None:
        await insert_quotes([quote])

    while not stop_event.is_set():
        subscriptions = await list_active_subscriptions()
        if not subscriptions:
            _RUNTIME.ws_connected = False
            _RUNTIME.ws_mode = "idle"
            await asyncio.sleep(5)
            continue
        reader = ClobReader(
            base_url=clob_cfg.get("base_url") or "https://clob.polymarket.com",
            ws_url=clob_cfg.get("ws_url") or "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            request_timeout_sec=int(clob_cfg.get("request_timeout_sec") or 8),
            burst=int(clob_cfg.get("burst") or 20),
            refill_per_sec=float(clob_cfg.get("refill_per_sec") or 5),
            max_concurrency=int(clob_cfg.get("max_concurrency") or 6),
            breaker_errors=int(clob_cfg.get("breaker_errors") or 8),
            breaker_cooldown_sec=int(clob_cfg.get("breaker_cooldown_sec") or 120),
        )
        try:
            _RUNTIME.ws_mode = "streaming"
            await reader.stream_quotes(subscriptions, on_quote=_on_quote, stop_event=stop_event)
            failure_count = 0
            _RUNTIME.last_error = None
            await set_source_state("clob_ws", cursor_type="ts", cursor_value="stream", last_ts=utc_now(), mark_success=True)
        except Exception as exc:
            failure_count += 1
            _RUNTIME.ws_connected = False
            _RUNTIME.ws_mode = "polling"
            _RUNTIME.last_error = str(exc)
            pause_for = backoffs[min(failure_count - 1, len(backoffs) - 1)]
            await set_source_state(
                "clob_ws",
                last_error=str(exc),
                mark_failure=True,
                paused_until=utc_now() + timedelta(seconds=pause_for),
            )
            logger.warning(f"Polymarket WS loop error ({failure_count}): {exc}")
            await asyncio.sleep(pause_for)


async def _markets_loop(cfg: Dict[str, Any], stop_event: asyncio.Event) -> None:
    interval = int(((cfg.get("defaults") or {}).get("gamma") or {}).get("refresh_interval_sec") or 1200)
    while not stop_event.is_set():
        try:
            await refresh_markets_once(cfg)
            _RUNTIME.last_error = None
        except Exception as exc:
            _RUNTIME.last_error = str(exc)
            logger.warning(f"Polymarket markets loop error: {exc}")
        await asyncio.sleep(max(300, interval))


async def run_worker(cfg: Optional[Dict[str, Any]] = None) -> None:
    global _STOP_EVENT
    config = cfg or load_polymarket_config()
    await init_pm_db()
    _STOP_EVENT = asyncio.Event()
    _RUNTIME.worker_running = True
    try:
        await asyncio.gather(
            _markets_loop(config, _STOP_EVENT),
            _quotes_loop(config, _STOP_EVENT),
            _ws_quotes_loop(config, _STOP_EVENT),
        )
    finally:
        _RUNTIME.worker_running = False
        await close_pm_db()


async def stop_worker() -> None:
    global _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()


def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--pull-only", action="store_true")
    parser.add_argument("--quotes-only", action="store_true")
    parser.add_argument("--categories", default="")
    args = parser.parse_args()

    categories = [x.strip().upper() for x in str(args.categories or "").split(",") if x.strip()]
    cfg = load_polymarket_config()

    async def _entry() -> None:
        await init_pm_db()
        try:
            if args.once or args.pull_only or args.quotes_only:
                await run_worker_once(cfg, refresh_markets=not args.quotes_only, refresh_quotes=not args.pull_only, categories=categories or None)
            else:
                await run_worker(cfg)
        finally:
            if args.once or args.pull_only or args.quotes_only:
                await close_pm_db()

    asyncio.run(_entry())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
