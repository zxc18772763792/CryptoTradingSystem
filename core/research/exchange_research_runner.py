"""Scheduler hook: delisting guard refresh + new-listing paper tracker.

Called from the research scheduler tick (every ~5 min) and rate-limited here:
announcements every 30 min (flags + one-time alerts for held / watchlisted
coins), the listing tracker every 60 min. Research and risk annotation only:
nothing here places or closes orders.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set

from loguru import logger

from core.research import exchange_notices, listing_short_tracker

NOTICE_INTERVAL_SEC = 1800
TRACKER_INTERVAL_SEC = 3600
ALERT_STATE_PATH = exchange_notices.ANNOUNCEMENT_DIR / "guard_alerts.json"
WATCHLIST_PATH = exchange_notices.PROJECT_ROOT / "data" / "research" / "pump_watchlist" / "latest.json"

_last_run: Dict[str, float] = {"notices": 0.0, "tracker": 0.0}
_status: Dict[str, Any] = {}


def status() -> Dict[str, Any]:
    return dict(_status)


def _client():
    import httpx

    from config.settings import settings
    from core.utils.shared_ssl import get_shared_ssl_context

    kwargs: Dict[str, Any] = {"timeout": 25, "verify": get_shared_ssl_context()}
    proxy = settings.HTTPS_PROXY or settings.HTTP_PROXY
    if proxy:
        kwargs["proxy"] = proxy
    return httpx.AsyncClient(**kwargs)


def _watched_bases() -> Dict[str, str]:
    """Coins we care about: open positions and this week's watchlist top."""
    out: Dict[str, str] = {}
    try:
        from core.trading.position_manager import position_manager  # noqa: PLC0415

        for position in position_manager.get_all_positions() or []:
            out[exchange_notices.base_of(getattr(position, "symbol", ""))] = "持仓"
    except Exception as exc:
        logger.debug(f"delisting guard: positions unavailable: {exc}")
    try:
        watchlist = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
        for row in watchlist.get("top") or []:
            out.setdefault(str(row.get("base") or "").upper(), "周度伏击名单")
    except Exception:
        pass
    return out


async def _alert(new: Dict[str, Dict[str, Any]], watched: Dict[str, str]) -> Set[str]:
    sent: Set[str] = set()
    hits = {b: n for b, n in new.items() if b in watched}
    if not hits:
        return sent
    lines = [f"{b}（{watched[b]}）：{n['kind']} · {n.get('effective_date') or '--'} · {n.get('title')}" for b, n in hits.items()]
    try:
        from core.notifications import notification_manager  # noqa: PLC0415

        await notification_manager.send_message(
            "交易所下架/监控公告命中关注币",
            "\n".join(lines) + "\n历史上 91% 的下架币在公告后继续下跌（中位 −34%）；AI 代理已禁止新开仓，请人工决定是否处理现有持仓。",
        )
        sent = set(hits)
    except Exception as exc:
        logger.warning(f"delisting guard alert failed: {exc}")
    return sent


async def _refresh_notices(client) -> None:
    history = exchange_notices.load_history()
    added = await exchange_notices.refresh_history(client, history)
    if added:
        exchange_notices.save_history(history)
    flags = exchange_notices.active_flags(history)
    exchange_notices.write_flags(flags)

    alerted = json.loads(ALERT_STATE_PATH.read_text(encoding="utf-8")) if ALERT_STATE_PATH.exists() else {}
    keys = {b: f"{b}|{n['kind']}|{n['announced_at']}" for b, n in flags.items()}
    new = {b: flags[b] for b, key in keys.items() if key not in alerted}
    sent = await _alert(new, _watched_bases())
    for b in new:  # remember every seen notice (alerted or not) so each fires at most once
        alerted[keys[b]] = {"alerted": b in sent}
    ALERT_STATE_PATH.write_text(json.dumps(alerted, ensure_ascii=False, indent=1), encoding="utf-8")
    _status["notices"] = {"checked_at": time.time(), "new_articles": added, "active_flags": len(flags), "alerts_sent": sorted(sent)}


async def _llm_extract(text: str) -> Optional[Dict[str, Any]]:
    from core.ai.research_context_generator import generate_json  # noqa: PLC0415

    return await generate_json(
        json.dumps({"announcement_text": text}, ensure_ascii=False),
        system_prompt=listing_short_tracker.EXTRACTION_SYSTEM_PROMPT,
        timeout=90,
    )


async def tick(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    async with _client() as client:
        if force or now - _last_run["notices"] >= NOTICE_INTERVAL_SEC:
            _last_run["notices"] = now
            try:
                await _refresh_notices(client)
            except Exception as exc:
                _status["notices_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
                logger.warning(f"delisting guard refresh failed: {exc}")
        if force or now - _last_run["tracker"] >= TRACKER_INTERVAL_SEC:
            _last_run["tracker"] = now
            try:
                _status["tracker"] = await listing_short_tracker.tick(
                    client, llm_extract=_llm_extract, history=exchange_notices.load_history()
                )
            except Exception as exc:
                _status["tracker_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
                logger.warning(f"listing short tracker failed: {exc}")
    return status()
