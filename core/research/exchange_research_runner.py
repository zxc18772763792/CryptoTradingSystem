"""Scheduler hook: delisting guard refresh + new-listing and unlock paper trackers.

Called from the research scheduler tick (every ~5 min) and rate-limited here:
announcements every 30 min (flags + one-time alerts for held / watchlisted
coins), the listing tracker every 60 min, the Upbit caution tracker every 30 min. Each paper tracker's pre-registered
retirement verdict (core/research/retirement.py) alerts once when it turns
"retire" or "confirmed". Research and risk annotation only:
nothing here places or closes orders.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set

from loguru import logger

from core.research import (
    delist_risk,
    exchange_notices,
    listing_short_tracker,
    supply_factor_tracker,
    unlock_short_tracker,
    upbit_caution_tracker,
)

NOTICE_INTERVAL_SEC = 1800
TRACKER_INTERVAL_SEC = 3600
UNLOCK_INTERVAL_SEC = 6 * 3600
DELIST_RISK_INTERVAL_SEC = 24 * 3600
ALERT_STATE_PATH = exchange_notices.ANNOUNCEMENT_DIR / "guard_alerts.json"
WATCHLIST_PATH = exchange_notices.PROJECT_ROOT / "data" / "research" / "pump_watchlist" / "latest.json"
VERDICT_STATE_PATH = exchange_notices.PROJECT_ROOT / "data" / "research" / "tracker_verdicts.json"
TRACKER_NAMES = {"listing_short": "新上市做空", "unlock_short": "大额解锁前做空", "supply_factor": "供给通胀因子",
                 "upbit_caution": "Upbit 警示后做空", "upbit_krw_listing": "Upbit 韩元上币后做空"}
ALERT_VERDICTS = {"retire", "confirmed"}

UPBIT_INTERVAL_SEC = 1800
_last_run: Dict[str, float] = {}
_status: Dict[str, Any] = {}
_durations: Dict[str, float] = {}
_running: "Optional[asyncio.Task[Any]]" = None


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


async def _verdict_alert(tracker: str, summary: Dict[str, Any]) -> None:
    """Alert once per verdict change into retire/confirmed; remember every verdict seen."""
    result = (summary or {}).get("retirement") or {}
    current = result.get("verdict")
    if not current:
        return
    seen = json.loads(VERDICT_STATE_PATH.read_text(encoding="utf-8")) if VERDICT_STATE_PATH.exists() else {}
    if seen.get(tracker) == current:
        return
    if current in ALERT_VERDICTS:
        try:
            from core.notifications import notification_manager  # noqa: PLC0415

            ci = result.get("ci90_pct") or ["--", "--"]
            await notification_manager.send_message(
                f"纸面跟踪判定：{TRACKER_NAMES.get(tracker, tracker)} {result.get('label')}",
                f"{result.get('reason')}。前向 {result.get('n')} 个样本，均值 {result.get('mean_pct')}%，90% 区间 [{ci[0]}, {ci[1]}]。"
                "规则在首个前向结果之前登记，仅为研究判定，不涉及下单。",
            )
        except Exception as exc:
            logger.warning(f"tracker verdict alert failed: {exc}")
            return  # retry on the next tick
    seen[tracker] = current
    VERDICT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    VERDICT_STATE_PATH.write_text(json.dumps(seen, ensure_ascii=False, indent=1), encoding="utf-8")


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


async def _llm_upbit_reason(text: str) -> Optional[Dict[str, Any]]:
    from core.ai.research_context_generator import generate_json  # noqa: PLC0415

    return await generate_json(
        json.dumps({"notice_text": text}, ensure_ascii=False),
        system_prompt=upbit_caution_tracker.REASON_SYSTEM_PROMPT,
        timeout=90,
    )


async def _run_job(name: str, interval_sec: float, now: float, force: bool, job) -> None:
    """Run one job if due: isolated errors, duration recorded, stale error cleared on success."""
    if not force and now - _last_run.get(name, 0.0) < interval_sec:
        return
    _last_run[name] = now
    started = time.perf_counter()
    try:
        await job()
        _status.pop(f"{name}_error", None)
    except Exception as exc:
        _status[f"{name}_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        logger.warning(f"exchange research job {name} failed: {exc}")
    finally:
        _durations[name] = round(time.perf_counter() - started, 2)
        _status["durations_sec"] = dict(_durations)


async def tick(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    async with _client() as client:
        async def notices():
            await _refresh_notices(client)

        async def listing():
            history = await asyncio.to_thread(exchange_notices.load_history)
            _status["tracker"] = await listing_short_tracker.tick(client, llm_extract=_llm_extract, history=history)
            await _verdict_alert("listing_short", _status["tracker"])

        async def unlock():
            _status["unlock_tracker"] = await unlock_short_tracker.tick(client)
            await _verdict_alert("unlock_short", _status["unlock_tracker"])

        async def supply():
            _status["supply_factor"] = await supply_factor_tracker.tick(client)
            await _verdict_alert("supply_factor", _status["supply_factor"])

        async def upbit_caution():
            _status["upbit_caution"] = await upbit_caution_tracker.tick(client, llm_extract=_llm_upbit_reason)
            await _verdict_alert("upbit_caution", _status["upbit_caution"])

        async def upbit_krw_listing():
            _status["upbit_krw_listing"] = await upbit_caution_tracker.tick(client, strategy="krw_listing")
            await _verdict_alert("upbit_krw_listing", _status["upbit_krw_listing"])

        async def delist():
            model = delist_risk.load_model()
            scored = await delist_risk.compute_live_scores(client, model)
            await asyncio.to_thread(delist_risk.write_scores, scored, model)
            _status["delist_risk"] = {"scored": int(len(scored)), "flagged": int(scored["flagged"].sum()), "checked_at": now}

        await _run_job("notices", NOTICE_INTERVAL_SEC, now, force, notices)
        await _run_job("tracker", TRACKER_INTERVAL_SEC, now, force, listing)
        await _run_job("unlock", UNLOCK_INTERVAL_SEC, now, force, unlock)
        await _run_job("supply", UNLOCK_INTERVAL_SEC, now, force, supply)
        await _run_job("upbit", UPBIT_INTERVAL_SEC, now, force, upbit_caution)
        await _run_job("upbit_krw_listing", UPBIT_INTERVAL_SEC, now, force, upbit_krw_listing)
        if delist_risk.MODEL_PATH.exists():
            await _run_job("delist_risk", DELIST_RISK_INTERVAL_SEC, now, force, delist)
    return status()


def start_background_tick() -> bool:
    """Run tick() as a background task unless one is still running.

    The research scheduler calls this every ~5 minutes. A full pass (hundreds of
    exchange requests after a restart) can take minutes; awaiting it inline held
    up the research-job queue behind it, and overlapping passes would race on
    the same state files. Returns False when a pass is already in flight.
    """
    global _running
    if _running is not None and not _running.done():
        return False
    _running = asyncio.get_running_loop().create_task(tick())
    _running.add_done_callback(_log_background_failure)
    return True


def _log_background_failure(task: "asyncio.Task[Any]") -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.warning(f"exchange research pass failed: {task.exception()}")
