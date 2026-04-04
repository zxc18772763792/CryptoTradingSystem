"""News API for dashboard widget and standalone news page."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Body, HTTPException, Query, Request
from loguru import logger
import pandas as pd
from pydantic import BaseModel, Field

from config.settings import settings
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.llm_glm5 import (
    _summarize_fallback,
    batch_summarize_titles_llm as batch_summarize_titles,
    extract_events_llm_with_meta,
)
from core.news.text_normalizer import clean_news_text
from core.news.eventizer.rules import SymbolMapper, load_news_rule_config
from core.news.service.worker import process_llm_batch
from core.news.storage import db as news_db
from core.news.storage.models import parse_any_datetime


router = APIRouter()
_DEFAULT_TOPIC_KEYWORDS = {
    "crypto",
    "bitcoin",
    "ethereum",
    "binance",
    "blockchain",
    "stablecoin",
    "defi",
    "etf",
    "fed",
    "sec",
    "比特币",
    "以太坊",
    "加密",
    "区块链",
    "币安",
    "美联储",
    "监管",
    "利率",
    "降息",
    "加息",
}
_AUTO_PULL_LOCK = asyncio.Lock()
_AUTO_PULL_RUNNING = False
_AUTO_PULL_LAST_AT: Optional[datetime] = None
_NEWS_PIPELINE_LOCK = asyncio.Lock()
_MANUAL_PULL_SEQ = 0
_MANUAL_LLM_SEQ = 0
_FAILED_REQUEUE_LOCK = asyncio.Lock()
_FAILED_REQUEUE_LAST_AT: Optional[datetime] = None
_NEWS_RESPONSE_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {"latest": {}, "summary": {}, "brief": {}, "health": {}}
_NEWS_HEALTH_REFRESH_TASK: Optional[asyncio.Task] = None
_NEWS_PROCESS_CACHE_TTL_SEC = 5.0
_NEWS_PROCESS_CACHE_AT = 0.0
_NEWS_PROCESS_CACHE_PAYLOAD: Dict[str, Any] = {}
_TRACKING_QUERY_KEYS = {
    "feature",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "oc",
    "ref",
    "ref_src",
    "source",
}


class PullNowRequest(BaseModel):
    since_minutes: int = Field(default=240, ge=15, le=1440)
    max_records: int = Field(default=120, ge=10, le=250)
    query: Optional[str] = None


class BackfillRecentRequest(BaseModel):
    hours: int = Field(default=72, ge=1, le=720)
    max_candidates: int = Field(default=160, ge=10, le=1000)
    force_reprocess_done: bool = False


class BackfillHistoryRequest(BaseModel):
    hours: int = Field(default=24 * 7, ge=1, le=24 * 90)
    max_records: int = Field(default=320, ge=20, le=800)
    query: Optional[str] = None
    source_names: List[str] = Field(default_factory=list)
    enqueue_llm: bool = False


class RequeueLLMTasksRequest(BaseModel):
    statuses: List[str] = Field(default_factory=lambda: ["failed"])
    limit: int = Field(default=200, ge=1, le=2000)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except Exception:
        return int(default)


def _news_llm_enabled() -> bool:
    return bool(
        str(os.environ.get("OPENAI_API_KEY") or "").strip()
        or str(getattr(settings, "OPENAI_API_KEY", "") or "").strip()
        or str(os.environ.get("OPENAI_BACKUP_API_KEY") or "").strip()
        or str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip()
    )


def _cache_key(*parts: Any) -> str:
    return "|".join(str(part or "") for part in parts)


def _cache_get(namespace: str, key: str, ttl_sec: int) -> Optional[Dict[str, Any]]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    item = bucket.get(key)
    if not item:
        return None
    ts = item.get("_cached_at")
    if not isinstance(ts, datetime):
        return None
    if (_now_utc() - ts).total_seconds() > max(1, int(ttl_sec)):
        return None
    payload = dict(item.get("payload") or {})
    payload["_cache"] = {"hit": True, "stale": False, "age_sec": round((_now_utc() - ts).total_seconds(), 2)}
    return payload


def _cache_get_stale(namespace: str, key: str) -> Optional[Dict[str, Any]]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    item = bucket.get(key)
    if not item:
        return None
    ts = item.get("_cached_at")
    payload = dict(item.get("payload") or {})
    age = round((_now_utc() - ts).total_seconds(), 2) if isinstance(ts, datetime) else None
    payload["_cache"] = {"hit": True, "stale": True, "age_sec": age}
    return payload


def _cache_set(namespace: str, key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    bucket[key] = {"_cached_at": _now_utc(), "payload": dict(payload or {})}
    result = dict(payload or {})
    result["_cache"] = {"hit": False, "stale": False, "age_sec": 0.0}
    return result


def _invalidate_news_caches(*, clear_feed: bool = False) -> None:
    # Keep latest/brief/summary as stale fallback snapshots by default.
    # This prevents "sudden empty feed" when DB is momentarily contended.
    namespaces = ["health", "pull_status", "worker_status", "coverage"]
    if clear_feed:
        namespaces.extend(["latest", "summary", "brief"])
    for namespace in namespaces:
        _NEWS_RESPONSE_CACHE.setdefault(namespace, {}).clear()


def _news_source_flags() -> Dict[str, bool]:
    def _enabled(name: str, default: bool = True) -> bool:
        return str(os.environ.get(name, "1" if default else "0")).strip().lower() not in {"0", "false", "no", "off"}

    gdelt_enabled = str(os.environ.get("NEWS_ENABLE_GDELT", "1")).strip().lower() not in {"0", "false", "no", "off"}
    jin10_enabled = str(os.environ.get("NEWS_ENABLE_JIN10", "1")).strip().lower() not in {"0", "false", "no", "off"}
    rss_enabled = str(os.environ.get("NEWS_ENABLE_RSS", "1")).strip().lower() not in {"0", "false", "no", "off"}
    newsapi_enabled = bool(os.environ.get("NEWSAPI_KEY"))
    if str(os.environ.get("NEWS_ENABLE_NEWSAPI", "1")).strip().lower() in {"0", "false", "no", "off"}:
        newsapi_enabled = False
    cryptopanic_enabled = bool(os.environ.get("CRYPTOPANIC_TOKEN") or os.environ.get("CRYPTOPANIC_API_KEY"))
    if str(os.environ.get("NEWS_ENABLE_CRYPTOPANIC", "1")).strip().lower() in {"0", "false", "no", "off"}:
        cryptopanic_enabled = False
    return {
        "jin10": jin10_enabled,
        "rss": rss_enabled,
        "gdelt": gdelt_enabled,
        "newsapi": newsapi_enabled,
        "cryptopanic": cryptopanic_enabled,
        "chaincatcher_flash": _enabled("NEWS_ENABLE_CHAINCATCHER_FLASH", True),
        "binance_announcements": _enabled("NEWS_ENABLE_BINANCE_ANNOUNCEMENTS", True),
        "okx_announcements": _enabled("NEWS_ENABLE_OKX_ANNOUNCEMENTS", True),
        "bybit_announcements": _enabled("NEWS_ENABLE_BYBIT_ANNOUNCEMENTS", True),
        "cryptocompare_news": _enabled("NEWS_ENABLE_CRYPTOCOMPARE_NEWS", True),
    }


def _task_running(task: Any) -> bool:
    return isinstance(task, asyncio.Task) and not task.done()


def _scan_external_news_processes() -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "worker_running": False,
        "llm_running": False,
        "worker_pids": [],
        "llm_pids": [],
        "detector": "none",
        "error": None,
    }
    worker_token = "core.news.service.worker"
    llm_token = "core.news.service.llm_worker"
    try:
        if sys.platform.startswith("win"):
            command = (
                "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
                "Where-Object { "
                "$n=[string]$_.Name; "
                "if ($n -and $n.ToLowerInvariant() -notin @('python.exe','pythonw.exe')) { return $false }; "
                "$cmd=[string]$_.CommandLine; "
                "if (-not $cmd) { return $false }; "
                "$lower=$cmd.ToLowerInvariant(); "
                f"return $lower.Contains('{worker_token}') -or $lower.Contains('{llm_token}') "
                "} | Select-Object ProcessId, CommandLine | ConvertTo-Json -Compress"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
            )
            snapshot["detector"] = "powershell:Get-CimInstance"
            if proc.returncode != 0:
                snapshot["error"] = (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()[:240] or None
                return snapshot
            raw_text = str(proc.stdout or "").strip()
            if not raw_text:
                return snapshot
            payload = json.loads(raw_text)
            rows = payload if isinstance(payload, list) else [payload]
        else:
            proc = subprocess.run(
                ["ps", "-eo", "pid=,args="],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
            )
            snapshot["detector"] = "ps"
            if proc.returncode != 0:
                snapshot["error"] = (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()[:240] or None
                return snapshot
            rows = []
            for line in str(proc.stdout or "").splitlines():
                text = str(line or "").strip()
                if not text:
                    continue
                parts = text.split(None, 1)
                if not parts:
                    continue
                pid_text = parts[0]
                command_line = parts[1] if len(parts) > 1 else ""
                rows.append({"ProcessId": pid_text, "CommandLine": command_line})
    except Exception as exc:
        snapshot["error"] = str(exc)[:240]
        return snapshot

    worker_pids: List[int] = []
    llm_pids: List[int] = []
    for row in rows:
        command_line = str((row or {}).get("CommandLine") or "").strip()
        if not command_line:
            continue
        lower_cmd = command_line.lower()
        try:
            pid = int((row or {}).get("ProcessId") or 0)
        except Exception:
            pid = 0
        if llm_token in lower_cmd:
            snapshot["llm_running"] = True
            if pid > 0:
                llm_pids.append(pid)
        if worker_token in lower_cmd:
            snapshot["worker_running"] = True
            if pid > 0:
                worker_pids.append(pid)

    snapshot["worker_pids"] = sorted(set(worker_pids))
    snapshot["llm_pids"] = sorted(set(llm_pids))
    return snapshot


def _external_news_process_snapshot() -> Dict[str, Any]:
    global _NEWS_PROCESS_CACHE_AT, _NEWS_PROCESS_CACHE_PAYLOAD
    now = time.monotonic()
    if _NEWS_PROCESS_CACHE_PAYLOAD and (now - _NEWS_PROCESS_CACHE_AT) <= _NEWS_PROCESS_CACHE_TTL_SEC:
        return dict(_NEWS_PROCESS_CACHE_PAYLOAD)
    payload = _scan_external_news_processes()
    _NEWS_PROCESS_CACHE_AT = now
    _NEWS_PROCESS_CACHE_PAYLOAD = dict(payload)
    return dict(payload)


def _news_background_state(request: Request) -> Dict[str, Any]:
    internal_pull_running = _task_running(getattr(request.app.state, "news_task", None))
    internal_llm_running = _task_running(getattr(request.app.state, "news_llm_task", None))

    external_pull_requested = _env_bool("START_NEWS_WORKER", False)
    external_llm_requested = _env_bool("START_NEWS_LLM_WORKER", False)
    internal_pull_requested = _env_bool("NEWS_BACKGROUND_ENABLED", True) and not external_pull_requested
    internal_llm_requested = _env_bool("NEWS_LLM_BACKGROUND_ENABLED", True) and not external_llm_requested

    external_snapshot = _external_news_process_snapshot()
    external_pull_running = bool(external_snapshot.get("worker_running"))
    external_llm_running = bool(external_snapshot.get("llm_running"))

    background_pull_enabled = internal_pull_requested or external_pull_requested or internal_pull_running or external_pull_running
    background_llm_enabled = internal_llm_requested or external_llm_requested or internal_llm_running or external_llm_running

    if internal_pull_running:
        background_pull_mode = "internal"
    elif external_pull_running or external_pull_requested:
        background_pull_mode = "external"
    elif internal_pull_requested:
        background_pull_mode = "internal"
    else:
        background_pull_mode = "disabled"

    if internal_llm_running:
        background_llm_mode = "internal"
    elif external_llm_running or external_llm_requested:
        background_llm_mode = "external"
    elif internal_llm_requested:
        background_llm_mode = "internal"
    else:
        background_llm_mode = "disabled"

    return {
        "background_pull_enabled": bool(background_pull_enabled),
        "background_llm_enabled": bool(background_llm_enabled),
        "background_pull_running": bool(internal_pull_running or external_pull_running),
        "background_llm_running": bool(internal_llm_running or external_llm_running),
        "background_pull_mode": background_pull_mode,
        "background_llm_mode": background_llm_mode,
        "background_pull_pids": list(external_snapshot.get("worker_pids") or []),
        "background_llm_pids": list(external_snapshot.get("llm_pids") or []),
        "background_process_detector": external_snapshot.get("detector") or "none",
        "background_process_scan_error": external_snapshot.get("error"),
    }


def _news_runtime_snapshot(request: Request) -> Dict[str, Any]:
    return {
        "service": "web_news",
        "timestamp": _now_utc().isoformat(),
        "llm_enabled": _news_llm_enabled(),
        "sync_pull_llm": _env_bool("NEWS_PULL_SYNC_LLM", False),
        **_news_background_state(request),
        "last_pull": getattr(request.app.state, "news_last_pull", None),
        "last_llm_batch": getattr(request.app.state, "news_last_llm_batch", None),
        "sources": _news_source_flags(),
        "source_states": [],
        "llm_queue": {},
    }


async def _collect_news_db_snapshot(timeout_sec: int) -> Dict[str, Any]:
    results = await asyncio.gather(
        asyncio.wait_for(asyncio.shield(news_db.list_source_states()), timeout=timeout_sec),
        asyncio.wait_for(asyncio.shield(news_db.get_llm_queue_stats()), timeout=timeout_sec),
        return_exceptions=True,
    )
    source_states_result, llm_queue_result = results
    payload = {"source_states": [], "llm_queue": {}, "failures": []}
    if not isinstance(source_states_result, Exception):
        payload["source_states"] = source_states_result
    else:
        payload["failures"].append(f"source_states={type(source_states_result).__name__}")
    if not isinstance(llm_queue_result, Exception):
        payload["llm_queue"] = llm_queue_result
    else:
        payload["failures"].append(f"llm_queue={type(llm_queue_result).__name__}")
    return payload


async def _refresh_news_health_cache(request: Request, cache_key: str) -> None:
    global _NEWS_HEALTH_REFRESH_TASK
    try:
        db_timeout = max(2, _env_int("NEWS_API_HEALTH_DB_TIMEOUT_SEC", 4))
        db_snapshot = await _collect_news_db_snapshot(db_timeout)
        payload = _news_runtime_snapshot(request)
        payload["source_states"] = list(db_snapshot.get("source_states") or [])
        payload["llm_queue"] = dict(db_snapshot.get("llm_queue") or {})
        payload["status"] = "ok" if not db_snapshot.get("failures") else "degraded"
        if db_snapshot.get("failures"):
            payload["fallback_reason"] = ", ".join(db_snapshot["failures"])
        _cache_set("health", cache_key, payload)
    except Exception as exc:
        logger.warning(f"news health background refresh failed: {type(exc).__name__}: {exc}")
    finally:
        _NEWS_HEALTH_REFRESH_TASK = None


def _config_paths() -> Dict[str, Path]:
    root = Path(__file__).resolve().parents[2]
    return {
        "rules": root / "config" / "news_rules.yaml",
        "symbols": root / "config" / "symbols.yaml",
    }


def load_news_cfg() -> Dict[str, Any]:
    paths = _config_paths()
    return load_news_rule_config(rules_path=paths["rules"], symbols_path=paths["symbols"])


def _get_cfg(request: Request) -> Dict[str, Any]:
    cfg = getattr(request.app.state, "news_cfg", None)
    if isinstance(cfg, dict):
        return cfg
    return load_news_cfg()


def _get_mapper(cfg: Dict[str, Any]) -> SymbolMapper:
    mapper = cfg.get("_symbol_mapper")
    if isinstance(mapper, SymbolMapper):
        return mapper
    return SymbolMapper({"symbols": cfg.get("symbols") or {}})


def _normalize_symbol(symbol: Optional[str], cfg: Dict[str, Any]) -> Optional[str]:
    if not symbol:
        return None
    mapper = _get_mapper(cfg)
    normalized = mapper.normalize_symbol(symbol)
    return normalized or str(symbol).strip().upper() or None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_llm_summary_source(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    if text in {"glm", "glm5", "llm", "llm_cache", "glm_cache", "glm5_cache", "openai", "openai_responses", "codex", "responses"}:
        return True
    return ("glm" in text) or text.startswith("llm") or text.startswith("openai") or text.startswith("codex") or text.startswith("responses")


def _is_fallback_summary_source(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return True
    if text in {"not_summarized", "rule_fallback", "fallback_rule", "api_timeout_fallback"}:
        return True
    return "fallback" in text


def _llm_min_importance() -> int:
    return max(0, min(100, _env_int("NEWS_LLM_MIN_IMPORTANCE", 35)))


def _failed_requeue_policy(
    counts: Dict[str, Any],
    *,
    limit: Optional[int] = None,
    cooldown_sec: Optional[int] = None,
) -> Dict[str, Any]:
    failed = max(0, int((counts or {}).get("failed") or 0))
    base_limit = max(0, min(int(limit if limit is not None else _env_int("NEWS_FAILED_REQUEUE_LIMIT", 2)), 20))
    base_cooldown = max(
        0,
        min(int(cooldown_sec if cooldown_sec is not None else _env_int("NEWS_FAILED_REQUEUE_COOLDOWN_SEC", 90)), 3600),
    )

    if failed >= 800:
        suggested_limit = 10
        suggested_cooldown = 20
        tier = "xlarge"
    elif failed >= 400:
        suggested_limit = 8
        suggested_cooldown = 30
        tier = "large"
    elif failed >= 150:
        suggested_limit = 6
        suggested_cooldown = 45
        tier = "medium"
    elif failed >= 40:
        suggested_limit = 4
        suggested_cooldown = 60
        tier = "small"
    else:
        suggested_limit = 2
        suggested_cooldown = 90
        tier = "tiny"

    effective_limit = base_limit if limit is not None else max(base_limit, suggested_limit)
    if cooldown_sec is not None:
        effective_cooldown = base_cooldown
    elif base_cooldown <= 0:
        effective_cooldown = 0
    else:
        effective_cooldown = min(base_cooldown, suggested_cooldown)

    return {
        "failed_backlog": failed,
        "backlog_tier": tier,
        "base_limit": base_limit,
        "base_cooldown_sec": base_cooldown,
        "suggested_limit": suggested_limit,
        "suggested_cooldown_sec": suggested_cooldown,
        "effective_limit": effective_limit,
        "effective_cooldown_sec": effective_cooldown,
    }


def _summary_fields_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    summary_title = _clean_display_text(payload.get("summary_title"))
    if not summary_title:
        return {}
    summary_sentiment = str(payload.get("summary_sentiment") or "neutral").strip().lower()
    if summary_sentiment not in {"positive", "negative", "neutral"}:
        summary_sentiment = "neutral"
    summary_source = str(payload.get("summary_source") or "").strip().lower() or "stored"
    return {
        "summary_title": summary_title,
        "summary_sentiment": summary_sentiment,
        "summary_source": summary_source,
    }


def _derive_unstructured_processing_status(raw_row: Dict[str, Any], llm_task_status: str, min_importance: int) -> str:
    payload = raw_row.get("payload") if isinstance(raw_row.get("payload"), dict) else {}
    summary_source = str(payload.get("summary_source") or "").strip().lower()
    status = str(llm_task_status or "").strip().lower()
    if status:
        if status == "done":
            return "done_no_event"
        if status in {"retry", "failed"} and _is_llm_summary_source(summary_source):
            return "summarized_no_event"
        if status in {"pending", "running", "retry", "failed"}:
            return status
        return status
    if _is_llm_summary_source(summary_source):
        return "summarized_no_event"
    importance = int(payload.get("importance_score") or 0)
    if importance < int(min_importance):
        return "skipped_low_importance"
    return "not_queued"


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    clean = sorted(float(v) for v in values)
    if len(clean) == 1:
        return round(clean[0], 3)
    idx = max(0.0, min(1.0, p / 100.0)) * (len(clean) - 1)
    lo = int(idx)
    hi = min(len(clean) - 1, lo + 1)
    frac = idx - lo
    return round(clean[lo] * (1 - frac) + clean[hi] * frac, 3)


def _canonical_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except Exception:
        return text.split("#", 1)[0].strip().rstrip("/")

    query_pairs: List[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_norm = str(key or "").strip().lower()
        if not key_norm:
            continue
        if key_norm.startswith("utm_") or key_norm in _TRACKING_QUERY_KEYS:
            continue
        query_pairs.append((str(key), str(value)))

    path = parsed.path.rstrip("/") or parsed.path or "/"
    query = urlencode(query_pairs, doseq=True)
    return urlunsplit(
        (
            str(parsed.scheme or "").lower(),
            str(parsed.netloc or "").lower(),
            path,
            query,
            "",
        )
    )


def _clean_display_text(value: Any) -> str:
    return clean_news_text(value)


def _display_title_core(value: Any, max_len: int = 96) -> str:
    text = _clean_display_text(value)
    if not text:
        return ""
    text = re.sub(r"^\s*快讯[:：]\s*", "", text)
    text = re.sub(r"\s+\|\s+[^|]{1,48}$", "", text)
    text = re.sub(r"\s+-\s+[A-Za-z][A-Za-z0-9 .&/_-]{1,42}$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -|:")
    if len(text) <= max_len:
        return text
    clipped = text[:max_len].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0]
    return clipped.rstrip(" ,;:.-")


def _canonical_title(title: Any) -> str:
    text = _clean_display_text(title).lower()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def _contains_anchor(text: str, anchors: List[str]) -> bool:
    for anchor in anchors:
        token = str(anchor or "").strip().lower()
        if not token:
            continue
        # Short ascii tokens like btc/eth/sol need word boundaries to avoid false hits.
        if token.isascii() and token.isalnum() and len(token) <= 4:
            if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text):
                return True
            continue
        if token in text:
            return True
    return False


def _topic_keywords(cfg: Dict[str, Any]) -> List[str]:
    mapper = _get_mapper(cfg)
    keywords = set(_DEFAULT_TOPIC_KEYWORDS)
    for item in (cfg.get("symbols") or {}).values():
        if not isinstance(item, dict):
            continue
        aliases = [item.get("canonical"), *(item.get("aliases") or [])]
        for alias in aliases:
            text = str(alias or "").strip().lower()
            if len(text) >= 3:
                keywords.add(text)
            normalized = mapper.normalize_symbol(alias).lower()
            if len(normalized) >= 3:
                keywords.add(normalized)
    return sorted(keywords)


def _topic_anchor_keywords(cfg: Dict[str, Any]) -> List[str]:
    anchors = {
        "crypto",
        "bitcoin",
        "ethereum",
        "binance",
        "blockchain",
        "btc",
        "eth",
        "bnb",
        "sol",
        "xrp",
        "ada",
        "doge",
        "\u6bd4\u7279\u5e01",
        "\u4ee5\u592a\u574a",
        "\u52a0\u5bc6",
        "\u533a\u5757\u94fe",
        "\u5e01\u5b89",
        "\u5c71\u5be8\u5e01",
    }
    for item in (cfg.get("symbols") or {}).values():
        if not isinstance(item, dict):
            continue
        canonical = str(item.get("canonical") or "").strip().upper()
        if canonical:
            anchors.add(canonical.lower())
            if canonical.endswith("USDT"):
                base = canonical[:-4].lower()
                if base in {"btc", "eth", "bnb", "sol", "xrp", "ada", "doge", "trx", "ltc", "bch"}:
                    anchors.add(base)
    return sorted(anchors)

def _is_relevant_news(item: Dict[str, Any], keywords: List[str], anchor_keywords: Optional[List[str]] = None) -> bool:
    title = str(item.get("title") or "").strip().lower()
    content = str(item.get("content") or item.get("summary") or "").strip().lower()
    text = f"{title}\n{content}"
    if not text.strip():
        return False
    if not any(keyword in text for keyword in keywords):
        return False

    provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "").strip().lower()
    source_name = str(item.get("source") or "").strip().lower()
    # GDELT/RSS are noisy on generic words like ETF/Fed; require a crypto anchor.
    if (provider in {"gdelt", "rss", "newsapi"} or (not provider and source_name != "jin10")) and anchor_keywords:
        if not _contains_anchor(text, anchor_keywords):
            return False
    return True


def _normalize_source_names(values: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        text = str(value or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _news_archive_contract() -> Dict[str, Any]:
    return {
        "stores_all_pulled_raw_news": True,
        "guarantees_full_upstream_history": False,
        "note": "\u65b0\u95fb\u5e93\u4f1a\u4fdd\u7559\u5df2\u7ecf\u6210\u529f\u62c9\u5230\u5e76\u53bb\u91cd\u540e\u7684\u539f\u59cb\u65b0\u95fb\uff1b\u82e5\u67d0\u6bb5\u5386\u53f2\u5f53\u65f6\u672a\u62c9\u5230\uff0c\u9ed8\u8ba4\u4e0d\u4f1a\u5929\u7136\u8865\u5168\uff0c\u9700\u8981\u989d\u5916\u6267\u884c\u5386\u53f2\u8865\u62c9\uff0c\u4e14\u6700\u7ec8\u4ecd\u53d7\u4e0a\u6e38\u6e90\u662f\u5426\u63d0\u4f9b\u5386\u53f2\u7a97\u53e3\u9650\u5236\u3002",
    }

def _raw_related_symbols(raw: Dict[str, Any], cfg: Dict[str, Any]) -> List[str]:
    mapper = _get_mapper(cfg)
    related: set[str] = set()
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    symbol_field = raw.get("symbols") or payload.get("symbols") or payload.get("currencies")

    candidates: List[Any] = []
    if isinstance(symbol_field, dict):
        candidates.extend(list(symbol_field.keys()))
        for value in symbol_field.values():
            if isinstance(value, str):
                candidates.append(value)
            elif isinstance(value, list):
                candidates.extend(value)
    elif isinstance(symbol_field, list):
        candidates.extend(symbol_field)
    elif isinstance(symbol_field, str):
        candidates.extend([x.strip() for x in symbol_field.split(",") if str(x).strip()])

    for raw_symbol in candidates:
        normalized = mapper.normalize_symbol(raw_symbol)
        if normalized:
            related.add(normalized)

    text = f"{raw.get('title') or ''}\n{raw.get('content') or raw.get('summary') or ''}"
    for inferred in mapper.extract_symbols_from_text(text, limit=10):
        normalized = mapper.normalize_symbol(inferred)
        if normalized:
            related.add(normalized)

    return sorted(related)


def _event_as_feed_item(event: Dict[str, Any]) -> Dict[str, Any]:
    evidence = event.get("evidence") or {}
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    provider = str(payload.get("provider") or "event")
    out = {
        "id": f"event-{event.get('id')}",
        "raw_news_id": event.get("raw_news_id"),
        "published_at": event.get("ts"),
        "title": _clean_display_text(evidence.get("title")),
        "url": str(evidence.get("url") or ""),
        "source": _clean_display_text(evidence.get("source") or "unknown"),
        "provider": provider,
        "symbol": str(event.get("symbol") or ""),
        "event_type": str(event.get("event_type") or ""),
        "sentiment": int(event.get("sentiment") or 0),
        "impact_score": _safe_float(event.get("impact_score")),
        "model_source": str(event.get("model_source") or "event"),
        "event_id": str(event.get("event_id") or ""),
        "has_event": True,
        "processing_status": "structured_event",
        "llm_task_status": "done",
        "importance_score": int(payload.get("importance_score") or 0),
        "related_symbols": [str(event.get("symbol") or "")] if str(event.get("symbol") or "").strip() else [],
        "related_event_types": [str(event.get("event_type") or "")] if str(event.get("event_type") or "").strip() else [],
        "related_providers": [provider] if provider else [],
        "event_count": 1,
    }
    persisted_summary = _summary_fields_from_payload(payload)
    if persisted_summary:
        out.update(persisted_summary)
    return out


def _raw_as_feed_item(raw: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    provider = str(payload.get("provider") or "raw")
    persisted_summary = _summary_fields_from_payload(payload)
    out = {
        "id": f"raw-{raw.get('id')}",
        "raw_news_id": raw.get("id"),
        "published_at": raw.get("published_at"),
        "title": _clean_display_text(raw.get("title")),
        "url": str(raw.get("url") or ""),
        "source": _clean_display_text(raw.get("source") or "unknown"),
        "provider": provider,
        "symbol": "",
        "event_type": "",
        "sentiment": 0,
        "impact_score": 0.0,
        "model_source": "raw",
        "event_id": "",
        "has_event": False,
        "processing_status": "unknown_unstructured",
        "llm_task_status": "",
        "importance_score": int(payload.get("importance_score") or 0),
        "related_symbols": [],
        "related_event_types": [],
        "related_providers": [provider] if provider else [],
        "event_count": 0,
    }
    if persisted_summary:
        out.update(persisted_summary)
    if not event:
        return out

    event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    out.update(
        {
            "symbol": str(event.get("symbol") or ""),
            "event_type": str(event.get("event_type") or ""),
            "sentiment": int(event.get("sentiment") or 0),
            "impact_score": _safe_float(event.get("impact_score")),
            "model_source": str(event.get("model_source") or "event"),
            "event_id": str(event.get("event_id") or ""),
            "provider": str(event_payload.get("provider") or provider),
            "has_event": True,
            "related_symbols": [str(event.get("symbol") or "")] if str(event.get("symbol") or "").strip() else [],
            "related_event_types": [str(event.get("event_type") or "")] if str(event.get("event_type") or "").strip() else [],
            "related_providers": [str(event_payload.get("provider") or provider)] if str(event_payload.get("provider") or provider).strip() else [],
            "event_count": 1,
        }
    )
    return out


def _sort_by_published_desc(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(item: Dict[str, Any]) -> float:
        try:
            ts = parse_any_datetime(item.get("published_at"))
            return ts.timestamp()
        except Exception:
            return 0.0

    return sorted(items, key=_key, reverse=True)


def _feed_sentiment_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"positive": 0, "neutral": 0, "negative": 0}
    structured = 0
    unstructured = 0
    unstructured_breakdown: Dict[str, int] = {
        "pending": 0,
        "running": 0,
        "retry": 0,
        "failed": 0,
        "done_no_event": 0,
        "summarized_no_event": 0,
        "skipped_low_importance": 0,
        "not_queued": 0,
        "unknown_unstructured": 0,
    }
    for item in items or []:
        if bool(item.get("has_event")):
            structured += 1
            s = int(item.get("sentiment") or 0)
            if s > 0:
                counts["positive"] += 1
            elif s < 0:
                counts["negative"] += 1
            else:
                counts["neutral"] += 1
            continue
        unstructured += 1
        key = str(item.get("processing_status") or "unknown_unstructured").strip().lower() or "unknown_unstructured"
        unstructured_breakdown[key] = unstructured_breakdown.get(key, 0) + 1
        ss = str(item.get("summary_sentiment") or "neutral").strip().lower()
        if ss == "positive":
            counts["positive"] += 1
        elif ss == "negative":
            counts["negative"] += 1
        else:
            counts["neutral"] += 1
    return {
        "total": len(items or []),
        "structured": structured,
        "unstructured": unstructured,
        "unstructured_breakdown": unstructured_breakdown,
        "sentiment": counts,
    }


def _sorted_unique_texts(values: List[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _event_story_key(event: Dict[str, Any]) -> str:
    evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
    anchor = _canonical_url(evidence.get("url")) or _canonical_title(evidence.get("title"))
    if not anchor:
        anchor = str(event.get("event_id") or "")
    try:
        bucket = int(parse_any_datetime(event.get("ts")).timestamp() // 1800)
    except Exception:
        bucket = 0
    return f"{anchor}|{bucket}"


def _merge_event_group_into_item(item: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [event for event in (events or []) if isinstance(event, dict)]
    if not rows:
        return item
    rows = sorted(
        rows,
        key=lambda event: (
            -_safe_float(event.get("impact_score")),
            str(event.get("symbol") or ""),
            str(event.get("event_id") or ""),
        ),
    )
    primary = rows[0]
    providers = []
    for event in rows:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        providers.append(payload.get("provider"))
    item["symbol"] = str(primary.get("symbol") or item.get("symbol") or "")
    item["event_type"] = str(primary.get("event_type") or item.get("event_type") or "")
    item["sentiment"] = int(primary.get("sentiment") or item.get("sentiment") or 0)
    item["impact_score"] = _safe_float(primary.get("impact_score") or item.get("impact_score"))
    item["model_source"] = str(primary.get("model_source") or item.get("model_source") or "event")
    item["event_id"] = str(primary.get("event_id") or item.get("event_id") or "")
    item["provider"] = str((primary.get("payload") or {}).get("provider") or item.get("provider") or "")
    item["has_event"] = True
    item["processing_status"] = "structured_event"
    item["llm_task_status"] = "done"
    item["related_symbols"] = _sorted_unique_texts([event.get("symbol") for event in rows])
    item["related_event_types"] = _sorted_unique_texts([event.get("event_type") for event in rows])
    item["related_providers"] = _sorted_unique_texts(providers or [item.get("provider")])
    item["event_count"] = len(rows)
    persisted_summary = _summary_fields_from_payload(primary.get("payload") if isinstance(primary.get("payload"), dict) else {})
    if persisted_summary:
        item.update(persisted_summary)
    return item


def _apply_display_summaries(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure each feed row has stable display summary fields."""
    for item in items or []:
        title = _clean_display_text(item.get("title"))
        if bool(item.get("has_event")):
            sentiment = int(item.get("sentiment") or 0)
            if item.get("summary_title"):
                item["summary_title"] = _display_title_core(item.get("summary_title"))
                summary_sentiment = str(item.get("summary_sentiment") or "").strip().lower()
                if summary_sentiment not in {"positive", "negative", "neutral"}:
                    summary_sentiment = "positive" if sentiment > 0 else "negative" if sentiment < 0 else "neutral"
                item["summary_sentiment"] = summary_sentiment
                item["summary_source"] = str(item.get("summary_source") or "event")
                continue
            item["summary_title"] = _display_title_core(title)
            item["summary_sentiment"] = "positive" if sentiment > 0 else "negative" if sentiment < 0 else "neutral"
            item["summary_source"] = str(item.get("summary_source") or "event")
            continue
        if item.get("summary_title"):
            item["summary_title"] = _display_title_core(item.get("summary_title"))
            item["summary_sentiment"] = str(item.get("summary_sentiment") or "neutral").strip().lower() or "neutral"
            item["summary_source"] = str(item.get("summary_source") or "llm")
            continue
        fallback = _summarize_fallback(title or "", 60)
        item["summary_title"] = _display_title_core(title)
        item["summary_sentiment"] = str(fallback.get("sentiment") or "neutral").strip().lower() or "neutral"
        item["summary_source"] = str(fallback.get("source") or "rule_fallback")
    return items


def _event_ts_hint(row: Dict[str, Any]) -> Any:
    if not isinstance(row, dict):
        return None
    for key in ("ts", "published_at", "created_at", "time"):
        value = row.get(key)
        if value:
            return value
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    for key in ("ts", "published_at", "created_at", "time"):
        value = payload.get(key)
        if value:
            return value
    return None


def _first_iso_ts(rows: List[Dict[str, Any]]) -> Optional[str]:
    for row in rows or []:
        value = _event_ts_hint(row)
        if not value:
            continue
        with contextlib.suppress(Exception):
            return parse_any_datetime(value).isoformat()
    return None


def _bucket_has_data(bucket_stats: Dict[str, List[Dict[str, Any]]]) -> bool:
    for rows in (bucket_stats or {}).values():
        if rows:
            return True
    return False


def _bucketize_events(events: List[Dict[str, Any]], granularities: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    rules = granularities or ["5m", "15m", "1h", "4h", "1d"]
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not events:
        return {g: [] for g in rules}

    rows: List[Dict[str, Any]] = []
    for event in events:
        ts_hint = _event_ts_hint(event)
        if not ts_hint:
            continue
        try:
            ts = parse_any_datetime(ts_hint)
        except Exception:
            continue
        rows.append({"ts": ts, "sentiment": int(event.get("sentiment") or 0)})
    if not rows:
        return {g: [] for g in rules}

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts")
    if df.empty:
        return {g: [] for g in rules}
    df = df.set_index("ts")

    rule_map = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1d"}
    for g in rules:
        freq = rule_map.get(g)
        if not freq:
            continue
        buckets = defaultdict(lambda: {"count": 0, "positive": 0, "neutral": 0, "negative": 0})
        for ts, row in df.iterrows():
            key = ts.floor(freq)
            slot = buckets[key]
            slot["count"] += 1
            s = int(row.get("sentiment") or 0)
            if s > 0:
                slot["positive"] += 1
            elif s < 0:
                slot["negative"] += 1
            else:
                slot["neutral"] += 1
        series: List[Dict[str, Any]] = []
        for ts_key in sorted(buckets.keys()):
            slot = buckets[ts_key]
            series.append(
                {
                    "bucket_start": ts_key.isoformat(),
                    "count": int(slot["count"]),
                    "positive": int(slot["positive"]),
                    "neutral": int(slot["neutral"]),
                    "negative": int(slot["negative"]),
                }
            )
        out[g] = series
    return out


def _latest_item_age_min(items: List[Dict[str, Any]]) -> Optional[float]:
    if not items:
        return None
    try:
        ts = parse_any_datetime(items[0].get("published_at"))
    except Exception:
        return None
    return max(0.0, (_now_utc() - ts).total_seconds() / 60.0)


def _cfg_int(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int((cfg.get("defaults") or {}).get(key) or default)
    except Exception:
        return int(default)


def _feed_summarize_cfg(cfg: Dict[str, Any], *, limit: int) -> Dict[str, Any]:
    effective = dict(cfg or {})
    llm_cfg = dict(effective.get("llm") or {})
    max_items = max(1, min(int(limit or 1), _env_int("NEWS_API_SUMMARY_MAX_ITEMS", 8)))
    default_batch_size = max(1, min(int(llm_cfg.get("summarize_batch_size") or 6), max_items))
    default_timeout_sec = max(3, min(int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 20), 20))
    batch_size = max(1, min(_env_int("NEWS_API_SUMMARY_BATCH_SIZE", default_batch_size), max_items))
    timeout_sec = max(3, min(_env_int("NEWS_API_SUMMARIZE_TIMEOUT_SEC", default_timeout_sec), 20))
    llm_cfg["summarize_limit"] = max(1, min(int(llm_cfg.get("summarize_limit") or max_items), max_items))
    llm_cfg["summarize_batch_size"] = max(1, min(int(llm_cfg.get("summarize_batch_size") or batch_size), batch_size))
    llm_cfg["summarize_timeout_sec"] = max(3, min(int(llm_cfg.get("summarize_timeout_sec") or timeout_sec), timeout_sec))
    effective["llm"] = llm_cfg
    return effective


def _needs_llm_summary(item: Dict[str, Any]) -> bool:
    source = str(item.get("summary_source") or "").strip().lower()
    summary_title = _clean_display_text(item.get("summary_title"))
    if summary_title and _is_llm_summary_source(source):
        return False
    if summary_title and not _is_fallback_summary_source(source):
        return False
    return bool(_clean_display_text(item.get("title")))


async def repair_recent_news_summaries(
    cfg: Dict[str, Any],
    *,
    hours: Optional[int] = None,
    raw_limit: Optional[int] = None,
    event_limit: Optional[int] = None,
) -> Dict[str, Any]:
    hours = max(6, min(int(hours or _env_int("NEWS_SUMMARY_REPAIR_HOURS", 24 * 30)), 24 * 90))
    raw_limit = max(0, min(int(raw_limit if raw_limit is not None else _env_int("NEWS_SUMMARY_REPAIR_RAW_LIMIT", 10)), 80))
    event_limit = max(0, min(int(event_limit if event_limit is not None else _env_int("NEWS_SUMMARY_REPAIR_EVENT_LIMIT", 10)), 80))
    total_limit = raw_limit + event_limit
    if total_limit <= 0:
        return {
            "hours": hours,
            "raw_candidates": 0,
            "event_candidates": 0,
            "requested": 0,
            "updated_raw_count": 0,
            "updated_event_count": 0,
            "skipped_non_llm": 0,
            "errors": [],
        }

    since = _now_utc() - timedelta(hours=hours)
    raw_scan_limit = max(80, raw_limit * max(4, _env_int("NEWS_SUMMARY_REPAIR_SCAN_MULTIPLIER", 8)))
    event_scan_limit = max(80, event_limit * max(4, _env_int("NEWS_SUMMARY_REPAIR_SCAN_MULTIPLIER", 8)))
    results = await asyncio.gather(
        news_db.list_news_raw(since=since, limit=raw_scan_limit),
        news_db.list_events(since=since, limit=event_scan_limit),
        return_exceptions=True,
    )
    raw_rows_result, event_rows_result = results
    errors: List[str] = []
    raw_rows = [] if isinstance(raw_rows_result, Exception) else list(raw_rows_result or [])
    event_rows = [] if isinstance(event_rows_result, Exception) else list(event_rows_result or [])
    if isinstance(raw_rows_result, Exception):
        errors.append(f"list_news_raw={type(raw_rows_result).__name__}")
    if isinstance(event_rows_result, Exception):
        errors.append(f"list_events={type(event_rows_result).__name__}")

    keywords = _topic_keywords(cfg)
    anchor_keywords = _topic_anchor_keywords(cfg)
    raw_candidates: List[Dict[str, Any]] = []
    if raw_limit > 0:
        for raw in raw_rows:
            if len(raw_candidates) >= raw_limit:
                break
            if not _is_relevant_news(raw, keywords, anchor_keywords):
                continue
            feed_item = _raw_as_feed_item(raw, None)
            if not _needs_llm_summary(feed_item):
                continue
            title = _clean_display_text(raw.get("title"))
            if not title:
                continue
            raw_candidates.append(
                {
                    "raw_news_id": int(raw.get("id")),
                    "title": title,
                }
            )

    event_candidates: List[Dict[str, Any]] = []
    if event_limit > 0:
        for event in event_rows:
            if len(event_candidates) >= event_limit:
                break
            feed_item = _event_as_feed_item(event)
            if not _needs_llm_summary(feed_item):
                continue
            title = _clean_display_text(feed_item.get("title"))
            if not title:
                continue
            event_candidates.append(
                {
                    "event_id": str(event.get("event_id") or ""),
                    "raw_news_id": event.get("raw_news_id"),
                    "title": title,
                }
            )

    targets: List[Dict[str, Any]] = []
    for item in raw_candidates:
        targets.append({"kind": "raw", **item})
    for item in event_candidates:
        targets.append({"kind": "event", **item})

    if not targets:
        return {
            "hours": hours,
            "raw_candidates": len(raw_candidates),
            "event_candidates": len(event_candidates),
            "requested": 0,
            "updated_raw_count": 0,
            "updated_event_count": 0,
            "skipped_non_llm": 0,
            "errors": errors,
        }

    summary_cfg = dict(cfg or {})
    llm_cfg = dict(summary_cfg.get("llm") or {})
    timeout_sec = max(6, min(int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 24), _env_int("NEWS_SUMMARY_REPAIR_TIMEOUT_SEC", 30)))
    batch_size = max(1, min(int(llm_cfg.get("summarize_batch_size") or 6), _env_int("NEWS_SUMMARY_REPAIR_BATCH_SIZE", 12), len(targets)))
    llm_cfg["summarize_limit"] = len(targets)
    llm_cfg["summarize_batch_size"] = batch_size
    llm_cfg["summarize_timeout_sec"] = timeout_sec
    summary_cfg["llm"] = llm_cfg

    try:
        summarized_results = await asyncio.wait_for(
            asyncio.to_thread(batch_summarize_titles, [item["title"] for item in targets], summary_cfg, 60),
            timeout=timeout_sec + 2,
        )
    except Exception as exc:
        logger.warning(f"background summary repair failed: {type(exc).__name__}: {exc}")
        return {
            "hours": hours,
            "raw_candidates": len(raw_candidates),
            "event_candidates": len(event_candidates),
            "requested": len(targets),
            "updated_raw_count": 0,
            "updated_event_count": 0,
            "skipped_non_llm": len(targets),
            "errors": errors + [f"summarize={type(exc).__name__}"],
        }

    raw_updates: List[Dict[str, Any]] = []
    event_updates: List[Dict[str, Any]] = []
    skipped_non_llm = 0
    for target, result in zip(targets, summarized_results):
        result_source = str((result or {}).get("source") or "").strip().lower()
        if not _is_llm_summary_source(result_source):
            skipped_non_llm += 1
            continue
        row = {
            "summary_title": (result or {}).get("summary") or target.get("title") or "",
            "summary_sentiment": (result or {}).get("sentiment") or "neutral",
            "summary_source": result_source,
        }
        if target.get("kind") == "raw" and target.get("raw_news_id"):
            raw_updates.append({"raw_news_id": int(target["raw_news_id"]), **row})
            continue
        if target.get("kind") == "event" and str(target.get("event_id") or "").strip():
            event_updates.append({"event_id": str(target["event_id"]), **row})
            raw_news_id = target.get("raw_news_id")
            if raw_news_id:
                raw_updates.append({"raw_news_id": int(raw_news_id), **row})

    raw_result = {"updated_count": 0, "skipped_count": 0}
    event_result = {"updated_count": 0, "skipped_count": 0}
    if raw_updates:
        with contextlib.suppress(Exception):
            raw_result = await news_db.save_news_raw_summaries(raw_updates)
    if event_updates:
        with contextlib.suppress(Exception):
            event_result = await news_db.save_news_event_summaries(event_updates)

    return {
        "hours": hours,
        "raw_candidates": len(raw_candidates),
        "event_candidates": len(event_candidates),
        "requested": len(targets),
        "updated_raw_count": int(raw_result.get("updated_count") or 0),
        "updated_event_count": int(event_result.get("updated_count") or 0),
        "skipped_non_llm": skipped_non_llm,
        "errors": errors,
    }


async def auto_requeue_failed_llm_tasks(
    cfg: Dict[str, Any],
    *,
    limit: Optional[int] = None,
    hours: Optional[int] = None,
    cooldown_sec: Optional[int] = None,
) -> Dict[str, Any]:
    del cfg  # reserved for future policy hooks
    global _FAILED_REQUEUE_LAST_AT

    queue = await news_db.get_llm_queue_stats()
    counts = dict(queue.get("counts") or {})
    policy = _failed_requeue_policy(counts, limit=limit, cooldown_sec=cooldown_sec)
    max_rows = int(policy.get("effective_limit") or 0)
    if max_rows <= 0:
        return {
            "enabled": False,
            "reason": "disabled",
            "requeued_count": 0,
            "queue_counts": counts,
            **policy,
        }
    if not _news_llm_enabled():
        return {
            "enabled": False,
            "reason": "llm_disabled",
            "requeued_count": 0,
            "queue_counts": counts,
            **policy,
        }

    cooldown = int(policy.get("effective_cooldown_sec") or 0)
    since_hours = max(24, min(int(hours if hours is not None else _env_int("NEWS_FAILED_REQUEUE_HOURS", 24 * 30)), 24 * 365))
    if int(queue.get("pending_total") or 0) > 0 or int(counts.get("running") or 0) > 0 or int(counts.get("retry") or 0) > 0:
        return {
            "enabled": True,
            "reason": "queue_busy",
            "requeued_count": 0,
            "queue_counts": counts,
            **policy,
        }
    if int(counts.get("failed") or 0) <= 0:
        return {
            "enabled": True,
            "reason": "no_failed",
            "requeued_count": 0,
            "queue_counts": counts,
            **policy,
        }

    now = _now_utc()
    if cooldown > 0 and _FAILED_REQUEUE_LAST_AT and (now - _FAILED_REQUEUE_LAST_AT).total_seconds() < cooldown:
        return {
            "enabled": True,
            "reason": "cooldown",
            "requeued_count": 0,
            "cooldown_remaining_sec": max(0, cooldown - int((now - _FAILED_REQUEUE_LAST_AT).total_seconds())),
            "queue_counts": counts,
            **policy,
        }

    async with _FAILED_REQUEUE_LOCK:
        now = _now_utc()
        if cooldown > 0 and _FAILED_REQUEUE_LAST_AT and (now - _FAILED_REQUEUE_LAST_AT).total_seconds() < cooldown:
            return {
                "enabled": True,
                "reason": "cooldown",
                "requeued_count": 0,
                "cooldown_remaining_sec": max(0, cooldown - int((now - _FAILED_REQUEUE_LAST_AT).total_seconds())),
                "queue_counts": counts,
                **policy,
            }
        result = await news_db.auto_requeue_failed_llm_tasks(
            limit=max_rows,
            since=now - timedelta(hours=since_hours),
        )
        requeued = int(result.get("requeued_count") or 0)
        if requeued > 0:
            _FAILED_REQUEUE_LAST_AT = now
        return {
            "enabled": True,
            "reason": "requeued" if requeued > 0 else "no_candidates",
            "cooldown_sec": cooldown,
            "hours": since_hours,
            "queue_counts": counts,
            **policy,
            **result,
        }


async def _auto_pull_if_stale(cfg: Dict[str, Any], latest_items: List[Dict[str, Any]], hours: int) -> bool:
    global _AUTO_PULL_RUNNING, _AUTO_PULL_LAST_AT
    stale_min = max(2, min(_cfg_int(cfg, "news_auto_pull_stale_min", 8), 180))
    cooldown_sec = max(5, min(_cfg_int(cfg, "news_auto_pull_cooldown_sec", 45), 600))
    latest_age = _latest_item_age_min(latest_items)
    should_pull = latest_age is None or latest_age >= float(stale_min)
    if not should_pull:
        return False

    now = _now_utc()
    if _AUTO_PULL_LAST_AT and (now - _AUTO_PULL_LAST_AT).total_seconds() < cooldown_sec:
        return False
    if _AUTO_PULL_RUNNING:
        return False

    async with _AUTO_PULL_LOCK:
        if _AUTO_PULL_RUNNING:
            return False
        if _AUTO_PULL_LAST_AT and (_now_utc() - _AUTO_PULL_LAST_AT).total_seconds() < cooldown_sec:
            return False
        _AUTO_PULL_RUNNING = True
        _AUTO_PULL_LAST_AT = _now_utc()

    async def _runner() -> None:
        global _AUTO_PULL_RUNNING
        try:
            await pull_and_store_news(
                cfg=cfg,
                payload=PullNowRequest(
                    since_minutes=max(60, min(1440, int(hours) * 60)),
                    max_records=max(60, _cfg_int(cfg, "news_auto_pull_max_records", 140)),
                ),
            )
        except Exception:
            pass
        finally:
            _AUTO_PULL_RUNNING = False

    asyncio.create_task(_runner())
    return True


def _count_by_provider(items: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        provider = str(item.get("provider") or payload.get("provider") or "legacy").strip().lower() or "legacy"
        counts[provider] = counts.get(provider, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))


def _build_source_summary(raw_rows: List[Dict[str, Any]], source_states: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for row in raw_rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        source = str(row.get("source") or payload.get("provider") or "unknown").strip().lower() or "unknown"
        slot = summary.setdefault(source, {"inserted_count": 0, "latencies": [], "max_importance": 0, "latest_at": None})
        slot["inserted_count"] += 1
        latency = _safe_float(payload.get("latency_sec"), 0.0)
        if latency > 0:
            slot["latencies"].append(latency)
        slot["max_importance"] = max(int(slot["max_importance"]), int(payload.get("importance_score") or 0))
        published = row.get("published_at")
        if published:
            slot["latest_at"] = str(published)
    state_map = {str(item.get("source") or "").strip().lower(): item for item in source_states}
    for source, slot in summary.items():
        latencies = slot.pop("latencies", [])
        state = state_map.get(source) or {}
        success_count = int(state.get("success_count") or 0)
        failure_count = int(state.get("failure_count") or 0)
        total_runs = success_count + failure_count
        slot["failure_rate"] = round((failure_count / total_runs), 4) if total_runs else 0.0
        slot["latency_p50"] = _percentile(latencies, 50)
        slot["latency_p95"] = _percentile(latencies, 95)
        slot["last_error"] = state.get("last_error")
        slot["paused_until"] = state.get("paused_until")
        slot["pending_errors"] = int(state.get("error_count") or 0)
    return dict(sorted(summary.items(), key=lambda kv: kv[1]["inserted_count"], reverse=True))


async def _emit_news_update_snapshot(limit: int = 12, hours: int = 24) -> None:
    try:
        from core.realtime import event_bus
    except Exception:
        return
    feed = await build_latest_feed(cfg=load_news_cfg(), symbol=None, hours=hours, limit=limit, summarize=False)
    await event_bus.publish_nowait_safe(
        event="news_update",
        payload={
            "timestamp": _now_utc().isoformat(),
            "count": int(feed.get("count") or 0),
            "items": feed.get("items") or [],
        },
    )


def _news_job_store(request: Request) -> Dict[str, Any]:
    store = getattr(request.app.state, "news_manual_jobs", None)
    if not isinstance(store, dict):
        store = {"active": None, "latest": None, "jobs": {}}
        request.app.state.news_manual_jobs = store
    return store


def _news_llm_job_store(request: Request) -> Dict[str, Any]:
    store = getattr(request.app.state, "news_manual_llm_jobs", None)
    if not isinstance(store, dict):
        store = {"active": None, "latest": None, "jobs": {}}
        request.app.state.news_manual_llm_jobs = store
    return store


async def _run_manual_pull_job(request: Request, job_id: str, cfg: Dict[str, Any], payload: PullNowRequest) -> None:
    store = _news_job_store(request)
    job = store["jobs"].get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    store["active"] = job_id
    store["jobs"][job_id] = job
    try:
        result = await pull_and_store_news(cfg=cfg, payload=payload)
        job["status"] = "completed"
        job["result"] = result
        store["latest"] = result
        with contextlib.suppress(Exception):
            await _emit_news_update_snapshot(limit=12, hours=24)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
    finally:
        job["finished_at"] = _now_utc().isoformat()
        if store.get("active") == job_id:
            store["active"] = None


async def _run_manual_llm_job(request: Request, job_id: str, cfg: Dict[str, Any], llm_limit: int) -> None:
    store = _news_llm_job_store(request)
    job = store["jobs"].get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    job["llm_limit"] = int(llm_limit)
    store["active"] = job_id
    store["jobs"][job_id] = job
    try:
        result = await process_llm_batch(cfg, limit=max(1, min(int(llm_limit or 8), 50)))
        failed_requeue = await auto_requeue_failed_llm_tasks(cfg)
        retry_result = {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}
        if int(failed_requeue.get("requeued_count") or 0) > 0:
            retry_result = await process_llm_batch(
                cfg,
                limit=max(1, min(int(llm_limit or 8), int(failed_requeue.get("requeued_count") or 0))),
            )
        summary_repair = await repair_recent_news_summaries(cfg)
        payload = {
            **result,
            "failed_requeue": failed_requeue,
            "retry_result": retry_result,
            "summary_repair": summary_repair,
            "timestamp": _now_utc().isoformat(),
            "source": "manual_run_once",
            "job_id": job_id,
            "llm_limit": int(llm_limit),
        }
        job["status"] = "completed"
        job["result"] = payload
        store["latest"] = payload
        request.app.state.news_last_llm_batch = payload
        _invalidate_news_caches()
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        request.app.state.news_last_llm_batch = {
            "timestamp": _now_utc().isoformat(),
            "claimed": 0,
            "events_count": 0,
            "errors": [str(exc)],
            "source": "manual_run_once",
            "job_id": job_id,
            "llm_limit": int(llm_limit),
        }
    finally:
        job["finished_at"] = _now_utc().isoformat()
        if store.get("active") == job_id:
            store["active"] = None


async def _run_manual_backfill_job(
    request: Request,
    job_id: str,
    cfg: Dict[str, Any],
    payload: BackfillRecentRequest,
) -> None:
    store = _news_llm_job_store(request)
    job = store["jobs"].get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    job["job_type"] = "backfill_recent"
    store["active"] = job_id
    store["jobs"][job_id] = job
    try:
        result = await _backfill_recent_events(
            cfg,
            hours=int(payload.hours),
            max_candidates=int(payload.max_candidates),
            force_reprocess_done=bool(payload.force_reprocess_done),
        )
        result_payload = {
            **result,
            "timestamp": _now_utc().isoformat(),
            "source": "manual_backfill_recent",
            "job_id": job_id,
            "job_type": "backfill_recent",
        }
        job["status"] = "completed"
        job["result"] = result_payload
        store["latest"] = result_payload
        request.app.state.news_last_llm_batch = result_payload
        _invalidate_news_caches()
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        request.app.state.news_last_llm_batch = {
            "timestamp": _now_utc().isoformat(),
            "claimed": 0,
            "events_count": 0,
            "errors": [str(exc)],
            "source": "manual_backfill_recent",
            "job_id": job_id,
            "job_type": "backfill_recent",
        }
    finally:
        job["finished_at"] = _now_utc().isoformat()
        if store.get("active") == job_id:
            store["active"] = None


def _event_lookup_maps(
    events: List[Dict[str, Any]],
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]], Dict[int, List[Dict[str, Any]]]]:
    by_url: Dict[str, List[Dict[str, Any]]] = {}
    by_title: Dict[str, List[Dict[str, Any]]] = {}
    by_raw_id: Dict[int, List[Dict[str, Any]]] = {}
    for event in events:
        evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
        url = _canonical_url(evidence.get("url"))
        title_key = _canonical_title(evidence.get("title"))
        raw_news_id = event.get("raw_news_id")
        if url:
            by_url.setdefault(url, []).append(event)
        if title_key:
            by_title.setdefault(title_key, []).append(event)
        if raw_news_id:
            by_raw_id.setdefault(int(raw_news_id), []).append(event)
    return by_url, by_title, by_raw_id


async def _backfill_recent_events(
    cfg: Dict[str, Any],
    hours: int = 24,
    max_candidates: int = 120,
    force_reprocess_done: bool = False,
) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    max_candidates = max(10, min(int(max_candidates or 120), 300))
    since = _now_utc() - timedelta(hours=hours)
    raw_rows = await news_db.list_news_raw(since=since, limit=5000)
    events = await news_db.list_events(since=since, limit=5000)
    if not raw_rows:
        return {"candidate_count": 0, "events_count": 0, "deduped_count": 0, "llm_used": False, "errors": []}
    task_status_map = await news_db.list_llm_task_status([int(row.get("id")) for row in raw_rows if row.get("id")])

    keywords = _topic_keywords(cfg)
    anchor_keywords = _topic_anchor_keywords(cfg)
    events_by_url, events_by_title, events_by_raw_id = _event_lookup_maps(events)

    candidates: List[Dict[str, Any]] = []
    seen_raw_keys: set[str] = set()
    skipped_by_task_status: Dict[str, int] = {"done": 0}
    for raw in raw_rows:
        if not _is_relevant_news(raw, keywords, anchor_keywords):
            continue
        raw_id = raw.get("id")
        task_status = str(task_status_map.get(int(raw_id)) or "").strip().lower() if raw_id else ""
        if task_status == "done" and not bool(force_reprocess_done):
            skipped_by_task_status["done"] = int(skipped_by_task_status.get("done", 0)) + 1
            continue
        raw_url = _canonical_url(raw.get("url"))
        raw_title = _canonical_title(raw.get("title"))
        if raw_id and int(raw_id) in events_by_raw_id:
            continue
        if raw_url and raw_url in events_by_url:
            continue
        if raw_title and raw_title in events_by_title:
            continue
        try:
            bucket = int(parse_any_datetime(raw.get("published_at")).timestamp() // 1800)
        except Exception:
            bucket = 0
        dedupe_key = f"{raw_title}|{bucket}"
        if not raw_title or dedupe_key in seen_raw_keys:
            continue
        seen_raw_keys.add(dedupe_key)
        candidates.append(raw)
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        return {
            "candidate_count": 0,
            "events_count": 0,
            "deduped_count": 0,
            "llm_used": False,
            "errors": [],
            "force_reprocess_done": bool(force_reprocess_done),
            "skipped_by_task_status": skipped_by_task_status,
        }

    extracted, llm_used, errors = await asyncio.to_thread(extract_events_llm_with_meta, candidates, cfg)
    url_to_provider: Dict[str, str] = {}
    title_to_provider: Dict[str, str] = {}
    for raw in candidates:
        provider = str(raw.get("provider") or (raw.get("payload") or {}).get("provider") or "").strip()
        if not provider:
            continue
        raw_url = str(raw.get("url") or "").strip()
        raw_title = _canonical_title(raw.get("title"))
        if raw_url:
            url_to_provider[raw_url] = provider
        if raw_title:
            title_to_provider[raw_title] = provider
    for event in extracted:
        evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
        provider = url_to_provider.get(str(evidence.get("url") or "").strip()) or title_to_provider.get(
            _canonical_title(evidence.get("title"))
        )
        if not provider:
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        payload["provider"] = provider
        event["payload"] = payload

    saved = await news_db.save_events(extracted, model_source="mixed_backfill")
    return {
        "candidate_count": len(candidates),
        "events_count": int(saved.get("events_count") or 0),
        "deduped_count": int(saved.get("deduped_count") or 0),
        "llm_used": bool(llm_used),
        "errors": errors,
        "force_reprocess_done": bool(force_reprocess_done),
        "skipped_by_task_status": skipped_by_task_status,
    }


async def build_latest_feed(
    cfg: Dict[str, Any],
    symbol: Optional[str] = None,
    hours: int = 24,
    limit: int = 30,
    summarize: bool = False,
) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    limit = max(1, min(int(limit or 30), 300))
    since = _now_utc() - timedelta(hours=hours)
    symbol_norm = _normalize_symbol(symbol, cfg)

    raw_limit = max(200, limit * 8)
    event_limit = max(300, limit * 10)
    db_timeout = max(2.5, min(8.0, float(_env_int("NEWS_API_FEED_DB_TIMEOUT_SEC", 5))))
    db_failures: List[str] = []
    raw_result, events_result = await asyncio.gather(
        asyncio.wait_for(asyncio.shield(news_db.list_news_raw(since=since, limit=raw_limit)), timeout=db_timeout),
        asyncio.wait_for(asyncio.shield(news_db.list_events(symbol=symbol_norm, since=since, limit=event_limit)), timeout=db_timeout),
        return_exceptions=True,
    )
    raw_items = list(raw_result) if not isinstance(raw_result, Exception) else []
    events = list(events_result) if not isinstance(events_result, Exception) else []
    if isinstance(raw_result, Exception):
        db_failures.append(f"list_news_raw={type(raw_result).__name__}")
    if isinstance(events_result, Exception):
        db_failures.append(f"list_events={type(events_result).__name__}")

    raw_ids = [int(row.get("id")) for row in raw_items if row.get("id")]
    task_status_map: Dict[int, str] = {}
    if raw_ids:
        task_status_result = await asyncio.gather(
            asyncio.wait_for(asyncio.shield(news_db.list_llm_task_status(raw_ids)), timeout=db_timeout),
            return_exceptions=True,
        )
        task_status_payload = task_status_result[0] if task_status_result else {}
        if isinstance(task_status_payload, Exception):
            db_failures.append(f"list_llm_task_status={type(task_status_payload).__name__}")
        else:
            task_status_map = dict(task_status_payload or {})
    min_importance = _llm_min_importance()
    keywords = _topic_keywords(cfg)
    anchor_keywords = _topic_anchor_keywords(cfg)

    events_by_url, events_by_title, events_by_raw_id = _event_lookup_maps(events)

    items: List[Dict[str, Any]] = []
    used_event_ids: set[str] = set()
    seen_story_keys: set[str] = set()
    reserve_structured = 0 if symbol_norm else min(max(6, limit // 3), max(0, limit - 8))
    raw_soft_limit = max(1, limit - reserve_structured) if reserve_structured else limit
    for raw in raw_items:
        if not _is_relevant_news(raw, keywords, anchor_keywords):
            continue
        raw_id = raw.get("id")
        related_symbols = _raw_related_symbols(raw, cfg)
        url = _canonical_url(raw.get("url"))
        title_key = _canonical_title(raw.get("title"))
        story_key = title_key or url
        if story_key and story_key in seen_story_keys:
            continue
        # Priority: match by raw_news_id (most reliable) > URL > title
        matched_events = (events_by_raw_id.get(int(raw_id)) if raw_id else None) or []
        if not matched_events:
            matched_events = events_by_url.get(url) or []
        if not matched_events:
            matched_events = events_by_title.get(title_key) or []
        matched_events = sorted(
            matched_events,
            key=lambda event: (
                -_safe_float(event.get("impact_score")),
                str(event.get("symbol") or ""),
                str(event.get("event_id") or ""),
            ),
        )

        picked_event: Optional[Dict[str, Any]] = None
        grouped_events: List[Dict[str, Any]] = []
        if symbol_norm:
            for event in matched_events:
                if str(event.get("symbol") or "").upper() == symbol_norm:
                    picked_event = event
                    break
            if matched_events and not picked_event:
                continue
            if not matched_events and symbol_norm not in related_symbols:
                continue
            grouped_events = [picked_event] if picked_event else []
        elif matched_events:
            picked_event = matched_events[0]
            grouped_events = matched_events

        feed_item = _raw_as_feed_item(raw, picked_event)
        if grouped_events:
            _merge_event_group_into_item(feed_item, grouped_events)
        elif not bool(feed_item.get("has_event")):
            task_status = str(task_status_map.get(int(raw_id)) or "").strip().lower() if raw_id else ""
            feed_item["llm_task_status"] = task_status
            feed_item["processing_status"] = _derive_unstructured_processing_status(
                raw_row=raw,
                llm_task_status=task_status,
                min_importance=min_importance,
            )
            if related_symbols:
                feed_item["related_symbols"] = _sorted_unique_texts(related_symbols)
            if symbol_norm and symbol_norm in related_symbols:
                feed_item["symbol"] = symbol_norm
        items.append(feed_item)
        if story_key:
            seen_story_keys.add(story_key)
        for event in grouped_events or ([picked_event] if picked_event else []):
            event_id = str((event or {}).get("event_id") or "")
            if event_id:
                used_event_ids.add(event_id)
        if len(items) >= raw_soft_limit:
            break

    if len(items) < limit:
        grouped_event_order: List[str] = []
        grouped_event_map: Dict[str, List[Dict[str, Any]]] = {}
        for event in events:
            event_id = str(event.get("event_id") or "")
            if event_id and event_id in used_event_ids:
                continue
            story_key = _event_story_key(event)
            if story_key not in grouped_event_map:
                grouped_event_map[story_key] = []
                grouped_event_order.append(story_key)
            grouped_event_map[story_key].append(event)

        for story_key in grouped_event_order:
            event_group = grouped_event_map.get(story_key) or []
            if not event_group:
                continue
            event_group = sorted(
                event_group,
                key=lambda event: (
                    -_safe_float(event.get("impact_score")),
                    str(event.get("symbol") or ""),
                    str(event.get("event_id") or ""),
                ),
            )
            primary = event_group[0]
            feed_item = _event_as_feed_item(primary)
            _merge_event_group_into_item(feed_item, event_group)
            items.append(feed_item)
            for event in event_group:
                event_id = str(event.get("event_id") or "")
                if event_id:
                    used_event_ids.add(event_id)
            if len(items) >= limit:
                break

    sorted_items = _sort_by_published_desc(items)

    # Summarize titles if requested
    if summarize and sorted_items:
        summary_cfg = _feed_summarize_cfg(cfg, limit=limit)
        llm_cfg = summary_cfg.get("llm") or {}
        summarize_limit = int(llm_cfg.get("summarize_limit") or min(120, limit))
        summarize_limit = max(1, min(limit, summarize_limit))
        summarize_timeout_sec = int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 20)
        summarize_timeout_sec = max(3, min(20, summarize_timeout_sec))

        candidates: List[tuple[int, Dict[str, Any]]] = []
        for idx, item in enumerate(sorted_items):
            if not _needs_llm_summary(item):
                continue
            candidates.append((idx, item))

        target_pairs = candidates[:summarize_limit]
        target_indices = [idx for idx, _ in target_pairs]
        titles = [item.get("title") or "" for _, item in target_pairs]
        summarized_results: List[Dict[str, Any]] = []
        if titles:
            try:
                summarized_results = await asyncio.wait_for(
                    asyncio.to_thread(batch_summarize_titles, titles, summary_cfg, 60),
                    timeout=summarize_timeout_sec + 2,
                )
            except Exception as e:
                logger.warning(f"title summarize timeout/failure, fallback to rule sentiment: {e}")
                summarized_results = []
                for t in titles:
                    item = _summarize_fallback(t or "", 60)
                    item.setdefault("source", "api_timeout_fallback")
                    summarized_results.append(item)

        persist_raw_rows: List[Dict[str, Any]] = []
        persist_event_rows: List[Dict[str, Any]] = []
        for item_idx, result in zip(target_indices, summarized_results):
            item = sorted_items[item_idx]
            existing_source = str(item.get("summary_source") or "").strip().lower()
            result_source = str(result.get("source") or "unknown").strip().lower()
            if _is_llm_summary_source(existing_source) and _is_fallback_summary_source(result_source):
                continue
            item["summary_title"] = result.get("summary", item.get("title", ""))
            item["summary_sentiment"] = result.get("sentiment", "neutral")
            item["summary_source"] = result_source or "unknown"

            if not _is_llm_summary_source(result_source):
                continue

            raw_id = item.get("raw_news_id")
            if raw_id:
                persist_raw_rows.append(
                    {
                        "raw_news_id": int(raw_id),
                        "summary_title": item.get("summary_title"),
                        "summary_sentiment": item.get("summary_sentiment"),
                        "summary_source": result_source,
                    }
                )
            if bool(item.get("has_event")) and str(item.get("event_id") or "").strip():
                persist_event_rows.append(
                    {
                        "event_id": str(item.get("event_id") or ""),
                        "summary_title": item.get("summary_title"),
                        "summary_sentiment": item.get("summary_sentiment"),
                        "summary_source": result_source,
                    }
                )

        if persist_raw_rows:
            with contextlib.suppress(Exception):
                await news_db.save_news_raw_summaries(persist_raw_rows)
        if persist_event_rows:
            with contextlib.suppress(Exception):
                await news_db.save_news_event_summaries(persist_event_rows)
        summarized_set = set(target_indices)
        for idx, item in enumerate(sorted_items):
            if idx in summarized_set:
                continue
            if item.get("summary_title"):
                continue
            item["summary_title"] = item.get("title", "")
            item["summary_sentiment"] = "neutral"
            item["summary_source"] = "not_summarized"

    _apply_display_summaries(sorted_items)

    by_provider = _count_by_provider(sorted_items)
    by_source: Dict[str, int] = {}
    for item in sorted_items:
        source_name = str(item.get("source") or "unknown").strip().lower() or "unknown"
        by_source[source_name] = by_source.get(source_name, 0) + 1

    response = {
        "count": len(items),
        "symbol": symbol_norm,
        "hours": hours,
        "since": since.isoformat(),
        "items": sorted_items,
        "feed_stats": _feed_sentiment_summary(sorted_items),
        "source_stats": {
            "by_provider": by_provider,
            "by_source": dict(sorted(by_source.items(), key=lambda kv: kv[1], reverse=True)[:12]),
        },
    }
    if db_failures:
        response["db_failures"] = db_failures
    return response


async def pull_and_store_news(cfg: Dict[str, Any], payload: PullNowRequest) -> Dict[str, Any]:
    async with _NEWS_PIPELINE_LOCK:
        collector = MultiSourceNewsCollector(cfg)
        errors: List[str] = []
        filtered_out_count = 0
        backfill_stats = {"candidate_count": 0, "events_count": 0, "deduped_count": 0, "llm_used": False, "errors": []}

        try:
            pulled_bundle = await collector.pull_latest_incremental(
                query=payload.query,
                max_records=payload.max_records,
                since_minutes=payload.since_minutes,
            )
            pulled_all = pulled_bundle.get("items") or []
            source_stats = pulled_bundle.get("source_stats") or {}
            errors.extend([str(x) for x in (pulled_bundle.get("errors") or []) if str(x).strip()])
            keywords = _topic_keywords(cfg)
            anchor_keywords = _topic_anchor_keywords(cfg)
            pulled = [item for item in pulled_all if _is_relevant_news(item, keywords, anchor_keywords)]
            filtered_out_count = max(0, len(pulled_all) - len(pulled))
            min_keep = min(max(12, int(payload.max_records * 0.3)), len(pulled_all))
            if len(pulled) < min_keep:
                preferred = []
                for item in pulled_all:
                    provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "").strip().lower()
                    if provider in {"jin10", "rss", "newsapi", "cryptopanic"}:
                        preferred.append(item)
                candidates = preferred + [x for x in pulled_all if x not in preferred]
                seen_urls = {str(x.get("url") or "").strip() for x in pulled}
                for item in candidates:
                    url = str(item.get("url") or "").strip()
                    if url and url in seen_urls:
                        continue
                    pulled.append(item)
                    if url:
                        seen_urls.add(url)
                    if len(pulled) >= min_keep:
                        break
                filtered_out_count = max(0, len(pulled_all) - len(pulled))
            if not pulled and pulled_all:
                pulled = pulled_all[: min(12, len(pulled_all))]
                filtered_out_count = max(0, len(pulled_all) - len(pulled))
        except Exception as exc:
            errors.append(f"news pull failed: {exc}")
            pulled = []
            source_stats = {}

        raw_stats = await news_db.save_news_raw(pulled)
        new_news = raw_stats.get("inserted") or []
        sync_llm = _env_bool("NEWS_PULL_SYNC_LLM", False)
        if new_news and not sync_llm:
            queue_stats = await news_db.enqueue_llm_tasks(new_news, min_importance=_env_int("NEWS_LLM_MIN_IMPORTANCE", 35))
            with contextlib.suppress(Exception):
                asyncio.create_task(process_llm_batch(cfg, limit=max(4, min(16, len(new_news)))))
        else:
            queue_stats = {"queued_count": 0, "skipped_count": 0}

        topic_matched_by_provider = _count_by_provider(pulled)
        inserted_by_provider = _count_by_provider(new_news)
        for provider, stat in source_stats.items():
            if not isinstance(stat, dict):
                continue
            stat["topic_matched_count"] = int(topic_matched_by_provider.get(provider, 0))
            stat["raw_inserted_count"] = int(inserted_by_provider.get(provider, 0))

        llm_used = False
        if new_news:
            if sync_llm:
                events, llm_used, llm_errors = await asyncio.to_thread(extract_events_llm_with_meta, new_news, cfg)
                errors.extend(llm_errors)
                url_to_provider: Dict[str, str] = {}
                for raw in new_news:
                    provider = str(raw.get("provider") or (raw.get("payload") or {}).get("provider") or "").strip()
                    url = str(raw.get("url") or "").strip()
                    if provider and url:
                        url_to_provider[url] = provider
                for event in events:
                    evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
                    event_url = str(evidence.get("url") or "").strip()
                    provider = url_to_provider.get(event_url)
                    if not provider:
                        continue
                    event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                    event_payload["provider"] = provider
                    event["payload"] = event_payload
            else:
                events = []
        else:
            events = []

        event_stats = await news_db.save_events(events, model_source="mixed")
        if sync_llm and new_news:
            with contextlib.suppress(Exception):
                await news_db.finish_llm_tasks(
                    [int(item["id"]) for item in new_news if item.get("id")],
                    success=True,
                )
        events_by_provider = _count_by_provider(events)
        for provider, stat in source_stats.items():
            if not isinstance(stat, dict):
                continue
            stat["events_extracted_count"] = int(events_by_provider.get(provider, 0))

        if sync_llm:
            try:
                backfill_stats = await _backfill_recent_events(
                    cfg,
                    hours=max(24, max(1, int(payload.since_minutes / 60))),
                    max_candidates=min(180, max(60, int(payload.max_records * 1.5))),
                )
                errors.extend([str(x) for x in (backfill_stats.get("errors") or []) if str(x).strip()])
            except Exception as exc:
                errors.append(f"recent backfill failed: {exc}")

        _invalidate_news_caches()

        return {
            "pulled_count": int(raw_stats.get("pulled_count") or 0),
            "pulled_all_count": len(pulled_all) if "pulled_all" in locals() else 0,
            "deduped_count": int(raw_stats.get("deduped_count") or 0),
            "raw_inserted_count": len(new_news),
            "filtered_out_count": int(filtered_out_count),
            "events_count": int(event_stats.get("events_count") or 0),
            "events_deduped_count": int(event_stats.get("deduped_count") or 0),
            "backfill_candidate_count": int(backfill_stats.get("candidate_count") or 0),
            "backfill_events_count": int(backfill_stats.get("events_count") or 0),
            "backfill_events_deduped_count": int(backfill_stats.get("deduped_count") or 0),
            "queued_count": int(queue_stats.get("queued_count") or 0),
            "source_stats": source_stats,
            "llm_used": bool(llm_used or backfill_stats.get("llm_used")),
            "sync_llm": bool(sync_llm),
            "errors": errors,
            "timestamp": _now_utc().isoformat(),
        }


async def backfill_and_store_news_history(cfg: Dict[str, Any], payload: BackfillHistoryRequest) -> Dict[str, Any]:
    async with _NEWS_PIPELINE_LOCK:
        collector = MultiSourceNewsCollector(cfg)
        errors: List[str] = []
        source_names = _normalize_source_names(payload.source_names)
        requested_at = _now_utc()

        try:
            pulled_bundle = await asyncio.to_thread(
                collector.pull_latest,
                payload.query,
                int(payload.max_records),
                int(payload.hours) * 60,
                source_names or None,
            )
            pulled_all = list(pulled_bundle.get("items") or [])
            source_stats = dict(pulled_bundle.get("source_stats") or {})
            errors.extend([str(x) for x in (pulled_bundle.get("errors") or []) if str(x).strip()])
        except Exception as exc:
            pulled_all = []
            source_stats = {}
            errors.append(f"history backfill pull failed: {exc}")

        keywords = _topic_keywords(cfg)
        anchor_keywords = _topic_anchor_keywords(cfg)
        pulled = [item for item in pulled_all if _is_relevant_news(item, keywords, anchor_keywords)]
        filtered_out_count = max(0, len(pulled_all) - len(pulled))
        if not pulled and pulled_all:
            pulled = pulled_all[: min(40, len(pulled_all))]
            filtered_out_count = max(0, len(pulled_all) - len(pulled))

        raw_stats = await news_db.save_news_raw(
            pulled,
            ingest_meta={
                "mode": "history_backfill",
                "requested_at": requested_at,
                "lookback_hours": int(payload.hours),
                "query": str(payload.query or "").strip() or None,
                "source_names": source_names,
            },
        )
        inserted_rows = list(raw_stats.get("inserted") or [])

        queue_stats = {"queued_count": 0, "skipped_count": 0}
        if inserted_rows and bool(payload.enqueue_llm):
            queue_stats = await news_db.enqueue_llm_tasks(inserted_rows, min_importance=_llm_min_importance())

        inserted_by_provider = _count_by_provider(inserted_rows)
        topic_matched_by_provider = _count_by_provider(pulled)
        for provider, stat in source_stats.items():
            if not isinstance(stat, dict):
                continue
            stat["topic_matched_count"] = int(topic_matched_by_provider.get(provider, 0))
            stat["raw_inserted_count"] = int(inserted_by_provider.get(provider, 0))

        coverage = await news_db.summarize_news_raw_coverage()
        return {
            "mode": "history_backfill",
            "requested_at": requested_at.isoformat(),
            "lookback_hours": int(payload.hours),
            "requested_sources": source_names,
            "query": str(payload.query or "").strip() or None,
            "pulled_count": int(raw_stats.get("pulled_count") or 0),
            "pulled_all_count": len(pulled_all),
            "deduped_count": int(raw_stats.get("deduped_count") or 0),
            "raw_inserted_count": len(inserted_rows),
            "filtered_out_count": int(filtered_out_count),
            "queued_count": int(queue_stats.get("queued_count") or 0),
            "source_stats": source_stats,
            "coverage": coverage,
            "archive_contract": _news_archive_contract(),
            "errors": errors,
        }


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    cache_key = "default"
    ttl_sec = _env_int("NEWS_API_HEALTH_CACHE_TTL_SEC", 15)
    cached = _cache_get("health", cache_key, ttl_sec)
    if cached:
        return cached

    base_payload = _news_runtime_snapshot(request)
    stale = _cache_get_stale("health", cache_key)
    if stale:
        base_payload["source_states"] = list(stale.get("source_states") or [])
        base_payload["llm_queue"] = dict(stale.get("llm_queue") or {})
    db_timeout = max(2, _env_int("NEWS_API_HEALTH_DB_TIMEOUT_SEC", 4))
    results = await asyncio.gather(
        asyncio.wait_for(asyncio.shield(news_db.list_source_states()), timeout=db_timeout),
        asyncio.wait_for(asyncio.shield(news_db.get_llm_queue_stats()), timeout=db_timeout),
        return_exceptions=True,
    )
    source_states_result, llm_queue_result = results
    failures: List[str] = []
    if not isinstance(source_states_result, Exception):
        base_payload["source_states"] = source_states_result
    else:
        failures.append(f"source_states={type(source_states_result).__name__}")
    if not isinstance(llm_queue_result, Exception):
        base_payload["llm_queue"] = llm_queue_result
    else:
        failures.append(f"llm_queue={type(llm_queue_result).__name__}")

    if not failures:
        base_payload["status"] = "ok"
        return _cache_set("health", cache_key, base_payload)

    reason = ", ".join(failures)
    logger.warning(f"news health degraded: {reason}")
    base_payload["status"] = "degraded"
    base_payload["fallback_reason"] = reason
    if base_payload.get("source_states") or base_payload.get("llm_queue") or base_payload.get("last_llm_batch") or base_payload.get("last_pull"):
        return base_payload
    if stale:
        stale["status"] = "degraded"
        stale["fallback_reason"] = reason
        return stale
    return base_payload


@router.post("/pull_now")
async def pull_now(
    request: Request,
    payload: PullNowRequest = Body(default_factory=PullNowRequest),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    if not background:
        return await pull_and_store_news(cfg=cfg, payload=payload)
    global _MANUAL_PULL_SEQ
    store = _news_job_store(request)
    active_job_id = store.get("active")
    if active_job_id:
        active = store.get("jobs", {}).get(active_job_id) or {}
        return {
            "queued": False,
            "status": "running",
            "job_id": active_job_id,
            "message": "已有新闻结构化任务在后台运行，当前请求未重复启动",
            "job": active,
        }
    _MANUAL_PULL_SEQ += 1
    job_id = f"news-pull-{_MANUAL_PULL_SEQ:06d}"
    job = {
        "job_id": job_id,
        "status": "pending",
        "created_at": _now_utc().isoformat(),
        "payload": payload.model_dump(),
        "result": None,
        "error": None,
    }
    store["jobs"][job_id] = job
    store["active"] = job_id
    asyncio.create_task(_run_manual_pull_job(request, job_id, cfg, payload))
    return {
        "queued": True,
        "status": "pending",
        "job_id": job_id,
        "message": "新闻抓取与结构化已转入后台串行执行",
        "job": job,
        "latest_result": store.get("latest"),
    }


@router.post("/ingest/pull_now")
async def pull_now_alias(
    request: Request,
    payload: PullNowRequest = Body(default_factory=PullNowRequest),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    return await pull_now(request=request, payload=payload, background=background)


@router.get("/pull_status")
async def pull_status(request: Request) -> Dict[str, Any]:
    cache_key = "default"
    ttl_sec = _env_int("NEWS_API_PULL_STATUS_CACHE_TTL_SEC", 6)
    pull_store = _news_job_store(request)
    llm_store = _news_llm_job_store(request)
    active_job_id = pull_store.get("active")
    active_job = (pull_store.get("jobs") or {}).get(active_job_id) if active_job_id else None
    active_llm_job_id = llm_store.get("active")
    active_llm_job = (llm_store.get("jobs") or {}).get(active_llm_job_id) if active_llm_job_id else None
    if not active_job_id and not active_llm_job_id:
        cached = _cache_get("pull_status", cache_key, ttl_sec)
        if cached:
            return cached

    payload = {
        "active_job_id": active_job_id,
        "active_job": active_job,
        "latest_result": pull_store.get("latest"),
        "jobs": list((pull_store.get("jobs") or {}).values())[-10:],
        "active_llm_job_id": active_llm_job_id,
        "active_llm_job": active_llm_job,
        "latest_llm_result": llm_store.get("latest"),
        "llm_jobs": list((llm_store.get("jobs") or {}).values())[-10:],
        "source_states": [],
        "llm_queue": {},
    }
    try:
        db_timeout = max(2, _env_int("NEWS_API_STATUS_DB_TIMEOUT_SEC", 4))
        db_snapshot = await _collect_news_db_snapshot(db_timeout)
        payload["source_states"] = list(db_snapshot.get("source_states") or [])
        payload["llm_queue"] = dict(db_snapshot.get("llm_queue") or {})
        if db_snapshot.get("failures"):
            payload["status"] = "degraded"
            payload["fallback_reason"] = ", ".join(list(db_snapshot.get("failures") or []))
        else:
            payload["status"] = "ok"
    except Exception as exc:
        payload["status"] = "degraded"
        payload["fallback_reason"] = f"pull_status:{type(exc).__name__}"
        stale = _cache_get_stale("pull_status", cache_key)
        if stale:
            stale["status"] = "degraded"
            stale["fallback_reason"] = payload["fallback_reason"]
            return stale

    if active_job_id or active_llm_job_id:
        return payload
    return _cache_set("pull_status", cache_key, payload)


@router.get("/worker_status")
async def worker_status(request: Request) -> Dict[str, Any]:
    cache_key = "default"
    ttl_sec = _env_int("NEWS_API_WORKER_STATUS_CACHE_TTL_SEC", 8)
    pull_store = _news_job_store(request)
    llm_store = _news_llm_job_store(request)
    has_active_llm_job = bool(llm_store.get("active"))
    if not has_active_llm_job:
        cached = _cache_get("worker_status", cache_key, ttl_sec)
        if cached:
            return cached

    payload = {
        "timestamp": _now_utc().isoformat(),
        "latest_result": pull_store.get("latest"),
        "manual_llm_job": {
            "active_job_id": llm_store.get("active"),
            "latest_result": llm_store.get("latest"),
        },
        **_news_background_state(request),
        "last_pull": getattr(request.app.state, "news_last_pull", None),
        "last_llm_batch": getattr(request.app.state, "news_last_llm_batch", None),
        "source_states": [],
        "llm_queue": {},
    }
    try:
        db_timeout = max(2, _env_int("NEWS_API_STATUS_DB_TIMEOUT_SEC", 4))
        db_snapshot = await _collect_news_db_snapshot(db_timeout)
        payload["source_states"] = list(db_snapshot.get("source_states") or [])
        payload["llm_queue"] = dict(db_snapshot.get("llm_queue") or {})
        if db_snapshot.get("failures"):
            payload["status"] = "degraded"
            payload["fallback_reason"] = ", ".join(list(db_snapshot.get("failures") or []))
        else:
            payload["status"] = "ok"
    except Exception as exc:
        payload["status"] = "degraded"
        payload["fallback_reason"] = f"worker_status:{type(exc).__name__}"
        stale = _cache_get_stale("worker_status", cache_key)
        if stale:
            stale["status"] = "degraded"
            stale["fallback_reason"] = payload["fallback_reason"]
            return stale

    if has_active_llm_job:
        return payload
    return _cache_set("worker_status", cache_key, payload)


@router.post("/worker/run_once")
async def worker_run_once(
    request: Request,
    llm_limit: int = Query(default=8, ge=1, le=50),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    llm_limit = max(1, min(int(llm_limit or 8), 50))
    if background:
        global _MANUAL_LLM_SEQ
        store = _news_llm_job_store(request)
        active_job_id = store.get("active")
        if active_job_id:
            active = store.get("jobs", {}).get(active_job_id) or {}
            return {
                "queued": False,
                "status": "running",
                "job_id": active_job_id,
                "job": active,
                "llm_queue": await news_db.get_llm_queue_stats(),
            }
        _MANUAL_LLM_SEQ += 1
        job_id = f"news-llm-{_MANUAL_LLM_SEQ:06d}"
        job = {
            "job_id": job_id,
            "status": "pending",
            "created_at": _now_utc().isoformat(),
            "llm_limit": llm_limit,
            "result": None,
            "error": None,
        }
        store["jobs"][job_id] = job
        store["active"] = job_id
        asyncio.create_task(_run_manual_llm_job(request, job_id, cfg, llm_limit))
        return {
            "queued": True,
            "status": "pending",
            "job_id": job_id,
            "job": job,
            "latest_result": store.get("latest"),
            "llm_queue": await news_db.get_llm_queue_stats(),
        }
    result = await process_llm_batch(cfg, limit=llm_limit)
    failed_requeue = await auto_requeue_failed_llm_tasks(cfg)
    retry_result = {"claimed": 0, "events_count": 0, "llm_used": False, "errors": []}
    if int(failed_requeue.get("requeued_count") or 0) > 0:
        retry_result = await process_llm_batch(
            cfg,
            limit=max(1, min(int(llm_limit or 8), int(failed_requeue.get("requeued_count") or 0))),
        )
    summary_repair = await repair_recent_news_summaries(cfg)
    _invalidate_news_caches()
    request.app.state.news_last_llm_batch = {
        **result,
        "failed_requeue": failed_requeue,
        "retry_result": retry_result,
        "summary_repair": summary_repair,
        "timestamp": _now_utc().isoformat(),
        "source": "manual_run_once",
    }
    return {
        "timestamp": _now_utc().isoformat(),
        "llm": result,
        "failed_requeue": failed_requeue,
        "retry_result": retry_result,
        "summary_repair": summary_repair,
        "llm_queue": await news_db.get_llm_queue_stats(),
        "source_states": await news_db.list_source_states(),
    }


@router.post("/worker/backfill_recent")
async def worker_backfill_recent(
    request: Request,
    payload: BackfillRecentRequest = Body(default_factory=BackfillRecentRequest),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    if background:
        global _MANUAL_LLM_SEQ
        store = _news_llm_job_store(request)
        active_job_id = store.get("active")
        if active_job_id:
            active = store.get("jobs", {}).get(active_job_id) or {}
            return {
                "queued": False,
                "status": "running",
                "job_id": active_job_id,
                "job": active,
                "llm_queue": await news_db.get_llm_queue_stats(),
            }
        _MANUAL_LLM_SEQ += 1
        job_id = f"news-backfill-{_MANUAL_LLM_SEQ:06d}"
        job = {
            "job_id": job_id,
            "job_type": "backfill_recent",
            "status": "pending",
            "created_at": _now_utc().isoformat(),
            "payload": payload.model_dump(),
            "result": None,
            "error": None,
        }
        store["jobs"][job_id] = job
        store["active"] = job_id
        asyncio.create_task(_run_manual_backfill_job(request, job_id, cfg, payload))
        return {
            "queued": True,
            "status": "pending",
            "job_id": job_id,
            "job": job,
            "latest_result": store.get("latest"),
            "llm_queue": await news_db.get_llm_queue_stats(),
        }

    result = await _backfill_recent_events(
        cfg,
        hours=int(payload.hours),
        max_candidates=int(payload.max_candidates),
        force_reprocess_done=bool(payload.force_reprocess_done),
    )
    _invalidate_news_caches()
    request.app.state.news_last_llm_batch = {
        **result,
        "timestamp": _now_utc().isoformat(),
        "source": "manual_backfill_recent",
    }
    return {
        "timestamp": _now_utc().isoformat(),
        "backfill": result,
        "llm_queue": await news_db.get_llm_queue_stats(),
        "source_states": await news_db.list_source_states(),
    }


@router.post("/worker/requeue")
async def worker_requeue_llm_tasks(
    request: Request,
    payload: RequeueLLMTasksRequest = Body(default_factory=RequeueLLMTasksRequest),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    statuses = [str(x or "").strip().lower() for x in list(payload.statuses or []) if str(x or "").strip()]
    failed_only = set(statuses or ["failed"]) == {"failed"}
    if failed_only:
        result = await news_db.auto_requeue_failed_llm_tasks(
            limit=min(int(payload.limit), 200),
            since=_now_utc() - timedelta(hours=24 * 180),
        )
        result["mode"] = "filtered_failed_only"
    else:
        result = await news_db.requeue_llm_tasks(statuses=statuses or ["failed"], limit=int(payload.limit))
        result["mode"] = "force_requeue"
    requeued_count = int(result.get("requeued_count") or 0)
    if requeued_count > 0:
        with contextlib.suppress(Exception):
            asyncio.create_task(process_llm_batch(cfg, limit=max(8, min(48, requeued_count))))
    _invalidate_news_caches()
    return {
        "timestamp": _now_utc().isoformat(),
        "requeue": result,
        "llm_queue": await news_db.get_llm_queue_stats(),
        "source_states": await news_db.list_source_states(),
    }


@router.post("/ingest/backfill_history")
async def ingest_backfill_history(
    request: Request,
    payload: BackfillHistoryRequest = Body(default_factory=BackfillHistoryRequest),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    result = await backfill_and_store_news_history(cfg, payload)
    request.app.state.news_last_pull = {
        **result,
        "timestamp": _now_utc().isoformat(),
        "source": "manual_history_backfill",
    }
    _invalidate_news_caches(clear_feed=True)
    return request.app.state.news_last_pull


@router.get("/raw/history")
async def raw_history(
    request: Request,
    source: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0, le=50000),
) -> Dict[str, Any]:
    del request
    try:
        since_ts = parse_any_datetime(since) if since else None
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid since value: {exc}") from exc
    try:
        until_ts = parse_any_datetime(until) if until else None
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid until value: {exc}") from exc

    result = await news_db.list_news_raw_history(
        since=since_ts,
        until=until_ts,
        source=source,
        text_query=q,
        limit=limit,
        offset=offset,
    )
    return {
        **result,
        "source": str(source or "").strip().lower() or None,
        "since": since_ts.isoformat() if since_ts else None,
        "until": until_ts.isoformat() if until_ts else None,
        "query": str(q or "").strip() or None,
        "archive_contract": _news_archive_contract(),
    }


@router.get("/raw/coverage")
async def raw_coverage(request: Request) -> Dict[str, Any]:
    del request
    cache_key = "default"
    ttl_sec = _env_int("NEWS_API_COVERAGE_CACHE_TTL_SEC", 120)
    cached = _cache_get("coverage", cache_key, ttl_sec)
    if cached:
        return cached

    timeout_sec = max(2.0, min(8.0, float(_env_int("NEWS_API_COVERAGE_DB_TIMEOUT_SEC", 5))))
    try:
        coverage = await asyncio.wait_for(
            asyncio.shield(news_db.summarize_news_raw_coverage()),
            timeout=timeout_sec,
        )
        payload = {
            **coverage,
            "archive_contract": _news_archive_contract(),
            "timestamp": _now_utc().isoformat(),
        }
        return _cache_set("coverage", cache_key, payload)
    except Exception as exc:
        stale = _cache_get_stale("coverage", cache_key)
        if stale:
            stale["degraded"] = True
            stale["fallback_reason"] = str(exc)
            return stale

        fallback = {
            "total_count": 0,
            "history_span_days": 0.0,
            "earliest_published_at": None,
            "latest_published_at": None,
            "earliest_fetched_at": None,
            "latest_fetched_at": None,
            "count_24h": 0,
            "count_7d": 0,
            "count_30d": 0,
            "active_sources_7d": 0,
            "top_sources": [],
            "recent_daily_counts": [],
            "sampled_provider_counts": {},
            "recent_ingest_mode_counts": {},
            "sample_size": 0,
            "archive_contract": _news_archive_contract(),
            "timestamp": _now_utc().isoformat(),
            "degraded": True,
            "fallback_reason": str(exc),
        }
        return fallback


@router.get("/latest")
async def latest(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=30, ge=1, le=300),
    summarize: bool = Query(default=False),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)
    cache_key = _cache_key(symbol_norm, hours, limit, "sum" if summarize else "fast")
    ttl_sec = _env_int("NEWS_API_SUMMARY_CACHE_TTL_SEC" if summarize else "NEWS_API_LATEST_CACHE_TTL_SEC", 20 if summarize else 8)
    cached = _cache_get("latest", cache_key, ttl_sec)
    if cached:
        cached["auto_pull_triggered"] = False
        return cached

    try:
        latest_timeout = max(4.0, min(10.0, float(_env_int("NEWS_API_LATEST_TOTAL_TIMEOUT_SEC", 6 if not summarize else 9))))
        if summarize:
            feed = await asyncio.wait_for(
                build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=True),
                timeout=latest_timeout,
            )
        else:
            feed = await asyncio.wait_for(
                build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=False),
                timeout=latest_timeout,
            )
        auto_pull = await _auto_pull_if_stale(cfg=cfg, latest_items=feed.get("items") or [], hours=hours)
        feed["auto_pull_triggered"] = bool(auto_pull)
        return _cache_set("latest", cache_key, feed)
    except Exception as exc:
        logger.warning(f"news latest failed summarize={summarize} symbol={symbol_norm or '-'}: {exc}")
        stale = _cache_get_stale("latest", cache_key)
        if stale:
            stale["auto_pull_triggered"] = False
            stale["fallback_reason"] = str(exc)
            return stale
        if summarize:
            fast_key = _cache_key(symbol_norm, hours, limit, "fast")
            fast_stale = _cache_get("latest", fast_key, _env_int("NEWS_API_LATEST_CACHE_TTL_SEC", 8)) or _cache_get_stale("latest", fast_key)
            if fast_stale:
                fast_stale["auto_pull_triggered"] = False
                fast_stale["fallback_reason"] = f"summarize fallback: {exc}"
                return fast_stale
            feed = await build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=False)
            feed["auto_pull_triggered"] = False
            feed["fallback_reason"] = f"summarize fallback: {exc}"
            return _cache_set("latest", fast_key, feed)
        return {
            "count": 0,
            "symbol": symbol_norm,
            "hours": hours,
            "since": (_now_utc() - timedelta(hours=max(1, int(hours or 24)))).isoformat(),
            "items": [],
            "feed_stats": _feed_sentiment_summary([]),
            "source_stats": {"by_provider": {}, "by_source": {}},
            "auto_pull_triggered": False,
            "fallback_reason": str(exc),
            "degraded": True,
        }


@router.get("/brief")
async def brief(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
    feed_limit: int = Query(default=40, ge=10, le=120),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)
    cache_key = _cache_key(symbol_norm, hours, feed_limit)
    ttl_sec = _env_int("NEWS_API_BRIEF_CACHE_TTL_SEC", 8)
    cached = _cache_get("brief", cache_key, ttl_sec)
    if cached:
        return cached

    since = _now_utc() - timedelta(hours=hours)
    try:
        db_timeout = max(1.5, min(4.0, float(_env_int("NEWS_API_BRIEF_DB_TIMEOUT_SEC", 4))))
        results = await asyncio.gather(
            asyncio.wait_for(
                build_latest_feed(cfg=cfg, symbol=symbol_norm, hours=hours, limit=min(max(10, feed_limit), 60), summarize=False),
                timeout=max(db_timeout + 2.0, 6.0),
            ),
            asyncio.wait_for(asyncio.shield(news_db.list_source_states()), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.get_llm_queue_stats()), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.count_news_raw(since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.count_events(symbol=symbol_norm, since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.latest_news_raw_timestamp(since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.latest_event_timestamp(symbol=symbol_norm, since=since)), timeout=db_timeout),
            return_exceptions=True,
        )
        failures: List[str] = []
        feed_preview_raw, source_states_raw, llm_queue_raw, raw_count_raw, events_count_raw, latest_raw_at_raw, latest_event_at_raw = results
        if isinstance(feed_preview_raw, Exception):
            failures.append(f"feed={type(feed_preview_raw).__name__}")
            feed_preview = {
                "count": 0,
                "feed_stats": _feed_sentiment_summary([]),
                "source_stats": {"by_provider": {}, "by_source": {}},
            }
        else:
            feed_preview = dict(feed_preview_raw or {})
        source_states = [] if isinstance(source_states_raw, Exception) else list(source_states_raw or [])
        llm_queue = {} if isinstance(llm_queue_raw, Exception) else dict(llm_queue_raw or {})
        raw_count = 0 if isinstance(raw_count_raw, Exception) else int(raw_count_raw or 0)
        events_count = 0 if isinstance(events_count_raw, Exception) else int(events_count_raw or 0)
        latest_raw_at = None if isinstance(latest_raw_at_raw, Exception) else latest_raw_at_raw
        latest_event_at = None if isinstance(latest_event_at_raw, Exception) else latest_event_at_raw
        if isinstance(source_states_raw, Exception):
            failures.append(f"source_states={type(source_states_raw).__name__}")
        if isinstance(llm_queue_raw, Exception):
            failures.append(f"llm_queue={type(llm_queue_raw).__name__}")
        if isinstance(raw_count_raw, Exception):
            failures.append(f"raw_count={type(raw_count_raw).__name__}")
        if isinstance(events_count_raw, Exception):
            failures.append(f"events_count={type(events_count_raw).__name__}")
        if isinstance(latest_raw_at_raw, Exception):
            failures.append(f"latest_raw_at={type(latest_raw_at_raw).__name__}")
        if isinstance(latest_event_at_raw, Exception):
            failures.append(f"latest_event_at={type(latest_event_at_raw).__name__}")
        payload = {
            "symbol": symbol_norm,
            "hours": hours,
            "since": since.isoformat(),
            "raw_count": int(raw_count),
            "events_count": int(events_count),
            "feed_count": int(feed_preview.get("count") or 0),
            "feed_stats": feed_preview.get("feed_stats") or _feed_sentiment_summary([]),
            "by_provider": ((feed_preview.get("source_stats") or {}).get("by_provider") or {}),
            "by_source": ((feed_preview.get("source_stats") or {}).get("by_source") or {}),
            "latest_raw_at": latest_raw_at,
            "latest_event_at": latest_event_at,
            "source_states": source_states,
            "llm_queue": llm_queue,
            "timestamp": _now_utc().isoformat(),
        }
        if failures:
            payload["degraded"] = True
            payload["failures"] = failures
        return _cache_set("brief", cache_key, payload)
    except Exception as exc:
        logger.warning(f"news brief failed symbol={symbol_norm or '-'}: {exc}")
        stale = _cache_get_stale("brief", cache_key)
        if stale:
            stale["fallback_reason"] = str(exc)
            return stale
        raise


@router.get("/events")
async def events(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)

    if since:
        try:
            since_ts = parse_any_datetime(since)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid since value: {exc}") from exc
    else:
        since_ts = _now_utc() - timedelta(hours=24)

    rows = await news_db.list_events(symbol=symbol_norm, since=since_ts, limit=limit)
    return {
        "count": len(rows),
        "symbol": symbol_norm,
        "since": since_ts.isoformat(),
        "items": rows,
    }


@router.get("/summary")
async def summary(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
    feed_limit: int = Query(default=120, ge=20, le=300),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)
    cache_key = _cache_key(symbol_norm, hours, feed_limit)
    ttl_sec = _env_int("NEWS_API_SUMMARY_CACHE_TTL_SEC", 12)
    cached = _cache_get("summary", cache_key, ttl_sec)
    if cached:
        return cached
    since = _now_utc() - timedelta(hours=hours)
    db_timeout = max(2.0, min(5.0, float(_env_int("NEWS_API_SUMMARY_DB_TIMEOUT_SEC", 6))))
    raw_limit = max(1000, min(3000, feed_limit * 24))
    event_limit = max(1000, min(3000, feed_limit * 24))
    try:
        results = await asyncio.gather(
            asyncio.wait_for(asyncio.shield(news_db.list_events(symbol=symbol_norm, since=since, limit=event_limit)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.list_news_raw(since=since, limit=raw_limit)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.list_source_states()), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.get_llm_queue_stats()), timeout=db_timeout),
            asyncio.wait_for(
                build_latest_feed(cfg=cfg, symbol=symbol_norm, hours=hours, limit=min(feed_limit, 60), summarize=False),
                timeout=max(db_timeout + 1.0, 5.5),
            ),
            asyncio.wait_for(asyncio.shield(news_db.count_events(symbol=symbol_norm, since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.latest_event_timestamp(symbol=symbol_norm, since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.count_news_raw(since=since)), timeout=db_timeout),
            asyncio.wait_for(asyncio.shield(news_db.latest_news_raw_timestamp(since=since)), timeout=db_timeout),
            return_exceptions=True,
        )
        failures: List[str] = []
        (
            events_raw,
            raw_rows_raw,
            source_states_raw,
            llm_queue_raw,
            feed_preview_raw,
            events_count_raw,
            latest_event_at_raw,
            raw_count_raw,
            latest_raw_at_raw,
        ) = results
        events = [] if isinstance(events_raw, Exception) else list(events_raw or [])
        raw_rows = [] if isinstance(raw_rows_raw, Exception) else list(raw_rows_raw or [])
        source_states = [] if isinstance(source_states_raw, Exception) else list(source_states_raw or [])
        llm_queue = {} if isinstance(llm_queue_raw, Exception) else dict(llm_queue_raw or {})
        events_count = len(events) if isinstance(events_count_raw, Exception) else int(events_count_raw or 0)
        latest_event_at = _first_iso_ts(events) if isinstance(latest_event_at_raw, Exception) else latest_event_at_raw
        raw_count = len(raw_rows) if (symbol_norm or isinstance(raw_count_raw, Exception)) else int(raw_count_raw or 0)
        latest_raw_at = _first_iso_ts(raw_rows) if (symbol_norm or isinstance(latest_raw_at_raw, Exception)) else latest_raw_at_raw
        if isinstance(feed_preview_raw, Exception):
            feed_preview = {
                "count": 0,
                "feed_stats": _feed_sentiment_summary([]),
                "source_stats": {"by_provider": _count_by_provider(raw_rows), "by_source": {}},
                "items": [],
            }
        else:
            feed_preview = dict(feed_preview_raw or {})
            if not isinstance(feed_preview.get("items"), list):
                feed_preview["items"] = []
        if isinstance(events_raw, Exception):
            failures.append(f"events={type(events_raw).__name__}")
        if isinstance(raw_rows_raw, Exception):
            failures.append(f"raw_rows={type(raw_rows_raw).__name__}")
        if isinstance(source_states_raw, Exception):
            failures.append(f"source_states={type(source_states_raw).__name__}")
        if isinstance(llm_queue_raw, Exception):
            failures.append(f"llm_queue={type(llm_queue_raw).__name__}")
        if isinstance(feed_preview_raw, Exception):
            failures.append(f"feed={type(feed_preview_raw).__name__}")
        if isinstance(events_count_raw, Exception):
            failures.append(f"events_count={type(events_count_raw).__name__}")
        if isinstance(latest_event_at_raw, Exception):
            failures.append(f"latest_event_at={type(latest_event_at_raw).__name__}")
        if isinstance(raw_count_raw, Exception) and not symbol_norm:
            failures.append(f"raw_count={type(raw_count_raw).__name__}")
        if isinstance(latest_raw_at_raw, Exception) and not symbol_norm:
            failures.append(f"latest_raw_at={type(latest_raw_at_raw).__name__}")

        sentiment = {"positive": 0, "neutral": 0, "negative": 0}
        by_type: Dict[str, int] = {}
        by_symbol: Dict[str, int] = {}
        by_provider: Dict[str, int] = _count_by_provider(raw_rows)

        for event in events:
            s = int(event.get("sentiment") or 0)
            if s > 0:
                sentiment["positive"] += 1
            elif s < 0:
                sentiment["negative"] += 1
            else:
                sentiment["neutral"] += 1

            event_type = str(event.get("event_type") or "other")
            by_type[event_type] = by_type.get(event_type, 0) + 1

            sym = str(event.get("symbol") or "")
            if sym:
                by_symbol[sym] = by_symbol.get(sym, 0) + 1

        sorted_by_type = dict(sorted(by_type.items(), key=lambda kv: kv[1], reverse=True))
        sorted_by_symbol = dict(sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:12])
        bucket_stats = _bucketize_events(events)
        if not _bucket_has_data(bucket_stats):
            fallback_rows = [
                {
                    "ts": item.get("published_at") or item.get("ts") or item.get("created_at"),
                    "sentiment": int(item.get("sentiment") or 0),
                }
                for item in (feed_preview.get("items") or [])
                if bool(item.get("has_event"))
            ]
            fallback_buckets = _bucketize_events(fallback_rows)
            if _bucket_has_data(fallback_buckets):
                bucket_stats = fallback_buckets
                failures.append("bucket_stats=feed_fallback")

        payload = {
            "symbol": symbol_norm,
            "hours": hours,
            "since": since.isoformat(),
            "raw_count": raw_count,
            "events_count": events_count,
            "latest_raw_at": latest_raw_at,
            "latest_event_at": latest_event_at,
            "sentiment": sentiment,
            "feed_count": int(feed_preview.get("count") or 0),
            "feed_stats": feed_preview.get("feed_stats") or _feed_sentiment_summary([]),
            "by_type": sorted_by_type,
            "by_symbol": sorted_by_symbol,
            "by_provider": by_provider,
            "source_summary": _build_source_summary(raw_rows, source_states),
            "source_states": source_states,
            "llm_queue": llm_queue,
            "bucket_stats": bucket_stats,
            "timestamp": _now_utc().isoformat(),
        }
        if failures:
            payload["degraded"] = True
            payload["failures"] = failures
        return _cache_set("summary", cache_key, payload)
    except Exception as exc:
        logger.warning(f"news summary failed symbol={symbol_norm or '-'}: {exc}")
        stale = _cache_get_stale("summary", cache_key)
        if stale:
            stale["fallback_reason"] = str(exc)
            return stale
        return {
            "symbol": symbol_norm,
            "hours": hours,
            "since": since.isoformat(),
            "raw_count": 0,
            "events_count": 0,
            "latest_raw_at": None,
            "latest_event_at": None,
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "feed_count": 0,
            "feed_stats": _feed_sentiment_summary([]),
            "by_type": {},
            "by_symbol": {},
            "by_provider": {},
            "source_summary": {},
            "source_states": [],
            "llm_queue": {},
            "bucket_stats": _bucketize_events([]),
            "timestamp": _now_utc().isoformat(),
            "degraded": True,
            "fallback_reason": str(exc),
        }
