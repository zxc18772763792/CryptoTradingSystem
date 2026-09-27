"""Binance delisting / monitoring notices -> active per-coin flags (delisting guard).

Why (docs/LLM_TRADING_RESEARCH_ROUND3_2026-09-26.md): after a delisting notice
91% of coins kept falling (spot median -34% by the day before removal, every
year 2024-26), while shorting them is a squeeze trap (ALPACA +2144%). The
usable edge is defensive: stop watching / entering coins that are on their way
out. This module keeps the announcement history fresh and derives which coins
are currently flagged; consumers are the weekly watchlist, the autonomous
agent's symbol scan and the radar page.

Titles are parsed with regexes: Binance's delisting notices are templated, so
no LLM is needed here. Token lists may contain non-ASCII tickers.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ANNOUNCEMENT_DIR = PROJECT_ROOT / "data" / "research" / "binance_announcements"
HISTORY_PATH = ANNOUNCEMENT_DIR / "history.json"
FLAGS_PATH = ANNOUNCEMENT_DIR / "active_flags.json"
CMS_LIST_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
CMS_DETAIL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
CATALOGS = ("161", "48")  # delisting, new listings (Binance files some notices under either)

SEVERITY = {"delist": 3, "futures_delist": 2, "monitoring_tag": 1}
MONITORING_TAG_DAYS = 180
POST_EFFECTIVE_GRACE_DAYS = 7

_DELIST = re.compile(r"^Binance Will Delist (.+?) on (\d{4}-\d{2}-\d{2})", re.I)
_FUTURES_DELIST = re.compile(r"Binance Futures Will Delist", re.I)
_MONITORING = re.compile(r"Monitoring Tag", re.I)
_MONITORING_REMOVE = re.compile(r"Remov\w* (the )?Monitoring Tag", re.I)
_DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _split_tokens(text: str) -> List[str]:
    tokens = []
    for raw in re.split(r",\s*|\s+and\s+|\s*&\s*", text):
        tok = raw.strip().strip(".").strip()
        tok = re.sub(r"\s*\(.*?\)$", "", tok)  # "U (Union)" -> "U"
        if tok and len(tok) <= 20 and " " not in tok:
            tokens.append(tok.upper())
    return tokens


def parse_notice(title: str) -> Optional[Dict[str, Any]]:
    """Return {kind, tokens, effective_date} for a delisting-type title, else None."""
    text = str(title or "").strip()
    m = _DELIST.match(text)
    if m:
        return {"kind": "delist", "tokens": _split_tokens(m.group(1)), "effective_date": m.group(2)}
    if _FUTURES_DELIST.search(text):
        tokens = [re.sub(r"^(1000000|100000|10000|1000)", "", t) for t in re.findall(r"([^\s,&()]+?)USDT\b", text)]
        date = _DATE.search(text)
        return {"kind": "futures_delist", "tokens": [t.upper() for t in tokens if t],
                "effective_date": date.group(1) if date else None} if tokens else None
    if _MONITORING.search(text) and not _MONITORING_REMOVE.search(text):
        # "Extend the Monitoring Tag to Include A, B": "Include" must win over "Tag to".
        marker = r"\bInclude\b" if re.search(r"\bInclude\b", text, re.I) else r"\bTag to\b|\bTag on\b|\bTag:\s*"
        after = re.split(marker, text, maxsplit=1, flags=re.I)
        if len(after) == 2:
            body = _DATE.sub("", after[1]).replace(" on ", " ")
            tokens = [t for t in _split_tokens(body) if not t.isdigit()]
            return {"kind": "monitoring_tag", "tokens": tokens, "effective_date": None} if tokens else None
    return None


def load_history(path: Path = HISTORY_PATH) -> Dict[str, List[Dict[str, Any]]]:
    if not path.exists():
        return {cat: [] for cat in CATALOGS}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {cat: list(data.get(cat) or []) for cat in CATALOGS}


def save_history(history: Dict[str, List[Dict[str, Any]]], path: Path = HISTORY_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


async def refresh_history(client, history: Dict[str, List[Dict[str, Any]]], max_pages: int = 5) -> int:
    """Prepend new articles (stop at the first page that contains a known code). Returns #added."""
    added = 0
    for cat in CATALOGS:
        known = {a.get("code") for a in history.get(cat, [])}
        fresh: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            resp = await client.get(CMS_LIST_URL, params={"type": 1, "catalogId": int(cat), "pageNo": page, "pageSize": 50})
            resp.raise_for_status()
            batch = [a for cc in ((resp.json().get("data") or {}).get("catalogs") or []) for a in (cc.get("articles") or [])]
            new = [a for a in batch if a.get("code") not in known]
            fresh += [{"code": a.get("code"), "title": a.get("title"), "release_ms": a.get("releaseDate")} for a in new]
            if not batch or len(new) < len(batch):
                break
        if fresh:
            history[cat] = fresh + history.get(cat, [])
            added += len(fresh)
    return added


async def fetch_article_text(client, code: str) -> str:
    """Plain text of one announcement (the body is a JSON node tree)."""
    resp = await client.get(CMS_DETAIL_URL, params={"articleCode": code})
    resp.raise_for_status()
    body = (resp.json().get("data") or {}).get("body") or ""
    if not body.startswith("{"):
        return re.sub(r"<[^>]+>", " ", body)
    parts: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("node") == "text" and node.get("text"):
                parts.append(str(node["text"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(json.loads(body))
    return " ".join(parts)


def active_flags(history: Dict[str, List[Dict[str, Any]]], now: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
    """Coins currently under a delisting-type notice, most severe notice per coin."""
    now = now or datetime.now(timezone.utc)
    flags: Dict[str, Dict[str, Any]] = {}
    seen = set()
    for article in (a for cat in CATALOGS for a in history.get(cat, [])):
        if article.get("code") in seen:
            continue
        seen.add(article.get("code"))
        notice = parse_notice(article.get("title", ""))
        if not notice or not article.get("release_ms"):
            continue
        announced = datetime.fromtimestamp(int(article["release_ms"]) / 1000, tz=timezone.utc)
        if notice["effective_date"]:
            effective = datetime.fromisoformat(notice["effective_date"]).replace(tzinfo=timezone.utc)
            expires = effective + timedelta(days=POST_EFFECTIVE_GRACE_DAYS)
        else:
            expires = announced + timedelta(days=MONITORING_TAG_DAYS if notice["kind"] == "monitoring_tag" else 30)
        if not (announced <= now <= expires):
            continue
        for token in notice["tokens"]:
            current = flags.get(token)
            if current and SEVERITY[current["kind"]] >= SEVERITY[notice["kind"]]:
                continue
            flags[token] = {
                "kind": notice["kind"],
                "announced_at": announced.isoformat(),
                "effective_date": notice["effective_date"],
                "expires_at": expires.isoformat(),
                "title": article.get("title"),
            }
    return flags


def write_flags(flags: Dict[str, Dict[str, Any]], path: Path = FLAGS_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "flags": flags},
                              ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


_FLAGS_CACHE: Dict[str, Any] = {"key": None, "payload": None}


def load_flags(path: Path = FLAGS_PATH, max_age_hours: float = 48.0) -> Dict[str, Dict[str, Any]]:
    """Flags written by the scheduler; empty if missing or stale (never raises).

    Cached on (path, mtime): the agent consults this for every scan candidate.
    """
    try:
        key = (str(path), path.stat().st_mtime)
        if _FLAGS_CACHE["key"] != key:
            _FLAGS_CACHE.update(key=key, payload=json.loads(path.read_text(encoding="utf-8")))
        payload = _FLAGS_CACHE["payload"]
        generated = datetime.fromisoformat(payload["generated_at"])
        if datetime.now(timezone.utc) - generated > timedelta(hours=max_age_hours):
            return {}
        return dict(payload.get("flags") or {})
    except Exception:
        return {}


def base_of(symbol: str) -> str:
    """'ABC/USDT', 'ABCUSDT', 'ABC/USDT:USDT' -> 'ABC'."""
    text = str(symbol or "").split(":")[0].upper()
    if "/" in text:
        return text.split("/")[0]
    return text[:-4] if text.endswith("USDT") else text


def flagged(symbols: Iterable[str], flags: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {s: flags[base_of(s)] for s in symbols if base_of(s) in flags}
