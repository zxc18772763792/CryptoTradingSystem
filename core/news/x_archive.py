"""Append-only archive of every X post the collector reads, for research.

One JSONL file per handle under data/research/x_kol/. A post is written once, at the
first sighting, with the time we first saw it (``first_seen_at``) and where it came from
(``provenance``: ``live`` polls or ``backfill`` of old timeline pages). ``created_at`` is
the time X says the post was made; only ``live`` records have a trustworthy
``first_seen_at`` close to it, so research must not treat backfilled posts as things a
person could have seen in real time without saying so.

Archiving never creates a news item, calls an AI API or places an order, and a failure
here must not stop news ingestion. Two processes may append to the same file (the news
worker and a backfill run); duplicates are harmless because readers de-duplicate.
"""
from __future__ import annotations

import html
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from core.news.collectors.x_accounts import publication_time

ROOT = Path(__file__).resolve().parents[2]
_HANDLE_RE = re.compile(r"[a-z0-9_]{1,15}\Z")
_lock = threading.Lock()
_seen: Dict[str, set] = {}


def archive_dir() -> Path:
    return ROOT / "data" / "research" / "x_kol"


def _path(handle: str, directory: Optional[Path]) -> Path:
    name = str(handle).strip().lower()
    if not _HANDLE_RE.fullmatch(name):
        raise ValueError("archive handle must be an X username")
    return (directory or archive_dir()) / f"{name}.jsonl"


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_record(handle: str, tweet: Dict[str, Any], observed_at: datetime, provenance: str) -> Optional[Dict[str, Any]]:
    """Compact, lossless-enough copy of one Scweet tweet dict; None if it has no usable id."""
    tweet_id = str(tweet.get("tweet_id") or "").strip()
    if not tweet_id.isdigit():
        return None
    user = tweet.get("user") if isinstance(tweet.get("user"), dict) else {}
    media = tweet.get("media") if isinstance(tweet.get("media"), dict) else {}
    created = publication_time(tweet.get("timestamp"))
    return {
        "tweet_id": tweet_id,
        "handle": str(handle).lower(),
        "author": str(user.get("screen_name") or "").lower(),
        "created_at": created.isoformat() if created else None,
        "created_raw": str(tweet.get("timestamp") or ""),
        "text": html.unescape(str(tweet.get("text") or "")),
        "lang": tweet.get("lang") or None,
        "is_retweet": bool(tweet.get("is_retweet")),
        "is_quote": bool(tweet.get("is_quote")),
        "in_reply_to_user": (str(tweet.get("in_reply_to_user")).lower() if tweet.get("in_reply_to_user") else None),
        "quoted_text": html.unescape(str(tweet.get("embedded_text") or "")) if tweet.get("is_quote") else "",
        "likes": _int(tweet.get("likes")),
        "retweets": _int(tweet.get("retweets")),
        "replies": _int(tweet.get("comments")),
        "quotes": _int(tweet.get("quotes")),
        "views": _int(tweet.get("views")),
        "hashtags": [str(h) for h in (tweet.get("hashtags") or [])][:30],
        "mentions": [str(m) for m in (tweet.get("mentions") or [])][:30],
        "urls": [str(u) for u in (tweet.get("urls") or [])][:20],
        "image_count": len(media.get("image_links") or []),
        "has_video": bool(media.get("video_links")),
        "first_seen_at": observed_at.astimezone(timezone.utc).isoformat(),
        "provenance": provenance,
    }


def _known_ids(path: Path) -> set:
    known = _seen.get(str(path))
    if known is None:
        known = set()
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        known.add(str(json.loads(line)["tweet_id"]))
                    except (ValueError, KeyError, TypeError):
                        continue
        _seen[str(path)] = known
    return known


def observe(handle: str, tweets: Iterable[Dict[str, Any]], *, provenance: str = "live",
            observed_at: Optional[datetime] = None, directory: Optional[Path] = None) -> Dict[str, int]:
    """Append the posts not archived yet. Returns how many were new and how many were offered."""
    if provenance not in {"live", "backfill"}:
        raise ValueError("provenance must be live or backfill")
    observed = observed_at or datetime.now(timezone.utc)
    path = _path(handle, directory)
    offered = 0
    fresh: List[str] = []
    with _lock:
        known = _known_ids(path)
        for tweet in tweets or []:
            if not isinstance(tweet, dict):
                continue
            offered += 1
            record = to_record(handle, tweet, observed, provenance)
            if record is None or record["tweet_id"] in known:
                continue
            known.add(record["tweet_id"])
            fresh.append(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        if fresh:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8", newline="\n") as handle_file:
                handle_file.write("\n".join(fresh) + "\n")
    return {"new": len(fresh), "offered": offered}


def load_archive(handles: Optional[Iterable[str]] = None, directory: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Every archived post once: the earliest sighting of each (handle, tweet_id) wins."""
    base = directory or archive_dir()
    wanted = {str(h).lower() for h in handles} if handles is not None else None
    best: Dict[tuple, Dict[str, Any]] = {}
    for path in sorted(base.glob("*.jsonl")):
        if wanted is not None and path.stem not in wanted:
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                key = (record.get("handle"), record.get("tweet_id"))
                kept = best.get(key)
                if kept is None or str(record.get("first_seen_at")) < str(kept.get("first_seen_at")):
                    best[key] = record
    return list(best.values())


def oldest_created(handle: str, directory: Optional[Path] = None) -> Optional[str]:
    """Earliest created_at archived for a handle (ISO string), or None."""
    path = _path(handle, directory)
    if not path.exists():
        return None
    oldest = None
    with path.open("r", encoding="utf-8") as handle_file:
        for line in handle_file:
            try:
                created = json.loads(line).get("created_at")
            except ValueError:
                continue
            if created and (oldest is None or created < oldest):
                oldest = created
    return oldest
