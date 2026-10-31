"""Price-blind event-count sentiment research on the existing Telegram archive.

This module reads derived, deidentified metadata. It neither changes the frozen
hourly study nor treats retrospective event windows as forward paper trades.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3

SOURCE = "BinanceChinese"
PROTOCOL = {
    "version": "binance-chat-event-count-exploration-v1",
    "source": SOURCE, "asset": "BTCUSDT", "forward_messages_only": True,
    "clocks": {"messages": [100, 500, 1000], "btc_views": [5, 10, 20]},
    "primary": {"clock": "btc_views", "size": 10},
    "minimum_authors": 5, "minimum_absolute_score": 40, "minimum_confidence": .7,
    "maximum_receipt_lag_seconds": 1800, "maximum_label_lag_seconds": 1800,
    "order": "max(first receipt, label completion), then numeric message id",
    "batch_anchor": "first eligible message after original activation; disjoint complete batches",
    "vote": "one message per identical text per batch, then equal-weight author mean direction",
    "unknowns": "unlabelled, mismatched, nonfinite and future metadata excluded explicitly",
    "coverage": "audit against retained complete frozen-window receipts; absence is unverified, not proof of a gap",
    "elapsed_time": "reported as a diagnostic; no elapsed-time cutoff in this exploration",
    "rolling": "latest N messages are a current descriptive snapshot, never extra independent trials",
    "outcomes": "none; sample-density exploration only, no return-based parameter selection",
    "selection_status": "exploratory; parameter grid was chosen after inspecting text density",
    "live_orders": False, "promotion_allowed": False,
}


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def utc(value) -> datetime:
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return stamp.astimezone(timezone.utc)


@dataclass(frozen=True)
class Message:
    message_id: int
    published: datetime
    available: datetime
    author: str
    text_sha: str
    side: int | None


def read_archive(base: Path, *, activation: dict, status: dict) -> tuple[list[Message], dict, list[tuple[datetime, datetime]]]:
    """Read a stable SQLite snapshot; do not select message content or quotes."""
    start, as_of = utc(activation["at"]), utc(status["checked_at"])
    if as_of > datetime.now(timezone.utc):
        raise ValueError("Future archive checkpoint")
    sql = """SELECT m.message_id,m.published_at,m.first_received_at,m.text_sha,
        json_extract(m.row_json,'$.author_hash'),
        length(json_extract(m.row_json,'$.content'))>0,
        json_extract(m.row_json,'$.service'),json_extract(m.row_json,'$.sender_is_bot'),
        json_extract(m.row_json,'$.sender_unknown'),json_extract(m.row_json,'$.forwarded'),
        json_extract(m.row_json,'$.revision_observed'),l.text_sha,l.labelled_at,
        json_extract(l.label_json,'$.kind'),
        (SELECT json_group_array(json_object('symbol',json_extract(e.value,'$.symbol'),
            'side',json_extract(e.value,'$.side'),'confidence',json_extract(e.value,'$.confidence')))
         FROM json_each(l.label_json,'$.events') e)
        FROM messages m LEFT JOIN labels l ON m.source_id=l.source_id
        WHERE m.channel=? AND m.mode='forward' AND m.published_at>=? AND m.published_at<=?"""
    messages, counts, intervals = [], Counter(), []
    with closing(sqlite3.connect((base / "observations.sqlite").resolve().as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        for row in db.execute(sql, (SOURCE, start.isoformat(), as_of.isoformat())):
            counts["raw_messages"] += 1
            (mid, pub, receipt, text_sha, author, has_text, service, bot, unknown, forwarded,
             revision, label_sha, labelled_at, kind, events_json) = row
            if not has_text or service or bot or unknown or not author or forwarded or revision:
                counts["excluded_metadata"] += 1
                continue
            if label_sha != text_sha or not labelled_at:
                counts["excluded_missing_or_stale_label"] += 1
                continue
            if kind in {"promotion", "technical"}:
                counts["excluded_non_market"] += 1
                continue
            try:
                published, received, labelled = utc(pub), utc(receipt), utc(labelled_at)
                if (not 0 <= (received - published).total_seconds() <= 1800
                        or not 0 <= (labelled - received).total_seconds() <= 1800
                        or max(received, labelled) > as_of):
                    counts["excluded_late_or_future"] += 1
                    continue
                events = [e for e in json.loads(events_json or "[]") if kind == "directional"
                          and e.get("symbol") == "BTCUSDT" and type(e.get("side")) is int and e["side"] in (-1, 1)
                          and type(e.get("confidence")) in (int, float) and math.isfinite(e["confidence"])
                          and e["confidence"] >= .7]
            except (ValueError, TypeError, KeyError):
                counts["excluded_invalid_metadata"] += 1
                continue
            # Ambiguous or duplicate BTC event entries do not get an inferred sign.
            side = events[0]["side"] if len(events) == 1 else None
            messages.append(Message(int(mid), published, max(received, labelled), author, text_sha, side))
        for (blob,) in db.execute("SELECT payload_json FROM observations WHERE feature_id>=? AND feature_id<?",
                                  (SOURCE + ":", SOURCE + ";")):
            v = json.loads(blob)
            if v.get("reason") == "coverage_gap" or utc(v["available_at"]) > as_of:
                continue
            intervals.append((utc(v["window_start"]), utc(v["window_end"])))
    health = (status.get("sources") or {}).get(SOURCE) or {}
    if health.get("member") is True and health.get("complete_batch") is True and not health.get("difference_gap") and health.get("clean_since"):
        clean, last = utc(health["clean_since"]), utc(health["last_collection_at"])
        if clean <= last <= as_of:
            intervals.append((max(clean, start), last))
    merged: list[tuple[datetime, datetime]] = []
    for a, b in sorted(intervals):
        if b <= a:
            continue
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    messages.sort(key=lambda m: (m.available, m.message_id))
    counts.update(clean_messages=len(messages), btc_views=sum(m.side is not None for m in messages),
                  btc_authors=len({m.author for m in messages if m.side is not None}))
    metadata = {"as_of": as_of.isoformat(), "activation_at": start.isoformat(), "counts": dict(counts),
                "input_sha256": digest([(m.message_id, m.published.isoformat(), m.available.isoformat(),
                                          m.author, m.text_sha, m.side) for m in messages]),
                "coverage_segments": len(merged), "sql": sql}
    return messages, metadata, merged


def aggregate(batch: list[Message], *, clock: str, size: int, index: int,
              coverage: list[tuple[datetime, datetime]], as_of: datetime) -> dict:
    authors = defaultdict(list)
    seen = set()
    repeats = 0
    for m in batch:
        if m.side is None:
            continue
        if m.text_sha in seen:
            repeats += 1
            continue
        seen.add(m.text_sha)
        authors[m.author].append(m.side)
    means = [sum(values) / len(values) for values in authors.values()]
    balance = sum(means) / len(means) if means else None
    first, last = min(m.published for m in batch), max(m.published for m in batch)
    available = max(m.available for m in batch)
    certified = any(a <= first and last <= b for a, b in coverage)
    complete = len(batch) == size
    qualified = complete and len(authors) >= 5 and abs(balance) >= .4
    return {"clock": clock, "size": size, "index": index, "messages": len(batch),
            "btc_views": sum(m.side is not None for m in batch), "authors": len(authors),
            "bullish_authors": sum(v > 0 for v in means), "bearish_authors": sum(v < 0 for v in means),
            "mixed_authors": sum(v == 0 for v in means), "repeated_view_texts": repeats,
            "score": balance * 100 if balance is not None else None, "qualified": qualified,
            "complete": complete, "first_published_at": first.isoformat(), "last_published_at": last.isoformat(),
            "available_at": available.isoformat(), "span_hours": (available - batch[0].available).total_seconds() / 3600,
            "age_hours": (as_of - available).total_seconds() / 3600,
            "coverage_status": "certified" if certified else "not_certified",
            "feature_sha256": digest([(m.message_id, m.text_sha) for m in batch])}


def study(messages: list[Message], *, coverage: list[tuple[datetime, datetime]], as_of: datetime) -> dict:
    configs = []
    for clock, sizes in PROTOCOL["clocks"].items():
        stream = messages if clock == "messages" else [m for m in messages if m.side is not None]
        for size in sizes:
            bars = [aggregate(stream[i:i + size], clock=clock, size=size, index=i // size,
                              coverage=coverage, as_of=as_of) for i in range(0, len(stream) - size + 1, size)]
            candidates = [b for b in bars if b["qualified"]]
            latest = aggregate(stream[-size:], clock=clock, size=size, index=-1, coverage=coverage, as_of=as_of) if stream else None
            configs.append({"clock": clock, "size": size, "complete_batches": len(bars),
                            "remainder_messages": len(stream) % size,
                            "qualified_batches": len(candidates),
                            "certified_qualified_batches": sum(b["coverage_status"] == "certified" for b in candidates),
                            "batches": bars, "latest_rolling": latest})
    return {"configs": configs, "promotion_allowed": False, "returns_evaluated": False,
            "interpretation": "Retrospective event-count sentiment candidates, not validated trading signals."}
