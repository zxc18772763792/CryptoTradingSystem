"""Independent, versioned event-time sentiment observation and paper evidence.

Consumes the existing reviewed BTC labels after a successful collection cycle.
No model calls, Telegram connections, chat text persistence or order endpoints.
The original five frozen books remain owned by the original research CLI.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
from uuid import uuid4

from core.research.telegram_event_sentiment import SOURCE, digest, read_archive, utc
from core.research.telegram_forward import outcome
from core.utils.file_lock import advisory_file_lock

PROTOCOL = {
    "version": "binance-chat-event-observer-v1", "asset": "BTCUSDT", "source": SOURCE,
    "windows": [10, 20], "minimum_authors": 5, "absolute_score": 40,
    "labels": "existing explicit-asset directional labels, confidence>=0.7, separate semantic review",
    "quality_guards": "quarantine explicit long-horizon or mixed-horizon theses and numeric price wishes without an explicit direction; never infer a replacement side",
    "author_vote": "latest eligible stance per author in each N-view window",
    "echo_rule": "identical normalized-text SHA contributes once across authors, earliest retained author wins",
    "score": "100*(bullish_authors-bearish_authors)/unique_authors",
    "smoothed_score": "Beta(1,1) descriptive shrinkage: 100*(bullish-bearish)/(authors+2)",
    "uncertainty": "Wilson 95% interval of bullish author share; accounts are not guaranteed independent people",
    "freshness": {"max_latest_view_age_hours": 6, "max_window_span_hours": 24},
    "coverage": "forward views must be published after activation and current clean_since; incomplete cycles or pending labels cannot qualify",
    "seed": "pre-activation views are historical background only, never forward samples",
    "sampling": "rolling windows describe current stance; disjoint complete N-view blocks per clean segment freeze evidence",
    "paper": {"variants": ["follow", "contrarian"], "hold_hours": 6, "cost_bps": 20, "stress_bps": 40,
              "entry": "next 5-minute boundary strictly after freeze+60 seconds", "overlap": "one per window/variant",
              "funding": "actual rates and settlement marks; missing remains pending", "missing_limit_days": 7},
    "validation": "at least 100 mature events and 8 calendar weeks before descriptive screening; no automatic promotion",
    "classification_accuracy_on_this_group": "not independently measured; model confidence is not a return probability",
    "promotion_allowed": False,
}
LABELS = {"warming_up": "条数积累中", "insufficient_authors": "独立作者不足", "low_consensus": "多空分歧",
          "candidate": "情绪候选 · 仅观察", "stale_views": "观点已过时", "long_window": "观点跨度过长",
          "coverage_gap": "覆盖不完整", "historical": "存量背景 · 非前向", "unavailable": "观察记录不可读"}

LONG_HORIZON = re.compile(r"长期|长线|定投|明年|后年|[一二三四五六七八九十百\d]+年后|未来哪天|终有一天|让时间证明|long[- ]term|next year", re.I)
SHORT_HORIZON = re.compile(r"短线|短期|日内|今天|今晚|(?:未来|接下来).{0,4}小时|intraday|short[- ]term|today|tonight", re.I)
DIRECTION_WORDS = re.compile(r"看[多空涨跌]|做[多空]|涨|跌|拉升|崩|突破|见[顶底]|买|卖|抄底|(?:快|直接)?[多空](?:大饼|比特币|BTC)|bullish|bearish|buy|sell|rally|fall", re.I)
PRICE_NUMBER = re.compile(r"\d[\d,.]*\s*(?:[万千字]|[kK]\b)|\d{4,}|\d[\d,]*\s*字开头")
PRICE_WISH = re.compile(r"期待|希望|目标|等你|等着|看到|见到|(?:去|到)\s*\d|target|hope", re.I)


def quality_guard(text: str) -> str | None:
    """Narrow conservative exclusions, not a new stance classifier."""
    if LONG_HORIZON.search(text):
        return "mixed_horizon" if SHORT_HORIZON.search(text) else "long_horizon"
    if PRICE_NUMBER.search(text) and PRICE_WISH.search(text) and not DIRECTION_WORDS.search(text):
        return "price_target_without_direction"
    return None


def _quality_filter(source: Path, messages: list) -> tuple[list, dict]:
    targets = [m for m in messages if m.side is not None]
    found = {}
    with closing(sqlite3.connect((source / "observations.sqlite").as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
        db.execute("PRAGMA query_only=ON")
        for offset in range(0, len(targets), 400):
            ids = [m.message_id for m in targets[offset:offset + 400]]
            marks = ",".join("?" for _ in ids)
            for mid, text_sha, text in db.execute(
                    f"SELECT message_id,text_sha,json_extract(row_json,'$.content') FROM messages WHERE channel=? AND message_id IN ({marks})", (SOURCE, *ids)):
                found[mid] = (text_sha, text or "")
    reasons, filtered = Counter(), []
    for m in messages:
        reason = None
        if m.side is not None:
            row = found.get(m.message_id)
            reason = "source_changed" if not row or row[0] != m.text_sha else quality_guard(row[1])
        if reason:
            reasons[reason] += 1
        filtered.append(replace(m, side=None) if reason else m)
    return filtered, {"labelled_btc_views": len(targets), "retained_btc_views": len(targets) - sum(reasons.values()),
                      "quarantined": dict(reasons), "accuracy_evaluated": False}


def observer_dir(source: Path) -> Path:
    override = str(os.environ.get("TELEGRAM_SENTIMENT_V2_DIR") or "").strip()
    return Path(override) if override else source.parent / "binance_chat_sentiment_v2"


def implementation_sha() -> str:
    root = Path(__file__).resolve().parents[2]
    return digest({p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in (
        "core/research/telegram_sentiment_observer.py", "core/research/telegram_event_sentiment.py",
        "core/research/telegram_forward.py")})


def _write(path: Path, value) -> None:
    temp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _read(path: Path) -> dict:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def wilson(positive: int, total: int) -> list[float] | None:
    if not total:
        return None
    p, z = positive / total, 1.959963984540054
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return [max(0., center - half), min(1., center + half)]


def measure(views: list[dict], size: int, at: datetime, *, covered: bool, historical: bool = False) -> dict:
    """Price-blind score; one latest stance per author and one vote per text."""
    views = sorted(views, key=lambda v: (v["available_at"], v["message_id"]))[-size:]
    latest = {}
    for v in views:
        latest[v["author"]] = v
    retained, seen = [], set()
    for v in sorted(latest.values(), key=lambda v: (v["available_at"], v["message_id"])):
        if v["text_sha"] not in seen:
            retained.append(v)
            seen.add(v["text_sha"])
    n = len(retained)
    up = sum(v["side"] == 1 for v in retained)
    down = n - up
    score = 100 * (up - down) / n if n else None
    interval = wilson(up, n)
    age = max(0., (at - utc(views[-1]["available_at"])).total_seconds() / 3600) if views else None
    span = (utc(views[-1]["available_at"]) - utc(views[0]["available_at"])).total_seconds() / 3600 if views else None
    if historical:
        state = "historical"
    elif not covered:
        state = "coverage_gap"
    elif len(views) < size:
        state = "warming_up"
    elif age > PROTOCOL["freshness"]["max_latest_view_age_hours"]:
        state = "stale_views"
    elif span > PROTOCOL["freshness"]["max_window_span_hours"]:
        state = "long_window"
    elif n < PROTOCOL["minimum_authors"]:
        state = "insufficient_authors"
    elif abs(score) < PROTOCOL["absolute_score"]:
        state = "low_consensus"
    else:
        state = "candidate"
    return {"size": size, "views": len(views), "authors": n, "bullish": up, "bearish": down,
            "score": score, "smoothed_score": 100 * (up - down) / (n + 2) if n else None,
            "bullish_share_ci95": interval, "interval_excludes_balance": bool(interval and (interval[0] > .5 or interval[1] < .5)),
            "superseded_views": len(views) - len(latest), "echo_authors_removed": len(latest) - n,
            "top_author_message_share": max(Counter(v["author"] for v in views).values()) / len(views) if views else None,
            "age_hours": age, "span_hours": span, "last_view_at": views[-1]["available_at"] if views else None,
            "status": state, "label": LABELS[state], "qualified": state == "candidate",
            "feature_sha": digest([(v["message_id"], v["text_sha"], v["side"]) for v in views])}


def _append(db, kind: str, data: dict) -> None:
    row = db.execute("SELECT sha FROM journal ORDER BY sequence DESC LIMIT 1").fetchone()
    previous = row[0] if row else "0" * 64
    payload = {"kind": kind, "data": data}
    db.execute("INSERT INTO journal(previous_sha,sha,payload_json) VALUES (?,?,?)",
               (previous, digest({"previous": previous, "payload": payload}), json.dumps(payload)))


def _verify(db) -> dict:
    previous, count = "0" * 64, 0
    for prev, sha, blob in db.execute("SELECT previous_sha,sha,payload_json FROM journal ORDER BY sequence"):
        if prev != previous or sha != digest({"previous": previous, "payload": json.loads(blob)}):
            raise ValueError("Sentiment observation journal mismatch")
        previous, count = sha, count + 1
    return {"records": count, "tail_sha": previous}


def _database(path: Path):
    db = sqlite3.connect(path, timeout=5)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""CREATE TABLE IF NOT EXISTS state(name TEXT PRIMARY KEY,payload_json TEXT);
        CREATE TABLE IF NOT EXISTS views(message_id INTEGER PRIMARY KEY,phase TEXT,segment TEXT,available_at TEXT,payload_json TEXT);
        CREATE TABLE IF NOT EXISTS features(feature_id TEXT PRIMARY KEY,recorded_at TEXT,payload_json TEXT);
        CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY,book TEXT,entry_at TEXT,exit_at TEXT,payload_json TEXT);
        CREATE TABLE IF NOT EXISTS outcomes(event_id TEXT PRIMARY KEY,payload_json TEXT);
        CREATE TABLE IF NOT EXISTS journal(sequence INTEGER PRIMARY KEY,previous_sha TEXT,sha TEXT,payload_json TEXT);""")
    return db


def _freeze(db, feature: dict, size: int, at: datetime) -> int:
    count = 0
    entry = datetime.fromtimestamp((int((at + timedelta(seconds=60)).timestamp()) // 300 + 1) * 300, timezone.utc)
    exit_at = entry + timedelta(hours=6)
    for variant, multiplier in (("follow", 1), ("contrarian", -1)):
        book = f"event{size}_{variant}"
        if db.execute("SELECT 1 FROM decisions WHERE book=? AND exit_at>?", (book, entry.isoformat())).fetchone():
            continue
        event_id = digest({"feature": feature["feature_id"], "book": book})
        event = {"event_id": event_id, "book": book, "feature_id": feature["feature_id"], "symbol": "BTCUSDT",
                 "side": (1 if feature["score"] > 0 else -1) * multiplier,
                 "frozen_at": at.isoformat(), "entry_at": entry.isoformat(), "exit_at": exit_at.isoformat(),
                 "protocol_sha": digest(PROTOCOL), "paper_only": True}
        db.execute("INSERT OR IGNORE INTO decisions VALUES (?,?,?,?,?)",
                   (event_id, book, entry.isoformat(), exit_at.isoformat(), json.dumps(event)))
        _append(db, "decision", event)
        count += 1
    return count


def _settle(db, market: dict, at: datetime) -> None:
    if market.get("symbol") != "BTCUSDT" or market.get("interval") != "5m":
        return
    if market.get("received_at") and utc(market["received_at"]) > at:
        return
    bars = [r for r in market.get("bars") or [] if utc(r["at"]) <= at]
    funding = [r for r in market.get("funding") or [] if utc(r["at"]) <= at]
    for (blob,) in db.execute("SELECT d.payload_json FROM decisions d LEFT JOIN outcomes o USING(event_id) WHERE o.event_id IS NULL").fetchall():
        event = json.loads(blob)
        if utc(event["exit_at"]) > at:
            continue
        if at - utc(event["exit_at"]) > timedelta(days=7):
            value = {"status": "data_unavailable_after_7days", "net": None}
        else:
            value = outcome(event, bars, funding)
            if value["status"] != "settled":
                continue
        db.execute("INSERT INTO outcomes VALUES (?,?)", (event["event_id"], json.dumps(value)))
        _append(db, "outcome", dict(event_id=event["event_id"], **value))


def _books(db) -> list[dict]:
    result = []
    for size in (10, 20):
        for variant in ("follow", "contrarian"):
            book = f"event{size}_{variant}"
            rows = db.execute("SELECT d.payload_json,o.payload_json FROM decisions d LEFT JOIN outcomes o USING(event_id) WHERE d.book=?", (book,)).fetchall()
            settled = [json.loads(o) for _, o in rows if o and json.loads(o)["status"] == "settled"]
            dates = {utc(json.loads(d)["exit_at"]).strftime("%G-%V") for d, o in rows if o and json.loads(o)["status"] == "settled"}
            mean = lambda field: sum(o[field] for o in settled) / len(settled) * 100 if settled else None
            result.append({"book": book, "size": size, "variant": variant, "decisions": len(rows), "settled": len(settled),
                           "pending": sum(o is None for _, o in rows), "unavailable": sum(o is not None and json.loads(o)["status"] != "settled" for _, o in rows),
                           "mean_net_pct": mean("net"), "mean_stress_pct": mean("net_stress"),
                           "mean_excess_pct": sum(o["net"] - o["same_entry_long_net"] for o in settled) / len(settled) * 100 if settled else None,
                           "win_rate": sum(o["net"] > 0 for o in settled) / len(settled) if settled else None,
                           "calendar_weeks": len(dates), "promotion_allowed": False})
    return result


def sync(source: Path, output: Path | None = None, *, now: datetime | None = None) -> dict:
    """Single-writer hook after the original cycle; source access is read-only."""
    source = Path(source).resolve()
    output = (output or observer_dir(source)).resolve()
    if output == source or source in output.parents:
        raise ValueError("Observer must use an independent directory")
    at = now or datetime.now(timezone.utc)
    source_activation, status = _read(source / "activation.json"), _read(source / "forward_status.json")
    as_of = utc(status["checked_at"])
    if as_of > at or (at - as_of).total_seconds() > 900:
        raise ValueError("A recent completed source cycle is required")
    messages, metadata, _ = read_archive(source, activation=source_activation, status=status)
    messages, quality = _quality_filter(source, messages)
    output.mkdir(parents=True, exist_ok=True)
    with advisory_file_lock(output / "observer"):
        with closing(_database(output / "observer.sqlite")) as db:
            row = db.execute("SELECT payload_json FROM state WHERE name='activation'").fetchone()
            activation = json.loads(row[0]) if row else None
            if activation:
                if (activation["protocol_sha"] != digest(PROTOCOL) or activation["implementation_sha"] != implementation_sha()
                        or activation["source_implementation_sha"] != source_activation["implementation_sha"]
                        or activation["source_activation_at"] != source_activation["at"]
                        or activation["source_path_sha"] != digest(str(source))):
                    raise ValueError("Observer implementation changed; explicit new version required")
                if (output / "protocol.json").exists() and _read(output / "protocol.json").get("protocol") != PROTOCOL:
                    raise ValueError("Observer protocol file changed")
                checkpoint = db.execute("SELECT payload_json FROM state WHERE name='checkpoint'").fetchone()
                if checkpoint and utc(json.loads(checkpoint[0])["as_of"]) > as_of:
                    raise ValueError("Observer source checkpoint moved backwards")
                _verify(db)
            else:
                activation = {"at": at.isoformat(), "protocol_sha": digest(PROTOCOL), "implementation_sha": implementation_sha(),
                              "source_implementation_sha": source_activation["implementation_sha"],
                              "source_activation_at": source_activation["at"], "source_path_sha": digest(str(source)), "paper_only": True}
                db.execute("INSERT INTO state VALUES ('activation',?)", (json.dumps(activation),))
                _append(db, "activation", activation)
            started = utc(activation["at"])
            health = (status.get("sources") or {}).get(SOURCE) or {}
            clean = utc(health["clean_since"]) if health.get("clean_since") else None
            covered = (health.get("member") is True and health.get("complete_batch") is True
                       and not health.get("difference_gap") and clean is not None and clean <= as_of
                       and not (status.get("pending_labels") or {}).get(SOURCE))
            segment = clean.isoformat() if clean else "unverified"
            for m in messages:
                if m.side is None:
                    continue
                phase = "background" if m.published < started else "forward"
                if phase == "forward" and (not covered or m.published < clean):
                    continue
                value = {"message_id": m.message_id, "published_at": m.published.isoformat(),
                         "available_at": m.available.isoformat(), "author": m.author, "text_sha": m.text_sha, "side": m.side}
                db.execute("INSERT OR IGNORE INTO views VALUES (?,?,?,?,?)", (m.message_id, phase, segment, m.available.isoformat(), json.dumps(value)))
            live = [json.loads(r[0]) for r in db.execute("SELECT payload_json FROM views WHERE phase='forward' AND segment=? ORDER BY available_at,message_id", (segment,))]
            background = [json.loads(r[0]) for r in db.execute("SELECT payload_json FROM views WHERE phase='background' ORDER BY available_at,message_id")]
            for size in (10, 20):
                for i in range(0, len(live) - size + 1, size):
                    feature_id = digest({"segment": segment, "size": size, "batch": i // size})
                    if db.execute("SELECT 1 FROM features WHERE feature_id=?", (feature_id,)).fetchone():
                        continue
                    feature = dict(measure(live[i:i + size], size, at, covered=covered), feature_id=feature_id,
                                   available_at=live[i + size - 1]["available_at"], recorded_at=at.isoformat(), batch=i // size)
                    db.execute("INSERT INTO features VALUES (?,?,?)", (feature_id, at.isoformat(), json.dumps(feature)))
                    _append(db, "feature", feature)
                    if feature["qualified"]:
                        _freeze(db, feature, size, at)
            _settle(db, _read(source / "forward_market.json"), at)
            db.execute("INSERT OR REPLACE INTO state VALUES ('checkpoint',?)", (json.dumps({"as_of": status["checked_at"]}),))
            db.commit()
            rolling = [measure(live[-size:], size, at, covered=covered) for size in (10, 20)]
            prior = [measure(background[-size:], size, at, covered=False, historical=True) for size in (10, 20)]
            recent = messages[-1000:]
            delta = rolling[0]["score"] - rolling[1]["score"] if all(r["status"] in {"candidate", "low_consensus"} for r in rolling) else None
            result = {"version": PROTOCOL["version"], "activation_at": activation["at"], "updated_at": at.isoformat(),
                      "source_as_of": status["checked_at"], "source_complete": bool(covered), "clean_since": health.get("clean_since"),
                      "classifier": {"model": source_activation.get("classifier_model"), "kind": "target_stance_with_semantic_review", "accuracy_evaluated": False},
                      "rolling": rolling, "background": prior, "change_10_vs_20": delta,
                      "new_views_in_segment": len(live), "archive_counts": metadata["counts"],
                      "quality_checks": quality,
                      "attention": {"messages": len(recent), "btc_views": sum(m.side is not None for m in recent)},
                      "books": _books(db), "journal": _verify(db),
                      "history": [json.loads(r[0]) for r in db.execute("SELECT payload_json FROM features ORDER BY recorded_at DESC LIMIT 8")],
                      "paper_only": True, "promotion_allowed": False}
        if not (output / "activation.json").exists():
            _write(output / "activation.json", activation)
        if not (output / "protocol.json").exists():
            _write(output / "protocol.json", {"protocol": PROTOCOL, "frozen_at": activation["at"]})
        _write(output / "summary.json", result)
        return result


def record_error(source: Path, error: Exception, output: Path | None = None) -> None:
    output = output or observer_dir(source)
    output.mkdir(parents=True, exist_ok=True)
    _write(output / "last_error.json", {"at": datetime.now(timezone.utc).isoformat(), "error_type": type(error).__name__})


def load_summary(source: Path, *, now: datetime | None = None, source_health: dict | None = None) -> dict:
    at = now or datetime.now(timezone.utc)
    base = observer_dir(source)
    result, error = _read(base / "summary.json"), _read(base / "last_error.json")
    if not result:
        return {"version": PROTOCOL["version"], "status": "unavailable", "label": "条数观察待初始化", "rolling": [], "books": [], "paper_only": True}
    try:
        updated = utc(result["updated_at"])
        failed = bool(error) and utc(error["at"]) > updated
        stale = updated > at or at - updated > timedelta(minutes=15)
    except (KeyError, ValueError, TypeError):
        failed, stale = True, True
    result["status"] = "error" if failed else "stale" if stale else "active"
    result["label"] = "条数观察异常" if failed else "条数观察更新延迟" if stale else "条数观察运行中"
    result["last_error"] = error if failed else None
    good = not source_health or source_health.get("status") == "healthy"
    for window in result.get("rolling") or []:
        if not good or failed or stale:
            window.update(qualified=False, status="unavailable", label="当前观察不可用")
        elif window.get("last_view_at"):
            window["age_hours"] = max(0., (at - utc(window["last_view_at"])).total_seconds() / 3600)
            if window["age_hours"] > PROTOCOL["freshness"]["max_latest_view_age_hours"]:
                window.update(qualified=False, status="stale_views", label=LABELS["stale_views"])
    if any(w.get("status") not in {"candidate", "low_consensus"} for w in result.get("rolling") or []):
        result["change_10_vs_20"] = None
    return result
