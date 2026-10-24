"""Frozen, paper-only Telegram research rules and receipt-time ledger."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3

from core.research.telegram_text_signals import ASSETS, redact_sensitive_text

SOURCES = ("cryptoquant_official", "BinanceChinese", "Binance_api_Chinese")
TECH = re.compile(r"(?<![A-Za-z])API(?![A-Za-z])|接口|签名|报错|订单参数|请求频率|限频|限流|超时|连接失败|服务器错误|\b(?:429|502|503|504)\b|exception|traceback|websocket", re.I)
STRESS = re.compile(r"报错|限频|限流|超时|连接失败|服务器错误|无法下单|不能下单|429|502|503|504|timeout|unavailable|connection (?:error|failed)", re.I)
PROMOTION = re.compile(r"返佣|邀请码|开户链接|稳赚|带单|加我|私聊|免费空投|领取空投|airdrop|referral|guaranteed profit", re.I)
PROTOCOL = {
    "version": "telegram-three-source-paper-v1",
    "sources": list(SOURCES), "asset": "BTCUSDT",
    "history_days": 45, "history_cap_per_source": 12000,
    "research_label_cap_per_source": 1500,
    "min_confidence": .7, "bucket": "closed UTC hour",
    "min_directional_authors": 5, "absolute_imbalance": .4,
    "cq_holding_hours": 24, "chat_holding_hours": 6,
    "chat_variants": ["follow", "contrarian"],
    "api_group": "technical stress only; not a directional trading source",
    "api_stress_min_authors": 3, "api_stress_author_share": .5,
    "history_split": "first 70% of source time span development, remainder validation; no fitting",
    "historical_entry": "next full UTC hour after feature availability plus 60 seconds",
    "forward_entry": "next 5-minute boundary after decision freeze plus 60 seconds",
    "forward_availability": "max(receipt, label completion, closed hour); never publication alone",
    "max_receipt_lag_seconds": 1800, "max_forward_label_lag_seconds": 1800,
    "roundtrip_cost_bps": 20, "stress_cost_bps": 40,
    "funding": "actual USDT perpetual settlements and settlement mark price; missing is not zero",
    "position_rule": "one overlapping paper position per policy/source/asset; earliest wins",
    "raw_text_retention_days": 30,
    "promotion_gate": "at least 100 mature forward events and 8 calendar weeks; net/excess CI lower > 0; review required",
    "execution": "paper journal only, no exchange order endpoints or strategy registry",
}
EXPLORATORY_PROTOCOL = {
    "version":"telegram-chat-6h-forward-v1", "window_hours":6, "min_authors":5,
    "absolute_imbalance":.4, "holding_hours":6, "variants":["follow","contrarian"],
    "selection_basis":"text coverage calibration only; original hourly study generated no signals",
    "historical_performance_claim":False, "forward_only":True,
}


def utc(value=None):
    if value is None:
        return datetime.now(timezone.utc)
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("UTC-aware timestamp required")
    return result.astimezone(timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def next_boundary(at, minutes):
    stamp = utc(at).timestamp()
    step = minutes * 60
    return datetime.fromtimestamp((int(stamp) // step + 1) * step, timezone.utc)


def normalize(row):
    if row["channel"] not in SOURCES:
        raise ValueError("Source outside the three-source allowlist")
    text = redact_sensitive_text(str(row.get("content") or ""))
    # Bot tokens and unlabelled long secrets are also removed from text.
    text = re.sub(r"\b\d{6,12}:[A-Za-z0-9_-]{25,}\b", "[REDACTED]", text)
    text = re.sub(r"\b[A-Za-z0-9_-]{48,}\b", "[REDACTED]", text)
    row = dict(row, content=text[:16000], text_sha256=hashlib.sha256(text[:16000].encode()).hexdigest())
    return row


def rule_kind(row):
    text = row.get("content", "")
    if not text or row.get("service"):
        return "other"
    if row.get("sender_is_bot"):
        return "other"
    if PROMOTION.search(text):
        return "promotion"
    if TECH.search(text):
        return "technical"
    if not any(re.search(pattern, text, re.I) for pattern in ASSETS.values()):
        return "other"
    return "needs_model"


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "observations.sqlite", timeout=5)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            source_id TEXT PRIMARY KEY, channel TEXT NOT NULL, message_id INTEGER NOT NULL,
            published_at TEXT NOT NULL, first_received_at TEXT NOT NULL, mode TEXT NOT NULL,
            row_json TEXT NOT NULL, text_sha TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS revisions (
            source_id TEXT, revision_sha TEXT, received_at TEXT, row_json TEXT,
            PRIMARY KEY(source_id,revision_sha));
        CREATE TABLE IF NOT EXISTS labels (
            source_id TEXT PRIMARY KEY, text_sha TEXT, labelled_at TEXT, label_json TEXT);
        CREATE TABLE IF NOT EXISTS state (name TEXT PRIMARY KEY, value_json TEXT);
        CREATE TABLE IF NOT EXISTS decisions (
            event_id TEXT PRIMARY KEY, policy TEXT, source TEXT, symbol TEXT,
            frozen_at TEXT, entry_at TEXT, exit_at TEXT, payload_json TEXT);
        CREATE TABLE IF NOT EXISTS outcomes (event_id TEXT PRIMARY KEY, settled_at TEXT, payload_json TEXT);
        CREATE TABLE IF NOT EXISTS journal (sequence INTEGER PRIMARY KEY, previous_sha TEXT,
            sha TEXT NOT NULL, payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS observations (feature_id TEXT PRIMARY KEY, recorded_at TEXT, payload_json TEXT);
        CREATE INDEX IF NOT EXISTS messages_published ON messages(published_at);
        CREATE INDEX IF NOT EXISTS decisions_overlap ON decisions(policy,source,symbol,exit_at);
        """)

    def state(self, name, default=None):
        row = self.db.execute("SELECT value_json FROM state WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_state(self, name, value):
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (name, json.dumps(value)))
        self.db.commit()

    def ingest(self, value, *, commit=True):
        row = normalize(value)
        old = self.db.execute("SELECT row_json,text_sha FROM messages WHERE source_id=?", (row["source_id"],)).fetchone()
        if old:
            original = json.loads(old[0])
            row["first_received_at"] = original["first_received_at"]
            row["mode"] = original["mode"]
            if old[1] == row["text_sha256"]:
                return False
            row["edited"] = True
            row["revision_observed"] = True
        blob = json.dumps(row, ensure_ascii=False)
        self.db.execute("INSERT OR IGNORE INTO revisions VALUES (?,?,?,?)", (row["source_id"], row["text_sha256"], row["received_at"], blob))
        self.db.execute("INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)", (
            row["source_id"], row["channel"], row["message_id"], row["published_at"], row["first_received_at"], row["mode"], blob, row["text_sha256"]))
        if commit:
            self.db.commit()
        return True

    def rows(self, since=None):
        query = "SELECT row_json FROM messages"
        params = ()
        if since:
            query += " WHERE published_at>=?"
            params = (utc(since).isoformat(),)
        return [json.loads(r[0]) for r in self.db.execute(query+" ORDER BY published_at,source_id",params)]

    def labels(self, since=None):
        if since:
            query = "SELECT l.source_id,l.label_json FROM labels l JOIN messages m ON l.source_id=m.source_id WHERE m.published_at>=?"
            data = self.db.execute(query,(utc(since).isoformat(),))
        else:
            data = self.db.execute("SELECT source_id,label_json FROM labels")
        return {r[0]:json.loads(r[1]) for r in data}

    def save_label(self, row, label, *, commit=True):
        label = dict(label, source_id=row["source_id"], labelled_at=utc().isoformat(), text_sha256=row["text_sha256"])
        self.db.execute("INSERT OR REPLACE INTO labels VALUES (?,?,?,?)", (row["source_id"], row["text_sha256"], label["labelled_at"], json.dumps(label, ensure_ascii=False)))
        if commit:
            self.db.commit()

    def delete_message(self, source_id):
        for table in ("messages", "revisions", "labels"):
            self.db.execute(f"DELETE FROM {table} WHERE source_id=?", (source_id,))
        self.db.commit()

    def expire_text(self, at):
        cutoff = utc(at)-timedelta(days=PROTOCOL["raw_text_retention_days"])
        expired = [json.loads(r[0]) for r in self.db.execute(
            "SELECT row_json FROM messages WHERE first_received_at<? AND json_extract(row_json,'$.content')<>''",(cutoff.isoformat(),))]
        cached_labels = {key:json.loads(blob) for key,blob in self.db.execute(
            "SELECT l.source_id,l.label_json FROM labels l JOIN messages m ON l.source_id=m.source_id WHERE m.first_received_at<? AND json_extract(m.row_json,'$.content')<>''",(cutoff.isoformat(),))}
        for row in expired:
            row["content"] = ""
            row["raw_text_expired"] = True
            self.db.execute("UPDATE messages SET row_json=? WHERE source_id=?", (json.dumps(row), row["source_id"]))
            self.db.execute("DELETE FROM revisions WHERE source_id=?", (row["source_id"],))
            old = cached_labels.get(row["source_id"])
            if old:
                for event in old.get("events", []):
                    event.pop("quote", None)
                self.db.execute("UPDATE labels SET label_json=? WHERE source_id=?", (json.dumps(old), row["source_id"]))
        self.db.commit()
        if expired:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return len(expired)

    def append_journal(self, kind, payload):
        previous = self.db.execute("SELECT sequence,sha FROM journal ORDER BY sequence DESC LIMIT 1").fetchone()
        sequence = previous[0]+1 if previous else 1
        row = {"sequence": sequence, "previous_sha": previous[1] if previous else None,
               "recorded_at": utc().isoformat(), "kind": kind, "payload": payload}
        self.db.execute("INSERT INTO journal VALUES (?,?,?,?)", (sequence, row["previous_sha"], digest(row), json.dumps(row)))

    def verify_journal(self):
        previous, number = None, 0
        sealed, sealed_outcomes, sealed_observations, sealed_activation = {}, {}, {}, None
        for sequence, parent, value, blob in self.db.execute("SELECT * FROM journal ORDER BY sequence"):
            row = json.loads(blob)
            if sequence != number+1 or parent != previous or row["previous_sha"] != previous or digest(row) != value:
                raise RuntimeError("Forward journal integrity failure")
            if row["kind"] == "decision":
                sealed[row["payload"]["event_id"]] = row["payload"]
            elif row["kind"] == "outcome":
                sealed_outcomes[row["payload"]["event_id"]] = row["payload"]
            elif row["kind"] == "activation":
                sealed_activation = row["payload"]
            elif row["kind"] == "feature_observation":
                sealed_observations[row["payload"]["feature_id"]] = row["payload"]
            previous, number = value, sequence
        for event_id, blob in self.db.execute("SELECT event_id,payload_json FROM decisions"):
            if sealed.get(event_id) != json.loads(blob):
                raise RuntimeError("Decision differs from its committed journal record")
        for event_id, blob in self.db.execute("SELECT event_id,payload_json FROM outcomes"):
            if sealed_outcomes.get(event_id) != json.loads(blob):
                raise RuntimeError("Outcome differs from its committed journal record")
        if self.state("activation") != sealed_activation:
            raise RuntimeError("Activation differs from its committed journal record")
        for feature_id,blob in self.db.execute("SELECT feature_id,payload_json FROM observations"):
            if sealed_observations.get(feature_id) != json.loads(blob):
                raise RuntimeError("Feature observation differs from its committed journal record")
        return {"records": number, "tail_sha": previous}

    def freeze(self, candidate, at):
        event_id = digest({k: candidate[k] for k in ("policy", "source", "symbol", "feature_id")})
        if self.db.execute("SELECT 1 FROM decisions WHERE event_id=?", (event_id,)).fetchone():
            return False
        entry = next_boundary(utc(at) + timedelta(seconds=60), 5)
        end = entry + timedelta(hours=candidate["holding_hours"])
        overlap = self.db.execute("SELECT 1 FROM decisions WHERE policy=? AND source=? AND symbol=? AND exit_at>?", (
            candidate["policy"], candidate["source"], candidate["symbol"], entry.isoformat())).fetchone()
        if overlap:
            return False
        payload = dict(candidate, event_id=event_id, frozen_at=utc(at).isoformat(), entry_at=entry.isoformat(), exit_at=end.isoformat(), protocol_sha=digest(PROTOCOL), paper_only=True)
        self.db.execute("INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?)", (
            event_id, candidate["policy"], candidate["source"], candidate["symbol"], utc(at).isoformat(), entry.isoformat(), end.isoformat(), json.dumps(payload)))
        self.append_journal("decision", payload)
        self.db.commit()
        return True

    def observe(self, payload):
        if self.db.execute("SELECT 1 FROM observations WHERE feature_id=?",(payload["feature_id"],)).fetchone():
            return False
        self.db.execute("INSERT INTO observations VALUES (?,?,?)",(payload["feature_id"],utc().isoformat(),json.dumps(payload)))
        self.append_journal("feature_observation",payload)
        self.db.commit()
        return True


def features(rows, labels, *, forward=False, activation=None, now=None, chat_bucket_hours=1):
    """Generate fixed hypotheses; no market prices enter text feature selection."""
    now = utc(now)
    votes, availability, ids, cq = defaultdict(lambda: defaultdict(list)), {}, defaultdict(list), []
    for row in rows:
        label = labels.get(row["source_id"])
        if not label or label.get("text_sha256") != row["text_sha256"]:
            continue
        if (label.get("kind") != "directional" or row.get("revision_observed")
                or (not forward and row.get("edited")) or row.get("forwarded") or row.get("sender_is_bot")):
            continue
        published = utc(row["published_at"])
        received = utc(row["first_received_at"])
        labelled = utc(label["labelled_at"])
        if forward:
            if row["mode"] != "forward" or not activation or published < utc(activation):
                continue
            if not 0 <= (received-published).total_seconds() <= PROTOCOL["max_receipt_lag_seconds"]:
                continue
            if not 0 <= (labelled-received).total_seconds() <= PROTOCOL["max_forward_label_lag_seconds"]:
                continue
        available = max(published, received, labelled) if forward else published
        for event in label.get("events", []):
            if event["symbol"] != PROTOCOL["asset"] or event["confidence"] < .7 or event["side"] not in (-1, 1):
                continue
            if row["channel"] == "cryptoquant_official":
                if available <= now:
                    cq.append({"policy": "cq_follow", "source": row["channel"], "symbol": event["symbol"], "side": event["side"],
                               "feature_id": row["source_id"], "available_at": available.isoformat(), "holding_hours": 24})
            elif row["channel"] == "BinanceChinese" and row.get("author_hash"):
                close = published.replace(hour=published.hour//chat_bucket_hours*chat_bucket_hours,
                                          minute=0, second=0, microsecond=0) + timedelta(hours=chat_bucket_hours)
                key = row["channel"], event["symbol"], close
                votes[key][row["author_hash"]].append(event["side"])
                availability[key] = max(availability.get(key, close), available)
                ids[key].append(row["source_id"])
    result = list(cq)
    for (channel, symbol, close), authors in sorted(votes.items()):
        available = max(close, availability[channel, symbol, close])
        score = sum(sum(v)/len(v) for v in authors.values())/len(authors)
        if close > now or available > now or len(authors) < 5 or abs(score) < .4:
            continue
        base = {"source": channel, "symbol": symbol, "feature_id": f"{channel}:{close.isoformat()}",
                "available_at": available.isoformat(), "holding_hours": 6,
                "directional_authors": len(authors), "imbalance": score,
                "source_ids_sha": digest(sorted(ids[channel, symbol, close]))}
        for variant, multiplier in (("follow", 1), ("contrarian", -1)):
            prefix = "chat_" if chat_bucket_hours==1 else f"chat_{chat_bucket_hours}h_"
            result.append(dict(base, policy=prefix+variant, bucket_hours=chat_bucket_hours,
                               side=(1 if score > 0 else -1)*multiplier))
    return sorted(result, key=lambda x: (x["available_at"], x["source"], x["policy"]))


def outcome(event, bars, funding):
    """Exact open-to-open paper return; absent funding coverage stays unavailable."""
    start, end = utc(event["entry_at"]), utc(event["exit_at"])
    prices = {utc(row["at"]): row for row in bars}
    if start not in prices or end not in prices:
        return {"status": "pending_price"}
    p0, p1 = float(prices[start]["open"]), float(prices[end]["open"])
    if not all(math.isfinite(p) and p > 0 for p in (p0,p1)):
        return {"status": "invalid_price"}
    side = event["side"]
    gross = side*(p1/p0-1)
    rates = sorted(funding, key=lambda r: r["at"])
    stamps = [utc(r["at"]) for r in rates]
    if not stamps or min(stamps) > start or max(stamps) < end:
        return {"status": "pending_funding", "gross": gross, "net_before_funding": gross-.002}
    covering = [r for r in rates if start-timedelta(hours=8) <= utc(r["at"]) <= end+timedelta(hours=8)]
    if any((utc(b["at"])-utc(a["at"])).total_seconds() > 8*3600+1 for a,b in zip(covering, covering[1:])):
        return {"status": "funding_gap"}
    settlements = [r for r in covering if start < utc(r["at"]) <= end]
    if any(r.get("mark") is None or not math.isfinite(float(r["mark"])) or float(r["mark"]) <= 0
           or not math.isfinite(float(r["rate"])) for r in settlements):
        return {"status": "funding_mark_missing"}
    long_fund = -sum(float(r["rate"])*float(r["mark"])/p0 for r in settlements)
    net = gross-.002+side*long_fund
    return {"status": "settled", "gross": gross, "funding": side*long_fund, "net": net,
            "net_stress": gross-.004+side*long_fund, "same_entry_long_net": p1/p0-1-.002+long_fund,
            "entry_price": p0, "exit_price": p1}
