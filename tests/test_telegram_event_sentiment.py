from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

import pytest

from core.research.telegram_event_sentiment import Message, aggregate, read_archive, study

START = datetime(2026, 10, 24, 3, tzinfo=timezone.utc)
AS_OF = START + timedelta(days=2)


def messages(sides, *, step=timedelta(minutes=1)):
    return [Message(i, START + i * step, START + i * step + timedelta(seconds=10), f"a{i}", f"text{i}", side)
            for i, side in enumerate(sides)]


def test_count_batches_cross_hours_and_are_not_overlapping_trials():
    rows = messages([1] * 8 + [-1] * 2 + [1] * 5, step=timedelta(hours=1))
    out = study(rows, coverage=[(START, AS_OF)], as_of=AS_OF)
    main = next(c for c in out["configs"] if c["clock"] == "btc_views" and c["size"] == 10)
    assert main["complete_batches"] == 1 and main["remainder_messages"] == 5
    assert main["qualified_batches"] == 1 and main["certified_qualified_batches"] == 1
    assert main["batches"][0]["score"] == pytest.approx(60)
    assert main["batches"][0]["span_hours"] == 9
    assert main["latest_rolling"]["index"] == -1


def test_author_weighting_and_repeated_text_do_not_create_a_crowd():
    rows = [Message(i, START, START, "one_author", f"text{i}", 1) for i in range(8)]
    rows += [Message(8, START, START, "other_author", "different", -1)]
    out = aggregate(rows, clock="btc_views", size=9, index=0, coverage=[], as_of=AS_OF)
    assert out["authors"] == 2 and out["score"] == 0 and not out["qualified"]
    rows = [Message(i, START, START, f"author{i}", "copied_text", 1) for i in range(5)]
    out = aggregate(rows, clock="btc_views", size=5, index=0, coverage=[], as_of=AS_OF)
    assert out["authors"] == 1 and out["repeated_view_texts"] == 4 and not out["qualified"]


def test_missing_coverage_is_reported_without_renaming_it_a_gap():
    rows = messages([1] * 5)
    out = aggregate(rows, clock="btc_views", size=5, index=0, coverage=[], as_of=AS_OF)
    assert out["qualified"] and out["coverage_status"] == "not_certified"
    assert "gap" not in out["coverage_status"]


def test_incomplete_batch_and_zero_opinions_do_not_become_signals():
    out = aggregate(messages([1] * 5), clock="btc_views", size=10, index=-1, coverage=[], as_of=AS_OF)
    assert not out["complete"] and not out["qualified"]
    out = aggregate(messages([None] * 100), clock="messages", size=100, index=0, coverage=[], as_of=AS_OF)
    assert out["authors"] == 0 and out["score"] is None and not out["qualified"]


def test_archive_filters_metadata_and_receipt_time_without_selecting_content(tmp_path):
    path = tmp_path / "observations.sqlite"
    with sqlite3.connect(path) as db:
        db.executescript("""CREATE TABLE messages (source_id TEXT PRIMARY KEY,channel TEXT,mode TEXT,published_at TEXT,
                           first_received_at TEXT,message_id INTEGER,text_sha TEXT,row_json TEXT);
                           CREATE TABLE labels (source_id TEXT PRIMARY KEY,text_sha TEXT,labelled_at TEXT,label_json TEXT);
                           CREATE TABLE observations (feature_id TEXT PRIMARY KEY,payload_json TEXT);""")
        for i, extras in enumerate([{}, {"sender_is_bot": True}, {"revision_observed": True}, {"late": True}]):
            pub = START + timedelta(minutes=i)
            received = pub + (timedelta(hours=1) if extras.get("late") else timedelta(seconds=5))
            row = dict(content="placeholder", author_hash=f"a{i}", **extras)
            db.execute("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)", (str(i), "BinanceChinese", "forward",
                       pub.isoformat(), received.isoformat(), i, f"sha{i}", json.dumps(row)))
            label = {"kind": "directional", "events": [{"symbol": "BTCUSDT", "side": 1, "confidence": .9, "quote": "placeholder"}]}
            db.execute("INSERT INTO labels VALUES (?,?,?,?)", (str(i), f"sha{i}", (received + timedelta(seconds=2)).isoformat(), json.dumps(label)))
    before = path.read_bytes()
    rows, metadata, _ = read_archive(tmp_path, activation={"at": START.isoformat()}, status={"checked_at": AS_OF.isoformat()})
    assert len(rows) == 1 and rows[0].side == 1
    assert metadata["counts"]["excluded_metadata"] == 2
    assert metadata["counts"]["excluded_late_or_future"] == 1
    assert "placeholder" not in json.dumps(metadata)
    assert path.read_bytes() == before
