from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import pytest

from core.research import telegram_sentiment_observer as observer

T0 = datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)


def views(sides, *, author_ids=None, text_ids=None, start=T0):
    return [{"message_id": i, "published_at": (start + timedelta(minutes=i)).isoformat(),
             "available_at": (start + timedelta(minutes=i, seconds=10)).isoformat(),
             "author": (author_ids or [f"a{i}" for i in range(len(sides))])[i],
             "text_sha": (text_ids or [f"text{i}" for i in range(len(sides))])[i], "side": side}
            for i, side in enumerate(sides)]


def test_latest_author_view_replaces_old_stance_and_copying_does_not_add_authors():
    data = views([1, -1, -1, -1, 1], author_ids=["a", "a", "a", "b", "c"])
    out = observer.measure(data, 5, T0 + timedelta(hours=1), covered=True)
    assert out["authors"] == 3 and out["bullish"] == 1 and out["bearish"] == 2
    assert out["superseded_views"] == 2 and out["score"] == pytest.approx(-100 / 3)
    assert out["smoothed_score"] == -20
    copied = observer.measure(views([1] * 5, text_ids=["same"] * 5), 5, T0 + timedelta(hours=1), covered=True)
    assert copied["authors"] == 1 and copied["echo_authors_removed"] == 4 and not copied["qualified"]


def test_wilson_interval_handles_small_samples_and_is_an_author_share_interval():
    assert observer.wilson(0, 0) is None
    assert observer.wilson(5, 10) == pytest.approx([.2365930905, .7634069095])
    lo, hi = observer.wilson(5, 5)
    assert lo == pytest.approx(.5655175352) and hi == pytest.approx(1)
    one = observer.measure(views([1]), 10, T0 + timedelta(hours=1), covered=True)
    assert one["score"] == 100 and one["smoothed_score"] == pytest.approx(100 / 3)
    assert one["status"] == "warming_up" and not one["qualified"]


@pytest.mark.parametrize("text,reason", [
    ("期待6字开头的BTC", "price_target_without_direction"),
    ("BTC目标10万，等你", "price_target_without_direction"),
    ("BTC继续上涨，目标10万", None),
    ("长期看多BTC", "long_horizon"),
    ("BTC长期看多，但今天短线看跌", "mixed_horizon"),
    ("今晚BTC看多", None),
])
def test_narrow_quality_guards_quarantine_ambiguous_targets_and_horizons(text, reason):
    assert observer.quality_guard(text) == reason


def test_age_span_coverage_and_background_never_qualify_as_live_candidates():
    data = views([1] * 10)
    assert observer.measure(data, 10, T0 + timedelta(hours=7), covered=True)["status"] == "stale_views"
    assert observer.measure(data, 10, T0 + timedelta(hours=1), covered=False)["status"] == "coverage_gap"
    historical = observer.measure(data, 10, T0 + timedelta(hours=1), covered=True, historical=True)
    assert historical["status"] == "historical" and not historical["qualified"]
    spread = views([1] * 10, start=T0 - timedelta(days=2))
    spread[-1].update(available_at=(T0 + timedelta(minutes=1)).isoformat())
    assert observer.measure(spread, 10, T0 + timedelta(hours=1), covered=True)["status"] == "long_window"


@pytest.fixture()
def study(tmp_path, monkeypatch):
    source, output = tmp_path / "source", tmp_path / "observer"
    source.mkdir()
    monkeypatch.setenv("TELEGRAM_SENTIMENT_V2_DIR", str(output))
    (source / "activation.json").write_text(json.dumps({"at": (T0 - timedelta(days=1)).isoformat(),
        "implementation_sha": "source-frozen-sha", "classifier_model": "audited_test_model"}))
    with sqlite3.connect(source / "observations.sqlite") as db:
        db.executescript("""CREATE TABLE messages(source_id TEXT PRIMARY KEY,channel TEXT,mode TEXT,message_id INTEGER,
            published_at TEXT,first_received_at TEXT,text_sha TEXT,row_json TEXT);
            CREATE TABLE labels(source_id TEXT PRIMARY KEY,text_sha TEXT,labelled_at TEXT,label_json TEXT);
            CREATE TABLE observations(feature_id TEXT PRIMARY KEY,payload_json TEXT);""")
    checkpoint(source, T0)
    return source, output


def checkpoint(source, at, *, complete=True, clean=None):
    payload = {"checked_at": at.isoformat(), "sources": {"BinanceChinese": {
        "member": True, "complete_batch": complete, "difference_gap": not complete,
        "clean_since": (clean or T0 - timedelta(hours=1)).isoformat(), "last_collection_at": at.isoformat()}}}
    (source / "forward_status.json").write_text(json.dumps(payload))


def insert(source, data):
    with sqlite3.connect(source / "observations.sqlite") as db:
        for v in data:
            row = {"author_hash": v["author"], "content": "redacted fixture text"}
            p = observer.utc(v["published_at"])
            label = {"kind": "directional", "events": [{"symbol": "BTCUSDT", "side": v["side"], "confidence": .9}]}
            db.execute("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)", (str(v["message_id"]), "BinanceChinese", "forward",
                v["message_id"], v["published_at"], (p + timedelta(seconds=5)).isoformat(), v["text_sha"], json.dumps(row)))
            db.execute("INSERT INTO labels VALUES (?,?,?,?)", (str(v["message_id"]), v["text_sha"], v["available_at"], json.dumps(label)))


def test_initialization_is_background_only_and_source_stays_unchanged(study):
    source, output = study
    insert(source, views([1] * 20, start=T0 - timedelta(hours=1)))
    before = (source / "observations.sqlite").read_bytes()
    activation = (source / "activation.json").read_bytes()
    result = observer.sync(source, output, now=T0)
    assert result["background"][0]["score"] == 100
    assert result["new_views_in_segment"] == 0 and all(w["views"] == 0 for w in result["rolling"])
    assert all(b["decisions"] == 0 and b["mean_net_pct"] is None for b in result["books"])
    assert (source / "observations.sqlite").read_bytes() == before
    assert (source / "activation.json").read_bytes() == activation
    assert (output / "protocol.json").exists() and not result["promotion_allowed"]


def test_new_complete_disjoint_batch_freezes_only_future_paper_entries_and_is_idempotent(study):
    source, output = study
    observer.sync(source, output, now=T0)
    insert(source, views([1] * 10, start=T0 + timedelta(minutes=1)))
    at = T0 + timedelta(hours=1)
    checkpoint(source, at)
    result = observer.sync(source, output, now=at)
    assert result["rolling"][0]["qualified"] and result["rolling"][1]["status"] == "warming_up"
    assert [b["decisions"] for b in result["books"]] == [1, 1, 0, 0]
    with sqlite3.connect(output / "observer.sqlite") as db:
        events = [json.loads(r[0]) for r in db.execute("SELECT payload_json FROM decisions")]
    assert all(observer.utc(e["entry_at"]) > at + timedelta(seconds=60) for e in events)
    assert {e["side"] for e in events} == {-1, 1}
    again = observer.sync(source, output, now=at + timedelta(seconds=1))
    assert again["journal"] == result["journal"] and again["books"] == result["books"]


def test_later_deletions_do_not_rewrite_frozen_views_or_features(study):
    source, output = study
    observer.sync(source, output, now=T0)
    insert(source, views([1] * 10, start=T0 + timedelta(minutes=1)))
    checkpoint(source, T0 + timedelta(hours=1))
    first = observer.sync(source, output, now=T0 + timedelta(hours=1))
    with sqlite3.connect(source / "observations.sqlite") as db:
        db.execute("DELETE FROM labels")
        db.execute("DELETE FROM messages")
    again = observer.sync(source, output, now=T0 + timedelta(hours=1, minutes=1))
    assert again["history"] == first["history"] and again["books"] == first["books"]
    assert again["rolling"][0]["score"] == 100


def test_gap_resets_forward_accumulation_and_cannot_create_new_trades(study):
    source, output = study
    observer.sync(source, output, now=T0)
    insert(source, views([1] * 10, start=T0 + timedelta(minutes=1)))
    checkpoint(source, T0 + timedelta(hours=1), complete=False, clean=T0 + timedelta(minutes=30))
    out = observer.sync(source, output, now=T0 + timedelta(hours=1))
    assert out["new_views_in_segment"] == 0
    assert all(w["status"] == "coverage_gap" for w in out["rolling"])
    assert all(b["decisions"] == 0 for b in out["books"])
    checkpoint(source, T0 + timedelta(hours=1, minutes=1), clean=T0 + timedelta(minutes=30))
    out = observer.sync(source, output, now=T0 + timedelta(hours=1, minutes=1))
    assert out["new_views_in_segment"] == 0  # Earlier views are not repaired into the new segment.


def test_protocol_drift_and_backwards_checkpoint_stop_observer(study, monkeypatch):
    source, output = study
    observer.sync(source, output, now=T0)
    monkeypatch.setattr(observer, "implementation_sha", lambda: "changed")
    with pytest.raises(ValueError, match="implementation changed"):
        observer.sync(source, output, now=T0 + timedelta(minutes=1))
    monkeypatch.undo()
    checkpoint(source, T0 - timedelta(minutes=1))
    with pytest.raises(ValueError, match="moved backwards"):
        observer.sync(source, output, now=T0 + timedelta(minutes=1))


def test_missing_funding_is_pending_and_settlement_uses_actual_cost_and_funding(study):
    source, output = study
    observer.sync(source, output, now=T0)
    insert(source, views([1] * 10, start=T0 + timedelta(minutes=1)))
    checkpoint(source, T0 + timedelta(hours=1))
    observer.sync(source, output, now=T0 + timedelta(hours=1))
    with sqlite3.connect(output / "observer.sqlite") as db:
        event = json.loads(db.execute("SELECT payload_json FROM decisions LIMIT 1").fetchone()[0])
    entry, exit_at = observer.utc(event["entry_at"]), observer.utc(event["exit_at"])
    market = {"symbol": "BTCUSDT", "interval": "5m", "bars": [{"at": entry.isoformat(), "open": 100}, {"at": exit_at.isoformat(), "open": 110}], "funding": []}
    (source / "forward_market.json").write_text(json.dumps(market))
    at = exit_at + timedelta(minutes=1)
    checkpoint(source, at)
    missing = observer.sync(source, output, now=at)
    assert missing["books"][0]["settled"] == 0 and missing["books"][0]["pending"] == 1
    assert missing["books"][0]["mean_net_pct"] is None
    market["funding"] = [{"at": (entry - timedelta(hours=1)).isoformat(), "rate": .001, "mark": 100},
                          {"at": (entry + timedelta(hours=3)).isoformat(), "rate": .001, "mark": 100},
                          {"at": (exit_at + timedelta(hours=1)).isoformat(), "rate": .001, "mark": 100}]
    (source / "forward_market.json").write_text(json.dumps(market))
    still_pending = observer.sync(source, output, now=at + timedelta(seconds=1))
    assert still_pending["books"][0]["settled"] == 0  # Future funding cannot certify coverage.
    at = exit_at + timedelta(hours=1, minutes=1)
    checkpoint(source, at)
    settled = observer.sync(source, output, now=at)
    assert settled["books"][0]["mean_net_pct"] == pytest.approx(9.7)
    assert settled["books"][0]["mean_stress_pct"] == pytest.approx(9.5)
    assert settled["books"][1]["mean_net_pct"] == pytest.approx(-10.1)


def test_reader_masks_current_qualification_on_source_failure_and_never_writes(study):
    source, output = study
    observer.sync(source, output, now=T0)
    before = (output / "summary.json").read_bytes()
    read = observer.load_summary(source, now=T0, source_health={"status": "coverage_gap"})
    assert all(w["status"] == "unavailable" and not w["qualified"] for w in read["rolling"])
    assert (output / "summary.json").read_bytes() == before
    stale = observer.load_summary(source, now=T0 + timedelta(minutes=16))
    assert stale["status"] == "stale"


def test_optional_observer_failure_does_not_fail_the_original_cycle(tmp_path, monkeypatch):
    import scripts.telegram_forward_cycle as wrapper
    import scripts.report_telegram_forward as report
    (tmp_path / "activation.json").write_text("{}")
    monkeypatch.setattr(observer, "observer_dir", lambda source: tmp_path)
    seen = []

    async def original(args, store):
        seen.append("original")

    def fail(*args, **kwargs):
        raise OSError("fixture failure")

    monkeypatch.setattr(wrapper, "_original_dispatch", original)
    monkeypatch.setattr(report, "summarize", lambda source: seen.append("summary"))
    monkeypatch.setattr(observer, "sync", fail)
    monkeypatch.setattr(observer, "record_error", fail)
    args = type("Args", (), {"command": "cycle"})()
    store = type("Store", (), {"root": tmp_path})()
    asyncio.run(wrapper.dispatch_with_summary(args, store))
    assert seen == ["original", "summary"]
