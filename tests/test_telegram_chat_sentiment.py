from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

import pytest

from core.research import telegram_chat_sentiment as sentiment
from core.research import telegram_forward_tracker as tracker
from core.research import tracker_registry

NOW = datetime(2026, 10, 31, 1, 5, tzinfo=timezone.utc)
ACTIVATION = {"at": "2026-10-24T02:51:47+00:00"}
STATUS = {"checked_at": NOW.isoformat(), "pending_labels": {}}
HEALTH = {"status": "healthy", "clean_since": "2026-10-25T00:00:00+00:00"}


@pytest.fixture()
def db(tmp_path):
    with sqlite3.connect(tmp_path / "observations.sqlite") as conn:
        conn.execute("CREATE TABLE observations (feature_id TEXT PRIMARY KEY, recorded_at TEXT, payload_json TEXT)")
    return tmp_path


def observe(base, *, hours=1, authors=6, imbalance=.5, reason="qualified", end=None, available=None):
    end = end or NOW.replace(hour=NOW.hour // hours * hours, minute=0, second=0, microsecond=0)
    available = available or end + timedelta(seconds=10)
    value = {"feature_id": f"BinanceChinese:{hours}h:{end.isoformat()}", "source": "BinanceChinese",
             "bucket_hours": hours, "window_start": (end - timedelta(hours=hours)).isoformat(),
             "window_end": end.isoformat(), "available_at": available.isoformat(),
             "directional_authors": authors, "imbalance": imbalance, "reason": reason}
    with sqlite3.connect(base / "observations.sqlite") as conn:
        conn.execute("INSERT INTO observations VALUES (?,?,?)",
                     (value["feature_id"], available.isoformat(), json.dumps(value)))


def snapshot(base, **kwargs):
    return sentiment.snapshot(base, now=NOW, activation=ACTIVATION, status=STATUS, health=HEALTH, **kwargs)


def test_reads_frozen_author_weighted_balance_and_never_changes_the_database(db):
    observe(db)
    observe(db, hours=6, authors=3, imbalance=-1 / 3, reason="insufficient_authors")
    path = db / "observations.sqlite"
    before = path.read_bytes()
    out = snapshot(db)
    one, six = out["windows"]
    assert one["authors"] == 6 and one["score"] == 50 and one["qualified"]
    assert six["authors"] == 3 and six["score"] == -33.33 and not six["qualified"]
    assert six["status"] == "insufficient_authors"
    assert out["read_only"] and path.read_bytes() == before
    assert "content" not in json.dumps(out) and "author_hash" not in json.dumps(out)


def test_zero_opinions_differs_from_a_measured_balanced_zero(db):
    observe(db, authors=0, imbalance=None, reason="no_directional_views")
    observe(db, hours=6, authors=6, imbalance=0, reason="low_consensus")
    one, six = snapshot(db)["windows"]
    assert one["authors"] == 0 and one["score"] is None
    assert six["authors"] == 6 and six["score"] == 0
    assert not one["qualified"] and not six["qualified"]


def test_gap_suppresses_current_scores_and_retains_explicit_history(db):
    observe(db)
    health = dict(HEALTH, status="coverage_gap")
    out = sentiment.snapshot(db, now=NOW, activation=ACTIVATION, status=STATUS, health=health)
    assert all(w["score"] is None and w["authors"] is None and not w["qualified"] for w in out["windows"])
    assert all(w["status"] == "coverage_gap" for w in out["windows"])
    assert out["history"][0]["score"] == 50  # Historical observations remain frozen.


def test_hourly_window_can_be_valid_while_six_hour_window_has_a_gap(db):
    observe(db, authors=0, imbalance=None, reason="no_directional_views")
    health = dict(HEALTH, clean_since="2026-10-30T20:43:30+00:00")
    out = sentiment.snapshot(db, now=NOW, activation=ACTIVATION, status=STATUS, health=health)
    one, six = out["windows"]
    assert one["status"] == "no_directional_views" and one["authors"] == 0
    assert six["status"] == "coverage_gap" and six["authors"] is None
    assert six["next_complete_at"] == "2026-10-31T06:00:00+00:00"


def test_missing_or_unreadable_observations_do_not_become_zero_authors(tmp_path):
    out = snapshot(tmp_path)
    assert not (tmp_path / "observations.sqlite").exists()
    assert not out["store_available"]
    assert all(d["recorded_windows"] is None for d in out["diagnostics"])
    assert all(w["authors"] is None and w["status"] == "unavailable" for w in out["windows"])
    (tmp_path / "observations.sqlite").write_bytes(b"invalid sqlite")
    assert not snapshot(tmp_path)["store_available"]


def test_no_record_is_not_evidence_of_no_opinions(db):
    out = snapshot(db)
    assert out["store_available"]
    assert all(w["status"] == "awaiting_observation" and w["authors"] is None for w in out["windows"])


@pytest.mark.parametrize("health_status", ["interrupted", "collection_error", "inaccessible", "coverage_unverified"])
def test_unhealthy_source_never_presents_a_current_direction(db, health_status):
    observe(db)
    out = sentiment.snapshot(db, now=NOW, activation=ACTIVATION, status=STATUS,
                             health=dict(HEALTH, status=health_status))
    assert all(w["status"] == health_status and w["score"] is None for w in out["windows"])


def test_pending_labels_and_activation_inside_window_remain_unknown(db):
    observe(db)
    out = sentiment.snapshot(db, now=NOW, activation=ACTIVATION,
                             status=dict(STATUS, pending_labels={"BinanceChinese": 1}), health=HEALTH)
    assert all(w["status"] == "labels_pending" and w["score"] is None for w in out["windows"])
    out = sentiment.snapshot(db, now=NOW, activation={"at": "2026-10-31T00:30:00+00:00"}, status=STATUS, health=HEALTH)
    assert out["windows"][0]["status"] == "warming_up"
    assert out["windows"][0]["next_complete_at"] == "2026-10-31T02:00:00+00:00"


@pytest.mark.parametrize("authors,imbalance,reason", [(0, 0, "no_directional_views"), (1, 1, "qualified"),
                                                       (6, float("nan"), "qualified"), (6, 1.1, "qualified")])
def test_invalid_scores_and_false_qualification_are_unavailable(db, authors, imbalance, reason):
    observe(db, authors=authors, imbalance=imbalance, reason=reason)
    out = snapshot(db)
    assert not out["store_available"] and out["windows"][0]["score"] is None


def test_read_does_not_include_observations_beyond_the_completed_cycle(db):
    observe(db, available=NOW + timedelta(seconds=10))
    out = snapshot(db)
    assert out["store_available"] and out["windows"][0]["status"] == "awaiting_observation"
    assert out["history"] == []


def test_missing_source_health_does_not_claim_coverage_or_running(db, monkeypatch):
    monkeypatch.setenv("TELEGRAM_FORWARD_DIR", str(db))
    for name, value in (("activation.json", ACTIVATION), ("forward_status.json", STATUS)):
        (db / name).write_text(json.dumps(value))
    out = tracker.summary("binance_chat", now=NOW)
    assert out["source_health"]["status"] == "coverage_unverified"
    assert "待确认" in out["tracker_status_label"]
    assert all(w["authors"] is None for w in out["chat_sentiment"]["windows"])


def test_empty_ledgers_suppress_placeholder_win_rate_and_returns():
    out = tracker._book_row("chat_follow", {"mature_settled": 0, "win_rate": 0, "mean_net_per_notional": 0})
    assert out["mean_net_pct"] is None and out["win_rate"] is None


def test_registry_preserves_chat_diagnostics_and_source_health(db, monkeypatch):
    monkeypatch.setenv("TELEGRAM_FORWARD_DIR", str(db))
    for name, value in (("activation.json", ACTIVATION), ("forward_status.json", STATUS)):
        (db / name).write_text(json.dumps(value))
    spec = dict(next(s for s in tracker_registry.TRACKERS if s["id"] == "telegram_binance_chat_sentiment"))
    spec["load"] = lambda: tracker.summary("binance_chat", now=NOW)
    out = tracker_registry.tracker_row(spec)
    assert len(out["chat_sentiment"]["windows"]) == 2 and len(out["ledgers"]) == 4
    assert out["source_health"]["status"] == "coverage_unverified"
    assert out["n"] == 0 and out["mean_pct"] is None


def test_future_collection_clock_never_reports_a_fresh_source(db, monkeypatch):
    monkeypatch.setenv("TELEGRAM_FORWARD_DIR", str(db))
    (db / "activation.json").write_text(json.dumps(ACTIVATION))
    (db / "forward_status.json").write_text(json.dumps(dict(STATUS, checked_at=(NOW + timedelta(minutes=2)).isoformat())))
    out = tracker.summary("binance_chat", now=NOW)
    assert out["runner_active"] is False and out["source_health"]["status"] == "coverage_unverified"
