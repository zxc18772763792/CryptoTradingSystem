"""The X research archive stores each post once, with honest availability times."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from core.news import x_archive


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(x_archive, "_seen", {})


def _tweet(tweet_id, **extra):
    tweet = {"tweet_id": str(tweet_id), "user": {"screen_name": "Alpha", "name": "A"},
             "timestamp": "Sat Oct 24 13:54:50 +0000 2026", "text": "a &amp; b $BTC", "likes": 3, "comments": 1,
             "media": {"image_links": ["x"], "video_links": []}, "is_quote": False, "is_retweet": False}
    tweet.update(extra)
    return tweet


def test_posts_are_stored_once_with_both_times(tmp_path):
    seen_at = datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc)
    result = x_archive.observe("Alpha", [_tweet(1), _tweet(1), _tweet("bad"), {"tweet_id": ""}, "junk"],
                               provenance="backfill", observed_at=seen_at, directory=tmp_path)
    assert result == {"new": 1, "offered": 4}  # "junk" is not a post at all
    (record,) = x_archive.load_archive(directory=tmp_path)
    assert record["handle"] == "alpha" and record["author"] == "alpha"
    assert record["text"] == "a & b $BTC" and record["image_count"] == 1
    assert record["created_at"] == "2026-10-24T13:54:50+00:00"
    assert record["first_seen_at"] == seen_at.isoformat() and record["provenance"] == "backfill"
    assert x_archive.observe("alpha", [_tweet(1), _tweet(2)], directory=tmp_path)["new"] == 1  # also across calls


def test_earliest_sighting_wins_when_two_writers_duplicate(tmp_path):
    early = datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc)
    x_archive.observe("alpha", [_tweet(1)], provenance="live", observed_at=early, directory=tmp_path)
    # A second process that had not yet seen the post appends its own line for the same post.
    later = x_archive.to_record("alpha", _tweet(1), early + timedelta(days=1), "backfill")
    with (tmp_path / "alpha.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(later) + chr(10))
    assert len((tmp_path / "alpha.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    (record,) = x_archive.load_archive(["alpha"], directory=tmp_path)
    assert record["provenance"] == "live" and record["first_seen_at"] == early.isoformat()


def test_missing_or_naive_dates_stay_empty_and_oldest_is_reported(tmp_path):
    x_archive.observe("alpha", [_tweet(1, timestamp="Wed Oct 21 12:34:56 2026"),
                                _tweet(2, timestamp="Thu Oct 22 08:00:00 +0000 2026"),
                                _tweet(3, timestamp="Fri Oct 23 08:00:00 +0000 2026")], directory=tmp_path)
    records = {r["tweet_id"]: r for r in x_archive.load_archive(directory=tmp_path)}
    assert records["1"]["created_at"] is None  # never invent a date
    assert x_archive.oldest_created("alpha", tmp_path) == "2026-10-22T08:00:00+00:00"
    assert x_archive.oldest_created("nobody", tmp_path) is None


def test_bad_handle_and_provenance_are_rejected(tmp_path):
    with pytest.raises(ValueError):
        x_archive.observe("../etc", [_tweet(1)], directory=tmp_path)
    with pytest.raises(ValueError):
        x_archive.observe("alpha", [_tweet(1)], provenance="guess", directory=tmp_path)
