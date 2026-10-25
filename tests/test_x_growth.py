"""The growth store keeps one honest follower snapshot per account per day."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.news import x_growth as growth

DAY0 = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn(tmp_path):
    connection = growth.connect(tmp_path / "growth.db")
    yield connection
    connection.close()


def _profile(user_id, username, followers, **extra):
    record = {"user_id": str(user_id), "username": username, "name": username.title(), "description": "",
              "followers_count": followers, "following_count": 10, "statuses_count": 100, "listed_count": 1,
              "verified": False, "blue_verified": True, "protected": False, "created_at": "2024-01-01"}
    record.update(extra)
    return record


def test_kind_guess_is_a_hint_not_a_filter():
    assert growth.guess_kind("Aster", "The official DEX of the Aster network") == "project"
    assert growth.guess_kind("Whale Alert", "Breaking crypto news and alerts") == "feed"
    assert growth.guess_kind("Kaleo", "Trader. Views are my own. NFA") == "individual"
    assert growth.guess_kind("加密研究员", "链上分析 个人观点") == "individual"
    assert growth.guess_kind("Someone", "gm") == "unknown"


def test_candidates_are_added_once_and_refreshed(conn):
    assert growth.upsert_candidate(conn, _profile(1, "Alpha", 1000), "search:crypto trader", DAY0) is True
    assert growth.upsert_candidate(conn, _profile(1, "Alpha", 1200, name="Alpha Renamed"), "mention", DAY0) is False
    assert growth.upsert_candidate(conn, {"user_id": "abc", "username": "bad"}, "mention", DAY0) is False
    row = conn.execute("select * from candidates").fetchone()
    assert row["name"] == "Alpha Renamed" and row["origin"] == "search:crypto trader"  # origin = first source


def test_pending_queue_skips_known_accounts_and_gives_up_after_three_tries(conn):
    growth.upsert_candidate(conn, _profile(1, "Known", 500), "x", DAY0)
    assert growth.add_pending(conn, {"known": 9, "Fresh": 5, "other": 2}) == 2   # Known is already a candidate
    assert growth.add_pending(conn, {"fresh": 7}) == 0                          # already queued: only the count rises
    assert growth.pending_batch(conn, 10) == [("fresh", 7), ("other", 2)]
    growth.mark_pending(conn, "fresh", "done", DAY0)
    for _ in range(3):
        growth.mark_pending(conn, "other", "retry", DAY0)
    assert growth.pending_batch(conn, 10) == []
    assert growth.status(conn)["pending"] == {"done": 1, "failed": 1}
    growth.mark_pending(conn, "ghost", "missing", DAY0)  # an unknown name is simply recorded as updated nothing


def test_one_snapshot_per_account_per_utc_day(conn):
    growth.upsert_candidate(conn, _profile(1, "Alpha", 1000), "x", DAY0)
    assert growth.save_snapshot(conn, _profile(1, "Alpha", 1000), DAY0) is True
    assert growth.save_snapshot(conn, _profile(1, "Alpha", 1010), DAY0 + timedelta(hours=5)) is False  # same day
    assert conn.execute("select followers from snapshots").fetchone()[0] == 1000
    assert growth.save_snapshot(conn, _profile(1, "Alpha", 1010), DAY0 + timedelta(hours=5), replace=True) is True
    assert growth.save_snapshot(conn, _profile(1, "Alpha", 1100), DAY0 + timedelta(days=1)) is True
    assert growth.save_snapshot(conn, {"user_id": "1", "followers_count": None}, DAY0) is False
    assert growth.due_ids(conn, "2026-10-02") == []
    assert growth.due_ids(conn, "2026-10-03") == ["1"]


def test_accounts_that_stop_coming_back_are_retired_after_three_misses(conn):
    for i in (1, 2):
        growth.upsert_candidate(conn, _profile(i, f"user{i}", 100), "x", DAY0)
    for _ in range(2):
        assert growth.note_lookup(conn, ["1", "2"], ["1"]) == 1
    assert [r["user_id"] for r in conn.execute("select user_id from candidates where active = 1")] == ["1", "2"]
    growth.note_lookup(conn, ["1", "2"], ["1"])
    assert [r["user_id"] for r in conn.execute("select user_id from candidates where active = 1")] == ["1"]
    growth.note_lookup(conn, ["1"], ["1"])  # coming back resets nothing for others and never retires a returning one
    assert conn.execute("select miss_count from candidates where user_id = '1'").fetchone()[0] == 0


def test_growth_ranks_by_relative_change_over_the_requested_span(conn):
    series = {1: (1000, 1500), 2: (10_000, 11_000), 3: (50, 100), 4: (2000, 2000)}
    for user_id, (start, end) in series.items():
        growth.upsert_candidate(conn, _profile(user_id, f"user{user_id}", start), "x", DAY0)
        growth.save_snapshot(conn, _profile(user_id, f"user{user_id}", start), DAY0)
        growth.save_snapshot(conn, _profile(user_id, f"user{user_id}", end), DAY0 + timedelta(days=7))
    growth.upsert_candidate(conn, _profile(5, "newcomer", 300), "x", DAY0)
    growth.save_snapshot(conn, _profile(5, "newcomer", 300), DAY0 + timedelta(days=7))   # one snapshot: no growth yet
    rows = growth.growth_rows(conn, 7, today=DAY0 + timedelta(days=7))
    assert [r["username"] for r in rows] == ["user3", "user1", "user2", "user4"]
    assert rows[0]["pct"] == pytest.approx(100.0) and rows[1]["change"] == 500
    assert [r["username"] for r in growth.growth_rows(conn, 7, min_followers=500, today=DAY0 + timedelta(days=7))] == [
        "user1", "user2", "user4"]
    assert growth.growth_rows(conn, 30, today=DAY0 + timedelta(days=7)) == []   # no snapshot 30 days back
    assert growth.status(conn)["candidates"] == 5


def test_stale_accounts_come_oldest_first_with_never_seen_before_them(conn):
    for user_id, age in {1: 9, 2: 3, 3: None, 4: 20, 5: 7}.items():
        growth.upsert_candidate(conn, _profile(user_id, f"user{user_id}", 100), "x", DAY0)
        if age is not None:
            growth.save_snapshot(conn, _profile(user_id, f"user{user_id}", 100), DAY0 - timedelta(days=age))
    assert growth.stale_accounts(conn, "2026-10-01", 7, 10) == [("3", "user3"), ("4", "user4"), ("1", "user1"), ("5", "user5")]
    assert growth.stale_accounts(conn, "2026-10-01", 7, 2) == [("3", "user3"), ("4", "user4")]
    growth.note_lookup(conn, ["3"], []); growth.note_lookup(conn, ["3"], []); growth.note_lookup(conn, ["3"], [])
    assert ("3", "user3") not in growth.stale_accounts(conn, "2026-10-01", 7, 10)    # retired accounts are not asked
