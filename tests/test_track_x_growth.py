"""The growth tracker records pools and snapshots, stays inside its request cap and never trusts a silent empty answer."""
from __future__ import annotations

import importlib.util
import logging
from datetime import timedelta
from pathlib import Path

import pytest

from core.news import x_batch, x_growth, x_pacing
from core.news.collectors.x_accounts import XRateLimited
from core.news.x_batch import DailyBudgetSpent

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "track_x_growth.py"
spec = importlib.util.spec_from_file_location("track_x_growth", SCRIPT)
track = importlib.util.module_from_spec(spec)
spec.loader.exec_module(track)


def _profile(user_id, username, followers):
    return {"user_id": str(user_id), "username": username, "name": username.title(), "description": "Trader. NFA",
            "followers_count": followers, "following_count": 5, "statuses_count": 50, "listed_count": 0,
            "verified": False, "blue_verified": False, "protected": False, "created_at": "2025-01-01"}


class FakeClient:
    """Answers like Scweet: a not-found user is only a debug log line, a silent miss is just absent."""

    def __init__(self, profiles=(), ghosts=(), search=None):
        self.profiles = {p["username"].lower(): p for p in profiles}
        self.by_id = {p["user_id"]: p for p in profiles}
        self.ghosts = {g.lower() for g in ghosts}
        self.search = search or {}
        self.calls = []

    async def aget_user_info(self, users=None, *, user_ids=None, **_):
        self.calls.append(("users", list(users or []), list(user_ids or [])))
        if users:
            rows = []
            for name in users:
                if name.lower() in self.profiles:
                    rows.append(self.profiles[name.lower()])
                elif name.lower() in self.ghosts:
                    logging.getLogger("Scweet.api_engine").debug("Profiles lookup returned no user for username=%s", name)
            return rows
        return [self.by_id[i] for i in user_ids if i in self.by_id]

    async def asearch_users(self, query, *, limit=None, **_):
        self.calls.append(("search", query, limit))
        return list(self.search.get(query, []))[:limit]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "ledger" / "request_ledger.jsonl")
    monkeypatch.setattr(track, "SLEEP", lambda _s: None)
    monkeypatch.setattr(x_batch, "refusal_code", lambda _c: None)


@pytest.fixture
def conn(tmp_path):
    connection = x_growth.connect(tmp_path / "growth.db")
    yield connection
    connection.close()


NOW = x_growth.now_utc()


def test_mentions_are_counted_once_per_post_and_followed_accounts_are_skipped():
    records = [{"mentions": ["Alpha", "alpha", "beta"]}, {"mentions": ["alpha", "mine"]}, {"mentions": []}]
    assert track.mention_counts(records, {"mine"}) == {"alpha": 2, "beta": 1}


def test_usernames_resolve_to_candidates_with_a_first_snapshot_and_honest_states(conn):
    client = FakeClient([_profile(1, "Alpha", 900), _profile(2, "Beta", 400)], ghosts=["Ghost"])
    x_growth.add_pending(conn, {"alpha": 5, "beta": 3, "ghost": 2, "flaky": 1})
    result = track.resolve_usernames(conn, client, ["alpha", "beta", "ghost", "flaky"], NOW, {"alpha": 5})
    assert result == {"resolved": 2, "missing": 1, "retry": 1, "new": 2}
    assert {r["username"] for r in conn.execute("select username from candidates")} == {"Alpha", "Beta"}
    assert conn.execute("select count(*) from snapshots").fetchone()[0] == 2
    states = {r["username"]: r["state"] for r in conn.execute("select username, state from pending")}
    assert states == {"alpha": "done", "beta": "done", "ghost": "missing", "flaky": "retry"}  # silent miss is retried


def test_seed_mentions_respects_the_run_cap_and_leaves_the_rest_queued(conn, monkeypatch):
    monkeypatch.setattr(track, "load_archive", lambda: [{"mentions": [f"user{i}"]} for i in range(6)] * 2)
    monkeypatch.setattr(track, "_tracked_handles", lambda: [])
    client = FakeClient([_profile(i, f"user{i}", 100 * i) for i in range(6)])
    totals = track.seed_mentions(conn, client, track.Budget(7), 2, 60, NOW)   # chunks of 5 and 1: 6 requests
    assert totals["resolved"] == 6 and totals["queued_now"] == 6
    client2 = FakeClient([_profile(i, f"user{i}", 100 * i) for i in range(6, 12)])
    monkeypatch.setattr(track, "load_archive", lambda: [{"mentions": [f"user{i}"]} for i in range(6, 12)] * 2)
    totals = track.seed_mentions(conn, client2, track.Budget(4), 2, 60, NOW)  # cap 4 < one chunk of 5
    assert totals.get("stopped") == "run cap reached" and totals["resolved"] == 0
    assert len(client2.calls) == 0 and x_growth.status(conn)["pending"]["new"] == 6
    assert x_pacing.recent_total() == 6                                       # the ledger saw exactly what was asked


def test_search_seeds_the_pool_and_stops_at_the_cap(conn):
    queries = track.SEARCH_QUERIES
    client = FakeClient(search={queries[0]: [_profile(1, "Alpha", 5000), _profile(2, "Beta", 900)],
                                queries[1]: [_profile(2, "Beta", 900), _profile(3, "Gamma", 100)]})
    totals = track.seed_search(conn, client, track.Budget(4), 2, NOW)          # 2 pages per phrase -> 2 phrases
    assert totals["queries"] == 2 and totals["found"] == 4 and totals["new"] == 3
    assert totals["stopped"] == "run cap reached"
    origins = {r["username"]: r["origin"] for r in conn.execute("select username, origin from candidates")}
    assert origins["Beta"] == f"search:{queries[0]}"                           # first source wins
    assert client.calls[0] == ("search", queries[0], 40)


def test_snapshots_run_once_per_day_in_batches_and_count_missing_accounts(conn):
    people = [_profile(i, f"user{i}", 1000 + i) for i in range(1, 251)]
    for p in people:
        x_growth.upsert_candidate(conn, p, "x", NOW)
    client = FakeClient(people[:-1])                                           # the last account does not come back
    totals = track.take_snapshots(conn, client, track.Budget(10), NOW)
    assert totals == {"mode": "ids", "due": 250, "saved": 249, "gone": 1}
    assert [len(c[2]) for c in client.calls] == [100, 100, 50]                 # one request per 100 ids
    assert conn.execute("select miss_count from candidates where user_id = '250'").fetchone()[0] == 1
    again = FakeClient(people)
    assert track.take_snapshots(conn, again, track.Budget(10), NOW) == {"mode": "ids", "due": 1, "saved": 1, "gone": 0}
    assert track.take_snapshots(conn, again, track.Budget(10), NOW)["due"] == 0   # nothing left today
    assert len(again.calls) == 1


def test_a_recorded_refusal_turns_a_silent_empty_answer_into_a_stop(conn, monkeypatch):
    x_growth.upsert_candidate(conn, _profile(1, "Alpha", 10), "x", NOW)
    monkeypatch.setattr(x_batch, "refusal_code", lambda _c: 429)
    with pytest.raises(XRateLimited):
        track.take_snapshots(conn, FakeClient([]), track.Budget(5), NOW)
    assert conn.execute("select miss_count from candidates").fetchone()[0] == 0   # a refusal says nothing about the account
    assert conn.execute("select count(*) from snapshots").fetchone()[0] == 0


def test_the_budget_refuses_more_than_its_cap():
    budget = track.Budget(3)
    budget.take(2)
    with pytest.raises(track.BudgetSpent):
        budget.take(2)
    assert budget.used == 2 and x_pacing.recent_total() == 2


def test_a_spent_daily_guard_is_reported_plainly_and_nothing_is_recorded_as_gone(conn):
    class Refusing(FakeClient):
        async def aget_user_info(self, users=None, *, user_ids=None, **_):
            logging.getLogger("Scweet.api_engine").warning(
                "Profiles lease unavailable total=1 eligible=0 blocked={'daily_limit': 1} sample=[...]")
            return []

    x_growth.upsert_candidate(conn, _profile(1, "Alpha", 10), "x", NOW)
    with pytest.raises(DailyBudgetSpent, match="resets at UTC midnight"):
        track.take_snapshots(conn, Refusing(), track.Budget(5), NOW)
    assert conn.execute("select miss_count from candidates").fetchone()[0] == 0


class _Args:
    min_mentions = 2
    limit = 5


def test_daily_snapshots_first_then_seeds_and_searches_only_when_due(conn, monkeypatch):
    monkeypatch.setattr(track, "load_archive", lambda: [{"mentions": ["newcomer"]}] * 2)
    monkeypatch.setattr(track, "_tracked_handles", lambda: [])
    monkeypatch.setattr(track, "SEARCH_QUERIES", ["q1", "q2"])
    x_growth.upsert_candidate(conn, _profile(1, "Alpha", 1000), "x", NOW - timedelta(days=1))
    client = FakeClient([_profile(1, "Alpha", 1010), _profile(2, "Newcomer", 50)],
                        search={"q1": [_profile(3, "Found", 400)]})
    out = track.run_daily(conn, client, track.Budget(20), _Args, NOW)
    assert out["snapshot"]["saved"] == 1 and out["mentions"]["resolved"] == 1 and out["search"]["new"] == 1
    assert x_growth.get_meta(conn, "last_search_seed")
    kinds = [c[0] for c in client.calls]
    assert kinds == ["users", "users", "search", "search"]                    # snapshot, mentions, then both phrases
    again = FakeClient([_profile(1, "Alpha", 1020)])
    out2 = track.run_daily(conn, again, track.Budget(20), _Args, NOW)
    assert out2["snapshot"]["due"] == 0 and "mentions" not in out2 and "search" not in out2   # queue empty, search weekly
    assert again.calls == []


class BlockedIds(FakeClient):
    """X answers 403 to the batch id lookup on this connection; username lookups still work."""

    async def aget_user_info(self, users=None, *, user_ids=None, **kw):
        if user_ids:
            self.calls.append(("ids", list(user_ids)))
            raise RuntimeError("X refused the id lookup (HTTP 403), and no rows arrived. Retry, or look up by username.")
        return await super().aget_user_info(users=users, **kw)


def _seeded(conn, ages):
    """Candidates whose newest snapshot is `age` days old (None = never snapshotted)."""
    for i, age in enumerate(ages, start=1):
        x_growth.upsert_candidate(conn, _profile(i, f"user{i}", 1000 * i), "x", NOW - timedelta(days=30))
        if age is not None:
            x_growth.save_snapshot(conn, _profile(i, f"user{i}", 900 * i), NOW - timedelta(days=age))


def test_a_blocked_id_lookup_falls_back_to_usernames_for_stale_accounts_only(conn):
    _seeded(conn, [8, 2, None])                                   # stale, fresh, never seen
    client = BlockedIds([_profile(i, f"user{i}", 1100 * i) for i in (1, 2, 3)])
    totals = track.take_snapshots(conn, client, track.Budget(10), NOW)
    assert totals["mode"] == "usernames" and totals["due"] == 2 and totals["saved"] == 2
    assert totals["id_lookup"].startswith("blocked")
    assert [c[0] for c in client.calls] == ["ids", "users"] and client.calls[1][1] == ["user3", "user1"]  # never seen first, then the oldest
    assert x_growth.get_meta(conn, "id_lookup_blocked_until")
    assert x_pacing.recent_total() == 1 + 2                       # one failed id request, two username requests
    again = BlockedIds([_profile(i, f"user{i}", 1100 * i) for i in (1, 2, 3)])
    second = track.take_snapshots(conn, again, track.Budget(10), NOW)
    assert second["mode"] == "usernames" and second["due"] == 0 and again.calls == []   # does not retry the id lookup


def test_username_refresh_counts_only_a_clear_not_found_against_an_account(conn):
    _seeded(conn, [10, 10, 10, 10])
    client = FakeClient([_profile(1, "user1", 5000), _profile(4, "user4", 7)], ghosts=["user2"])
    client.profiles["user4"] = _profile(99, "user4", 7)           # user4's name now belongs to someone else
    part = track.refresh_usernames(conn, client, [("1", "user1"), ("2", "user2"), ("3", "user3"), ("4", "user4")], NOW)
    assert part == {"saved": 1, "gone": 2, "unanswered": 1}
    misses = {r["user_id"]: r["miss_count"] for r in conn.execute("select user_id, miss_count from candidates")}
    assert misses == {"1": 0, "2": 1, "3": 0, "4": 1}               # silence (user3) says nothing about the account


def test_mode_names_never_touches_the_id_lookup(conn):
    _seeded(conn, [9])
    client = BlockedIds([_profile(1, "user1", 1)])
    totals = track.take_snapshots(conn, client, track.Budget(5), NOW, mode="names")
    assert totals["mode"] == "usernames" and totals["saved"] == 1 and [c[0] for c in client.calls] == ["users"]
