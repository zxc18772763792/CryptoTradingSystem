"""The standalone X archive poller archives live posts, creates no news and steps aside for the news worker."""
from __future__ import annotations

import importlib.util
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from config.settings import settings
from core.news import x_archive, x_pacing
from core.news.collectors import x_accounts
from core.news.collectors.x_accounts import XAccountsCollector

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "poll_x_archive.py"
spec = importlib.util.spec_from_file_location("poll_x_archive", SCRIPT)
poller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(poller)

STAMP = "Sat Oct 24 13:54:50 +0000 2026"


def _tweet(tweet_id, handle):
    return {"tweet_id": str(tweet_id), "user": {"screen_name": handle, "name": handle}, "timestamp": STAMP,
            "text": f"$BTC note {tweet_id}", "likes": 1, "retweets": 0, "comments": 0, "is_retweet": False,
            "is_quote": False, "media": {"image_links": [], "video_links": []}}


class FakeClient:
    def __init__(self, fail=()):
        self.calls, self.fail = [], set(fail)

    async def aget_profile_tweets(self, users, *, limit=None):
        self.calls.append(users[0])
        if users[0] in self.fail:
            raise RuntimeError("boom")
        return [_tweet(i, users[0]) for i in (1, 2)]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(x_archive, "archive_dir", lambda: tmp_path / "archive")
    monkeypatch.setattr(x_archive, "_seen", {})
    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "ledger" / "request_ledger.jsonl")
    monkeypatch.setattr(x_accounts, "_pauses", {})
    monkeypatch.setattr(poller, "NEWS_DB", tmp_path / "news.db")
    monkeypatch.setenv("X_AUTH_TOKEN", "f00dfeedcafe0123456789abcdef0123456789ab")
    monkeypatch.setattr(settings, "X_AUTH_TOKEN", "")


def _with_client(monkeypatch, client):
    monkeypatch.setattr(XAccountsCollector, "_client", lambda self: client)


def test_polling_archives_live_posts_for_every_handle_and_makes_no_news(monkeypatch, tmp_path):
    client = FakeClient()
    _with_client(monkeypatch, client)
    result = poller.poll(["alpha", "beta", "gamma"])
    assert result == {"handles": 3, "failed": {}, "new_posts_archived": 6}
    records = x_archive.load_archive(directory=tmp_path / "archive")
    assert {r["handle"] for r in records} == {"alpha", "beta", "gamma"} and {r["provenance"] for r in records} == {"live"}
    assert poller.poll(["alpha", "beta", "gamma"])["new_posts_archived"] == 0          # a second poll adds nothing
    assert x_pacing.recent_total(path=tmp_path / "ledger" / "request_ledger.jsonl") >= 3


def test_one_failing_handle_does_not_stop_the_others(monkeypatch):
    client = FakeClient(fail={"beta"})
    _with_client(monkeypatch, client)
    result = poller.poll(["alpha", "beta", "gamma"])
    assert set(result["failed"]) == {"beta"} and result["new_posts_archived"] == 4


def test_every_handle_failing_is_a_nonzero_exit(monkeypatch, capsys):
    _with_client(monkeypatch, FakeClient(fail={"alpha", "beta"}))
    monkeypatch.setattr(poller, "load_handles", lambda: ["alpha", "beta"])
    assert poller.main([]) == 1
    assert "All X accounts failed" in capsys.readouterr().err


def _news_db_with_x_state(path, last_success):
    con = sqlite3.connect(path)
    con.execute("create table news_source_state(source text, last_success_at text)")
    con.execute("insert into news_source_state values('x_accounts', ?)", (last_success,))
    con.commit()
    con.close()


def test_it_steps_aside_while_the_news_worker_is_polling_x(monkeypatch, tmp_path, capsys):
    now = datetime.now(timezone.utc)
    _news_db_with_x_state(tmp_path / "news.db", (now - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S.%f"))
    client = FakeClient()
    _with_client(monkeypatch, client)
    monkeypatch.setattr(poller, "load_handles", lambda: ["alpha"])
    assert poller.main([]) == 0 and client.calls == []
    assert "skipped" in capsys.readouterr().out
    assert poller.main(["--ignore-worker"]) == 0 and client.calls == ["alpha"]


def test_a_stale_or_missing_worker_state_does_not_block_polling(tmp_path):
    now = datetime.now(timezone.utc)
    assert poller.worker_polled_recently(now) is None                              # no database at all
    _news_db_with_x_state(tmp_path / "news.db", (now - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S"))
    assert poller.worker_polled_recently(now) is None


def test_dry_run_sends_nothing(monkeypatch, capsys):
    client = FakeClient()
    _with_client(monkeypatch, client)
    monkeypatch.setattr(poller, "load_handles", lambda: ["alpha", "beta"])
    assert poller.main(["--dry-run"]) == 0 and client.calls == []
    assert '"handles": 2' in capsys.readouterr().out


def test_the_shipped_handle_list_covers_the_frozen_research_cohort():
    handles = set(poller.load_handles())
    assert set(x_archive.FROZEN_COHORT) <= handles and len(handles) >= len(x_archive.FROZEN_COHORT)


def test_status_reports_live_posts_per_handle_and_the_workers_last_poll(monkeypatch, tmp_path, capsys):
    now = datetime.now(timezone.utc)
    _news_db_with_x_state(tmp_path / "news.db", (now - timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S"))
    x_archive.observe("alpha", [_tweet(1, "alpha"), _tweet(2, "alpha")], provenance="live", directory=tmp_path / "archive")
    x_archive.observe("beta", [_tweet(3, "beta")], provenance="backfill", directory=tmp_path / "archive")
    monkeypatch.setattr(poller, "load_handles", lambda: ["alpha", "beta", "gamma"])
    assert poller.main(["--status"]) == 0
    out = __import__("json").loads(capsys.readouterr().out)
    assert out["live_posts_total"] == 2 and out["worker_polling_now"] is False      # the worker's last poll was 5 h ago
    assert out["worker_last_x_success_utc"].startswith(str((now - timedelta(hours=5)).date()))
    assert out["per_handle"]["alpha"]["live_posts"] == 2 and out["per_handle"]["beta"]["live_posts"] == 0
    assert out["per_handle"]["gamma"]["newest_post"] is None
