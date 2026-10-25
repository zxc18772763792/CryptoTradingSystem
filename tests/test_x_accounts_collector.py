"""The X account collector reads only allowlisted handles and fails closed."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from config.settings import settings
from core.news.collectors import manager as manager_module
from core.news.collectors import x_accounts
from core.news.collectors.x_accounts import XAccountsCollector, _x_handles
from core.news.eventizer.rules import load_news_rule_config
from core.news.service import worker
from core.news.storage import db as news_db


ROOT = Path(__file__).resolve().parents[1]
TOKEN = "f00dfeedcafe0123456789abcdef0123456789ab"
NOW = datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.setattr(x_accounts, "_pauses", {})
    monkeypatch.setenv("X_AUTH_TOKEN", TOKEN)
    monkeypatch.delenv("X_PROXY", raising=False)
    # Keep the developer's real .env / .env.local out of these tests.
    for name, value in (("X_AUTH_TOKEN", ""), ("X_PROXY", ""), ("NEWS_ENABLE_X_ACCOUNTS", False)):
        monkeypatch.setattr(settings, name, value)


def _cfg(**extra):
    defaults = {"x_accounts_handles": ["Alpha", "beta"], "x_accounts_min_interval_sec": 0.001,
                "x_accounts_jitter_sec": 0.001}
    defaults.update(extra)
    return {"defaults": defaults}


def _stamp(minutes_ago: float = 5) -> str:
    # The shape X returns: "Wed Oct 21 12:34:56 +0000 2026".
    return (NOW - timedelta(minutes=minutes_ago)).strftime("%a %b %d %H:%M:%S +0000 %Y")


def _tweet(tweet_id: int, handle: str = "alpha", text: str = "$BTC looks heavy here", **extra):
    tweet = {
        "tweet_id": str(tweet_id), "user": {"screen_name": handle, "name": handle.title()},
        "timestamp": _stamp(), "text": text, "likes": 7, "retweets": 2, "comments": 1,
        "media": {"image_links": [], "video_links": []}, "lang": "en", "hashtags": [], "mentions": [],
        "urls": [], "is_retweet": False, "is_quote": False, "in_reply_to_user": None,
    }
    tweet.update(extra)
    return tweet


class FakeClient:
    def __init__(self, by_handle=None, errors=None):
        self.by_handle = by_handle or {}
        self.errors = errors or {}
        self.calls = []

    async def aget_profile_tweets(self, users, *, limit=None):
        self.calls.append((tuple(users), limit))
        handle = users[0]
        if handle in self.errors:
            raise self.errors[handle]
        return list(self.by_handle.get(handle, []))


class FlakyClient(FakeClient):
    """Scweet answers a failed request with an empty list and a log warning instead of raising."""

    def __init__(self, by_handle=None, fail_times=0, detail="status=599 snippet=curl: (35) connection closed"):
        super().__init__(by_handle)
        self.fail_times = fail_times
        self.detail = detail

    async def aget_profile_tweets(self, users, *, limit=None):
        if self.fail_times > 0:
            self.fail_times -= 1
            self.calls.append((tuple(users), limit))
            logging.getLogger("Scweet.api_engine").warning("Profile user lookup failed username=%s %s", users[0], self.detail)
            return []
        return await super().aget_profile_tweets(users, limit=limit)


def _collector(client, **extra):
    collector = XAccountsCollector(_cfg(**extra))
    collector._client = lambda: client
    return collector


def test_handles_are_explicit_names_only():
    assert _x_handles(["@Alpha", "alpha", " beta "]) == ["alpha", "beta"]
    assert _x_handles("one, two") == ["one", "two"]
    for bad in ([], None, ["https://x.com/alpha"], ["a b"], ["toolongtoolonghandle"], [f"h{i}" for i in range(21)]):
        with pytest.raises(ValueError):
            _x_handles(bad)


def test_constructor_requires_a_token_and_handles(monkeypatch):
    monkeypatch.delenv("X_AUTH_TOKEN")
    with pytest.raises(ValueError, match="X_AUTH_TOKEN"):
        XAccountsCollector(_cfg())
    monkeypatch.setenv("X_AUTH_TOKEN", TOKEN)
    with pytest.raises(ValueError, match="x_accounts_handles"):
        XAccountsCollector(_cfg(x_accounts_handles=[]))


def test_values_kept_in_env_files_reach_the_collector(monkeypatch):
    # settings reads .env.local without exporting it, so the process environment alone is not enough.
    monkeypatch.delenv("X_AUTH_TOKEN")
    monkeypatch.setattr(settings, "X_AUTH_TOKEN", TOKEN)
    monkeypatch.setattr(settings, "NEWS_ENABLE_X_ACCOUNTS", True)
    assert x_accounts.x_auth_token() == TOKEN and x_accounts.x_accounts_enabled()
    assert XAccountsCollector(_cfg())._token == TOKEN
    monkeypatch.setenv("NEWS_ENABLE_X_ACCOUNTS", "false")  # an explicit environment value wins
    assert not x_accounts.x_accounts_enabled()


def test_only_the_handles_own_posts_become_items():
    quote = _tweet(6, is_quote=True, embedded_text="quoted &amp; text", text="my take")
    tweets = [
        _tweet(1, text="Fed day: $BTC and $eth &amp; BTCUSDT, $100 is not a coin"),
        _tweet(2, is_retweet=True, text="RT @other: not their words"),
        _tweet(3, in_reply_to_user="stranger", text="@stranger thanks"),
        _tweet(4, handle="someoneelse"),
        _tweet(5, text=""),
        quote,
        _tweet(7, timestamp="Wed Oct 21 12:34:56 2026"),  # no timezone: never invent a date
        _tweet(8, in_reply_to_user="alpha", text="thread part two"),
        _tweet(1, text="duplicate id"),
    ]
    collector = _collector(FakeClient({"alpha": tweets}))
    items = collector._from_tweets("alpha", tweets)
    assert [item["source_id"] for item in items] == ["x:alpha:1", "x:alpha:6", "x:alpha:8"]
    first = items[0]
    assert first["url"] == "https://x.com/alpha/status/1"
    assert first["source"] == "x_accounts"
    assert first["content"].startswith("Fed day: $BTC and $eth & BTCUSDT")
    assert first["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert first["published_at"] == (NOW - timedelta(minutes=5)).isoformat()
    assert first["payload"]["source_kind"] == "third_party_commentary"
    assert first["payload"]["likes"] == 7 and first["payload"]["replies"] == 1
    assert items[1]["payload"]["quoted_text"] == "quoted & text"
    assert items[1]["payload"]["is_quote"] is True


def test_pull_latest_filters_age_sorts_and_isolates_a_failed_handle():
    client = FakeClient(
        {"alpha": [_tweet(1, timestamp=_stamp(10)), _tweet(2, timestamp=_stamp(600)), _tweet(3, timestamp=_stamp(2))]},
        errors={"beta": RuntimeError(f"boom with {TOKEN} inside")},
    )
    collector = _collector(client)
    items = collector.pull_latest(since_minutes=120, max_records=2)
    assert [item["payload"]["tweet_id"] for item in items] == ["3", "1"]
    assert client.calls == [(("alpha",), 20), (("beta",), 20)]
    assert set(collector.last_account_errors) == {"beta"}
    assert TOKEN not in collector.last_account_errors["beta"]


def test_every_handle_failing_raises_without_leaking_the_token():
    errors = {"alpha": RuntimeError(f"a {TOKEN}"), "beta": RuntimeError("b")}
    collector = _collector(FakeClient(errors=errors))
    with pytest.raises(RuntimeError, match="All X accounts failed") as caught:
        collector.pull_latest()
    assert TOKEN not in str(caught.value)


def test_rejected_login_stops_asking_and_pauses_later_polls():
    scweet_exceptions = pytest.importorskip("Scweet.exceptions")
    client = FakeClient(errors={"alpha": scweet_exceptions.AuthError("401 from X")})
    collector = _collector(client)
    with pytest.raises(RuntimeError, match="All X accounts failed"):
        collector.pull_latest()
    assert client.calls == [(("alpha",), 20)]  # beta was never asked: X refuses the session itself
    follow_up = _collector(client)
    with pytest.raises(RuntimeError, match="paused for another"):
        follow_up.pull_latest()
    assert len(client.calls) == 1  # a paused poll sends nothing to X


def test_rate_limit_pauses_but_keeps_items_already_read():
    scweet_exceptions = pytest.importorskip("Scweet.exceptions")
    client = FakeClient({"alpha": [_tweet(1)]}, errors={"beta": scweet_exceptions.RateLimitError("429")})
    collector = _collector(client)
    items = collector.pull_latest()
    assert [item["payload"]["tweet_id"] for item in items] == ["1"]
    with pytest.raises(RuntimeError, match="paused for another"):
        _collector(client).pull_latest()


def test_a_dropped_connection_is_retried(monkeypatch):
    monkeypatch.setattr(x_accounts.time, "sleep", lambda _seconds: None)
    for detail in ("status=599 snippet=closed", "Transaction-id bootstrap exception: curl: (35) closed abruptly"):
        client = FlakyClient({"alpha": [_tweet(1)]}, fail_times=2, detail=detail)
        collector = _collector(client, x_accounts_handles=["alpha"])
        assert [item["payload"]["tweet_id"] for item in collector.pull_latest()] == ["1"]
        assert len(client.calls) == 3 and collector.last_account_errors == {}


def test_an_empty_answer_with_warnings_is_a_failure_not_a_quiet_account(monkeypatch):
    monkeypatch.setattr(x_accounts.time, "sleep", lambda _seconds: None)
    client = FlakyClient({"beta": [_tweet(2, handle="beta")]}, fail_times=3, detail=f"status=599 token={TOKEN}")
    collector = _collector(client)
    items = collector.pull_latest()
    assert [item["payload"]["handle"] for item in items] == ["beta"]
    assert set(collector.last_account_errors) == {"alpha"}
    assert "X request failed" in collector.last_account_errors["alpha"]
    assert TOKEN not in collector.last_account_errors["alpha"]
    assert len(client.calls) == 4  # alpha three times (two retries), then beta
    with pytest.raises(RuntimeError, match="All X accounts failed"):
        _collector(FlakyClient(fail_times=4, detail="status=403 forbidden")).pull_latest()


def test_a_quiet_account_without_warnings_is_not_an_error():
    collector = _collector(FakeClient())
    assert collector.pull_latest() == [] and collector.last_account_errors == {}


def test_async_scweet_call_runs_with_or_without_a_live_loop():
    async def work():
        return "done"

    async def inside_loop():
        return x_accounts._run_async(work)

    assert x_accounts._run_async(work) == "done"
    assert asyncio.run(inside_loop()) == "done"


def test_manager_keeps_x_off_until_enabled_configured_and_credentialed(monkeypatch):
    cfg = load_news_rule_config(ROOT / "config/news_rules.yaml", ROOT / "config/symbols.yaml")
    assert "x_accounts" in cfg["defaults"]["news_sources"]
    assert worker._source_interval("x_accounts") == 7200
    assert news_db.SOURCE_IMPORTANCE["x_accounts"] < news_db.SOURCE_IMPORTANCE["telegram_alastar"]

    def build(config, **env):
        for name in ("NEWS_ENABLE_X_ACCOUNTS", "X_AUTH_TOKEN"):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        manager = manager_module.MultiSourceNewsCollector(config)
        specs, errors = manager._build_collectors(source_names=["x_accounts"])
        manager._close_collectors(specs)
        return [spec.name for spec in specs], errors

    with_handles = {"defaults": {**cfg["defaults"], "x_accounts_handles": ["alpha"],
                                 "x_accounts_archive_only_handles": []}}
    names, errors = build(with_handles)
    assert names == [] and any("NEWS_ENABLE_X_ACCOUNTS is not true" in e for e in errors)
    names, errors = build(with_handles, NEWS_ENABLE_X_ACCOUNTS="true")
    assert names == [] and any("X_AUTH_TOKEN missing" in e for e in errors)
    names, errors = build(with_handles, NEWS_ENABLE_X_ACCOUNTS="true", X_AUTH_TOKEN=TOKEN)
    assert names == ["x_accounts"] and errors == []
    assert _x_handles(cfg["defaults"]["x_accounts_handles"])  # the shipped allowlist is valid
    no_handles = {"defaults": {**cfg["defaults"], "x_accounts_handles": []}}
    names, errors = build(no_handles, NEWS_ENABLE_X_ACCOUNTS="true", X_AUTH_TOKEN=TOKEN)
    assert names == [] and any("x_accounts disabled" in e and "x_accounts_handles" in e for e in errors)
    assert all(TOKEN not in e for e in errors)


def test_all_source_pulls_skip_disabled_x_quietly(monkeypatch):
    monkeypatch.delenv("NEWS_ENABLE_X_ACCOUNTS", raising=False)
    cfg = load_news_rule_config(ROOT / "config/news_rules.yaml", ROOT / "config/symbols.yaml")
    manager = manager_module.MultiSourceNewsCollector(cfg)
    specs, errors = manager._build_collectors()
    try:
        assert "x_accounts" in manager.sources
        assert "x_accounts" not in [spec.name for spec in specs]
        assert not any("x_accounts" in e for e in errors)
    finally:
        manager._close_collectors(specs)


def test_a_swallowed_429_is_a_rate_limit_not_a_quiet_account(monkeypatch):
    monkeypatch.setattr(x_accounts, "refusal_code", lambda _client: 429)
    collector = _collector(FakeClient())
    with pytest.raises(RuntimeError, match="All X accounts failed"):
        collector.pull_latest()
    assert list(collector.last_account_errors) == ["alpha"]  # the same answer awaits the rest: stop asking
    assert "429" in collector.last_account_errors["alpha"]
    with pytest.raises(RuntimeError, match="paused for another"):
        _collector(FakeClient()).pull_latest()


def test_an_empty_answer_with_no_refusal_on_record_stays_quiet(monkeypatch):
    monkeypatch.setattr(x_accounts, "refusal_code", lambda _client: None)
    assert _collector(FakeClient()).pull_latest() == []


def _archived(tmp_path, monkeypatch):
    from core.news import x_archive

    monkeypatch.setattr(x_archive, "archive_dir", lambda: tmp_path)
    monkeypatch.setattr(x_archive, "_seen", {})
    return x_archive


def test_archive_keeps_everything_a_poll_returns_and_archive_only_handles_make_no_news(tmp_path, monkeypatch):
    archive = _archived(tmp_path, monkeypatch)
    alpha = [_tweet(1), _tweet(2, is_retweet=True, text="RT @x: no"), _tweet(3, in_reply_to_user="stranger")]
    client = FakeClient({"alpha": alpha, "beta": [_tweet(4, handle="beta")]})
    collector = _collector(client, x_accounts_archive=True, x_accounts_archive_only_handles=["beta"])
    items = collector.pull_latest()
    assert [item["payload"]["tweet_id"] for item in items] == ["1"]  # news: alpha's own post only
    stored = {(r["handle"], r["tweet_id"]): r for r in archive.load_archive(directory=tmp_path)}
    assert set(stored) == {("alpha", "1"), ("alpha", "2"), ("alpha", "3"), ("beta", "4")}
    assert stored[("alpha", "2")]["is_retweet"] and stored[("beta", "4")]["provenance"] == "live"
    collector.pull_latest()  # a second poll adds nothing
    assert len(archive.load_archive(directory=tmp_path)) == 4


def test_archive_failure_does_not_stop_news(tmp_path, monkeypatch):
    archive = _archived(tmp_path, monkeypatch)
    monkeypatch.setattr(archive, "observe", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    items = _collector(FakeClient({"alpha": [_tweet(1)]}), x_accounts_archive=True).pull_latest()
    assert [item["payload"]["tweet_id"] for item in items] == ["1"]


def test_archive_only_handles_must_be_allowlisted_and_archived():
    with pytest.raises(ValueError, match="archive_only"):
        XAccountsCollector(_cfg(x_accounts_archive=True, x_accounts_archive_only_handles=["stranger"]))
    with pytest.raises(ValueError, match="archive_only"):
        XAccountsCollector(_cfg(x_accounts_archive_only_handles=["alpha"]))
