"""Failures at archive commit, pagination and shared request-budget boundaries."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.news import alastar_observation as observations
from core.news import x_pacing
from core.research import coinglass_snapshot_archive as snapshots

NOW = datetime(2026, 10, 28, 8, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad_payload", [None, {}, {"code": "0", "data": {}},
                                        {"code": "0", "data": [{"symbol": "C"}, None]}])
def test_malformed_later_page_never_commits_a_partial_snapshot(tmp_path, monkeypatch, bad_payload):
    monkeypatch.setattr(snapshots, "PER_PAGE", 2)
    monkeypatch.setattr(snapshots, "PAGE_SPACING_SEC", 0.)

    class Client:
        async def request_json(self, path, params):
            return {"payload": {"code": "0", "data": [{"symbol": "A"}, {"symbol": "B"}]}
                    if params["page"] == 1 else bad_payload}

    with pytest.raises(ValueError):
        asyncio.run(snapshots.tick(NOW, client=Client(), root=tmp_path))
    assert not snapshots.snapshot_path(NOW.replace(hour=6), tmp_path).exists()


@pytest.mark.parametrize("repeated", [False, True])
def test_incomplete_or_repeated_pagination_is_not_a_complete_snapshot(tmp_path, monkeypatch, repeated):
    monkeypatch.setattr(snapshots, "PER_PAGE", 2)
    monkeypatch.setattr(snapshots, "MAX_PAGES", 2)
    monkeypatch.setattr(snapshots, "PAGE_SPACING_SEC", 0.)

    class Client:
        async def request_json(self, path, params):
            prefix = "same" if repeated else str(params["page"])
            return {"payload": {"code": "0", "data": [{"symbol": prefix + "A"}, {"symbol": prefix + "B"}]}}

    with pytest.raises(ValueError):
        asyncio.run(snapshots.tick(NOW, client=Client(), root=tmp_path))
    assert not snapshots.snapshot_path(NOW.replace(hour=6), tmp_path).exists()


def test_missing_liquidation_map_stays_retryable(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "PAGE_SPACING_SEC", 0.)
    calls = []

    class Client:
        invalid = True

        async def request_json(self, path, params):
            coin = params["symbol"]
            calls.append(coin)
            return {"payload": {"code": "0", "data": None if coin == "B" and self.invalid else {"last_price": 1., "data": []}}}

    client = Client()
    with pytest.raises(ValueError):
        asyncio.run(snapshots.tick_liq_maps(NOW, client=client, root=tmp_path, bases=["A", "B"]))
    partial = tmp_path / "2026-10-28/liq_map_7d.partial.json"
    assert set(json.loads(partial.read_text())["maps"]) == {"A"}
    client.invalid = False
    result = asyncio.run(snapshots.tick_liq_maps(NOW, client=client, root=tmp_path, bases=["A", "B"]))
    assert result["status"] == "captured"
    assert calls == ["A", "B", "B"]


def test_net_position_progress_tracks_members_not_only_their_count(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "PAGE_SPACING_SEC", 0.)
    calls = []
    day_ms = int((NOW.replace(hour=0) - timedelta(days=1)).timestamp() * 1000)

    class Client:
        async def request_json(self, path, params):
            calls.append(params["symbol"])
            return {"payload": {"code": "0", "data": [{"time": day_ms, "net_long_change": 1., "net_short_change": -1.}]}}

    client = Client()
    for coins in [[("A", "AUSDT"), ("B", "BUSDT")], [("B", "BUSDT"), ("C", "CUSDT")]]:
        asyncio.run(snapshots.tick_net_positions(NOW, client=client, root=tmp_path,
                                               seed_dir=tmp_path / "seed", coins=coins))
    assert calls == ["AUSDT", "BUSDT", "CUSDT"]


@pytest.mark.parametrize("channel", ["alastartrades", "cryptoquant_alert"])
def test_uncommitted_compaction_bundle_does_not_break_recovery(tmp_path, monkeypatch, channel):
    for n in range(4):
        observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=n),
                                     raw_preview=b"synthetic preview", channel=channel)
    original = observations._atomic_bytes

    def interrupted_manifest(path, raw):
        if path == tmp_path / "run_bundles/manifest.json":
            raise OSError("synthetic crash before manifest commit")
        return original(path, raw)

    options = dict(now=NOW + timedelta(days=3), keep_recent=1, min_batch=1, min_age=timedelta(0), channel=channel)
    with monkeypatch.context() as patch:
        patch.setattr(observations, "_atomic_bytes", interrupted_manifest)
        with pytest.raises(OSError, match="manifest commit"):
            observations.compact_runs(tmp_path, max_batch=2, **options)
    observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=4),
                                 raw_preview=b"synthetic preview", channel=channel)
    observations.compact_runs(tmp_path, max_batch=3, **options)
    assert observations.verify_archive(tmp_path, channel=channel)["run_count"] == 5


def test_compaction_cannot_delete_its_checkpoint_tail(tmp_path):
    for n in range(3):
        observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=n),
                                     raw_preview=b"synthetic preview")
    with pytest.raises(ValueError):
        observations.compact_runs(tmp_path, now=NOW + timedelta(days=3), keep_recent=0,
                                  min_age=timedelta(0), min_batch=1)
    assert observations.verify_archive(tmp_path)["run_count"] == 3


def test_compaction_repairs_a_lagging_checkpoint_before_removing_its_tail(tmp_path, monkeypatch):
    for n in range(4):
        observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=n),
                                     raw_preview=b"synthetic preview")
    original = observations._atomic_bytes

    def failed_projection(path, raw):
        if path == tmp_path / "state.json":
            raise OSError("synthetic crash after raw run commit")
        return original(path, raw)

    with monkeypatch.context() as patch:
        patch.setattr(observations, "_atomic_bytes", failed_projection)
        with pytest.raises(OSError, match="raw run commit"):
            observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=4),
                                         raw_preview=b"synthetic preview")
    observations.compact_runs(tmp_path, now=NOW + timedelta(days=3), keep_recent=1,
                              min_age=timedelta(0), min_batch=1)
    observations.observe_records([], tmp_path, mode="live_public_preview", observed_at=NOW + timedelta(minutes=5),
                                 raw_preview=b"synthetic preview")
    assert observations.verify_archive(tmp_path)["run_count"] == 6


def test_batch_budget_cannot_be_overbooked_between_check_and_reservation(tmp_path, monkeypatch):
    from scripts.track_x_growth import Budget

    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "requests.jsonl")
    x_pacing.record(39, "existing")
    original_wait = x_pacing.wait_for_room

    def competing_job(*args, **kwargs):
        result = original_wait(*args, **kwargs)
        x_pacing.record(1, "live")
        return result

    monkeypatch.setattr(x_pacing, "wait_for_room", competing_job)
    Budget(1).take(1)
    assert x_pacing.recent_total() <= x_pacing.WINDOW_MAX_REQUESTS


def test_live_poll_respects_an_already_full_shared_request_window(tmp_path, monkeypatch):
    from core.news.collectors.x_accounts import XAccountsCollector

    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "requests.jsonl")
    monkeypatch.setenv("X_AUTH_TOKEN", "audit-synthetic-token")
    x_pacing.record(40, "backfill")
    client = SimpleNamespace(aget_profile_tweets=AsyncMock(return_value=[{"tweet_id": "1"}]))
    collector = XAccountsCollector({"defaults": {"x_accounts_handles": ["alpha"]}})
    try:
        with pytest.raises(RuntimeError, match="window"):
            collector._fetch(client, "alpha")
        client.aget_profile_tweets.assert_not_awaited()
    finally:
        collector.close()


def test_two_simultaneous_captures_keep_the_first_committed_snapshot(tmp_path):
    async def run():
        barrier = asyncio.Barrier(2)

        class Client:
            def __init__(self, marker):
                self.marker = marker

            async def request_json(self, path, params):
                await barrier.wait()
                return {"payload": {"code": "0", "data": [{"symbol": "A", "marker": self.marker}]}}

        return await asyncio.gather(*(snapshots.tick(NOW, client=Client(n), root=tmp_path) for n in range(2)))

    results = asyncio.run(run())
    assert sorted(row["status"] for row in results) == ["captured", "done"]
    winner = next(n for n, row in enumerate(results) if row["status"] == "captured")
    assert next(snapshots.iter_snapshots(tmp_path))[2][0]["marker"] == winner


@pytest.mark.parametrize("invalid", ["missing", "nonfinite", "conflicting_duplicate"])
def test_invalid_net_position_response_never_completes_the_coin(tmp_path, invalid):
    stamp = int((NOW.replace(hour=0) - timedelta(days=1)).timestamp() * 1000)
    row = {"time": stamp, "net_long_change": 1., "net_short_change": -1.}
    data = None if invalid == "missing" else [{**row, "net_long_change": "NaN"}] if invalid == "nonfinite" else [row, {**row, "net_long_change": 5.}]
    client = SimpleNamespace(request_json=AsyncMock(return_value={"payload": {"code": "0", "data": data}}))
    with pytest.raises(ValueError):
        asyncio.run(snapshots.tick_net_positions(NOW, client=client, root=tmp_path,
                                               seed_dir=tmp_path / "seed", coins=[("A", "AUSDT")]))
    assert json.loads((tmp_path / "_status.json").read_text())["done"] == []
    assert not (tmp_path / "AUSDT.json").exists()


def test_failed_progress_replace_preserves_the_previous_valid_maps(tmp_path, monkeypatch):
    partial = tmp_path / "2026-10-28/liq_map_7d.partial.json"
    partial.parent.mkdir()
    original = {"day": "2026-10-28", "maps": {"A": {"captured_at": NOW.isoformat(), "data": {"last_price": 1}}}}
    partial.write_text(json.dumps(original))
    replace = snapshots.os.replace

    def fail_progress(source, target):
        if Path(target) == partial:
            raise OSError("synthetic interrupted progress commit")
        return replace(source, target)

    monkeypatch.setattr(snapshots.os, "replace", fail_progress)
    client = SimpleNamespace(request_json=AsyncMock(return_value={"payload": {"code": "0", "data": {"last_price": 2}}}))
    with pytest.raises(OSError, match="progress commit"):
        asyncio.run(snapshots.tick_liq_maps(NOW, client=client, root=tmp_path, bases=["A", "B"]))
    assert json.loads(partial.read_text()) == original
    assert not snapshots.liq_map_path(NOW, tmp_path).exists()


@pytest.mark.parametrize("needed", [0, -1, 41])
def test_impossible_window_reservations_are_rejected(tmp_path, needed):
    with pytest.raises(ValueError):
        x_pacing.seconds_to_wait(needed, path=tmp_path / "requests.jsonl", now=1000.)


def test_two_processes_share_the_same_hard_request_cap(tmp_path):
    ledger = tmp_path / "requests.jsonl"
    source = """
import sys
from pathlib import Path
from core.news.x_pacing import try_reserve
accepted = sum(try_reserve(1, 'audit-process', path=Path(sys.argv[1]), now=1000.) == 0 for _ in range(50))
print(accepted)
"""
    processes = [subprocess.Popen([sys.executable, "-c", source, str(ledger)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    counts = []
    try:
        for process in processes:
            output, error = process.communicate(timeout=20)
            assert process.returncode == 0, error.decode("utf-8", "replace")
            counts.append(int(output))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    assert sum(counts) == 40
    assert x_pacing.recent_total(path=ledger, now=1000.) == 40


def test_failed_live_attempts_and_retries_are_reserved_too(tmp_path, monkeypatch):
    from core.news.collectors import x_accounts

    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "requests.jsonl")
    monkeypatch.setenv("X_AUTH_TOKEN", "audit-synthetic-token")
    monkeypatch.setattr(x_accounts.time, "sleep", lambda _: None)
    attempts = []

    async def fetch(*args, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            logging.getLogger("Scweet").warning("curl: synthetic transient failure")
            return []
        return [{"tweet_id": "1"}]

    collector = x_accounts.XAccountsCollector({"defaults": {"x_accounts_handles": ["alpha"]}})
    try:
        assert collector._fetch(SimpleNamespace(aget_profile_tweets=fetch), "alpha")
        assert len(attempts) == 3
        assert x_pacing.recent_total() == 3
    finally:
        collector.close()


def test_growth_retry_stops_before_exceeding_its_run_cap(tmp_path, monkeypatch):
    from core.news import x_growth
    from scripts import track_x_growth as track

    monkeypatch.setattr(x_pacing, "LEDGER", tmp_path / "requests.jsonl")
    monkeypatch.setattr(track, "SEARCH_QUERIES", ["synthetic query"])
    calls = []

    async def search(*args, **kwargs):
        calls.append(1)
        logging.getLogger("Scweet").warning("curl: synthetic transient failure")
        return []

    conn = x_growth.connect(tmp_path / "growth.db")
    budget = track.Budget(1)
    try:
        result = track.seed_search(conn, SimpleNamespace(asearch_users=search), budget, 1, NOW)
    finally:
        conn.close()
    assert result["stopped"] == "run cap reached"
    assert calls == [1] and budget.used == 1
    assert x_pacing.recent_total() == 1


def test_snapshot_expired_during_fetch_is_not_finalized(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "SLOT_GRACE", timedelta(milliseconds=10))

    async def slow_response(*args, **kwargs):
        await asyncio.sleep(.03)
        return {"payload": {"code": "0", "data": [{"symbol": "A"}]}}

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(snapshots.tick(NOW.replace(hour=6), client=SimpleNamespace(request_json=slow_response), root=tmp_path))
    assert not snapshots.snapshot_path(NOW.replace(hour=6), tmp_path).exists()


@pytest.mark.parametrize("reason", ["timeout", "cancelled"])
def test_forward_cycle_cleanup_drains_child_output(tmp_path, monkeypatch, reason):
    from core.research import telegram_forward_runner as runner

    class Process:
        returncode = None
        killed = False
        drained = False

        async def communicate(self):
            if not self.killed:
                raise asyncio.TimeoutError() if reason == "timeout" else asyncio.CancelledError()
            self.drained = True
            self.returncode = -9
            return b"", None

        def kill(self):
            self.killed = True

        async def wait(self):
            return -9

    process = Process()
    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", AsyncMock(return_value=process))

    async def run():
        try:
            result = await runner.run_cycle(tmp_path)
            assert reason == "timeout" and result["outcome"] == "timeout"
        except asyncio.CancelledError:
            assert reason == "cancelled"

    asyncio.run(run())
    assert process.killed and process.drained
