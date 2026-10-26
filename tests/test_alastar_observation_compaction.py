"""Packing old duplicate-only archive polls into gzip bundles: replay, verification and reader invariants."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import json

import pytest

from core.news import alastar_observation as archive

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
POLLS = 60
NEW_AT = {3, 4, 20, 41}           # polls that receive a new message (their runs carry new_revisions)


def record(message_id, published):
    text = f"#BTC note {message_id}"
    return {"source_id": f"telegram:alastartrades:{message_id}", "url": f"https://t.me/alastartrades/{message_id}",
            "content": text, "published_at": published.isoformat(),
            "payload": {"message_id": message_id, "channel": "alastartrades", "original_text": text}}


def build(path, polls=POLLS):
    """One live poll per hour; every page shows all messages so far, so most polls see nothing new."""
    seen = []
    for i in range(polls):
        at = START + timedelta(hours=i)
        if i in NEW_AT:
            seen.append(record(1000 + i, at - timedelta(seconds=30)))
        archive.observe_records(list(seen), path, mode="live_public_preview", observed_at=at,
                                raw_preview=f"<html>{len(seen)}</html>".encode())
    return START + timedelta(hours=polls - 1)


def compact(path, **kwargs):
    options = {"now": START + timedelta(hours=POLLS + 1), "keep_recent": 10, "min_batch": 5, "max_batch": 100}
    return archive.compact_runs(path, **{**options, **kwargs})


def files(path):
    return sorted(int(p.stem) for p in (path / "runs").glob("*.json"))


def test_old_duplicate_only_runs_are_packed_and_everything_still_replays(tmp_path):
    build(tmp_path)
    before = archive.verify_archive(tmp_path)
    latency_before = archive.receipt_latency(tmp_path, now=START + timedelta(hours=POLLS))
    messages_before = json.loads((tmp_path / "state.json").read_bytes())["messages"]
    result = compact(tmp_path)
    # Runs 1..50 are outside the newest 10; "now" is hour 61, so polls at hours 0..13 (runs 1..14) are at
    # least 48 h old. Those are scanned, and the two that received messages (runs 4 and 5) stay files.
    assert result["scanned"] == 14 and result["kept_with_revisions"] == 2 and result["packed"] == 12
    assert files(tmp_path) == [4, 5] + list(range(15, POLLS + 1))
    after = archive.verify_archive(tmp_path)
    assert after["verified_full_chain"] is True and after["run_count"] == before["run_count"] == POLLS
    assert after["last_run_sha256"] == before["last_run_sha256"]
    assert json.loads((tmp_path / "state.json").read_bytes())["messages"] == messages_before
    latency_after = archive.receipt_latency(tmp_path, now=START + timedelta(hours=POLLS))
    assert latency_after["live_run_count"] == latency_before["live_run_count"] == POLLS
    assert latency_after["poll_wait_all_gaps"] == latency_before["poll_wait_all_gaps"]
    manifest = json.loads((tmp_path / "run_bundles" / "manifest.json").read_bytes())
    assert manifest["packed_through"] == 14 and manifest["bundles"][0]["files_removed"] is True
    # New polls keep appending after compaction, and the checkpoint path still works.
    archive.observe_records([record(5000, START + timedelta(hours=POLLS))], tmp_path, mode="live_public_preview",
                            observed_at=START + timedelta(hours=POLLS, minutes=1), raw_preview=b"<html>n</html>")
    assert archive.verify_archive(tmp_path)["run_count"] == POLLS + 1


def test_runs_ledgers_open_by_path_are_never_packed(tmp_path):
    build(tmp_path)
    while not compact(tmp_path, now=START + timedelta(days=30)).get("reason"):
        pass
    remaining = set(files(tmp_path))
    count = json.loads((tmp_path / "state.json").read_bytes())["run_count"]
    assert set(range(count - 9, count + 1)) <= remaining                    # newest runs (health, receipt walk-back)
    assert {n + 1 for n in NEW_AT} <= remaining                              # every run with new_revisions
    for key in json.loads((tmp_path / "state.json").read_bytes())["messages"]:
        # The Alastar ledgers find a first receipt by scanning runs/*.json in name order.
        hit = next(p for p in sorted((tmp_path / "runs").glob("*.json"))
                   if any(x["message_key"] == key for x in json.loads(p.read_bytes())["new_revisions"]))
        assert hit.exists()
    assert archive.verify_archive(tmp_path)["verified_full_chain"] is True


def test_recent_runs_stay_files_and_dry_run_writes_nothing(tmp_path):
    build(tmp_path)
    plan = compact(tmp_path, dry_run=True)
    assert plan["would_pack_through"] == 14 and not (tmp_path / "run_bundles").exists()
    assert files(tmp_path) == list(range(1, POLLS + 1))
    young = compact(tmp_path, now=START + timedelta(hours=POLLS + 1), min_age=timedelta(days=30))
    assert young["packed"] == 0 and young["reason"] and files(tmp_path) == list(range(1, POLLS + 1))


def test_an_interrupted_deletion_is_accepted_by_replay_and_finished_next_time(tmp_path):
    build(tmp_path)
    compact(tmp_path)
    manifest_path = tmp_path / "run_bundles" / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    bundle = tmp_path / manifest["bundles"][0]["file"]
    number, raw = next(archive._bundle_runs(bundle))
    (tmp_path / "runs" / f"{number:012d}.json").write_bytes(raw)            # left behind by a crash
    manifest["bundles"][0]["files_removed"] = False
    manifest_path.write_bytes(archive._json_bytes(manifest))
    assert archive.verify_archive(tmp_path)["verified_full_chain"] is True   # identical duplicate is fine
    compact(tmp_path)
    assert not (tmp_path / "runs" / f"{number:012d}.json").exists()
    (tmp_path / "runs" / f"{number:012d}.json").write_bytes(raw.replace(b"live_public_preview", b"historical_import"))
    with pytest.raises(RuntimeError, match="differs"):
        archive.verify_archive(tmp_path)


def test_a_tampered_bundle_breaks_the_chain(tmp_path):
    build(tmp_path)
    compact(tmp_path)
    bundle = next((tmp_path / "run_bundles").glob("*.jsonl.gz"))
    lines = gzip.decompress(bundle.read_bytes()).decode("utf-8").splitlines()
    entry = json.loads(lines[2])
    entry["raw"] = entry["raw"].replace("live_public_preview", "historical_import")
    entry["sha256"] = archive._digest(entry["raw"].encode("utf-8"))             # even with a consistent line hash
    lines[2] = json.dumps(entry, ensure_ascii=False, sort_keys=True)
    bundle.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
    with pytest.raises(RuntimeError, match="chain"):
        archive.verify_archive(tmp_path)
    entry["sha256"] = "0" * 64
    lines[2] = json.dumps(entry, ensure_ascii=False, sort_keys=True)
    bundle.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
    with pytest.raises(RuntimeError, match="bundle is invalid"):
        archive.verify_archive(tmp_path)


def test_the_collector_compacts_only_when_configured(monkeypatch):
    from core.news.collectors.telegram import TelegramPublicChannelCollector
    from tests.test_telegram_collector import _message, _response

    calls = []
    monkeypatch.setattr(archive, "observe_records", lambda records, **kwargs: None)
    monkeypatch.setattr(archive, "compact_runs", lambda output, **kwargs: calls.append((output, kwargs["channel"])))
    now = datetime.now(timezone.utc).isoformat()
    for enabled in (False, True):
        collector = TelegramPublicChannelCollector({"defaults": {
            "telegram_alastar_channels": ["alastartrades"], "telegram_observation_archive_channels": ["alastartrades"],
            "telegram_alastar_observation_archive": True, "telegram_observation_compaction": enabled}})
        monkeypatch.setattr(collector, "_request", lambda endpoint, *a, **k: _response(_message(date=now), url=endpoint))
        collector.pull_latest()
        collector.close()
    assert calls == [(archive.DEFAULT_OUTPUT, "alastartrades")]
