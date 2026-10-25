"""The shared X request ledger keeps every job under one 15-minute window."""
from __future__ import annotations

import json

import pytest

from core.news import x_pacing as pacing


def test_empty_ledger_never_waits(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    assert pacing.seconds_to_wait(26, path=ledger, now=10_000.0) == 0.0
    assert pacing.recent_total(path=ledger, now=10_000.0) == 0


def test_window_arithmetic_matches_the_cap(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    now = 10_000.0
    pacing.record(26, "backfill", path=ledger, now=now - 100)
    assert pacing.recent_total(path=ledger, now=now) == 26
    assert pacing.seconds_to_wait(14, path=ledger, now=now) == 0.0      # 40 fits exactly
    assert pacing.seconds_to_wait(15, path=ledger, now=now) == 800.0    # 41: wait for the first read to leave
    pacing.record(14, "tracker", path=ledger, now=now - 50)
    assert pacing.seconds_to_wait(10, path=ledger, now=now) == 800.0    # the oldest entry frees 26
    assert pacing.seconds_to_wait(10, path=ledger, now=now + 801) == 0.0
    assert pacing.recent_total(path=ledger, now=now + 840) == 14        # 26 expired, the 14 still count


def test_entries_from_every_job_count(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    for who in ("live", "backfill", "tracker"):
        pacing.record(15, who, path=ledger, now=9_950.0)
    assert pacing.recent_total(path=ledger, now=10_000.0) == 45
    assert pacing.seconds_to_wait(1, path=ledger, now=10_000.0) > 0


def test_a_damaged_line_is_ignored(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text('not json\n{"t": 9990, "n": "x"}\n{"t": 9990, "n": 5}\n', encoding="utf-8")
    assert pacing.recent_total(path=ledger, now=10_000.0) == 5


def test_wait_for_room_sleeps_then_rechecks(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    pacing.record(40, "backfill", path=ledger, now=1_000.0)
    clock = {"now": 1_100.0}
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    messages = []
    waited = pacing.wait_for_room(10, path=ledger, sleep=fake_sleep, clock=lambda: clock["now"],
                                  announce=messages.append)
    assert waited == sum(slept) and clock["now"] >= 1_000.0 + pacing.WINDOW_SECONDS
    assert max(slept) <= 120.0 and len(slept) >= 7 and messages          # long waits are taken in slices
    assert pacing.wait_for_room(10, path=ledger, sleep=fake_sleep, clock=lambda: clock["now"]) == 0.0


def test_the_ledger_is_compacted_and_never_raises(tmp_path, monkeypatch):
    ledger = tmp_path / "ledger.jsonl"
    monkeypatch.setattr(pacing, "_COMPACT_BYTES", 300)
    for i in range(30):
        pacing.record(1, "who", path=ledger, now=1_000_000.0 + i)   # long after the old entries
    pacing.record(1, "who", path=ledger, now=1_000_000.0 + 40_000)  # older than the keep horizon -> dropped
    lines = ledger.read_text(encoding="utf-8").splitlines()
    assert len(lines) < 31 and all(json.loads(line)["n"] == 1 for line in lines)
    blocked = tmp_path / "file"
    blocked.write_text("x", encoding="utf-8")
    pacing.record(1, "who", path=blocked / "child" / "ledger.jsonl")     # parent is a file: swallowed
