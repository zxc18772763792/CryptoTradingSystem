"""A torn journal tail, from the real ledger modules, repaired by the script and accepted by the ledger again."""
from __future__ import annotations

import importlib

import pytest

from scripts import repair_ledger_journal_tail as repair

LEDGERS = sorted(repair.LEDGERS)


def _journal(ledger, output):
    return importlib.import_module(repair.LEDGERS[ledger]).Journal(output)


def _torn(ledger, output, *, appended_after=True):
    journal = _journal(ledger, output)
    journal.append("activation", {"protocol": "test", "nested": {"at": "looks like a record start"}})
    journal.append("event", {"source_id": "a"})
    with journal.path.open("ab") as stream:
        stream.write(b'{"at":"2026-11-06T00:00:00+00:00","data":{"source_')  # what a crash mid-write leaves
    if appended_after:
        _journal(ledger, output).append("event", {"source_id": "b"})
    return journal.path


@pytest.mark.parametrize("ledger", LEDGERS)
def test_the_tear_breaks_the_journal_and_the_repair_restores_every_complete_record(ledger, tmp_path):
    path = _torn(ledger, tmp_path)
    with pytest.raises(ValueError):
        _journal(ledger, tmp_path).records()
    broken = path.read_bytes()
    assert repair.main([ledger, "--output", str(tmp_path)]) == 0
    assert path.read_bytes() == broken  # a dry run changes nothing
    assert repair.main([ledger, "--output", str(tmp_path), "--apply"]) == 0
    records = _journal(ledger, tmp_path).records()
    assert [r["kind"] for r in records] == ["activation", "event", "event"]
    assert [r["data"].get("source_id") for r in records[1:]] == ["a", "b"]
    assert list(tmp_path.glob("journal.jsonl.before_repair_*"))[0].read_bytes() == broken
    assert repair.main([ledger, "--output", str(tmp_path)]) == 0  # clean now
    _journal(ledger, tmp_path).append("event", {"source_id": "c"})
    assert len(_journal(ledger, tmp_path).records()) == 4


@pytest.mark.parametrize("ledger", LEDGERS)
def test_a_fragment_nothing_was_appended_to_is_removed_too(ledger, tmp_path):
    path = _torn(ledger, tmp_path, appended_after=False)
    assert len(_journal(ledger, tmp_path).records()) == 2
    assert repair.main([ledger, "--output", str(tmp_path), "--apply"]) == 0
    assert path.read_bytes().endswith(b"\n")
    assert len(_journal(ledger, tmp_path).records()) == 2


def test_refuses_while_a_writer_may_hold_the_lock(tmp_path):
    ledger = LEDGERS[0]
    path = _torn(ledger, tmp_path)
    (tmp_path / "journal.lock").write_text("1")
    before = path.read_bytes()
    assert repair.main([ledger, "--output", str(tmp_path), "--apply"]) == 2
    assert path.read_bytes() == before


def test_refuses_damage_beyond_a_torn_tail(tmp_path):
    ledger = LEDGERS[0]
    path = _torn(ledger, tmp_path, appended_after=False)
    lines = path.read_bytes().split(b"\n")
    path.write_bytes(lines[1] + b"\n" + lines[2])  # the first record is gone: the chain cannot verify
    before = path.read_bytes()
    assert repair.main([ledger, "--output", str(tmp_path), "--apply"]) == 1
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("journal.jsonl.before_repair_*"))
