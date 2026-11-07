"""Remove the torn fragment a crash left in a hash-chained ledger journal. Dry run unless --apply.

These ledgers tolerate a torn last line when reading, but their next append lands right behind it, and from then on the
journal cannot be read (the tracker page shows 日志校验失败). Their modules are hash-pinned, so the repair lives here.
Only bytes outside a complete record are removed, the result must pass the ledger's own chain check, and the original
stays beside the journal. Stop the web service first. See docs/LEDGER_JOURNAL_REPAIR.md.
"""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LEDGERS = {"event_short_preclose_v1": "core.research.event_short_preclose_ledger",
           "unlock_t7_big_v1": "core.research.unlock_t7_big_ledger",
           "korea_frozen_skip_v1": "core.research.korea_frozen_skip_ledger"}
RECORD_START = b'{"at":'  # records are written with sorted keys, so "at" always comes first


def _parses(raw: bytes) -> bool:
    try:
        json.loads(raw)
        return True
    except ValueError:
        return False


def _split_torn_line(line: bytes) -> Tuple[bytes, bytes]:
    """(fragment, record) for a line where a record was appended right behind a fragment; no record if none is complete."""
    start = line.find(RECORD_START)
    while start >= 0:
        if _parses(line[start:]):
            return line[:start], line[start:]
        start = line.find(RECORD_START, start + 1)
    return line, b""


def strip_fragments(raw: bytes) -> Tuple[bytes, List[bytes]]:
    """(journal without fragments, the removed fragments). Complete records are kept byte for byte."""
    lines = raw.split(b"\n")
    kept, removed = [], []
    for line in lines[:-1]:
        if _parses(line):
            kept.append(line)
            continue
        fragment, record = _split_torn_line(line)
        removed.append(fragment)
        if record:
            kept.append(record)
    if lines[-1]:  # no newline yet: a fragment nothing has been appended to
        removed.append(lines[-1])
    return b"".join(line + b"\n" for line in kept), removed


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("ledger", choices=sorted(LEDGERS))
    parser.add_argument("--apply", action="store_true", help="write the repair; without it nothing changes")
    parser.add_argument("--output", type=Path, help="ledger directory; defaults to data/research/<ledger>")
    args = parser.parse_args(argv)
    output = args.output or ROOT / "data/research" / args.ledger
    path = output / "journal.jsonl"
    fixed, removed = strip_fragments(path.read_bytes())
    if not removed:
        print(f"{path}: no fragments, nothing to do")
        return 0
    if (output / "journal.lock").exists():
        print(f"{output / 'journal.lock'} exists: a writer may be active. Stop the web service and try again.")
        return 2
    with tempfile.TemporaryDirectory() as temp:
        (Path(temp) / "journal.jsonl").write_bytes(fixed)
        try:
            records = importlib.import_module(LEDGERS[args.ledger]).Journal(Path(temp)).records()
        except Exception as exc:  # noqa: BLE001 - any failure means the damage is more than a torn tail
            print(f"refusing: the result does not pass the ledger's chain check ({type(exc).__name__}: {exc}). Journal unchanged.")
            return 1
    print(f"{path}: {len(removed)} fragment(s), {sum(map(len, removed))} bytes to remove; {len(records)} records verify")
    if not args.apply:
        print("dry run: nothing changed. Add --apply to write.")
        return 0
    backup = path.with_name(f"journal.jsonl.before_repair_{time.strftime('%Y%m%d_%H%M%S')}")
    shutil.copy2(path, backup)
    temporary = path.with_name("journal.jsonl.repair_tmp")
    temporary.write_bytes(fixed)
    temporary.replace(path)
    print(f"repaired; the original is at {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
