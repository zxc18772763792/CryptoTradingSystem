"""Confirm the final full run matches the current production sources."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
FINAL_RUN = "173801"


def read(name):
    return json.loads((OUT / name).read_text(encoding="utf-8"))


def main():
    run = read(f"run_{FINAL_RUN}.json")
    assert run["exit_code"] == 0, run
    snapshot = read(f"snapshot_{FINAL_RUN}.json")
    differences = [name for name, expected in snapshot["hashes"].items()
                   if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected]
    assert not differences, differences
    counts = Counter()
    audit_cases, previous_cases = {}, {}
    for case in ET.parse(OUT / f"pytest_{FINAL_RUN}.xml").iter("testcase"):
        status = "failed" if case.find("failure") is not None or case.find("error") is not None else "skipped" if case.find("skipped") is not None else "passed"
        counts[status] += 1
        key = case.attrib["classname"] + "::" + case.attrib["name"]
        if "test_audit_continuation_20261028::" in key:
            audit_cases[key] = status
        if "test_change_audit_20261028::" in key:
            previous_cases[key] = status
    assert counts["failed"] == 0
    assert len(audit_cases) == 28 and set(audit_cases.values()) == {"passed"}
    assert len(previous_cases) == 15 and set(previous_cases.values()) == {"passed"}
    checks = read("static_checks.json")
    assert checks["unexpected_changed_files"] == [] and checks["errors"] == []
    assert all(checks["previous_round_preserved"].values())
    assert all(checks["pinned_sources_preserved"].values())
    assert all(checks["protocols_preserved"].values())
    report = {"run": FINAL_RUN, "seconds": run["seconds"], "counts": dict(counts),
              "all_source_hashes_match_final_full_run": True, "new_audit_cases": audit_cases,
              "previous_audit_cases": previous_cases, "source_file_count": len(snapshot["hashes"]),
              "changed_paths": checks["changed_since_entry"], "unexpected_changes": [],
              "previous_round_files_preserved": 10, "pinned_implementations_preserved": 9,
              "protocols_preserved": True, "interpreter": sys.executable, "python": sys.version}
    (OUT / "final_verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key not in {"new_audit_cases", "previous_audit_cases"}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
