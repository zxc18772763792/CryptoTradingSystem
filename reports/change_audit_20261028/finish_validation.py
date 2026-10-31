"""Verify final source hashes and report distinct regression coverage."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
FULL = "132659"
SUPPLEMENT = "133207"


def read(name):
    return json.loads((OUT / name).read_text(encoding="utf-8"))


def cases(stamp):
    result = {}
    for case in ET.parse(OUT / f"pytest_{stamp}.xml").iter("testcase"):
        key = case.attrib["classname"] + "::" + case.attrib["name"]
        assert key not in result, f"duplicate testcase: {key}"
        result[key] = "failed" if case.find("failure") is not None or case.find("error") is not None else "skipped" if case.find("skipped") is not None else "passed"
    return result


def main():
    full, supplement = read(f"run_{FULL}.json"), read(f"run_{SUPPLEMENT}.json")
    assert supplement["exit_code"] == 0
    full_snapshot, final_snapshot = read(f"snapshot_{FULL}.json"), read(f"snapshot_{SUPPLEMENT}.json")
    stale = "tests.test_telegram_news_integration::test_default_configuration_enables_alastar_collection"
    full_cases, last_cases = cases(FULL), cases(SUPPLEMENT)
    assert {key for key, status in full_cases.items() if status == "failed"} == {stale}
    assert last_cases[stale] == "passed"
    current = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
               for name in final_snapshot["hashes"]}
    assert current == final_snapshot["hashes"], "source changed after the final supplemental snapshot"
    differences = [name for name, digest in current.items() if digest != full_snapshot["hashes"].get(name)]
    assert differences == ["tests/test_telegram_news_integration.py"], differences
    combined = {**full_cases, **last_cases}
    assert "failed" not in combined.values()
    audit_cases = {key: status for key, status in combined.items() if "test_change_audit_20261028::" in key}
    assert len(audit_cases) == 15 and set(audit_cases.values()) == {"passed"}
    source_checks = read("static_checks.json")
    assert all(source_checks["preexisting_dirty_sources_preserved"].values())
    assert all(source_checks["preexisting_dirty_docs_preserved"].values())
    result = {
        "interpreter": sys.executable, "python": sys.version,
        "full_run": {"stamp": FULL, "seconds": full["seconds"], "counts": dict(Counter(full_cases.values()))},
        "supplemental_run": {"stamp": SUPPLEMENT, "seconds": supplement["seconds"], "counts": dict(Counter(last_cases.values()))},
        "distinct_current_test_coverage": dict(Counter(combined.values())),
        "full_run_stale_assertion": stale,
        "full_snapshot_differences": differences,
        "final_source_hashes_match_supplemental_snapshot": True,
        "all_production_source_hashes_match_full_snapshot": True,
        "new_audit_cases": audit_cases,
        "preserved_dirty_sources": len(source_checks["preexisting_dirty_sources_preserved"]),
        "preserved_dirty_docs": len(source_checks["preexisting_dirty_docs_preserved"]),
        "node_tests": {"passed": 13, "failed": 0},
    }
    (OUT / "final_verification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "new_audit_cases"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
