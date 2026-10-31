"""Reuse the offline snapshot harness and keep this round's evidence separate."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent


def main():
    entry = OUT / "entry_state.json"
    if not entry.exists():
        names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT).decode().split("\0")
        hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                  for name in names if name and name.startswith(("core/", "web/", "scripts/", "tests/", "config/", "docs/")) and (ROOT / name).is_file()}
        entry.write_text(json.dumps({"head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                                     "hashes": hashes}, indent=2), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("audit_harness", ROOT / "reports/change_audit_20261028/run_validation.py")
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    harness.OUT = OUT
    return harness.main()


if __name__ == "__main__":
    raise SystemExit(main())
