"""Syntax, change inventory and preservation checks without importing the app."""
from __future__ import annotations

import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
BASELINE = "b00fda75c1e1676abb44f4b0dcabc9b0b2a48452"
NODE = Path(r"C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe")
PREFIXES = ("core/", "web/", "scripts/", "strategies/", "prediction_markets/", "config/", "tests/")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, encoding="utf-8").splitlines()


def main():
    names = sorted(set(git("ls-files") + git("ls-files", "--others", "--exclude-standard")))
    scope = sorted(set(git("diff", "--name-only", BASELINE) + git("ls-files", "--others", "--exclude-standard")))
    scope = [name for name in scope if name.startswith(PREFIXES)]
    counts, errors, hashes = Counter(), [], {}
    for name in names:
        path = ROOT / name
        if not name.startswith(PREFIXES) or not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in {".py", ".js", ".cjs", ".json", ".yaml", ".yml", ".ps1"}:
            continue
        raw = path.read_bytes()
        hashes[name] = hashlib.sha256(raw).hexdigest()
        counts[suffix] += 1
        try:
            text = raw.decode("utf-8-sig")
            if suffix == ".py":
                ast.parse(text, filename=name)
            elif suffix == ".json":
                json.loads(text)
            elif suffix in {".yaml", ".yml"}:
                yaml.safe_load(text)
            elif suffix in {".js", ".cjs"}:
                result = subprocess.run([str(NODE), "--check", str(path)], capture_output=True, text=True)
                if result.returncode:
                    raise ValueError(result.stderr[:1000])
        except Exception as exc:
            errors.append({"path": name, "error": str(exc)})
    baseline = json.loads((OUT / "snapshot_130356.json").read_text(encoding="utf-8"))
    # Every pre-existing dirty source from the initial status, including its
    # untracked helper, must remain byte-identical during this audit.
    original_dirty = [
        "core/ai/research_loop_v3.py", "core/news/x_archive.py", "core/news/x_pacing.py",
        "core/research/announcement_short_tracker.py", "core/research/coinglass_composite_tracker.py",
        "core/research/exchange_research_runner.py", "core/research/paired_evaluation.py",
        "core/research/tracker_registry.py", "core/research/upbit_caution_tracker.py",
        "core/research/xs_return_evaluation.py", "core/research/xs_reversal_tracker.py",
        "core/utils/shared_ssl.py", "core/utils/file_lock.py", "scripts/news_library.py",
        "scripts/research_telegram_new_kols.py", "scripts/research_x_kol.py", "scripts/research_x_panel.py",
        "scripts/review_telegram_signals.py", "scripts/telegram_forward_cycle.py", "scripts/upbit_short_backtest.py",
        "tests/test_research_telegram_new_kols.py", "tests/test_shared_ssl.py", "tests/test_tracker_robustness.py",
        "tests/test_upbit_caution_tracker.py", "tests/test_upbit_short_backtest.py", "tests/test_xs_reversal_tracker.py",
        "tests/test_october_audit_regressions.py", "web/asset_versions.py", "web/static/js/app.js",
    ]
    preserved = {name: hashes.get(name) == baseline["hashes"].get(name) for name in original_dirty}
    original_docs = ["docs/TELEGRAM_AGENT_HANDOFF.md", "docs/TELEGRAM_NEW_KOLS_RESEARCH_20261025.md",
                     "docs/KOREAN_TRACKER_RELIABILITY_2026-10-27.md", "docs/NEWS_RESEARCH_LIBRARY.md",
                     "docs/TELEGRAM_KOL_FOLLOWUP_20261027.md", "docs/TELEGRAM_NEW_KOLS_RECHECK_20261025.md"]
    doc_baseline_path = OUT / "initial_doc_hashes.json"
    if doc_baseline_path.exists():
        doc_baseline = json.loads(doc_baseline_path.read_text(encoding="utf-8"))
    else:
        doc_baseline = {name: hashlib.sha256((Path(baseline["source"]) / name).read_bytes()).hexdigest()
                        for name in original_docs}
        doc_baseline_path.write_text(json.dumps(doc_baseline, indent=2), encoding="utf-8")
    preserved_docs = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == doc_baseline[name]
                      for name in original_docs}
    result = {"baseline": BASELINE, "head": git("rev-parse", "HEAD")[0],
              "commits": int(git("rev-list", "--count", f"{BASELINE}..HEAD")[0]),
              "changed_source_and_test_paths": len(scope), "changed_paths": scope,
              "parsed_by_suffix": dict(counts), "errors": errors,
              "preexisting_dirty_sources_preserved": preserved,
              "preexisting_dirty_docs_preserved": preserved_docs, "sha256": hashes}
    (OUT / "static_checks.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in {"changed_paths", "sha256", "preexisting_dirty_sources_preserved", "preexisting_dirty_docs_preserved"}}))
    print(f"Pre-existing dirty sources preserved: {sum(preserved.values())}/{len(preserved)}")
    print(f"Pre-existing dirty docs preserved: {sum(preserved_docs.values())}/{len(preserved_docs)}")
    return bool(errors) or not all(preserved.values()) or not all(preserved_docs.values())


if __name__ == "__main__":
    raise SystemExit(main())
