"""Record syntax and ensure this continuation changes only its reviewed files."""
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
NODE = Path(r"C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe")
PREFIXES = ("core/", "web/", "scripts/", "strategies/", "prediction_markets/", "config/", "tests/")
ALLOWED = {
    "core/news/alastar_observation.py", "core/news/collectors/x_accounts.py", "core/news/x_batch.py",
    "core/news/x_pacing.py", "core/research/coinglass_snapshot_archive.py", "core/research/telegram_forward_runner.py",
    "scripts/backfill_x_kol.py", "scripts/track_x_growth.py", "tests/test_x_accounts_collector.py",
    "tests/test_audit_continuation_20261028.py",
}
PREVIOUS_ROUND = [
    "core/ai/autonomous_agent.py", "core/data/data_storage.py", "core/data/kline_partitions.py",
    "core/data/kline_venue.py", "core/market_state/benchmark_beta.py", "core/strategies/strategy_manager.py",
    "web/api/data.py", "tests/test_change_audit_20261028.py", "tests/test_strategy_manager_runtime_data.py",
    "tests/test_telegram_news_integration.py",
]
PINNED = [
    "core/research/alastar_forward_paper.py", "core/research/alastar_paper_tracker.py",
    "core/research/alastar_horizon_tracker.py", "scripts/observe_alastar_paper.py",
    "scripts/observe_alastar_horizons.py", "core/research/cryptoquant_alert_shadow.py",
    "core/research/next10xgem1_fade_tracker.py", "core/research/kol4_call_labels.py",
    "scripts/telegram_forward_research.py",
]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, encoding="utf-8").splitlines()


def digest(name):
    return hashlib.sha256((ROOT / name).read_bytes()).hexdigest()


def main():
    entry = json.loads((OUT / "entry_state.json").read_text(encoding="utf-8"))
    differences = [name for name, expected in entry["hashes"].items() if digest(name) != expected]
    unexpected = sorted(set(differences) - ALLOWED)
    previous = {name: digest(name) == entry["hashes"][name] for name in PREVIOUS_ROUND}
    pinned = {name: digest(name) == entry["hashes"][name] for name in PINNED}
    protocols = {name: digest(name) == expected for name, expected in entry["hashes"].items()
                 if name.startswith("config/research_protocols/")}
    names = sorted(set(git("ls-files") + git("ls-files", "--others", "--exclude-standard")))
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
    report = {"head": git("rev-parse", "HEAD")[0], "changed_since_entry": differences,
              "unexpected_changed_files": unexpected, "entry_file_count": len(entry["hashes"]),
              "previous_round_preserved": previous, "pinned_sources_preserved": pinned,
              "protocols_preserved": protocols, "parsed": dict(counts), "errors": errors, "sha256": hashes}
    (OUT / "static_checks.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "sha256"}))
    return bool(errors or unexpected) or not all(previous.values()) or not all(pinned.values()) or not all(protocols.values())


if __name__ == "__main__":
    raise SystemExit(main())
