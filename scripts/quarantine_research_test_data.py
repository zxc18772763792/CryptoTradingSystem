"""Offline, reversible quarantine of research records with exact fixture evidence.

Dry run by default. Stop the web service before --apply. No strategy-name or
performance-threshold heuristics are used to classify real research as test data.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
from datetime import datetime, timezone
from pathlib import Path


FIXTURE_REPORTS = {"research_ops.csv", "research_out.csv", "research_bg.csv"}
REGISTRIES = {
    "proposals.json": ("proposals", "proposal_id"),
    "candidates.json": ("candidates", "candidate_id"),
    "experiments.json": ("experiments", "experiment_id"),
    "experiment_runs.json": ("runs", "run_id"),
    "lifecycle.json": ("lifecycle", "object_id"),
}


def quarantine_plan(payloads: dict) -> tuple[dict, dict]:
    candidates = payloads.get("candidates.json", {}).get("candidates", [])
    proposals = payloads.get("proposals.json", {}).get("proposals", [])
    experiments = payloads.get("experiments.json", {}).get("experiments", [])
    runs = payloads.get("experiment_runs.json", {}).get("runs", [])
    bad_candidates = {
        row["candidate_id"] for row in candidates
        if row["candidate_id"] == "cand-test-trigger"
        or row.get("metadata", {}).get("csv_path") in FIXTURE_REPORTS
    }
    bad_experiments = {row["experiment_id"] for row in runs if row.get("result", {}).get("csv_path") in FIXTURE_REPORTS}
    bad_proposals: set[str] = set()
    # Follow only explicit registry links from proven fixture rows.
    while True:
        before = (len(bad_candidates), len(bad_experiments), len(bad_proposals))
        for row in candidates:
            if row["candidate_id"] in bad_candidates or row.get("experiment_id") in bad_experiments or row.get("proposal_id") in bad_proposals:
                bad_candidates.add(row["candidate_id"])
                bad_experiments.add(row["experiment_id"])
                bad_proposals.add(row["proposal_id"])
        for row in experiments:
            if row["experiment_id"] in bad_experiments or row.get("proposal_id") in bad_proposals:
                bad_experiments.add(row["experiment_id"])
                bad_proposals.add(row["proposal_id"])
        for row in proposals:
            meta = row.get("metadata", {})
            if meta.get("parent_candidate_id") in bad_candidates or row.get("latest_candidate_id") in bad_candidates:
                bad_proposals.add(row["proposal_id"])
        if before == (len(bad_candidates), len(bad_experiments), len(bad_proposals)):
            break
    bad_runs = {row["run_id"] for row in runs if row["experiment_id"] in bad_experiments}
    bad = {
        "candidate": bad_candidates, "proposal": bad_proposals,
        "experiment": bad_experiments, "run": bad_runs,
    }
    cleaned, report = {}, {}
    for filename, (root, key) in REGISTRIES.items():
        payload = payloads.get(filename, {root: []})
        rows = payload.get(root, [])
        if root == "lifecycle":
            removed = [row for row in rows if row.get(key) in set().union(*bad.values())]
        else:
            ids = bad[{"proposals": "proposal", "candidates": "candidate", "experiments": "experiment", "runs": "run"}[root]]
            removed = [row for row in rows if row.get(key) in ids]
        removed_objects = {id(row) for row in removed}
        cleaned[filename] = {**payload, root: [row for row in rows if id(row) not in removed_objects]}
        report[filename] = {"before": len(rows), "quarantined": len(removed), "remaining": len(rows) - len(removed)}
    jobs = payloads.get("research_jobs.json", {})
    cleaned["research_jobs.json"] = {
        key: row for key, row in jobs.items()
        if row.get("proposal_id") not in bad_proposals and row.get("experiment_id") not in bad_experiments
    }
    report["research_jobs.json"] = {"quarantined": len(jobs) - len(cleaned["research_jobs.json"])}
    return cleaned, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-dir", type=Path, default=Path(__file__).resolve().parents[1] / "data" / "research")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    research_dir = args.research_dir.resolve()
    registry_dir = research_dir / "ai"
    paths = [registry_dir / name for name in (*REGISTRIES, "research_jobs.json")]
    paths.append(research_dir / "latest.json")
    originals = {path: path.read_bytes() for path in paths if path.exists()}
    payloads = {path.name: json.loads(raw) for path, raw in originals.items()}
    cleaned, report = quarantine_plan(payloads)
    latest = payloads.get("latest.json", {})
    quarantine_latest = latest.get("proposal_id") == "proposal-timeout" or latest.get("csv_path") in FIXTURE_REPORTS
    report["latest.json"] = {"quarantined": int(quarantine_latest)}
    if args.apply and any(item["quarantined"] for item in report.values()):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", args.port)) == 0:
                raise SystemExit("Stop the web service before applying quarantine.")
        backup = research_dir / "quarantine" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup.mkdir(parents=True, exist_ok=False)
        for path, raw in originals.items():
            target = backup / path.relative_to(research_dir)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        manifest = {"counts": report, "sha256": {str(path.relative_to(research_dir)): hashlib.sha256(raw).hexdigest() for path, raw in originals.items()}}
        (backup / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        if any(path.read_bytes() != raw for path, raw in originals.items()):
            raise SystemExit("Research files changed during backup; no quarantine applied.")
        for path in originals:
            if not report[path.name]["quarantined"]:
                continue
            value = {} if path.name == "latest.json" else cleaned[path.name]
            temporary = path.with_suffix(".quarantine.tmp")
            temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            temporary.replace(path)
        report["backup"] = str(backup)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
