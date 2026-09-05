"""Refresh checksums after executing the notebook and building the native report payload."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
OUTPUT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    summary = json.loads((OUTPUT / "analysis_summary.json").read_text(encoding="utf-8"))
    previous = json.loads((OUTPUT / "evidence_manifest.json").read_text(encoding="utf-8"))
    required = [
        BASELINE / "daily_feature_panel.csv.gz",
        BASELINE / "futures_4h_panel.csv.gz",
        BASELINE / "factor_model.json",
        BASELINE / "exchange_info_snapshot.json",
        BASELINE / "current_watchlist.csv",
    ]
    files = [path for path in OUTPUT.rglob("*") if path.is_file()]
    manifest = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "baseline_inputs": {path.name: checksum(path) for path in required},
        "analysis_files": {
            str(path.relative_to(OUTPUT)): {"bytes": path.stat().st_size, "sha256": checksum(path)}
            for path in sorted(files)
            if path.name not in {"evidence_manifest.json", "verification_summary.json", "VALIDATION_REPORT.md"}
            and path.suffix.lower() != ".log"
        },
        "generated_charts": previous.get("generated_charts", []),
        "analysis_hash": hashlib.sha256(
            json.dumps(summary, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    }
    (OUTPUT / "evidence_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"manifest files={len(manifest['analysis_files'])}")


if __name__ == "__main__":
    main()
