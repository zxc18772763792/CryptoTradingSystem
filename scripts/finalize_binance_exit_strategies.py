"""Attach the independently validated exit-study decision to the main analysis summary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = args.report_dir.resolve()
    summary_path = report / "analysis_summary.json"
    exit_path = report / "exit_strategy_decision.json"
    verification_path = report / "exit_verification_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    decision = json.loads(exit_path.read_text(encoding="utf-8"))
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Exit evidence did not pass independent verification")
    summary["exit_strategy"] = decision
    summary["paper_strategy_superseded_by_exit_validation"] = True
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary_path)


if __name__ == "__main__":
    main()
