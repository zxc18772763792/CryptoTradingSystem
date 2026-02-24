from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from _common import add_common_args, ensure_output_dir


def _run(script: str, common_args: list[str]) -> int:
    cmd = [sys.executable, str(Path("scripts/research") / script), *common_args]
    print(f"[all_reports] run: {' '.join(cmd)}")
    return subprocess.call(cmd)


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="Run HF research report suite"))
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")
    common = [
        "--exchange", args.exchange,
        "--symbol", args.symbol,
        "--timeframe", args.timeframe,
        "--days", str(args.days),
        "--config", args.config,
        "--output-dir", str(out_dir),
    ]
    scripts = ["data_qa.py", "factor_study.py", "cost_sensitivity.py", "walk_forward.py", "robustness.py"]
    rc_map = {}
    for s in scripts:
        rc_map[s] = _run(s, common)
    print("[all_reports] summary:", rc_map)
    if any(v != 0 for v in rc_map.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
