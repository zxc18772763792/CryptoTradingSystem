"""Explicit project/port identity in managed Python process command lines."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import runpy
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance-port", required=True, type=int)
    parser.add_argument("--module", required=True, choices=[
        "uvicorn", "core.news.service.worker", "core.news.service.llm_worker",
        "prediction_markets.polymarket.worker",
    ])
    args, remaining = parser.parse_known_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    sys.path.insert(0, str(root))
    sys.argv = [args.module, *remaining]
    runpy.run_module(args.module, run_name="__main__")


if __name__ == "__main__":
    main()
