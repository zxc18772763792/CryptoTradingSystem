"""Reproduce the +300% (4x price) Binance factor-mining run."""

from __future__ import annotations

import os
import runpy
from pathlib import Path


REPORT_DIR = Path(__file__).resolve().parent
SOURCE_DIR = REPORT_DIR.parent / "binance_200pct_factor_mining_2026-07-18"
PIPELINE = SOURCE_DIR / "binance_200pct_factor_mining.py"

os.environ["BINANCE_FACTOR_TARGET_RETURN"] = "3.0"
os.environ["BINANCE_FACTOR_REPORT_DIR"] = str(REPORT_DIR)
os.environ.setdefault(
    "BINANCE_FACTOR_CACHE_DIR", str(SOURCE_DIR / "cache" / "spot_4h")
)

if not PIPELINE.exists():
    raise FileNotFoundError(f"Generic factor-mining pipeline not found: {PIPELINE}")

runpy.run_path(str(PIPELINE), run_name="__main__")

