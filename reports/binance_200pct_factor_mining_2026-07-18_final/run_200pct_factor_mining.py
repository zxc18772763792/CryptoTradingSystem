"""Re-run the factor study with the fixed +200% (3x) target definition."""

from __future__ import annotations

import os
import runpy
from pathlib import Path


HERE = Path(__file__).resolve().parent
PIPELINE = (
    HERE.parent
    / "binance_200pct_factor_mining_2026-07-18"
    / "binance_200pct_factor_mining.py"
)
CACHE = (
    HERE.parent
    / "binance_200pct_factor_mining_2026-07-18"
    / "cache"
    / "spot_4h"
)

os.environ["BINANCE_FACTOR_TARGET_RETURN"] = "2.0"
os.environ["BINANCE_FACTOR_REPORT_DIR"] = str(HERE)
os.environ["BINANCE_FACTOR_CACHE_DIR"] = str(CACHE)

runpy.run_path(str(PIPELINE), run_name="__main__")
