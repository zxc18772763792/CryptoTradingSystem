"""Run the +200% factor study on Binance USD-M USDT perpetuals."""

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

os.environ["BINANCE_FACTOR_MARKET"] = "futures"
os.environ["BINANCE_FACTOR_TARGET_RETURN"] = "2.0"
os.environ["BINANCE_FACTOR_REPORT_DIR"] = str(HERE)
os.environ["BINANCE_FACTOR_CACHE_DIR"] = str(HERE / "cache" / "futures_4h")
os.environ.setdefault("BINANCE_FACTOR_WORKERS", "10")

runpy.run_path(str(PIPELINE), run_name="__main__")
