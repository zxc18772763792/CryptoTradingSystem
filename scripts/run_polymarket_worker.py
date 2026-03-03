from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from prediction_markets.polymarket.worker import main


if __name__ == "__main__":
    raise SystemExit(main())
