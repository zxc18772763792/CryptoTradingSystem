"""CLI for the alastartrader1_as_written_v1 forward paper ledger (records only, never orders).

    activate    explicit registration (Binance clock and perpetual list; posts after this count)
    cycle       one pass of what the web task does every 30 seconds
    status      journal counts and results (no network)
    verify      full re-read of the hash chain and the pinned files
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import alastartrader1_ledger as ledger  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("activate", "cycle", "status", "verify"))
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    book = ledger.Ledger()
    if args.action == "activate":
        result = book.activate()
    elif args.action == "cycle":
        result = book.cycle()
    elif args.action == "status":
        result = ledger.summary()
    else:
        st = ledger.state(book.journal)
        result = {"records": st["last_seq"] + 1, "drift": st["drift"], "pins": ledger.pins()}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
