"""CLI for the unlock_t7_big_v1 forward paper overlay (records only, never orders).

  activate   check the source tracker, then start the journal (once)
  cycle      one record + settle pass (the research runner does this every hour)
  status     descriptive summary
  verify     re-check the journal hash chain, the pinned file hashes and the source module hash
  replay     offline check: the pinned book rule on the development backtest must reproduce the protocol's figures
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import unlock_t7_big_ledger as ledger  # noqa: E402

DEVELOPMENT = ledger.OUTPUT / "development_t7_5_stop40.csv"


def emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, default=str))


def replay() -> dict:
    import pandas as pd

    rows = pd.read_csv(DEVELOPMENT, parse_dates=["entry"]).dropna(subset=["basket_hedged"])
    rows = rows.assign(ret=rows.basket_hedged * 100, book=[ledger.book_of(s) for s in rows.size_pct])
    out: dict = {"file": DEVELOPMENT.relative_to(ROOT).as_posix(), "sha256": ledger.sha(DEVELOPMENT.read_bytes()),
                 "hedged_trades": len(rows)}
    means = {}
    for book, group in rows.groupby("book"):
        days = group.groupby("entry").ret.mean().sort_index().tolist()
        means[book] = days
        out[book] = {"trades": len(group), "trade_mean_pct": round(group.ret.mean(), 2),
                     "win_rate": round((group.ret > 0).mean(), 2), "worst_pct": round(group.ret.min(), 1),
                     "batches": len(days), "batch_mean_pct": round(sum(days) / len(days), 2)}
    out["sign_flip_p"] = round(ledger.sign_flip_p(means["big"]), 4)
    out["big_vs_small_p"] = round(ledger.permutation_p(means["big"], means["small"]), 4)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["activate", "cycle", "status", "verify", "replay"])
    args = ap.parse_args()
    if args.cmd == "activate":
        rec = ledger.Ledger().activate()
        emit({"outcome": "activated", "at": rec["at"], "pins": rec["pins"], "data": rec["data"]})
    elif args.cmd == "cycle":
        emit(ledger.Ledger().cycle())
    elif args.cmd == "status":
        emit(ledger.summary())
    elif args.cmd == "verify":
        try:
            recs = ledger.Journal().records()
            st = ledger.state(ledger.Journal())
            activated = st["activation"] or {}
            current = ledger.sha(ledger.SOURCE_MODULE.read_bytes())
            emit({"records": len(recs), "chain": "ok", "pins_current": ledger.pins(), "pins_activated": activated.get("pins"),
                  "drift": st["drift"], "source_module_unchanged": activated.get("source_module_sha256") == current})
        except Exception as exc:  # noqa: BLE001
            emit({"chain": "broken", "error": str(exc)})
    else:
        emit(replay())


if __name__ == "__main__":
    main()
