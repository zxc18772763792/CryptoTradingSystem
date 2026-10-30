"""CLI for the hub_runup_filter_v1 forward paper ledger (records only, never orders).

  activate   verify clock/session, record the HUB baseline message id, start the journal (once)
  cycle      one poll + freeze + settlement pass (the web task runs this every 30 s)
  status     descriptive summary
  verify     re-check the journal hash chain and the pinned file hashes
  replay     offline check: the ledger's parser and execution on historical HUB posts must reproduce
             the study's trades (scratch directory, never the live journal)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
import random
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import hub_runup_ledger as ledger  # noqa: E402

STUDY = Path.home() / ".crypto_trading_system/research/kol_execution_20261028"
HISTORY = Path.home() / ".crypto_trading_system/research/multi_channel_backtest_180d_20261025/cryptosignalshub_history.json"


def emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, default=str))


def replay(sample: int) -> dict:
    """Parser parity on every frozen study label, execution parity on a sample of the study's trades."""
    labels = {l["message_id"]: l for l in json.loads((STUDY / "labels_before_prices.json").read_text(encoding="utf-8"))
              if l["channel"] == "hub"}
    posts = {p["message_id"]: p for p in json.loads(HISTORY.read_text(encoding="utf-8"))["posts"]}
    mismatched = []
    for mid, lab in labels.items():
        p = posts[mid]
        got = ledger.parse(mid, ledger.utc(p["published_at"]), ledger.utc(p["edited_at"]) if p["edited_at"] else None, p["text"])
        want = {k: lab[k] for k in ("symbol", "side", "entry_type", "targets", "stop", "leverage", "completeness")}
        if {k: got.get(k) for k in want} != want:
            mismatched.append(mid)
    trades = [t for t in json.loads((STUDY / "trades.json").read_text(encoding="utf-8"))
              if t["channel"] == "hub" and t.get("ret_on_filled") is not None]
    random.Random(7).shuffle(trades)
    checks = []
    with tempfile.TemporaryDirectory() as tmp:
        market = ledger.Market(Path(tmp), max_calls=400)
        cache = ledger.BarCache(market, Path(tmp))
        for t in trades[:sample]:
            lab = labels[t["message_id"]]
            p = ledger.parse(t["message_id"], ledger.utc(lab["published_at"]), None, posts[t["message_id"]]["text"])
            act = ledger.utc(t["activation"]).timestamp()
            entry = ledger.datetime.fromtimestamp(math.ceil(act / 60) * 60, ledger.timezone.utc)
            dec = {"message_id": t["message_id"], "native": t["native"], "mult": t["mult"], "parsed": p,
                   "entry_at": entry.isoformat()}
            # price-only replay; the study's own funding cash flow is then subtracted (old funding is beyond
            # the latest-N window the live ledger uses right after each exit)
            r = ledger.simulate(dec, cache, entry + ledger.HOLD + ledger.timedelta(minutes=10))
            if r.get("ret_on_filled") is not None:
                r["ret_on_filled"] -= t.get("funding") or 0.0
            checks.append({"message_id": t["message_id"], "symbol": t["symbol"], "study": t["ret_on_filled"],
                           "ledger": r.get("ret_on_filled"), "study_exit": t["exit_reason"], "ledger_exit": r.get("exit_reason"),
                           "match": r.get("ret_on_filled") is not None and abs(r["ret_on_filled"] - t["ret_on_filled"]) < 1e-9})
    return {"labels": len(labels), "parser_mismatches": mismatched, "execution_checked": len(checks),
            "execution_matches": sum(c["match"] for c in checks), "checks": checks}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["activate", "cycle", "status", "verify", "replay"])
    ap.add_argument("--sample", type=int, default=20)
    args = ap.parse_args()
    try:
        if args.cmd == "activate":
            rec = asyncio.run(ledger.Ledger().activate())
            emit({"outcome": "activated", "at": rec["at"], "baseline_message_id": rec["data"]["baseline_message_id"],
                  "pins": rec["pins"]})
        elif args.cmd == "cycle":
            emit(asyncio.run(ledger.Ledger().cycle()))
        elif args.cmd == "status":
            emit(ledger.summary())
        elif args.cmd == "verify":
            st = ledger.state(ledger.Journal())
            emit({"outcome": "ok" if not st["drift"] else "drift", "records": st["last_seq"] + 1, "drift": st["drift"],
                  "pins": ledger.pins()})
        elif args.cmd == "replay":
            emit(replay(args.sample))
    except Exception as exc:  # one JSON line for the supervising worker
        emit({"outcome": "failed", "error_type": type(exc).__name__, "error": str(exc)[:300]})
        sys.exit(1)


if __name__ == "__main__":
    main()
