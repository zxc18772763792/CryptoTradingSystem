"""CLI for the korea_frozen_skip_v1 forward paper overlay (records only, never orders).

  activate   check the source tracker and both evidence endpoints, then start the journal (once)
  cycle      one observe + decide + settle pass (the web task runs this every 15 minutes)
  status     descriptive summary
  verify     re-check the journal hash chain and the pinned file hashes
  replay     offline check: the pinned wallet rule on the cached Upbit notices of the 38 development
             events must reproduce the protocol's 11 both-suspended events
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import korea_frozen_skip_ledger as ledger  # noqa: E402

DEVELOPMENT = Path.home() / ".crypto_trading_system/research/init_squeeze_20261031/korea_notices"


def emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, default=str))


async def _with_client(call):
    from core.research.exchange_research_runner import _client
    async with _client() as client:
        return await call(client)


def replay() -> dict:
    import pandas as pd

    events = pd.read_csv(DEVELOPMENT / "event_wallet_status.csv")
    rows = []
    for ev in events.itertuples():
        if pd.isna(ev.ret) or pd.isna(ev.notice):
            continue
        path = DEVELOPMENT / f"upbit_{int(ev.notice)}.json"
        try:
            body = (json.loads(path.read_text(encoding="utf-8")).get("data") or {}).get("body") or ""
        except (OSError, ValueError):
            continue
        rows.append({"token": ev.token, "date": ev.date, "ret_pct": round(float(ev.ret) * 100, 2),
                     "phrase": ledger.wallet_phrase(body)})
    both = [r for r in rows if r["phrase"]]
    rest = [r["ret_pct"] for r in rows if not r["phrase"]]
    return {"events": len(rows), "both_suspended": len(both),
            "both_mean_pct": round(sum(r["ret_pct"] for r in both) / len(both), 2) if both else None,
            "rest_mean_pct": round(sum(rest) / len(rest), 2) if rest else None,
            "both": [{k: r[k] for k in ("token", "date", "ret_pct", "phrase")} for r in both]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["activate", "cycle", "status", "verify", "replay"])
    args = ap.parse_args()
    if args.cmd == "activate":
        rec = asyncio.run(_with_client(ledger.Ledger().activate))
        emit({"outcome": "activated", "at": rec["at"], "pins": rec["pins"], "data": rec["data"]})
    elif args.cmd == "cycle":
        emit(asyncio.run(_with_client(ledger.Ledger().cycle)))
    elif args.cmd == "status":
        emit(ledger.summary())
    elif args.cmd == "verify":
        try:
            recs = ledger.Journal().records()
            st = ledger.state(ledger.Journal())
            emit({"records": len(recs), "chain": "ok", "pins_current": ledger.pins(),
                  "pins_activated": (st["activation"] or {}).get("pins"), "drift": st["drift"]})
        except Exception as exc:  # noqa: BLE001
            emit({"chain": "broken", "error": str(exc)})
    else:
        emit(replay())


if __name__ == "__main__":
    main()
