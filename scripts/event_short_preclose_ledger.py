"""CLI for the event_short_preclose_v1 forward paper ledger (records only, never orders).

  activate   check both source trackers, then start the journal (once)
  cycle      one pass (the web job does this every 15 minutes, and every 20 seconds inside the capture window)
  status     descriptive summary
  verify     re-check the journal hash chain, the pinned file hashes and the source module hashes
  replay     offline check: the reference file must reproduce the protocol's figures
  dryrun     read the real market once for a symbol and print what an entry would record; writes nothing
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import event_short_preclose_ledger as ledger  # noqa: E402

REFERENCE = ledger.OUTPUT / "development_reference.csv"
SUMMARY = ledger.OUTPUT / "development_summary.json"


def emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, default=str))


def replay() -> dict:
    protocol = json.loads(ledger.PROTOCOL.read_text(encoding="utf-8"))["development_evidence"]
    out = {"reference_sha256": ledger.sha(REFERENCE.read_bytes()), "summary_sha256": ledger.sha(SUMMARY.read_bytes())}
    out["reference_matches_protocol"] = out["reference_sha256"] == protocol["reference_sha256"]
    out["summary_matches_protocol"] = out["summary_sha256"] == protocol["summary_sha256"]
    import pandas as pd

    rows = pd.read_csv(REFERENCE, parse_dates=["entry_day"])
    rows = rows[rows.entry_day >= "2022-11-01"]
    for label, part in (("all_from_2022_11", rows), ("holdout", rows[rows["sample"] == "holdout"])):
        means = part.groupby("entry_day").hedged.mean() * 100
        capped = part.groupby("entry_day").hedged_capped.mean() * 100
        out[label] = {"trades": len(part), "batches": len(means), "batch_mean_pct": round(means.mean(), 2), "capped_batch_mean_pct": round(capped.mean(), 2),
                      "sign_flip_p": round(ledger.sign_flip_p(means.tolist()), 4)}
    return out


async def dryrun(symbol: str) -> dict:
    book = ledger.Ledger(Path(tempfile.mkdtemp()))
    event = {"source_id": f"K|dryrun-{symbol}", "stream": "K", "token": symbol, "symbol": symbol, "entry_day": str(book.clock().date())}
    async with ledger._default_client() as client:
        market = await ledger.market_snapshot(client)
        record = await book._entry_record(client, {"event": event}, market, {})
        fake_exit = await book._exit_record(client, {"event": {**event, "exit_day": event["entry_day"]}, "entry": record}, market)
        rows = await ledger.funding_rows(client, symbol, record["entry_ms"] - 3 * ledger.DAY_MS, ledger.ms(book.clock()))
    return {"live_contracts": len(market["live"]), "pool": len(market["ranked"]), "entry_price": record["entry_price"], "book": record["book"], "sigma": record["sigma"],
            "quote_volume": record["quote_volume"], "impact_side": record["impact_side"], "basket": [m["s"] for m in record["basket"]],
            "basket_sigma_default": sum(m["sigma"] == ledger.DEFAULT_SIGMA for m in record["basket"]), "exit_coin_price": fake_exit["coin"]["exit_price"],
            "exit_reason": fake_exit["reason"], "funding_rows_3d": len(rows), "funding_sample": rows[-1:]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["activate", "cycle", "status", "verify", "replay", "dryrun"])
    ap.add_argument("--symbol", default="BTCUSDT")
    args = ap.parse_args()
    if args.cmd == "activate":
        rec = ledger.Ledger().activate()
        emit({"outcome": "activated", "at": rec["at"], "pins": rec["pins"], "data": rec["data"]})
    elif args.cmd == "cycle":
        async def one():
            async with ledger._default_client() as client:
                return await ledger.Ledger().cycle(client)
        emit(asyncio.run(one()))
    elif args.cmd == "status":
        emit(ledger.summary())
    elif args.cmd == "verify":
        try:
            recs = ledger.Journal().records()
            st = ledger.state(ledger.Journal())
            activated = st["activation"] or {}
            current = {p.name: ledger.sha(p.read_bytes()) for p in ledger.SOURCE_MODULES}
            emit({"records": len(recs), "chain": "ok", "pins_current": ledger.pins(), "pins_activated": activated.get("pins"), "drift": st["drift"],
                  "source_modules_unchanged": activated.get("source_modules_sha256") == current})
        except Exception as exc:  # noqa: BLE001
            emit({"chain": "broken", "error": str(exc)})
    elif args.cmd == "replay":
        emit(replay())
    else:
        emit(asyncio.run(dryrun(args.symbol)))


if __name__ == "__main__":
    main()
