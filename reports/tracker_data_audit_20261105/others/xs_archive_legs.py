"""Audit each recorded XS leg against Binance USD-M archive timestamps."""
import json
from functools import lru_cache
from pathlib import Path

import pandas as pd

R = Path("data/research/xs_reversal")
H = Path("data/historical/binance_futures")
O = Path("reports/tracker_data_audit_20261105/others")
HOUR = pd.Timedelta(hours=1)

@lru_cache(maxsize=None)
def series(symbol, field):
    folder = H / f"{symbol[:-4]}_USDT"
    files = [folder / f"{field}.parquet"]
    if field == "1h":
        parts = folder / "1h_parts"
        files += sorted(parts.glob("2026*.parquet"))
    frames = [pd.read_parquet(f, columns=["close"] if field == "1h" else ["funding_rate", "mark_price"]) for f in files if f.exists()]
    if not frames:
        return pd.DataFrame()
    d = pd.concat(frames)
    d.index = pd.to_datetime(d.index, utc=True).tz_convert(None)
    return d[~d.index.duplicated(keep="last")].sort_index()

results = {"n_legs": 0, "entry_matches": 0, "exit_matches": 0, "funding_matches_to_1e8": 0, "gaps": [], "cash_impact_pp": {}}
ledger = json.loads((R / "tracker.json").read_text(encoding="utf-8"))
for day, book in ledger["days"].items():
    if book["status"] != "closed" or book.get("late") or book.get("missing_positions"):
        continue
    entry_bar = pd.Timestamp(book["entry_bar_ms"], unit="ms")
    exit_bar = entry_bar + pd.Timedelta(days=1)
    start, end = entry_bar + HOUR, exit_bar + HOUR
    cash_impact = 0
    for side in ("longs", "shorts"):
        for symbol, leg in book[side].items():
            results["n_legs"] += 1
            prices = series(symbol, "1h").get("close", pd.Series(dtype=float))
            data = series(symbol, "funding_rate")
            saved = {k: leg[k] for k in ("entry", "exit", "funding")}
            source = {"entry": prices.get(entry_bar), "exit": prices.get(exit_bar), "funding": None}
            if not data.empty:
                d = data[(data.index > start) & (data.index <= end)]
                source["funding"] = float(d.funding_rate.sum())
                fixed = float((d.funding_rate * d.mark_price).sum()) / leg["entry"]
                sign = -1 if side == "longs" else 1
                cash_impact += sign * (fixed - leg["funding"]) * 50 / len(book[side])
            matches = [saved["entry"] == source["entry"], saved["exit"] == source["exit"],
                       source["funding"] is not None and abs(saved["funding"] - source["funding"]) <= 1e-8]
            for k, hit in zip(("entry_matches", "exit_matches", "funding_matches_to_1e8"), matches):
                results[k] += int(hit)
            if not all(matches):
                results["gaps"].append({"date": day, "symbol": symbol, "side": side, "saved": saved, "source": source})
    results["cash_impact_pp"][day] = cash_impact
(O / "xs_archive_legs.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
print({k: v for k, v in results.items() if k not in ("gaps", "cash_impact_pp")}, len(results["gaps"]))
