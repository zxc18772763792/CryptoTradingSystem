"""Revalue the stored constituent list from exchange archives, without network or tracker writes."""
import json
from functools import lru_cache
from pathlib import Path
from statistics import mean

import pandas as pd

R = Path("data/research")
H = Path("data/historical")
O = Path("reports/tracker_data_audit_20261105/others")
DAY = pd.Timedelta(days=1)

def state(name, key):
    return json.loads((R / name / "tracker.json").read_text(encoding="utf-8"))[key]

@lru_cache(maxsize=None)
def series(venue, symbol, tf):
    path = H / venue / f"{symbol[:-4]}_USDT" / f"{tf}.parquet"
    data = pd.read_parquet(path)
    data.index = pd.to_datetime(data.index, utc=True).tz_convert(None)
    return data[~data.index.duplicated(keep="last")].sort_index()

def archived_leg(symbol, start, end, direction, fee, include_start=False, stop=None):
    venue = "binance_futures"
    close = series(venue, symbol, "1d").close
    prices = [float(close.loc[time - DAY]) for time in (start, end)]
    if stop is not None:
        prices[1] = stop
    funding = series(venue, symbol, "funding_rate")
    after = funding.index >= start if include_start else funding.index > start
    funds = funding[after & (funding.index <= end)]
    funding_sum = float(funds.funding_rate.sum())
    cash_funding_sum = float((funds.funding_rate * funds.mark_price).sum() / prices[0])
    price_change = prices[1] / prices[0] - 1
    return {"model": 100 * (direction * (price_change - funding_sum) - fee),
            "cash_funding_model": 100 * (direction * (price_change - cash_funding_sum) - fee),
            "funding_rows": len(funds)}

def cases():
    for key, t in state("unlock_short_t7", "trades").items():
        if t.get("hedged_return_pct") is not None and not t.get("late"):
            yield "unlock", key, t, t["basket"], pd.Timestamp(t["entry_day"]) + DAY, pd.Timestamp(t["exit_day_actual"]) + DAY, .002
    for key, t in state("korea_caution_hedged", "trades").items():
        if t.get("hedged_return_pct") is not None and not t.get("late") and not t.get("backfilled"):
            yield "korea", key, t, t["evidence"]["market_basket"]["symbols"], pd.Timestamp(t["entry_at"], unit="ms"), pd.Timestamp(t["exit_at"], unit="ms"), .001

out = {"unlock": {}, "korea": {}, "supply": {}}
for kind, key, t, symbols, start, end, fee in cases():
    short = archived_leg(t["symbol"], start, end, -1, fee, include_start=kind == "unlock",
                         stop=t["exit_price"] if t["status"] == "stopped" else None)
    longs, unresolved = {}, {}
    for symbol in symbols:
        try:
            longs[symbol] = archived_leg(symbol, start, end, +1, fee, include_start=kind == "unlock")
        except (FileNotFoundError, KeyError) as exc:
            unresolved[symbol] = str(exc)
    outcome = {"n_frozen": len(symbols), "n_local": len(longs), "unresolved": unresolved,
               "saved": t["hedged_return_pct"], "short": short, "longs": longs}
    for model in ("model", "cash_funding_model"):
        outcome[model] = short[model] + mean(x[model] for x in longs.values())
    out[kind][key] = outcome
    print(kind, key, {k: v for k, v in outcome.items() if k not in ("longs", "short")}, flush=True)
for day, t in state("supply_factor", "months").items():
    if t["status"] != "closed":
        continue
    rows = []
    for leg in ("long", "short"):
        for token, x in t[leg].items():
            try:
                close = series("binance", t["symbols"][token], "1d").close
            except FileNotFoundError:
                close = pd.Series(dtype=float)
            entry_date, exit_date = pd.Timestamp(t["rebalance_day"]), pd.Timestamp(t["exit_day"])
            a, b = close.get(entry_date), close.get(exit_date)
            if b is None and x.get("exit") is not None:
                folder = H / "binance" / f"{token}_USDT" / "1d_parts"
                for part in (folder / f"{exit_date.year}.parquet", folder / f"{exit_date.date()}.parquet"):
                    if part.exists():
                        b = pd.read_parquet(part, columns=["close"]).close.get(exit_date)
                        if b is not None:
                            break
            rows.append({"token": token, "leg": leg, "archive_entry": a, "archive_exit": b, "saved_entry": x.get("entry"), "saved_exit": x.get("exit")})
    out["supply"][day] = rows
    print("supply", day, len(rows), "entry matches", sum(x["archive_entry"] == x["saved_entry"] for x in rows),
          "exit matches", sum(x["archive_exit"] == x["saved_exit"] for x in rows), flush=True)
(O / "archive_basket_recalc.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
