"""Read-only checks of legacy event studies and saved unlock schedule vintages."""
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import numpy as np
import pandas as pd

OUTPUT = Path("reports/tracker_data_audit_20261106_0215")
RESEARCH = Path("data/research")
SCRATCH = Path(r"C:\Users\zxc\AppData\Local\Temp\claude\93bc013aacae6232\d7a802af-aec4-4746-bd5c-659cad3a9a92\scratchpad\team_mining\round2")
BINANCE = "https://fapi.binance.com/fapi/v1"
ONE_DAY = pd.Timedelta(days=1)


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def ms(value):
    return int(pd.Timestamp(value, tz="UTC").timestamp() * 1000)


def public(path, **params):
    for _ in range(2):
        try:
            with urlopen(f"{BINANCE}/{path}?{urlencode(params)}", timeout=8) as response:
                return {"status": response.status, "rows": json.load(response)}
        except HTTPError as exc:
            return {"status": exc.code, "error_code": json.load(exc).get("code")}
        except (URLError, TimeoutError) as exc:
            result = {"error_type": type(exc).__name__}
    return result


@lru_cache(maxsize=None)
def archive_funding(symbol):
    folder = Path("data/historical/binance_futures") / (symbol[:-4] + "_USDT")
    source = folder / "funding_rate.parquet"
    paths = ([source] if source.exists() else []) + sorted((folder / "funding_rate_parts").glob("*.parquet"))
    if not paths:
        return None
    f = pd.concat([pd.read_parquet(path) for path in paths])
    f.index = pd.to_datetime(f.index, utc=True).tz_convert(None)
    return f[~f.index.duplicated(keep="last")].sort_index()


def funding_audit():
    old_path = RESEARCH / "unlock_study/short_backtest_-7_-1_5.csv"
    old = pd.read_csv(old_path)
    old["day"] = pd.to_datetime(old["date"], utc=True).dt.tz_convert(None)
    research = pd.read_csv(SCRATCH / "trades.csv")
    research = research[research.stream.eq("U")].copy()
    for key in ("day", "entry_time", "exit_time"):
        research[key] = pd.to_datetime(research[key], format="mixed")
    joined = old[["token", "day", "funding"]].merge(research, on=["token", "day"], suffixes=("_old", "_model"), validate="one_to_one")
    rows = []
    for t in joined.itertuples(index=False):
        f = archive_funding(t.perp)
        if f is None:
            continue
        # Same comparison window as the Claude model to isolate rates and units.
        lower, upper = t.entry_time + pd.Timedelta(minutes=1), t.exit_time + pd.Timedelta(minutes=1)
        matched = f[(f.index > lower) & (f.index <= upper)]
        if matched.empty:
            continue
        weighted = (matched["funding_rate"] * matched["mark_price"].fillna(t.entry_px) / t.entry_px).sum()
        rows.append({"token": t.token, "day": str(t.day.date()), "events": len(matched),
                     "old_pct": 100 * t.funding_old, "model_pct": 100 * t.funding_model,
                     "archive_pct": float(100 * weighted), "raw_rate_sum_pct": float(100 * matched["funding_rate"].sum())})
    layer = next(t for t in rows if t["token"] == "LAYER" and t["day"] == "2025-05-12")
    cache = read(RESEARCH / "unlock_study/perps/LAYER.json")
    assert isinstance(cache, dict) and cache["symbol"] == "LAYERUSDT"
    return {
        "old_n": len(old), "old_zero_funding_n": int(old.funding.eq(0).sum()),
        "research_n": len(research), "matched_n": len(joined), "matched_old_zero_n": int(joined.funding_old.eq(0).sum()),
        "matched_old_zero_model_nonzero_n": int((joined.funding_old.eq(0) & joined.funding_model.ne(0)).sum()),
        "matched_old_minus_model_mean_pp": float(100 * (joined.funding_old-joined.funding_model).mean()),
        "archive_with_events_n": len(rows), "archive_old_zero_nonzero_n": sum(r["old_pct"] == 0 and abs(r["archive_pct"]) > 1e-9 for r in rows),
        "layer": layer, "layer_cached_events": len(cache["funding"]), "rows": rows,
    }


def index(folder):
    value = read(folder / "emissions_index.json")
    if isinstance(value, dict):
        value = value["data"]
    return {x["protocolSlug"]: x for x in value if x.get("protocolSlug")}


def extract_cliffs(idx, path):
    data = read(path)
    series = []
    for cat in data.get("documentedData", {}).get("data", []):
        points = {p["timestamp"] // 86400: float(p.get("unlocked") or 0) for p in cat.get("data", [])}
        if points:
            series.append(pd.Series(points))
    if not series:
        return {}
    totals = pd.concat(series, axis=1).sort_index().ffill().fillna(0).sum(axis=1)
    events = {}
    for event in idx.get("unlockEvents") or []:
        day = event["timestamp"] // 86400
        amount = sum(float(p.get("amount") or 0) for p in event.get("cliffAllocations") or [])
        p = totals.index.searchsorted(day) - 1
        if p >= 0 and amount > 0 and totals.iloc[p] > 0:
            ratio = float(amount / totals.iloc[p] * 100)
            events[day] = max(events.get(day, 0.0), ratio)
    return events


def schedule_audit():
    p0, p1 = RESEARCH / "unlock_study", RESEARCH / "unlock_short/cache"
    x, y = index(p0), index(p1)
    dates = [ms(d)//86400000 for d in ("2025-01-01", "2026-09-27", "2026-11-03")]
    checked = []
    total = 0
    for slug in sorted(x.keys() & y.keys()):
        a, b = p0 / "emissions" / (slug + ".json"), p1 / "emissions" / (slug + ".json")
        if not a.is_file() or not b.is_file():
            continue
        total += 1
        s0, s1 = extract_cliffs(x[slug], a), extract_cliffs(y[slug], b)
        for day in sorted(s0.keys() | s1.keys()):
            if dates[0] <= day <= dates[2] and max(s0.get(day, 0.0),s1.get(day, 0.0)) >= 5:
                checked.append({"slug": slug, "date": str(pd.Timestamp(day,unit="D").date()),
                                "after_early_snapshot": day > dates[1], "early": s0.get(day), "late": s1.get(day)})
    grouped = {}
    for cohort in (False, True):
        earlier = [r for r in checked if r["after_early_snapshot"] == cohort and (r["early"] or 0) >= 10]
        later = [r for r in checked if r["after_early_snapshot"] == cohort and (r["late"] or 0) >= 10]
        grouped["future_due" if cohort else "past_2025_plus"] = {
            "earlier_ge_10": len(earlier), "later_ge_10": len(later),
            "lost_ge_10": [r for r in earlier if (r["late"] or 0) < 10],
            "gained_ge_10": [r for r in later if (r["early"] or 0) < 10],
            "changed_sizes": [r for r in earlier if abs((r["late"] or 0)-(r["early"] or 0)) > 1e-5],
        }
    return {"shared_schedule_slugs":total,"groups":grouped,"exact_day_events":checked}


def check_public():
    symbols = {"ICXUSDT":"2026-08-28", "AIUSDT":"2026-06-30", "RAYUSDT":"2025-06-19"}
    jobs = {"LAYER_funding": ("fundingRate", {"symbol":"LAYERUSDT", "startTime":ms("2025-05-06")+60_001,
                                             "endTime":ms("2025-05-12")+60_000, "limit":1000})}
    for symbol,date in symbols.items():
        jobs[symbol] = ("klines", {"symbol":symbol,"interval":"1d","startTime":ms(date),"endTime":ms(date)+8*86400000-1,"limit":8})
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = {k:pool.submit(public,path,**params) for k,(path,params) in jobs.items()}
    results = {k:f.result() for k,f in results.items()}
    f = results["LAYER_funding"]
    if isinstance(f.get("rows"),list):
        f["weighted_pct"] = sum(float(x["fundingRate"])*float(x["markPrice"])/2.9587 for x in f["rows"])*100
        f["raw_pct"] = sum(float(x["fundingRate"]) for x in f["rows"])*100
    return results


if __name__ == "__main__":
    public_results = check_public()
    funding = funding_audit()
    schedule = schedule_audit()
    result = {"public":public_results,"funding":funding,"schedule":schedule}
    (OUTPUT/"legacy_event_sources.json").write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
    print("Funding:", {k:v for k,v in funding.items() if k!="rows"})
    print("Schedule:",schedule["shared_schedule_slugs"],schedule["groups"])
    for k, v in public_results.items():
        rows=v.get("rows") or []
        print("Binance:",k,{a:b for a,b in v.items() if a!="rows"}, "row count",len(rows),
              [(pd.Timestamp(x[0],unit="ms").date().isoformat(),x[4],x[5],x[8]) for x in rows] if k!="LAYER_funding" else "")
