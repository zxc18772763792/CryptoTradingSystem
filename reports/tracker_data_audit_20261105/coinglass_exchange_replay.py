"""Read-only replay of saved CoinGlass books against public Binance USD-M data."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics as stats

import httpx

REPORT = Path("reports/tracker_data_audit_20261105")
BASE = "https://fapi.binance.com/fapi/v1"
TRACKERS = {
    "main": "coinglass_composite",
    "wide": "coinglass_composite_wide_eq",
    "monthly": "coinglass_composite_monthly_v1",
}
COST = 0.0012


def epoch(value):
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def book_rows(states):
    for variant, state in states.items():
        for day, record in sorted(state["books"].items()):
            for book_type in ("main", "shadow"):
                book = record.get(book_type, {})
                if book.get("status") == "closed":
                    yield (variant, day, book_type), record, book


def tally(states):
    result = {}
    for key, record, book in book_rows(states):
        sides = {}
        gaps = []
        for side, sign in (("longs", 1), ("shorts", -1)):
            positions = book[side]
            if len(positions) != book["scored"] // 5:
                gaps.append(f"{side} count {len(positions)}")
            sides[side] = []
            for symbol, pos in positions.items():
                expected = sign * (pos["exit"] / pos["entry"] - 1 - pos["funding"])
                if abs(expected - pos["return"]) > 1.1e-8:
                    gaps.append(f"{side} {symbol} return mismatch {expected - pos['return']}")
                sides[side].append(pos["return"])
        pnl = 100 * (stats.mean(sides["longs"]) + stats.mean(sides["shorts"]) - 2 * COST * book["turnover"])
        if abs(pnl - book["net_pct"]) > 0.000051:
            gaps.append(f"book net difference {pnl - book['net_pct']}")
        result["/".join(key)] = {
            "net_pct": book["net_pct"], "recalc_net_pct": pnl,
            "positions": sum(len(x) for x in sides.values()), "mismatches": gaps,
        }
    return result


def end_time(book, pos):
    return pos.get("funding_until_ms") or epoch(pos.get("exit_observed_at") or book["settled_at"])


def price_jobs(states):
    jobs = {}
    for (variant, day, book_type), record, book in book_rows(states):
        if variant != "main" or book_type != "main":
            continue
        for side in ("longs", "shorts"):
            positions = book[side]
            symbol = max(positions, key=lambda sym: abs(positions[sym]["funding"]))
            pos = positions[symbol]
            for field, ts in (("entry", epoch(record["formed_at"])), ("exit", end_time(book, pos))):
                jobs[(day, side, symbol, field)] = (ts, pos[field])
    return jobs


async def query_all(client, jobs):
    semaphore = asyncio.Semaphore(4)
    pause = asyncio.Lock()
    rate_limited = asyncio.Event()

    async def get(key, endpoint, params):
        async with semaphore:
            if rate_limited.is_set():
                return key, {"error": "Skipped after rate limit"}
            async with pause:
                await asyncio.sleep(0.20)
            try:
                resp = await client.get(BASE + endpoint, params=params)
                if resp.status_code == 429:
                    rate_limited.set()
                if resp.status_code != 200:
                    return key, {"error": f"HTTP {resp.status_code}"}
                body = resp.json()
                if not isinstance(body, list):
                    return key, {"error": "Unexpected response shape"}
                return key, body
            except (httpx.HTTPError, ValueError) as exc:
                return key, {"error": type(exc).__name__}

    return dict(await asyncio.gather(*(get(key, endpoint, params) for key, (endpoint, params) in jobs.items())))


def verify_prices(jobs, replies):
    checks = []
    for (day, side, symbol, field), (ts, price) in jobs.items():
        bars = replies[(day, side, symbol, field)]
        row = {"day": day, "side": side, "symbol": symbol, "field": field, "timestamp_ms": ts, "ledger_price": price}
        if not isinstance(bars, list) or not bars:
            row["status"] = bars.get("error") if isinstance(bars, dict) else "no bars"
        else:
            same_minute = [b for b in bars if b[0] <= ts <= b[6]]
            nearby = [b for b in bars if b[0] <= ts + 120_000 and b[6] >= ts - 60_000]
            row["nearby_low"] = min(float(b[3]) for b in nearby)
            row["nearby_high"] = max(float(b[2]) for b in nearby)
            if any(float(b[3]) <= price <= float(b[2]) for b in same_minute):
                row["status"] = "same minute"
            elif any(float(b[3]) <= price <= float(b[2]) for b in nearby):
                row["status"] = "nearby minute"
            elif any(float(b[3]) <= price <= float(b[2]) for b in bars):
                row["status"] = "within six minutes"
            else:
                row["status"] = "outside"
        checks.append(row)
    return checks


def verify_funding(states, replies):
    checks, books = [], {}
    for (variant, day, book_type), record, book in book_rows(states):
        if variant != "main":
            continue
        lo = epoch(record["formed_at"])
        corrected_sides = {}
        for side, sign in (("longs", 1), ("shorts", -1)):
            corrected_sides[side] = []
            for symbol, pos in book[side].items():
                hi = end_time(book, pos)
                series = replies[symbol]
                row = {"day": day, "book": book_type, "side": side, "symbol": symbol, "entry": pos["entry"],
                       "exit": pos["exit"], "from_ms": lo, "to_ms": hi, "ledger_raw_rate": pos["funding"]}
                if not isinstance(series, list):
                    row["status"] = series["error"]
                else:
                    matched = [x for x in series if lo < int(x["fundingTime"]) <= hi]
                    raw = math.fsum(float(x["fundingRate"]) for x in matched)
                    row.update(events=len(matched), api_raw_rate=raw, raw_difference=raw - pos["funding"])
                    ids = [int(x["fundingTime"]) for x in matched]
                    if not matched or len(ids) != len(set(ids)) or any(x["symbol"] != symbol for x in matched):
                        row["status"] = "incomplete or duplicate events"
                    elif any(not math.isfinite(float(x.get("markPrice") or 0)) or float(x.get("markPrice") or 0) <= 0 for x in matched):
                        row["status"] = "missing mark price"
                    else:
                        adjusted = math.fsum(float(x["fundingRate"]) * float(x["markPrice"]) / pos["entry"] for x in matched)
                        ret = sign * (pos["exit"] / pos["entry"] - 1 - adjusted)
                        row.update(status="match" if abs(raw - pos["funding"]) <= 5.01e-9 else "rate sum mismatch",
                                   marked_funding_per_entry=adjusted, corrected_return=ret)
                        corrected_sides[side].append(ret)
                checks.append(row)
        if all(len(corrected_sides[s]) == len(book[s]) for s in corrected_sides):
            corrected = 100 * (sum(stats.mean(x) for x in corrected_sides.values()) - 2 * COST * book["turnover"])
            books[day + "/" + book_type] = {"old_net_pct": book["net_pct"], "marked_net_pct": corrected,
                                            "delta_pp": corrected - book["net_pct"]}
    return checks, books


async def run():
    states = {variant: json.loads((Path("data/research") / name / "tracker.json").read_text(encoding="utf-8-sig"))
              for variant, name in TRACKERS.items()}
    arithmetic = tally(states)
    times = {}
    for (variant, _, _), record, book in book_rows(states):
        if variant == "main":
            for side in ("longs", "shorts"):
                for symbol, pos in book[side].items():
                    lo, hi = epoch(record["formed_at"]), end_time(book, pos)
                    prev = times.get(symbol, (lo, hi))
                    times[symbol] = min(lo, prev[0]), max(hi, prev[1])
    samples = price_jobs(states)
    jobs = {sym: ("/fundingRate", {"symbol": sym, "startTime": lo + 1, "endTime": hi, "limit": 1000})
            for sym, (lo, hi) in times.items()}
    for key, (ts, _) in samples.items():
        minute = ts // 60_000 * 60_000
        jobs[key] = ("/klines", {"symbol": key[2], "interval": "1m", "startTime": minute - 360_000,
                                  "endTime": minute + 360_000, "limit": 13})
    print(f"Requesting {len(times)} funding series and {len(samples)} public price windows", flush=True)
    async with httpx.AsyncClient(timeout=httpx.Timeout(10, connect=5)) as client:
        replies = await query_all(client, jobs)
    funding_checks, marked_books = verify_funding(states, replies)
    prices = verify_prices(samples, replies)
    result = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "records": {k: len(v["books"]) for k, v in states.items()},
        "arithmetic": arithmetic, "funding_checks": funding_checks, "marked_books": marked_books,
        "price_checks": prices,
        "funding_api": {symbol: replies[symbol] for symbol in times},
        "price_api": [{"key": list(k), "bars": replies[k]} for k in samples],
    }
    (REPORT / "coinglass_exchange_replay.json").write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
    print("Arithmetic:", len(arithmetic), sum(b["positions"] for b in arithmetic.values()),
          {k: b["mismatches"] for k, b in arithmetic.items() if b["mismatches"]})
    for name, checks in (("Price", prices), ("Funding", funding_checks)):
        print(name, Counter(r["status"] for r in checks))
        print("Exceptions", [r for r in checks if r["status"] not in {"match", "same minute", "nearby minute"}][:9])
    for book_type in ("main", "shadow"):
        valid = [b for name, b in marked_books.items() if name.endswith("/" + book_type)]
        if valid:
            print(book_type, len(valid), {key: stats.mean(b[key] for b in valid) for key in valid[0]})
    worst = sorted((r for r in funding_checks if "marked_funding_per_entry" in r),
                   key=lambda r: abs(r["marked_funding_per_entry"] - r["ledger_raw_rate"]), reverse=True)
    print("Largest funding valuation deltas:", worst[:3])


if __name__ == "__main__":
    asyncio.run(run())
