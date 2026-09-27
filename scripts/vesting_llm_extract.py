"""Round 6: LLM reads the project's own docs and returns a vesting spec (plus a no-docs memory baseline).

Usage: vesting_llm_extract.py [input docs file] [output file] [modes]
defaults: docs.json extractions.json docs,memory (step 1); step 2 runs
discovered.json discovered_extractions.json docs.
"""
import asyncio, json, re, sys
from pathlib import Path
import httpx
sys.path.insert(0, ".")
from core.ai.research_context_generator import generate_json

OUT = Path("data/research/vesting_llm")
KEY = re.compile(r"vest|unlock|cliff|allocat|tge|token generation|distribut|supply|release|lock|month|year|investor|team|contributor|ecosystem|treasury|airdrop", re.I)

SYSTEM = "You extract token vesting schedules from project documentation into strict JSON. Never invent numbers the text does not support."
SPEC = """Return JSON:
{"total_supply": number|null,
 "tge_date": "YYYY-MM-DD"|null,
 "allocations": [{"name": str, "pct_of_total": number (0-100),
   "tge_unlock_pct": number (0-100, share of THIS allocation unlocked at its start),
   "cliff_months": number, "vesting_months": number (linear period after the cliff; 0 = all at cliff end),
   "frequency": "monthly"|"daily"|"quarterly"|"once",
   "start_date": "YYYY-MM-DD"|null (only if the schedule starts at a date other than TGE),
   "schedule_known": bool (false when release is discretionary / governance-controlled / emissions without a fixed schedule)}],
 "confidence": number 0-1, "notes": str}
Allocations must cover the whole supply (sum ~100). Mining/staking/farming emissions with a fixed formula: approximate as linear over the stated period."""

def focus(text, limit=24000):
    """Keep paragraphs near vesting keywords so long pages fit."""
    paras = [p for p in text.split("\n") if p.strip()]
    keep = set()
    for i, p in enumerate(paras):
        if KEY.search(p):
            keep.update(range(max(0, i - 2), min(len(paras), i + 3)))
    out = "\n".join(paras[i] for i in sorted(keep)) or text
    return out[:limit]

async def first_trade_day(c, ticker):
    r = await c.get("https://api.binance.com/api/v3/klines", params={"symbol": f"{ticker}USDT", "interval": "1d", "startTime": 0, "limit": 1})
    rows = r.json()
    return str(__import__("pandas").Timestamp(rows[0][0], unit="ms").date()) if isinstance(rows, list) and rows else None

async def one(row, mode, sem, c):
    async with sem:
        hint = f"Token: {row['ticker']} (project slug: {row['slug']}). First traded on Binance spot: {row['binance_first_day']} (use only as a TGE fallback if the text gives no date)."
        if mode == "docs":
            docs = "\n\n".join(f"### Source: {s['url']}\n{focus(s['text'], 24000 // len(row['sources']))}" for s in row["sources"])
            prompt = f"{hint}\n\nDocumentation:\n{docs}\n\n{SPEC}"
        else:
            prompt = f"{hint}\n\nNo documentation is provided. From your own knowledge only, give the token's vesting schedule. Set confidence low if unsure.\n\n{SPEC}"
        for attempt in range(3):
            try:
                out = await generate_json(prompt, system_prompt=SYSTEM, timeout=240)
                out.pop("_generation", None)
                return out
            except Exception as exc:  # noqa: BLE001
                err = f"{type(exc).__name__}: {exc}"[:200]
                await asyncio.sleep(5)
        return {"error": err}

async def main():
    src, dst, modes = (sys.argv[1:] + ["docs.json", "extractions.json", "docs,memory"][len(sys.argv) - 1:])[:3]
    modes = modes.split(",")
    rows = json.loads((OUT / src).read_text(encoding="utf-8"))
    path = OUT / dst
    done = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    async with httpx.AsyncClient(timeout=30) as c:
        for r in rows:
            r["binance_first_day"] = await first_trade_day(c, r["ticker"])
        sem = asyncio.Semaphore(4)
        jobs = [(r, m) for r in rows for m in modes if not done.get(r["ticker"], {}).get(m) or "error" in done[r["ticker"]][m]]
        results = await asyncio.gather(*(one(r, m, sem, c) for r, m in jobs))
    for (r, m), res in zip(jobs, results):
        done.setdefault(r["ticker"], {"slug": r["slug"], "binance_first_day": r["binance_first_day"], "covered": r.get("covered")})[m] = res
    path.write_text(json.dumps(done, ensure_ascii=False, indent=1), encoding="utf-8")
    errs = sum("error" in v.get(m, {}) for v in done.values() for m in modes)
    print("tokens", len(done), "errors", errs)

asyncio.run(main())
